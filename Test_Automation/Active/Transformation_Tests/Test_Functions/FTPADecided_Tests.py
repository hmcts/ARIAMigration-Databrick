from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import inspect

#Import Test Results class
from models.test_result import TestResult

#Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "ftpaDecided"

############################################################################################

#######################
# default mapping Init code
#######################

def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "isDlrmSetAsideEnabled",
            "isReheardAppealEnabled"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

def test_ftpaDecided_defaultValues(test_df, fields_to_exclude):
    try:
        expected_defaults = {
            "isDlrmSetAsideEnabled": "Yes",
            "isReheardAppealEnabled": "Yes"
        }
        
        results_list = []

        for field, expected in expected_defaults.items():
            if field in fields_to_exclude:
                continue
            condition = (col(field) != expected)
            if test_df.filter(condition).count() > 0:
                results_list.append(TestResult(
                    field, 
                    "FAIL", 
                    f"Failed to check Default Mapping for : {field} - expected : {expected} - found {str(test_df.filter(condition).count())} records not matching", 
                    test_from_state,
                    inspect.stack()[0].function
                ))
            else:
                results_list.append(TestResult(
                    field, 
                    "PASS", 
                    f"Checked Default Mapping for : {field} - found correct value : {expected}", 
                    test_from_state,
                    inspect.stack()[0].function
                ))


        return results_list
    except Exception as e:
        error_message = str(e)        
        return [TestResult("DefaultMapping", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)]



############################################################################################
#######################
#ftpa Init code
#######################
def test_ftpa_init(json, M1_bronze, M3_bronze):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "ftpaApplicantType",
            "ftpaFirstDecision",
            "ftpaAppellantDecisionDate",
            "ftpaRespondentDecisionDate",
            "ftpaFinalDecisionForDisplay",
            "ftpaAppellantRjDecisionOutcomeType",
            "ftpaRespondentRjDecisionOutcomeType",
            "isFtpaAppellantNoticeOfDecisionSetAside",
            "isFtpaRespondentNoticeOfDecisionSetAside",
            "isAppellantFtpaDecisionVisibleToAll",
            "isRespondentFtpaDecisionVisibleToAll",
            "isFtpaAppellantDecided",
            "isFtpaRespondentDecided",
            "secondFtpaDecisionExists",
            "allFtpaAppellantDecisionDocs",
            "allFtpaRespondentDecisionDocs",
            "ftpaAppellantNoticeDocument",
            "ftpaRespondentNoticeDocument",
            "ftpaList",
            "ftpaAppellantApplicationDate", 
            "ftpaRespondentApplicationDate",
            "ftpaFinalDecisionForDisplay",  
            "ftpaAppellantOutOfTimeExplanation",   
            "ftpaRespondentOutOfTimeExplanation",  
            "isFtpaAppellantNoticeOfDecisionSetAside", 
            "isFtpaRespondentNoticeOfDecisionSetAside"  
        )

        M1_bronze = M1_bronze.select(
            "CaseNo"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "CaseStatus",
            "StatusId",
            "Party",
            "Outcome"
        )

        test_df = test_df.join(
            M1_bronze,
            json["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        ).join(
            M3_bronze,
            json["appealReferenceNumber"] == M3_bronze["CaseNo"],
            "inner"
        ).drop(M1_bronze["CaseNo"], M3_bronze["CaseNo"])

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("ftpa", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state,inspect.stack()[0].function)
    

#######################
# ftpaApplicantType - IF M3.Party 1 = "appellant"; IF M3.Party 2 = "respondent" 
# (MAX StatusId WHERE CaseStatus = 39 AND Outcome IN (30, 31, 14))
#######################
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window

def test_ftpaApplicantType_test1(test_df):
    try:
        # 1. Filter for the specific Decided State and Outcomes
        outcome_list = [30, 31, 14]
        target_records = test_df.filter(
            (col("CaseStatus") == 39) & 
            (col("Outcome").isin(outcome_list))
        ) 
        
        if target_records.count() == 0:
            return TestResult("ftpaApplicantType", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 with Outcome 30, 31, or 14 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Define the expected value based on the Party
        # Party 1 -> appellant, Party 2 -> respondent
        expected_val = when(col("Party") == 1, "appellant") \
                      .when(col("Party") == 2, "respondent") \
                      .otherwise("unknown")

        # 4. Acceptance Criteria: Find rows where the actual value does not match the expected logic
        acceptance_critera = winning_records.filter(
            col("ftpaApplicantType") != expected_val
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaApplicantType", "FAIL", f"ftpaApplicantType acceptance criteria failed: found {acceptance_critera.count()} rows where applicant type does not match Party mapping", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaApplicantType", "PASS", "ftpaApplicantType acceptance criteria pass: all rows correctly map Party to 'appellant' or 'respondent'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaApplicantType", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaFirstDecision - Mapping Outcome to String Status
# (MAX StatusId WHERE CaseStatus = 39 AND Outcome IN (30, 31, 14))
#######################
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window

def test_ftpaFirstDecision_test1(test_df):
    try:
        # 1. Filter for the Decided State and specific Outcomes
        outcome_list = [30, 31, 14]
        target_records = test_df.filter(
            (col("CaseStatus") == 39) & 
            (col("Outcome").isin(outcome_list))
        ) 
        
        if target_records.count() == 0:
            return TestResult("ftpaFirstDecision", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 with Outcome 30, 31, or 14 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Define the expected mapping logic
        expected_val = when(col("Outcome") == 30, "granted") \
                      .when(col("Outcome") == 31, "refused") \
                      .when(col("Outcome") == 14, "notAdmitted") \
                      .otherwise("unknown")

        # 4. Acceptance Criteria: Validate actual ftpaFirstDecision against the map
        acceptance_critera = winning_records.filter(
            col("ftpaFirstDecision") != expected_val
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaFirstDecision", "FAIL", f"ftpaFirstDecision mapping failed: found {acceptance_critera.count()} rows with incorrect status strings", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaFirstDecision", "PASS", "ftpaFirstDecision mapping pass: all Outcomes (30, 31, 14) correctly mapped to granted, refused, or notAdmitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaFirstDecision", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantDecisionDate - IF M3.Party IS 1 = Include ISO 8601
# (MAX StatusId WHERE CaseStatus = 39 AND Outcome IN (30, 31, 14))
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_ftpaAppellantDecisionDate_test1(test_df):
    try:
        # 1. Filter for the Decided State and specific Outcomes
        outcome_list = [30, 31, 14]
        target_records = test_df.filter(
            (col("CaseStatus") == 39) & 
            (col("Outcome").isin(outcome_list))
        ) 
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantDecisionDate", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 with Outcome 30, 31, or 14 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Check Party 1 for ISO 8601 Format (YYYY-MM-DD)
        # Regex matches 4 digits, dash, 2 digits, dash, 2 digits
        iso_8601_regex = r"^\d{4}-\d{2}-\d{2}$"
        
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (~col("ftpaAppellantDecisionDate").rlike(iso_8601_regex))
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantDecisionDate", "FAIL", f"ftpaAppellantDecisionDate failed: found {acceptance_critera.count()} rows where Party is 1 & date is not in ISO 8601 format", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantDecisionDate", "PASS", "ftpaAppellantDecisionDate pass: all Appellant records follow ISO 8601 format", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantDecisionDate", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantDecisionDate - IF M3.Party IS 2 = OMIT
# (MAX StatusId WHERE CaseStatus = 39 AND Outcome IN (30, 31, 14))
#######################
def test_ftpaAppellantDecisionDate_test2(test_df):
    try:
        # 1. Filter for the Decided State and specific Outcomes
        outcome_list = [30, 31, 14]
        target_records = test_df.filter(
            (col("CaseStatus") == 39) & 
            (col("Outcome").isin(outcome_list))
        ) 
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantDecisionDate", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Check that field is Omitted (Null) for Party 2
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaAppellantDecisionDate").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantDecisionDate", "FAIL", f"ftpaAppellantDecisionDate failed: found {acceptance_critera.count()} rows where Party is 2 & ftpaAppellantDecisionDate is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantDecisionDate", "PASS", "ftpaAppellantDecisionDate pass: field is correctly omitted for Respondent applications", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantDecisionDate", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaRespondentDecisionDate - IF M3.Party IS 2 = Include ISO 8601
# (MAX StatusId WHERE CaseStatus = 39 AND Outcome IN (30, 31, 14))
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_ftpaRespondentDecisionDate_test1(test_df):
    try:
        # 1. Filter for the Decided State and specific Outcomes
        outcome_list = [30, 31, 14]
        target_records = test_df.filter(
            (col("CaseStatus") == 39) & 
            (col("Outcome").isin(outcome_list))
        ) 
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentDecisionDate", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 with Outcome 30, 31, or 14 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Check Party 2 for ISO 8601 Format (YYYY-MM-DD)
        iso_8601_regex = r"^\d{4}-\d{2}-\d{2}$"
        
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (~col("ftpaRespondentDecisionDate").rlike(iso_8601_regex))
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentDecisionDate", "FAIL", f"ftpaRespondentDecisionDate failed: found {acceptance_critera.count()} rows where Party is 2 & date is not in ISO 8601 format", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentDecisionDate", "PASS", "ftpaRespondentDecisionDate pass: all Respondent records follow ISO 8601 format", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentDecisionDate", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    



#######################
# ftpaRespondentDecisionDate - IF M3.Party IS 1 = OMIT
# (MAX StatusId WHERE CaseStatus = 39 AND Outcome IN (30, 31, 14))
#######################
def test_ftpaRespondentDecisionDate_test2(test_df):
    try:
        # 1. Filter for the Decided State and specific Outcomes
        outcome_list = [30, 31, 14]
        target_records = test_df.filter(
            (col("CaseStatus") == 39) & 
            (col("Outcome").isin(outcome_list))
        ) 
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentDecisionDate", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Check that field is Omitted (Null) for Party 1
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaRespondentDecisionDate").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentDecisionDate", "FAIL", f"ftpaRespondentDecisionDate failed: found {acceptance_critera.count()} rows where Party is 1 & ftpaRespondentDecisionDate is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentDecisionDate", "PASS", "ftpaRespondentDecisionDate pass: field is correctly omitted for Appellant applications", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentDecisionDate", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaFinalDecisionForDisplay - Mapping Outcome to String Status
# (MAX StatusId)
#######################
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window

def test_ftpaFinalDecisionForDisplay_test1(test_df):
    try:
        # 1. Identify the MAX StatusID record per appeal across the whole dataset
        # (Requirement specifies MAX StatusId without CaseStatus filter for the selection)
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = test_df.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 2. Define the expected mapping logic for the display field
        expected_val = when(col("Outcome") == 30, "granted") \
                      .when(col("Outcome") == 31, "refused") \
                      .when(col("Outcome") == 14, "notAdmitted") \
                      .otherwise(None) # Or a default value if your business logic requires one

        # 3. Acceptance Criteria: Validate actual vs expected for the three target outcomes
        # We filter the winning records for the outcomes we actually want to test
        target_outcomes = [30, 31, 14]
        failures = winning_records.filter(col("Outcome").isin(target_outcomes)).filter(
            col("ftpaFinalDecisionForDisplay") != expected_val
        )

        if failures.count() != 0:
            return TestResult("ftpaFinalDecisionForDisplay", "FAIL", f"ftpaFinalDecisionForDisplay mapping failed: found {failures.count()} rows with incorrect display strings", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaFinalDecisionForDisplay", "PASS", "ftpaFinalDecisionForDisplay mapping pass: Outcomes 30, 31, and 14 correctly mapped", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaFinalDecisionForDisplay", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaAppellantRjDecisionOutcomeType - Mapping Outcome to String Status
# (MAX StatusId WHERE CaseStatus = 39 AND Outcome IN (30, 31, 14))
#######################
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window

def test_ftpaAppellantRjDecisionOutcomeType_test1(test_df):
    try:
        # 1. Filter for the Decided State and specific Outcomes
        outcome_list = [30, 31, 14]
        target_records = test_df.filter(
            (col("CaseStatus") == 39) & 
            (col("Outcome").isin(outcome_list))
        ) 
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantRjDecisionOutcomeType", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 with Outcome 30, 31, or 14 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Define the expected mapping logic
        expected_val = when(col("Outcome") == 30, "granted") \
                      .when(col("Outcome") == 31, "refused") \
                      .when(col("Outcome") == 14, "notAdmitted") \
                      .otherwise("unknown")

        # 4. Acceptance Criteria: Validate actual ftpaAppellantRjDecisionOutcomeType against the map
        # We check only the rows where Outcome is one of the three target values
        acceptance_critera = winning_records.filter(
            col("ftpaAppellantRjDecisionOutcomeType") != expected_val
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantRjDecisionOutcomeType", "FAIL", f"ftpaAppellantRjDecisionOutcomeType mapping failed: found {acceptance_critera.count()} rows with incorrect status strings", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantRjDecisionOutcomeType", "PASS", "ftpaAppellantRjDecisionOutcomeType mapping pass: all Outcomes (30, 31, 14) correctly mapped", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantRjDecisionOutcomeType", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaRespondentRjDecisionOutcomeType - Mapping Outcome to String Status
# (MAX StatusId WHERE CaseStatus = 39 AND Outcome IN (30, 31, 14))
#######################
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window

def test_ftpaRespondentRjDecisionOutcomeType_test1(test_df):
    try:
        # 1. Filter for the Decided State and specific Outcomes
        outcome_list = [30, 31, 14]
        target_records = test_df.filter(
            (col("CaseStatus") == 39) & 
            (col("Outcome").isin(outcome_list))
        ) 
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentRjDecisionOutcomeType", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 with Outcome 30, 31, or 14 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Define the expected mapping logic
        expected_val = when(col("Outcome") == 30, "granted") \
                      .when(col("Outcome") == 31, "refused") \
                      .when(col("Outcome") == 14, "notAdmitted") \
                      .otherwise("unknown")

        # 4. Acceptance Criteria: Validate actual ftpaRespondentRjDecisionOutcomeType against the map
        acceptance_critera = winning_records.filter(
            col("ftpaRespondentRjDecisionOutcomeType") != expected_val
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentRjDecisionOutcomeType", "FAIL", f"ftpaRespondentRjDecisionOutcomeType mapping failed: found {acceptance_critera.count()} rows with incorrect status strings", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentRjDecisionOutcomeType", "PASS", "ftpaRespondentRjDecisionOutcomeType mapping pass: all Outcomes (30, 31, 14) correctly mapped", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentRjDecisionOutcomeType", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantNoticeOfDecisionSetAside - IF M3.Party IS 1 = "No" (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_isFtpaAppellantNoticeOfDecisionSetAside_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantNoticeOfDecisionSetAside", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaAppellantNoticeOfDecisionSetAside") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantNoticeOfDecisionSetAside", "FAIL", f"isFtpaAppellantNoticeOfDecisionSetAside failed: found {acceptance_critera.count()} rows where Party is 1 & value is not 'No'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantNoticeOfDecisionSetAside", "PASS", "isFtpaAppellantNoticeOfDecisionSetAside pass: all Appellant records are set to 'No'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantNoticeOfDecisionSetAside", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# isFtpaAppellantNoticeOfDecisionSetAside - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantNoticeOfDecisionSetAside_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantNoticeOfDecisionSetAside", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Check that field is Omitted (Null) for Party 2
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaAppellantNoticeOfDecisionSetAside").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantNoticeOfDecisionSetAside", "FAIL", f"isFtpaAppellantNoticeOfDecisionSetAside failed: found {acceptance_critera.count()} rows where Party is 2 & field is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantNoticeOfDecisionSetAside", "PASS", "isFtpaAppellantNoticeOfDecisionSetAside pass: field is correctly omitted for Respondent applications", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantNoticeOfDecisionSetAside", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentNoticeOfDecisionSetAside - IF M3.Party IS 2 = "No" (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_isFtpaRespondentNoticeOfDecisionSetAside_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentNoticeOfDecisionSetAside", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaRespondentNoticeOfDecisionSetAside") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentNoticeOfDecisionSetAside", "FAIL", f"isFtpaRespondentNoticeOfDecisionSetAside failed: found {acceptance_critera.count()} rows where Party is 2 & value is not 'No'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentNoticeOfDecisionSetAside", "PASS", "isFtpaRespondentNoticeOfDecisionSetAside pass: all Respondent records are set to 'No'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentNoticeOfDecisionSetAside", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentNoticeOfDecisionSetAside - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentNoticeOfDecisionSetAside_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentNoticeOfDecisionSetAside", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Check that field is Omitted (Null) for Party 1
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaRespondentNoticeOfDecisionSetAside").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentNoticeOfDecisionSetAside", "FAIL", f"isFtpaRespondentNoticeOfDecisionSetAside failed: found {acceptance_critera.count()} rows where Party is 1 & field is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentNoticeOfDecisionSetAside", "PASS", "isFtpaRespondentNoticeOfDecisionSetAside pass: field is correctly omitted for Appellant applications", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentNoticeOfDecisionSetAside", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)



#######################
# isAppellantFtpaDecisionVisibleToAll - IF M3.Party IS 1 = "Yes" (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_isAppellantFtpaDecisionVisibleToAll_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isAppellantFtpaDecisionVisibleToAll", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isAppellantFtpaDecisionVisibleToAll") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isAppellantFtpaDecisionVisibleToAll", "FAIL", f"isAppellantFtpaDecisionVisibleToAll failed: found {acceptance_critera.count()} rows where Party is 1 & value is not 'Yes'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isAppellantFtpaDecisionVisibleToAll", "PASS", "isAppellantFtpaDecisionVisibleToAll pass: all Appellant records are set to 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isAppellantFtpaDecisionVisibleToAll", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isAppellantFtpaDecisionVisibleToAll - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isAppellantFtpaDecisionVisibleToAll_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isAppellantFtpaDecisionVisibleToAll", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Check that field is Omitted (Null) for Party 2
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isAppellantFtpaDecisionVisibleToAll")!= "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isAppellantFtpaDecisionVisibleToAll", "FAIL", f"isAppellantFtpaDecisionVisibleToAll failed: found {acceptance_critera.count()} rows where Party is 2 & field is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isAppellantFtpaDecisionVisibleToAll", "PASS", "isAppellantFtpaDecisionVisibleToAll pass: field is correctly omitted for Respondent applications", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isAppellantFtpaDecisionVisibleToAll", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# isRespondentFtpaDecisionVisibleToAll - IF M3.Party IS 2 = "Yes" (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_isRespondentFtpaDecisionVisibleToAll_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isRespondentFtpaDecisionVisibleToAll", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isRespondentFtpaDecisionVisibleToAll") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isRespondentFtpaDecisionVisibleToAll", "FAIL", f"isRespondentFtpaDecisionVisibleToAll failed: found {acceptance_critera.count()} rows where Party is 2 & value is not 'Yes'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isRespondentFtpaDecisionVisibleToAll", "PASS", "isRespondentFtpaDecisionVisibleToAll pass: all Respondent records are set to 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isRespondentFtpaDecisionVisibleToAll", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# isRespondentFtpaDecisionVisibleToAll - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isRespondentFtpaDecisionVisibleToAll_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isRespondentFtpaDecisionVisibleToAll", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Check that field is Omitted (Null) for Party 1
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isRespondentFtpaDecisionVisibleToAll")!= "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isRespondentFtpaDecisionVisibleToAll", "FAIL", f"isRespondentFtpaDecisionVisibleToAll failed: found {acceptance_critera.count()} rows where Party is 1 & field is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isRespondentFtpaDecisionVisibleToAll", "PASS", "isRespondentFtpaDecisionVisibleToAll pass: field is correctly omitted for Appellant applications", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isRespondentFtpaDecisionVisibleToAll", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantDecided - Expected: "Yes" (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_isFtpaAppellantDecided_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Check if the field is exactly "Yes"
        acceptance_critera = winning_records.filter(
            col("isFtpaAppellantDecided") != "Yes"
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantDecided", "FAIL", f"isFtpaAppellantDecided failed: found {acceptance_critera.count()} rows where value is not 'Yes'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantDecided", "PASS", "isFtpaAppellantDecided pass: all latest status 39 records are set to 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# isFtpaRespondentDecided - Expected: "Yes" (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_isFtpaRespondentDecided_test1(test_df):
    try:
        # 1. Filter only for the Appeal State (CaseStatus 39)
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Check if the field is exactly "Yes"
        # This confirms the Respondent view is correctly flagged as Decided
        acceptance_critera = winning_records.filter(
            col("isFtpaRespondentDecided") != "Yes"
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentDecided", "FAIL", f"isFtpaRespondentDecided failed: found {acceptance_critera.count()} rows where value is not 'Yes'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentDecided", "PASS", "isFtpaRespondentDecided pass: all latest status 39 records are set to 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# secondFtpaDecisionExists - SCENARIO 1: CaseStatus = 46 & Outcome = 31 -> "Yes"
#######################
def test_secondFtpaDecisionExists_test1(test_df):
    try:
        # 1. Identify MAX StatusID record
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = test_df.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 2. Filter for the specific scenario criteria
        scenario_df = winning_records.filter((col("CaseStatus") == 46) & (col("Outcome") == 31))
        
        if scenario_df.count() == 0:
            return TestResult("secondFtpaDecisionExists", "PASS", "Scenario 1: No records found with Status 46 & Outcome 31 to test.", test_from_state, inspect.stack()[0].function)

        # 3. Acceptance Criteria
        failures = scenario_df.filter(col("secondFtpaDecisionExists") != "Yes")

        if failures.count() != 0:
            return TestResult("secondFtpaDecisionExists", "FAIL", f"Scenario 1 Failed: {failures.count()} rows found where Status 46/Outcome 31 was NOT 'Yes'", test_from_state, inspect.stack()[0].function)
        return TestResult("secondFtpaDecisionExists", "PASS", "Scenario 1 Passed: Status 46 & Outcome 31 correctly mapped to 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("secondFtpaDecisionExists", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    



#######################
# secondFtpaDecisionExists - SCENARIO 2: CaseStatus != 46 & Outcome = 31 -> "No"
#######################
def test_secondFtpaDecisionExists_test2(test_df):
    try:
        winning_records = test_df.withColumn("row_rank", row_number().over(Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc()))).filter(col("row_rank") == 1)

        # 2. Filter: Outcome is 31 but Status is NOT 46
        scenario_df = winning_records.filter((col("CaseStatus") != 46) & (col("Outcome") == 31))
        
        # 3. Acceptance Criteria
        failures = scenario_df.filter(col("secondFtpaDecisionExists") != "No")

        if failures.count() != 0:
            return TestResult("secondFtpaDecisionExists", "FAIL", f"Scenario 2 Failed: Outcome 31 with Status != 46 should be 'No'", test_from_state, inspect.stack()[0].function)
        return TestResult("secondFtpaDecisionExists", "PASS", "Scenario 2 Passed: Correctly returned 'No' for Outcome 31 outside Status 46", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("secondFtpaDecisionExists", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# secondFtpaDecisionExists - SCENARIO 3: CaseStatus = 46 & Outcome != 31 -> "No"
#######################
def test_secondFtpaDecisionExists_test3(test_df):
    try:
        winning_records = test_df.withColumn("row_rank", row_number().over(Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc()))).filter(col("row_rank") == 1)

        # 2. Filter: Status is 46 but Outcome is NOT 31
        scenario_df = winning_records.filter((col("CaseStatus") == 46) & (col("Outcome") != 31))
        
        # 3. Acceptance Criteria
        failures = scenario_df.filter(col("secondFtpaDecisionExists") != "No")

        if failures.count() != 0:
            return TestResult("secondFtpaDecisionExists", "FAIL", f"Scenario 3 Failed: Status 46 with Outcome != 31 should be 'No'", test_from_state, inspect.stack()[0].function)
        return TestResult("secondFtpaDecisionExists", "PASS", "Scenario 3 Passed: Correctly returned 'No' for Status 46 with different outcome", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("secondFtpaDecisionExists", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# secondFtpaDecisionExists - SCENARIO 4: CaseStatus != 46 & Outcome != 31 -> "No"
#######################
def test_secondFtpaDecisionExists_test4(test_df):
    try:
        winning_records = test_df.withColumn("row_rank", row_number().over(Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc()))).filter(col("row_rank") == 1)

        # 2. Filter: Neither criteria met
        scenario_df = winning_records.filter((col("CaseStatus") != 46) & (col("Outcome") != 31))
        
        # 3. Acceptance Criteria
        failures = scenario_df.filter(col("secondFtpaDecisionExists") != "No")

        if failures.count() != 0:
            return TestResult("secondFtpaDecisionExists", "FAIL", f"Scenario 4 Failed: General records should be 'No'", test_from_state, inspect.stack()[0].function)
        return TestResult("secondFtpaDecisionExists", "PASS", "Scenario 4 Passed: Correctly returned 'No' for general case records", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("secondFtpaDecisionExists", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# allFtpaAppellantDecisionDocs - IF M3.Party IS 1 = Include []
#######################
def test_allFtpaAppellantDecisionDocs_test1(test_df):
    try:
        # 1. MUST use Window logic to find the latest record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        latest_df = test_df.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)
        
        # 2. Filter for Party 1
        party1_records = latest_df.filter(col("Party") == 1)
        
        # 3. Acceptance Criteria: size 0 array (NOT NULL)
        failures = party1_records.filter(
            (col("allFtpaAppellantDecisionDocs").isNull()) | 
            (size(col("allFtpaAppellantDecisionDocs")) != 0)
        )

        if failures.count() != 0:
            return TestResult("allFtpaAppellantDecisionDocs", "FAIL", f"Found {failures.count()} Party 1 LATEST records that are NULL instead of []", "Docs", inspect.stack()[0].function)
        
        return TestResult("allFtpaAppellantDecisionDocs", "PASS", "All Party 1 latest records include []", "Docs", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("allFtpaAppellantDecisionDocs", "FAIL", f"EXCEPTION: {str(e)[:300]}", "Docs", inspect.stack()[0].function)


#######################
# allFtpaAppellantDecisionDocs - IF M3.Party IS 2 = OMIT
#######################
def test_allFtpaAppellantDecisionDocs_test2(test_df):
    try:
        # 1. MUST use Window logic to find the latest record
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        latest_df = test_df.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)
        
        # 2. Filter for Party 2
        party2_records = latest_df.filter(col("Party") == 2)
        
        # 3. Acceptance Criteria: MUST BE NULL
        failures = party2_records.filter(col("allFtpaAppellantDecisionDocs").isNotNull())

        if failures.count() != 0:
            return TestResult("allFtpaAppellantDecisionDocs", "FAIL", f"Found {failures.count()} Party 2 LATEST records where field was not Omitted", "Docs", inspect.stack()[0].function)
        
        return TestResult("allFtpaAppellantDecisionDocs", "PASS", "All Party 2 latest records correctly Omitted", "Docs", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("allFtpaAppellantDecisionDocs", "FAIL", f"EXCEPTION: {str(e)[:300]}", "Docs", inspect.stack()[0].function)


#######################
# allFtpaRespondentDecisionDocs - IF M3.Party IS 2 = Include []
#######################
from pyspark.sql.functions import size, col, row_number
from pyspark.sql.window import Window

def test_allFtpaRespondentDecisionDocs_test1(test_df):
    try:
        # 1. Apply Window logic to get latest record
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        latest_df = test_df.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)
        
        # 2. Filter for Party 2
        party2_records = latest_df.filter(col("Party") == 2)
        
        if party2_records.count() == 0:
            return TestResult("allFtpaRespondentDecisionDocs", "PASS", "No records found for M3.Party = 2 to test.", test_from_state, inspect.stack()[0].function)

        # 3. Acceptance Criteria: Field must be an empty array (size 0)
        failures = party2_records.filter(
            (col("allFtpaRespondentDecisionDocs").isNull()) | 
            (size(col("allFtpaRespondentDecisionDocs")) != 0)
        )

        if failures.count() != 0:
            return TestResult("allFtpaRespondentDecisionDocs", "FAIL", f"Found {failures.count()} rows where Party is 2 but allFtpaRespondentDecisionDocs is not an empty array", test_from_state, inspect.stack()[0].function)
        
        return TestResult("allFtpaRespondentDecisionDocs", "PASS", "All Party 2 records correctly include an empty array for allFtpaRespondentDecisionDocs", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("allFtpaRespondentDecisionDocs", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# allFtpaRespondentDecisionDocs - IF M3.Party IS 1 = OMIT
#######################
def test_allFtpaRespondentDecisionDocs_test2(test_df):
    try:
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        latest_df = test_df.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)
        
        party1_records = latest_df.filter(col("Party") == 1)
        
        if party1_records.count() == 0:
            return TestResult("allFtpaRespondentDecisionDocs", "PASS", "No records found for M3.Party = 1 to test.", test_from_state, inspect.stack()[0].function)

        failures = party1_records.filter(col("allFtpaRespondentDecisionDocs").isNotNull())

        if failures.count() != 0:
            return TestResult("allFtpaRespondentDecisionDocs", "FAIL", f"Found {failures.count()} rows where Party is 1 but allFtpaRespondentDecisionDocs was not omitted", test_from_state, inspect.stack()[0].function)
        
        return TestResult("allFtpaRespondentDecisionDocs", "PASS", "All Party 1 records correctly omit allFtpaRespondentDecisionDocs", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("allFtpaRespondentDecisionDocs", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


#######################
# ftpaAppellantNoticeDocument - IF M3.Party IS 1 = Include []
#######################
def test_ftpaAppellantNoticeDocument_test1(test_df):
    try:
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        latest_df = test_df.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)
        
        party1_records = latest_df.filter(col("Party") == 1)
        
        if party1_records.count() == 0:
            return TestResult("ftpaAppellantNoticeDocument", "PASS", "No records found for M3.Party = 1 to test.", test_from_state, inspect.stack()[0].function)

        failures = party1_records.filter(
            (col("ftpaAppellantNoticeDocument").isNull()) | 
            (size(col("ftpaAppellantNoticeDocument")) != 0)
        )

        if failures.count() != 0:
            return TestResult("ftpaAppellantNoticeDocument", "FAIL", f"Found {failures.count()} rows where Party is 1 but ftpaAppellantNoticeDocument is not an empty array", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaAppellantNoticeDocument", "PASS", "All Party 1 records correctly include an empty array for ftpaAppellantNoticeDocument", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantNoticeDocument", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantNoticeDocument - IF M3.Party IS 2 = OMIT
#######################
def test_ftpaAppellantNoticeDocument_test2(test_df):
    try:
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        latest_df = test_df.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)
        
        party2_records = latest_df.filter(col("Party") == 2)
        
        if party2_records.count() == 0:
            return TestResult("ftpaAppellantNoticeDocument", "PASS", "No records found for M3.Party = 2 to test.", test_from_state, inspect.stack()[0].function)

        failures = party2_records.filter(col("ftpaAppellantNoticeDocument").isNotNull())

        if failures.count() != 0:
            return TestResult("ftpaAppellantNoticeDocument", "FAIL", f"Found {failures.count()} rows where Party is 2 but ftpaAppellantNoticeDocument was not omitted", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaAppellantNoticeDocument", "PASS", "All Party 2 records correctly omit ftpaAppellantNoticeDocument", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantNoticeDocument", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# ftpaRespondentNoticeDocument - IF M3.Party IS 2 = Include []
#######################
def test_ftpaRespondentNoticeDocument_test1(test_df):
    try:
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        latest_df = test_df.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)
        
        party2_records = latest_df.filter(col("Party") == 2)
        
        if party2_records.count() == 0:
            return TestResult("ftpaRespondentNoticeDocument", "PASS", "No records found for M3.Party = 2 to test.", test_from_state, inspect.stack()[0].function)

        failures = party2_records.filter(
            (col("ftpaRespondentNoticeDocument").isNull()) | 
            (size(col("ftpaRespondentNoticeDocument")) != 0)
        )

        if failures.count() != 0:
            return TestResult("ftpaRespondentNoticeDocument", "FAIL", f"Found {failures.count()} rows where Party is 2 but ftpaRespondentNoticeDocument is not an empty array", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaRespondentNoticeDocument", "PASS", "All Party 2 records correctly include an empty array for ftpaRespondentNoticeDocument", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentNoticeDocument", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentNoticeDocument - IF M3.Party IS 1 = OMIT
#######################
def test_ftpaRespondentNoticeDocument_test2(test_df):
    try:
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        latest_df = test_df.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)
        
        party1_records = latest_df.filter(col("Party") == 1)
        
        if party1_records.count() == 0:
            return TestResult("ftpaRespondentNoticeDocument", "PASS", "No records found for M3.Party = 1 to test.", test_from_state, inspect.stack()[0].function)

        failures = party1_records.filter(col("ftpaRespondentNoticeDocument").isNotNull())

        if failures.count() != 0:
            return TestResult("ftpaRespondentNoticeDocument", "FAIL", f"Found {failures.count()} rows where Party is 1 but ftpaRespondentNoticeDocument was not omitted", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaRespondentNoticeDocument", "PASS", "All Party 1 records correctly omit ftpaRespondentNoticeDocument", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentNoticeDocument", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

def test_ftpaList_test1(test_df):
    try:
        # 1. Get latest record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        latest_df = test_df.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 2. Filter for Party 1
        party1_records = latest_df.filter(col("Party") == 1)

        # 3. Acceptance Criteria
        # First, ensure ftpaList is actually an array with at least one element
        failures = party1_records.filter(
            (size(col("ftpaList")) == 0) |
            (col("ftpaList")[0]["value"]["ftpaApplicant"] != "appellant") |
            (col("ftpaList")[0]["value"]["ftpaDecisionDate"] != col("ftpaAppellantDecisionDate")) |
            (col("ftpaList")[0]["value"]["ftpaApplicationDate"] != col("ftpaAppellantApplicationDate")) |
            (col("ftpaList")[0]["value"]["ftpaDecisionOutcomeType"] != col("ftpaFinalDecisionForDisplay"))
            # ... add other checks here ...
        )

        if failures.count() != 0:
            return TestResult("ftpaList", "FAIL", f"Found {failures.count()} Appellant rows with incorrect ftpaList values", "List", inspect.stack()[0].function)
        
        return TestResult("ftpaList", "PASS", "Appellant ftpaList values are correct", "List", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaList", "FAIL", f"Exception: {str(e)[:300]}", "List", inspect.stack()[0].function)



#######################
# ftpaList - IF M3.Party IS 2 (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaList_test2(test_df):
    try:
        # 1. Filter and identify winning records
        target_records = test_df.filter(col("CaseStatus") == 39)
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 2. Acceptance Criteria for Respondent (Party 2)
        # Note the respondent-specific column mappings
        failures = winning_records.filter(col("Party") == 2).filter(
            (col("ftpaList")[0]["value"]["ftpaApplicant"] != "respondent") |
            (col("ftpaList")[0]["value"]["ftpaDecisionDate"] != col("ftpaRespondentDecisionDate")) |
            (col("ftpaList")[0]["value"]["ftpaApplicationDate"] != col("ftpaRespondentApplicationDate")) |
            (size(col("ftpaList")[0]["value"]["ftpaGroundsDocuments"]) != 0) |
            (size(col("ftpaList")[0]["value"]["ftpaEvidenceDocuments"]) != 0) |
            (size(col("ftpaList")[0]["value"]["ftpaOutOfTimeDocuments"]) != 0) |
            (col("ftpaList")[0]["value"]["ftpaDecisionOutcomeType"] != col("ftpaFinalDecisionForDisplay")) |
            (col("ftpaList")[0]["value"]["ftpaOutOfTimeExplanation"] != col("ftpaRespondentOutOfTimeExplanation")) |
            (col("ftpaList")[0]["value"]["isFtpaNoticeOfDecisionSetAside"] != col("isFtpaRespondentNoticeOfDecisionSetAside"))
        )

        if failures.count() != 0:
            return TestResult("ftpaList", "FAIL", f"Found {failures.count()} Respondent rows with incorrect ftpaList decided values", test_from_state, inspect.stack()[0].function)
        return TestResult("ftpaList", "PASS", "Respondent ftpaList decided values are correct", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaList", "FAIL", f"Exception: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
