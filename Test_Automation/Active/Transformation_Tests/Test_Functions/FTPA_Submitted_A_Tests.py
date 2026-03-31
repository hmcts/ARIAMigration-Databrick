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
test_from_state = "ftpaSubmitted(a)"



############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "isFtpaListVisible"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

def test_ftpa_a_defaultValues(test_df, fields_to_exclude):
    try:
        expected_defaults = {
            "isFtpaListVisible": "Yes"

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
            "ftpaAppellantApplicationDate",
            "ftpaAppellantSubmissionOutOfTime",
            "ftpaAppellantOutOfTimeExplanation",
            "ftpaRespondentApplicationDate",
            "ftpaRespondentSubmissionOutOfTime",
            "ftpaRespondentOutOfTimeExplanation",
            "ftpaAppellantSubmitted",
            "isFtpaAppellantDocsVisibleInDecided",
            "isFtpaAppellantDocsVisibleInSubmitted",
            "isFtpaAppellantOotDocsVisibleInDecided",
            "isFtpaAppellantOotDocsVisibleInSubmitted",
            "isFtpaAppellantGroundsDocsVisibleInDecided",
            "isFtpaAppellantEvidenceDocsVisibleInDecided",
            "isFtpaAppellantGroundsDocsVisibleInSubmitted",
            "isFtpaAppellantEvidenceDocsVisibleInSubmitted",
            "isFtpaAppellantOotExplanationVisibleInDecided",
            "isFtpaAppellantOotExplanationVisibleInSubmitted",
            "ftpaRespondentSubmitted",
            "isFtpaRespondentDocsVisibleInDecided",
            "isFtpaRespondentDocsVisibleInSubmitted",
            "isFtpaRespondentOotDocsVisibleInDecided",
            "isFtpaRespondentOotDocsVisibleInSubmitted",
            "isFtpaRespondentGroundsDocsVisibleInDecided",
            "isFtpaRespondentEvidenceDocsVisibleInDecided",
            "isFtpaRespondentGroundsDocsVisibleInSubmitted",
            "isFtpaRespondentEvidenceDocsVisibleInSubmitted",
            "isFtpaRespondentOotExplanationVisibleInDecided",
            "isFtpaRespondentOotExplanationVisibleInSubmitted",
            "ftpaAppellantDocuments",
            "ftpaRespondentDocuments",
            "ftpaAppellantGroundsDocuments",
            "ftpaRespondentGroundsDocuments",
            "ftpaAppellantEvidenceDocuments",
            "ftpaRespondentEvidenceDocuments",
            "ftpaAppellantOutOfTimeDocuments",
            "ftpaRespondentOutOfTimeDocuments",
            "ftpaList"

        )

        M1_bronze = M1_bronze.select(
            "CaseNo"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "CaseStatus",
            "StatusId",
            "Party",
            "OutOfTime"
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
# ftpaList - IF M3.Party IS 1 (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaList_appellant(test_df):
    try:
        # 1. Filter and get the latest record
        target_records = test_df.filter(col("CaseStatus") == 39)
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 2. Acceptance Criteria for Appellant
        # We check the first element of the list [0]
        failures = winning_records.filter(col("Party") == 1).filter(
            (col("ftpaList")[0]["value"]["ftpaApplicant"] != "appellant") |
            (col("ftpaList")[0]["value"]["ftpaApplicationDate"] != col("ftpaAppellantApplicationDate")) |
            (size(col("ftpaList")[0]["value"]["ftpaGroundsDocuments"]) != 0) |
            (size(col("ftpaList")[0]["value"]["ftpaEvidenceDocuments"]) != 0) |
            (size(col("ftpaList")[0]["value"]["ftpaOutOfTimeDocuments"]) != 0) |
            (col("ftpaList")[0]["value"]["ftpaOutOfTimeExplanation"] != col("ftpaAppellantOutOfTimeExplanation"))
        )

        if failures.count() != 0:
            return TestResult("ftpaList", "FAIL", f"Found {failures.count()} appellant rows with incorrect ftpaList values", test_from_state, inspect.stack()[0].function)
        return TestResult("ftpaList", "PASS", "Appellant ftpaList values are correct", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaList", "FAIL", f"Exception: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaList - IF M3.Party IS 2 (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaList_respondent(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39)
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 2. Acceptance Criteria for Respondent
        failures = winning_records.filter(col("Party") == 2).filter(
            (col("ftpaList")[0]["value"]["ftpaApplicant"] != "respondent") |
            (col("ftpaList")[0]["value"]["ftpaApplicationDate"] != col("ftpaAppellantApplicationDate")) |
            (size(col("ftpaList")[0]["value"]["ftpaGroundsDocuments"]) != 0) |
            (size(col("ftpaList")[0]["value"]["ftpaEvidenceDocuments"]) != 0) |
            (size(col("ftpaList")[0]["value"]["ftpaOutOfTimeDocuments"]) != 0) |
            (col("ftpaList")[0]["value"]["ftpaOutOfTimeExplanation"] != col("ftpaAppellantOutOfTimeExplanation"))
        )

        if failures.count() != 0:
            return TestResult("ftpaList", "FAIL", f"Found {failures.count()} respondent rows with incorrect ftpaList values", test_from_state, inspect.stack()[0].function)
        return TestResult("ftpaList", "PASS", "Respondent ftpaList values are correct", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaList", "FAIL", f"Exception: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    
    

#######################
# ftpaAppellantApplicationDate - IF M3.Party IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantApplicationDate_test1(test_df):
    try:
        # 1. Filter for the specific CaseStatus and Party required
        target_records = test_df.filter((col("CaseStatus") == 39))

        if target_records.count() == 0:
            return TestResult("ftpaAppellantApplicationDate", "FAIL", "NO RECORDS TO TEST (No M3.Party 1 CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party 1 date is missing (NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) &
            (col("ftpaAppellantApplicationDate").isNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantApplicationDate", "FAIL", f"ftpaAppellantApplicationDate acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaAppellantApplicationDate is not included", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantApplicationDate", "PASS", "ftpaAppellantApplicationDate acceptance criteria pass: all rows where M3.Party is 1 have ftpaAppellantApplicationDate included", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("ftpaAppellantApplicationDate", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    
#######################
# ftpaAppellantApplicationDate - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantApplicationDate_test2(test_df):
    try:
        # 1. Filter for the specific CaseStatus and Party required
        target_records = test_df.filter((col("CaseStatus") == 39))

        if target_records.count() == 0:
            return TestResult("ftpaAppellantApplicationDate", "FAIL", "NO RECORDS TO TEST (No M3.Party 2 CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party 2 date is NOT missing (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) &
            (col("ftpaAppellantApplicationDate").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantApplicationDate", "FAIL", f"ftpaAppellantApplicationDate acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party = 2 & ftpaAppellantApplicationDate is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantApplicationDate", "PASS", "ftpaAppellantApplicationDate acceptance criteria pass: all rows where M3.Party = 2 have ftpaAppellantApplicationDate omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("ftpaAppellantApplicationDate", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    
#######################
# ftpaAppellantSubmissionOutOfTime - IF M3.OutOfTime IS 1 AND M3.Party IS 1 = Yes (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantSubmissionOutOfTime_test1(test_df):
    try:
        # 1. Filter for the specific CaseStatus, OutOfTime, and Party required
        target_records = test_df.filter((col("CaseStatus") == 39)) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", "NO RECORDS TO TEST (No M3.OutOfTime 1 & Party 1 CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("ftpaAppellantSubmissionOutOfTime") != "Yes") &
            (col("OutOfTime") == 1) &
            (col("Party") == 1))


        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"ftpaAppellantSubmissionOutOfTime acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is 1 and M3.Party is 1 & ftpaAppellantSubmissionOutOfTime is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "PASS", "ftpaAppellantSubmissionOutOfTime acceptance criteria pass: all rows where M3.OutOfTime is 1 and M3.Party is 1 have ftpaAppellantSubmissionOutOfTime equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantSubmissionOutOfTime - IF M3.OutOfTime IS NOT 1 AND M3.Party IS 1 = No (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantSubmissionOutOfTime_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is NOT "No"
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") != 1) &
            (col("Party") == 1) &
            (col("ftpaAppellantSubmissionOutOfTime") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"ftpaAppellantSubmissionOutOfTime acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is not 1 and M3.Party is 1 & ftpaAppellantSubmissionOutOfTime is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "PASS", "ftpaAppellantSubmissionOutOfTime acceptance criteria pass: all rows where M3.OutOfTime is not 1 and M3.Party is 1 have ftpaAppellantSubmissionOutOfTime equals No", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantSubmissionOutOfTime - IF M3.OutOfTime IS NOT 1 AND M3.Party IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantSubmissionOutOfTime_test3(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") != 1) &
            (col("Party") != 1) &
            (col("ftpaAppellantSubmissionOutOfTime").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"ftpaAppellantSubmissionOutOfTime acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is not 1 and M3.Party is not 1 & ftpaAppellantSubmissionOutOfTime is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "PASS", "ftpaAppellantSubmissionOutOfTime acceptance criteria pass: all rows where M3.OutOfTime is not 1 and M3.Party is not 1 have ftpaAppellantSubmissionOutOfTime omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantSubmissionOutOfTime - IF M3.OutOfTime IS 1 AND M3.Party IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantSubmissionOutOfTime_test4(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") == 1) &
            (col("Party") != 1) &
            (col("ftpaAppellantSubmissionOutOfTime").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"ftpaAppellantSubmissionOutOfTime acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is 1 and M3.Party is not 1 & ftpaAppellantSubmissionOutOfTime is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "PASS", "ftpaAppellantSubmissionOutOfTime acceptance criteria pass: all rows where M3.OutOfTime is 1 and M3.Party is not 1 have ftpaAppellantSubmissionOutOfTime omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantOutOfTimeExplanation - IF M3.OutOfTime IS 1 AND M3.Party IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantOutOfTimeExplanation_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but string is incorrect
        expected_str = "This is a migrated ARIA case. Please refer to the documents."
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") == 1) &
            (col("Party") == 1) &
            (col("ftpaAppellantOutOfTimeExplanation") != expected_str)
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"ftpaAppellantOutOfTimeExplanation acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is 1 and M3.Party is 1 & ftpaAppellantOutOfTimeExplanation is not: {expected_str}", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", f"ftpaAppellantOutOfTimeExplanation acceptance criteria pass: all rows where M3.OutOfTime is 1 and M3.Party is 1 have correct migrated explanation string", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantOutOfTimeExplanation - IF M3.OutOfTime IS 1 AND M3.Party IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantOutOfTimeExplanation_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is not 1 but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") == 1) &
            (col("Party") != 1) &
            (col("ftpaAppellantOutOfTimeExplanation").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"ftpaAppellantOutOfTimeExplanation acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is 1 and M3.Party is not 1 & ftpaAppellantOutOfTimeExplanation is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "ftpaAppellantOutOfTimeExplanation acceptance criteria pass: all rows where M3.OutOfTime is 1 and M3.Party is not 1 have ftpaAppellantOutOfTimeExplanation omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantOutOfTimeExplanation - IF M3.OutOfTime IS NOT 1 AND M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantOutOfTimeExplanation_test3(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where OutOfTime is not 1 but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") != 1) &
            (col("Party") == 1) &
            (col("ftpaAppellantOutOfTimeExplanation").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"ftpaAppellantOutOfTimeExplanation acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is not 1 and M3.Party is 1 & ftpaAppellantOutOfTimeExplanation is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "ftpaAppellantOutOfTimeExplanation acceptance criteria pass: all rows where M3.OutOfTime is not 1 and M3.Party is 1 have ftpaAppellantOutOfTimeExplanation omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantOutOfTimeExplanation - IF M3.OutOfTime IS NOT 1 AND M3.Party IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantOutOfTimeExplanation_test4(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where neither condition is met but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") != 1) &
            (col("Party") != 1) &
            (col("ftpaAppellantOutOfTimeExplanation").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"ftpaAppellantOutOfTimeExplanation acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is not 1 and M3.Party is not 1 & ftpaAppellantOutOfTimeExplanation is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "ftpaAppellantOutOfTimeExplanation acceptance criteria pass: all rows where M3.OutOfTime is not 1 and M3.Party is not 1 have ftpaAppellantOutOfTimeExplanation omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentApplicationDate - IF M3.Party IS 2 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentApplicationDate_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentApplicationDate", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but date is missing (NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaRespondentApplicationDate").isNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentApplicationDate", "FAIL", f"ftpaRespondentApplicationDate acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & ftpaRespondentApplicationDate is not included", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentApplicationDate", "PASS", "ftpaRespondentApplicationDate acceptance criteria pass: all rows where M3.Party is 2 have ftpaRespondentApplicationDate included", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("ftpaRespondentApplicationDate", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentApplicationDate - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentApplicationDate_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentApplicationDate", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaRespondentApplicationDate").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentApplicationDate", "FAIL", f"ftpaRespondentApplicationDate acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaRespondentApplicationDate is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentApplicationDate", "PASS", "ftpaRespondentApplicationDate acceptance criteria pass: all rows where M3.Party is 1 have ftpaRespondentApplicationDate omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("ftpaRespondentApplicationDate", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentSubmissionOutOfTime - IF M3.OutOfTime IS 1 AND M3.Party IS 2 = Yes (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentSubmissionOutOfTime_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") == 1) &
            (col("Party") == 2) &
            (col("ftpaRespondentSubmissionOutOfTime") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", f"ftpaRespondentSubmissionOutOfTime acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is 1 and M3.Party is 2 & ftpaRespondentSubmissionOutOfTime is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "PASS", "ftpaRespondentSubmissionOutOfTime acceptance criteria pass: all rows where M3.OutOfTime is 1 and M3.Party is 2 have ftpaRespondentSubmissionOutOfTime equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentSubmissionOutOfTime - IF M3.OutOfTime IS NOT 1 AND M3.Party IS 2 = No (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentSubmissionOutOfTime_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") != 1) &
            (col("Party") == 2) &
            (col("ftpaRespondentSubmissionOutOfTime") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", f"ftpaRespondentSubmissionOutOfTime acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is not 1 and M3.Party is 2 & ftpaRespondentSubmissionOutOfTime is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "PASS", "ftpaRespondentSubmissionOutOfTime acceptance criteria pass: all rows where M3.OutOfTime is not 1 and M3.Party is 2 have ftpaRespondentSubmissionOutOfTime equals No", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentSubmissionOutOfTime - IF M3.OutOfTime IS 1 AND M3.Party IS NOT 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentSubmissionOutOfTime_test3(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but field is not omitted
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") == 1) &
            (col("Party") != 2) &
            (col("ftpaRespondentSubmissionOutOfTime").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", f"ftpaRespondentSubmissionOutOfTime acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is 1 and M3.Party is not 2 & ftpaRespondentSubmissionOutOfTime is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "PASS", "ftpaRespondentSubmissionOutOfTime acceptance criteria pass: all rows where M3.OutOfTime is 1 and M3.Party is not 2 have ftpaRespondentSubmissionOutOfTime omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentSubmissionOutOfTime - IF M3.OutOfTime IS NOT 1 AND M3.Party IS NOT 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentSubmissionOutOfTime_test4(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but field is not omitted
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") != 1) &
            (col("Party") != 2) &
            (col("ftpaRespondentSubmissionOutOfTime").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", f"ftpaRespondentSubmissionOutOfTime acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is not 1 and M3.Party is not 2 & ftpaRespondentSubmissionOutOfTime is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "PASS", "ftpaRespondentSubmissionOutOfTime acceptance criteria pass: all rows where M3.OutOfTime is not 1 and M3.Party is not 2 have ftpaRespondentSubmissionOutOfTime omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentOutOfTimeExplanation - IF M3.OutOfTime IS 1 AND M3.Party IS 2 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentOutOfTimeExplanation_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but string is incorrect
        expected_str = "This is a migrated ARIA case. Please refer to the documents."
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") == 1) &
            (col("Party") == 2) &
            (col("ftpaRespondentOutOfTimeExplanation") != expected_str)
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"ftpaRespondentOutOfTimeExplanation acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is 1 and M3.Party is 2 & ftpaRespondentOutOfTimeExplanation is not: {expected_str}", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "ftpaRespondentOutOfTimeExplanation acceptance criteria pass: all rows where M3.OutOfTime is 1 and M3.Party is 2 have correct migrated explanation string", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaRespondentOutOfTimeExplanation - IF M3.OutOfTime IS 1 AND M3.Party IS NOT 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentOutOfTimeExplanation_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is not 2 but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") == 1) &
            (col("Party") != 2) &
            (col("ftpaRespondentOutOfTimeExplanation").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"ftpaRespondentOutOfTimeExplanation acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is 1 and M3.Party is not 2 & ftpaRespondentOutOfTimeExplanation is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "ftpaRespondentOutOfTimeExplanation acceptance criteria pass: all rows where M3.OutOfTime is 1 and M3.Party is not 2 have ftpaRespondentOutOfTimeExplanation omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaRespondentOutOfTimeExplanation - IF M3.OutOfTime IS NOT 1 AND M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentOutOfTimeExplanation_test3(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where OutOfTime is not 1 but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") != 1) &
            (col("Party") == 2) &
            (col("ftpaRespondentOutOfTimeExplanation").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"ftpaRespondentOutOfTimeExplanation acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is not 1 and M3.Party is 2 & ftpaRespondentOutOfTimeExplanation is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "ftpaRespondentOutOfTimeExplanation acceptance criteria pass: all rows where M3.OutOfTime is not 1 and M3.Party is 2 have ftpaRespondentOutOfTimeExplanation omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentOutOfTimeExplanation - IF M3.OutOfTime IS NOT 1 AND M3.Party IS NOT 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentOutOfTimeExplanation_test4(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where neither condition is met but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("OutOfTime") != 1) &
            (col("Party") != 2) &
            (col("ftpaRespondentOutOfTimeExplanation").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"ftpaRespondentOutOfTimeExplanation acceptance criteria failed: found {acceptance_critera.count()} rows where M3.OutOfTime is not 1 and M3.Party is not 2 & ftpaRespondentOutOfTimeExplanation is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "ftpaRespondentOutOfTimeExplanation acceptance criteria pass: all rows where M3.OutOfTime is not 1 and M3.Party is not 2 have ftpaRespondentOutOfTimeExplanation omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantSubmitted - IF M3.Party IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantSubmitted_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaAppellantSubmitted") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantSubmitted", "FAIL", f"ftpaAppellantSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaAppellantSubmitted is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantSubmitted", "PASS", "ftpaAppellantSubmitted acceptance criteria pass: all rows where M3.Party is 1 have ftpaAppellantSubmitted equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("ftpaAppellantSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantSubmitted - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantSubmitted_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaAppellantSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantSubmitted", "FAIL", f"ftpaAppellantSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & ftpaAppellantSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantSubmitted", "PASS", "ftpaAppellantSubmitted acceptance criteria pass: all rows where M3.Party is 2 have ftpaAppellantSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("ftpaAppellantSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantDocsVisibleInDecided - IF M3.Party IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantDocsVisibleInDecided_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaAppellantDocsVisibleInDecided") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", f"isFtpaAppellantDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & isFtpaAppellantDocsVisibleInDecided is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "PASS", "isFtpaAppellantDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 have isFtpaAppellantDocsVisibleInDecided equals No", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantDocsVisibleInDecided - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantDocsVisibleInDecided_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaAppellantDocsVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", f"isFtpaAppellantDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & isFtpaAppellantDocsVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "PASS", "isFtpaAppellantDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 have isFtpaAppellantDocsVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantDocsVisibleInSubmitted - IF M3.Party IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantDocsVisibleInSubmitted_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaAppellantDocsVisibleInSubmitted") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", f"isFtpaAppellantDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & isFtpaAppellantDocsVisibleInSubmitted is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "PASS", "isFtpaAppellantDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 have isFtpaAppellantDocsVisibleInSubmitted equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantDocsVisibleInSubmitted - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantDocsVisibleInSubmitted_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaAppellantDocsVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", f"isFtpaAppellantDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & isFtpaAppellantDocsVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "PASS", "isFtpaAppellantDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 have isFtpaAppellantDocsVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotDocsVisibleInDecided - IF M3.Party IS 1 AND M3.OutOfTime IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotDocsVisibleInDecided_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaAppellantOotDocsVisibleInDecided") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"isFtpaAppellantOotDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is 1 & isFtpaAppellantOotDocsVisibleInDecided is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "PASS", "isFtpaAppellantOotDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is 1 have isFtpaAppellantOotDocsVisibleInDecided equals No", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotDocsVisibleInDecided - IF M3.Party IS 1 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotDocsVisibleInDecided_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but OutOfTime is NOT 1, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaAppellantOotDocsVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"isFtpaAppellantOotDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is not 1 & isFtpaAppellantOotDocsVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "PASS", "isFtpaAppellantOotDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is not 1 have isFtpaAppellantOotDocsVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotDocsVisibleInDecided - IF M3.Party IS 2 AND M3.OutOfTime IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotDocsVisibleInDecided_test3(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2, should always be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaAppellantOotDocsVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"isFtpaAppellantOotDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is 1 & isFtpaAppellantOotDocsVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "PASS", "isFtpaAppellantOotDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is 1 have isFtpaAppellantOotDocsVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotDocsVisibleInDecided - IF M3.Party IS 2 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotDocsVisibleInDecided_test4(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2, should always be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaAppellantOotDocsVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"isFtpaAppellantOotDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is not 1 & isFtpaAppellantOotDocsVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "PASS", "isFtpaAppellantOotDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is not 1 have isFtpaAppellantOotDocsVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotDocsVisibleInSubmitted - IF M3.Party IS 1 AND M3.OutOfTime IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotDocsVisibleInSubmitted_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaAppellantOotDocsVisibleInSubmitted") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"isFtpaAppellantOotDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is 1 & isFtpaAppellantOotDocsVisibleInSubmitted is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "PASS", "isFtpaAppellantOotDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is 1 have isFtpaAppellantOotDocsVisibleInSubmitted equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotDocsVisibleInSubmitted - IF M3.Party IS 1 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotDocsVisibleInSubmitted_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 1 but not OutOfTime, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaAppellantOotDocsVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"isFtpaAppellantOotDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is not 1 & isFtpaAppellantOotDocsVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "PASS", "isFtpaAppellantOotDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is not 1 have isFtpaAppellantOotDocsVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotDocsVisibleInSubmitted - IF M3.Party IS 2 AND M3.OutOfTime IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotDocsVisibleInSubmitted_test3(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 2, should be omitted regardless of OutOfTime
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaAppellantOotDocsVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"isFtpaAppellantOotDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is 1 & isFtpaAppellantOotDocsVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "PASS", "isFtpaAppellantOotDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is 1 have isFtpaAppellantOotDocsVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotDocsVisibleInSubmitted - IF M3.Party IS 2 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotDocsVisibleInSubmitted_test4(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 2, should be omitted regardless of OutOfTime
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaAppellantOotDocsVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"isFtpaAppellantOotDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is not 1 & isFtpaAppellantOotDocsVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "PASS", "isFtpaAppellantOotDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is not 1 have isFtpaAppellantOotDocsVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantGroundsDocsVisibleInDecided - IF M3.Party IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantGroundsDocsVisibleInDecided_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaAppellantGroundsDocsVisibleInDecided") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", f"isFtpaAppellantGroundsDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & isFtpaAppellantGroundsDocsVisibleInDecided is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "PASS", "isFtpaAppellantGroundsDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 have isFtpaAppellantGroundsDocsVisibleInDecided equals No", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantGroundsDocsVisibleInDecided - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantGroundsDocsVisibleInDecided_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaAppellantGroundsDocsVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", f"isFtpaAppellantGroundsDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & isFtpaAppellantGroundsDocsVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "PASS", "isFtpaAppellantGroundsDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 have isFtpaAppellantGroundsDocsVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantEvidenceDocsVisibleInDecided - IF M3.Party IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantEvidenceDocsVisibleInDecided_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaAppellantEvidenceDocsVisibleInDecided") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", f"isFtpaAppellantEvidenceDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & isFtpaAppellantEvidenceDocsVisibleInDecided is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "PASS", "isFtpaAppellantEvidenceDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 have isFtpaAppellantEvidenceDocsVisibleInDecided equals No", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantEvidenceDocsVisibleInDecided - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantEvidenceDocsVisibleInDecided_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaAppellantEvidenceDocsVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", f"isFtpaAppellantEvidenceDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & isFtpaAppellantEvidenceDocsVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "PASS", "isFtpaAppellantEvidenceDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 have isFtpaAppellantEvidenceDocsVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantGroundsDocsVisibleInSubmitted - IF M3.Party IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantGroundsDocsVisibleInSubmitted_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaAppellantGroundsDocsVisibleInSubmitted") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", f"isFtpaAppellantGroundsDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & isFtpaAppellantGroundsDocsVisibleInSubmitted is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "PASS", "isFtpaAppellantGroundsDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 have isFtpaAppellantGroundsDocsVisibleInSubmitted equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantGroundsDocsVisibleInSubmitted - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantGroundsDocsVisibleInSubmitted_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaAppellantGroundsDocsVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", f"isFtpaAppellantGroundsDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & isFtpaAppellantGroundsDocsVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "PASS", "isFtpaAppellantGroundsDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 have isFtpaAppellantGroundsDocsVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantEvidenceDocsVisibleInSubmitted - IF M3.Party IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantEvidenceDocsVisibleInSubmitted_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaAppellantEvidenceDocsVisibleInSubmitted") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", f"isFtpaAppellantEvidenceDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & isFtpaAppellantEvidenceDocsVisibleInSubmitted is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "PASS", "isFtpaAppellantEvidenceDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 have isFtpaAppellantEvidenceDocsVisibleInSubmitted equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantEvidenceDocsVisibleInSubmitted - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantEvidenceDocsVisibleInSubmitted_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaAppellantEvidenceDocsVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", f"isFtpaAppellantEvidenceDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & isFtpaAppellantEvidenceDocsVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "PASS", "isFtpaAppellantEvidenceDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 have isFtpaAppellantEvidenceDocsVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotExplanationVisibleInDecided - IF M3.Party IS 1 AND M3.OutOfTime IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotExplanationVisibleInDecided_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaAppellantOotExplanationVisibleInDecided") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"isFtpaAppellantOotExplanationVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is 1 & isFtpaAppellantOotExplanationVisibleInDecided is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "PASS", "isFtpaAppellantOotExplanationVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is 1 have isFtpaAppellantOotExplanationVisibleInDecided equals No", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotExplanationVisibleInDecided - IF M3.Party IS 1 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotExplanationVisibleInDecided_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 1 but not OutOfTime, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaAppellantOotExplanationVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"isFtpaAppellantOotExplanationVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is not 1 & isFtpaAppellantOotExplanationVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "PASS", "isFtpaAppellantOotExplanationVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is not 1 have isFtpaAppellantOotExplanationVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotExplanationVisibleInDecided - IF M3.Party IS 2 AND M3.OutOfTime IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotExplanationVisibleInDecided_test3(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 2, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaAppellantOotExplanationVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"isFtpaAppellantOotExplanationVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is 1 & isFtpaAppellantOotExplanationVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "PASS", "isFtpaAppellantOotExplanationVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is 1 have isFtpaAppellantOotExplanationVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotExplanationVisibleInDecided - IF M3.Party IS 2 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotExplanationVisibleInDecided_test4(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 2, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaAppellantOotExplanationVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"isFtpaAppellantOotExplanationVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is not 1 & isFtpaAppellantOotExplanationVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "PASS", "isFtpaAppellantOotExplanationVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is not 1 have isFtpaAppellantOotExplanationVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotExplanationVisibleInSubmitted - IF M3.Party IS 1 AND M3.OutOfTime IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotExplanationVisibleInSubmitted_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaAppellantOotExplanationVisibleInSubmitted") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"isFtpaAppellantOotExplanationVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is 1 & isFtpaAppellantOotExplanationVisibleInSubmitted is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "PASS", "isFtpaAppellantOotExplanationVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is 1 have isFtpaAppellantOotExplanationVisibleInSubmitted equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotExplanationVisibleInSubmitted - IF M3.Party IS 1 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotExplanationVisibleInSubmitted_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 1 but not OutOfTime, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaAppellantOotExplanationVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"isFtpaAppellantOotExplanationVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is not 1 & isFtpaAppellantOotExplanationVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "PASS", "isFtpaAppellantOotExplanationVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is not 1 have isFtpaAppellantOotExplanationVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotExplanationVisibleInSubmitted - IF M3.Party IS 2 AND M3.OutOfTime IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotExplanationVisibleInSubmitted_test3(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 2, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaAppellantOotExplanationVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"isFtpaAppellantOotExplanationVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is 1 & isFtpaAppellantOotExplanationVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "PASS", "isFtpaAppellantOotExplanationVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is 1 have isFtpaAppellantOotExplanationVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantOotExplanationVisibleInSubmitted - IF M3.Party IS 2 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaAppellantOotExplanationVisibleInSubmitted_test4(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 2, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaAppellantOotExplanationVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"isFtpaAppellantOotExplanationVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is not 1 & isFtpaAppellantOotExplanationVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "PASS", "isFtpaAppellantOotExplanationVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is not 1 have isFtpaAppellantOotExplanationVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentSubmitted - IF M3.Party IS 2 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentSubmitted_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaRespondentSubmitted") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentSubmitted", "FAIL", f"ftpaRespondentSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & ftpaRespondentSubmitted is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentSubmitted", "PASS", "ftpaRespondentSubmitted acceptance criteria pass: all rows where M3.Party is 2 have ftpaRespondentSubmitted equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("ftpaRespondentSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentSubmitted - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentSubmitted_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaRespondentSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentSubmitted", "FAIL", f"ftpaRespondentSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaRespondentSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentSubmitted", "PASS", "ftpaRespondentSubmitted acceptance criteria pass: all rows where M3.Party is 1 have ftpaRespondentSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("ftpaRespondentSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentDocsVisibleInDecided - IF M3.Party IS 2 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentDocsVisibleInDecided_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaRespondentDocsVisibleInDecided") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", f"isFtpaRespondentDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & isFtpaRespondentDocsVisibleInDecided is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentDocsVisibleInDecided", "PASS", "isFtpaRespondentDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 have isFtpaRespondentDocsVisibleInDecided equals No", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentDocsVisibleInDecided - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentDocsVisibleInDecided_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaRespondentDocsVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", f"isFtpaRespondentDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & isFtpaRespondentDocsVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentDocsVisibleInDecided", "PASS", "isFtpaRespondentDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 have isFtpaRespondentDocsVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentDocsVisibleInSubmitted - IF M3.Party IS 2 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentDocsVisibleInSubmitted_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaRespondentDocsVisibleInSubmitted") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", f"isFtpaRespondentDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & isFtpaRespondentDocsVisibleInSubmitted is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "PASS", "isFtpaRespondentDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 have isFtpaRespondentDocsVisibleInSubmitted equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentDocsVisibleInSubmitted - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentDocsVisibleInSubmitted_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaRespondentDocsVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", f"isFtpaRespondentDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & isFtpaRespondentDocsVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "PASS", "isFtpaRespondentDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 have isFtpaRespondentDocsVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotDocsVisibleInDecided - IF M3.Party IS 2 AND M3.OutOfTime IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotDocsVisibleInDecided_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaRespondentOotDocsVisibleInDecided") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"isFtpaRespondentOotDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is 1 & isFtpaRespondentOotDocsVisibleInDecided is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "PASS", "isFtpaRespondentOotDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is 1 have isFtpaRespondentOotDocsVisibleInDecided equals No", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotDocsVisibleInDecided - IF M3.Party IS 2 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotDocsVisibleInDecided_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 2 but not OutOfTime, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaRespondentOotDocsVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"isFtpaRespondentOotDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is not 1 & isFtpaRespondentOotDocsVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "PASS", "isFtpaRespondentOotDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is not 1 have isFtpaRespondentOotDocsVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotDocsVisibleInDecided - IF M3.Party IS 1 AND M3.OutOfTime IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotDocsVisibleInDecided_test3(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 1, should be omitted regardless of OutOfTime
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaRespondentOotDocsVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"isFtpaRespondentOotDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is 1 & isFtpaRespondentOotDocsVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "PASS", "isFtpaRespondentOotDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is 1 have isFtpaRespondentOotDocsVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotDocsVisibleInDecided - IF M3.Party IS 1 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotDocsVisibleInDecided_test4(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 1, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaRespondentOotDocsVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"isFtpaRespondentOotDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is not 1 & isFtpaRespondentOotDocsVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "PASS", "isFtpaRespondentOotDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is not 1 have isFtpaRespondentOotDocsVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotDocsVisibleInSubmitted - IF M3.Party IS 2 AND M3.OutOfTime IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotDocsVisibleInSubmitted_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaRespondentOotDocsVisibleInSubmitted") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"isFtpaRespondentOotDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is 1 & isFtpaRespondentOotDocsVisibleInSubmitted is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "PASS", "isFtpaRespondentOotDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is 1 have isFtpaRespondentOotDocsVisibleInSubmitted equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotDocsVisibleInSubmitted - IF M3.Party IS 2 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotDocsVisibleInSubmitted_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 2 but not OutOfTime, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaRespondentOotDocsVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"isFtpaRespondentOotDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is not 1 & isFtpaRespondentOotDocsVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "PASS", "isFtpaRespondentOotDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is not 1 have isFtpaRespondentOotDocsVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotDocsVisibleInSubmitted - IF M3.Party IS 1 AND M3.OutOfTime IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotDocsVisibleInSubmitted_test3(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 1, should be omitted regardless of OutOfTime
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaRespondentOotDocsVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"isFtpaRespondentOotDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is 1 & isFtpaRespondentOotDocsVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "PASS", "isFtpaRespondentOotDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is 1 have isFtpaRespondentOotDocsVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotDocsVisibleInSubmitted - IF M3.Party IS 1 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotDocsVisibleInSubmitted_test4(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 1, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaRespondentOotDocsVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"isFtpaRespondentOotDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is not 1 & isFtpaRespondentOotDocsVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "PASS", "isFtpaRespondentOotDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is not 1 have isFtpaRespondentOotDocsVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentGroundsDocsVisibleInDecided - IF M3.Party IS 2 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentGroundsDocsVisibleInDecided_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaRespondentGroundsDocsVisibleInDecided") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", f"isFtpaRespondentGroundsDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & isFtpaRespondentGroundsDocsVisibleInDecided is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "PASS", "isFtpaRespondentGroundsDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 have isFtpaRespondentGroundsDocsVisibleInDecided equals No", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentGroundsDocsVisibleInDecided - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentGroundsDocsVisibleInDecided_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaRespondentGroundsDocsVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", f"isFtpaRespondentGroundsDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & isFtpaRespondentGroundsDocsVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "PASS", "isFtpaRespondentGroundsDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 have isFtpaRespondentGroundsDocsVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentEvidenceDocsVisibleInDecided - IF M3.Party IS 2 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentEvidenceDocsVisibleInDecided_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaRespondentEvidenceDocsVisibleInDecided") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", f"isFtpaRespondentEvidenceDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & isFtpaRespondentEvidenceDocsVisibleInDecided is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "PASS", "isFtpaRespondentEvidenceDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 have isFtpaRespondentEvidenceDocsVisibleInDecided equals No", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentEvidenceDocsVisibleInDecided - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentEvidenceDocsVisibleInDecided_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaRespondentEvidenceDocsVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", f"isFtpaRespondentEvidenceDocsVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & isFtpaRespondentEvidenceDocsVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "PASS", "isFtpaRespondentEvidenceDocsVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 have isFtpaRespondentEvidenceDocsVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentGroundsDocsVisibleInSubmitted - IF M3.Party IS 2 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaRespondentGroundsDocsVisibleInSubmitted") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", f"isFtpaRespondentGroundsDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & isFtpaRespondentGroundsDocsVisibleInSubmitted is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "PASS", "isFtpaRespondentGroundsDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 have isFtpaRespondentGroundsDocsVisibleInSubmitted equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentGroundsDocsVisibleInSubmitted - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaRespondentGroundsDocsVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", f"isFtpaRespondentGroundsDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & isFtpaRespondentGroundsDocsVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "PASS", "isFtpaRespondentGroundsDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 have isFtpaRespondentGroundsDocsVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentEvidenceDocsVisibleInSubmitted - IF M3.Party IS 2 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("isFtpaRespondentEvidenceDocsVisibleInSubmitted") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", f"isFtpaRespondentEvidenceDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & isFtpaRespondentEvidenceDocsVisibleInSubmitted is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "PASS", "isFtpaRespondentEvidenceDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 have isFtpaRespondentEvidenceDocsVisibleInSubmitted equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentEvidenceDocsVisibleInSubmitted - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but field is NOT omitted (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("isFtpaRespondentEvidenceDocsVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", f"isFtpaRespondentEvidenceDocsVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & isFtpaRespondentEvidenceDocsVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "PASS", "isFtpaRespondentEvidenceDocsVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 have isFtpaRespondentEvidenceDocsVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotExplanationVisibleInDecided - IF M3.Party IS 2 AND M3.OutOfTime IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotExplanationVisibleInDecided_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is not "No"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaRespondentOotExplanationVisibleInDecided") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"isFtpaRespondentOotExplanationVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is 1 & isFtpaRespondentOotExplanationVisibleInDecided is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "PASS", "isFtpaRespondentOotExplanationVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is 1 have isFtpaRespondentOotExplanationVisibleInDecided equals No", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotExplanationVisibleInDecided - IF M3.Party IS 2 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotExplanationVisibleInDecided_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 2 but not OutOfTime, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaRespondentOotExplanationVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"isFtpaRespondentOotExplanationVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is not 1 & isFtpaRespondentOotExplanationVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "PASS", "isFtpaRespondentOotExplanationVisibleInDecided acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is not 1 have isFtpaRespondentOotExplanationVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotExplanationVisibleInDecided - IF M3.Party IS 1 AND M3.OutOfTime IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotExplanationVisibleInDecided_test3(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 1, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaRespondentOotExplanationVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"isFtpaRespondentOotExplanationVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is 1 & isFtpaRespondentOotExplanationVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "PASS", "isFtpaRespondentOotExplanationVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is 1 have isFtpaRespondentOotExplanationVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotExplanationVisibleInDecided - IF M3.Party IS 1 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotExplanationVisibleInDecided_test4(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 1, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaRespondentOotExplanationVisibleInDecided").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"isFtpaRespondentOotExplanationVisibleInDecided acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is not 1 & isFtpaRespondentOotExplanationVisibleInDecided is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "PASS", "isFtpaRespondentOotExplanationVisibleInDecided acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is not 1 have isFtpaRespondentOotExplanationVisibleInDecided omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotExplanationVisibleInSubmitted - IF M3.Party IS 2 AND M3.OutOfTime IS 1 = Include (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotExplanationVisibleInSubmitted_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where conditions are met but value is not "Yes"
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaRespondentOotExplanationVisibleInSubmitted") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"isFtpaRespondentOotExplanationVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is 1 & isFtpaRespondentOotExplanationVisibleInSubmitted is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "PASS", "isFtpaRespondentOotExplanationVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is 1 have isFtpaRespondentOotExplanationVisibleInSubmitted equals Yes", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotExplanationVisibleInSubmitted - IF M3.Party IS 2 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotExplanationVisibleInSubmitted_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 2 but not OutOfTime, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaRespondentOotExplanationVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"isFtpaRespondentOotExplanationVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 and M3.OutOfTime is not 1 & isFtpaRespondentOotExplanationVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "PASS", "isFtpaRespondentOotExplanationVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 2 and M3.OutOfTime is not 1 have isFtpaRespondentOotExplanationVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotExplanationVisibleInSubmitted - IF M3.Party IS 1 AND M3.OutOfTime IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotExplanationVisibleInSubmitted_test3(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 1, should be omitted regardless of OutOfTime
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") == 1) &
            (col("isFtpaRespondentOotExplanationVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"isFtpaRespondentOotExplanationVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is 1 & isFtpaRespondentOotExplanationVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "PASS", "isFtpaRespondentOotExplanationVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is 1 have isFtpaRespondentOotExplanationVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaRespondentOotExplanationVisibleInSubmitted - IF M3.Party IS 1 AND M3.OutOfTime IS NOT 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotExplanationVisibleInSubmitted_test4(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party is 1, should be omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("OutOfTime") != 1) &
            (col("isFtpaRespondentOotExplanationVisibleInSubmitted").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"isFtpaRespondentOotExplanationVisibleInSubmitted acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 and M3.OutOfTime is not 1 & isFtpaRespondentOotExplanationVisibleInSubmitted is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "PASS", "isFtpaRespondentOotExplanationVisibleInSubmitted acceptance criteria pass: all rows where M3.Party is 1 and M3.OutOfTime is not 1 have isFtpaRespondentOotExplanationVisibleInSubmitted omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantDocuments - IF M3.Party IS 1 = Include [] (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import size

def test_ftpaAppellantDocuments_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Use size() == 0 to check for an empty array correctly
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (size(col("ftpaAppellantDocuments")) != 0)
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantDocuments", "FAIL", f"ftpaAppellantDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaAppellantDocuments is not an empty array", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantDocuments", "PASS", "ftpaAppellantDocuments acceptance criteria pass: all rows where M3.Party is 1 have ftpaAppellantDocuments as an empty array", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    
#######################
# ftpaAppellantDocuments - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantDocuments_test2(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaAppellantDocuments").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantDocuments", "FAIL", f"ftpaAppellantDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & ftpaAppellantDocuments is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantDocuments", "PASS", "ftpaAppellantDocuments acceptance criteria pass: all rows where M3.Party is 2 have ftpaAppellantDocuments omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentDocuments - IF M3.Party IS 2 = Include [] (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import size, col, row_number
from pyspark.sql.window import Window

def test_ftpaRespondentDocuments_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Use size() == 0 to check for an empty array
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (size(col("ftpaRespondentDocuments")) != 0)
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentDocuments", "FAIL", f"ftpaRespondentDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & ftpaRespondentDocuments is not an empty array", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentDocuments", "PASS", "ftpaRespondentDocuments acceptance criteria pass: all rows where M3.Party is 2 have ftpaRespondentDocuments as an empty array", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentDocuments - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentDocuments_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaRespondentDocuments").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentDocuments", "FAIL", f"ftpaRespondentDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaRespondentDocuments is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentDocuments", "PASS", "ftpaRespondentDocuments acceptance criteria pass: all rows where M3.Party is 1 have ftpaRespondentDocuments omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantGroundsDocuments - IF M3.Party IS 1 = Include [] (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import size, col, row_number
from pyspark.sql.window import Window

def test_ftpaAppellantGroundsDocuments_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantGroundsDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Use size() == 0 to check for an empty array
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (size(col("ftpaAppellantGroundsDocuments")) != 0)
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantGroundsDocuments", "FAIL", f"ftpaAppellantGroundsDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaAppellantGroundsDocuments is not an empty array", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantGroundsDocuments", "PASS", "ftpaAppellantGroundsDocuments acceptance criteria pass: all rows where M3.Party is 1 have ftpaAppellantGroundsDocuments as an empty array", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantGroundsDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantGroundsDocuments - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantGroundsDocuments_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantGroundsDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaAppellantGroundsDocuments").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantGroundsDocuments", "FAIL", f"ftpaAppellantGroundsDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & ftpaAppellantGroundsDocuments is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantGroundsDocuments", "PASS", "ftpaAppellantGroundsDocuments acceptance criteria pass: all rows where M3.Party is 2 have ftpaAppellantGroundsDocuments omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantGroundsDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaRespondentGroundsDocuments - IF M3.Party IS 2 = Include [] (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import size, col, row_number
from pyspark.sql.window import Window

def test_ftpaRespondentGroundsDocuments_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentGroundsDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Use size() == 0 to check for an empty array
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (size(col("ftpaRespondentGroundsDocuments")) != 0)
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentGroundsDocuments", "FAIL", f"ftpaRespondentGroundsDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & ftpaRespondentGroundsDocuments is not an empty array", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentGroundsDocuments", "PASS", "ftpaRespondentGroundsDocuments acceptance criteria pass: all rows where M3.Party is 2 have ftpaRespondentGroundsDocuments as an empty array", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentGroundsDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentGroundsDocuments - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentGroundsDocuments_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentGroundsDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaRespondentGroundsDocuments").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentGroundsDocuments", "FAIL", f"ftpaRespondentGroundsDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaRespondentGroundsDocuments is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentGroundsDocuments", "PASS", "ftpaRespondentGroundsDocuments acceptance criteria pass: all rows where M3.Party is 1 have ftpaRespondentGroundsDocuments omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentGroundsDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantEvidenceDocuments - IF M3.Party IS 1 = Include [] (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import size, col, row_number
from pyspark.sql.window import Window

def test_ftpaAppellantEvidenceDocuments_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantEvidenceDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Use size() == 0 to check for an empty array
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (size(col("ftpaAppellantEvidenceDocuments")) != 0)
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantEvidenceDocuments", "FAIL", f"ftpaAppellantEvidenceDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaAppellantEvidenceDocuments is not an empty array", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantEvidenceDocuments", "PASS", "ftpaAppellantEvidenceDocuments acceptance criteria pass: all rows where M3.Party is 1 have ftpaAppellantEvidenceDocuments as an empty array", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantEvidenceDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantEvidenceDocuments - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantEvidenceDocuments_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantEvidenceDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaAppellantEvidenceDocuments").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantEvidenceDocuments", "FAIL", f"ftpaAppellantEvidenceDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & ftpaAppellantEvidenceDocuments is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantEvidenceDocuments", "PASS", "ftpaAppellantEvidenceDocuments acceptance criteria pass: all rows where M3.Party is 2 have ftpaAppellantEvidenceDocuments omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantEvidenceDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentEvidenceDocuments - IF M3.Party IS 2 = Include [] (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import size, col, row_number
from pyspark.sql.window import Window

def test_ftpaRespondentEvidenceDocuments_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentEvidenceDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Use size() == 0 to check for an empty array correctly
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (size(col("ftpaRespondentEvidenceDocuments")) != 0)
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentEvidenceDocuments", "FAIL", f"ftpaRespondentEvidenceDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & ftpaRespondentEvidenceDocuments is not an empty array", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentEvidenceDocuments", "PASS", "ftpaRespondentEvidenceDocuments acceptance criteria pass: all rows where M3.Party is 2 have ftpaRespondentEvidenceDocuments as an empty array", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentEvidenceDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentEvidenceDocuments - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentEvidenceDocuments_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentEvidenceDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaRespondentEvidenceDocuments").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentEvidenceDocuments", "FAIL", f"ftpaRespondentEvidenceDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaRespondentEvidenceDocuments is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentEvidenceDocuments", "PASS", "ftpaRespondentEvidenceDocuments acceptance criteria pass: all rows where M3.Party is 1 have ftpaRespondentEvidenceDocuments omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentEvidenceDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantOutOfTimeDocuments - IF M3.Party IS 1 = Include [] (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import size, col, row_number
from pyspark.sql.window import Window

def test_ftpaAppellantOutOfTimeDocuments_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Use size() == 0 to check for an empty array
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (size(col("ftpaAppellantOutOfTimeDocuments")) != 0)
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantOutOfTimeDocuments", "FAIL", f"ftpaAppellantOutOfTimeDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaAppellantOutOfTimeDocuments is not an empty array", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantOutOfTimeDocuments", "PASS", "ftpaAppellantOutOfTimeDocuments acceptance criteria pass: all rows where M3.Party is 1 have ftpaAppellantOutOfTimeDocuments as an empty array", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantOutOfTimeDocuments - IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantOutOfTimeDocuments_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 2 but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaAppellantOutOfTimeDocuments").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantOutOfTimeDocuments", "FAIL", f"ftpaAppellantOutOfTimeDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & ftpaAppellantOutOfTimeDocuments is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantOutOfTimeDocuments", "PASS", "ftpaAppellantOutOfTimeDocuments acceptance criteria pass: all rows where M3.Party is 2 have ftpaAppellantOutOfTimeDocuments omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaRespondentOutOfTimeDocuments - IF M3.Party IS 2 = Include [] (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import size, col, row_number
from pyspark.sql.window import Window

def test_ftpaRespondentOutOfTimeDocuments_test1(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Use size() == 0 to check for an empty array
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) & 
            (size(col("ftpaRespondentOutOfTimeDocuments")) != 0)
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentOutOfTimeDocuments", "FAIL", f"ftpaRespondentOutOfTimeDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 2 & ftpaRespondentOutOfTimeDocuments is not an empty array", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentOutOfTimeDocuments", "PASS", "ftpaRespondentOutOfTimeDocuments acceptance criteria pass: all rows where M3.Party is 2 have ftpaRespondentOutOfTimeDocuments as an empty array", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaRespondentOutOfTimeDocuments - IF M3.Party IS 1 = OMIT (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentOutOfTimeDocuments_test2(test_df):
    try:
        # 1. Filter only for the Appeal State
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeDocuments", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party is 1 but field is NOT omitted
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaRespondentOutOfTimeDocuments").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaRespondentOutOfTimeDocuments", "FAIL", f"ftpaRespondentOutOfTimeDocuments acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaRespondentOutOfTimeDocuments is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaRespondentOutOfTimeDocuments", "PASS", "ftpaRespondentOutOfTimeDocuments acceptance criteria pass: all rows where M3.Party is 1 have ftpaRespondentOutOfTimeDocuments omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeDocuments", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

    
