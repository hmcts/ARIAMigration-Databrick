from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import inspect

#Import Test Results class
from models.test_result import TestResult

#Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "decided(b)"

############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "ftpaFirstDecision",
            "ftpaFinalDecisionForDisplay",
            "reasonRehearingRule32",
            "rule32ListingAdditionalIns",
            "ftpaFinalDecisionRemadeRule32",
            "isDlrmSetAsideEnabled",
            "isReheardAppealEnabled",
            "secondFtpaDecisionExists",
            "caseFlagSetAsideReheardExists",
            "allSetAsideDocs"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

def test_dec_b_defaultValues(test_df, fields_to_exclude):
    try:
        expected_defaults = {
            "ftpaFirstDecision": "remadeRule32",
            "ftpaFinalDecisionForDisplay": "undecided",
            "reasonRehearingRule32": "Set aside and to be reheard under rule 32",
            "rule32ListingAdditionalIns": "This is an ARIA Migrated case. Please refer to the documents for any additional listing instructions.",
            "updateTribunalDecisionList": "underRule32",
            "ftpaFinalDecisionRemadeRule32": "",
            "isDlrmSetAsideEnabled": "Yes",
            "isReheardAppealEnabled": "Yes",
            "secondFtpaDecisionExists": "No",
            "caseFlagSetAsideReheardExists": "Yes"

        }

        expected_arrays = {
        "allSetAsideDocs": None
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

        for field, contains_val in expected_arrays.items():
            if field in fields_to_exclude:
                continue
            if contains_val:
                condition = (~array_contains(col(field), contains_val))
            else:
                condition = (size(col(field)) != 0)
                
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
def test_ftpa_init(json, M1_bronze, M3_bronze, M6_bronze):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "ftpaApplicantType",
            "ftpaAppellantDecisionDate",
            "ftpaRespondentDecisionDate",
            "ftpaAppellantRjDecisionOutcomeType",
            "ftpaRespondentRjDecisionOutcomeType",
            "judgesNamesToExclude",
            "updateTribunalDecisionDateRule32",
            "ftpaAppellantDecisionRemadeRule32Text",
            "ftpaRespondentDecisionRemadeRule32Text",
            "isFtpaAppellantDecided",
            "isFtpaRespondentDecided",
            "allFtpaAppellantDecisionDocs",
            "allFtpaRespondentDecisionDocs"
        )

        M1_bronze = M1_bronze.select(
            "CaseNo"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "CaseStatus",
            "StatusId",
            "Party",
        )

        M6_bronze = M6_bronze.select(
            "CaseNo",
            "Required",
            "Judge_Forenames",
            "Judge_Surname",
            "Judge_Title"
        )
        test_df = test_df.join(
            M1_bronze,
            json["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        ).join(
            M3_bronze,
            json["appealReferenceNumber"] == M3_bronze["CaseNo"],
            "inner"
        ).join(
            M6_bronze,
            json["appealReferenceNumber"] == M3_bronze["CaseNo"],
            "inner"
        ).drop(M1_bronze["CaseNo"], M3_bronze["CaseNo"])

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("ftpa", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state,inspect.stack()[0].function)




#######################
# ftpaApplicantType - IF M3.Party IS 1 = "appellant"
# (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaApplicantType_test1(test_df):
    try:
        # 1. Filter for the specific FTPA state
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        if target_records.count() == 0:
            return TestResult("ftpaApplicantType", "PASS", "No records found with CaseStatus 39 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 1 must be "appellant"
        failures = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaApplicantType") != "appellant")
        )

        if failures.count() != 0:
            return TestResult("ftpaApplicantType", "FAIL", f"Found {failures.count()} rows where Party 1 was not mapped to 'appellant'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaApplicantType", "PASS", "Party 1 correctly mapped to 'appellant'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaApplicantType", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    



#######################
# ftpaApplicantType - IF M3.Party IS 2 = "respondent"
# (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaApplicantType_test2(test_df):
    try:
        # 1. Filter for the specific FTPA state
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        if target_records.count() == 0:
            return TestResult("ftpaApplicantType", "PASS", "No records found with CaseStatus 39 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 2 must be "respondent"
        failures = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaApplicantType") != "respondent")
        )

        if failures.count() != 0:
            return TestResult("ftpaApplicantType", "FAIL", f"Found {failures.count()} rows where Party 2 was not mapped to 'respondent'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaApplicantType", "PASS", "Party 2 correctly mapped to 'respondent'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaApplicantType", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaAppellantDecisionDate - IF M3.Party IS 1 = Include
# (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantDecisionDate_test1(test_df):
    try:
        # 1. Filter for the FTPA Decided state
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantDecisionDate", "PASS", "No records found with CaseStatus 39 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Get the latest record per Case
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 1 must have a populated date
        # We fail if it is Null or empty
        failures = winning_records.filter(
            (col("Party") == 1) & 
            ((col("ftpaAppellantDecisionDate").isNull()) | (col("ftpaAppellantDecisionDate") == ""))
        )

        if failures.count() != 0:
            return TestResult("ftpaAppellantDecisionDate", "FAIL", f"Found {failures.count()} Party 1 rows where decision date was missing", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaAppellantDecisionDate", "PASS", "Party 1 correctly includes ftpaAppellantDecisionDate", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantDecisionDate", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaAppellantDecisionDate - IF M3.Party IS 2 = OMIT
# (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantDecisionDate_test2(test_df):
    try:
        # 1. Filter and identify winning records
        target_records = test_df.filter(col("CaseStatus") == 39)
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 2. Acceptance Criteria: Party 2 must be Omitted (Null)
        # If your dev uses empty string instead of Null, add: | (col("ftpaAppellantDecisionDate") != "")
        failures = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaAppellantDecisionDate").isNotNull())
        )

        if failures.count() != 0:
            return TestResult("ftpaAppellantDecisionDate", "FAIL", f"Found {failures.count()} Party 2 rows where Appellant date was not omitted", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaAppellantDecisionDate", "PASS", "Party 2 correctly omits ftpaAppellantDecisionDate", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantDecisionDate", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaRespondentDecisionDate - IF M3.Party IS 2 = Include
# (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentDecisionDate_test1(test_df):
    try:
        # 1. Filter for the FTPA Decided state
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentDecisionDate", "PASS", "No records found with CaseStatus 39 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 2 must have a populated date
        failures = winning_records.filter(
            (col("Party") == 2) & 
            ((col("ftpaRespondentDecisionDate").isNull()) | (col("ftpaRespondentDecisionDate") == ""))
        )

        if failures.count() != 0:
            return TestResult("ftpaRespondentDecisionDate", "FAIL", f"Found {failures.count()} Party 2 rows where Respondent decision date was missing", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaRespondentDecisionDate", "PASS", "Party 2 correctly includes ftpaRespondentDecisionDate", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentDecisionDate", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    



#######################
# ftpaRespondentDecisionDate - IF M3.Party IS 1 = OMIT
# (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentDecisionDate_test2(test_df):
    try:
        # 1. Filter and identify winning records
        target_records = test_df.filter(col("CaseStatus") == 39)
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 2. Acceptance Criteria: Party 1 must be Omitted (Null)
        # We use isNotNull() to find failures (if it has a value like "No" or a date, it fails)
        failures = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaRespondentDecisionDate").isNotNull())
        )

        if failures.count() != 0:
            return TestResult("ftpaRespondentDecisionDate", "FAIL", f"Found {failures.count()} Party 1 rows where Respondent date was not omitted", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaRespondentDecisionDate", "PASS", "Party 1 correctly omits ftpaRespondentDecisionDate", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentDecisionDate", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaAppellantRjDecisionOutcomeType - IF M3.Party IS 1 = "remadeRule32"
# (MAX StatusID WHERE CaseStatus = 39)
######################
def test_ftpaAppellantRjDecisionOutcomeType_test1(test_df):
    try:
        # 1. Filter for the FTPA Decided state
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantRjDecisionOutcomeType", "PASS", "No records found with CaseStatus 39 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 1 must be "remadeRule32"
        failures = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaAppellantRjDecisionOutcomeType") != "remadeRule32")
        )

        if failures.count() != 0:
            return TestResult("ftpaAppellantRjDecisionOutcomeType", "FAIL", f"Found {failures.count()} Party 1 rows where outcome type was not 'remadeRule32'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaAppellantRjDecisionOutcomeType", "PASS", "Party 1 correctly includes 'remadeRule32'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantRjDecisionOutcomeType", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    



#######################
# ftpaAppellantRjDecisionOutcomeType - IF M3.Party IS 2 = OMIT
# (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantRjDecisionOutcomeType_test2(test_df):
    try:
        # 1. Filter and identify winning records
        target_records = test_df.filter(col("CaseStatus") == 39)
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 2. Acceptance Criteria: Party 2 must be Omitted (Null)
        # We fail if the column IS NOT NULL (matches your confirmed OMIT = NULL logic)
        failures = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaAppellantRjDecisionOutcomeType").isNotNull())
        )

        if failures.count() != 0:
            return TestResult("ftpaAppellantRjDecisionOutcomeType", "FAIL", f"Found {failures.count()} Party 2 rows where Appellant outcome was not omitted", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaAppellantRjDecisionOutcomeType", "PASS", "Party 2 correctly omits ftpaAppellantRjDecisionOutcomeType", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantRjDecisionOutcomeType", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaRespondentRjDecisionOutcomeType - IF M3.Party IS 2 = "remadeRule32"
# (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentRjDecisionOutcomeType_test1(test_df):
    try:
        # 1. Filter for the FTPA Decided state
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentRjDecisionOutcomeType", "PASS", "No records found with CaseStatus 39 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 2 must be "remadeRule32"
        failures = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaRespondentRjDecisionOutcomeType") != "remadeRule32")
        )

        if failures.count() != 0:
            return TestResult("ftpaRespondentRjDecisionOutcomeType", "FAIL", f"Found {failures.count()} Party 2 rows where outcome type was not 'remadeRule32'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaRespondentRjDecisionOutcomeType", "PASS", "Party 2 correctly includes 'remadeRule32'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentRjDecisionOutcomeType", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    



#######################
# ftpaRespondentRjDecisionOutcomeType - IF M3.Party IS 1 = OMIT
# (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentRjDecisionOutcomeType_test2(test_df):
    try:
        # 1. Filter and identify winning records
        target_records = test_df.filter(col("CaseStatus") == 39)
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 2. Acceptance Criteria: Party 1 must be Omitted (Null)
        failures = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaRespondentRjDecisionOutcomeType").isNotNull())
        )

        if failures.count() != 0:
            return TestResult("ftpaRespondentRjDecisionOutcomeType", "FAIL", f"Found {failures.count()} Party 1 rows where Respondent outcome was not omitted", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaRespondentRjDecisionOutcomeType", "PASS", "Party 1 correctly omits ftpaRespondentRjDecisionOutcomeType", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentRjDecisionOutcomeType", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    
#######################
# judgesNamesToExclude 
# Requirement: <Surname>, <Forenames> (<Title>)
#######################

#######################
# judgesNamesToExclude - Left Join Logic
# Requirement: <Surname>, <Forenames> (<Title>)
#######################
#######################
# judgesNamesToExclude - Left Join Logic (Ambiguity Fix)
#######################
#######################
# judgesNamesToExclude - Left Join Logic (Ambiguity Fix)
# Requirement: <Surname>, <Forenames> (<Title>) aggregated as a list/string
#######################
from pyspark.sql.functions import col, concat, lit, collect_list, concat_ws, first, trim, array_distinct

def test_judgesNamesToExclude_test1(json_data, M6_bronze):
    try:
        # 1. Create a clean M6 lookup with ALIASED columns to avoid ambiguity
        # Filter for Required = 0 (Excluded)
        m6_clean = M6_bronze.filter((col("Required") == 0) | (col("Required") == False)) \
                            .select(
                                col("CaseNo").alias("m6_CaseNo"), 
                                col("Judge_Surname").alias("m6_Surname"), 
                                col("Judge_Forenames").alias("m6_Forenames"), 
                                col("Judge_Title").alias("m6_Title")
                            ).distinct()

        # 2. Left Join JSON to M6 on Appeal Reference
        test_df = json_data.join(
            m6_clean, 
            json_data.appealReferenceNumber == m6_clean.m6_CaseNo, 
            "left"
        )

        formatted_df = test_df.withColumn(
            "formatted_name_bronze",
            concat(
                trim(col("m6_Surname")), lit(", "), 
                trim(col("m6_Forenames")), lit(" ("), 
                trim(col("m6_Title")), lit(")")
            )
        )

        comparison_df = formatted_df.groupBy("appealReferenceNumber").agg(
            concat_ws(", ", array_distinct(collect_list("formatted_name_bronze"))).alias("expected_string"),
            first("judgesNamesToExclude").alias("actual_field")
        )

        # Acceptance Criteria
        # Handle Omitted Logic: If M6 is empty ("") and JSON is Null, that is a match.
        failures = comparison_df.filter(
            (col("actual_field") != col("expected_string")) & 
            ~((col("actual_field").isNull()) & (col("expected_string") == ""))
        )

        if failures.count() != 0:
            return TestResult(
                "judgesNamesToExclude", 
                "FAIL", 
                f"Found {failures.count()} mismatches between M6 Bronze and JSON output.", 
                "Exclusions", 
                inspect.stack()[0].function
            )
        
        return TestResult(
            "judgesNamesToExclude", 
            "PASS", 
            "Requirement Met: All excluded judges (M6.Required=0) have been correctly formatted as '<Surname>, <Forenames> (<Title>)' and aggregated into the judgesNamesToExclude field.", 
            "Exclusions", 
            inspect.stack()[0].function
        )

    except Exception as e:
        return TestResult("judgesNamesToExclude", "FAIL", f"EXCEPTION: {str(e)[:300]}", "Exclusions", inspect.stack()[0].function)
# updateTribunalDecisionDateRule32 - MAX (StatusId)
# Value: ISO 8601 Standard
#######################

def test_updateTribunalDecisionDateRule32_test1(test_df):
    try:
        # 1. Identify the absolute MAX StatusID record per appeal
        # We do not filter by CaseStatus here because the requirement is simply MAX(StatusId)
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        
        # Rank the records and pick the top 1 (the most recent status)
        winning_records = test_df.withColumn("row_rank", row_number().over(window_spec)) \
                                 .filter(col("row_rank") == 1)

        # 2. Acceptance Criteria:
        # A. Must not be Null or Empty
        # B. Should match ISO 8601 (Basic check for "YYYY-MM-DD" and "T" timestamp separator)
        iso_regex = r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?"
        
        failures = winning_records.filter(
            (col("updateTribunalDecisionDateRule32").isNull()) | 
            (col("updateTribunalDecisionDateRule32") == "") |
            (~col("updateTribunalDecisionDateRule32").rlike(iso_regex))
        )

        if failures.count() != 0:
            return TestResult(
                "updateTribunalDecisionDateRule32", 
                "FAIL", 
                f"Found {failures.count()} rows where date is missing or not ISO 8601 format", 
                test_from_state, 
                inspect.stack()[0].function
            )
        
        return TestResult(
            "updateTribunalDecisionDateRule32", 
            "PASS", 
            "Most recent record correctly contains ISO 8601 date", 
            test_from_state, 
            inspect.stack()[0].function
        )

    except Exception as e:
        return TestResult(
            "updateTribunalDecisionDateRule32", 
            "FAIL", 
            f"EXCEPTION: {str(e)[:300]}", 
            test_from_state, 
            inspect.stack()[0].function
        )




#######################
# ftpaAppellantDecisionRemadeRule32Text - IF M3.Party IS 1 = "This is an ARIA Migrated case..."
# (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_ftpaAppellantDecisionRemadeRule32Text_test1(test_df):
    try:
        # 1. Filter for the FTPA Decided state
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantDecisionRemadeRule32Text", "PASS", "No records found with CaseStatus 39", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 1 must have the specific migration string
        expected_text = "This is an ARIA Migrated case. Please refer to the documents for the notice to set aside."
        
        failures = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaAppellantDecisionRemadeRule32Text") != expected_text)
        )

        if failures.count() != 0:
            return TestResult("ftpaAppellantDecisionRemadeRule32Text", "FAIL", f"Found {failures.count()} Party 1 rows where text was missing or incorrect", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaAppellantDecisionRemadeRule32Text", "PASS", "Party 1 correctly includes: This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantDecisionRemadeRule32Text", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaAppellantDecisionRemadeRule32Text - IF M3.Party IS 2 = OMIT
# (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaAppellantDecisionRemadeRule32Text_test2(test_df):
    try:
        # 1. Filter and identify winning records
        target_records = test_df.filter(col("CaseStatus") == 39)
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 2. Acceptance Criteria: Party 2 must be Omitted (Null)
        failures = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaAppellantDecisionRemadeRule32Text").isNotNull())
        )

        if failures.count() != 0:
            return TestResult("ftpaAppellantDecisionRemadeRule32Text", "FAIL", f"Found {failures.count()} Party 2 rows where Appellant text was not omitted", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaAppellantDecisionRemadeRule32Text", "PASS", "Party 2 correctly omits ftpaAppellantDecisionRemadeRule32Text", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantDecisionRemadeRule32Text", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaRespondentDecisionRemadeRule32Text - IF M3.Party IS 2 = "This is an ARIA Migrated case..."
# (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_ftpaRespondentDecisionRemadeRule32Text_test1(test_df):
    try:
        # 1. Filter for the FTPA Decided state
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentDecisionRemadeRule32Text", "PASS", "No records found with CaseStatus 39", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 2 must have the exact migration string
        expected_text = "This is an ARIA Migrated case. Please refer to the documents for the notice to set aside."
        
        failures = winning_records.filter(
            (col("Party") == 2) & 
            (col("ftpaRespondentDecisionRemadeRule32Text") != expected_text)
        )

        if failures.count() != 0:
            return TestResult("ftpaRespondentDecisionRemadeRule32Text", "FAIL", f"Found {failures.count()} M3.Party 2 rows where text was missing or incorrect", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaRespondentDecisionRemadeRule32Text", "PASS", f"Party 2 correctly includes: {expected_text}", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentDecisionRemadeRule32Text", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaRespondentDecisionRemadeRule32Text - IF M3.Party IS 1 = OMIT
# (MAX StatusID WHERE CaseStatus = 39)
#######################
def test_ftpaRespondentDecisionRemadeRule32Text_test2(test_df):
    try:
        # 1. Filter and identify winning records
        target_records = test_df.filter(col("CaseStatus") == 39)
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 2. Acceptance Criteria: Party 1 must be Omitted (Null)
        failures = winning_records.filter(
            (col("Party") == 1) & 
            (col("ftpaRespondentDecisionRemadeRule32Text").isNotNull())
        )

        if failures.count() != 0:
            return TestResult("ftpaRespondentDecisionRemadeRule32Text", "FAIL", f"Found {failures.count()} M3.Party 1 rows where Respondent text was not omitted", test_from_state, inspect.stack()[0].function)
        
        return TestResult("ftpaRespondentDecisionRemadeRule32Text", "PASS", "M3.Party 1 correctly omits ftpaRespondentDecisionRemadeRule32Text", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentDecisionRemadeRule32Text", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    



#######################
# isFtpaAppellantDecided - MAX (StatusID) WHERE CaseStatus = 39
# Value: Yes
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_isFtpaAppellantDecided_test1(test_df):
    try:
        # 1. Filter for the FTPA Decided state (39)
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantDecided", "PASS", "No records found with CaseStatus 39 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # Acceptance Criteria: 
        # If Party is 1, it MUST be 'Yes'. 
        # If Party is 2, it should be NULL/Omitted.
        failures = winning_records.filter(
            ((col("Party") == 1) & (col("isFtpaAppellantDecided") != "Yes")) |
            ((col("Party") == 2) & (col("isFtpaAppellantDecided").isNotNull()))
        )

        if failures.count() != 0:
            return TestResult("isFtpaAppellantDecided", "FAIL", f"Found {failures.count()} rows where isFtpaAppellantDecided was not 'Yes'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("isFtpaAppellantDecided", "PASS", "All records with CaseStatus 39 correctly set isFtpaAppellantDecided to 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# isFtpaRespondentDecided - MAX (StatusID) WHERE CaseStatus = 39
# Value: Yes
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_isFtpaRespondentDecided_test1(test_df):
    try:
        # 1. Filter for the FTPA Decided state (39)
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentDecided", "PASS", "No records found with CaseStatus 39 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        # This ensures we are only checking the absolute latest update for this state
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # Acceptance Criteria: 
        # If Party is 2, it MUST be 'Yes'. 
        # If Party is 1, it should be NULL/Omitted.
        failures = winning_records.filter(
            ((col("Party") == 2) & (col("isFtpaRespondentDecided") != "Yes")) |
            ((col("Party") == 1) & (col("isFtpaRespondentDecided").isNotNull()))
        )

        if failures.count() != 0:
            return TestResult("isFtpaRespondentDecided", "FAIL", f"Found {failures.count()} rows where isFtpaRespondentDecided was not 'Yes'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("isFtpaRespondentDecided", "PASS", "All records with CaseStatus 39 correctly set isFtpaRespondentDecided to 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaRespondentDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# allFtpaAppellantDecisionDocs - IF M3.Party IS 1 = Include []
#######################
from pyspark.sql.functions import col, row_number, size
from pyspark.sql.window import Window

def test_allFtpaAppellantDecisionDocs_test1(test_df):
    try:
        # 1. Filter for the FTPA state (39)
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Filter for Party 1
        party1_records = winning_records.filter(col("Party") == 1)
        
        if party1_records.count() == 0:
            return TestResult("allFtpaAppellantDecisionDocs", "PASS", "No records found for M3.Party = 1 in Status 39.", test_from_state, inspect.stack()[0].function)

        # 4. Acceptance Criteria: Field must be an empty array (size 0) and NOT NULL
        failures = party1_records.filter(
            (col("allFtpaAppellantDecisionDocs").isNull()) | 
            (size(col("allFtpaAppellantDecisionDocs")) != 0)
        )

        if failures.count() != 0:
            return TestResult("allFtpaAppellantDecisionDocs", "FAIL", f"Found {failures.count()} rows where Party is 1 but allFtpaAppellantDecisionDocs is NULL or not an empty array []", test_from_state, inspect.stack()[0].function)
        
        return TestResult("allFtpaAppellantDecisionDocs", "PASS", "All Party 1 records correctly include an empty array []", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("allFtpaAppellantDecisionDocs", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    



#######################
# allFtpaAppellantDecisionDocs - IF M3.Party IS 2 = OMIT
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_allFtpaAppellantDecisionDocs_test2(test_df):
    try:
        # 1. Filter for the FTPA state (39)
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Filter for Party 2
        party2_records = winning_records.filter(col("Party") == 2)
        
        if party2_records.count() == 0:
            return TestResult("allFtpaAppellantDecisionDocs", "PASS", "No records found for M3.Party = 2 in Status 39.", test_from_state, inspect.stack()[0].function)

        # 4. Acceptance Criteria: Field must be omitted (Null)
        failures = party2_records.filter(col("allFtpaAppellantDecisionDocs").isNotNull())

        if failures.count() != 0:
            return TestResult("allFtpaAppellantDecisionDocs", "FAIL", f"Found {failures.count()} rows where Party is 2 but allFtpaAppellantDecisionDocs was not omitted (NULL)", test_from_state, inspect.stack()[0].function)
        
        return TestResult("allFtpaAppellantDecisionDocs", "PASS", "All Party 2 records correctly omit allFtpaAppellantDecisionDocs", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("allFtpaAppellantDecisionDocs", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# allFtpaRespondentDecisionDocs - IF M3.Party IS 2 = Include []
#######################
from pyspark.sql.functions import col, row_number, size
from pyspark.sql.window import Window

def test_allFtpaRespondentDecisionDocs_test1(test_df):
    try:
        # 1. Filter for the FTPA Decided state (39)
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Filter for Party 2 (Respondent)
        party2_records = winning_records.filter(col("Party") == 2)
        
        if party2_records.count() == 0:
            return TestResult("allFtpaRespondentDecisionDocs", "PASS", "No records found for M3.Party = 2 in Status 39.", test_from_state, inspect.stack()[0].function)

        # 4. Acceptance Criteria: Field must be an empty array (size 0) and NOT NULL
        failures = party2_records.filter(
            (col("allFtpaRespondentDecisionDocs").isNull()) | 
            (size(col("allFtpaRespondentDecisionDocs")) != 0)
        )

        if failures.count() != 0:
            return TestResult("allFtpaRespondentDecisionDocs", "FAIL", f"Found {failures.count()} rows where Party is 2 but allFtpaRespondentDecisionDocs is NULL or not an empty array []", test_from_state, inspect.stack()[0].function)
        
        return TestResult("allFtpaRespondentDecisionDocs", "PASS", "All Party 2 records correctly include an empty array []", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("allFtpaRespondentDecisionDocs", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    




#######################
# allFtpaRespondentDecisionDocs - IF M3.Party IS 1 = OMIT
#######################
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def test_allFtpaRespondentDecisionDocs_test2(test_df):
    try:
        # 1. Filter for the FTPA Decided state (39)
        target_records = test_df.filter(col("CaseStatus") == 39)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Filter for Party 1 (Appellant)
        party1_records = winning_records.filter(col("Party") == 1)
        
        if party1_records.count() == 0:
            return TestResult("allFtpaRespondentDecisionDocs", "PASS", "No records found for M3.Party = 1 in Status 39.", test_from_state, inspect.stack()[0].function)

        # 4. Acceptance Criteria: Field must be omitted (Null)
        failures = party1_records.filter(col("allFtpaRespondentDecisionDocs").isNotNull())

        if failures.count() != 0:
            return TestResult("allFtpaRespondentDecisionDocs", "FAIL", f"Found {failures.count()} rows where Party is 1 but allFtpaRespondentDecisionDocs was not omitted (NULL)", test_from_state, inspect.stack()[0].function)
        
        return TestResult("allFtpaRespondentDecisionDocs", "PASS", "All Party 1 records correctly omit allFtpaRespondentDecisionDocs", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("allFtpaRespondentDecisionDocs", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    



