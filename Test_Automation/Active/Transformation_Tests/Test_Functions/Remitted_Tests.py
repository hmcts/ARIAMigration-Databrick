from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    date_format, row_number, expr,
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp, transform, first, array_contains
)
from pyspark.sql.window import Window
import inspect

#Import Test Results class
from models.test_result import TestResult

#Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "remitted"

############################################################################################
#######################
# default mapping Init code
#######################

def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "rehearingReason",
            "sourceOfRemittal",
            "courtReferenceNumber",
            "caseFlagSetAsideReheardExists",
            "remittalDocuments",
            "uploadOtherRemittalDocs"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

def test_remitted_defaultValues(test_df, fields_to_exclude):
    try:
        expected_defaults = {
            "rehearingReason": "Remitted",
            "sourceOfRemittal": "Upper Tribunal",
            "courtReferenceNumber": "This is a migrated ARIA case. Please refer to the documents.",
            "caseFlagSetAsideReheardExists": "Yes"
        }

        expected_arrays = {
           "remittalDocuments": None,
           "uploadOtherRemittalDocs": None
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
# remitted Init code
#######################

def test_remittalDetails_init(json, M3_bronze):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "appealRemittedDate"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "CaseStatus",
            "StatusId",
            "Outcome",
            "DecisionDate"
        )

        test_df = test_df.join(
            M3_bronze,
            test_df["appealReferenceNumber"] == M3_bronze["CaseNo"],
            "inner"
        ).drop(M3_bronze["CaseNo"])
        
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("remittalDetails", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)


############################################################################################
#######################
# appealRemittedDate
#######################

def test_appealRemittedDate_test1(test_df):
    try:
        # 1. Filter only for the Remitted States (42, 43, 44) and Outcome 86
        target_records = test_df.filter(
            (col("CaseStatus").isin(42, 43, 44)) & (col("Outcome") == 86)
        ) 
        
        if target_records.count() == 0:
            return TestResult("appealRemittedDate", "FAIL", "NO RECORDS TO TEST (No Remitted Status/Outcome found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Define the expected date format logic (ISO 8601: yyyy-MM-dd)
        # We cast DecisionDate to a timestamp and format it to match the Gold field string
        expected_date_format = date_format(col("DecisionDate").cast("timestamp"), "yyyy-MM-dd")

        # 4. Acceptance Criteria: Check if the appealRemittedDate matches the source DecisionDate
        acceptance_criteria = winning_records.filter(
            col("appealRemittedDate") != expected_date_format
        )

        if acceptance_criteria.count() != 0:
            return TestResult("appealRemittedDate", "FAIL", f"appealRemittedDate acceptance criteria failed: found {acceptance_criteria.count()} rows where date does not match DecisionDate or ISO 8601 format", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("appealRemittedDate", "PASS", "appealRemittedDate acceptance criteria pass: all rows match the expected MAX DecisionDate in yyyy-MM-dd format", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("appealRemittedDate", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)




