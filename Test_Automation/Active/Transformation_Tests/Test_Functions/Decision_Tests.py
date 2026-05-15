from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
import inspect

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

#Import Test Results class
from models.test_result import TestResult

#Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "decision"

############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "scheduleOfIssuesAgreement",
            "scheduleOfIssuesDisagreementDescription",
            "immigrationHistoryAgreement",
            "immigrationHistoryDisagreementDescription",
            "hmcts",
            "stitchingStatus",
            "bundleConfiguration",
            "decisionAndReasonsAvailable",
            "caseBundles"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

def test_dec_defaultValues(test_df, fields_to_exclude):
    try:
        expected_defaults = {
            "scheduleOfIssuesAgreement": "No",
            "scheduleOfIssuesDisagreementDescription": "This is a migrated ARIA case. Please see the documents for information on the schedule of issues.",
            "immigrationHistoryAgreement": "No",
            "immigrationHistoryDisagreementDescription": "This is a migrated ARIA case. Please see the documents for information on the immigration history.",
            "stitchingStatus": "DONE",
            "bundleConfiguration": "iac-hearing-bundle-config.yaml",
            "decisionAndReasonsAvailable": "No"

        }

        expected_arrays = {
        #    "hmcts":[userImage:hmcts.png],
           "caseBundles": None
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
#general Init code
#######################
def test_general_init(json, M1_bronze, M2_bronze):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "bundleFileNamePrefix"
        )

        M1_bronze = M1_bronze.select(
            "CaseNo"
        )

        M2_bronze = M2_bronze.select(
            col("CaseNo").alias("M2_CaseNo"),
            "Appellant_Name"
        )

        test_df = test_df.join(
            M1_bronze,
            test_df["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        ).join(
            M2_bronze,
            test_df["appealReferenceNumber"] == M2_bronze["M2_CaseNo"],
            "inner"
        ).drop("M2_CaseNo")
                
        
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("hearingDetails", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
#bundleFileNamePrefix - Concatenate to the format 'CaseNo-Appellant_Name' replacing the '/' from CaseNo with ' ' (space) e.g "XX 00000 0000-Smith"
#######################
def test_bundleFileNamePrefix(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("bundleFileNamePrefix").isNotNull()
            ).count() == 0:
            return TestResult("bundleFileNamePrefix", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)
        
        final_df = test_df.withColumn(
            "expected_prefix",
            F.concat(
                F.regexp_replace(F.col("appealReferenceNumber"), "/", " "),
                F.lit("-"),
                F.col("Appellant_Name")
            )
        )

        acceptance_criteria = final_df.filter(
            col("bundleFileNamePrefix") != col("expected_prefix")
        )

        if acceptance_criteria.count() != 0:
            return TestResult("bundleFileNamePrefix","FAIL", f"bundleFileNamePrefix acceptance criteria failed: found {acceptance_criteria.count()} rows where bundleFileNamePrefix does not match with the required format: 'CaseNo-Appellant_Name'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("bundleFileNamePrefix","PASS", "bundleFileNamePrefix acceptance criteria pass: all rows have bundleFileNamePrefix matching with the required format: 'CaseNo-Appellant_Name'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("bundleFileNamePrefix", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 










