from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
import inspect

#Import Test Results class
from models.test_result import TestResult

#Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "awaitingRespondentEvidence(a)"




############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "uploadHomeOfficeBundleAvailable",
            "directions"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("defaults", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

def test_defaultValues(test_df, fields_to_exclude):
    try:
        expected_defaults = {
             "uploadHomeOfficeBundleAvailable": "Yes"
        }

        expected_arrays = {
            "directions": None
        }

        failed_field_names = []
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
        return TestResult("DefaultMapping", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)