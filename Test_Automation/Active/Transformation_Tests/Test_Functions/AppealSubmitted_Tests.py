from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
import inspect

#Import Test Results class
from models.test_result import TestResult

#Not working to set default value,
# TestResult.DEFAULT_TEST_FROM_STATE = "appealSubmitted"

#Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "appealSubmitted"

def as_testcase1():    
    return TestResult("as_testcasefield", "FAIL", f"AppealSubmitted - test 1 complete",test_from_state, inspect.stack()[0].function)

def as_testcase2():    
    return TestResult("as_testcasefield", "FAIL", f"AppealSubmitted - test 2 complete",test_from_state, inspect.stack()[0].function)

############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "paAppealTypePaymentOption",
            "paAppealTypeAipPaymentOption",
            "additionalPaymentInfo"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("defaults", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state)

def test_defaultValues(test_df):
    expected_defaults = {
        "paAppealTypePaymentOption": "payLater",
        "paAppealTypeAipPaymentOption": "payLater",
        "additionalPaymentInfo": "This is an ARIA Migrated Case. The payment was made in ARIA and the payment history can be found in the case notes."
    }

    failed_field_names = []
    results_list = []

    for field, expected in expected_defaults.items():
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

