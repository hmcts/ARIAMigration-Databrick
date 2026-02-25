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
test_from_state = "caseUnderReview"

def testcase1():    
    return TestResult("testcasefield", "FAIL", f"caseUnderReview - test 1 complete", test_from_state, inspect.stack()[0].function)

def testcase2():    
    return TestResult("testcasefield", "PASS", f"caseUnderReview - test 2 complete", test_from_state, inspect.stack()[0].function)



############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json, M1_silver):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "caseArgumentAvailable"
        )

        M1_silver = M1_silver.select(
            "CaseNo",
            "dv_representation"
        )

        test_df = test_df.join(
            M1_silver,
            json["appealReferenceNumber"] == M1_silver["CaseNo"],
            "inner"
        ).drop(M1_silver["CaseNo"])

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state,inspect.stack()[0].function)

def test_defaultValues(test_df):
    try:
        results_list = []

        acceptance_critera_lr = test_df.filter(
            ((col("dv_representation") == "LR") & (col("caseArgumentAvailable") != "Yes"))
        )

        if acceptance_critera_lr.count() != 0:
            results_list.append(TestResult(
                "caseArgumentAvailable", 
                "FAIL", 
                f"Failed to check Default Mapping for : caseArgumentAvailable - expected : 'Yes' - found {acceptance_critera_lr.count()} records not matching", 
                test_from_state,
                inspect.stack()[0].function
            ))
        else:
            results_list.append(TestResult(
                "caseArgumentAvailable", 
                "PASS", 
                f"Checked Default Mapping for : caseArgumentAvailable - found correct value", 
                test_from_state,
                inspect.stack()[0].function
            ))
            
        return results_list
    except Exception as e:
        error_message = str(e)        
        return [TestResult("DefaultMapping", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)]