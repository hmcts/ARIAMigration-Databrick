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



