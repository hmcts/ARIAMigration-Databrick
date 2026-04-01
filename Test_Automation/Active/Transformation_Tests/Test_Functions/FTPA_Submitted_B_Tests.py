from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)

from pyspark.sql import functions as F
from pyspark.sql.functions import col, row_number, concat_ws, lit
from pyspark.sql.window import Window
import inspect

# Import Test Results class
from models.test_result import TestResult

# Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "ftpaSubmitted(b)"

############################################################################################
#######################
# default mapping Init code
#######################

def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "judgeAllocationExists"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

def test_ftpa_b_defaultValues(test_df, fields_to_exclude):
    try:
        expected_defaults = {
            "judgeAllocationExists": "Yes"
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
            "allocatedJudge",
            "allocatedJudgeEdit"
        )

        M1_bronze = M1_bronze.select(
            "CaseNo"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "CaseStatus",
            "StatusId",
            "Adj_Title",
            "Adj_Forenames",
            "Adj_Surname"
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
# allocatedJudge - Adj_Title + ' ' + Adj_Forenames + ' ' + Adj_Surname (MAX StatusID WHERE CaseStatus = 39)
#######################

def test_allocatedJudge_test1(test_df):
    try:
        # 1. Filter only for the Appeal State (ftpaSubmitted(b))
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("allocatedJudge", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Define the expected concatenation logic
        # concat_ws(' ', ...) ensures a single space between each non-null name part
        expected_name_format = concat_ws(" ", col("Adj_Title"), col("Adj_Forenames"), col("Adj_Surname"))

        # 4. Acceptance Criteria: Check if the field matches the concatenated string
        acceptance_critera = winning_records.filter(
            col("allocatedJudge") != expected_name_format
        )

        if acceptance_critera.count() != 0:
            return TestResult("allocatedJudge", "FAIL", f"allocatedJudge acceptance criteria failed: found {acceptance_critera.count()} rows where name format does not match 'Title Forenames Surname'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("allocatedJudge", "PASS", "allocatedJudge acceptance criteria pass: all rows match the expected 'Title Forenames Surname' format", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("allocatedJudge", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# allocatedJudgeEdit - Adj_Title + ' ' + Adj_Forenames + ' ' + Adj_Surname (MAX StatusID WHERE CaseStatus = 39)
#######################
from pyspark.sql.functions import col, row_number, concat_ws
from pyspark.sql.window import Window

def test_allocatedJudgeEdit_test1(test_df):
    try:
        # 1. Filter only for the Appeal State (ftpaSubmitted(b))
        target_records = test_df.filter(col("CaseStatus") == 39) 
        if target_records.count() == 0:
            return TestResult("allocatedJudgeEdit", "FAIL", "NO RECORDS TO TEST (No CaseStatus 39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Define the expected concatenation logic
        # concat_ws handles nulls gracefully and inserts a single space between parts
        expected_name_format = concat_ws(" ", col("Adj_Title"), col("Adj_Forenames"), col("Adj_Surname"))

        # 4. Acceptance Criteria: Check if the field matches the concatenated string
        acceptance_critera = winning_records.filter(
            col("allocatedJudgeEdit") != expected_name_format
        )

        if acceptance_critera.count() != 0:
            return TestResult("allocatedJudgeEdit", "FAIL", f"allocatedJudgeEdit acceptance criteria failed: found {acceptance_critera.count()} rows where name format does not match 'Title Forenames Surname'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("allocatedJudgeEdit", "PASS", "allocatedJudgeEdit acceptance criteria pass: all rows match the expected 'Title Forenames Surname' format", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("allocatedJudgeEdit", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
