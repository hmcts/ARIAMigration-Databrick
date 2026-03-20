from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
import inspect
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#Import Test Results class
from models.test_result import TestResult

#Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "decided(a)"

############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "anonymityOrder",
            "appealDecisionAvailable",
            "finalDecisionAndReasonsDocuments"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

def test_dec_a_defaultValues(test_df, fields_to_exclude):
    try:
        expected_defaults = {
            "anonymityOrder": "No",
            "appealDecisionAvailable": "Yes"

        }

        expected_arrays = {
           "finalDecisionAndReasonsDocuments": None
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
#substantiveDecision Init code
#######################
def test_substantiveDecision_init(json, M3_bronze):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "sendDecisionsAndReasonsDate",
            "appealDate",
            "appealDecision",
            "isDecisionAllowed"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "DecisionDate",
            "Outcome",
            "CaseStatus",
            "StatusId"
        )

        test_df = test_df.join(
            M3_bronze,
            test_df["appealReferenceNumber"] == M3_bronze["CaseNo"],
            "inner"
        )
        
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("hearingDetails", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)


#######################
#sendDecisionsAndReasonsDate code - Ensure M3.DecisionDate and sendDecisionsAndReasonsDate are equal
# MAX(StatusId) WHERE CaseStatus IN (37,38,26) AND Outcome IN (1,2)
#######################
def test_sendDecisionsAndReasonsDate(test_df):
    try:
        # Filter where CaseStatus is 37/38/26 and Outcome is 1/2
        target_records = test_df.filter(
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1,2))
        )

        if target_records.count() == 0:
            return TestResult("sendDecisionsAndReasonsDate", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        winning_records = winning_records.select("appealReferenceNumber", "sendDecisionsAndReasonsDate", "DecisionDate", "CaseStatus", "Outcome")

        acceptance_critera = winning_records.filter(
            col("sendDecisionsAndReasonsDate") != col("DecisionDate")
        )

        if acceptance_critera.count() > 0:
            return TestResult("sendDecisionsAndReasonsDate", "FAIL", f"sendDecisionsAndReasonsDate acceptance criteria failed: found {acceptance_critera.count()} mismatches between DecisionDate and sendDecisionsAndReasonsDate", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("sendDecisionsAndReasonsDate", "PASS", "sendDecisionsAndReasonsDate acceptance criteria passed: all sendDecisionsAndReasonsDate values correctly match DecisionDate", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("sendDecisionsAndReasonsDate", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
#appealDate code - Ensure M3.DecisionDate and appealDate are equal
# MAX(StatusId) WHERE CaseStatus IN (37,38,26) AND Outcome IN (1,2)
#######################
def test_appealDate(test_df):
    try:
        # Filter where CaseStatus is 37/38/26 and Outcome is 1/2
        target_records = test_df.filter(
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1,2))
        )

        if target_records.count() == 0:
            return TestResult("appealDate", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        winning_records = winning_records.select("appealReferenceNumber", "appealDate", "DecisionDate", "CaseStatus", "Outcome")

        acceptance_critera = winning_records.filter(
            col("appealDate") != col("DecisionDate")
        )

        if acceptance_critera.count() > 0:
            return TestResult("appealDate", "FAIL", f"appealDate acceptance criteria failed: found {acceptance_critera.count()} mismatches between DecisionDate and appealDate", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("appealDate", "PASS", "appealDate acceptance criteria passed: all appealDate values correctly match DecisionDate", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("appealDate", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
#appealDecision code - Where M3.Outcome = 1 & appealDecision = ‘Allowed’
# MAX(StatusId) WHERE CaseStatus IN (37,38,26) AND Outcome IN (1,2)
#######################
def test_appealDecision_ac1(test_df):
    try:
        # Filter where CaseStatus is 37/38/26 and Outcome is 1/2
        target_records = test_df.filter(
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1,2))
        )

        if target_records.count() == 0:
            return TestResult("appealDecision", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        winning_records = winning_records.select("appealReferenceNumber", "appealDecision", "Outcome")

        acceptance_critera = winning_records.filter(
            (col("Outcome") == 1) & (col("appealDecision") != "Allowed")
        )

        if acceptance_critera.count() > 0:
            return TestResult("appealDecision", "FAIL", f"appealDecision acceptance criteria failed: found {acceptance_critera.count()} cases where M3.Outcome = 1 & appealDecision != Allowed", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("appealDecision", "PASS", "appealDecision acceptance criteria passed: all appealDate cases where M3.Outcome = 1 have appealDecision = Allowed", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("appealDecision", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
#appealDecision code - Where M3.Outcome = 2 & appealDecision = ‘‘Dismissed’’
# MAX(StatusId) WHERE CaseStatus IN (37,38,26) AND Outcome IN (1,2)
#######################
def test_appealDecision_ac2(test_df):
    try:
        # Filter where CaseStatus is 37/38/26 and Outcome is 1/2
        target_records = test_df.filter(
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1,2))
        )

        if target_records.count() == 0:
            return TestResult("appealDecision", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        winning_records = winning_records.select("appealReferenceNumber", "appealDecision", "Outcome")

        acceptance_critera = winning_records.filter(
            (col("Outcome") == 2) & (col("appealDecision") != "Dismissed")
        )

        if acceptance_critera.count() > 0:
            return TestResult("appealDecision", "FAIL", f"appealDecision acceptance criteria failed: found {acceptance_critera.count()} cases where M3.Outcome = 2 & appealDecision != Dismissed", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("appealDecision", "PASS", "appealDecision acceptance criteria passed: all appealDate cases where M3.Outcome = 2 have appealDecision = Dismissed", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("appealDecision", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


#######################
#isDecisionAllowed code - Where M3.Outcome = 1 & isDecisionAllowed = ‘allowed’
# MAX(StatusId) WHERE CaseStatus IN (37,38,26) AND Outcome IN (1,2)
#######################
def test_isDecisionAllowed_ac1(test_df):
    try:
        # Filter where CaseStatus is 37/38/26 and Outcome is 1/2
        target_records = test_df.filter(
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1,2))
        )

        if target_records.count() == 0:
            return TestResult("isDecisionAllowed", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        winning_records = winning_records.select("appealReferenceNumber", "isDecisionAllowed", "Outcome")

        acceptance_critera = winning_records.filter(
            (col("Outcome") == 1) & (col("isDecisionAllowed") != "allowed")
        )

        if acceptance_critera.count() > 0:
            return TestResult("isDecisionAllowed", "FAIL", f"isDecisionAllowed acceptance criteria failed: found {acceptance_critera.count()} cases where M3.Outcome = 1 & isDecisionAllowed != allowed", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isDecisionAllowed", "PASS", "appealDecision acceptance criteria passed: all appealDate cases where M3.Outcome = 1 have isDecisionAllowed = allowed", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isDecisionAllowed", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
#isDecisionAllowed code - Where M3.Outcome = 2 & isDecisionAllowed = ‘dismissed’
# MAX(StatusId) WHERE CaseStatus IN (37,38,26) AND Outcome IN (1,2)
#######################
def test_isDecisionAllowed_ac2(test_df):
    try:
        # Filter where CaseStatus is 37/38/26 and Outcome is 1/2
        target_records = test_df.filter(
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1,2))
        )

        if target_records.count() == 0:
            return TestResult("isDecisionAllowed", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        winning_records = winning_records.select("appealReferenceNumber", "isDecisionAllowed", "Outcome")

        acceptance_critera = winning_records.filter(
            (col("Outcome") == 2) & (col("isDecisionAllowed") != "dismissed")
        )

        if acceptance_critera.count() > 0:
            return TestResult("isDecisionAllowed", "FAIL", f"isDecisionAllowed acceptance criteria failed: found {acceptance_critera.count()} cases where M3.Outcome = 2 & isDecisionAllowed != dismissed", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isDecisionAllowed", "PASS", "isDecisionAllowed acceptance criteria passed: all appealDate cases where M3.Outcome = 2 have isDecisionAllowed = Dismissed", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isDecisionAllowed", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


############################################################################################
#######################
#hearingActuals Init code
#######################
def test_hearingActuals_init(json, M3_bronze):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "attendingJudge",
            "ftpaApplicationDeadline"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "Adj_Determination_Title",
            "Adj_Determination_Forenames",
            "Adj_Determination_Surname",
            "HearingDuration",
            "DecisionDate",
            "CaseStatus",
            "StatusId"
        )

        test_df = test_df.join(
            M3_bronze,
            test_df["appealReferenceNumber"] == M3_bronze["CaseNo"],
            "inner"
        )
        
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("hearingDetails", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
#attendingJudge = M3.Adj_Determination_Title + ' ' + M3.Adj_Determination_Forenames + ' ' +  M3.Adj_Determination_Surname
# MAX(StatusId) WHERE CaseStatus IN (37,38,26)
#######################
def test_attendingJudge(test_df):
    try:
        # Filter for CaseStatus in 26, 37, 38
        target_records = test_df.filter(F.col("CaseStatus").isin(37, 38, 26))

        if target_records.count() == 0:
            return TestResult("attendingJudge", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Partition by Appeal, Order by desc StatusId, where there is name
        status_window = Window.partitionBy("appealReferenceNumber").orderBy(
            F.when(F.col("Adj_Determination_Surname").isNotNull(), 1).otherwise(0).desc(),
            F.desc("StatusId")
        )

        # Select most relevant records
        winning_records = target_records.withColumn("rank", F.row_number().over(status_window)) \
            .filter(F.col("rank") == 1) \
            .withColumn(
                "expected_judge_name", 
                F.regexp_replace(
                    F.trim(F.concat_ws(" ", "Adj_Determination_Title", "Adj_Determination_Forenames", "Adj_Determination_Surname")),
                    r'\s+', ' '
                )
            )

        # Replace Non-Breaking Spaces (\u00A0) with normal spaces
        # Replace multiple whitespaces (\s+) with a single space
        # Trim
        def remove_spaces(col):
            return F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.col(col), r'\u00A0', ' '), 
                    r'\s+', 
                    ' '
                )
            )

        # Test
        acceptance_criteria = winning_records.filter(
            remove_spaces("attendingJudge") != remove_spaces("expected_judge_name")
        )

        if acceptance_criteria.count() > 0:
            return TestResult("attendingJudge", "FAIL", f"attendingJudge acceptance criteria failed: found {acceptance_criteria.count()} cases where attendingJudge does not match with M3 fields", test_from_state, inspect.stack()[0].function)
            return
        else:
            return TestResult("attendingJudge", "PASS", "attendingJudge acceptance criteria passed: all attendingJudge fields match with M3 fields", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("attendingJudge", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
       