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
            "actualCaseHearingLength",
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
            "StatusId",
            "Outcome"
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
    

#######################
#actualCaseHearingLength = "actualCaseHearingLength" : {
#     "hours": hoursFromARIA,
#     "minutes": minutesFromARIA
#   }
# MAX(StatusId) WHERE CaseStatus IN (37,38,26) AND Outcome IN (1,2)
#######################
def test_actualCaseHearingLength(test_df):
    try:
        # Filter where CaseStatus is 37/38/26 and Outcome is 1/2
        target_records = test_df.filter(
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1,2))
        )

        if target_records.count() == 0:
            return TestResult("actualCaseHearingLength", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1).select("appealReferenceNumber", "actualCaseHearingLength", "HearingDuration")

        # Extract from struct
        winning_records = winning_records.withColumn(
            "actual_hours", col("actualCaseHearingLength").getItem("hours").cast("int")
        ).withColumn(
            "actual_minutes", col("actualCaseHearingLength").getItem("minutes").cast("int")
        )

        # Expected Values from M3 HearingDuration
        winning_records = winning_records.withColumn(
            "expected_hours", F.floor(col("HearingDuration") / 60)
        ).withColumn(
            "expected_minutes", col("HearingDuration") % 60
        )

        # 4. Validation
        acceptance_criteria = winning_records.filter(
            (F.col("actual_hours") != F.col("expected_hours")) | 
            (F.col("actual_minutes") != F.col("expected_minutes"))
        )

        if acceptance_criteria.count() > 0:
            return TestResult("actualCaseHearingLength", "FAIL", f"actualCaseHearingLength acceptance criteria failed: found {acceptance_criteria.count()} cases where actualCaseHearingLength does not match with M3.HearingDuration", test_from_state, inspect.stack()[0].function)
            return
        else:
            return TestResult("actualCaseHearingLength", "PASS", "actualCaseHearingLength acceptance criteria passed: all actualCaseHearingLength match with M3.HearingDuration", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("actualCaseHearingLength", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


#######################
#ftpaApplicationDeadline Init code
#######################
def test_ftpaApplicationDeadline_init(json, M3_bronze, C):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "ftpaApplicationDeadline"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "DecisionDate",
            "CaseStatus",
            "StatusId",
            "Outcome"
        )

        C = C.select(
            col("CaseNo").alias("C_CaseNo"),
            "CategoryId"
        )

        test_df = test_df.join(
            M3_bronze,
            test_df["appealReferenceNumber"] == M3_bronze["CaseNo"],
            "inner"
        ).join(
            C,
            test_df["appealReferenceNumber"] == C["C_CaseNo"],
            "inner"
        ).drop(
            C["C_CaseNo"]
        )

        test_df = test_df.groupBy("appealReferenceNumber").agg(
            collect_list("CategoryId").alias("CategoryIds"),
            first("CategoryId").alias("CategoryId"),
            first("ftpaApplicationDeadline").alias("ftpaApplicationDeadline"),
            first("DecisionDate").alias("DecisionDate"),
            first("CaseStatus").alias("CaseStatus"),
            first("StatusId").alias("StatusId"),
            first("Outcome").alias("Outcome")
        )

        # CategoryId filter
        categoryid_rule = ( 
            (array_contains(col("CategoryIds"), 37)) |
            (array_contains(col("CategoryIds"), 38))
        )

        # Apply filters for CaseStatus, Outcome and CategoryID
        test_df = test_df.filter(
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1,2)) &
            (categoryid_rule) &
            (col("CategoryId").isin(37, 38))
        )

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        test_df = test_df.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("hearingDetails", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    
#######################
#ftpaApplicationDeadline 
# Where CategoryId in 37, ftpaApplicationDeadline = M3.DecisionDate + 14 days
# Where CategoryId in 38, ftpaApplicationDeadline = M3.DecisionDate + 28 days
# MAX(StatusId) WHERE CaseStatus IN (37,38,26) AND Outcome IN (1,2)
#######################
def test_ftpaApplicationDeadline_ac1(test_df):
    try:
        latest_window = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusId"))

        winning_df = test_df.withColumn("row_rank", F.row_number().over(latest_window)).filter(F.col("row_rank") == 1)

        winning_df = winning_df.withColumn(
            "expected_deadline_37",
            F.when(F.col("CategoryId") == 37, F.date_add(F.to_date("DecisionDate"), 14))
            .otherwise(None)
        )

        acceptance_criteria = winning_df.filter(
            F.to_date(col("ftpaApplicationDeadline")) != col("expected_deadline_37")
        )

        if acceptance_criteria.count() > 0:
            return TestResult("ftpaApplicationDeadline", "FAIL", f"actualCaseHearingLength acceptance criteria failed: found {acceptance_criteria.count()} cases where where CategoryId is 37 and ftpaApplicationDeadline does not match with M3.DecisionDate + 14 days", test_from_state, inspect.stack()[0].function)
            return
        else:
            return TestResult("ftpaApplicationDeadline", "PASS", "ftpaApplicationDeadline acceptance criteria passed: all ftpaApplicationDeadline, CategoryId is 37 and  match with M3.DecisionDate + 14 days", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaApplicationDeadline", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

def test_ftpaApplicationDeadline_ac2(test_df):
    try:
        latest_window = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusId"))

        winning_df = test_df.withColumn("row_rank", F.row_number().over(latest_window)).filter(F.col("row_rank") == 1)

        winning_df = winning_df.withColumn(
            "expected_deadline_38",
            F.when(F.col("CategoryId") == 38, F.date_add(F.to_date("DecisionDate"), 28))
            .otherwise(None)
        )

        acceptance_criteria = winning_df.filter(
            F.to_date(col("ftpaApplicationDeadline")) != col("expected_deadline_38")
        )

        if acceptance_criteria.count() > 0:
            return TestResult("ftpaApplicationDeadline", "FAIL", f"actualCaseHearingLength acceptance criteria failed: found {acceptance_criteria.count()} cases where CategoryId is 38 and ftpaApplicationDeadline does not match with M3.DecisionDate + 28 days", test_from_state, inspect.stack()[0].function)
            return
        else:
            return TestResult("ftpaApplicationDeadline", "PASS", "ftpaApplicationDeadline acceptance criteria passed: all ftpaApplicationDeadline, CategoryId is 38 match with M3.DecisionDate + 28 days", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaApplicationDeadline", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)