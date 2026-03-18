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
#hearingDetails Init code
#######################
def test_hearingDetails_init(json, M3_bronze, bll):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "listCaseHearingLength",
            "listCaseHearingDate",
            "listCaseHearingCentre",
            "listCaseHearingCentreAddress"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "TimeEstimate",
            "CaseStatus",
            "HearingCentre",
            "HearingDate",
            "StartTime",
            "StatusID"
        )

        test_df = test_df.join(
            M3_bronze,
            test_df["appealReferenceNumber"] == M3_bronze["CaseNo"],
            "inner"
        ).join(
            bll,
            M3_bronze["HearingCentre"] == bll["ListedCentre"],
            "left"
        ).drop(M3_bronze["CaseNo"]).drop(bll["listCaseHearingCentre"])
        
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("hearingDetails", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
#listCaseHearingLength code
#######################
def test_listCaseHearingLength(test_df):
    try:
        # Filter where CaseStatus is 37/38
        target_records = test_df.filter(col("CaseStatus").isin(37, 38))

        if target_records.count() == 0:
            return TestResult("listingLength", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        # Rounding: divide by 30, round to nearest whole number, then multiply by 30
        final_df = winning_records.withColumn(
            "calculated_length", (F.round(F.col("TimeEstimate") / 30) * 30).cast("int")
        ).withColumn(
            "expected_length", F.greatest(F.lit(30), F.col("calculated_length"))
        )

        acceptance_critera = final_df.filter(F.col("listCaseHearingLength") != F.col("expected_length"))

        if acceptance_critera.count() > 0:
            return TestResult("listCaseHearingLength", "FAIL", f"listCaseHearingLength acceptance criteria failed: found {acceptance_critera.count()} rounding mismatches. JSON does not match nearest 30min increment.", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("listCaseHearingLength", "PASS", "listCaseHearingLength acceptance criteria passed: all listCaseHearingDate values correctly rounded to nearest 30m increment from Max Status row.", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("listCaseHearingLength", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
#listCaseHearingDate code
#######################
def test_listCaseHearingDate(test_df):
    try:
        # Filter where CaseStatus is 37/38
        target_records = test_df.filter(col("CaseStatus").isin(37, 38))

        if target_records.count() == 0:
            return TestResult("listCaseHearingDate", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        final_df = winning_records.withColumn("expected_hearing_date", 
        F.to_timestamp(
            F.concat(
                F.date_format(F.col("HearingDate"), "yyyy-MM-dd"), 
                F.lit(" "), 
                F.date_format(F.col("StartTime"), "HH:mm:ss")
            )
        ))

        acceptance_critera = final_df.filter(
            F.col("listCaseHearingDate").cast("timestamp") != F.col("expected_hearing_date")
        )

        if acceptance_critera.count() > 0:
            return TestResult("listCaseHearingDate", "FAIL", f"listCaseHearingDate acceptance criteria failed: found {acceptance_critera.count()} mismatches between listCaseHearingDate and M3.HearingDate and StartTime", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("listCaseHearingDate", "PASS", "listCaseHearingDate acceptance criteria passed: all listCaseHearingDate values correctly match between listCaseHearingDate and M3.HearingDate and StartTime", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("listCaseHearingDate", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

