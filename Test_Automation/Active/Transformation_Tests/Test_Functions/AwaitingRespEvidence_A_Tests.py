from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list,
    max as spark_max, date_format, row_number, expr,
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import inspect

#Import Test Results class
from models.test_result import TestResult

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
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

def test_AREA_defaultValues(test_df, fields_to_exclude):
    try:
        expected_defaults = {
             "uploadHomeOfficeBundleAvailable": "Yes"
        }

        expected_arrays = {
            "directions": None
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
#homeoffice negative test code
# #######################

def test_ARE_homeOfficeExclusion(json_data):
    # Post-ARIADM-2357: for PA/RP appeals the 5 homeOffice search/match fields are DEFAULTED
    # (inherited from appealSubmitted); for every other appeal type they are omitted (null).
    # (Was a 2205 negative "must be excluded" test; 2357 + mapping v1.17 supersede it.)
    try:
        results_list = []

        if "appealType" not in json_data.columns:
            return [TestResult("homeOfficeExclusion", "NO_DATA", "appealType not in payload - cannot evaluate HO defaulting", test_from_state, inspect.stack()[0].function)]

        is_pa_rp = col("appealType").isin("protection", "revocationOfProtection")

        # Scalar fields: PA/RP -> fixed default; all other appeal types -> null
        scalar_defaults = {
            "homeOfficeSearchStatus": "SUCCESS",
            "homeOfficeSearchNoMatch": "NO_MATCH",
            "matchingAppellantDetailsFound": "No",
        }
        for field, expected in scalar_defaults.items():
            if field not in json_data.columns:
                results_list.append(TestResult(field, "NO_DATA", f"{field} not present in this state's payload", test_from_state, inspect.stack()[0].function))
                continue
            bad = json_data.filter(
                (is_pa_rp & ~col(field).eqNullSafe(expected)) |
                (~is_pa_rp & col(field).isNotNull())
            )
            n = bad.count()
            if n > 0:
                results_list.append(TestResult(field, "FAIL", f"{field}: {n} cases not matching 2357 default (PA/RP must be '{expected}', all others null).", test_from_state, inspect.stack()[0].function))
            else:
                results_list.append(TestResult(field, "PASS", f"{field} correct: '{expected}' for PA/RP, null otherwise.", test_from_state, inspect.stack()[0].function))

        # Struct fields: PA/RP -> present (appellantsList value.code = 'NoMatch'); all other appeal types -> null
        for field, subcol, subval in [
            ("homeOfficeAppellantsList", "homeOfficeAppellantsList.value.code", "NoMatch"),
            ("homeOfficeCaseStatusData", None, None),
        ]:
            if field not in json_data.columns:
                results_list.append(TestResult(field, "NO_DATA", f"{field} not present in this state's payload", test_from_state, inspect.stack()[0].function))
                continue
            if subcol:
                bad = json_data.filter(
                    (is_pa_rp & ~col(subcol).eqNullSafe(subval)) |
                    (~is_pa_rp & col(field).isNotNull())
                )
                detail = f"PA/RP must have {subcol}='{subval}', all others null"
            else:
                bad = json_data.filter(
                    (is_pa_rp & col(field).isNull()) |
                    (~is_pa_rp & col(field).isNotNull())
                )
                detail = "PA/RP must be populated, all others null"
            n = bad.count()
            if n > 0:
                results_list.append(TestResult(field, "FAIL", f"{field}: {n} cases not matching 2357 default ({detail}).", test_from_state, inspect.stack()[0].function))
            else:
                results_list.append(TestResult(field, "PASS", f"{field} correct ({detail}).", test_from_state, inspect.stack()[0].function))

        return results_list

    except Exception as e:
        error_message = str(e)
        return [TestResult("homeOfficeExclusion", "FAIL", f"homeOffice test exception: {error_message[:100]}", test_from_state, inspect.stack()[0].function)]


############################################################################################
#######################
#appellant details init code
#######################
def test_appellant_details_init(json, M2_bronze):
    try:
        json = json.select(
            "appealReferenceNumber",
            "appellantFullName"
        )
        M2_bronze = M2_bronze.select(
            "CaseNo",
            "Appellant_Forenames",
            "Appellant_Name"
        )
        test_df = json.join(
            M2_bronze,
            json["appealReferenceNumber"] == M2_bronze["CaseNo"],
            "inner"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("appellantDetails", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
 

# appellantFullName - Where M2.AppellantForenames + ' ' + M2.AppellantName = = appellantFullName    
#######################

def test_appellantFullName_test1(test_df):
    try:
        if test_df.filter(
        (col("Appellant_Forenames").isNotNull()) & 
        (col("Appellant_Name").isNotNull())   
        ).count() == 0:
            return TestResult("appellantFullName", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)
        
        acceptance_criteria = test_df.filter(
            (col("Appellant_Forenames")) + " " + (col("Appellant_Name")) != (col("appellantFullName"))
        )

        if acceptance_criteria.count() != 0:
            return TestResult("appellantFullName", "FAIL", f"appellantFullName acceptance criteria failed: found{acceptance_criteria.count()}cases where AppellantForenames + AppellantName = appellantFullName", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("appellantFullName", "PASS", "appellantFullName acceptance criteria passed: found all cases where AppellantForenames + AppellantName = appellantFullName", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("appellantFullName", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)


#######################
# recordedOutOfTimeDecision (awaitingRespondentEvidence(a) onwards)
# Rule: IF M1.OutOfTimeIssue == 1 -> "Yes" ELSE omitted
# Outcome is not considered — once a case has progressed past pp/as, the out-of-time decision is assumed made.
# M1 is effectively one row per CaseNo, so no window or M3 join is needed.
#######################
def test_recordedOutOfTimeDecision_ac1(json, M1_bronze):
    try:
        try:
            test_df = json.join(
                M1_bronze,
                json["appealReferenceNumber"] == M1_bronze["CaseNo"],
                "inner"
            ).select(
                "appealReferenceNumber",
                "recordedOutOfTimeDecision",
                "OutOfTimeIssue"
            ).dropDuplicates(["appealReferenceNumber"])
        except Exception as e:
            error_message = str(e)
            return TestResult("recordedOutOfTimeDecision", "FAIL",f"Failed to setup test data, no data exists for 'recordedOutOfTimeDecision'. Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

        if test_df != None:
            if test_df.filter(col("OutOfTimeIssue") == 1).count() == 0:
                return TestResult("recordedOutOfTimeDecision", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

            ac = test_df.filter(
                (col("OutOfTimeIssue") == 1) &
                ((col("recordedOutOfTimeDecision") != "Yes") | (col("recordedOutOfTimeDecision").isNull()))
            )

            if ac.count() != 0:
                return TestResult("recordedOutOfTimeDecision", "FAIL", f"recordedOutOfTimeDecision acceptance criteria failed: {str(ac.count())} cases have been found where M1.OutOfTimeIssue == 1 but recordedOutOfTimeDecision != 'Yes'", test_from_state, inspect.stack()[0].function)
            else:
                return TestResult("recordedOutOfTimeDecision", "PASS", f"recordedOutOfTimeDecision acceptance criteria passed, all cases where M1.OutOfTimeIssue == 1 have recordedOutOfTimeDecision = 'Yes'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("recordedOutOfTimeDecision", "FAIL",f"Failed to Setup Data for Test - recordedOutOfTimeDecision does not exist in the payload", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("recordedOutOfTimeDecision", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)


def test_recordedOutOfTimeDecision_ac2(json, M1_bronze):
    try:
        try:
            test_df = json.join(
                M1_bronze,
                json["appealReferenceNumber"] == M1_bronze["CaseNo"],
                "inner"
            ).select(
                "appealReferenceNumber",
                "recordedOutOfTimeDecision",
                "OutOfTimeIssue"
            ).dropDuplicates(["appealReferenceNumber"])
        except Exception as e:
            error_message = str(e)
            return TestResult("recordedOutOfTimeDecision", "FAIL",f"Failed to setup test data, no data exists for 'recordedOutOfTimeDecision'. Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

        if test_df != None:
            if test_df.filter(
                (col("OutOfTimeIssue").isNull()) | (col("OutOfTimeIssue") != 1)
                ).count() == 0:
                return TestResult("recordedOutOfTimeDecision", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

            ac = test_df.filter(
                ((col("OutOfTimeIssue").isNull()) | (col("OutOfTimeIssue") != 1)) &
                (col("recordedOutOfTimeDecision").isNotNull())
            )

            if ac.count() != 0:
                return TestResult("recordedOutOfTimeDecision", "FAIL", f"recordedOutOfTimeDecision acceptance criteria failed: {str(ac.count())} cases have been found where M1.OutOfTimeIssue != 1 but recordedOutOfTimeDecision is not omitted", test_from_state, inspect.stack()[0].function)
            else:
                return TestResult("recordedOutOfTimeDecision", "PASS", f"recordedOutOfTimeDecision acceptance criteria passed, all cases where M1.OutOfTimeIssue != 1 have recordedOutOfTimeDecision omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("recordedOutOfTimeDecision", "FAIL",f"Failed to Setup Data for Test - recordedOutOfTimeDecision does not exist in the payload", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("recordedOutOfTimeDecision", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)       
   
   
   