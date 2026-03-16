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
test_from_state = "prepareForHearing"


############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "isRemoteHearing",
            "isMultimediaAllowed",
            "multimediaTribunalResponse",
            "multimediaDecisionForDisplay",
            "isVulnerabilitiesAllowed",
            "vulnerabilitiesTribunalResponse",
            "vulnerabilitiesDecisionForDisplay",
            "isRemoteHearingAllowed",
            "remoteVideoCallTribunalResponse",
            "remoteHearingDecisionForDisplay",
            "isAdditionalAdjustmentsAllowed",
            "additionalTribunalResponse",
            "otherDecisionForDisplay",
            "isAdditionalInstructionAllowed",
            "witnessDetails",
            "witness1InterpreterSignLanguage",
            "witness2InterpreterSignLanguage",
            "witness3InterpreterSignLanguage",
            "witness4InterpreterSignLanguage",
            "witness5InterpreterSignLanguage",
            "witness6InterpreterSignLanguage",
            "witness7InterpreterSignLanguage",
            "witness8InterpreterSignLanguage",
            "witness9InterpreterSignLanguage",
            "witness10InterpreterSignLanguage",
            "witness1InterpreterSpokenLanguage",
            "witness2InterpreterSpokenLanguage",
            "witness3InterpreterSpokenLanguage",
            "witness4InterpreterSpokenLanguage",
            "witness5InterpreterSpokenLanguage",
            "witness6InterpreterSpokenLanguage",
            "witness7InterpreterSpokenLanguage",
            "witness8InterpreterSpokenLanguage",
            "witness9InterpreterSpokenLanguage",
            "witness10InterpreterSpokenLanguage",
            "hearingDocuments",
            "letterBundleDocuments"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

def test_pFH_defaultValues(test_df, fields_to_exclude):
    try:
        expected_defaults = {
            "isRemoteHearing": "No",
            "isMultimediaAllowed": "Granted",
            "multimediaTribunalResponse": "This is a migrated ARIA case. Please refer to the documents.",
            "multimediaDecisionForDisplay": "Granted - This is a migrated ARIA case. Please refer to the documents.",
            "isVulnerabilitiesAllowed": "Granted",
            "vulnerabilitiesTribunalResponse": "This is a migrated ARIA case. Please refer to the documents.",
            "vulnerabilitiesDecisionForDisplay": "Granted - This is a migrated ARIA case. Please refer to the documents.",
            "isRemoteHearingAllowed": "Granted",
            "remoteVideoCallTribunalResponse": "This is a migrated ARIA case. Please refer to the documents.",
            "remoteHearingDecisionForDisplay": "Granted - This is a migrated ARIA case. Please refer to the documents.",
            "isAdditionalAdjustmentsAllowed": "Granted",
            "additionalTribunalResponse": "This is a migrated ARIA case. Please refer to the documents.",
            "otherDecisionForDisplay": "Granted - This is a migrated ARIA case. Please refer to the documents.",
            "isAdditionalInstructionAllowed": "Yes"
            }

        expected_arrays = {
            "witnessDetails": None,
            "witness1InterpreterSignLanguage": None,
            "witness2InterpreterSignLanguage": None,
            "witness3InterpreterSignLanguage": None,
            "witness4InterpreterSignLanguage": None,
            "witness5InterpreterSignLanguage": None,
            "witness6InterpreterSignLanguage": None,
            "witness7InterpreterSignLanguage": None,
            "witness8InterpreterSignLanguage": None,
            "witness9InterpreterSignLanguage": None,
            "witness10InterpreterSignLanguage": None,
            "witness1InterpreterSpokenLanguage": None,
            "witness2InterpreterSpokenLanguage": None,
            "witness3InterpreterSpokenLanguage": None,
            "witness4InterpreterSpokenLanguage": None,
            "witness5InterpreterSpokenLanguage": None,
            "witness6InterpreterSpokenLanguage": None,
            "witness7InterpreterSpokenLanguage": None,
            "witness8InterpreterSpokenLanguage": None,
            "witness9InterpreterSpokenLanguage": None,
            "witness10InterpreterSpokenLanguage": None,
            "hearingDocuments": None,
            "letterBundleDocuments": None
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

