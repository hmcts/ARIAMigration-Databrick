from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
import inspect

#Import Test Results class
from models.test_result import TestResult

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

############################################################################################
#######################
#hearing response Init code
#######################
def test_hearingResponse_init(json, M1_bronze, M3_bronze):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "isAppealSuitableToFloat",
            "isInCameraCourtAllowed",
            "inCameraCourtTribunalResponse",
            "inCameraCourtDecisionForDisplay",
            "isSingleSexCourtAllowed",
            "singleSexCourtTribunalResponse",
            "singleSexCourtDecisionForDisplay",
            "additionalInstructionsTribunalResponse"
        )

        M1_bronze = M1_bronze.select(
            "CaseNo",
            "InCamera",
            "CourtPreference"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "ListTypeId"
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
        return None,TestResult("hearingResponse", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state,inspect.stack()[0].function)

#######################
# isAppealSuitableToFloat - If M3.ListTypeId is 5 & isAppealSuitableToFloat is ‘Yes’
#######################
def test_isAppealSuitableToFloat_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("ListTypeId") == 5
            ).count() == 0:
            return TestResult("isAppealSuitableToFloat", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (col("ListTypeId") == 5) &
            (col("isAppealSuitableToFloat") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isAppealSuitableToFloat","FAIL", f"isAppealSuitableToFloat acceptance criteria failed: found {acceptance_critera.count()} rows where M3.ListTypeId is 5 & isAppealSuitableToFloat is not Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isAppealSuitableToFloat","PASS", "isAppealSuitableToFloat acceptance criteria pass: all rows where M3.ListTypeId is 5 have isAppealSuitableToFloat equals Yes", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("isAppealSuitableToFloat", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# isAppealSuitableToFloat - If M3.ListTypeId is not 5 & isAppealSuitableToFloat is 'No'
#######################
def test_isAppealSuitableToFloat_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("ListTypeId") != 5
            ).count() == 0:
            return TestResult("isAppealSuitableToFloat", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (col("ListTypeId") != 5) &
            (col("isAppealSuitableToFloat") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isAppealSuitableToFloat","FAIL", f"isAppealSuitableToFloat acceptance criteria failed: found {acceptance_critera.count()} rows where M3.ListTypeId is not 5 & isAppealSuitableToFloat is not No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isAppealSuitableToFloat","PASS", "isAppealSuitableToFloat acceptance criteria pass: all rows where M3.ListTypeId is not 5 have isAppealSuitableToFloat equals No", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("isAppealSuitableToFloat", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# isAppealSuitableToFloat - If isAppealSuitableToFloat is only either ‘Yes’ or ‘No’
#######################
def test_isAppealSuitableToFloat_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("isAppealSuitableToFloat").isNotNull()
            ).count() == 0:
            return TestResult("isAppealSuitableToFloat", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        yes_no = test_df.filter(
            (col("isAppealSuitableToFloat") == "Yes") |
            (col("isAppealSuitableToFloat") == "No")
        )

        acceptance_critera = test_df.count() - yes_no.count()

        if acceptance_critera != 0:
            return TestResult("isAppealSuitableToFloat","FAIL", f"isAppealSuitableToFloat acceptance criteria failed: found {acceptance_critera} rows isAppealSuitableToFloat is not only either ‘Yes’ or ‘No’", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isAppealSuitableToFloat","PASS", "isAppealSuitableToFloat acceptance criteria pass: all rows have isAppealSuitableToFloat only either ‘Yes’ or ‘No’", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("isAppealSuitableToFloat", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# isInCameraCourtAllowed - If M1.InCamera is 1 & isInCameraCourtAllowed is Granted
#######################
def test_isInCameraCourtAllowed_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("InCamera") == 1
            ).count() == 0:
            return TestResult("isInCameraCourtAllowed", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (col("InCamera") == 1) &
            (col("isInCameraCourtAllowed") != "Granted")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isInCameraCourtAllowed","FAIL", f"isInCameraCourtAllowed acceptance criteria failed: found {acceptance_critera.count()} rows where M1.InCamera is 1 & isInCameraCourtAllowed is not Granted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isInCameraCourtAllowed","PASS", "isInCameraCourtAllowed acceptance criteria pass: all rows where M1.InCamera is 1 have isInCameraCourtAllowed is Granted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("isInCameraCourtAllowed", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# isInCameraCourtAllowed - If M1.InCamera is not 1 & isInCameraCourtAllowed is Refused
#######################
def test_isInCameraCourtAllowed_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("InCamera") != 1
            ).count() == 0:
            return TestResult("isInCameraCourtAllowed", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (col("InCamera") != 1) &
            (col("isInCameraCourtAllowed") != "Refused")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isInCameraCourtAllowed","FAIL", f"isInCameraCourtAllowed acceptance criteria failed: found {acceptance_critera.count()} rows where M1.InCamera is not 1 & isInCameraCourtAllowed is not Refused", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isInCameraCourtAllowed","PASS", "isInCameraCourtAllowed acceptance criteria pass: all rows where M1.InCamera is not 1 have isInCameraCourtAllowed is Refused", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("isInCameraCourtAllowed", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# inCameraCourtTribunalResponse - If M1.InCamera is 1 & inCameraCourtTribunalResponse is ‘“This is a migrated ARIA case. Please refer to the documents.”’
#######################
def test_inCameraCourtTribunalResponse_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("InCamera") == 1
            ).count() == 0:
            return TestResult("inCameraCourtTribunalResponse", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (col("InCamera") == 1) &
            (col("inCameraCourtTribunalResponse") != "This is a migrated ARIA case. Please refer to the documents.")
        )

        if acceptance_critera.count() != 0:
            return TestResult("inCameraCourtTribunalResponse","FAIL", f"inCameraCourtTribunalResponse acceptance criteria failed: found {acceptance_critera.count()} rows where M1.InCamera is 1 & inCameraCourtTribunalResponse is not: This is a migrated ARIA case. Please refer to the documents.", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("inCameraCourtTribunalResponse","PASS", "inCameraCourtTribunalResponse acceptance criteria pass: all rows where M1.InCamera is 1 have inCameraCourtTribunalResponse is not: This is a migrated ARIA case. Please refer to the documents.", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("inCameraCourtTribunalResponse", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# inCameraCourtTribunalResponse - If M1.InCamera is not 1 & isInCameraCourtAllowed is omitted 
#######################
def test_inCameraCourtTribunalResponse_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("InCamera") != 1
            ).count() == 0:
            return TestResult("inCameraCourtTribunalResponse", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (col("InCamera") != 1) &
            (col("inCameraCourtTribunalResponse").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("inCameraCourtTribunalResponse","FAIL", f"inCameraCourtTribunalResponse acceptance criteria failed: found {acceptance_critera.count()} rows where M1.InCamera is not 1 & inCameraCourtTribunalResponse is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("inCameraCourtTribunalResponse","PASS", "inCameraCourtTribunalResponse acceptance criteria pass: all rows where M1.InCamera is not 1 have inCameraCourtTribunalResponse omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("inCameraCourtTribunalResponse", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 
    
#######################
# inCameraCourtDecisionForDisplay - If M1.InCamera is 1 & inCameraCourtDecisionForDisplay is ‘“This is a migrated ARIA case. Please refer to the documents.”’
#######################
def test_inCameraCourtDecisionForDisplay_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("InCamera") == 1
            ).count() == 0:
            return TestResult("inCameraCourtDecisionForDisplay", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (col("InCamera") == 1) &
            (col("inCameraCourtDecisionForDisplay") != "Granted - This is a migrated ARIA case. Please refer to the documents.")
        )

        if acceptance_critera.count() != 0:
            return TestResult("inCameraCourtDecisionForDisplay","FAIL", f"inCameraCourtDecisionForDisplay acceptance criteria failed: found {acceptance_critera.count()} rows where M1.InCamera is 1 & inCameraCourtDecisionForDisplay is not: This is a migrated ARIA case. Please refer to the documents.", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("inCameraCourtDecisionForDisplay","PASS", "inCameraCourtDecisionForDisplay acceptance criteria pass: all rows where M1.InCamera is 1 have inCameraCourtDecisionForDisplay is not: Granted - This is a migrated ARIA case. Please refer to the documents.", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("inCameraCourtDecisionForDisplay", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# inCameraCourtDecisionForDisplay - If M1.InCamera is not 1 & inCameraCourtDecisionForDisplay is omitted 
#######################
def test_inCameraCourtDecisionForDisplay_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("InCamera") != 1
            ).count() == 0:
            return TestResult("inCameraCourtDecisionForDisplay", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (col("InCamera") != 1) &
            (col("inCameraCourtDecisionForDisplay").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("inCameraCourtDecisionForDisplay","FAIL", f"inCameraCourtDecisionForDisplay acceptance criteria failed: found {acceptance_critera.count()} rows where M1.InCamera is not 1 & inCameraCourtDecisionForDisplay is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("inCameraCourtDecisionForDisplay","PASS", "inCameraCourtDecisionForDisplay acceptance criteria pass: all rows where M1.InCamera is not 1 have inCameraCourtDecisionForDisplay omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("inCameraCourtDecisionForDisplay", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 
    
#######################
# isSingleSexCourtAllowed - If M1.CourtPreference is 1 or 2 & isSingleSexCourtAllowed = Granted
#######################
def test_isSingleSexCourtAllowed_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference") == 1) | 
            (col("CourtPreference") == 2) 
            ).count() == 0:
            return TestResult("isSingleSexCourtAllowed", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("CourtPreference") == 1) | 
            (col("CourtPreference") == 2) 
        )&
            (col("isSingleSexCourtAllowed") != "Granted")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isSingleSexCourtAllowed","FAIL", f"isSingleSexCourtAllowed acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference is 1 or 2 & isSingleSexCourtAllowed != Granted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isSingleSexCourtAllowed","PASS", "isSingleSexCourtAllowed acceptance criteria pass: all rows where M1.CourtPreference is 1 or 2 have isSingleSexCourtAllowed = Granted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("isSingleSexCourtAllowed", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# isSingleSexCourtAllowed - If M1.CourtPreference is not 1 or 2 & isSingleSexCourtAllowed = Refused
#######################
def test_isSingleSexCourtAllowed_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference") != 1) | 
            (col("CourtPreference") != 2) 
            ).count() == 0:
            return TestResult("isSingleSexCourtAllowed", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (~col("CourtPreference").isin(1, 2)) &
            (col("isSingleSexCourtAllowed") != "Refused")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isSingleSexCourtAllowed","FAIL", f"isSingleSexCourtAllowed acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference is not 1 or 2 & isSingleSexCourtAllowed != Refused", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isSingleSexCourtAllowed","PASS", "isSingleSexCourtAllowed acceptance criteria pass: all rows where M1.CourtPreference is not 1 or 2 have isSingleSexCourtAllowed = Refused", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("isSingleSexCourtAllowed", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# singleSexCourtTribunalResponse - If M1.CourtPreference is 1 or 2 & singleSexCourtTribunalResponse = ‘“This is a migrated ARIA case. Please refer to the documents.”’
#######################
def test_singleSexCourtTribunalResponse_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference") == 1) | 
            (col("CourtPreference") == 2) 
            ).count() == 0:
            return TestResult("singleSexCourtTribunalResponse", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("CourtPreference") == 1) | 
            (col("CourtPreference") == 2) 
        )&
            (col("singleSexCourtTribunalResponse") != "This is a migrated ARIA case. Please refer to the documents.")
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourtTribunalResponse","FAIL", f"singleSexCourtTribunalResponse acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference is 1 or 2 & singleSexCourtTribunalResponse != This is a migrated ARIA case. Please refer to the documents.", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourtTribunalResponse","PASS", "singleSexCourtTribunalResponse acceptance criteria pass: all rows where M1.CourtPreference is 1 or 2 have singleSexCourtTribunalResponse = This is a migrated ARIA case. Please refer to the documents.", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourtTribunalResponse", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# singleSexCourtTribunalResponse - If M1.CourtPreference is not 1 or 2 & singleSexCourtTribunalResponse is omitted
#######################
def test_singleSexCourtTribunalResponse_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference") != 1) | 
            (col("CourtPreference") != 2) 
            ).count() == 0:
            return TestResult("singleSexCourtTribunalResponse", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (~col("CourtPreference").isin(1, 2)) &
            (col("singleSexCourtTribunalResponse").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourtTribunalResponse","FAIL", f"singleSexCourtTribunalResponse acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference is not 1 or 2 & singleSexCourtTribunalResponse is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourtTribunalResponse","PASS", "singleSexCourtTribunalResponse acceptance criteria pass: all rows where where M1.CourtPreference is not 1 or 2 & singleSexCourtTribunalResponse is omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourtTribunalResponse", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# singleSexCourtDecisionForDisplay - If M1.CourtPreference is 1 or 2 & singleSexCourtDecisionForDisplay = ‘"Granted - This is a migrated ARIA case. Please refer to the documents."’
#######################
def test_singleSexCourtDecisionForDisplay_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference") == 1) | 
            (col("CourtPreference") == 2) 
            ).count() == 0:
            return TestResult("singleSexCourtTribunalResponse", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("CourtPreference") == 1) | 
            (col("CourtPreference") == 2) 
        )&
            (col("singleSexCourtDecisionForDisplay") != "Granted - This is a migrated ARIA case. Please refer to the documents.")
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourtDecisionForDisplay","FAIL", f"singleSexCourtDecisionForDisplay acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference is 1 or 2 & singleSexCourtDecisionForDisplay != Granted - This is a migrated ARIA case. Please refer to the documents.", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourtDecisionForDisplay","PASS", "singleSexCourtDecisionForDisplay acceptance criteria pass: all rows where M1.CourtPreference is 1 or 2 have singleSexCourtDecisionForDisplay = Granted - This is a migrated ARIA case. Please refer to the documents.", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourtDecisionForDisplay", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# singleSexCourtDecisionForDisplay - If M1.CourtPreference is not 1 or 2 & singleSexCourtDecisionForDisplay is omitted
#######################
def test_singleSexCourtDecisionForDisplay_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference") != 1) | 
            (col("CourtPreference") != 2) 
            ).count() == 0:
            return TestResult("singleSexCourtDecisionForDisplay", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (~col("CourtPreference").isin(1, 2)) &
            (col("singleSexCourtDecisionForDisplay").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourtDecisionForDisplay","FAIL", f"singleSexCourtDecisionForDisplay acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference is not 1 or 2 & singleSexCourtDecisionForDisplay is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourtDecisionForDisplay","PASS", "singleSexCourtDecisionForDisplay acceptance criteria pass: all rows where where M1.CourtPreference is not 1 or 2 & singleSexCourtDecisionForDisplay is omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourtDecisionForDisplay", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
#######################
#hearing details Init code
#######################
def test_hearingDetails_init(json, M1_bronze, M3_bronze):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "hearingChannel",
            "listingLocation",
            "listingLength"
        )

        M1_bronze = M1_bronze.select(
            "CaseNo",
            "VisitVisaType"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "HearingCentre"
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
        return None,TestResult("hearingResponse", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state,inspect.stack()[0].function)




