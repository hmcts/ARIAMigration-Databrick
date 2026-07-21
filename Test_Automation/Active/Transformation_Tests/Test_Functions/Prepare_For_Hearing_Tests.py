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
            "isAdditionalInstructionAllowed": "Yes",
            "witness1InterpreterSignLanguage": "{}",
            "witness2InterpreterSignLanguage": "{}",
            "witness3InterpreterSignLanguage": "{}",
            "witness4InterpreterSignLanguage": "{}",
            "witness5InterpreterSignLanguage": "{}",
            "witness6InterpreterSignLanguage": "{}",
            "witness7InterpreterSignLanguage": "{}",
            "witness8InterpreterSignLanguage": "{}",
            "witness9InterpreterSignLanguage": "{}",
            "witness10InterpreterSignLanguage": "{}",
            "witness1InterpreterSpokenLanguage": "{}",
            "witness2InterpreterSpokenLanguage": "{}",
            "witness3InterpreterSpokenLanguage": "{}",
            "witness4InterpreterSpokenLanguage": "{}",
            "witness5InterpreterSpokenLanguage": "{}",
            "witness6InterpreterSpokenLanguage": "{}",
            "witness7InterpreterSpokenLanguage": "{}",
            "witness8InterpreterSpokenLanguage": "{}",
            "witness9InterpreterSpokenLanguage": "{}",
            "witness10InterpreterSpokenLanguage": "{}"
        }
        
        expected_arrays = {
    
            "witnessDetails": None,
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
            "ListTypeId",
            "CaseStatus",
            "StatusId"
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
# MAX(StatusId) WHERE CaseStatus IN (37,38)
#######################
def test_isAppealSuitableToFloat_test1(test_df):
    try:
        test_df = test_df.filter(col("CaseStatus").isin(37,38))
        
        #Check we have Records To test
        if test_df.count() == 0:
            return TestResult("isAppealSuitableToFloat", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)
        
        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        test_df = test_df.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        test_df = test_df.select("appealReferenceNumber", "isAppealSuitableToFloat", "ListTypeId", "CaseStatus")

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
        test_df = test_df.filter(col("CaseStatus").isin(37,38))
        
        #Check we have Records To test
        if test_df.count() == 0:
            return TestResult("isAppealSuitableToFloat", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)
        
        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        test_df = test_df.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        test_df = test_df.select("appealReferenceNumber", "isAppealSuitableToFloat", "ListTypeId", "CaseStatus")

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
def test_hearingDetails_init(json, M1_bronze, M3_bronze, bll):
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
            "HearingCentre",
            "TimeEstimate",
            "CaseStatus",
            "StatusId"
        )

        test_df = test_df.join(
            M1_bronze,
            json["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        ).join(
            M3_bronze,
            json["appealReferenceNumber"] == M3_bronze["CaseNo"],
            "inner"
        ).join(
            bll,
            M3_bronze["HearingCentre"] == bll["ListedCentre"],
            "left"
        ).drop(M1_bronze["CaseNo"], M3_bronze["CaseNo"])

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("hearingResponse", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state,inspect.stack()[0].function)

#######################
# hearingChannel - If VisitVisaType is 1 and channelCode = "ONPPRS", channelLabel = "On the Papers"
#######################
def test_hearingChannel_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("VisitVisaType") == 1
            ).count() == 0:
            return TestResult("hearingChannel", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            col("VisitVisaType") == 1
        ) &
        (
            (col("hearingChannel.code") != "ONPPRS") |
            (col("hearingChannel.label") != "On The Papers")   
        ))

        if acceptance_critera.count() != 0:
            return TestResult("hearingChannel","FAIL", f"hearingChannel acceptance criteria failed: found {acceptance_critera.count()} rows where VisitVisaType is 1 and channelCode != ONPPRS, channelLabel != On the Papers", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("hearingChannel","PASS", "hearingChannel acceptance criteria pass: all rows VisitVisaType is 1 have channelCode = ONPPRS, channelLabel = On the Papers", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("hearingChannel", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)


#######################
# hearingChannel - If VisitVisaType is 2 and channelCode = "INTER", channelLabel = "In Person"
#######################
def test_hearingChannel_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("VisitVisaType") == 2
            ).count() == 0:
            return TestResult("hearingChannel", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            col("VisitVisaType") == 2
        ) &
        (
            (col("hearingChannel.code") != "INTER") |
            (col("hearingChannel.label") != "In Person")   
        ))

        if acceptance_critera.count() != 0:
            return TestResult("hearingChannel","FAIL", f"hearingChannel acceptance criteria failed: found {acceptance_critera.count()} rows where VisitVisaType is 1 and channelCode != INTER, channelLabel != In Person", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("hearingChannel","PASS", "hearingChannel acceptance criteria pass: all rows VisitVisaType is 1 have channelCode = INTER, channelLabel = In Person", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("hearingChannel", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    
#Migrated from decsion due to change
#######################
#hearingDetails Init code
#######################
def test_hearingDetails_init2(json, M3_bronze, bll):
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
            "StatusId"
        )

        test_df = test_df.join(
            M3_bronze,
            test_df["appealReferenceNumber"] == M3_bronze["CaseNo"],
            "inner"
        ).join(
            bll,
            M3_bronze["HearingCentre"] == bll["ListedCentre"],
            "left"
        ).drop(M3_bronze["CaseNo"]
        ).drop(bll["listCaseHearingCentre"]
        ).drop(bll["listCaseHearingCentreAddress"]
        )
        
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
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusId"))

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
    

#######################
#listCaseHearingCentre & listCaseHearingCentreAddress code
#######################
def test_listCaseHearingCentre_Address(test_df, spark):
    try:
        # Filter where CaseStatus is 37/38
        target_records = test_df.filter(col("CaseStatus").isin(37, 38))

        if target_records.count() == 0:
            return TestResult("listCaseHearingCentre, listCaseHearingCentreAddress", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        winning_records = winning_records.select("appealReferenceNumber", "HearingCentre", "listCaseHearingCentre", "listCaseHearingCentreAddress")

        # Mapping requirements
        mapping_data = [
            ("Alloa Sheriff Court", "alloaSherrif", "Alloa Sheriff Court, 47 Drysdale Street, Alloa, FK10 1JA"),
            ("Belfast - Laganside", "belfast", "Belfast Laganside Court, 45 Donegall Quay, BT1 3LL"),
            ("Birmingham IAC (Priory Courts)", "birmingham", "Birmingham Civil And Family Justice Centre, Priory Courts, 33 Bull Street, B4 6DS"),
            ("Birmingham IAC Sheldon Court", "birmingham", "Birmingham Civil And Family Justice Centre, Priory Courts, 33 Bull Street, B4 6DS"),
            ("Bradford", "bradford", "Bradford Tribunal Hearing Centre, Rushton Avenue, BD3 7BH"),
            ("Bradford Magistrates Court", "bradfordKeighley", "Bradford and Keighley Magistrates Court and Family Court, The Tyrls, PO Box 187, BD1 1JL"),
            ("Coventry Magistrates' Court IAC", "coventry", "Coventry Magistrates Court, Little Park Street, CV1 2SQ"),
            ("Glasgow (Eagle Building)", "glasgowTribunalsCentre", "Atlantic Quay - Glasgow, 20 York Street, Glasgow, G2 8GT"),
            ("Glasgow (Tribunals Centre)", "glasgowTribunalsCentre", "Atlantic Quay - Glasgow, 20 York Street, Glasgow, G2 8GT"),
            ("Harmondsworth", "harmondsworth", "Harmondsworth Tribunal Hearing Centre, Colnbrook Bypass, UB7 0HB"),
            ("Harmondsworth (HX)", "harmondsworth", "Harmondsworth Tribunal Hearing Centre, Colnbrook Bypass, UB7 0HB"),
            ("Hatton Cross", "hattonCross", "Hatton Cross Tribunal Hearing Centre, York House And Wellington House, 2-3 Dukes Green, Feltham, Middlesex, TW14 0LS"),
            ("Hendon Magistrates Court (HX)", "hendon", "Hendon Magistrates Court, The Court House, The Hyde, NW9 7BY"),
            ("Hendon Magistrates Court (TH)", "hendon", "Hendon Magistrates Court, The Court House, The Hyde, NW9 7BY"),
            ("Manchester (Piccadilly)", "manchester", "Manchester Tribunal Hearing Centre - Piccadilly Exchange, Piccadilly Plaza, M1 4AH"),
            ("Newcastle CFCTC", "newcastle", "Newcastle Civil And Family Courts And Tribunals Centre, Barras Bridge, Newcastle-Upon-Tyne, NE1 8QF"),
            ("Newport (Columbus House)", "newport", "Newport Tribunal Centre - Columbus House, Langstone Business Park, Newport, NP18 2LX"),
            ("North Tyneside Magistrates Court", "nthTyneMags", "North Tyneside Magistrates Court, Tynemouth Road, The Court House, NE30 1AG"),
            ("Nottingham Justice Centre", "nottingham", "Nottingham Magistrates Court, Carrington Street, NG2 1EE"),
            ("Taylor House", "taylorHouse", "Taylor House Tribunal Hearing Centre, Rosebery Avenue, EC1R 4QU"),
            ("Yarl's Wood", "yarlsWood", "Yarls Wood Immigration And Asylum Hearing Centre, Twinwood Road, MK44 1FD")
        ]

        schema = StructType([
            StructField("ref_HearingCentre", StringType(), True),
            StructField("ref_CentreCode", StringType(), True),
            StructField("ref_Address", StringType(), True)
        ])
        
        # Create reference dataframe
        ref_df = spark.createDataFrame(mapping_data, schema)
        
        # Comparison dataframe
        comparison_df = winning_records.join(
            ref_df, 
            winning_records["HearingCentre"] == ref_df["ref_HearingCentre"], 
            "left"
        )

        def clean_address(col_name):
            return F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.col(col_name), r'\u00A0', ' '), # Handle non-breaking spaces
                    r'\s+', ' '                                       # Squash multiple spaces to one
                )
            )

        acceptance_critera = comparison_df.filter(
            (F.col("ref_HearingCentre").isNotNull()) & 
            (
                (F.col("listCaseHearingCentre") != F.col("ref_CentreCode")) | 
                (clean_address("listCaseHearingCentreAddress") != clean_address("ref_Address"))
            )
        )

        if acceptance_critera.count() > 0:
            return TestResult("listCaseHearingCentre,listCaseHearingCentreAddress", "FAIL", f"listCaseHearingCentre,listCaseHearingCentreAddress acceptance criteria failed: found {acceptance_critera.count()} mismatches between listCaseHearingCentre,listCaseHearingCentreAddress and M3.HearingCentre and address", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("listCaseHearingCentre,listCaseHearingCentreAddress", "PASS", "listCaseHearingDate acceptance criteria passed: all listCaseHearingCentre,listCaseHearingCentreAddress and M3.HearingCentre and address values correctly match", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("listCaseHearingCentre,listCaseHearingCentreAddress", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################

#######################
# listingLocation
#######################
def test_listingLocation_mapping(test_df):
    try:
        target_records = test_df.filter(col("CaseStatus").isin(37, 38))

        if target_records.count() == 0:
            return TestResult("listingLocation", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.col("StatusId").desc(), F.col("TimeEstimate").desc())

        # Apply rank and grab the most relevant row for each CaseNumber
        ranked_df = target_records.withColumn("row_rank", F.row_number().over(window_spec))
        winning_records = ranked_df.filter(F.col("row_rank") == 1)

        collapsed_df = winning_records.groupBy("appealReferenceNumber").agg(
            F.max("listingLocation").alias("listingLocation"),
            F.max("listingLocation.code").alias("actual_code"),
            F.max("listingLocation.label").alias("actual_label"),
            F.max("locationCode").alias("expected_code"),
            F.max("locationLabel").alias("expected_label"),
            F.max("ListedCentre").alias("ListedCentre") # To ensure we have a ListedCentre
        )

        acceptance_critera = collapsed_df.filter(
            (F.col("ListedCentre").isNotNull()) & 
            (
                (F.col("actual_code") != F.col("expected_code")) | 
                (F.col("actual_label") != F.col("expected_label"))
            )
        )

        if acceptance_critera.count() > 0:
            return TestResult("listingLocation","FAIL", f"listingLocation acceptance criteria failed: found {acceptance_critera.count()} rows where ListedCentre and listingLocation do not match", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("listingLocation","PASS", "listingLocation acceptance criteria pass: all rows have matching ListedCentre and listingLocation rows", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("listingLocation", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# listingLength
#######################
def test_listingLength_mapping(test_df):
    try:
        # Filter CaseStatus = 37 or 38
        target_records = test_df.filter(F.col("CaseStatus").isin(37, 38))

        if target_records.count() == 0:
            return TestResult("listingLength", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Partition by appealReferenceNumber
        # Order by Status DESC (38 above 37)
        # Order by TimeEstimate DESC (120 above 60 if status is tied)
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.col("StatusId").desc(), F.col("TimeEstimate").desc())

        # Apply rank and grab the most relevant row for each CaseNumber
        ranked_df = target_records.withColumn("row_rank", F.row_number().over(window_spec))
        winning_records = ranked_df.filter(F.col("row_rank") == 1)

        # Extract JSON and test expected values from
        base_df = winning_records.select(
            "appealReferenceNumber",
            col("listingLength"),
            col("listingLength.hours").alias("actual_hours"),
            col("listingLength.minutes").alias("actual_minutes"),
            col("TimeEstimate").alias("source_mins")
        )

        raw_mins = F.col("source_mins").cast("int") % 60
        base_hrs = F.floor(F.col("source_mins").cast("int") / 60)

        final_comparison_df = base_df.withColumn(
            "expected_hours", 
            F.when(F.col("source_mins").isNull() | (F.col("source_mins") == 0), F.lit(0).cast("long"))
             .when(raw_mins >= 45, base_hrs + 1)
             .otherwise(base_hrs).cast("long")
        ).withColumn(
            "expected_minutes",
            F.when(F.col("source_mins").isNull() | (F.col("source_mins") == 0), F.lit(30).cast("long"))
             .when(raw_mins == 0, F.lit(0))
             .when(raw_mins < 45, F.lit(30))
             .otherwise(F.lit(0)).cast("long")
        )
        
        acceptance_critera = final_comparison_df.filter(
            (F.col("actual_hours") != F.col("expected_hours")) | 
            (F.col("actual_minutes") != F.col("expected_minutes"))
        )

        if acceptance_critera.count() > 0:
            return TestResult("listingLength","FAIL", f"listingLength acceptance criteria failed: found {acceptance_critera.count()} CaseNos where listingLength and TimeEstimate do not match", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("listingLength","PASS", "listingLength acceptance criteria pass: all rows have matching listingLength and TimeEstimate rows", test_from_state, inspect.stack()[0].function)


    except Exception as e:
        return TestResult("listingLength", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
