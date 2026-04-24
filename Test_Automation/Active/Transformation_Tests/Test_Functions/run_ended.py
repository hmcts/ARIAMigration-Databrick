import Test_Functions.Ended_Tests as ended_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


import Test_Functions.Ended_Tests as ended_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all

def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, M3_silver, M6_bronze, bac, C, bhc, fields_to_exclude):
    all_test_results = []

    # ---------------------------------------------------------
    # 1. Default Mappings
    # ---------------------------------------------------------
    test_df, test_data_setup = ended_tests.test_default_mapping_init(json_data, M1_silver, M3_bronze)
    
    if test_data_setup is True and test_df is not None:
        all_test_results.extend(ended_tests.test_ended_defaultValues(test_df, fields_to_exclude))
    elif test_data_setup is not True:
        all_test_results.append(test_data_setup)

    # ---------------------------------------------------------
    # 2. caseData Tests
    # ---------------------------------------------------------
    test_df, test_data_setup = ended_tests.test_caseData_init(json_data, M1_bronze, M3_bronze)
    
    if test_data_setup is True and test_df is not None:
        if "outOfTimeDecisionType" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_outOfTimeDecisionType_test1(test_df))
            all_test_results.append(ended_tests.test_outOfTimeDecisionType_test2(test_df))
            all_test_results.append(ended_tests.test_outOfTimeDecisionType_test3(test_df))
    elif test_data_setup is not True:
        all_test_results.append(test_data_setup)

    # ---------------------------------------------------------
    # 3. hearingRequirements Tests
    # ---------------------------------------------------------
    test_df, test_data_setup = ended_tests.test_hearingRequirements_init(json_data, M1_bronze, M3_bronze, bac)
    
    if test_data_setup is True and test_df is not None:
        if "isEvidenceFromOutsideUkOoc" not in fields_to_exclude:
            all_test_results.extend([
                ended_tests.test_isEvidenceFromOutsideUkOoc_test1(test_df),
                ended_tests.test_isEvidenceFromOutsideUkOoc_test2(test_df),
                ended_tests.test_isEvidenceFromOutsideUkOoc_test3(test_df),
                ended_tests.test_isEvidenceFromOutsideUkOoc_test4(test_df)
            ])
        
        if "isEvidenceFromOutsideUkInCountry" not in fields_to_exclude:
            all_test_results.extend([
                ended_tests.test_isEvidenceFromOutsideUkInCountry_test1(test_df),
                ended_tests.test_isEvidenceFromOutsideUkInCountry_test2(test_df),
                ended_tests.test_isEvidenceFromOutsideUkInCountry_test3(test_df),
                ended_tests.test_isEvidenceFromOutsideUkInCountry_test4(test_df)
            ])

        if "isInterpreterServicesNeeded" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isInterpreterServicesNeeded_test1(test_df))
            all_test_results.append(ended_tests.test_isInterpreterServicesNeeded_test2(test_df))
            
        if "singleSexCourt" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_singleSexCourt_test1(test_df))
            all_test_results.append(ended_tests.test_singleSexCourt_test2(test_df))     
            
        if "singleSexCourtType" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_singleSexCourtType_test1(test_df))
            all_test_results.append(ended_tests.test_singleSexCourtType_test2(test_df))       
            all_test_results.append(ended_tests.test_singleSexCourtType_test3(test_df)) 
            
        if "singleSexCourtTypeDescription" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_singleSexCourtTypeDescription_test1(test_df))
            all_test_results.append(ended_tests.test_singleSexCourtTypeDescription_test2(test_df))       
            all_test_results.append(ended_tests.test_singleSexCourtTypeDescription_test3(test_df)) 
            
        if "inCameraCourt" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_inCameraCourt_test1(test_df))
            all_test_results.append(ended_tests.test_inCameraCourt_test2(test_df)) 
    elif test_data_setup is not True:
        all_test_results.append(test_data_setup)

    # ---------------------------------------------------------
    # 4. hearingResponse Tests
    # ---------------------------------------------------------
    test_df, test_data_setup = ended_tests.test_hearingResponse_init(json_data, M1_bronze, M3_bronze, bac, M6_bronze)

    if test_data_setup is True and test_df is not None:
        if "isAppealSuitableToFloat" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isAppealSuitableToFloat_test1(test_df))
            all_test_results.append(ended_tests.test_isAppealSuitableToFloat_test2(test_df))
            
        if "listingLength" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_listingLength_test1(test_df))
            all_test_results.append(ended_tests.test_listingLength_test2(test_df))

        if "hearingChannel" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_hearingChannel_test1(test_df))
            all_test_results.append(ended_tests.test_hearingChannel_test2(test_df))
        if "listingLocation" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_listingLocation_test1(test_df))
        if "listCaseHearingLength" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_listCaseHearingLength_test1(test_df))
        if "listCaseHearingDate" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_listCaseHearingDate_test1(test_df))
        if "listCaseHearingCentre" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_listCaseHearingCentre_test1(test_df))
        if "listCaseHearingCentreAddress" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_listCaseHearingCentreAddress_test1(test_df))        
    elif test_data_setup is not True:
        all_test_results.append(test_data_setup)

        # ---------------------------------------------------------
        # 5. substantiveDecision Tests
        # ---------------------------------------------------------
    if test_data_setup is True and test_df is not None:
        if "sendDecisionsAndReasonsDate" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_sendDecisionsAndReasonsDate_test1(test_df))
        if "appealDate" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_appealDate_test1(test_df))
        if "appealDecision" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_appealDecision_test1(test_df))
            all_test_results.append(ended_tests.test_appealDecision_test2(test_df))
        if "isDecisionAllowed" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isDecisionAllowed_test1(test_df))
            all_test_results.append(ended_tests.test_isDecisionAllowed_test2(test_df))

        # ---------------------------------------------------------
        # 5. hearingActuals Tests
        # ---------------------------------------------------------
    if test_data_setup is True and test_df is not None:
        if "attendingJudge" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_attendingJudge_test1(test_df))   
        if "actualCaseHearingLength" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_actualCaseHearingLength_test1(test_df))   


        return classify_all(all_test_results)
