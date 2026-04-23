import Test_Functions.Ended_Tests as ended_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, M3_silver, M6_bronze, bac, C, bhc, fields_to_exclude, M3_bronze_full=None):
    all_test_results = []

    # ---------------------------------------------------------
    # 1. Default Mappings
    # ---------------------------------------------------------
    test_df, test_data_setup = ended_tests.test_default_mapping_init(json_data, M3_bronze)
    
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
    test_df, test_data_setup = ended_tests.test_hearingRequirements_init(json_data, M1_bronze, M3_bronze)
    
    if test_data_setup is True and test_df is not None:
        # evidence tests
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
    elif test_data_setup is not True:
        all_test_results.append(test_data_setup)


    return classify_all(all_test_results)
