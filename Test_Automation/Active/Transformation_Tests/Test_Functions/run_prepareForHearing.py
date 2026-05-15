import Test_Functions.Prepare_For_Hearing_Tests as pfh_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bll, fields_to_exclude):
    all_test_results = []

    # -- hearingResponse --
    test_data_setup = None
    test_df, test_data_setup = pfh_tests.test_hearingResponse_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "isAppealSuitableToFloat" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_isAppealSuitableToFloat_test1(test_df))
              all_test_results.append(pfh_tests.test_isAppealSuitableToFloat_test2(test_df))
              all_test_results.append(pfh_tests.test_isAppealSuitableToFloat_test3(test_df))

         if "isInCameraCourtAllowed" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_isInCameraCourtAllowed_test1(test_df))
              all_test_results.append(pfh_tests.test_isInCameraCourtAllowed_test2(test_df))

         if "inCameraCourtTribunalResponse" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_inCameraCourtTribunalResponse_test1(test_df))
              all_test_results.append(pfh_tests.test_inCameraCourtTribunalResponse_test2(test_df))

         if "inCameraCourtDecisionForDisplay" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_inCameraCourtDecisionForDisplay_test1(test_df))
              all_test_results.append(pfh_tests.test_inCameraCourtDecisionForDisplay_test2(test_df))

         if "isSingleSexCourtAllowed" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_isSingleSexCourtAllowed_test1(test_df))
              all_test_results.append(pfh_tests.test_isSingleSexCourtAllowed_test2(test_df))

         if "singleSexCourtTribunalResponse" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_singleSexCourtTribunalResponse_test1(test_df))
              all_test_results.append(pfh_tests.test_singleSexCourtTribunalResponse_test2(test_df))

         if "singleSexCourtDecisionForDisplay" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_singleSexCourtDecisionForDisplay_test1(test_df))
              all_test_results.append(pfh_tests.test_singleSexCourtDecisionForDisplay_test2(test_df))

    # -- hearingDetails --
    test_data_setup = None
    test_df, test_data_setup = pfh_tests.test_hearingDetails_init(json_data, M1_bronze, M3_bronze, bll)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "listingLength" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_listingLength_mapping(test_df))

         if "hearingChannel" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_hearingChannel_test1(test_df))
              all_test_results.append(pfh_tests.test_hearingChannel_test2(test_df))

         if "listingLocation" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_listingLocation_mapping(test_df))

     # -- hearingDetails -- Moved from decsion
    test_data_setup = None
    test_df, test_data_setup =  pfh_tests.test_hearingDetails_init2(json_data, M3_bronze, bll)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "listCaseHearingLength" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_listCaseHearingLength(test_df))

         if "listCaseHearingDate" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_listCaseHearingDate(test_df))

         if "listCaseHearingCentre" or "listCaseHearingCentreAddress" not in fields_to_exclude:
              all_test_results.append(pfh_tests.test_listCaseHearingCentre_Address(test_df, spark))


    # -- Default Mappings --
    test_data_setup = None
    test_df, test_data_setup =  pfh_tests.test_default_mapping_init(json_data)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(pfh_tests.test_pFH_defaultValues(test_df, fields_to_exclude))

    return classify_all(all_test_results)
