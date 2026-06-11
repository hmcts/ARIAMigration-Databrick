import Test_Functions.Listing_Tests as list_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bac, bll, b, fields_to_exclude):
    all_test_results = []

    # -- Default mappings --
    test_data_setup = None
    test_df, test_data_setup =  list_tests.test_default_mapping_init(json_data, M1_silver, bac, b)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(list_tests.test_Listing_defaultValues(test_df))


    # -- HearingRequirements --
    test_data_setup = None
    test_df, test_data_setup =  list_tests.test_hearingRequirements_init(json_data, M1_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "isInterpreterServicesNeeded" not in fields_to_exclude:
              all_test_results.append(list_tests.test_isInterpreterServicesNeeded_test1(test_df))
              all_test_results.append(list_tests.test_isInterpreterServicesNeeded_test2(test_df))
              all_test_results.append(list_tests.test_isInterpreterServicesNeeded_test3(test_df))

         if "singleSexCourt" not in fields_to_exclude:
              all_test_results.append(list_tests.test_singleSexCourt_test1(test_df))
              all_test_results.append(list_tests.test_singleSexCourt_test2(test_df))
              all_test_results.append(list_tests.test_singleSexCourt_test3(test_df))

         if "singleSexCourtType" not in fields_to_exclude:
              all_test_results.append(list_tests.test_singleSexCourtType_test1(test_df))
              all_test_results.append(list_tests.test_singleSexCourtType_test2(test_df))
              all_test_results.append(list_tests.test_singleSexCourtType_test3(test_df))

         if "singleSexCourtTypeDescription" not in fields_to_exclude:
              all_test_results.append(list_tests.test_singleSexCourtTypeDescription_test1(test_df))
              all_test_results.append(list_tests.test_singleSexCourtTypeDescription_test2(test_df))
              all_test_results.append(list_tests.test_singleSexCourtTypeDescription_test3(test_df))

         if "inCameraCourt" not in fields_to_exclude:
              all_test_results.append(list_tests.test_inCameraCourt_test1(test_df))
              all_test_results.append(list_tests.test_inCameraCourt_test2(test_df))
              all_test_results.append(list_tests.test_inCameraCourt_test3(test_df))

         if "inCameraCourtDescription" not in fields_to_exclude:
              all_test_results.append(list_tests.test_inCameraCourtDescription_test1(test_df))
              all_test_results.append(list_tests.test_inCameraCourtDescription_test2(test_df))
         
         if "appellantLevelFlags" not in fields_to_exclude:
             flags_df, flags_setup_pass = list_tests.test_flags_init(json_data, M1_bronze, C)

             if flags_setup_pass != True:
                 all_test_results.append(flags_setup_pass)

             elif flags_df is not None and flags_setup_pass == True:
                 all_test_results.append(list_tests.test_appellantFlags(flags_df))
                 all_test_results.append(list_tests.test_interpreterFlags(flags_df))

         # if language fields not in fields_to_exclude here:
         #      language tests here

    # -- HearingRequirements - language tests --
    test_data_setup = None
    test_df, test_data_setup =  list_tests.test_languages_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    language_fields = ["appellantInterpreterLanguageCategory, appellantInterpreterSpokenLanguage, appellantInterpreterSignLanguage"]
    if test_df != None:
      if language_fields not in fields_to_exclude:
        all_test_results.append(list_tests.test_languageInterpreterMapping(test_df))


    return classify_all(all_test_results)