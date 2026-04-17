import Test_Functions.Decision_Tests as dec_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bll, spark, fields_to_exclude):
    all_test_results = []

    # -- Default mappings --
    test_data_setup = None
    test_df, test_data_setup =  dec_tests.test_default_mapping_init(json_data)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(dec_tests.test_dec_defaultValues(test_df, fields_to_exclude))
    # display(all_test_results)

    # -- hearingDetails --
    test_data_setup = None
    test_df, test_data_setup =  dec_tests.test_hearingDetails_init(json_data, M3_bronze, bll)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "listCaseHearingLength" not in fields_to_exclude:
              all_test_results.append(dec_tests.test_listCaseHearingLength(test_df))

         if "listCaseHearingLength" not in fields_to_exclude:
              all_test_results.append(dec_tests.test_listCaseHearingDate(test_df))

         if "listCaseHearingCentre" or "listCaseHearingCentreAddress" not in fields_to_exclude:
              all_test_results.append(dec_tests.test_listCaseHearingCentre_Address(test_df, spark))

    # -- general --
    test_data_setup = None
    test_df, test_data_setup =  dec_tests.test_general_init(json_data, M1_bronze, M2_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "bundleFileNamePrefix" not in fields_to_exclude:
              all_test_results.append(dec_tests.test_bundleFileNamePrefix(test_df))

    return classify_all(all_test_results)
