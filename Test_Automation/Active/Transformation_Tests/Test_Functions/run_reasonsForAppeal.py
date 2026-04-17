import Test_Functions.ReasonsForAppealSubmitted_Tests as rfas_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, M3_silver, M6_bronze, C, bhc, fields_to_exclude):
    all_test_results = []

    # -- Default mappings --
    test_data_setup = None
    test_df, test_data_setup =  rfas_tests.test_default_mapping_init(json_data, M1_silver)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(rfas_tests.test_RFAS_defaultValues(test_df))

    # -- hearingResponse --
    test_data_setup = None
    test_df, test_data_setup =  rfas_tests.test_hearingResponse_init(json_data, M3_silver, M6_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.append(rfas_tests.test_hearingResponse(test_df))

    return classify_all(all_test_results)
