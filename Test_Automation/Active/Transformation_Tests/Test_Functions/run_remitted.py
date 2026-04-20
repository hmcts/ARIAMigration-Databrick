import Test_Functions.Remitted_Tests as rem_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, fields_to_exclude):
    all_test_results = []


    # -- Default Mappings --
    test_data_setup = None
    test_df, test_data_setup = rem_tests.test_default_mapping_init(json_data)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(rem_tests.test_remitted_defaultValues(test_df, fields_to_exclude))
    # display(all_test_results)

    return classify_all(all_test_results)