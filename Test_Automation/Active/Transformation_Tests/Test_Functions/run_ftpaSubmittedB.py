import Test_Functions.FTPA_Submitted_B_Tests as ftpa_b_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, fields_to_exclude):
    all_test_results = []

    # -- ftpa Tests --
    test_data_setup = None
    test_df, test_data_setup =  ftpa_b_tests.test_ftpa_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "ftpaList" not in fields_to_exclude:
              all_test_results.append(ftpa_b_tests.test_allocatedJudge_test1(test_df))
         if "ftpaList" not in fields_to_exclude:
              all_test_results.append(ftpa_b_tests.test_allocatedJudgeEdit_test1(test_df))
    # display(all_test_results)

    # -- Default Mappings --
    test_data_setup = None
    test_df, test_data_setup =  ftpa_b_tests.test_default_mapping_init(json_data)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(ftpa_b_tests.test_ftpa_b_defaultValues(test_df, fields_to_exclude))
    # display(all_test_results)

    return classify_all(all_test_results)
