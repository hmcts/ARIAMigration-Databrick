import Test_Functions.Ended_Tests as ended_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, M3_silver, M6_bronze, C, bhc, fields_to_exclude, M3_bronze_full=None):
    all_test_results = []

    # -- Default mappings --
    # test_df, test_data_setup = ended_tests.test_default_mapping_init(json_data, M3_bronze)
    # if test_data_setup != True:
    #     all_test_results.append(test_data_setup)

    # if test_df is not None:
    #     all_test_results.extend(ended_tests.test_ended_defaultValues(test_df, fields_to_exclude))

    # -- caseData Tests --
    test_data_setup = None
    test_df, test_data_setup =  ended_tests.test_caseData_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "outOfTimeDecisionType" not in fields_to_exclude:
              all_test_results.append(ended_tests.test_outOfTimeDecisionType_test1(test_df))
              all_test_results.append(ended_tests.test_outOfTimeDecisionType_test2(test_df))
              all_test_results.append(ended_tests.test_outOfTimeDecisionType_test3(test_df))

    return classify_all(all_test_results)

    # -- hearingRequirements Tests --
    test_data_setup = None
    test_df, test_data_setup =  ended_tests.test_hearingRequirements_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "isEvidenceFromOutsideUkOoc" not in fields_to_exclude:
              all_test_results.append(ended_tests.test_isEvidenceFromOutsideUkOoc_test1(test_df))
              all_test_results.append(ended_tests.test_isEvidenceFromOutsideUkOoc_test2(test_df))
              all_test_results.append(ended_tests.test_isEvidenceFromOutsideUkOoc_test3(test_df))
              all_test_results.append(ended_tests.test_isEvidenceFromOutsideUkOoc_test4(test_df))

    return classify_all(all_test_results)
