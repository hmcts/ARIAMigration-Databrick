import Test_Functions.Decided_A_Tests as dec_a_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, fields_to_exclude):
    all_test_results = []

    # -- Default mappings --
    test_data_setup = None
    test_df, test_data_setup =  dec_a_tests.test_default_mapping_init(json_data)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(dec_a_tests.test_dec_a_defaultValues(test_df, fields_to_exclude))

    # -- substantiveDecision --
    test_data_setup = None
    test_df, test_data_setup =  dec_a_tests.test_substantiveDecision_init(json_data, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        if "sendDecisionsAndReasonsDate" not in fields_to_exclude:
            all_test_results.append(dec_a_tests.test_sendDecisionsAndReasonsDate(test_df))

        if "appealDate" not in fields_to_exclude:
            all_test_results.append(dec_a_tests.test_appealDate(test_df))

        if "appealDecision" not in fields_to_exclude:
            all_test_results.append(dec_a_tests.test_appealDecision_ac1(test_df))
            all_test_results.append(dec_a_tests.test_appealDecision_ac2(test_df))

        if "isDecisionAllowed" not in fields_to_exclude:
            all_test_results.append(dec_a_tests.test_isDecisionAllowed_ac1(test_df))
            all_test_results.append(dec_a_tests.test_isDecisionAllowed_ac2(test_df))

    # -- hearingActuals --
    test_data_setup = None
    test_df, test_data_setup =  dec_a_tests.test_hearingActuals_init(json_data, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        if "attendingJudge" not in fields_to_exclude:
            all_test_results.append(dec_a_tests.test_attendingJudge(test_df))

        if "actualCaseHearingLength" not in fields_to_exclude:
            all_test_results.append(dec_a_tests.test_actualCaseHearingLength(test_df))

    # -- ftpaApplicationDeadline --
    if "ftpaApplicationDeadline" not in fields_to_exclude:
        test_df, test_data_setup =  dec_a_tests.test_ftpaApplicationDeadline_init(json_data, M3_bronze, C)
        if test_data_setup != True:
            all_test_results.append(test_data_setup)

        if test_df != None:
            all_test_results.append(dec_a_tests.test_ftpaApplicationDeadline_combined(test_df))

    return classify_all(all_test_results)
