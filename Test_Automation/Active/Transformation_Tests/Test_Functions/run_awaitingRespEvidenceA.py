import Test_Functions.AwaitingRespEvidence_A_Tests as are_a_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, fields_to_exclude):
    all_test_results = []

    # -- Default mappings --
    test_data_setup = None
    test_df, test_data_setup =  are_a_tests.test_default_mapping_init(json_data)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(are_a_tests.test_AREA_defaultValues(test_df, fields_to_exclude))
    #display(all_test_results)

    # -- Appellant Details --
    test_data_setup = None
    test_df, test_data_setup =  are_a_tests.test_appellant_details_init(json_data, M2_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        if "appellantFullName" not in fields_to_exclude:
            all_test_results.append(are_a_tests.test_appellantFullName_test1(test_df))

    # -- recordedOutOfTimeDecision (applies in ARE(a) and every subsequent state) --
    # Distinct key from the pp version: state notebooks exclude "recordedOutOfTimeDecision"
    # to suppress the pp test while keeping the simpler ARE(a)-onwards test running here.
    # Rule references M1 only; no M3 join needed.
    if "recordedOutOfTimeDecision_simple" not in fields_to_exclude:
        all_test_results.append(are_a_tests.test_recordedOutOfTimeDecision_ac1(json_data, M1_bronze))
        all_test_results.append(are_a_tests.test_recordedOutOfTimeDecision_ac2(json_data, M1_bronze))

    return classify_all(all_test_results)
