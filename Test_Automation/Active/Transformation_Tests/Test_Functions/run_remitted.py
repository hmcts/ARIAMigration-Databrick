import Test_Functions.Remitted_Tests as rem_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, fields_to_exclude):
    all_test_results = []

    all_test_results.append(rem_tests.testcase1())
    all_test_results.append(rem_tests.testcase2())

    return classify_all(all_test_results)
