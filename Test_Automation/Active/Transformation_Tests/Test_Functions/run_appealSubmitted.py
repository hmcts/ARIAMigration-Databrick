import Test_Functions.AppealSubmitted_Tests as as_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bat, bhoref, external_storage, spark, fields_to_exclude, M4_silver=None, M4_bronze=None):
    all_test_results = []

    # -- Default mappings --
    test_data_setup = None
    test_df, test_data_setup =  as_tests.test_default_mapping_init(json_data, M1_silver)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(as_tests.test_AS_defaultValues(test_df,fields_to_exclude))
    #display(all_test_results)

    # -- Payment --
    test_data_setup = None
    test_df, test_data_setup =  as_tests.test_payment_init(json_data, M1_bronze, M4_silver)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        if "paymentStatus" not in fields_to_exclude:
            all_test_results.append(as_tests.test_paymentStatus_test1(test_df))
            all_test_results.append(as_tests.test_paymentStatus_test2(test_df))

        if "rpDcAppealHearingOption" not in fields_to_exclude:
            all_test_results.append(as_tests.test_rpDcAppealHearingOption_test1(test_df))
            all_test_results.append(as_tests.test_rpDcAppealHearingOption_test2(test_df))
            all_test_results.append(as_tests.test_rpDcAppealHearingOption_test3(test_df))

        if "paidDate" not in fields_to_exclude:
            all_test_results.append(as_tests.test_paidDate_test1(test_df))
            all_test_results.append(as_tests.test_paidDate_test2(test_df))

        if "paidAmount" not in fields_to_exclude:
            all_test_results.append(as_tests.test_paidAmount_test1(test_df))
            all_test_results.append(as_tests.test_paidAmount_test2(test_df))

        if "paymentDescription" not in fields_to_exclude:
            all_test_results.append(as_tests.test_paymentDescription_test1(test_df))
            all_test_results.append(as_tests.test_paymentDescription_test2(test_df))
            all_test_results.append(as_tests.test_paymentDescription_test3(test_df))

    # -- Remission --
    test_data_setup = None
    test_df, test_data_setup =  as_tests.test_remission_init(json_data, M1_bronze, M4_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        if "remissionDecision" not in fields_to_exclude:
            all_test_results.append(as_tests.test_remissionDecision_test1(test_df))
            all_test_results.append(as_tests.test_remissionDecision_test2(test_df))
            all_test_results.append(as_tests.test_remissionDecision_test3(test_df))

        if "remissionDecisionReason" not in fields_to_exclude:
            all_test_results.append(as_tests.test_remissionDecisionReason_test1(test_df))
            all_test_results.append(as_tests.test_remissionDecisionReason_test2(test_df))
            all_test_results.append(as_tests.test_remissionDecisionReason_test3(test_df))

        if "amountLeftToPay" not in fields_to_exclude:
            all_test_results.append(as_tests.test_amountLeftToPay_test1(test_df))
            all_test_results.append(as_tests.test_amountLeftToPay_test2(test_df))
            all_test_results.append(as_tests.test_amountLeftToPay_test3(test_df))

        if "amountRemitted" not in fields_to_exclude:
            all_test_results.append(as_tests.test_amountRemitted_test1(test_df))
            all_test_results.append(as_tests.test_amountRemitted_test2(test_df))
            all_test_results.append(as_tests.test_amountRemitted_test3(test_df))

    return classify_all(all_test_results)
