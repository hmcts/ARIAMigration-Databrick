import Test_Functions.paymentPending_appealType_caseData_hearingCentre_flagLabels_general as _c_appealType_caseData_hearingCentre_flagLabels_general
import Test_Functions.paymentPending_appellantDetails_legalRepDetails_sponsorDetails as _c_appellantDetails_legalRepDetails_sponsorDetails
import Test_Functions.paymentPending_partyIds_payment_remission_homeOffice_defaultMappings as _c_partyIds_payment_remission_homeOffice_defaultMappings
import Test_Functions.paymentPending_Detained as _c_Detained

from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bat, bhoref, external_storage, spark, fields_to_exclude, M4_silver=None, M4_bronze=None, M2_silver=None, H_silver=None, state_under_test="paymentPending"):
    all_test_results = []

    res = _c_appealType_caseData_hearingCentre_flagLabels_general.run(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bat, bhoref, external_storage, spark, fields_to_exclude, M4_silver, M4_bronze, M2_silver, H_silver, state_under_test)
    all_test_results.extend(res)

    res = _c_appellantDetails_legalRepDetails_sponsorDetails.run(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bat, bhoref, external_storage, spark, fields_to_exclude, M4_silver, M4_bronze, M2_silver, H_silver, state_under_test)
    all_test_results.extend(res)

    res = _c_partyIds_payment_remission_homeOffice_defaultMappings.run(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bat, bhoref, external_storage, spark, fields_to_exclude, M4_silver, M4_bronze, M2_silver, H_silver, state_under_test)
    all_test_results.extend(res)

    res = _c_Detained.run(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bat, bhoref, external_storage, spark, fields_to_exclude, M4_silver, M4_bronze, M2_silver, H_silver, state_under_test)
    all_test_results.extend(res)

    return classify_all(all_test_results)
