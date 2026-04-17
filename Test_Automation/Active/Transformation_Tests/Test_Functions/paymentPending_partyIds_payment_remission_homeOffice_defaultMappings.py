import Test_Functions.PaymentPending_Tests as pp_tests


def run(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bat, bhoref, external_storage, spark, fields_to_exclude, M4_silver=None, M4_bronze=None, M2_silver=None, H_silver=None, state_under_test="paymentPending"):
    all_test_results = []

    # -- partyIds --
    test_data_setup = None
    test_df, test_data_setup =  pp_tests.test_partyIds_init(json_data, M1_silver)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        if "appellantPartyId" not in fields_to_exclude:
            all_test_results.append(pp_tests.test_appellantPartyId(test_df))

        if "legalRepIndividualPartyId" not in fields_to_exclude:
            all_test_results.append(pp_tests.test_legalRepIndividualPartyId_ac1(test_df))
            all_test_results.append(pp_tests.test_legalRepIndividualPartyId_ac2(test_df))

        if "legalRepOrganisationPartyId" not in fields_to_exclude:
            all_test_results.append(pp_tests.test_legalRepOrganisationPartyId_ac1(test_df))
            all_test_results.append(pp_tests.test_legalRepOrganisationPartyId_ac2(test_df))

        if "sponsorPartyId" not in fields_to_exclude:
            all_test_results.append(pp_tests.test_sponsorPartyId_ac1(test_df))
            all_test_results.append(pp_tests.test_sponsorPartyId_ac2(test_df))

    # -- payment --
    test_data_setup = None
    test_df, test_data_setup =  pp_tests.test_payment_init(json_data, M1_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "feeAmountGbp" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_feeAmountGbp_ac1(test_df))
              all_test_results.append(pp_tests.test_feeAmountGbp_ac2(test_df))

         if "feeDescription" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_feeDescription_ac1(test_df))
              all_test_results.append(pp_tests.test_feeDescription_ac2(test_df))

         if "feeWithHearing" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_feeWithHearing_ac1(test_df))
              all_test_results.append(pp_tests.test_feeWithHearing_ac2(test_df))

         if "feeWithoutHearing" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_feeWithoutHearing_ac1(test_df))
              all_test_results.append(pp_tests.test_feeWithoutHearing_ac2(test_df))

         if "paymentDescription" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_paymentDescription_ac1(test_df))
              all_test_results.append(pp_tests.test_paymentDescription_ac2(test_df))

         if "decisionHearingFeeOption" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_decisionHearingFeeOption_ac1(test_df))
              all_test_results.append(pp_tests.test_decisionHearingFeeOption_ac2(test_df))

    # -- remission --
    test_data_setup = None
    test_df, test_data_setup =  pp_tests.test_remission_init(json_data, M1_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         check_fields = ["remissionType", "remissionClaim", "feeRemissionType", "exceptionalCircumstances", "legalAidAccountNumber", "asylumSupportReference", "helpWithFeesReferenceNumber"]
         if not all(field in fields_to_exclude for field in check_fields):
              all_test_results.append(pp_tests.test_remission_ac1(test_df))
              all_test_results.append(pp_tests.test_remission_ac2(test_df))
              all_test_results.append(pp_tests.test_remission_ac3(test_df))
              all_test_results.append(pp_tests.test_remission_ac4(test_df))
              all_test_results.append(pp_tests.test_remission_ac5(test_df))
              all_test_results.append(pp_tests.test_remission_ac6(test_df))
              all_test_results.append(pp_tests.test_remission_ac7(test_df))
              all_test_results.append(pp_tests.test_remission_ac8(test_df))
              all_test_results.append(pp_tests.test_remission_ac9(test_df))
              all_test_results.append(pp_tests.test_remission_ac10(test_df))
              all_test_results.append(pp_tests.test_remission_ac11(test_df))
              all_test_results.append(pp_tests.test_remission_ac12(test_df))

    # -- homeOffice --
    test_data_setup = None
    test_df, test_data_setup =  pp_tests.test_homeOffice_init(json_data, C, M1_bronze, M2_bronze, bhoref)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "homeOfficeDecisionDate" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_homeOfficeDecisionDate_ac1(test_df))
              all_test_results.append(pp_tests.test_homeOfficeDecisionDate_ac2(test_df))

         if "decisionLetterReceivedDate" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_decisionLetterReceivedDate_ac1(test_df))
              all_test_results.append(pp_tests.test_decisionLetterReceivedDate_ac2(test_df))
              all_test_results.append(pp_tests.test_decisionLetterReceivedDate_ac3(test_df))

         if "dateEntryClearanceDecision" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_dateEntryClearanceDecision_ac1(test_df))
              all_test_results.append(pp_tests.test_dateEntryClearanceDecision_ac2(test_df))
              all_test_results.append(pp_tests.test_dateEntryClearanceDecision_ac3(test_df))

         if "gwfReferenceNumber" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_gwfReferenceNumber_ac1(test_df))
              all_test_results.append(pp_tests.test_gwfReferenceNumber_ac2(test_df))
              all_test_results.append(pp_tests.test_gwfReferenceNumber_ac3(test_df))

         if "homeOfficeReferenceNumber" not in fields_to_exclude:
              ho_test_df, init_success = pp_tests.test_homeOfficeReferenceNumber_init(json_data, C, M1_bronze, M2_bronze, bhoref)

              if init_success is not True:
                   all_test_results.append(ho_test_df)
              else:
                   all_test_results.append(pp_tests.test_homeOfficeReferenceNumber_ac1(ho_test_df))
                   all_test_results.append(pp_tests.test_homeOfficeReferenceNumber_ac2(ho_test_df))
                   all_test_results.append(pp_tests.test_homeOfficeReferenceNumber_ac3(ho_test_df))

    # -- Default mappings --
    test_data_setup = None
    test_df, test_data_setup =  pp_tests.test_default_mapping_init(json_data)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(pp_tests.test_PP_defaultValues(test_df,fields_to_exclude))

    return all_test_results
