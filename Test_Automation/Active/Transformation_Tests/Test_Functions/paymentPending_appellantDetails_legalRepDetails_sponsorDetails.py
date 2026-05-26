import Test_Functions.PaymentPending_Tests as pp_tests


def run(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bat, bhoref, external_storage, spark, fields_to_exclude, M4_silver=None, M4_bronze=None, M2_silver=None, H_silver=None, state_under_test="paymentPending"):
    all_test_results = []

    # -- appellantDetails --
    test_data_setup = None
    test_df, test_data_setup =  pp_tests.test_appellantdetails_init(json_data, M2_bronze, M1_bronze, C, bhoref)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "appellantFamilyName" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appellantFamilyName(test_df))

         if "appellantGivenNames" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appellantGivenNames(test_df))

         if "appellantNameForDisplay" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appellantNameForDisplay(test_df))

         if "appellantDateOfBirth" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appellantDateOfBirth(test_df))

         if "isAppellantMinor" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_isAppellantMinor(test_df))

         if "caseNameHmctsInternal" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_caseNameHmctsInternal(test_df))

         if "hmctsCaseNameInternal" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_hmctsCaseNameInternal(test_df))

         if "internalAppellantEmail" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_internalAppellantEmail_ac1(test_df))
              all_test_results.append(pp_tests.test_internalAppellantEmail_ac2(test_df))

         if "email" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_email_ac1(test_df))
              all_test_results.append(pp_tests.test_email_ac2(test_df))

         if "internalAppellantMobileNumber" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_internalAppellantMobileNumber_ac1(json_data, M2_bronze))
              all_test_results.append(pp_tests.test_internalAppellantMobileNumber_ac2(json_data, M2_bronze))

         if "mobileNumber" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_mobileNumber_ac1(json_data, M2_bronze))
              all_test_results.append(pp_tests.test_mobileNumber_ac2(json_data, M2_bronze))

         if "appellantInUk" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appellantInUk_ac1(test_df))
              all_test_results.append(pp_tests.test_appellantInUk_ac2(test_df))

         if "appealOutOfCountry" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appealOutOfCountry_ac1(test_df))
              all_test_results.append(pp_tests.test_appealOutOfCountry_ac2(test_df))

         if "oocAppealAdminJ" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_oocAppealAdminJ_ac1(test_df))
              all_test_results.append(pp_tests.test_oocAppealAdminJ_ac2(test_df))
              all_test_results.append(pp_tests.test_oocAppealAdminJ_ac3(test_df))
              all_test_results.append(pp_tests.test_oocAppealAdminJ_ac4(test_df))
              all_test_results.append(pp_tests.test_oocAppealAdminJ_ac5(test_df))

         if "appellantAddress" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appellantAddress_ac1(test_df))
              all_test_results.append(pp_tests.test_appellantAddress_ac2(test_df))

         if "addressLine1AdminJ" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_addressLine1AdminJ_ac1(test_df))
              all_test_results.append(pp_tests.test_addressLine1AdminJ_ac2(test_df))
              all_test_results.append(pp_tests.test_addressLine1AdminJ_ac3(test_df))

         if "addressLine2AdminJ" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_addressLine2AdminJ_ac1(test_df))
              all_test_results.append(pp_tests.test_addressLine2AdminJ_ac2(test_df))
              all_test_results.append(pp_tests.test_addressLine2AdminJ_ac3(test_df))

         if "addressLine3AdminJ" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_addressLine3AdminJ_ac1(test_df))
              all_test_results.append(pp_tests.test_addressLine3AdminJ_ac2(test_df))

         if "addressLine4AdminJ" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_addressLine4AdminJ_ac1(test_df))
              all_test_results.append(pp_tests.test_addressLine4AdminJ_ac2(test_df))

         if "countryGovUkOocAdminJ" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_countryGovUkOocAdminJ_ac1(test_df))
          #     all_test_results.append(pp_tests.test_countryGovUkOocAdminJ_ac2(test_df,external_storage,spark)[0])

         if "appellantStateless" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appellantStateless_ac1(test_df))
              all_test_results.append(pp_tests.test_appellantStateless_ac2(test_df))

         if "appellantNationalities" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appellantNationalities(test_df))

         if "appellantNationalitiesDescription" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appellantNationalitiesDescription(test_df))

         if "deportationOrderOptions" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_deportationOrderOptions_ac1(test_df))
              all_test_results.append(pp_tests.test_deportationOrderOptions_ac2(test_df))
              all_test_results.append(pp_tests.test_deportationOrderOptions_ac3(test_df))
              all_test_results.append(pp_tests.test_deportationOrderOptions_ac3(test_df))

         if "appellantHasFixedAddress" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appellantHasFixedAddress(test_df))

         if "appellantHasFixedAddressAdminJ" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appellantHasFixedAddressAdminJ(test_df))

    # -- legalRepDetails --
    test_data_setup = None
    test_df, test_data_setup =  pp_tests.test_legalRepDetails_init(json_data, M1_bronze, M1_silver)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "legalRepEmail" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_legalRepEmail_test1(test_df))
              all_test_results.append(pp_tests.test_legalRepEmail_test2(test_df))
              all_test_results.append(pp_tests.test_legalRepEmail_test3(test_df))
              all_test_results.append(pp_tests.test_legalRepEmail_test4(test_df))
              all_test_results.append(pp_tests.test_legalRepEmail_test5(test_df))

         if "legalRepGivenName" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_legalRepGivenName_test1(test_df))
              all_test_results.append(pp_tests.test_legalRepGivenName_test2(test_df))
              all_test_results.append(pp_tests.test_legalRepGivenName_test3(test_df))

         if "legalRepFamilyNamePaperJ" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_legalRepFamilyNamePaperJ_test1(test_df))
              all_test_results.append(pp_tests.test_legalRepFamilyNamePaperJ_test2(test_df))

         if "legalRepCompanyPaperJ" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_legalRepCompanyPaperJ_test1(test_df))
              all_test_results.append(pp_tests.test_legalRepCompanyPaperJ_test2(test_df))

         if "legalRepHasAddress" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_legalRepHasAddress_test1(test_df))
              all_test_results.append(pp_tests.test_legalRepHasAddress_test2(test_df))
              all_test_results.append(pp_tests.test_legalRepHasAddress_test3(test_df))

         if "legalRepAddressUK" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_legalRepAddressUK_test1(test_df))
              all_test_results.append(pp_tests.test_legalRepAddressUK_test2(test_df))

         if "oocAddressLine1" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_oocAddressLine1_test1(test_df))
              all_test_results.append(pp_tests.test_oocAddressLine1_test2(test_df))

         if "oocAddressLine2" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_oocAddressLine2_test1(test_df))
              all_test_results.append(pp_tests.test_oocAddressLine2_test2(test_df))

         if "oocAddressLine3" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_oocAddressLine3_test1(test_df))
              all_test_results.append(pp_tests.test_oocAddressLine3_test2(test_df))

         if "oocAddressLine4" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_oocAddressLine4_test1(test_df))
              all_test_results.append(pp_tests.test_oocAddressLine4_test2(test_df))

         if "oocLrCountryGovUkAdminJ" not in fields_to_exclude:
              result, df = pp_tests.test_oocLrCountryGovUkAdminJ_test1(test_df,external_storage,spark)
              all_test_results.append(result)

    # -- sponsorDetails --
    test_data_setup = None
    test_df_sd, test_data_setup =  pp_tests.test_sponsorDetails_init(json_data, M1_bronze, C)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df_sd != None:
       if "hasSponsor" not in fields_to_exclude:
          all_test_results.append(pp_tests.test_hasSponsor_test1(test_df_sd))
          all_test_results.append(pp_tests.test_hasSponsor_test2(test_df_sd))
          all_test_results.append(pp_tests.test_hasSponsor_test3(test_df_sd))

       if "sponsorGivenNames" not in fields_to_exclude:
          all_test_results.append(pp_tests.test_sponsorGivenNames_test1(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorGivenNames_test2(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorGivenNames_test3(test_df_sd))

       if "sponsorFamilyName" not in fields_to_exclude:
          all_test_results.append(pp_tests.test_sponsorFamilyName_test1(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorFamilyName_test2(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorFamilyName_test3(test_df_sd))

       if "sponsorAuthorisation" not in fields_to_exclude:
          all_test_results.append(pp_tests.test_sponsorAuthorisation_test1(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorAuthorisation_test2(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorAuthorisation_test3(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorAuthorisation_test4(test_df_sd))

       if "sponsorAddress" not in fields_to_exclude:
          all_test_results.append(pp_tests.test_sponsorAddress_ac1(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorAddress_ac2(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorAddress_ac3(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorAddress_ac4(test_df_sd))

       if "sponsorEmailAdminJ" not in fields_to_exclude:
          all_test_results.append(pp_tests.test_sponsorEmailAdminJ_ac1(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorEmailAdminJ_ac2(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorEmailAdminJ_ac3(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorEmailAdminJ_ac4(test_df_sd))

       if "sponsorMobileNumberAdminJ" not in fields_to_exclude:
          all_test_results.append(pp_tests.test_sponsorMobileNumberAdminJ_ac1(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorMobileNumberAdminJ_ac2(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorMobileNumberAdminJ_ac3(test_df_sd))
          all_test_results.append(pp_tests.test_sponsorMobileNumberAdminJ_ac4(test_df_sd))

    return all_test_results
