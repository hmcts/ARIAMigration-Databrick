import Test_Functions.PaymentPending_Tests as ppd_tests


def run(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bat, bhoref, external_storage, spark, fields_to_exclude, M4_silver=None, M4_bronze=None, M2_silver=None, H_silver=None, state_under_test="paymentPending"):
    all_test_results = []

    # -- Detained tests --
    test_df, test_data_setup = ppd_tests.test_detained_init(json_data, M2_bronze, M1_bronze)
    if test_data_setup is not True:
        all_test_results.append(test_data_setup)

    if test_df is not None:
        if "detentionFacility" not in fields_to_exclude:
            all_test_results.append(ppd_tests.test_detentionFacility_ac1(test_df))
            all_test_results.append(ppd_tests.test_detentionFacility_ac2(test_df))
            all_test_results.append(ppd_tests.test_detentionFacility_ac3(test_df))
            all_test_results.append(ppd_tests.test_detentionFacility_ac4(test_df))

        if "prisonName" not in fields_to_exclude:
            all_test_results.append(ppd_tests.test_prisonName_ac1(test_df))

        if "prisonNOMSNumber" not in fields_to_exclude:
            all_test_results.append(ppd_tests.test_prisonNOMSNumber_ac1(test_df))
            all_test_results.append(ppd_tests.test_prisonNOMSNumber_ac2(test_df))

        if "otherDetentionFacilityName" not in fields_to_exclude:
            all_test_results.append(ppd_tests.test_otherDetentionFacilityName_ac1(test_df))
            all_test_results.append(ppd_tests.test_otherDetentionFacilityName_ac2(test_df))

        if "ircName" not in fields_to_exclude:
            all_test_results.append(ppd_tests.test_ircName_ac1(test_df))

        if "removalOrderOptions" not in fields_to_exclude:
            all_test_results.append(ppd_tests.test_removalOrderOptions_ac1(test_df))
            all_test_results.append(ppd_tests.test_removalOrderOptions_ac2(test_df))

        if "removalOrderDate" not in fields_to_exclude:
            all_test_results.append(ppd_tests.test_removalOrderDate_ac1(test_df))
            all_test_results.append(ppd_tests.test_removalOrderDate_ac2(test_df))

        if "detentionDetails" not in fields_to_exclude:
            all_test_results.append(ppd_tests.test_detentionDetails_ac1(test_df))

    # -- Detained caseData tests --
    test_df2, test_data_setup2 = ppd_tests.test_caseData_init_detained(json_data, M2_bronze)
    if test_data_setup2 is not True:
        all_test_results.append(test_data_setup2)

    if test_df2 is not None:
        if "caseData" not in fields_to_exclude:
            mapping_df = ppd_tests.caseDataMap(spark)
            result, defect_df = ppd_tests.caseData_ac1(mapping_df, test_df2)
            all_test_results.append(result)

        if "appellantInDetention" not in fields_to_exclude:
            all_test_results.append(ppd_tests.test_appellantInDetention_ac1(test_df2))
            all_test_results.append(ppd_tests.test_appellantInDetention_ac2(test_df2))

    # -- Detained appellantDetails tests --
    test_df3, test_data_setup3 = ppd_tests.test_appellantDetails_init_detained(json_data, M2_bronze)
    if test_data_setup3 is not True:
        all_test_results.append(test_data_setup3)

    if test_df3 is not None:
        if "appellantInUk" not in fields_to_exclude:
            all_test_results.append(ppd_tests.test_appellantInUk_ac1(test_df3))
            all_test_results.append(ppd_tests.test_appellantInUk_ac2(test_df3))
            all_test_results.append(ppd_tests.test_appellantInUk_ac3(test_df3))

        if "appealOutOfCountry" not in fields_to_exclude:
            all_test_results.append(ppd_tests.test_appealOutOfCountry_ac1(test_df3))
            all_test_results.append(ppd_tests.test_appealOutOfCountry_ac2(test_df3))
            all_test_results.append(ppd_tests.test_appealOutOfCountry_ac3(test_df3))        

        if "appellantAddress" not in fields_to_exclude:
            all_test_results.append(ppd_tests.test_appellantAddress_ac1(test_df3))
            all_test_results.append(ppd_tests.test_appellantAddress_ac2(test_df3))
            all_test_results.append(ppd_tests.test_appellantAddress_ac3(test_df3))

    return all_test_results
