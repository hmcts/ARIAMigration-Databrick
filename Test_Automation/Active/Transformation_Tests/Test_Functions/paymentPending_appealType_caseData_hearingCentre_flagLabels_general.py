import Test_Functions.PaymentPending_Tests as pp_tests


def run(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, bat, bhoref, external_storage, spark, fields_to_exclude, M4_silver=None, M4_bronze=None, M2_silver=None, H_silver=None, state_under_test="paymentPending"):
    all_test_results = []

    # -- appealType --
    test_data_setup = None
    test_df_appealType, test_data_setup =  pp_tests.test_appealType_init(json_data, M1_silver, C, bat)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df_appealType != None:
         check_fields = ["appealType", "hmctsCaseCategory", "appealTypeDescription"]
         if not all(field in fields_to_exclude for field in check_fields):
              all_test_results.append(pp_tests.test_appealType_ac1(test_df_appealType))
              all_test_results.append(pp_tests.test_appealType_ac3(test_df_appealType))
              all_test_results.append(pp_tests.test_appealType_ac3(test_df_appealType))
              all_test_results.append(pp_tests.test_appealType_ac4(test_df_appealType))
              all_test_results.append(pp_tests.test_appealType_ac5(test_df_appealType))
              all_test_results.append(pp_tests.test_appealType_ac6(test_df_appealType))
              all_test_results.append(pp_tests.test_appealType_ac7(test_df_appealType))
              all_test_results.append(pp_tests.test_appealType_ac8(test_df_appealType))
              all_test_results.append(pp_tests.test_appealType_ac9(test_df_appealType))
              all_test_results.append(pp_tests.test_appealType_ac10(test_df_appealType))
              all_test_results.append(pp_tests.test_appealType_ac11(test_df_appealType))
              all_test_results.append(pp_tests.test_appealType_ac12(test_df_appealType))
              all_test_results.append(pp_tests.test_appealType_ac13(test_df_appealType))

         if "appealReferenceNumber" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appealReferenceNumber_ac1(test_df_appealType))

    # -- caseData --
    test_data_setup = None
    test_df, test_data_setup =  pp_tests.test_caseData_init(json_data, M1_bronze, M1_silver, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
          if "outOfTimeDecisionType" not in fields_to_exclude:
               all_test_results.append(pp_tests.test_outOfTimeDecisionType(test_df))

          if "outOfTimeDecisionMaker" not in fields_to_exclude:
               all_test_results.append(pp_tests.test_outOfTimeDecisionMaker(test_df))

          if "submissionOutOfTime" not in fields_to_exclude:
               all_test_results.append(pp_tests.test_submissionOutOfTime_ac1(test_df))
               all_test_results.append(pp_tests.test_submissionOutOfTime_ac2(test_df))
               all_test_results.append(pp_tests.test_submissionOutOfTime_ac3(test_df))

          if "appealSubmissionDate" not in fields_to_exclude:
               all_test_results.append(pp_tests.test_appealSubmissionDate(test_df))

          if "appealSubmissionInternalDate" not in fields_to_exclude:
               all_test_results.append(pp_tests.test_appealSubmissionInternalDate(test_df))

          if "tribunalReceivedDate" not in fields_to_exclude:
               all_test_results.append(pp_tests.test_tribunalReceivedDate(test_df))

          if "appellantsRepresentation" not in fields_to_exclude:
               all_test_results.append(pp_tests.test_appellantsRepresentation_ac1(test_df))
               all_test_results.append(pp_tests.test_appellantsRepresentation_ac2(test_df))

          if "recordedOutOfTimeDecision" not in fields_to_exclude:
               all_test_results.append(pp_tests.test_recordedOutOfTimeDecision_ac1(json_data, M3_bronze, M1_bronze))
               all_test_results.append(pp_tests.test_recordedOutOfTimeDecision_ac2(json_data, M3_bronze, M1_bronze))

          if "applicationOutOfTimeExplanation" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_applicationOutOfTimeExplanation(json_data, M1_bronze, M3_bronze))

          if "caseManagementLocationRefData" not in fields_to_exclude:
               all_test_results.append(pp_tests.test_caseManagementLocationRefData(test_df))

          if "hearingCentreDynamicList" not in fields_to_exclude:
               all_test_results.append(pp_tests.test_hearingCentreDynamicList(test_df))

    # -- hearingCentre --
    check_fields = ["hearingCentre", "staffLocation", "caseManagementLocation", "selectedHearingCentreRefData", "applicationChangeDesignatedHearingCentre"]
    if not all(field in fields_to_exclude for field in check_fields):
        audit_df, hc_results = pp_tests.hearing_centre_field_test(M1_silver, M2_silver, H_silver, bhc, json_data, state_under_test)
        if isinstance(hc_results, list):
            all_test_results.extend(hc_results)
        else:
            all_test_results.append(hc_results)

    # -- flagLabels --
    test_data_setup = None
    test_df, test_data_setup =  pp_tests.test_flags_init(json_data, M1_bronze, C)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "caseFlags" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_caseFlags(test_df))

         if "appellantLevelFlags" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_appellantFlags(test_df))

         if "isAriaMigratedFeeExemption" not in fields_to_exclude:
              all_test_results.append(pp_tests.test_isAriaMigratedFeeExemption_ac1(test_df))
              all_test_results.append(pp_tests.test_isAriaMigratedFeeExemption_ac2(test_df))

    # -- general --
    test_data_setup = None
    test_df, test_data_setup =  pp_tests.test_isServiceRequestTabVisibleConsideringRemissions_init(json_data, M1_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        if "isServiceRequestTabVisibleConsideringRemissions" not in fields_to_exclude:
            all_test_results.append(pp_tests.test_isServiceRequestTabVisibleConsideringRemissions_ac1(test_df))
            all_test_results.append(pp_tests.test_isServiceRequestTabVisibleConsideringRemissions_ac2(test_df))
            all_test_results.append(pp_tests.test_isServiceRequestTabVisibleConsideringRemissions_ac3(test_df))
            all_test_results.append(pp_tests.test_isServiceRequestTabVisibleConsideringRemissions_ac4(test_df))

     # -- timeToLive --
    test_data_setup = None
    test_df, test_data_setup =  pp_tests.test_timeToLive_detained(json_data, M1_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        if "timeToLive" not in fields_to_exclude:
            all_test_results.append(pp_tests.test_timeToLive_ac1(test_df))
            all_test_results.append(pp_tests.test_timeToLive_ac2(test_df))

    return all_test_results
