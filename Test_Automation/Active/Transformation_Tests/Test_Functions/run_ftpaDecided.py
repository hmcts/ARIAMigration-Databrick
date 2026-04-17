import Test_Functions.FTPADecided_Tests as ftpaDecided_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, fields_to_exclude):
    all_test_results = []

    # -- ftpa Tests --
    test_data_setup = None
    test_df, test_data_setup =  ftpaDecided_tests.test_ftpa_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "ftpaList" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_ftpaApplicantType_test1(test_df))
         if "ftpaFirstDecision" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_ftpaFirstDecision_test1(test_df))
         if "ftpaAppellantDecisionDate" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_ftpaAppellantDecisionDate_test1(test_df))
              all_test_results.append(ftpaDecided_tests.test_ftpaAppellantDecisionDate_test2(test_df))
         if "ftpaRespondentDecisionDate" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_ftpaRespondentDecisionDate_test1(test_df))
              all_test_results.append(ftpaDecided_tests.test_ftpaRespondentDecisionDate_test2(test_df))
         if "ftpaFinalDecisionForDisplay" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_ftpaFinalDecisionForDisplay_test1(test_df))
         if "ftpaAppellantRjDecisionOutcomeType" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_ftpaAppellantRjDecisionOutcomeType_test1(test_df))
         if "ftpaRespondentRjDecisionOutcomeType" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_ftpaRespondentRjDecisionOutcomeType_test1(test_df))
         if "isFtpaAppellantNoticeOfDecisionSetAside" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_isFtpaAppellantNoticeOfDecisionSetAside_test1(test_df))
              all_test_results.append(ftpaDecided_tests.test_isFtpaAppellantNoticeOfDecisionSetAside_test2(test_df))
         if "isFtpaRespondentNoticeOfDecisionSetAside" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_isFtpaRespondentNoticeOfDecisionSetAside_test1(test_df))
              all_test_results.append(ftpaDecided_tests.test_isFtpaRespondentNoticeOfDecisionSetAside_test2(test_df))
    # display(all_test_results)

    # -- general Tests --
    test_data_setup = None
    test_df, test_data_setup =  ftpaDecided_tests.test_ftpa_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        if "isAppellantFtpaDecisionVisibleToAll" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_isAppellantFtpaDecisionVisibleToAll_test1(test_df))
              all_test_results.append(ftpaDecided_tests.test_isAppellantFtpaDecisionVisibleToAll_test2(test_df))
        if "isRespondentFtpaDecisionVisibleToAll" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_isRespondentFtpaDecisionVisibleToAll_test1(test_df))
              all_test_results.append(ftpaDecided_tests.test_isRespondentFtpaDecisionVisibleToAll_test2(test_df))
        if "isFtpaAppellantDecided" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_isFtpaAppellantDecided_test1(test_df))
        if "isFtpaRespondentDecided" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_isFtpaRespondentDecided_test1(test_df))
        if "secondFtpaDecisionExists" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_secondFtpaDecisionExists_test1(test_df))
              all_test_results.append(ftpaDecided_tests.test_secondFtpaDecisionExists_test2(test_df))
              all_test_results.append(ftpaDecided_tests.test_secondFtpaDecisionExists_test3(test_df))
              all_test_results.append(ftpaDecided_tests.test_secondFtpaDecisionExists_test4(test_df))
    # display(all_test_results)

    # -- document Tests --

    test_data_setup = None
    test_df, test_data_setup =  ftpaDecided_tests.test_ftpa_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "allFtpaAppellantDecisionDocs" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_allFtpaAppellantDecisionDocs_test1(test_df))
              all_test_results.append(ftpaDecided_tests.test_allFtpaAppellantDecisionDocs_test2(test_df))
         if "allFtpaRespondentDecisionDocs" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_allFtpaRespondentDecisionDocs_test1(test_df))
              all_test_results.append(ftpaDecided_tests.test_allFtpaRespondentDecisionDocs_test2(test_df))
         if "ftpaAppellantNoticeDocument" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_ftpaAppellantNoticeDocument_test1(test_df))
              all_test_results.append(ftpaDecided_tests.test_ftpaAppellantNoticeDocument_test2(test_df))
         if "ftpaRespondentNoticeDocument" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_ftpaRespondentNoticeDocument_test1(test_df))
              all_test_results.append(ftpaDecided_tests.test_ftpaRespondentNoticeDocument_test2(test_df))

    # display(all_test_results)

    # -- ftpaList Tests --

    test_data_setup = None
    test_df, test_data_setup =  ftpaDecided_tests.test_ftpa_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "ftpaList" not in fields_to_exclude:
              all_test_results.append(ftpaDecided_tests.test_ftpaList_test1(test_df))
              all_test_results.append(ftpaDecided_tests.test_ftpaList_test2(test_df))
    # display(all_test_results)

    # -- Default Mappings --
    test_data_setup = None
    test_df, test_data_setup =  ftpaDecided_tests.test_default_mapping_init(json_data)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(ftpaDecided_tests.test_ftpaDecided_defaultValues(test_df, fields_to_exclude))
    # display(all_test_results)

    return classify_all(all_test_results)
