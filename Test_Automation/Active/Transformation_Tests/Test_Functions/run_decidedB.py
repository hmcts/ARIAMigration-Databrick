import Test_Functions.Decided_B_Tests as dec_b_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, M6_bronze, C, bhc, fields_to_exclude):
    all_test_results = []

    # -- ftpa Tests --
    test_data_setup = None
    test_df, test_data_setup =  dec_b_tests.test_ftpa_init(json_data, M1_bronze, M3_bronze, M6_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "ftpaApplicantType" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_ftpaApplicantType_test1(test_df))
              all_test_results.append(dec_b_tests.test_ftpaApplicantType_test2(test_df))
         if "ftpaAppellantDecisionDate" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_ftpaAppellantDecisionDate_test1(test_df))
              all_test_results.append(dec_b_tests.test_ftpaAppellantDecisionDate_test2(test_df))
         if "ftpaRespondentDecisionDate" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_ftpaRespondentDecisionDate_test1(test_df))
              all_test_results.append(dec_b_tests.test_ftpaRespondentDecisionDate_test2(test_df))
         if "ftpaAppellantRjDecisionOutcomeType" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_ftpaAppellantRjDecisionOutcomeType_test1(test_df))
              all_test_results.append(dec_b_tests.test_ftpaAppellantRjDecisionOutcomeType_test2(test_df))
         if "ftpaRespondentRjDecisionOutcomeType" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_ftpaRespondentRjDecisionOutcomeType_test1(test_df))
              all_test_results.append(dec_b_tests.test_ftpaRespondentRjDecisionOutcomeType_test2(test_df))

    # display(all_test_results)

    # -- setAside Tests --

    test_data_setup = None
    test_df, test_data_setup =  dec_b_tests.test_ftpa_init(json_data, M1_bronze, M3_bronze, M6_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "judgesNamesToExclude" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_judgesNamesToExclude_test1(test_df, M6_bronze))
         if "updateTribunalDecisionDateRule32" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_updateTribunalDecisionDateRule32_test1(test_df))
         if "ftpaAppellantDecisionRemadeRule32Text" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_ftpaAppellantDecisionRemadeRule32Text_test1(test_df))
              all_test_results.append(dec_b_tests.test_ftpaAppellantDecisionRemadeRule32Text_test2(test_df))
         if "ftpaRespondentDecisionRemadeRule32Text" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_ftpaRespondentDecisionRemadeRule32Text_test1(test_df))
              all_test_results.append(dec_b_tests.test_ftpaRespondentDecisionRemadeRule32Text_test2(test_df))


    # -- general Tests --

    test_data_setup = None
    test_df, test_data_setup =  dec_b_tests.test_ftpa_init(json_data, M1_bronze, M3_bronze, M6_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "isFtpaAppellantDecided" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_isFtpaAppellantDecided_test1(test_df))
         if "isFtpaRespondentDecided" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_isFtpaRespondentDecided_test1(test_df))


    # -- document Tests --

    test_data_setup = None
    test_df, test_data_setup =  dec_b_tests.test_ftpa_init(json_data, M1_bronze, M3_bronze, M6_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "allFtpaAppellantDecisionDocs" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_allFtpaAppellantDecisionDocs_test1(test_df))
              all_test_results.append(dec_b_tests.test_allFtpaAppellantDecisionDocs_test2(test_df))
         if "allFtpaRespondentDecisionDocs" not in fields_to_exclude:
              all_test_results.append(dec_b_tests.test_allFtpaRespondentDecisionDocs_test1(test_df))
              all_test_results.append(dec_b_tests.test_allFtpaRespondentDecisionDocs_test2(test_df))

    # -- Default Mappings --
    test_data_setup = None
    test_df, test_data_setup =  dec_b_tests.test_default_mapping_init(json_data)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(dec_b_tests.test_dec_b_defaultValues(test_df, fields_to_exclude))
    # display(all_test_results)

    return classify_all(all_test_results)
