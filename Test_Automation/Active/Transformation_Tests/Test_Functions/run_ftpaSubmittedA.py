import Test_Functions.FTPA_Submitted_A_Tests as ftpa_a_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all


def run_all_tests(json_data, M1_bronze, M1_silver, M2_bronze, M3_bronze, C, bhc, fields_to_exclude):
    all_test_results = []

    # -- ftpa Tests --
    test_data_setup = None
    test_df, test_data_setup =  ftpa_a_tests.test_ftpa_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "ftpaList" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaList_appellant(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaList_respondent(test_df))
         if "ftpaAppellantApplicationDate" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantApplicationDate_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantApplicationDate_test2(test_df))
         if "ftpaAppellantSubmissionOutOfTime" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantSubmissionOutOfTime_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantSubmissionOutOfTime_test2(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantSubmissionOutOfTime_test3(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantSubmissionOutOfTime_test4(test_df))
         if "ftpaAppellantOutOfTimeExplanation" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantOutOfTimeExplanation_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantOutOfTimeExplanation_test2(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantOutOfTimeExplanation_test3(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantOutOfTimeExplanation_test4(test_df))
         if "ftpaRespondentApplicationDate" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentApplicationDate_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentApplicationDate_test2(test_df))
         if "ftpaRespondentSubmissionOutOfTime" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentSubmissionOutOfTime_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentSubmissionOutOfTime_test2(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentSubmissionOutOfTime_test3(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentSubmissionOutOfTime_test4(test_df))
         if "ftpaRespondentOutOfTimeExplanation" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentOutOfTimeExplanation_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentOutOfTimeExplanation_test2(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentOutOfTimeExplanation_test3(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentOutOfTimeExplanation_test4(test_df))
    # display(all_test_results)

    # -- general Tests --

    test_data_setup = None
    test_df, test_data_setup =  ftpa_a_tests.test_ftpa_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "ftpaAppellantSubmitted" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantSubmitted_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantSubmitted_test2(test_df))
         if "isFtpaAppellantDocsVisibleInDecided" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantDocsVisibleInDecided_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantDocsVisibleInDecided_test2(test_df))
         if "isFtpaAppellantDocsVisibleInSubmitted" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantDocsVisibleInSubmitted_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantDocsVisibleInSubmitted_test2(test_df))
         if "isFtpaAppellantOotDocsVisibleInDecided" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotDocsVisibleInDecided_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotDocsVisibleInDecided_test2(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotDocsVisibleInDecided_test3(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotDocsVisibleInDecided_test4(test_df))
         if "isFtpaAppellantOotDocsVisibleInSubmitted" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotDocsVisibleInSubmitted_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotDocsVisibleInSubmitted_test2(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotDocsVisibleInSubmitted_test3(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotDocsVisibleInSubmitted_test4(test_df))
         if "isFtpaAppellantGroundsDocsVisibleInDecided" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantGroundsDocsVisibleInDecided_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantGroundsDocsVisibleInDecided_test2(test_df))
         if "isFtpaAppellantEvidenceDocsVisibleInDecided" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantEvidenceDocsVisibleInDecided_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantEvidenceDocsVisibleInDecided_test2(test_df))
         if "isFtpaAppellantGroundsDocsVisibleInSubmitted" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantGroundsDocsVisibleInSubmitted_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantGroundsDocsVisibleInSubmitted_test2(test_df))
         if "isFtpaAppellantEvidenceDocsVisibleInSubmitted" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantEvidenceDocsVisibleInSubmitted_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantEvidenceDocsVisibleInSubmitted_test2(test_df))
         if "isFtpaAppellantOotExplanationVisibleInDecided" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotExplanationVisibleInDecided_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotExplanationVisibleInDecided_test2(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotExplanationVisibleInDecided_test3(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotExplanationVisibleInDecided_test4(test_df))
         if "isFtpaAppellantOotExplanationVisibleInSubmitted" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotExplanationVisibleInSubmitted_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotExplanationVisibleInSubmitted_test2(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotExplanationVisibleInSubmitted_test3(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaAppellantOotExplanationVisibleInSubmitted_test4(test_df))
         if "ftpaRespondentSubmitted" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentSubmitted_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentSubmitted_test2(test_df))
         if "isFtpaRespondentDocsVisibleInDecided" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentDocsVisibleInDecided_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentDocsVisibleInDecided_test2(test_df))
         if "isFtpaRespondentDocsVisibleInSubmitted" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentDocsVisibleInSubmitted_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentDocsVisibleInSubmitted_test2(test_df))
         if "isFtpaRespondentOotDocsVisibleInDecided" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotDocsVisibleInDecided_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotDocsVisibleInDecided_test2(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotDocsVisibleInDecided_test3(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotDocsVisibleInDecided_test4(test_df))
         if "isFtpaRespondentOotDocsVisibleInSubmitted" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotDocsVisibleInSubmitted_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotDocsVisibleInSubmitted_test2(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotDocsVisibleInSubmitted_test3(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotDocsVisibleInSubmitted_test4(test_df))
         if "isFtpaRespondentGroundsDocsVisibleInDecided" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentGroundsDocsVisibleInDecided_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentGroundsDocsVisibleInDecided_test2(test_df))
         if "isFtpaRespondentEvidenceDocsVisibleInDecided" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentEvidenceDocsVisibleInDecided_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentEvidenceDocsVisibleInDecided_test2(test_df))
         if "isFtpaRespondentGroundsDocsVisibleInSubmitted" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test2(test_df))
         if "isFtpaRespondentEvidenceDocsVisibleInSubmitted" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test2(test_df))
         if "isFtpaRespondentOotExplanationVisibleInDecided" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotExplanationVisibleInDecided_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotExplanationVisibleInDecided_test2(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotExplanationVisibleInDecided_test3(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotExplanationVisibleInDecided_test4(test_df))
         if "isFtpaRespondentOotExplanationVisibleInSubmitted" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotExplanationVisibleInSubmitted_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotExplanationVisibleInSubmitted_test2(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotExplanationVisibleInSubmitted_test3(test_df))
              all_test_results.append(ftpa_a_tests.test_isFtpaRespondentOotExplanationVisibleInSubmitted_test4(test_df))
    # display(all_test_results)

    # -- document Tests --

    test_data_setup = None
    test_df, test_data_setup =  ftpa_a_tests.test_ftpa_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
         if "ftpaAppellantDocuments" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantDocuments_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantDocuments_test2(test_df))
         if "ftpaRespondentDocuments" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentDocuments_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentDocuments_test2(test_df))
         if "ftpaAppellantGroundsDocuments" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantGroundsDocuments_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantGroundsDocuments_test2(test_df))
         if "ftpaRespondentGroundsDocuments" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentGroundsDocuments_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentGroundsDocuments_test2(test_df))
         if "ftpaAppellantEvidenceDocuments" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantEvidenceDocuments_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantEvidenceDocuments_test2(test_df))
         if "ftpaRespondentEvidenceDocuments" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentEvidenceDocuments_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentEvidenceDocuments_test2(test_df))
         if "ftpaAppellantOutOfTimeDocuments" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantOutOfTimeDocuments_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaAppellantOutOfTimeDocuments_test2(test_df))
         if "ftpaRespondentOutOfTimeDocuments" not in fields_to_exclude:
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentOutOfTimeDocuments_test1(test_df))
              all_test_results.append(ftpa_a_tests.test_ftpaRespondentOutOfTimeDocuments_test2(test_df))
    # display(all_test_results)

    # -- Default Mappings --
    test_data_setup = None
    test_df, test_data_setup =  ftpa_a_tests.test_default_mapping_init(json_data)
    if test_data_setup != True:
         all_test_results.append(test_data_setup)

    if test_df != None:
        all_test_results.extend(ftpa_a_tests.test_ftpa_a_defaultValues(test_df, fields_to_exclude))
    # display(all_test_results)

    return classify_all(all_test_results)
