import Test_Functions.Ended_Tests as ended_tests
from models.test_result import TestResult
from Test_Functions.test_helpers import classify_all

def run_all_tests(json_data, M1_bronze, M2_bronze, M3_bronze, M1_silver, M2_silver, bac, M6_bronze, fields_to_exclude):
    all_test_results = []

    # ---------------------------------------------------------
    # 1. Default Mappings
    # ---------------------------------------------------------
    test_df, test_data_setup = ended_tests.test_default_mapping_init(json_data, M1_silver, M3_bronze)
    
    if test_data_setup is True and test_df is not None:
        all_test_results.extend(ended_tests.test_ended_defaultValues(test_df, fields_to_exclude))
    elif test_data_setup is not True:
        all_test_results.append(test_data_setup)

    # ---------------------------------------------------------
    # 2. caseData Tests
    # ---------------------------------------------------------
    test_df, test_data_setup = ended_tests.test_caseData_init(json_data, M1_bronze, M3_bronze)
    
    if test_data_setup is True and test_df is not None:
        if "outOfTimeDecisionType" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_outOfTimeDecisionType_test1(test_df))
            all_test_results.append(ended_tests.test_outOfTimeDecisionType_test2(test_df))
            all_test_results.append(ended_tests.test_outOfTimeDecisionType_test3(test_df))
    elif test_data_setup is not True:
        all_test_results.append(test_data_setup)

    # ---------------------------------------------------------
    # 3. hearingRequirements Tests
    # ---------------------------------------------------------
    test_df, test_data_setup = ended_tests.test_hearingRequirements_init(json_data, M1_bronze, M3_bronze, bac)
    
    if test_data_setup is True and test_df is not None:
        if "isEvidenceFromOutsideUkOoc" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isEvidenceFromOutsideUkOoc_test1(test_df))
        
        if "isEvidenceFromOutsideUkInCountry" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isEvidenceFromOutsideUkInCountry_test1(test_df))

        if "isInterpreterServicesNeeded" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isInterpreterServicesNeeded_test1(test_df))
            all_test_results.append(ended_tests.test_isInterpreterServicesNeeded_test2(test_df))
            
        if "singleSexCourt" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_singleSexCourt_test1(test_df))
            all_test_results.append(ended_tests.test_singleSexCourt_test2(test_df))     

        if "inCameraCourt" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_inCameraCourt_test1(test_df))
            all_test_results.append(ended_tests.test_inCameraCourt_test2(test_df)) 

    # --- Self-Contained hearingRequirements Tests ---
    if "singleSexCourtType" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_singleSexCourtType_test1(json_data, M1_bronze, M3_bronze))
        all_test_results.append(ended_tests.test_singleSexCourtType_test2(json_data, M1_bronze, M3_bronze))
        all_test_results.append(ended_tests.test_singleSexCourtType_test3(json_data, M1_bronze, M3_bronze))

    if "singleSexCourtTypeDescription" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_singleSexCourtTypeDescription_test1(json_data, M1_bronze, M3_bronze))
        all_test_results.append(ended_tests.test_singleSexCourtTypeDescription_test2(json_data, M1_bronze, M3_bronze))
        all_test_results.append(ended_tests.test_singleSexCourtTypeDescription_test3(json_data, M1_bronze, M3_bronze))

    if "inCameraCourtDescription" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_inCameraCourtDescription_test1(json_data, M1_bronze, M3_bronze))
        all_test_results.append(ended_tests.test_inCameraCourtDescription_test2(json_data, M1_bronze, M3_bronze))

    # ---------------------------------------------------------
    # 4. hearingResponse Tests
    # ---------------------------------------------------------
    test_df, test_data_setup = ended_tests.test_hearingResponse_init(json_data, M1_bronze, M3_bronze, bac, M6_bronze, M1_silver, M2_bronze)

    if test_data_setup is True and test_df is not None:
        if "isAppealSuitableToFloat" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isAppealSuitableToFloat_test1(test_df))
            all_test_results.append(ended_tests.test_isAppealSuitableToFloat_test2(test_df))
            all_test_results.append(ended_tests.test_isAppealSuitableToFloat_test3(test_df))
        if "listingLength" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_listingLength_test1(test_df))
            all_test_results.append(ended_tests.test_listingLength_test2(test_df))
        if "hearingChannel" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_hearingChannel_test1(test_df))
            all_test_results.append(ended_tests.test_hearingChannel_test2(test_df))
        if "listingLocation" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_listingLocation_test1(test_df))
        if "listCaseHearingLength" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_listCaseHearingLength_test1(test_df))
        if "listCaseHearingDate" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_listCaseHearingDate_test1(test_df))
        if "listCaseHearingCentre" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_listCaseHearingCentre_test1(test_df))
        if "listCaseHearingCentreAddress" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_listCaseHearingCentreAddress_test1(test_df))

    # --- Self-Contained hearingResponse Tests ---
    if "isInCameraCourtAllowed" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_isInCameraCourtAllowed_test1(json_data, M1_bronze, M3_bronze))
        all_test_results.append(ended_tests.test_isInCameraCourtAllowed_test2(json_data, M1_bronze, M3_bronze))
    if "inCameraCourtTribunalResponse" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_inCameraCourtTribunalResponse_test1(json_data, M1_bronze, M3_bronze))
        all_test_results.append(ended_tests.test_inCameraCourtTribunalResponse_test2(json_data, M1_bronze, M3_bronze))
    if "inCameraCourtDecisionForDisplay" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_inCameraCourtDecisionForDisplay_test1(json_data, M1_bronze, M3_bronze))
        all_test_results.append(ended_tests.test_inCameraCourtDecisionForDisplay_test2(json_data, M1_bronze, M3_bronze))
    if "isSingleSexCourtAllowed" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_isSingleSexCourtAllowed_test1(json_data, M1_bronze, M3_bronze))
        all_test_results.append(ended_tests.test_isSingleSexCourtAllowed_test2(json_data, M1_bronze, M3_bronze))
    if "singleSexCourtTribunalResponse" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_singleSexCourtTribunalResponse_test1(json_data, M1_bronze, M3_bronze))
        all_test_results.append(ended_tests.test_singleSexCourtTribunalResponse_test2(json_data, M1_bronze, M3_bronze))
    if "singleSexCourtDecisionForDisplay" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_singleSexCourtDecisionForDisplay_test1(json_data, M1_bronze, M3_bronze))
        all_test_results.append(ended_tests.test_singleSexCourtDecisionForDisplay_test2(json_data, M1_bronze, M3_bronze))

    # ---------------------------------------------------------
    # 5. substantiveDecision Tests
    # ---------------------------------------------------------
    if test_data_setup is True and test_df is not None:
        if "sendDecisionsAndReasonsDate" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_sendDecisionsAndReasonsDate_test1(test_df))
        if "appealDate" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_appealDate_test1(test_df))
        if "appealDecision" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_appealDecision_test1(test_df))
            all_test_results.append(ended_tests.test_appealDecision_test2(test_df))
        if "isDecisionAllowed" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isDecisionAllowed_test1(test_df))
            all_test_results.append(ended_tests.test_isDecisionAllowed_test2(test_df))

    # ---------------------------------------------------------
    # 6. hearingActuals Tests
    # ---------------------------------------------------------
    if test_data_setup is True and test_df is not None:
        if "attendingJudge" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_attendingJudge_test1(test_df))   
        if "actualCaseHearingLength" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_actualCaseHearingLength_test1(test_df))   
        
    # ---------------------------------------------------------
    # 7. ftpa Tests (Appellant)
    # ---------------------------------------------------------
    if test_data_setup is True and test_df is not None:
        if "ftpaApplicationDeadline" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_ftpaApplicationDeadline_test1(test_df))   
        if "ftpaList" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_ftpaList_test1(test_df)) 
        if "ftpaAppellantApplicationDate" in ["ftpaAppellantApplicationDate"]:
            all_test_results.append(ended_tests.test_ftpaAppellantApplicationDate_test1(test_df))
            all_test_results.append(ended_tests.test_ftpaAppellantApplicationDate_test2(test_df))
        if "ftpaAppellantSubmissionOutOfTime" in ["ftpaAppellantSubmissionOutOfTime"]:
            all_test_results.append(ended_tests.test_ftpaAppellantSubmissionOutOfTime_test1(test_df))
            all_test_results.append(ended_tests.test_ftpaAppellantSubmissionOutOfTime_test2(test_df))
            all_test_results.append(ended_tests.test_ftpaAppellantSubmissionOutOfTime_test3(test_df))
            all_test_results.append(ended_tests.test_ftpaAppellantSubmissionOutOfTime_test4(test_df))
        if "ftpaAppellantOutOfTimeExplanation" in ["ftpaAppellantOutOfTimeExplanation"]:
            all_test_results.append(ended_tests.test_ftpaAppellantOutOfTimeExplanation_test1(test_df))
            all_test_results.append(ended_tests.test_ftpaAppellantOutOfTimeExplanation_test2(test_df))
            all_test_results.append(ended_tests.test_ftpaAppellantOutOfTimeExplanation_test3(test_df))
            all_test_results.append(ended_tests.test_ftpaAppellantOutOfTimeExplanation_test4(test_df))

    # --- Self-Contained FTPA Respondent Tests ---
    if "ftpaRespondentApplicationDate" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_ftpaRespondentApplicationDate_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_ftpaRespondentApplicationDate_test2(json_data, M3_bronze))
    if "ftpaRespondentSubmissionOutOfTime" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_ftpaRespondentSubmissionOutOfTime_test1(json_data, M3_bronze))
    if "ftpaRespondentOutOfTimeExplanation" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_ftpaRespondentOutOfTimeExplanation_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_ftpaRespondentOutOfTimeExplanation_test2(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_ftpaRespondentOutOfTimeExplanation_test3(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_ftpaRespondentOutOfTimeExplanation_test4(json_data, M3_bronze))

    # ---------------------------------------------------------
    # 8. additionalInstructionsTribunalResponse Test
    # ---------------------------------------------------------
    test_df_instr, test_data_setup_instr = ended_tests.test_additionalInstructionsTribunalResponse_init(json_data, M3_bronze, M6_bronze)
    if test_data_setup_instr is True and test_df_instr is not None:
        all_test_results.append(ended_tests.test_additionalInstructionsTribunalResponse(test_df_instr))
    else:
        all_test_results.append(test_data_setup_instr)

    # ---------------------------------------------------------
    # 9. Ended State Specific Tests
    # ---------------------------------------------------------
    test_df_ended, test_data_setup_ended = ended_tests.test_ended_init(json_data, M1_bronze, M3_bronze, M1_silver)
    if test_data_setup_ended is True and test_df_ended is not None:
        if "endAppealOutcome" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_endAppealOutcome_test1(test_df_ended))
        if "endAppealOutcomeReason" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_endAppealOutcomeReason_test1(test_df_ended))            
        if "endAppealApproverType" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_endAppealApproverType_test1(test_df_ended))            
        if "endAppealApproverName" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_endAppealApproverName_test1(test_df_ended))
        if "endAppealDate" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_endAppealDate_test1(test_df_ended))
        if "stateBeforeEndAppeal" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_stateBeforeEndAppeal_test1(test_df_ended))

    # ---------------------------------------------------------
    # 10. general Tests
    # ---------------------------------------------------------
    test_df_gen, test_data_setup_gen = ended_tests.test_general_init(json_data, M1_bronze, M2_bronze, M3_bronze, M1_silver, bac)

    if test_data_setup_gen is True and test_df_gen is not None:
        if "bundleFileNamePrefix" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_bundleFileNamePrefix_test1(test_df_gen)) 
        
        if "ftpaAppellantSubmitted" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_ftpaAppellantSubmitted_test1(test_df_gen))
            all_test_results.append(ended_tests.test_ftpaAppellantSubmitted_test2(test_df_gen))
        if "isFtpaAppellantDocsVisibleInDecided" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isFtpaAppellantDocsVisibleInDecided_test1(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantDocsVisibleInDecided_test2(test_df_gen))
        if "isFtpaAppellantDocsVisibleInSubmitted" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isFtpaAppellantDocsVisibleInSubmitted_test1(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantDocsVisibleInSubmitted_test2(test_df_gen))
        if "isFtpaAppellantOotDocsVisibleInDecided" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isFtpaAppellantOotDocsVisibleInDecided_test1(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantOotDocsVisibleInDecided_test2(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantOotDocsVisibleInDecided_test3(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantOotDocsVisibleInDecided_test3(test_df_gen))
        if "isFtpaAppellantOotDocsVisibleInSubmitted" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isFtpaAppellantOotDocsVisibleInSubmitted_test1(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantOotDocsVisibleInSubmitted_test2(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantOotDocsVisibleInSubmitted_test3(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantOotDocsVisibleInSubmitted_test4(test_df_gen))
        if "isFtpaAppellantGroundsDocsVisibleInDecided" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isFtpaAppellantGroundsDocsVisibleInDecided_test1(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantGroundsDocsVisibleInDecided_test2(test_df_gen))
        if "isFtpaAppellantEvidenceDocsVisibleInDecided" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isFtpaAppellantEvidenceDocsVisibleInDecided_test1(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantEvidenceDocsVisibleInDecided_test2(test_df_gen))
        if "isFtpaAppellantGroundsDocsVisibleInSubmitted" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isFtpaAppellantGroundsDocsVisibleInSubmitted_test1(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantGroundsDocsVisibleInSubmitted_test2(test_df_gen))
        if "isFtpaAppellantEvidenceDocsVisibleInSubmitted" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isFtpaAppellantEvidenceDocsVisibleInSubmitted_test1(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantEvidenceDocsVisibleInSubmitted_test2(test_df_gen))
        if "isFtpaAppellantOotExplanationVisibleInDecided" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isFtpaAppellantOotExplanationVisibleInDecided_test1(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantOotExplanationVisibleInDecided_test2(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantOotExplanationVisibleInDecided_test3(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantOotExplanationVisibleInDecided_test4(test_df_gen))
        if "isFtpaAppellantOotExplanationVisibleInSubmitted" not in fields_to_exclude:
            all_test_results.append(ended_tests.test_isFtpaAppellantOotExplanationVisibleInSubmitted_test1(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantOotExplanationVisibleInSubmitted_test2(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantOotExplanationVisibleInSubmitted_test3(test_df_gen))
            all_test_results.append(ended_tests.test_isFtpaAppellantOotExplanationVisibleInSubmitted_test4(test_df_gen))

    # --- Self-Contained FTPA Respondent Visibility Tests ---
    if "ftpaRespondentSubmitted" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_ftpaRespondentSubmitted_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_ftpaRespondentSubmitted_test2(json_data, M3_bronze))
    if "isFtpaRespondentDocsVisibleInDecided" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_isFtpaRespondentDocsVisibleInDecided_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentDocsVisibleInDecided_test2(json_data, M3_bronze))
    if "isFtpaRespondentDocsVisibleInSubmitted" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_isFtpaRespondentDocsVisibleInSubmitted_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentDocsVisibleInSubmitted_test2(json_data, M3_bronze))
    if "isFtpaRespondentOotDocsVisibleInDecided" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_isFtpaRespondentOotDocsVisibleInDecided_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentOotDocsVisibleInDecided_test2(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentOotDocsVisibleInDecided_test3(json_data, M3_bronze))
    if "isFtpaRespondentOotDocsVisibleInSubmitted" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_isFtpaRespondentOotDocsVisibleInSubmitted_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentOotDocsVisibleInSubmitted_test2(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentOotDocsVisibleInSubmitted_test3(json_data, M3_bronze))
    if "isFtpaRespondentGroundsDocsVisibleInDecided" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_isFtpaRespondentGroundsDocsVisibleInDecided_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentGroundsDocsVisibleInDecided_test2(json_data, M3_bronze))
    if "isFtpaRespondentEvidenceDocsVisibleInDecided" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_isFtpaRespondentEvidenceDocsVisibleInDecided_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentEvidenceDocsVisibleInDecided_test2(json_data, M3_bronze))
    if "isFtpaRespondentGroundsDocsVisibleInSubmitted" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test2(json_data, M3_bronze))
    if "isFtpaRespondentEvidenceDocsVisibleInSubmitted" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test2(json_data, M3_bronze))
    if "isFtpaRespondentOotExplanationVisibleInDecided" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_isFtpaRespondentOotExplanationVisibleInDecided_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentOotExplanationVisibleInDecided_test2(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentOotExplanationVisibleInDecided_test3(json_data, M3_bronze))
    if "isFtpaRespondentOotExplanationVisibleInSubmitted" not in fields_to_exclude:
        all_test_results.append(ended_tests.test_isFtpaRespondentOotExplanationVisibleInSubmitted_test1(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentOotExplanationVisibleInSubmitted_test2(json_data, M3_bronze))
        all_test_results.append(ended_tests.test_isFtpaRespondentOotExplanationVisibleInSubmitted_test3(json_data, M3_bronze))

    # ---------------------------------------------------------
    # 11. appellantInterpreter Tests
    # ---------------------------------------------------------
    test_df_lang, test_data_setup_lang = ended_tests.test_languages_init(json_data, M1_bronze, M3_bronze)
    if test_data_setup_lang is True and test_df_lang is not None:
        all_test_results.append(ended_tests.test_languageInterpreterMapping(test_df_lang))
    else:
        all_test_results.append(test_data_setup_lang)

    return classify_all(all_test_results)