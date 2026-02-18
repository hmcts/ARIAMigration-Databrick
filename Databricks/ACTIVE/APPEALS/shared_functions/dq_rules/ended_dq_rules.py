from .dq_rules import DQRulesBase


class endedDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_ended()
        checks = checks | self.get_checks_document()
        checks = checks | self.get_checks_hearingDetails()
        checks = checks | self.get_checks_ftpa()
        checks = checks | self.get_checks_hearingActuals()
        checks = checks | self.get_checks_substantiveDecision()
        checks = checks | self.get_checks_hearingResponse()
        checks = checks | self.get_checks_hearingRequirements()
        checks = checks | self.get_checks_general()

        return checks


    def get_checks_ended(self, checks={}):

        checks["valid_endAppealOutcome"] = """(
            endAppealOutcome = endAppealOutcome_end
            OR
            (endAppealOutcome IS NULL AND endAppealOutcome_end IS NULL)
        )"""

        checks["valid_endAppealOutcomeReason"] = """(
            endAppealOutcomeReason = endAppealOutcomeReason_end
            OR
            (endAppealOutcomeReason IS NULL AND endAppealOutcomeReason_end IS NULL)
        )"""

        checks["valid_endAppealApproverType"] = """(
            CASE 
                WHEN CaseStatus_end = 46 THEN endAppealApproverType = 'Judge'
                ELSE endAppealApproverType = 'Case Worker'
            END
        )"""

        checks["valid_endAppealApproverName"] = """(
            CASE 
                WHEN CaseStatus_end = 46 THEN endAppealApproverName = concat(Adj_Determination_Title_end, ' ', Adj_Determination_Forenames_end, ' ', Adj_Determination_Surname_end)
                ELSE endAppealApproverName = 'This is a migrated ARIA case'
            END
        )"""

        checks["valid_endAppealDate"] = """(
            endAppealDate = decision_ts
        )"""

        checks["valid_stateBeforeEndAppeal"] = """(
            stateBeforeEndAppeal = stateBeforeEndAppeal_end
            OR
            (stateBeforeEndAppeal IS NULL AND stateBeforeEndAppeal_end IS NULL)
        )"""

        return checks


    def get_checks_document(checks=None):

        checks["valid_respondentDocuments"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 26 AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(respondentDocuments, ARRAY()) = COALESCE(respondentDocuments_ended, ARRAY())
                ELSE
                    respondentDocuments IS NULL
            END
        )
        """

        checks["valid_hearingRequirements"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(hearingRequirements, ARRAY()) = COALESCE(hearingRequirements_ended, ARRAY())
                ELSE
                    hearingRequirements IS NULL
            END
        )
        """

        checks["valid_hearingDocuments"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(hearingDocuments, ARRAY()) = COALESCE(hearingDocuments_ended, ARRAY())
                ELSE
                    hearingDocuments IS NULL
            END
        )
        """

        checks["valid_letterBundleDocuments"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(letterBundleDocuments, ARRAY()) = COALESCE(letterBundleDocuments_ended, ARRAY())
                ELSE
                    letterBundleDocuments IS NULL
            END
        )
        """

        checks["valid_caseBundles"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(caseBundles, ARRAY()) = COALESCE(caseBundles_ended, ARRAY())
                ELSE
                    caseBundles IS NULL
            END
        )
        """

        checks["valid_finalDecisionAndReasonsDocuments"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(finalDecisionAndReasonsDocuments, ARRAY()) = COALESCE(finalDecisionAndReasonsDocuments_ended, ARRAY())
                ELSE
                    finalDecisionAndReasonsDocuments IS NULL
            END
        )
        """

        checks["valid_ftpaAppellantDocuments"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(ftpaAppellantDocuments, ARRAY()) = COALESCE(ftpaAppellantDocuments_ended, ARRAY())
                ELSE
                    ftpaAppellantDocuments IS NULL
            END
        )
        """

        checks["valid_ftpaRespondentDocuments"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(ftpaRespondentDocuments, ARRAY()) = COALESCE(ftpaRespondentDocuments_ended, ARRAY())
                ELSE
                    ftpaRespondentDocuments IS NULL
            END
        )
        """

        checks["valid_ftpaAppellantGroundsDocuments"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(ftpaAppellantGroundsDocuments, ARRAY()) = COALESCE(ftpaAppellantGroundsDocuments_ended, ARRAY())
                ELSE
                    ftpaAppellantGroundsDocuments IS NULL
            END
        )
        """

        checks["valid_ftpaRespondentGroundsDocuments"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(ftpaRespondentGroundsDocuments, ARRAY()) = COALESCE(ftpaRespondentGroundsDocuments_ended, ARRAY())
                ELSE
                    ftpaRespondentGroundsDocuments IS NULL
            END
        )
        """

        checks["valid_ftpaAppellantEvidenceDocuments"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(ftpaAppellantEvidenceDocuments, ARRAY()) = COALESCE(ftpaAppellantEvidenceDocuments_ended, ARRAY())
                ELSE
                    ftpaAppellantEvidenceDocuments IS NULL
            END
        )
        """

        checks["valid_ftpaRespondentEvidenceDocuments"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(ftpaRespondentEvidenceDocuments, ARRAY()) = COALESCE(ftpaRespondentEvidenceDocuments_ended, ARRAY())
                ELSE
                    ftpaRespondentEvidenceDocuments IS NULL
            END
        )
        """

        checks["valid_ftpaAppellantOutOfTimeDocuments"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(ftpaAppellantOutOfTimeDocuments, ARRAY()) = COALESCE(ftpaAppellantOutOfTimeDocuments_ended, ARRAY())
                ELSE
                    ftpaAppellantOutOfTimeDocuments IS NULL
            END
        )
        """

        checks["valid_ftpaRespondentOutOfTimeDocuments"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(ftpaRespondentOutOfTimeDocuments, ARRAY()) = COALESCE(ftpaRespondentOutOfTimeDocuments_ended, ARRAY())
                ELSE
                    ftpaRespondentOutOfTimeDocuments IS NULL
            END
        )
        """

        return checks

    def get_checks_hearingDetails(checks=None):

        checks["valid_listingLength"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(listingLength), '{}') = coalesce(to_json(listingLength_ended), '{}')
                ELSE
                    listingLength IS NULL
            END
        )
        """

        checks["valid_hearingChannel"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(hearingChannel), '{}') = coalesce(to_json(hearingChannel_ended), '{}')
                ELSE
                    hearingChannel IS NULL
            END
        )
        """

        checks["valid_witnessDetails"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(witnessDetails, ARRAY()) = COALESCE(witnessDetails_ended, ARRAY())
                ELSE
                    witnessDetails IS NULL
            END
        )
        """

        checks["valid_listingLocation"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(listingLocation), '{}') = coalesce(to_json(listingLocation_ended), '{}')
                ELSE
                    listingLocation IS NULL
            END
        )
        """

        checks["valid_witness1InterpreterSignLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness1InterpreterSignLanguage), '{}') = coalesce(to_json(witness1InterpreterSignLanguage_ended), '{}')
                ELSE
                    witness1InterpreterSignLanguage IS NULL
            END
        )
        """

        checks["valid_witness2InterpreterSignLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness2InterpreterSignLanguage), '{}') = coalesce(to_json(witness2InterpreterSignLanguage_ended), '{}')
                ELSE
                    witness2InterpreterSignLanguage IS NULL
            END
        )
        """

        checks["valid_witness3InterpreterSignLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness3InterpreterSignLanguage), '{}') = coalesce(to_json(witness3InterpreterSignLanguage_ended), '{}')
                ELSE
                    witness3InterpreterSignLanguage IS NULL
            END
        )
        """

        checks["valid_witness4InterpreterSignLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness4InterpreterSignLanguage), '{}') = coalesce(to_json(witness4InterpreterSignLanguage_ended), '{}')
                ELSE
                    witness4InterpreterSignLanguage IS NULL
            END
        )
        """

        checks["valid_witness5InterpreterSignLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness5InterpreterSignLanguage), '{}') = coalesce(to_json(witness5InterpreterSignLanguage_ended), '{}')
                ELSE
                    witness5InterpreterSignLanguage IS NULL
            END
        )
        """

        checks["valid_witness6InterpreterSignLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness6InterpreterSignLanguage), '{}') = coalesce(to_json(witness6InterpreterSignLanguage_ended), '{}')
                ELSE
                    witness6InterpreterSignLanguage IS NULL
            END
        )
        """

        checks["valid_witness7InterpreterSignLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness7InterpreterSignLanguage), '{}') = coalesce(to_json(witness7InterpreterSignLanguage_ended), '{}')
                ELSE
                    witness7InterpreterSignLanguage IS NULL
            END
        )
        """

        checks["valid_witness8InterpreterSignLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness8InterpreterSignLanguage), '{}') = coalesce(to_json(witness8InterpreterSignLanguage_ended), '{}')
                ELSE
                    witness8InterpreterSignLanguage IS NULL
            END
        )
        """

        checks["valid_witness9InterpreterSignLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness9InterpreterSignLanguage), '{}') = coalesce(to_json(witness9InterpreterSignLanguage_ended), '{}')
                ELSE
                    witness9InterpreterSignLanguage IS NULL
            END
        )
        """

        checks["valid_witness10InterpreterSignLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness10InterpreterSignLanguage), '{}') = coalesce(to_json(witness10InterpreterSignLanguage_ended), '{}')
                ELSE
                    witness10InterpreterSignLanguage IS NULL
            END
        )
        """

        checks["valid_witness1InterpreterSpokenLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness1InterpreterSpokenLanguage), '{}') = coalesce(to_json(witness1InterpreterSpokenLanguage_ended), '{}')
                ELSE
                    witness1InterpreterSpokenLanguage IS NULL
            END
        )
        """

        checks["valid_witness2InterpreterSpokenLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness2InterpreterSpokenLanguage), '{}') = coalesce(to_json(witness2InterpreterSpokenLanguage_ended), '{}')
                ELSE
                    witness2InterpreterSpokenLanguage IS NULL
            END
        )
        """

        checks["valid_witness3InterpreterSpokenLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness3InterpreterSpokenLanguage), '{}') = coalesce(to_json(witness3InterpreterSpokenLanguage_ended), '{}')
                ELSE
                    witness3InterpreterSpokenLanguage IS NULL
            END
        )
        """

        checks["valid_witness4InterpreterSpokenLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness4InterpreterSpokenLanguage), '{}') = coalesce(to_json(witness4InterpreterSpokenLanguage_ended), '{}')
                ELSE
                    witness4InterpreterSpokenLanguage IS NULL
            END
        )
        """

        checks["valid_witness5InterpreterSpokenLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness5InterpreterSpokenLanguage), '{}') = coalesce(to_json(witness5InterpreterSpokenLanguage_ended), '{}')
                ELSE
                    witness5InterpreterSpokenLanguage IS NULL
            END
        )
        """

        checks["valid_witness6InterpreterSpokenLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness6InterpreterSpokenLanguage), '{}') = coalesce(to_json(witness6InterpreterSpokenLanguage_ended), '{}')
                ELSE
                    witness6InterpreterSpokenLanguage IS NULL
            END
        )
        """

        checks["valid_witness7InterpreterSpokenLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness7InterpreterSpokenLanguage), '{}') = coalesce(to_json(witness7InterpreterSpokenLanguage_ended), '{}')
                ELSE
                    witness7InterpreterSpokenLanguage IS NULL
            END
        )
        """

        checks["valid_witness8InterpreterSpokenLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness8InterpreterSpokenLanguage), '{}') = coalesce(to_json(witness8InterpreterSpokenLanguage_ended), '{}')
                ELSE
                    witness8InterpreterSpokenLanguage IS NULL
            END
        )
        """

        checks["valid_witness9InterpreterSpokenLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness9InterpreterSpokenLanguage), '{}') = coalesce(to_json(witness9InterpreterSpokenLanguage_ended), '{}')
                ELSE
                    witness9InterpreterSpokenLanguage IS NULL
            END
        )
        """

        checks["valid_witness10InterpreterSpokenLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(witness10InterpreterSpokenLanguage), '{}') = coalesce(to_json(witness10InterpreterSpokenLanguage_ended), '{}')
                ELSE
                    witness10InterpreterSpokenLanguage IS NULL
            END
        )
        """

        checks["valid_listCaseHearingLength"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN             
                    COALESCE(listCaseHearingLength, ARRAY()) = COALESCE(listCaseHearingLength_ended, ARRAY())
                ELSE
                    SIZE(COALESCE(listCaseHearingLength, ARRAY())) = 0
            END
        )
        """


        checks["valid_listCaseHearingDate"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(listCaseHearingDate, '') = coalesce(listCaseHearingDate_ended, '')
                ELSE
                    listCaseHearingDate IS NULL
            END
        )
        """


        checks["valid_listCaseHearingCentre"] = """
        (
            CASE 
                WHEN (CaseStatus_ended = 39 AND Outcome_ended = 25) THEN
                    ARRAY_SORT(COALESCE(listCaseHearingCentre, ARRAY())) =
                    ARRAY_SORT(COALESCE(listCaseHearingCentre_ended, ARRAY()))
                ELSE
                    SIZE(COALESCE(listCaseHearingCentre, ARRAY())) = 0
            END
        )
        """



        checks["valid_listCaseHearingCentreAddress"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(listCaseHearingCentreAddress, '') = coalesce(listCaseHearingCentreAddress_ended, '')
                ELSE
                    listCaseHearingCentreAddress IS NULL
            END
        )
        """

        return checks


    def get_checks_ftpa(self, checks={}):

        checks["valid_ftpaApplicationDeadline"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(ftpaApplicationDeadline, '') = coalesce(ftpaApplicationDeadline_ended, '')
                ELSE
                    ftpaApplicationDeadline IS NULL
            END
        )
        """

        checks["valid_ftpaList"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(ftpaList), '{}') = coalesce(to_json(ftpaList_ended), '{}')
                ELSE
                    ftpaList IS NULL
            END
        )
        """


        checks["valid_ftpaAppellantApplicationDate"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(ftpaAppellantApplicationDate, '') = coalesce(ftpaAppellantApplicationDate_ended, '')
                ELSE
                    ftpaAppellantApplicationDate IS NULL
            END
        )
        """

        checks["valid_ftpaAppellantSubmissionOutOfTime"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(ftpaAppellantSubmissionOutOfTime, '') = coalesce(ftpaAppellantSubmissionOutOfTime_ended, '')
                ELSE
                    ftpaAppellantSubmissionOutOfTime IS NULL
            END
        )
        """

        checks["valid_ftpaAppellantOutOfTimeExplanation"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(ftpaAppellantOutOfTimeExplanation, '') = coalesce(ftpaAppellantOutOfTimeExplanation_ended, '')
                ELSE
                    ftpaAppellantOutOfTimeExplanation IS NULL
            END
        )
        """

        checks["valid_ftpaRespondentApplicationDate"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(ftpaRespondentApplicationDate, '') = coalesce(ftpaRespondentApplicationDate_ended, '')
                ELSE
                    ftpaRespondentApplicationDate IS NULL
            END
        )
        """

        checks["valid_ftpaRespondentSubmissionOutOfTime"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(ftpaRespondentSubmissionOutOfTime, '') = coalesce(ftpaRespondentSubmissionOutOfTime_ended, '')
                ELSE
                    ftpaRespondentSubmissionOutOfTime IS NULL
            END
        )
        """

        checks["valid_ftpaRespondentOutOfTimeExplanation"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(ftpaRespondentOutOfTimeExplanation, '') = coalesce(ftpaRespondentOutOfTimeExplanation_ended, '')
                ELSE
                    ftpaRespondentOutOfTimeExplanation IS NULL
            END
        )
        """

        return checks

    def get_checks_hearingActuals(self, checks={}):

        checks["valid_attendingJudge"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(attendingJudge, '') = coalesce(attendingJudge_ended, '')
                ELSE
                    attendingJudge IS NULL
            END
        )
        """

        checks["valid_actualCaseHearingLength"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(actualCaseHearingLength), '{}') = coalesce(to_json(actualCaseHearingLength_ended), '{}')
                ELSE
                    actualCaseHearingLength IS NULL
            END
        )
        """

        return checks

    def get_checks_substantiveDecision(self, checks={}):

        checks["valid_scheduleOfIssuesAgreement"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(scheduleOfIssuesAgreement, '') = coalesce(scheduleOfIssuesAgreement_ended, '')
                ELSE
                    scheduleOfIssuesAgreement IS NULL
            END
        )
        """

        checks["valid_scheduleOfIssuesDisagreementDescription"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(scheduleOfIssuesDisagreementDescription, '') = coalesce(scheduleOfIssuesDisagreementDescription_ended, '')
                ELSE
                    scheduleOfIssuesDisagreementDescription IS NULL
            END
        )
        """

        checks["valid_immigrationHistoryAgreement"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(immigrationHistoryAgreement, '') = coalesce(immigrationHistoryAgreement_ended, '')
                ELSE
                    immigrationHistoryAgreement IS NULL
            END
        )
        """

        checks["valid_immigrationHistoryDisagreementDescription"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(immigrationHistoryDisagreementDescription, '') = coalesce(immigrationHistoryDisagreementDescription_ended, '')
                ELSE
                    immigrationHistoryDisagreementDescription IS NULL
            END
        )
        """

        checks["valid_sendDecisionsAndReasonsDate"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(sendDecisionsAndReasonsDate, '') = coalesce(sendDecisionsAndReasonsDate_ended, '')
                ELSE
                    sendDecisionsAndReasonsDate IS NULL
            END
        )
        """

        checks["valid_appealDate"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(appealDate, '') = coalesce(appealDate_ended, '')
                ELSE
                    appealDate IS NULL
            END
        )
        """

        checks["valid_appealDecision"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(appealDecision, '') = coalesce(appealDecision_ended, '')
                ELSE
                    appealDecision IS NULL
            END
        )
        """

        checks["valid_isDecisionAllowed"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isDecisionAllowed, '') = coalesce(isDecisionAllowed_ended, '')
                ELSE
                    isDecisionAllowed IS NULL
            END
        )
        """

        checks["valid_anonymityOrder"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(anonymityOrder, '') = coalesce(anonymityOrder_ended, '')
                ELSE
                    anonymityOrder IS NULL
            END
        )
        """

        return checks

        

    def get_checks_hearingResponse(self, checks={}):

        checks["valid_isRemoteHearing"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isRemoteHearing, '') = coalesce(isRemoteHearing_ended, '')
                ELSE
                    isRemoteHearing IS NULL
            END
        )
        """

        checks["valid_isAppealSuitableToFloat"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isAppealSuitableToFloat, '') = coalesce(isAppealSuitableToFloat_ended, '')
                ELSE
                    isAppealSuitableToFloat IS NULL
            END
        )
        """

        checks["valid_isMultimediaAllowed"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isMultimediaAllowed, '') = coalesce(isMultimediaAllowed_ended, '')
                ELSE
                    isMultimediaAllowed IS NULL
            END
        )
        """

        checks["valid_multimediaTribunalResponse"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(multimediaTribunalResponse, '') = coalesce(multimediaTribunalResponse_ended, '')
                ELSE
                    multimediaTribunalResponse IS NULL
            END
        )
        """

        checks["valid_multimediaDecisionForDisplay"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(multimediaDecisionForDisplay, '') = coalesce(multimediaDecisionForDisplay_ended, '')
                ELSE
                    multimediaDecisionForDisplay IS NULL
            END
        )
        """

        checks["valid_isInCameraCourtAllowed"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isInCameraCourtAllowed, '') = coalesce(isInCameraCourtAllowed_ended, '')
                ELSE
                    isInCameraCourtAllowed IS NULL
            END
        )
        """

        checks["valid_inCameraCourtTribunalResponse"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(inCameraCourtTribunalResponse, '') = coalesce(inCameraCourtTribunalResponse_ended, '')
                ELSE
                    inCameraCourtTribunalResponse IS NULL
            END
        )
        """

        checks["valid_inCameraCourtDecisionForDisplay"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(inCameraCourtDecisionForDisplay, '') = coalesce(inCameraCourtDecisionForDisplay_ended, '')
                ELSE
                    inCameraCourtDecisionForDisplay IS NULL
            END
        )
        """

        checks["valid_isSingleSexCourtAllowed"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isSingleSexCourtAllowed, '') = coalesce(isSingleSexCourtAllowed_ended, '')
                ELSE
                    isSingleSexCourtAllowed IS NULL
            END
        )
        """

        checks["valid_singleSexCourtTribunalResponse"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(singleSexCourtTribunalResponse, '') = coalesce(singleSexCourtTribunalResponse_ended, '')
                ELSE
                    singleSexCourtTribunalResponse IS NULL
            END
        )
        """

        checks["valid_singleSexCourtDecisionForDisplay"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(singleSexCourtDecisionForDisplay, '') = coalesce(singleSexCourtDecisionForDisplay_ended, '')
                ELSE
                    singleSexCourtDecisionForDisplay IS NULL
            END
        )
        """

        checks["valid_isVulnerabilitiesAllowed"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isVulnerabilitiesAllowed, '') = coalesce(isVulnerabilitiesAllowed_ended, '')
                ELSE
                    isVulnerabilitiesAllowed IS NULL
            END
        )
        """

        checks["valid_vulnerabilitiesTribunalResponse"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(vulnerabilitiesTribunalResponse, '') = coalesce(vulnerabilitiesTribunalResponse_ended, '')
                ELSE
                    vulnerabilitiesTribunalResponse IS NULL
            END
        )
        """

        checks["valid_vulnerabilitiesDecisionForDisplay"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(vulnerabilitiesDecisionForDisplay, '') = coalesce(vulnerabilitiesDecisionForDisplay_ended, '')
                ELSE
                    vulnerabilitiesDecisionForDisplay IS NULL
            END
        )
        """

        checks["valid_isRemoteHearingAllowed"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isRemoteHearingAllowed, '') = coalesce(isRemoteHearingAllowed_ended, '')
                ELSE
                    isRemoteHearingAllowed IS NULL
            END
        )
        """

        checks["valid_remoteVideoCallTribunalResponse"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(remoteVideoCallTribunalResponse, '') = coalesce(remoteVideoCallTribunalResponse_ended, '')
                ELSE
                    remoteVideoCallTribunalResponse IS NULL
            END
        )
        """

        checks["valid_remoteHearingDecisionForDisplay"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(remoteHearingDecisionForDisplay, '') = coalesce(remoteHearingDecisionForDisplay_ended, '')
                ELSE
                    remoteHearingDecisionForDisplay IS NULL
            END
        )
        """

        checks["valid_isAdditionalAdjustmentsAllowed"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isAdditionalAdjustmentsAllowed, '') = coalesce(isAdditionalAdjustmentsAllowed_ended, '')
                ELSE
                    isAdditionalAdjustmentsAllowed IS NULL
            END
        )
        """

        checks["valid_additionalTribunalResponse"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(additionalTribunalResponse, '') = coalesce(additionalTribunalResponse_ended, '')
                ELSE
                    additionalTribunalResponse IS NULL
            END
        )
        """

        checks["valid_otherDecisionForDisplay"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(otherDecisionForDisplay, '') = coalesce(otherDecisionForDisplay_ended, '')
                ELSE
                    otherDecisionForDisplay IS NULL
            END
        )
        """

        checks["valid_isAdditionalInstructionAllowed"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isAdditionalInstructionAllowed, '') = coalesce(isAdditionalInstructionAllowed_ended, '')
                ELSE
                    isAdditionalInstructionAllowed IS NULL
            END
        )
        """

        checks["valid_additionalInstructionsTribunalResponse"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(additionalInstructionsTribunalResponse, '') = coalesce(additionalInstructionsTribunalResponse_ended, '')
                ELSE
                    additionalInstructionsTribunalResponse IS NULL
            END
        )
        """

        return checks


        

        

    def get_checks_hearingRequirements(self, checks={}):

        checks["valid_isAppellantAttendingTheHearing"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isAppellantAttendingTheHearing, '') = coalesce(isAppellantAttendingTheHearing_ended, '')
                ELSE
                    isAppellantAttendingTheHearing IS NULL
            END
        )
        """

        checks["valid_isAppellantGivingOralEvidence"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isAppellantGivingOralEvidence, '') = coalesce(isAppellantGivingOralEvidence_ended, '')
                ELSE
                    isAppellantGivingOralEvidence IS NULL
            END
        )
        """

        checks["valid_isWitnessesAttending"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isWitnessesAttending, '') = coalesce(isWitnessesAttending_ended, '')
                ELSE
                    isWitnessesAttending IS NULL
            END
        )
        """

        checks["valid_isEvidenceFromOutsideUkOoc"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isEvidenceFromOutsideUkOoc, '') = coalesce(isEvidenceFromOutsideUkOoc_ended, '')
                ELSE
                    isEvidenceFromOutsideUkOoc IS NULL
            END
        )
        """

        checks["valid_isEvidenceFromOutsideUkInCountry"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isEvidenceFromOutsideUkInCountry, '') = coalesce(isEvidenceFromOutsideUkInCountry_ended, '')
                ELSE
                    isEvidenceFromOutsideUkInCountry IS NULL
            END
        )
        """

        checks["valid_isInterpreterServicesNeeded"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isInterpreterServicesNeeded, '') = coalesce(isInterpreterServicesNeeded_ended, '')
                ELSE
                    isInterpreterServicesNeeded IS NULL
            END
        )
        """

        checks["valid_appellantInterpreterLanguageCategory"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(appellantInterpreterLanguageCategory, ARRAY()) = COALESCE(appellantInterpreterLanguageCategory_ended, ARRAY())
                ELSE
                    appellantInterpreterLanguageCategory IS NULL
            END
        )
        """

        checks["valid_appellantInterpreterSpokenLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(appellantInterpreterSpokenLanguage), '{}') = coalesce(to_json(appellantInterpreterSpokenLanguage_ended), '{}')
                ELSE
                    appellantInterpreterSpokenLanguage IS NULL
            END
        )
        """

        checks["valid_appellantInterpreterSignLanguage"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(to_json(appellantInterpreterSignLanguage), '{}') = coalesce(to_json(appellantInterpreterSignLanguage_ended), '{}')
                ELSE
                    appellantInterpreterSignLanguage IS NULL
            END
        )
        """

        checks["valid_isHearingRoomNeeded"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isHearingRoomNeeded, '') = coalesce(isHearingRoomNeeded_ended, '')
                ELSE
                    isHearingRoomNeeded IS NULL
            END
        )
        """

        checks["valid_isHearingLoopNeeded"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isHearingLoopNeeded, '') = coalesce(isHearingLoopNeeded_ended, '')
                ELSE
                    isHearingLoopNeeded IS NULL
            END
        )
        """

        checks["valid_remoteVideoCall"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(remoteVideoCall, '') = coalesce(remoteVideoCall_ended, '')
                ELSE
                    remoteVideoCall IS NULL
            END
        )
        """

        checks["valid_remoteVideoCallDescription"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(remoteVideoCallDescription, '') = coalesce(remoteVideoCallDescription_ended, '')
                ELSE
                    remoteVideoCallDescription IS NULL
            END
        )
        """

        checks["valid_physicalOrMentalHealthIssues"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(physicalOrMentalHealthIssues, '') = coalesce(physicalOrMentalHealthIssues_ended, '')
                ELSE
                    physicalOrMentalHealthIssues IS NULL
            END
        )
        """

        checks["valid_physicalOrMentalHealthIssuesDescription"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(physicalOrMentalHealthIssuesDescription, '') = coalesce(physicalOrMentalHealthIssuesDescription_ended, '')
                ELSE
                    physicalOrMentalHealthIssuesDescription IS NULL
            END
        )
        """

        checks["valid_pastExperiences"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(pastExperiences, '') = coalesce(pastExperiences_ended, '')
                ELSE
                    pastExperiences IS NULL
            END
        )
        """

        checks["valid_pastExperiencesDescription"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(pastExperiencesDescription, '') = coalesce(pastExperiencesDescription_ended, '')
                ELSE
                    pastExperiencesDescription IS NULL
            END
        )
        """

        checks["valid_multimediaEvidence"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(multimediaEvidence, '') = coalesce(multimediaEvidence_ended, '')
                ELSE
                    multimediaEvidence IS NULL
            END
        )
        """

        checks["valid_multimediaEvidenceDescription"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(multimediaEvidenceDescription, '') = coalesce(multimediaEvidenceDescription_ended, '')
                ELSE
                    multimediaEvidenceDescription IS NULL
            END
        )
        """

        checks["valid_singleSexCourt"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(singleSexCourt, '') = coalesce(singleSexCourt_ended, '')
                ELSE
                    singleSexCourt IS NULL
            END
        )
        """

        checks["valid_singleSexCourtType"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(singleSexCourtType, '') = coalesce(singleSexCourtType_ended, '')
                ELSE
                    singleSexCourtType IS NULL
            END
        )
        """

        checks["valid_singleSexCourtTypeDescription"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(singleSexCourtTypeDescription, '') = coalesce(singleSexCourtTypeDescription_ended, '')
                ELSE
                    singleSexCourtTypeDescription IS NULL
            END
        )
        """

        checks["valid_inCameraCourt"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(inCameraCourt, '') = coalesce(inCameraCourt_ended, '')
                ELSE
                    inCameraCourt IS NULL
            END
        )
        """

        checks["valid_inCameraCourtDescription"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(inCameraCourtDescription, '') = coalesce(inCameraCourtDescription_ended, '')
                ELSE
                    inCameraCourtDescription IS NULL
            END
        )
        """

        checks["valid_additionalRequests"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(additionalRequests, '') = coalesce(additionalRequests_ended, '')
                ELSE
                    additionalRequests IS NULL
            END
        )
        """

        checks["valid_additionalRequestsDescription"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(additionalRequestsDescription, '') = coalesce(additionalRequestsDescription_ended, '')
                ELSE
                    additionalRequestsDescription IS NULL
            END
        )
        """

        checks["valid_datesToAvoidYesNo"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(datesToAvoidYesNo, '') = coalesce(datesToAvoidYesNo_ended, '')
                ELSE
                    datesToAvoidYesNo IS NULL
            END
        )
        """

        return checks

        


    def get_checks_general(checks=None):

        checks["valid_directions"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 26 AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    COALESCE(directions, ARRAY()) = COALESCE(directions_ended, ARRAY())
                ELSE
                    directions IS NULL
            END
        )
        """

        checks["valid_uploadHomeOfficeBundleAvailable"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 26 AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(uploadHomeOfficeBundleAvailable, '') = coalesce(uploadHomeOfficeBundleAvailable_ended, '')
                ELSE
                    uploadHomeOfficeBundleAvailable IS NULL
            END
        )
        """

        checks["valid_uploadHomeOfficeBundleActionAvailable"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 26 AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(uploadHomeOfficeBundleActionAvailable, '') = coalesce(uploadHomeOfficeBundleActionAvailable_ended, '')
                ELSE
                    uploadHomeOfficeBundleActionAvailable IS NULL
            END
        )
        """

        checks["valid_caseArgumentAvailable"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 26 AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(caseArgumentAvailable, '') = coalesce(caseArgumentAvailable_ended, '')
                ELSE
                    caseArgumentAvailable IS NULL
            END
        )
        """

        checks["valid_reasonsForAppealDecision"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 26 AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(reasonsForAppealDecision, '') = coalesce(reasonsForAppealDecision_ended, '')
                ELSE
                    reasonsForAppealDecision IS NULL
            END
        )
        """

        checks["valid_appealReviewOutcome"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(appealReviewOutcome, '') = coalesce(appealReviewOutcome_ended, '')
                ELSE
                    appealReviewOutcome IS NULL
            END
        )
        """

        checks["valid_appealResponseAvailable"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(appealResponseAvailable, '') = coalesce(appealResponseAvailable_ended, '')
                ELSE
                    appealResponseAvailable IS NULL
            END
        )
        """

        checks["valid_reviewedHearingRequirements"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(reviewedHearingRequirements, '') = coalesce(reviewedHearingRequirements_ended, '')
                ELSE
                    reviewedHearingRequirements IS NULL
            END
        )
        """

        checks["valid_amendResponseActionAvailable"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(amendResponseActionAvailable, '') = coalesce(amendResponseActionAvailable_ended, '')
                ELSE
                    amendResponseActionAvailable IS NULL
            END
        )
        """

        checks["valid_currentHearingDetailsVisible"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(currentHearingDetailsVisible, '') = coalesce(currentHearingDetailsVisible_ended, '')
                ELSE
                    currentHearingDetailsVisible IS NULL
            END
        )
        """

        checks["valid_reviewResponseActionAvailable"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(reviewResponseActionAvailable, '') = coalesce(reviewResponseActionAvailable_ended, '')
                ELSE
                    reviewResponseActionAvailable IS NULL
            END
        )
        """

        checks["valid_reviewHomeOfficeResponseByLegalRep"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(reviewHomeOfficeResponseByLegalRep, '') = coalesce(reviewHomeOfficeResponseByLegalRep_ended, '')
                ELSE
                    reviewHomeOfficeResponseByLegalRep IS NULL
            END
        )
        """

        checks["valid_submitHearingRequirementsAvailable"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(submitHearingRequirementsAvailable, '') = coalesce(submitHearingRequirementsAvailable_ended, '')
                ELSE
                    submitHearingRequirementsAvailable IS NULL
            END
        )
        """

        checks["valid_uploadHomeOfficeAppealResponseActionAvailable"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended IN (37, 38) AND Outcome_ended IN (80, 25, 13)) OR
                    (CaseStatus_ended = 38 AND Outcome_ended = 72) OR
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(uploadHomeOfficeAppealResponseActionAvailable, '') = coalesce(uploadHomeOfficeAppealResponseActionAvailable_ended, '')
                ELSE
                    uploadHomeOfficeAppealResponseActionAvailable IS NULL
            END
        )
        """

        checks["valid_hmcts"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(hmcts, '') = coalesce(hmcts_ended, '')
                ELSE
                    hmcts IS NULL
            END
        )
        """

        checks["valid_stitchingStatus"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(stitchingStatus, '') = coalesce(stitchingStatus_ended, '')
                ELSE
                    stitchingStatus IS NULL
            END
        )
        """

        checks["valid_bundleConfiguration"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(bundleConfiguration, '') = coalesce(bundleConfiguration_ended, '')
                ELSE
                    bundleConfiguration IS NULL
            END
        )
        """

        checks["valid_bundleFileNamePrefix"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(bundleFileNamePrefix, '') = coalesce(bundleFileNamePrefix_ended, '')
                ELSE
                    bundleFileNamePrefix IS NULL
            END
        )
        """

        checks["valid_appealDecisionAvailable"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(appealDecisionAvailable, '') = coalesce(appealDecisionAvailable_ended, '')
                ELSE
                    appealDecisionAvailable IS NULL
            END
        )
        """

        checks["valid_isFtpaListVisible"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaListVisible, '') = coalesce(isFtpaListVisible_ended, '')
                ELSE
                    isFtpaListVisible IS NULL
            END
        )
        """

        checks["valid_ftpaAppellantSubmitted"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(ftpaAppellantSubmitted, '') = coalesce(ftpaAppellantSubmitted_ended, '')
                ELSE
                    ftpaAppellantSubmitted IS NULL
            END
        )
        """

        checks["valid_isFtpaAppellantDocsVisibleInDecided"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaAppellantDocsVisibleInDecided, '') = coalesce(isFtpaAppellantDocsVisibleInDecided_ended, '')
                ELSE
                    isFtpaAppellantDocsVisibleInDecided IS NULL
            END
        )
        """

        checks["valid_isFtpaAppellantDocsVisibleInSubmitted"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaAppellantDocsVisibleInSubmitted, '') = coalesce(isFtpaAppellantDocsVisibleInSubmitted_ended, '')
                ELSE
                    isFtpaAppellantDocsVisibleInSubmitted IS NULL
            END
        )
        """

        checks["valid_isFtpaAppellantOotDocsVisibleInDecided"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaAppellantOotDocsVisibleInDecided, '') = coalesce(isFtpaAppellantOotDocsVisibleInDecided_ended, '')
                ELSE
                    isFtpaAppellantOotDocsVisibleInDecided IS NULL
            END
        )
        """

        checks["valid_isFtpaAppellantOotDocsVisibleInSubmitted"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaAppellantOotDocsVisibleInSubmitted, '') = coalesce(isFtpaAppellantOotDocsVisibleInSubmitted_ended, '')
                ELSE
                    isFtpaAppellantOotDocsVisibleInSubmitted IS NULL
            END
        )
        """

        checks["valid_isFtpaAppellantGroundsDocsVisibleInDecided"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaAppellantGroundsDocsVisibleInDecided, '') = coalesce(isFtpaAppellantGroundsDocsVisibleInDecided_ended, '')
                ELSE
                    isFtpaAppellantGroundsDocsVisibleInDecided IS NULL
            END
        )
        """

        checks["valid_isFtpaAppellantEvidenceDocsVisibleInDecided"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaAppellantEvidenceDocsVisibleInDecided, '') = coalesce(isFtpaAppellantEvidenceDocsVisibleInDecided_ended, '')
                ELSE
                    isFtpaAppellantEvidenceDocsVisibleInDecided IS NULL
            END
        )
        """

        checks["valid_isFtpaAppellantGroundsDocsVisibleInSubmitted"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaAppellantGroundsDocsVisibleInSubmitted, '') = coalesce(isFtpaAppellantGroundsDocsVisibleInSubmitted_ended, '')
                ELSE
                    isFtpaAppellantGroundsDocsVisibleInSubmitted IS NULL
            END
        )
        """

        checks["valid_isFtpaAppellantEvidenceDocsVisibleInSubmitted"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaAppellantEvidenceDocsVisibleInSubmitted, '') = coalesce(isFtpaAppellantEvidenceDocsVisibleInSubmitted_ended, '')
                ELSE
                    isFtpaAppellantEvidenceDocsVisibleInSubmitted IS NULL
            END
        )
        """

        checks["valid_isFtpaAppellantOotExplanationVisibleInDecided"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaAppellantOotExplanationVisibleInDecided, '') = coalesce(isFtpaAppellantOotExplanationVisibleInDecided_ended, '')
                ELSE
                    isFtpaAppellantOotExplanationVisibleInDecided IS NULL
            END
        )
        """

        checks["valid_isFtpaAppellantOotExplanationVisibleInSubmitted"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaAppellantOotExplanationVisibleInSubmitted, '') = coalesce(isFtpaAppellantOotExplanationVisibleInSubmitted_ended, '')
                ELSE
                    isFtpaAppellantOotExplanationVisibleInSubmitted IS NULL
            END
        )
        """

        checks["valid_ftpaRespondentSubmitted"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(ftpaRespondentSubmitted, '') = coalesce(ftpaRespondentSubmitted_ended, '')
                ELSE
                    ftpaRespondentSubmitted IS NULL
            END
        )
        """

        checks["valid_isFtpaRespondentDocsVisibleInDecided"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaRespondentDocsVisibleInDecided, '') = coalesce(isFtpaRespondentDocsVisibleInDecided_ended, '')
                ELSE
                    isFtpaRespondentDocsVisibleInDecided IS NULL
            END
        )
        """

        checks["valid_isFtpaRespondentDocsVisibleInSubmitted"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaRespondentDocsVisibleInSubmitted, '') = coalesce(isFtpaRespondentDocsVisibleInSubmitted_ended, '')
                ELSE
                    isFtpaRespondentDocsVisibleInSubmitted IS NULL
            END
        )
        """

        checks["valid_isFtpaRespondentOotDocsVisibleInDecided"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaRespondentOotDocsVisibleInDecided, '') = coalesce(isFtpaRespondentOotDocsVisibleInDecided_ended, '')
                ELSE
                    isFtpaRespondentOotDocsVisibleInDecided IS NULL
            END
        )
        """

        checks["valid_isFtpaRespondentOotDocsVisibleInSubmitted"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaRespondentOotDocsVisibleInSubmitted, '') = coalesce(isFtpaRespondentOotDocsVisibleInSubmitted_ended, '')
                ELSE
                    isFtpaRespondentOotDocsVisibleInSubmitted IS NULL
            END
        )
        """

        checks["valid_isFtpaRespondentGroundsDocsVisibleInDecided"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaRespondentGroundsDocsVisibleInDecided, '') = coalesce(isFtpaRespondentGroundsDocsVisibleInDecided_ended, '')
                ELSE
                    isFtpaRespondentGroundsDocsVisibleInDecided IS NULL
            END
        )
        """

        checks["valid_isFtpaRespondentEvidenceDocsVisibleInDecided"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaRespondentEvidenceDocsVisibleInDecided, '') = coalesce(isFtpaRespondentEvidenceDocsVisibleInDecided_ended, '')
                ELSE
                    isFtpaRespondentEvidenceDocsVisibleInDecided IS NULL
            END
        )
        """

        checks["valid_isFtpaRespondentGroundsDocsVisibleInSubmitted"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaRespondentGroundsDocsVisibleInSubmitted, '') = coalesce(isFtpaRespondentGroundsDocsVisibleInSubmitted_ended, '')
                ELSE
                    isFtpaRespondentGroundsDocsVisibleInSubmitted IS NULL
            END
        )
        """

        checks["valid_isFtpaRespondentEvidenceDocsVisibleInSubmitted"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaRespondentEvidenceDocsVisibleInSubmitted, '') = coalesce(isFtpaRespondentEvidenceDocsVisibleInSubmitted_ended, '')
                ELSE
                    isFtpaRespondentEvidenceDocsVisibleInSubmitted IS NULL
            END
        )
        """

        checks["valid_isFtpaRespondentOotExplanationVisibleInDecided"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaRespondentOotExplanationVisibleInDecided, '') = coalesce(isFtpaRespondentOotExplanationVisibleInDecided_ended, '')
                ELSE
                    isFtpaRespondentOotExplanationVisibleInDecided IS NULL
            END
        )
        """

        checks["valid_isFtpaRespondentOotExplanationVisibleInSubmitted"] = """
        (
            CASE 
                WHEN (
                    (CaseStatus_ended = 39 AND Outcome_ended = 25)
                ) THEN
                    coalesce(isFtpaRespondentOotExplanationVisibleInSubmitted, '') = coalesce(isFtpaRespondentOotExplanationVisibleInSubmitted_ended, '')
                ELSE
                    isFtpaRespondentOotExplanationVisibleInSubmitted IS NULL
            END
        )
        """

        return checks
