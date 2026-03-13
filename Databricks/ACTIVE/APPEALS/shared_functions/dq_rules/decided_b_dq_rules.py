from .dq_rules import DQRulesBase


class decidedBDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_document()
        checks = checks | self.get_checks_general()
        checks = checks | self.get_checks_general_default()
        checks = checks | self.get_checks_ftpa()
        checks = checks | self.get_checks_set_aside()

        return checks

    def get_checks_document(self, checks={}):

        checks["valid_allSetAsideDocs"] = """(
            size(allSetAsideDocs) = 0
        )"""

        checks["valid_allFtpaAppellantDecisionDocs"] = """(
            CASE
                WHEN Party = 1 THEN size(allFtpaAppellantDecisionDocs) = 0
                ELSE allFtpaAppellantDecisionDocs IS NULL
            END
        )"""

        checks["valid_allFtpaRespondentDecisionDocs"] = """(
            CASE
                WHEN Party = 2 THEN size(allFtpaRespondentDecisionDocs) = 0
                ELSE allFtpaRespondentDecisionDocs IS NULL
            END
        )"""


        return checks

    def get_checks_general_default(self, checks={}):

        checks["valid_isDlrmSetAsideEnabled"] = ("(isDlrmSetAsideEnabled = 'Yes')")
        checks["valid_isReheardAppealEnabled"] = ("(isReheardAppealEnabled = 'Yes')")
        checks["valid_secondFtpaDecisionExists"] = ("(secondFtpaDecisionExists = 'No')")
        checks["valid_caseFlagSetAsideReheardExists"] = ("(caseFlagSetAsideReheardExists = 'Yes')")

        return checks

    def get_checks_general(self, checks={}):


        checks["valid_isFtpaAppellantDecided"] = """(
            CASE
                WHEN Party = 1 AND CaseStatus_decb = 39 THEN isFtpaAppellantDecided = 'Yes'
                ELSE isFtpaAppellantDecided IS NULL
            END
        )"""

        checks["valid_isFtpaRespondentDecided"] = """(
            CASE
                WHEN Party = 2 AND CaseStatus_decb = 39 THEN isFtpaRespondentDecided = 'Yes'
                ELSE isFtpaRespondentDecided IS NULL
            END
        )"""

        return checks

    def get_checks_ftpa(self, checks={}):

        checks["valid_ftpaList"] = """
            (CASE
        WHEN Party = 1 THEN
            ftpaList IS NOT NULL
        AND size(ftpaList) = 1
        AND element_at(ftpaList, 1).id = '1'
        AND lower(element_at(ftpaList, 1).value.ftpaApplicant) = 'appellant'
        AND element_at(ftpaList, 1).value.ftpaApplicationDate <=> ftpaAppellantApplicationDate
        AND size(element_at(ftpaList, 1).value.ftpaGroundsDocuments) = 0
        AND size(element_at(ftpaList, 1).value.ftpaEvidenceDocuments) = 0
        AND size(element_at(ftpaList, 1).value.ftpaOutOfTimeDocuments) = 0
        AND element_at(ftpaList, 1).value.ftpaOutOfTimeExplanation <=> ftpaAppellantOutOfTimeExplanation

        WHEN Party = 2 THEN
            ftpaList IS NOT NULL
        AND size(ftpaList) = 1
        AND element_at(ftpaList, 1).id = '1'
        AND lower(element_at(ftpaList, 1).value.ftpaApplicant) = 'respondent'
        AND element_at(ftpaList, 1).value.ftpaApplicationDate <=> ftpaRespondentApplicationDate
        AND size(element_at(ftpaList, 1).value.ftpaGroundsDocuments) = 0
        AND size(element_at(ftpaList, 1).value.ftpaEvidenceDocuments) = 0
        AND size(element_at(ftpaList, 1).value.ftpaOutOfTimeDocuments) = 0
        AND element_at(ftpaList, 1).value.ftpaOutOfTimeExplanation <=> ftpaRespondentOutOfTimeExplanation
        END
        )
        OR
        ftpaList IS NULL
        """

        checks["valid_ftpaApplicantType"] = """(

            CASE
                WHEN Party = 1 and CaseStatus_decb = 39 THEN ftpaApplicantType = 'appellant'
                WHEN Party = 2 and CaseStatus_decb = 39 THEN ftpaApplicantType = 'respondent'
                ELSE ftpaApplicantType IS NULL
            END
        )"""

        checks["valid_ftpaFirstDecision"] = """(
            
            ftpaFirstDecision = 'remadeRule32'

        )"""

        checks["valid_ftpaAppellantDecisionDate"] = """(

            CASE
                WHEN Party = 1 and CaseStatus_decb = 39 THEN ftpaAppellantDecisionDate <=> date_format(DecisionDate_ftpa,'yyyy-MM-dd')
                ELSE ftpaAppellantDecisionDate IS NULL
            END
        )"""

        checks["valid_ftpaRespondentDecisionDate"] = """(

            CASE
                WHEN Party = 2 and CaseStatus_decb = 39 THEN ftpaRespondentDecisionDate <=> date_format(DecisionDate_ftpa,'yyyy-MM-dd')
                ELSE ftpaRespondentDecisionDate IS NULL
            END
        )"""

        checks["valid_ftpaFinalDecisionForDisplay"] = """(
            
            ftpaFinalDecisionForDisplay = 'undecided'

        )"""

        checks["valid_ftpaAppellantRjDecisionOutcomeType"] = """(

            CASE
                WHEN Party = 1 and CaseStatus_decb = 39 THEN ftpaAppellantRjDecisionOutcomeType <=> 'remadeRule32'
                ELSE ftpaAppellantRjDecisionOutcomeType IS NULL
            END
        )"""

        checks["valid_ftpaRespondentRjDecisionOutcomeType"] = """(

            CASE
                WHEN Party = 2 and CaseStatus_decb = 39 THEN ftpaRespondentRjDecisionOutcomeType <=> 'remadeRule32'
                ELSE ftpaRespondentRjDecisionOutcomeType IS NULL
            END
        )"""

        return checks
    

    def get_checks_set_aside(self, checks={}):

        checks["valid_reasonRehearingRule32"] = """(
            
            reasonRehearingRule32 = 'Set aside and to be reheard under rule 32'
            
        )"""

        checks["valid_rule32ListingAdditionalIns"] = """(
            
            rule32ListingAdditionalIns = 'This is an ARIA Migrated case. Please refer to the documents for any additional listing instructions.'
            
        )"""

        checks["valid_updateTribunalDecisionList"] = """(
            
            updateTribunalDecisionList = 'underRule32'
            
        )"""

        checks["valid_ftpaFinalDecisionRemadeRule32"] = """(
            
            ftpaFinalDecisionRemadeRule32 = ''
            
        )"""

        checks["valid_ftpaFinalDecisionRemadeRule32"] = """(
            
            updateTribunalDecisionDateRule32 = date_format(DecisionDate_decb,'yyyy-MM-dd')
            
        )"""

        checks["valid_judgesNamesToExclude"] = """(
            CASE
                WHEN Required = 0 THEN
                        judgesNamesToExclude <=> Judges
                ELSE judgesNamesToExclude IS NULL
            END
            
        )"""

        checks["valid_ftpaAppellantDecisionRemadeRule32Text"] = """(
            CASE
                WHEN Party = 1 AND CaseStatus_decb = 39 THEN ftpaAppellantDecisionRemadeRule32Text = 'This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.'
                ELSE ftpaAppellantDecisionRemadeRule32Text IS NULL
            END
        )"""

        checks["valid_ftpaRespondentDecisionRemadeRule32Text"] = """(
            CASE
                WHEN Party = 2 AND CaseStatus_decb = 39 THEN ftpaRespondentDecisionRemadeRule32Text = 'This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.'
                ELSE ftpaRespondentDecisionRemadeRule32Text IS NULL
            END
        )"""

        return checks
