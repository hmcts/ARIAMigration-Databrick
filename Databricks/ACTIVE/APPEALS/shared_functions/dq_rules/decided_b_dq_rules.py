from .dq_rules import DQRulesBase


class decidedBDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_document()
        checks = checks | self.get_checks_general()
        checks = checks | self.get_checks_general_default()
        checks = checks | self.get_checks_ftpa()

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
                WHEN Party = 1 and CaseStatus_decb = 39 THEN ftpaAppellantDecisionDate = date_format(to_timestamp(DecisionDate_decb, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'),'yyyy-MM-dd')
                ELSE ftpaAppellantDecisionDate IS NULL
            END
        )"""

        checks["valid_ftpaRespondentDecisionDate"] = """(

            CASE
                WHEN Party = 2 and CaseStatus_decb = 39 THEN ftpaRespondentDecisionDate = date_format(to_timestamp(DecisionDate_decb, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'),'yyyy-MM-dd')
                ELSE ftpaRespondentDecisionDate IS NULL
            END
        )"""

        checks["valid_ftpaFinalDecisionForDisplay"] = """(
            
            ftpaFinalDecisionForDisplay = 'undecided'

        )"""

        checks["valid_ftpaAppellantRjDecisionOutcomeType"] = """(

            CASE
                WHEN Party = 1 and CaseStatus_decb = 39 THEN ftpaAppellantRjDecisionOutcomeType = 'remadeRule32'
                ELSE ftpaAppellantRjDecisionOutcomeType IS NULL
            END
        )"""

        checks["valid_ftpaRespondentRjDecisionOutcomeType"] = """(

            CASE
                WHEN Party = 2 and CaseStatus_decb = 39 THEN ftpaRespondentRjDecisionOutcomeType = 'remadeRule32'
                ELSE ftpaRespondentRjDecisionOutcomeType IS NULL
            END
        )"""

        return checks
