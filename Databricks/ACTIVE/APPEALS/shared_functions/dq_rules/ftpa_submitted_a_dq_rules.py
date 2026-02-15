from .dq_rules import DQRulesBase


class ftpaSubmittedADQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_document()
        checks = checks | self.get_checks_general()
        checks = checks | self.get_checks_general_default()
        checks = checks | self.get_checks_ftpa()

        return checks

    def get_checks_document(self, checks={}):
        checks["valid_ftpaAppellantDocuments"] = """(
            CASE
                WHEN Party = 1 THEN size(ftpaAppellantDocuments) = 0
                ELSE ftpaAppellantDocuments IS NULL
            END
        )"""

        checks["valid_ftpaRespondentDocuments"] = """(
            CASE
                WHEN Party = 2 THEN size(ftpaRespondentDocuments) = 0
                ELSE ftpaRespondentDocuments IS NULL
            END
        )"""

        checks["valid_ftpaAppellantGroundsDocuments"] = """(
            CASE
                WHEN Party = 1 THEN size(ftpaAppellantGroundsDocuments) = 0
                ELSE ftpaAppellantGroundsDocuments IS NULL
            END
        )"""

        checks["valid_ftpaRespondentGroundsDocuments"] = """(
            CASE
                WHEN Party = 2 THEN size(ftpaRespondentGroundsDocuments) = 0
                ELSE ftpaRespondentGroundsDocuments IS NULL
            END
        )"""

        checks["valid_ftpaAppellantEvidenceDocuments"] = """(
            CASE
                WHEN Party = 1 THEN size(ftpaAppellantEvidenceDocuments) = 0
                ELSE ftpaAppellantEvidenceDocuments IS NULL
            END
        )"""

        checks["valid_ftpaRespondentEvidenceDocuments"] = """(
            CASE
                WHEN Party = 2 THEN size(ftpaRespondentEvidenceDocuments) = 0
                ELSE ftpaRespondentEvidenceDocuments IS NULL
            END
        )"""

        checks["valid_ftpaAppellantOutOfTimeDocuments"] = """(
            CASE
                WHEN Party = 1 THEN size(ftpaAppellantOutOfTimeDocuments) = 0
                ELSE ftpaAppellantOutOfTimeDocuments IS NULL
            END
        )"""

        checks["valid_ftpaRespondentOutOfTimeDocuments"] = """(
            CASE
                WHEN Party = 2 THEN size(ftpaRespondentOutOfTimeDocuments) = 0
                ELSE ftpaRespondentOutOfTimeDocuments IS NULL
            END
        )"""

        return checks

    def get_checks_general_default(self, checks={}):

        checks["valid_isFtpaListVisible"] = ("(isFtpaListVisible = 'Yes')")

        return checks

    def get_checks_general(self, checks={}):

        ##### Appellant ######
        checks["valid_ftpaAppellantSubmitted"] = """(
            CASE
                WHEN Party = 1 THEN ftpaAppellantSubmitted = 'Yes'
                ELSE ftpaAppellantSubmitted IS NULL
            END
        )"""

        checks["valid_isFtpaAppellantDocsVisibleInDecided"] = """(
            CASE
                WHEN Party = 1 THEN isFtpaAppellantDocsVisibleInDecided = 'No'
                ELSE isFtpaAppellantDocsVisibleInDecided IS NULL
            END
        )"""

        checks["valid_isFtpaAppellantDocsVisibleInSubmitted"] = """(
            CASE
                WHEN Party = 1 THEN isFtpaAppellantDocsVisibleInSubmitted = 'Yes'
                ELSE isFtpaAppellantDocsVisibleInSubmitted IS NULL
            END
        )"""

        checks["valid_isFtpaAppellantOotDocsVisibleInDecided"] = """(
            CASE
                WHEN Party = 1 AND OutOfTime = 1 THEN isFtpaAppellantOotDocsVisibleInDecided = 'No'
                ELSE isFtpaAppellantOotDocsVisibleInDecided IS NULL
            END
        )"""

        checks["valid_isFtpaAppellantOotDocsVisibleInSubmitted"] = """(
            CASE
                WHEN Party = 1 AND OutOfTime = 1 THEN isFtpaAppellantOotDocsVisibleInSubmitted = 'Yes'
                ELSE isFtpaAppellantOotDocsVisibleInSubmitted IS NULL
            END
        )"""

        checks["valid_isFtpaAppellantGroundsDocsVisibleInDecided"] = """(
            CASE
                WHEN Party = 1 THEN isFtpaAppellantGroundsDocsVisibleInDecided = 'No'
                ELSE isFtpaAppellantGroundsDocsVisibleInDecided IS NULL
            END
        )"""

        checks["valid_isFtpaAppellantEvidenceDocsVisibleInDecided"] = """(
            CASE
                WHEN Party = 1 THEN isFtpaAppellantEvidenceDocsVisibleInDecided = 'No'
                ELSE isFtpaAppellantEvidenceDocsVisibleInDecided IS NULL
            END
        )"""

        checks["valid_isFtpaAppellantGroundsDocsVisibleInSubmitted"] = """(
            CASE
                WHEN Party = 1 THEN isFtpaAppellantGroundsDocsVisibleInSubmitted = 'Yes'
                ELSE isFtpaAppellantGroundsDocsVisibleInSubmitted IS NULL
            END
        )"""

        checks["valid_isFtpaAppellantEvidenceDocsVisibleInSubmitted"] = """(
            CASE
                WHEN Party = 1 THEN isFtpaAppellantEvidenceDocsVisibleInSubmitted = 'Yes'
                ELSE isFtpaAppellantEvidenceDocsVisibleInSubmitted IS NULL
            END
        )"""

        checks["valid_isFtpaAppellantOotExplanationVisibleInDecided"] = """(
            CASE
                WHEN Party = 1 AND OutOfTime = 1 THEN isFtpaAppellantOotExplanationVisibleInDecided = 'No'
                ELSE isFtpaAppellantOotExplanationVisibleInDecided IS NULL
            END
        )"""

        checks["valid_isFtpaAppellantOotExplanationVisibleInSubmitted"] = """(
            CASE
                WHEN Party = 1 AND OutOfTime = 1 THEN isFtpaAppellantOotExplanationVisibleInSubmitted = 'Yes'
                ELSE isFtpaAppellantOotExplanationVisibleInSubmitted IS NULL
            END
        )"""

        ##### Respondent ######
        checks["valid_ftpaRespondentSubmitted"] = """(
            CASE
                WHEN Party = 2 THEN ftpaRespondentSubmitted = 'Yes'
                ELSE ftpaRespondentSubmitted IS NULL
            END
        )"""

        checks["valid_isFtpaRespondentDocsVisibleInDecided"] = """(
            CASE
                WHEN Party = 2 THEN isFtpaRespondentDocsVisibleInDecided = 'No'
                ELSE isFtpaRespondentDocsVisibleInDecided IS NULL
            END
        )"""

        checks["valid_isFtpaRespondentDocsVisibleInSubmitted"] = """(
            CASE
                WHEN Party = 2 THEN isFtpaRespondentDocsVisibleInSubmitted = 'Yes'
                ELSE isFtpaRespondentDocsVisibleInSubmitted IS NULL
            END
        )"""

        checks["valid_isFtpaRespondentOotDocsVisibleInDecided"] = """(
            CASE
                WHEN Party = 2 AND OutOfTime = 1 THEN isFtpaRespondentOotDocsVisibleInDecided = 'No'
                ELSE isFtpaRespondentOotDocsVisibleInDecided IS NULL
            END
        )"""

        checks["valid_isFtpaRespondentOotDocsVisibleInSubmitted"] = """(
            CASE
                WHEN Party = 2 AND OutOfTime = 1 THEN isFtpaRespondentOotDocsVisibleInSubmitted = 'Yes'
                ELSE isFtpaRespondentOotDocsVisibleInSubmitted IS NULL
            END
        )"""

        checks["valid_isFtpaRespondentGroundsDocsVisibleInDecided"] = """(
            CASE
                WHEN Party = 2 THEN isFtpaRespondentGroundsDocsVisibleInDecided = 'No'
                ELSE isFtpaRespondentGroundsDocsVisibleInDecided IS NULL
            END
        )"""

        checks["valid_isFtpaRespondentEvidenceDocsVisibleInDecided"] = """(
            CASE
                WHEN Party = 2 THEN isFtpaRespondentEvidenceDocsVisibleInDecided = 'No'
                ELSE isFtpaRespondentEvidenceDocsVisibleInDecided IS NULL
            END
        )"""

        checks["valid_isFtpaRespondentGroundsDocsVisibleInSubmitted"] = """(
            CASE
                WHEN Party = 2 THEN isFtpaRespondentGroundsDocsVisibleInSubmitted = 'Yes'
                ELSE isFtpaRespondentGroundsDocsVisibleInSubmitted IS NULL
            END
        )"""

        checks["valid_isFtpaRespondentEvidenceDocsVisibleInSubmitted"] = """(
            CASE
                WHEN Party = 2 THEN isFtpaRespondentEvidenceDocsVisibleInSubmitted = 'Yes'
                ELSE isFtpaRespondentEvidenceDocsVisibleInSubmitted IS NULL
            END
        )"""

        checks["valid_isFtpaRespondentOotExplanationVisibleInDecided"] = """(
            CASE
                WHEN Party = 2 AND OutOfTime = 1 THEN isFtpaRespondentOotExplanationVisibleInDecided = 'No'
                ELSE isFtpaRespondentOotExplanationVisibleInDecided IS NULL
            END
        )"""

        checks["valid_isFtpaRespondentOotExplanationVisibleInSubmitted"] = """(
            CASE
                WHEN Party = 2 AND OutOfTime = 1 THEN isFtpaRespondentOotExplanationVisibleInSubmitted = 'Yes'
                ELSE isFtpaRespondentOotExplanationVisibleInSubmitted IS NULL
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

        ####### Appellant #######
        checks["valid_ftpaAppellantApplicationDate"] = """(
            ftpaAppellantApplicationDate IS NULL
            OR
            CASE
                WHEN Party = 1 THEN ftpaAppellantApplicationDate = date_format(to_timestamp(DateReceived, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'),'dd/MM/yyyy')
                ELSE ftpaAppellantApplicationDate IS NULL
            END
        )"""

        checks["valid_ftpaAppellantSubmissionOutOfTime"] = """(
            ftpaAppellantSubmissionOutOfTime IS NULL
            OR
            CASE
                WHEN Party = 1 AND OutOfTime = 1 THEN ftpaAppellantSubmissionOutOfTime = 'Yes'
                WHEN Party = 1 AND OutOfTime != 1 THEN ftpaAppellantSubmissionOutOfTime = 'No'
                ELSE ftpaAppellantSubmissionOutOfTime IS NULL
            END
        )"""

        checks["valid_ftpaAppellantOutOfTimeExplanation"] = """(
            CASE
                WHEN Party = 1 AND OutOfTime = 1 THEN ftpaAppellantOutOfTimeExplanation = 'This is a migrated ARIA case. Please refer to the documents.'
                ELSE ftpaAppellantOutOfTimeExplanation IS NULL
            END
        )"""

        ####### Respondent #######
        checks["valid_ftpaRespondentApplicationDate"] = """(
            CASE
                WHEN Party = 2 THEN ftpaRespondentApplicationDate = date_format(to_timestamp(DateReceived, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'),'dd/MM/yyyy')
                ELSE ftpaRespondentApplicationDate IS NULL
            END
        )"""

        checks["valid_ftpaRespondentSubmissionOutOfTime"] = """(
            CASE
                WHEN Party = 2 AND OutOfTime = 1 THEN ftpaRespondentSubmissionOutOfTime = 'Yes'
                WHEN Party = 2 AND OutOfTime != 1 THEN ftpaRespondentSubmissionOutOfTime = 'No'
                ELSE ftpaRespondentSubmissionOutOfTime IS NULL
            END
        )"""

        checks["valid_ftpaRespondentOutOfTimeExplanation"] = """(
            CASE
                WHEN Party = 2 AND OutOfTime = 1 THEN ftpaRespondentOutOfTimeExplanation = 'This is a migrated ARIA case. Please refer to the documents.'
                ELSE ftpaRespondentOutOfTimeExplanation IS NULL
            END
        )"""

        return checks
