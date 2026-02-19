from .dq_rules import DQRulesBase


class decisionDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_hearing_details()
        checks = checks | self.get_checks_document()
        checks = checks | self.get_checks_substantive_decision()
        checks = checks | self.get_checks_general_default()
        checks = checks | self.get_checks_general()

        return checks

    def get_checks_hearing_details(self, checks={}):

        checks["valid_listCaseHearingLength"] = ("""
        (
            TimeEstimate = listCaseHearingLength[0]
        )
        """)

        checks["valid_listCaseHearingDate"] = (
            """
            (
                listCaseHearingDate = concat(date_format(HearingDate, 'yyyy-MM-dd'),'T',date_format(StartTime, 'HH:mm:ss'),'.000')
            )
            """)

        # checks["valid_listCaseHearingCentre"] = (
        #     "(listCaseHearingCentre[0] = bronze_listCaseHearingCentre)"
        # )

        # checks["valid_listCaseHearingCentreAddress"] = (
        #     "(listCaseHearingCentreAddress = bronze_listCaseHearingCentreAddress)"
        # )

        return checks

    def get_checks_document(self, checks={}):
        
        checks["valid_caseBundles"] = (
            "(size(caseBundles) = 0) "
        )
        return checks

    def get_checks_substantive_decision(self, checks={}):

        checks["valid_scheduleOfIssuesAgreement"] = ("(scheduleOfIssuesAgreement = 'No')")

        checks["valid_scheduleOfIssuesDisagreementDescription"] = ("(scheduleOfIssuesDisagreementDescription = 'This is a migrated ARIA case. Please see the documents for information on the schedule of issues.')")

        checks["valid_immigrationHistoryAgreement"] = ("(immigrationHistoryAgreement = 'No')")

        checks["valid_immigrationHistoryDisagreementDescription"] = ("(immigrationHistoryDisagreementDescription = 'This is a migrated ARIA case. Please see the documents for information on the immigration history.')")

        return checks

    def get_checks_general_default(self, checks={}):

        checks["valid_hmcts"] = ("(hmcts = '[userImage:hmcts.png]')")

        checks["valid_stitchingStatus"] = ("(stitchingStatus = 'DONE')")

        checks["valid_bundleConfiguration"] = ("(bundleConfiguration = 'iac-hearing-bundle-config.yaml')")

        checks["valid_decisionAndReasonsAvailable"] = ("(decisionAndReasonsAvailable = 'No')")

        return checks

    def get_checks_general(self, checks={}):

        checks["valid_bundleFileNamePrefix"] = (
            """
            (
                bundleFileNamePrefix = replace(CaseNo, '/', ' ') || '-' || Appellant_Name
            )
            """
        )

        return checks
