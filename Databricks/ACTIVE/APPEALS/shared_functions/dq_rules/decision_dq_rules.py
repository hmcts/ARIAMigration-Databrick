from .dq_rules import DQRulesBase


class decisionDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        # checks = checks | self.get_checks_hearing_details()
        checks = checks | self.get_checks_document()
        checks = checks | self.get_checks_substantive_decision()
        checks = checks | self.get_checks_general_default()
        checks = checks | self.get_checks_general()

        return checks

    # def get_checks_hearing_details(self, checks={}):

    #     checks["valid_listCaseHearingLength"] = ("""
    #     (
    #         CAST(roundedTimeEstimate AS STRING) <=> CAST(listCaseHearingLength AS STRING) 
    #         AND CaseStatus_dec IN (37,38)
    #         AND CAST(roundedTimeEstimate AS INT) IN (30, 60, 90, 120, 150, 180,210, 240, 270, 300, 330, 360)
    #     )
    #     """)

    #     checks["valid_listCaseHearingDate"] = (
    #         """
    #         (
    #             listCaseHearingDate <=>
    #                 CONCAT(date_format(CAST(HearingDate AS timestamp), 'yyyy-MM-dd'),'T',
    #                     CASE
    #                     WHEN StartTime IS NULL THEN '00:00:00.000'
    #                     ELSE date_format(CAST(StartTime AS timestamp), 'HH:mm:ss.SSS')
    #                     END)
    #                 AND CaseStatus_dec IN (37,38)
    #         )
    #         """)

    #     checks["valid_listCaseHearingCentre"] = (
    #         "(listCaseHearingCentre <=> bronze_listCaseHearingCentre AND CaseStatus_dec IN (37,38) )" 
    #     )

    #     checks["valid_listCaseHearingCentreAddress"] = (
    #         "(listCaseHearingCentreAddress <=> bronze_listCaseHearingCentreAddress AND CaseStatus_dec IN (37,38))"
    #     )

    #     return checks

    def get_checks_document(self, checks={}):
        
        checks["valid_caseBundles"] = (
            "(COALESCE(size(caseBundles), 0) = 0) "
        )
        return checks

    def get_checks_substantive_decision(self, checks={}):

        checks["valid_scheduleOfIssuesAgreement"] = ("(scheduleOfIssuesAgreement <=> 'No')")

        checks["valid_scheduleOfIssuesDisagreementDescription"] = ("(scheduleOfIssuesDisagreementDescription <=> 'This is a migrated ARIA case. Please see the documents for information on the schedule of issues.')")

        checks["valid_immigrationHistoryAgreement"] = ("(immigrationHistoryAgreement <=> 'No')")

        checks["valid_immigrationHistoryDisagreementDescription"] = ("(immigrationHistoryDisagreementDescription <=> 'This is a migrated ARIA case. Please see the documents for information on the immigration history.')")

        return checks

    def get_checks_general_default(self, checks={}):
        checks["valid_ariaDesiredState"] = "(ariaDesiredState <=> 'decision')"

        checks["valid_hmcts"] = ("(hmcts <=> '[userImage:hmcts.png]')")

        checks["valid_stitchingStatus"] = ("(stitchingStatus <=> 'DONE')")

        checks["valid_bundleConfiguration"] = ("(bundleConfiguration <=> 'iac-hearing-bundle-config.yaml')")

        checks["valid_decisionAndReasonsAvailable"] = ("(decisionAndReasonsAvailable <=> 'No')")

        checks["valid_sendDirectionActionAvailable"] = ("(sendDirectionActionAvailable <=> 'No')")

        checks["valid_changeDirectionDueDateActionAvailable"] = ("(changeDirectionDueDateActionAvailable <=> 'No')")

        checks["valid_markEvidenceAsReviewedActionAvailable"] = ("(markEvidenceAsReviewedActionAvailable <=> 'No')")

        checks["valid_uploadAdditionalEvidenceActionAvailable"] = ("(uploadAdditionalEvidenceActionAvailable <=> 'No')")

        checks["valid_uploadAdditionalEvidenceHomeOfficeActionAvailable"] = ("(uploadAdditionalEvidenceHomeOfficeActionAvailable <=> 'No')")

        checks["valid_uploadAddendumEvidenceActionAvailable"] = ("(uploadAddendumEvidenceActionAvailable <=> 'Yes')")

        checks["valid_markAddendumEvidenceAsReviewedActionAvailable"] = ("(markAddendumEvidenceAsReviewedActionAvailable <=> 'Yes')")

        checks["valid_uploadAddendumEvidenceLegalRepActionAvailable"] = ("(uploadAddendumEvidenceLegalRepActionAvailable <=> 'Yes')")

        checks["valid_uploadAddendumEvidenceHomeOfficeActionAvailable"] = ("(uploadAddendumEvidenceHomeOfficeActionAvailable <=> 'Yes')")

        checks["valid_uploadAddendumEvidenceAdminOfficerActionAvailable"] = ("(uploadAddendumEvidenceAdminOfficerActionAvailable <=> 'Yes')")

        return checks

    def get_checks_general(self, checks={}):

        checks["valid_bundleFileNamePrefix"] = (
            """
            (
                bundleFileNamePrefix <=> replace(CaseNo, '/', ' ') || '-' || Appellant_Name AND bundleFileNamePrefix IS NOT NULL AND bundleFileNamePrefix != ""
            )
            """
        )

        return checks
