from .dq_rules import DQRulesBase


class prepareForHearingDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_hearing_response()
        checks = checks | self.get_checks_hearing_details()
        checks = checks | self.get_checks_document()

        return checks

    def get_checks_hearing_response(self, checks={}):
        checks["valid_isRemoteHearing"] = ("(isRemoteHearing <=> 'No')")

        checks["valid_isAppealSuitableToFloat"] = (
            """(
                CaseStatus_dec IS NOT NULL AND CaseStatus_dec IN (37,38)
                AND
                (
                    (listTypeId <=> 5 AND isAppealSuitableToFloat <=> 'Yes')
                    OR
                    ((listTypeId != 5 OR listTypeId IS NULL) AND isAppealSuitableToFloat <=> 'No')
                )
            )
            OR
            (
                (CaseStatus_dec IS NULL OR CaseStatus_dec NOT IN (37,38))
                AND isAppealSuitableToFloat IS NULL
            )"""
        )

        checks["valid_isMultimediaAllowed"] = ("(isMultimediaAllowed <=> 'Granted')")

        checks["valid_multimediaTribunalResponse"] = ("(multimediaTribunalResponse <=> 'This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_multimediaDecisionForDisplay"] = ("(multimediaDecisionForDisplay <=> 'Granted - This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_isInCameraCourtAllowed"] = (
            """(
                (InCamera <=> 1 AND isInCameraCourtAllowed <=> 'Granted')
                OR
                (NOT(InCamera <=> 1) AND isInCameraCourtAllowed IS NULL)
            )"""
        )

        checks["valid_inCameraCourtTribunalResponse"] = (
            """(
                (InCamera <=> 1 AND inCameraCourtTribunalResponse <=> 'This is a migrated ARIA case. Please refer to the documents.')
                OR
                (NOT(InCamera <=> 1) AND inCameraCourtTribunalResponse IS NULL)
            )"""
        )

        checks["valid_inCameraCourtDecisionForDisplay"] = (
            """(
                (InCamera <=> 1 AND inCameraCourtDecisionForDisplay <=> 'Granted - This is a migrated ARIA case. Please refer to the documents.')
                OR
                (NOT(InCamera <=> 1) AND inCameraCourtDecisionForDisplay IS NULL)
            )"""
        )

        checks["valid_isSingleSexCourtAllowed"] = (
            """(
                (CourtPreference IS NOT NULL AND CourtPreference IN (1, 2) AND isSingleSexCourtAllowed <=> 'Granted')
                OR
                ((CourtPreference IS NULL OR CourtPreference NOT IN (1, 2)) AND isSingleSexCourtAllowed IS NULL)
            )"""
        )

        checks["valid_singleSexCourtTribunalResponse"] = (
            """(
                (CourtPreference IS NOT NULL AND CourtPreference IN (1, 2) AND singleSexCourtTribunalResponse <=> 'This is a migrated ARIA case. Please refer to the documents.')
                OR
                ((CourtPreference IS NULL OR CourtPreference NOT IN (1, 2)) AND singleSexCourtTribunalResponse IS NULL)
            )"""
        )

        checks["valid_singleSexCourtDecisionForDisplay"] = (
            """(
                (CourtPreference IS NOT NULL AND CourtPreference IN (1, 2) AND singleSexCourtDecisionForDisplay <=> 'Granted - This is a migrated ARIA case. Please refer to the documents.')
                OR
                ((CourtPreference IS NULL OR CourtPreference NOT IN (1, 2)) AND singleSexCourtDecisionForDisplay IS NULL)
            )"""
        )

        checks["valid_isVulnerabilitiesAllowed"] = ("(isVulnerabilitiesAllowed <=> 'Granted')")

        checks["valid_vulnerabilitiesTribunalResponse"] = ("(vulnerabilitiesTribunalResponse <=> 'This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_vulnerabilitiesDecisionForDisplay"] = ("(vulnerabilitiesDecisionForDisplay <=> 'Granted - This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_isRemoteHearingAllowed"] = ("(isRemoteHearingAllowed <=> 'Granted')")

        checks["valid_remoteVideoCallTribunalResponse"] = ("(remoteVideoCallTribunalResponse <=> 'This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_remoteHearingDecisionForDisplay"] = ("(remoteHearingDecisionForDisplay <=> 'Granted - This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_isAdditionalAdjustmentsAllowed"] = ("(isAdditionalAdjustmentsAllowed <=> 'Granted')")

        checks["valid_additionalTribunalResponse"] = ("(additionalTribunalResponse <=> 'This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_otherDecisionForDisplay"] = ("(otherDecisionForDisplay <=> 'Granted - This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_isAdditionalInstructionAllowed"] = ("(isAdditionalInstructionAllowed <=> 'Yes')")

        checks["valid_additionalInstructionsTribunalResponse"] = ("""
            additionalInstructionsTribunalResponse IS NULL OR
            (
                LENGTH(additionalInstructionsTribunalResponse) <= 2000 AND
                (
                    LENGTH(additionalInstructionsTribunalResponse) = 2000 OR
                    (
                        additionalInstructionsTribunalResponse LIKE 'Listed details from ARIA: %' AND
                        additionalInstructionsTribunalResponse LIKE '%\\nHearing Centre: %' AND
                        additionalInstructionsTribunalResponse LIKE '%\\nHearing Date: %' AND
                        additionalInstructionsTribunalResponse LIKE '%\\nHearing Type: %' AND
                        additionalInstructionsTribunalResponse LIKE '%\\nCourt: %' AND
                        additionalInstructionsTribunalResponse LIKE '%\\nList Type: %' AND
                        additionalInstructionsTribunalResponse LIKE '%\\nList Start Time: %' AND
                        additionalInstructionsTribunalResponse LIKE '%\\nJudge First Tier: %' AND
                        additionalInstructionsTribunalResponse LIKE '%\\nCourt Clerk / Usher: %' AND
                        additionalInstructionsTribunalResponse LIKE '%\\nStart Time: %' AND
                        additionalInstructionsTribunalResponse LIKE '%\\nEstimated Duration: %' AND
                        additionalInstructionsTribunalResponse LIKE '%\\nRequired/Incompatible Judicial Officers: %' AND
                        additionalInstructionsTribunalResponse LIKE '%\\nNotes: %'
                    )
                )
            )
        """)

        return checks

    def get_checks_hearing_details(self, checks={}):

        checks["valid_listingLength"] = """
        (
            listingLength IS NULL
            OR
            (
                element_at(listingLength, 'hours') <=>
                    CASE
                        WHEN TimeEstimate IS NULL THEN 0
                        WHEN CAST(TimeEstimate AS INT) = 0 THEN 0
                        WHEN (CAST(TimeEstimate AS INT) % 60) >= 45
                            THEN FLOOR(CAST(TimeEstimate AS INT) / 60) + 1
                        ELSE FLOOR(CAST(TimeEstimate AS INT) / 60)
                    END

                AND

                element_at(listingLength, 'minutes') <=>
                    CASE
                        WHEN TimeEstimate IS NULL THEN 30
                        WHEN CAST(TimeEstimate AS INT) = 0 THEN 30
                        WHEN (CAST(TimeEstimate AS INT) % 60) = 0 THEN 0
                        WHEN (CAST(TimeEstimate AS INT) % 60) < 45 THEN 30
                        ELSE 0
                    END

                AND
                (
                    element_at(listingLength, 'minutes') IN (0,30)
                    OR element_at(listingLength, 'minutes') IS NULL
                )
            )
        )
        """

        checks["valid_hearingChannel"] = ("""
        (
        -- Case: VisitVisaType = 1
        (
            VisitVisaType <=> 1 AND
            hearingChannel.value.code <=> 'ONPPRS' AND
            hearingChannel.value.label <=> 'On The Papers'
        )
        )
        OR
        (
        -- Case: VisitVisaType = 2
        (
            VisitVisaType <=> 2 AND
            hearingChannel.value.code <=> 'INTER' AND
            hearingChannel.value.label <=> 'In Person'
        )
        )
        OR
        (
        -- Case: Other / NULL VisitVisaType → both code and label must be NULL
        (
            (VisitVisaType IS NULL OR (VisitVisaType <> 1 AND VisitVisaType <> 2)) AND
            hearingChannel.value.code IS NULL AND
            hearingChannel.value.label IS NULL
        )
        )
        """)

        checks["valid_witnessDetails"] = (
            "(COALESCE(size(witnessDetails), 0) = 0)"
        )

        checks["valid_listingLocation"] = ("""
        (
            (
                ListedCentre IS NULL
                OR
                (
                    listingLocation.value.code <=> locationCode AND
                    listingLocation.value.label <=> locationLabel
                )
            )
            OR
            (
                ListedCentre IS NOT NULL
                AND
                (
                    listingLocation.value.code IS NOT NULL AND listingLocation.value.label IS NOT NULL
                )
            )
        )
        """)

        checks["valid_witness1InterpreterSignLanguage"] = (
            "(COALESCE(size(witness1InterpreterSignLanguage), 0) = 0)"
        )
        checks["valid_witness2InterpreterSignLanguage"] = (
            "(COALESCE(size(witness2InterpreterSignLanguage), 0) = 0)"
        )
        checks["valid_witness3InterpreterSignLanguage"] = (
            "(COALESCE(size(witness3InterpreterSignLanguage), 0) = 0)"
        )
        checks["valid_witness4InterpreterSignLanguage"] = (
            "(COALESCE(size(witness4InterpreterSignLanguage), 0) = 0)"
        )
        checks["valid_witness5InterpreterSignLanguage"] = (
            "(COALESCE(size(witness5InterpreterSignLanguage), 0) = 0)"
        )
        checks["valid_witness6InterpreterSignLanguage"] = (
            "(COALESCE(size(witness6InterpreterSignLanguage), 0) = 0)"
        )
        checks["valid_witness7InterpreterSignLanguage"] = (
            "(COALESCE(size(witness7InterpreterSignLanguage), 0) = 0)"
        )
        checks["valid_witness8InterpreterSignLanguage"] = (
            "(COALESCE(size(witness8InterpreterSignLanguage), 0) = 0)"
        )
        checks["valid_witness9InterpreterSignLanguage"] = (
            "(COALESCE(size(witness9InterpreterSignLanguage), 0) = 0)"
        )
        checks["valid_witness10InterpreterSignLanguage"] = (
            "(COALESCE(size(witness10InterpreterSignLanguage), 0) = 0)"
        )
        checks["valid_witness1InterpreterSpokenLanguage"] = (
            "(COALESCE(size(witness1InterpreterSpokenLanguage), 0) = 0)"
        )
        checks["valid_witness2InterpreterSpokenLanguage"] = (
            "(COALESCE(size(witness2InterpreterSpokenLanguage), 0) = 0)"
        )
        checks["valid_witness3InterpreterSpokenLanguage"] = (
            "(COALESCE(size(witness3InterpreterSpokenLanguage), 0) = 0)"
        )
        checks["valid_witness4InterpreterSpokenLanguage"] = (
            "(COALESCE(size(witness4InterpreterSpokenLanguage), 0) = 0)"
        )
        checks["valid_witness5InterpreterSpokenLanguage"] = (
            "(COALESCE(size(witness5InterpreterSpokenLanguage), 0) = 0)"
        )
        checks["valid_witness6InterpreterSpokenLanguage"] = (
            "(COALESCE(size(witness6InterpreterSpokenLanguage), 0) = 0)"
        )
        checks["valid_witness7InterpreterSpokenLanguage"] = (
            "(COALESCE(size(witness7InterpreterSpokenLanguage), 0) = 0)"
        )
        checks["valid_witness8InterpreterSpokenLanguage"] = (
            "(COALESCE(size(witness8InterpreterSpokenLanguage), 0) = 0)"
        )
        checks["valid_witness9InterpreterSpokenLanguage"] = (
            "(COALESCE(size(witness9InterpreterSpokenLanguage), 0) = 0)"
        )
        checks["valid_witness10InterpreterSpokenLanguage"] = (
            "(COALESCE(size(witness10InterpreterSpokenLanguage), 0) = 0)"
        )

        checks["valid_listCaseHearingLength"] = ("""
        (
            (
                CAST(roundedTimeEstimate AS STRING) <=> CAST(listCaseHearingLength AS STRING)
                AND CaseStatus_dec IS NOT NULL AND CaseStatus_dec IN (37,38)
                AND roundedTimeEstimate IS NOT NULL AND CAST(roundedTimeEstimate AS INT) IN (30, 60, 90, 120, 150, 180,210, 240, 270, 300, 330, 360)
            )
            OR
            (
                (CaseStatus_dec NOT IN (37,38) OR CaseStatus_dec IS NULL) AND roundedTimeEstimate IS NULL
            )
        )
        """)

        checks["valid_listCaseHearingDate"] = (
            """
            (
                (
                    listCaseHearingDate IS NOT NULL
                    AND listCaseHearingDate <=>
                        CONCAT(date_format(CAST(HearingDate AS timestamp), 'yyyy-MM-dd'),'T',
                            CASE
                            WHEN StartTime IS NULL THEN '00:00:00.000'
                            ELSE date_format(CAST(StartTime AS timestamp), 'HH:mm:ss.SSS')
                            END)
                        AND CaseStatus_dec IS NOT NULL AND CaseStatus_dec IN (37,38)
                )
            )
            """)

        checks["valid_listCaseHearingCentre"] = (
            """
            (
                (
                    listCaseHearingCentre <=> bronze_listCaseHearingCentre AND CaseStatus_dec IS NOT NULL AND CaseStatus_dec IN (37,38)
                )
                OR
                (
                    (CaseStatus_dec NOT IN (37,38) OR CaseStatus_dec IS NULL) AND listCaseHearingCentre IS NULL
                )
            )
        """)

        checks["valid_listCaseHearingCentreAddress"] = (
            """
            (
                (
                    listCaseHearingCentreAddress <=> bronze_listCaseHearingCentreAddress AND CaseStatus_dec IS NOT NULL AND CaseStatus_dec IN (37,38)
                )
                OR
                (
                    (CaseStatus_dec NOT IN (37,38) OR CaseStatus_dec IS NULL) AND listCaseHearingCentreAddress IS NULL
                )
            )
            """)

        return checks

    def get_checks_document(self, checks={}):
        checks["valid_hearingDocuments"] = (
            "(COALESCE(size(hearingDocuments), 0) = 0) "
        )

        checks["valid_letterBundleDocuments"] = (
            "(COALESCE(size(letterBundleDocuments), 0) = 0) "
        )

        return checks
