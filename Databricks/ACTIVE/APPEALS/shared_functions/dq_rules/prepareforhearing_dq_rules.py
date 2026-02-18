from .dq_rules import DQRulesBase


class prepareForHearingDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_hearing_response()
        checks = checks | self.get_checks_hearing_details()
        checks = checks | self.get_checks_document()

        return checks

    def get_checks_hearing_response(self, checks={}):
        checks["valid_isRemoteHearing"] = ("(isRemoteHearing = 'No')")

        checks["valid_isAppealSuitableToFloat"] = (
            """(
                (listTypeId = 5 AND isAppealSuitableToFloat = 'Yes')
                OR
                (listTypeId != 5 AND isAppealSuitableToFloat = 'No')
            )"""
        )

        checks["valid_isMultimediaAllowed"] = ("(isMultimediaAllowed = 'Granted')")

        checks["valid_multimediaTribunalResponse"] = ("(multimediaTribunalResponse = 'This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_multimediaDecisionForDisplay"] = ("(multimediaDecisionForDisplay = 'Granted - This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_isInCameraCourtAllowed"] = (
            """(
                (InCamera = 1 AND isInCameraCourtAllowed = 'Granted')
                OR
                (InCamera != 1 AND isInCameraCourtAllowed IS NULL)
            )"""
        )

        checks["valid_inCameraCourtTribunalResponse"] = (
            """(
                (InCamera = 1 AND inCameraCourtTribunalResponse = 'This is a migrated ARIA case. Please refer to the documents.')
                OR
                (InCamera != 1 AND inCameraCourtTribunalResponse IS NULL)
            )"""
        )

        checks["valid_inCameraCourtDecisionForDisplay"] = (
            """(
                (InCamera = 1 AND inCameraCourtDecisionForDisplay = 'Granted - This is a migrated ARIA case. Please refer to the documents.')
                OR
                (InCamera != 1 AND inCameraCourtDecisionForDisplay IS NULL)
            )"""
        )

        checks["valid_isSingleSexCourtAllowed"] = (
            """(
                (CourtPreference IN (1, 2) AND isSingleSexCourtAllowed = 'Granted')
                OR
                (CourtPreference NOT IN (1, 2) AND isSingleSexCourtAllowed IS NULL)
            )"""
        )

        checks["valid_singleSexCourtTribunalResponse"] = (
            """(
                (CourtPreference IN (1, 2) AND singleSexCourtTribunalResponse = 'This is a migrated ARIA case. Please refer to the documents.')
                OR
                (CourtPreference NOT IN (1, 2)  AND singleSexCourtTribunalResponse IS NULL)
            )"""
        )

        checks["valid_singleSexCourtDecisionForDisplay"] = (
            """(
                (CourtPreference IN (1, 2)  AND singleSexCourtDecisionForDisplay = 'Granted - This is a migrated ARIA case. Please refer to the documents.')
                OR
                (CourtPreference NOT IN (1, 2)  AND singleSexCourtDecisionForDisplay IS NULL)
            )"""
        )

        checks["valid_isVulnerabilitiesAllowed"] = ("(isVulnerabilitiesAllowed = 'Granted')")

        checks["valid_vulnerabilitiesTribunalResponse"] = ("(vulnerabilitiesTribunalResponse = 'This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_vulnerabilitiesDecisionForDisplay"] = ("(vulnerabilitiesDecisionForDisplay = 'Granted - This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_isRemoteHearingAllowed"] = ("(isRemoteHearingAllowed = 'Granted')")

        checks["valid_remoteVideoCallTribunalResponse"] = ("(remoteVideoCallTribunalResponse = 'This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_remoteHearingDecisionForDisplay"] = ("(remoteHearingDecisionForDisplay = 'Granted - This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_isAdditionalAdjustmentsAllowed"] = ("(isAdditionalAdjustmentsAllowed = 'Granted')")

        checks["valid_additionalTribunalResponse"] = ("(additionalTribunalResponse = 'This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_otherDecisionForDisplay"] = ("(otherDecisionForDisplay = 'Granted - This is a migrated ARIA case. Please refer to the documents.')")

        checks["valid_isAdditionalInstructionAllowed"] = ("(isAdditionalInstructionAllowed = 'Yes')")

        checks["valid_additionalInstructionsTribunalResponse"] = ("""
            additionalInstructionsTribunalResponse IS NULL OR
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
        """)

        return checks

    def get_checks_hearing_details(self, checks={}):

        checks["valid_listinglength"] = ("""
        (TimeEstimate IS NULL AND
            element_at(listingLength, 'hours') IS NULL AND
            element_at(listingLength, 'minutes') IS NULL
        ) OR (
            TimeEstimate IS NOT NULL AND
            element_at(listingLength, 'hours') >=0 AND
            element_at(listingLength, 'minutes') >=0
        )
        """)

        checks["valid_hearingChannel"] = ("""
        (
        -- Case: VisitVisaType = 1
        (
            VisitVisaType = 1 AND
            element_at(hearingChannel, 'code') = 'ONPPRS' AND
            element_at(hearingChannel, 'label') = 'On The Papers'
        )
        )
        OR
        (
        -- Case: VisitVisaType = 2
        (
            VisitVisaType = 2 AND
            element_at(hearingChannel, 'code') = 'INTER' AND
            element_at(hearingChannel, 'label') = 'In Person'
        )
        )
        OR
        (
        -- Case: Other / NULL VisitVisaType → both code and label must be NULL
        (
            (VisitVisaType IS NULL OR (VisitVisaType <> 1 AND VisitVisaType <> 2)) AND
            element_at(hearingChannel, 'code') IS NULL AND
            element_at(hearingChannel, 'label') IS NULL
        )
        )
        """)

        checks["valid_witnessDetails"] = (
            "(size(witnessDetails) = 0)"
        )

        checks["listing_location_struct_consistent_when_matched"] = ("""
        (
        ListedCentre IS NULL
        OR
        (
            listingLocation.code = locationCode AND
            listingLocation.label = locationLabel
        )
        )
        """)

        # If no match, both fields in listingLocation must be NULL.
        checks["valid_listinglocation_null_when_not_matched"] = ("""
        (
        ListedCentre IS NOT NULL
        AND
        (listingLocation.code IS NOT NULL AND listingLocation.label IS NOT NULL)
        )
        """)

        checks["valid_witness1InterpreterSignLanguage"] = (
            "(size(witness1InterpreterSignLanguage) = 0)"
        )
        checks["valid_witness2InterpreterSignLanguage"] = (
            "(size(witness2InterpreterSignLanguage) = 0)"
        )
        checks["valid_witness2InterpreterSignLanguage"] = (
            "(size(witness2InterpreterSignLanguage) = 0)"
        )
        checks["valid_witness3InterpreterSignLanguage"] = (
            "(size(witness3InterpreterSignLanguage) = 0)"
        )
        checks["valid_witness4InterpreterSignLanguage"] = (
            "(size(witness4InterpreterSignLanguage) = 0)"
        )
        checks["valid_witness5InterpreterSignLanguage"] = (
            "(size(witness5InterpreterSignLanguage) = 0)"
        )
        checks["valid_witness6InterpreterSignLanguage"] = (
            "(size(witness6InterpreterSignLanguage) = 0)"
        )
        checks["valid_witness7InterpreterSignLanguage"] = (
            "(size(witness7InterpreterSignLanguage) = 0)"
        )
        checks["valid_witness8InterpreterSignLanguage"] = (
            "(size(witness8InterpreterSignLanguage) = 0)"
        )
        checks["valid_witness9InterpreterSignLanguage"] = (
            "(size(witness9InterpreterSignLanguage) = 0)"
        )
        checks["valid_witness10InterpreterSignLanguage"] = (
            "(size(witness10InterpreterSignLanguage) = 0)"
        )
        checks["valid_witness1InterpreterSpokenLanguage"] = (
            "(size(witness1InterpreterSpokenLanguage) = 0)"
        )
        checks["valid_witness2InterpreterSpokenLanguage"] = (
            "(size(witness2InterpreterSpokenLanguage) = 0)"
        )
        checks["valid_witness3InterpreterSpokenLanguage"] = (
            "(size(witness3InterpreterSpokenLanguage) = 0)"
        )
        checks["valid_witness4InterpreterSpokenLanguage"] = (
            "(size(witness4InterpreterSpokenLanguage) = 0)"
        )
        checks["valid_witness5InterpreterSpokenLanguage"] = (
            "(size(witness5InterpreterSpokenLanguage) = 0)"
        )
        checks["valid_witness6InterpreterSpokenLanguage"] = (
            "(size(witness6InterpreterSpokenLanguage) = 0)"
        )
        checks["valid_witness7InterpreterSpokenLanguage"] = (
            "(size(witness7InterpreterSpokenLanguage) = 0)"
        )
        checks["valid_witness8InterpreterSpokenLanguage"] = (
            "(size(witness8InterpreterSpokenLanguage) = 0)"
        )
        checks["valid_witness9InterpreterSpokenLanguage"] = (
            "(size(witness9InterpreterSpokenLanguage) = 0)"
        )
        checks["valid_witness10InterpreterSpokenLanguage"] = (
            "(size(witness10InterpreterSpokenLanguage) = 0)"
        )

        return checks

    def get_checks_document(self, checks={}):
        checks["valid_hearingDocuments"] = (
            "(size(hearingDocuments) = 0) "
        )

        checks["valid_letterBundleDocuments"] = (
            "(size(letterBundleDocuments) = 0) "
        )

        return checks
