def add_checks(checks={}):
    checks = add_checks_hearing_response(checks)
    checks = add_checks_hearing_details(checks)
    checks = add_checks_document(checks)

    return checks


def add_checks_hearing_response(checks={}):
    checks["valid_isRemoteHearing"] = ( "(isRemmoteHearing = 'No')")

    checks["valid_isAppealSuitableToFloat"] = (
        """(
            (listTypeId = 5 AND isAppealSuitableToFloat = 'Yes')
            OR
            (listTypeId != 5 AND isAppealSuitableToFloat = 'No')
        )"""
    )

    checks["valid_isMultimediaAllowed"] = ( "(isMultimediaAllowed = 'Granted')")

    checks["valid_multimediaTribunalResponse"] = ( "(multimediaTribunalResponse = 'This is a migrated ARIA case. Please refer to the documents.')")

    checks["valid_multimediaDecisionForDisplay"] = ( "(multimediaDecisionForDisplay = 'Granted - This is a migrated ARIA case. Please refer to the documents.')")

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

    checks["valid_isVulnerabilitiesAllowed"] = ( "(isVulnerabilitiesAllowed = 'Granted')")

    checks["valid_isVulnerabilitiesAllowed"] = ( "(isVulnerabilitiesAllowed = 'This is a migrated ARIA case. Please refer to the documents.')")

    checks["valid_vulnerabilitiesDecisionForDisplay"] = ( "(vulnerabilitiesDecisionForDisplay = 'Granted - This is a migrated ARIA case. Please refer to the documents.')")

    checks["valid_isRemoteHearingAllowed"] = ( "(isRemoteHearingAllowed = 'Granted')")

    checks["valid_remoteVideoCallTribunalResponse"] = ( "(remoteVideoCallTribunalResponse = 'This is a migrated ARIA case. Please refer to the documents.')")

    checks["valid_remoteHearingDecisionForDisplay"] = ( "(remoteHearingDecisionForDisplay = 'Granted - This is a migrated ARIA case. Please refer to the documents.')")

    checks["valid_isAdditionalAdjustmentsAllowed"] = ( "(isAdditionalAdjustmentsAllowed = 'Granted')")

    checks["valid_additionalTribunalResponse"] = ( "(additionalTribunalResponse = 'This is a migrated ARIA case. Please refer to the documents.')")

    checks["valid_otherDecisionForDisplay"] = ( "(otherDecisionForDisplay = 'Granted - This is a migrated ARIA case. Please refer to the documents.')")

    checks["valid_isAdditionalInstructionAllowed"] = ( "(isAdditionalInstructionAllowed = 'Yes')")

    
    checks["valid_additionalInstructionsTribunalResponse"] = ("""
        additionalInstructionsTribunalResponse IS NULL OR
        (
            additionalInstructionsTribunalResponse LIKE '%\\n Hearing Centre: %' AND
            additionalInstructionsTribunalResponse LIKE '%\\n Hearing Date: %' AND
            additionalInstructionsTribunalResponse LIKE '%\\n Hearing Type: %' AND
            additionalInstructionsTribunalResponse LIKE '%\\n Court: %' AND
            additionalInstructionsTribunalResponse LIKE '%\\n List Type: %' AND
            additionalInstructionsTribunalResponse LIKE '%\\n List Start Time: %' AND
            additionalInstructionsTribunalResponse LIKE '%\\n Judge First Tier: %' AND
            additionalInstructionsTribunalResponse LIKE '%\\n Court Clerk / Usher: %' AND
            additionalInstructionsTribunalResponse LIKE '%\\n Start Time: %' AND
            additionalInstructionsTribunalResponse LIKE '%\\n Estimated Duration: %' AND
            additionalInstructionsTribunalResponse LIKE '%\\n Required/Incompatible Judicial Officers: %' AND
            additionalInstructionsTribunalResponse LIKE '%\\n Notes: %'
        )
    """)


    return checks


def add_checks_hearing_details(checks={}):

    
    checks["valid_listinglength_hours_non_negative"] = """
    (listingLength.hours IS NULL OR listingLength.hours >= 0)
    """

    checks["valid_listinglength_minutes_range"] = """
    (listingLength.minutes IS NULL OR (listingLength.minutes >= 0 AND listingLength.minutes < 60))
    """

    checks["valid_hearingChannel"] = """
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
    """

    checks["valid_witnessDetails"] = (
        "(witnessDetails = '[]')"
    )
    
    checks["listing_location_struct_consistent_when_matched"] = """
    (
    location.ListedCentre IS NULL
    OR
    (
        listingLocation.code = location.locationCode AND
        listingLocation.label = location.locationLabel
    )
    )
    """

    # If no match, both fields in listingLocation must be NULL.
    checks["valid_listinglocation_null_when_not_matched"] = """
    (
    location.ListedCentre IS NOT NULL
    OR
    (listingLocation.code IS NULL AND listingLocation.label IS NULL)
    )
    """

    checks["valid_witness1InterpreterSignLanguage"] = (
        "(witness1InterpreterSignLanguage = '{}')"
    )
    checks["valid_witness2InterpreterSignLanguage"] = (
        "(witness2InterpreterSignLanguage = '{}')"
    )
    checks["valid_witness2InterpreterSignLanguage"] = (
        "(witness2InterpreterSignLanguage = '{}')"
    )
    checks["valid_witness3InterpreterSignLanguage"] = (
        "(witness3InterpreterSignLanguage = '{}')"
    )
    checks["valid_witness4InterpreterSignLanguage"] = (
        "(witness4InterpreterSignLanguage = '{}')"
    )
    checks["valid_witness5InterpreterSignLanguage"] = (
        "(witness5InterpreterSignLanguage = '{}')"
    )
    checks["valid_witness6InterpreterSignLanguage"] = (
        "(witness6InterpreterSignLanguage = '{}')"
    )
    checks["valid_witness7InterpreterSignLanguage"] = (
        "(witness7InterpreterSignLanguage = '{}')"
    )
    checks["valid_witness8InterpreterSignLanguage"] = (
        "(witness8InterpreterSignLanguage = '{}')"
    )
    checks["valid_witness9InterpreterSignLanguage"] = (
        "(witness9InterpreterSignLanguage = '{}')"
    )
    checks["valid_witness10InterpreterSignLanguage"] = (
        "(witness10InterpreterSignLanguage = '{}')"
    )
    checks["valid_witness1InterpreterSpokenLanguage"] = (
        "(witness1InterpreterSpokenLanguage = '{}')"
    )
    checks["valid_witness2InterpreterSpokenLanguage"] = (
        "(witness2InterpreterSpokenLanguage = '{}')"
    )
    checks["valid_witness3InterpreterSpokenLanguage"] = (
        "(witness3InterpreterSpokenLanguage = '{}')"
    )
    checks["valid_witness4InterpreterSpokenLanguage"] = (
        "(witness4InterpreterSpokenLanguage = '{}')"
    )
    checks["valid_witness5InterpreterSpokenLanguage"] = (
        "(witness5InterpreterSpokenLanguage = '{}')"
    )
    checks["valid_witness6InterpreterSpokenLanguage"] = (
        "(witness6InterpreterSpokenLanguage = '{}')"
    )
    checks["valid_witness7InterpreterSpokenLanguage"] = (
        "(witness7InterpreterSpokenLanguage = '{}')"
    )
    checks["valid_witness8InterpreterSpokenLanguage"] = (
        "(witness8InterpreterSpokenLanguage = '{}')"
    )
    checks["valid_witness9InterpreterSpokenLanguage"] = (
        "(witness9InterpreterSpokenLanguage = '{}')"
    )
    checks["valid_witness10InterpreterSpokenLanguage"] = (
        "(witness10InterpreterSpokenLanguage = '{}')"
    )

    return checks

def add_checks_document(checks={}):
    checks["valid_hearingDocuments"] = (
        "(hearingDocuments = '[]')"
    )

    checks["valid_letterBundleDocuments"] = (
        "(letterBundleDocuments = '[]')"
    )

    return checks


if __name__ == "__main__":
    pass