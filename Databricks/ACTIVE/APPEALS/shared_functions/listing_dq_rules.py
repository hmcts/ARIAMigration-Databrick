def add_checks(checks={}):
    checks = add_checks_hearing_requirements(checks)
    checks = add_checks_general(checks)
    checks = add_checks_general_default(checks)
    checks = add_checks_document(checks)

    return checks


def add_checks_hearing_requirements(checks={}):
    checks["valid_isAppellantAttendingTheHearing"] = (
        "(isAppellantAttendingTheHearing = 'Yes')"
    )

    checks["valid_isAppellantGivingOralEvidence"] = (
        "(isAppellantGivingOralEvidence = 'Yes')"
    )

    checks["valid_isWitnessesAttending"] = (
        "(isWitnessesAttending = 'No')"
    )

    checks["valid_isEvidenceFromOutsideUkOoc"] = (
        """(
            (valid_categoryIdList IS NULL AND isEvidenceFromOutsideUkOoc IS NULL)
            OR
            (NOT(ARRAY_CONTAINS(valid_categoryIdList, 38)) AND isEvidenceFromOutsideUkOoc IS NULL)
            OR
            (ARRAY_CONTAINS(valid_categoryIdList, 38) AND Sponsor_Name IS NOT NULL AND isEvidenceFromOutsideUkOoc = 'Yes')
            OR
            (ARRAY_CONTAINS(valid_categoryIdList, 38) AND Sponsor_Name IS NULL AND isEvidenceFromOutsideUkOoc = 'No')
        )"""
    )

    checks["valid_isEvidenceFromOutsideUkInCountry"] = (
        """(
            (valid_categoryIdList IS NULL AND isEvidenceFromOutsideUkInCountry IS NULL)
            OR
            (NOT(ARRAY_CONTAINS(valid_categoryIdList, 37)) AND isEvidenceFromOutsideUkInCountry IS NULL)
            OR
            (ARRAY_CONTAINS(valid_categoryIdList, 37) AND Sponsor_Name IS NOT NULL AND isEvidenceFromOutsideUkInCountry = 'Yes')
            OR
            (ARRAY_CONTAINS(valid_categoryIdList, 37) AND Sponsor_Name IS NULL AND isEvidenceFromOutsideUkInCountry = 'No')
        )"""
    )

    checks["valid_isInterpreterServicesNeeded"] = (
        """(
            (Interpreter = 1 AND isInterpreterServicesNeeded = 'Yes')
            OR
            (Interpreter = 2 AND isInterpreterServicesNeeded = 'No')
            OR
            (Interpreter NOT IN (1, 2) AND isInterpreterServicesNeeded = 'No')
        )"""
    )

    checks["valid_appellantInterpreterLanguageCategory"] = (
        """(
            CASE
                WHEN (
                    (LanguageId IS NULL OR LanguageId = 0)
                    AND
                    (AdditionalLanguageId IS NULL OR AdditionalLanguageId = 0)
                ) THEN (
                    ARRAY_SIZE(appellantInterpreterLanguageCategory) = 0
                ) ELSE (
                    (
                        (LanguageId IS NULL OR LanguageId = 0)
                        OR
                        (LanguageId IS NOT NULL AND LanguageId != 0 AND ARRAY_CONTAINS(appellantInterpreterLanguageCategory, valid_languageCategory))
                    )
                    AND
                    (
                        (AdditionalLanguageId IS NULL OR AdditionalLanguageId = 0)
                        OR
                        (AdditionalLanguageId IS NOT NULL AND AdditionalLanguageId != 0 AND ARRAY_CONTAINS(appellantInterpreterLanguageCategory, valid_additionalLanguageCategory))
                    )
                )
            END
        )"""
    )

    checks["valid_appellantInterpreterSpokenLanguage"] = (
        """(
            CASE
                WHEN (
                    (LanguageId IS NULL OR LanguageId = 0 OR valid_languageCategory != 'spokenLanguageInterpreter')
                    AND
                    (AdditionalLanguageId IS NULL OR AdditionalLanguageId = 0 OR valid_additionalLanguageCategory != 'spokenLanguageInterpreter')
                ) THEN (
                    appellantInterpreterSpokenLanguage IS NULL
                ) ELSE (
                    (
                        (
                            (LanguageId IS NULL OR LanguageId = 0 OR valid_languageCategory != 'spokenLanguageInterpreter')
                            OR
                            (
                                CASE
                                    WHEN (
                                        (valid_manualEntry = 'Yes')
                                    ) THEN (
                                        (ARRAY_CONTAINS(appellantInterpreterSpokenLanguage.languageManualEntry, valid_manualEntry))
                                        AND
                                        (ARRAY_CONTAINS(appellantInterpreterSpokenLanguage.languageManualEntryDescription, valid_manualEntryDescription))
                                        AND
                                        (appellantInterpreterSpokenLanguage.languageRefData IS NULL OR appellantInterpreterSpokenLanguage.languageRefData.value IS NULL OR ARRAY_SIZE(appellantInterpreterSpokenLanguage.languageRefData.value) < 2)
                                    ) ELSE (
                                        (ARRAY_CONTAINS(TRANSFORM(appellantInterpreterSpokenLanguage.languageRefData.value, x -> x.code), valid_languageCode))
                                        AND
                                        (ARRAY_CONTAINS(TRANSFORM(appellantInterpreterSpokenLanguage.languageRefData.value, x -> x.label), valid_languageLabel))
                                        AND
                                        (ARRAY_SIZE(COALESCE(appellantInterpreterSpokenLanguage.languageManualEntry, ARRAY())) < 2)
                                        AND
                                        (ARRAY_SIZE(COALESCE(appellantInterpreterSpokenLanguage.languageManualEntryDescription, ARRAY())) < 2)
                                    )
                                END
                            )
                        )
                        AND
                        (
                            (AdditionalLanguageId IS NULL OR AdditionalLanguageId = 0 OR valid_additionalLanguageCategory != 'spokenLanguageInterpreter')
                            OR
                            (
                                CASE
                                    WHEN (
                                        (valid_additionalManualEntry = 'Yes')
                                    ) THEN (
                                        (ARRAY_CONTAINS(appellantInterpreterSpokenLanguage.languageManualEntry, valid_additionalManualEntry))
                                        AND
                                        (ARRAY_CONTAINS(appellantInterpreterSpokenLanguage.languageManualEntryDescription, valid_additionalManualEntryDescription))
                                        AND
                                        (appellantInterpreterSpokenLanguage.languageRefData IS NULL OR appellantInterpreterSpokenLanguage.languageRefData.value IS NULL OR ARRAY_SIZE(appellantInterpreterSpokenLanguage.languageRefData.value) < 2)
                                    ) ELSE (
                                        (ARRAY_CONTAINS(TRANSFORM(appellantInterpreterSpokenLanguage.languageRefData.value, x -> x.code), valid_additionalLanguageCode))
                                        AND
                                        (ARRAY_CONTAINS(TRANSFORM(appellantInterpreterSpokenLanguage.languageRefData.value, x -> x.label), valid_additionalLanguageLabel))
                                        AND
                                        (ARRAY_SIZE(COALESCE(appellantInterpreterSpokenLanguage.languageManualEntry, ARRAY())) < 2)
                                        AND
                                        (ARRAY_SIZE(COALESCE(appellantInterpreterSpokenLanguage.languageManualEntryDescription, ARRAY())) < 2)
                                    )
                                END
                            )
                        )
                    )
                )
            END
        )"""
    )

    checks["valid_appellantInterpreterSignLanguage"] = (
        """(
            CASE
                WHEN (
                    (LanguageId IS NULL OR LanguageId = 0 OR valid_languageCategory != 'signLanguageInterpreter')
                    AND
                    (AdditionalLanguageId IS NULL OR AdditionalLanguageId = 0 OR valid_additionalLanguageCategory != 'signLanguageInterpreter')
                ) THEN (
                    appellantInterpreterSignLanguage IS NULL
                ) ELSE (
                    (
                        (
                            (LanguageId IS NULL OR LanguageId = 0 OR valid_languageCategory != 'signLanguageInterpreter')
                            OR
                            (
                                CASE
                                    WHEN (
                                        (valid_manualEntry = 'Yes')
                                    ) THEN (
                                        (ARRAY_CONTAINS(appellantInterpreterSignLanguage.languageManualEntry, valid_manualEntry))
                                        AND
                                        (ARRAY_CONTAINS(appellantInterpreterSignLanguage.languageManualEntryDescription, valid_manualEntryDescription))
                                        AND
                                        (appellantInterpreterSignLanguage.languageRefData IS NULL OR appellantInterpreterSignLanguage.languageRefData.value IS NULL OR ARRAY_SIZE(appellantInterpreterSignLanguage.languageRefData.value) < 2)
                                    ) ELSE (
                                        (ARRAY_CONTAINS(TRANSFORM(appellantInterpreterSignLanguage.languageRefData.value, x -> x.code), valid_languageCode))
                                        AND
                                        (ARRAY_CONTAINS(TRANSFORM(appellantInterpreterSignLanguage.languageRefData.value, x -> x.label), valid_languageLabel))
                                        AND
                                        (ARRAY_SIZE(COALESCE(appellantInterpreterSignLanguage.languageManualEntry, ARRAY())) < 2)
                                        AND
                                        (ARRAY_SIZE(COALESCE(appellantInterpreterSignLanguage.languageManualEntryDescription, ARRAY())) < 2)
                                    )
                                END
                            )
                        )
                        AND
                        (
                            (AdditionalLanguageId IS NULL OR AdditionalLanguageId = 0 OR valid_additionalLanguageCategory != 'signLanguageInterpreter')
                            OR
                            (
                                CASE
                                    WHEN (
                                        (valid_additionalManualEntry = 'Yes')
                                    ) THEN (
                                        (ARRAY_CONTAINS(appellantInterpreterSignLanguage.languageManualEntry, valid_additionalManualEntry))
                                        AND
                                        (ARRAY_CONTAINS(appellantInterpreterSignLanguage.languageManualEntryDescription, valid_additionalManualEntryDescription))
                                        AND
                                        (appellantInterpreterSignLanguage.languageRefData IS NULL OR appellantInterpreterSignLanguage.languageRefData.value IS NULL OR ARRAY_SIZE(appellantInterpreterSignLanguage.languageRefData.value) < 2)
                                    ) ELSE (
                                        (ARRAY_CONTAINS(TRANSFORM(appellantInterpreterSignLanguage.languageRefData.value, x -> x.code), valid_additionalLanguageCode))
                                        AND
                                        (ARRAY_CONTAINS(TRANSFORM(appellantInterpreterSignLanguage.languageRefData.value, x -> x.label), valid_additionalLanguageLabel))
                                        AND
                                        (ARRAY_SIZE(COALESCE(appellantInterpreterSignLanguage.languageManualEntry, ARRAY())) < 2)
                                        AND
                                        (ARRAY_SIZE(COALESCE(appellantInterpreterSignLanguage.languageManualEntryDescription, ARRAY())) < 2)
                                    )
                                END
                            )
                        )
                    )
                )
            END
        )"""
    )

    checks["valid_isHearingRoomNeeded"] = (
        "(isHearingRoomNeeded = 'Yes')"
    )

    checks["valid_isHearingLoopNeeded"] = (
        "(isHearingLoopNeeded = 'Yes')"
    )

    checks["valid_remoteVideoCall"] = (
        "(remoteVideoCall = 'Yes')"
    )
        
    checks["valid_remoteVideoCallDescription"] = (
        "(remoteVideoCallDescription = 'This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form.')"
    )

    checks["valid_physicalOrMentalHealthIssues"] = (
        "(physicalOrMentalHealthIssues = 'Yes')"
    )
    
    checks["valid_physicalOrMentalHealthIssuesDescription"] = (
        "(physicalOrMentalHealthIssuesDescription = 'This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form.')"
    )

    checks["valid_pastExperiences"] = (
        "(pastExperiences = 'Yes')"
    )

    checks["valid_pastExperiencesDescription"] = (
        "(pastExperiencesDescription = 'This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form.')"
    )

    checks["valid_multimediaEvidence"] = (
        "(multimediaEvidence = 'Yes')"
    )

    checks["valid_multimediaEvidenceDescription"] = (
        "(multimediaEvidenceDescription = 'This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form.')"
    )

    checks["valid_singleSexCourt"] = (
        """(
            (CourtPreference = 0 AND singleSexCourt = 'No')
            OR
            ((CourtPreference = 1 OR CourtPreference = 2) AND singleSexCourt = 'Yes')
            OR
            (CourtPreference NOT IN (0, 1, 2) AND singleSexCourt = 'No') 
        )"""
    )

    checks["valid_singleSexCourtType"] = (
        """(
            (CourtPreference = 1 AND singleSexCourtType = 'All male')
            OR
            (CourtPreference = 2 AND singleSexCourtType = 'All female')
            OR
            (CourtPreference NOT IN (1, 2) AND singleSexCourtType IS NULL) 
        )"""
    )

    checks["valid_singleSexCourtTypeDescription"] = (
        """(
            ((CourtPreference = 1 OR CourtPreference = 2) AND singleSexCourtTypeDescription = 'This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court.')
            OR
            (CourtPreference NOT IN (1, 2) AND singleSexCourtTypeDescription IS NULL) 
        )"""
    )

    checks["valid_inCameraCourt"] = (
        """(
            (InCamera IS NULL AND inCameraCourt = 'No')
            OR
            (InCamera = 1 AND inCameraCourt = 'Yes')
            OR
            (InCamera = 0 AND inCameraCourt = 'No')
            OR
            (INT(InCamera) NOT IN (0, 1) AND inCameraCourt = 'No') 
        )"""
    )
        
    checks["valid_inCameraCourtDescription"] = (
        """(
            (InCamera IS NULL AND inCameraCourtDescription IS NULL)
            OR
            (InCamera = 1 AND inCameraCourtDescription = 'This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the appellants need for an in camera court.')
            OR
            (InCamera != 1 AND inCameraCourtDescription IS NULL) 
        )"""
    )
            
    checks["valid_additionalRequests"] = (
        "(additionalRequests = 'Yes')"
    )
                
    checks["valid_additionalRequestsDescription"] = (
        "(additionalRequestsDescription = 'This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form.')"
    )
                    
    checks["valid_datesToAvoidYesNo"] = (
        "(datesToAvoidYesNo = 'No')"
    )

    return checks


def add_checks_general(checks={}):
    checks["valid_caseArgumentAvailable"] = (
        """(
            (dv_representation = 'LR' AND caseArgumentAvailable = 'Yes')
            OR
            (dv_representation != 'LR' AND caseArgumentAvailable IS NULL)
        )"""
    )

    checks["valid_reasonsForAppealDecision"] = (
        """(
            (dv_representation = 'AIP' AND reasonsForAppealDecision = 'This is a migrated ARIA case. Please see the documents provided as part of the notice of appeal.')
            OR
            (dv_representation != 'AIP' AND reasonsForAppealDecision IS NULL)
        )"""
    )

    return checks


def add_checks_general_default(checks={}):
    checks["valid_appealReviewOutcome"] = (
        "(appealReviewOutcome = 'decisionMaintained')"
    )
        
    checks["valid_appealResponseAvailable"] = (
        "(appealResponseAvailable = 'Yes')"
    )

    checks["valid_reviewedHearingRequirements"] = (
        "(reviewedHearingRequirements = 'No')"
    )

    checks["valid_amendResponseActionAvailableamendResponseActionAvailable"] = (
        "(amendResponseActionAvailable = 'Yes')"
    )

    checks["valid_currentHearingDetailsVisible"] = (
        "(currentHearingDetailsVisible = 'Yes')"
    )

    checks["valid_reviewResponseActionAvailable"] = (
        "(reviewResponseActionAvailable = 'No')"
    )

    checks["valid_reviewHomeOfficeResponseByLegalRep"] = (
        "(reviewHomeOfficeResponseByLegalRep = 'Yes')"
    )

    checks["valid_submitHearingRequirementsAvailable"] = (
        "(submitHearingRequirementsAvailable = 'Yes')"
    )

    checks["valid_uploadHomeOfficeAppealResponseActionAvailable"] = (
        "(uploadHomeOfficeAppealResponseActionAvailable = 'No')"
    )

    return checks


def add_checks_document(checks={}):
    checks["valid_hearingRequirements"] = (
        "(ARRAY_SIZE(hearingRequirements) = 0)"
    )

    return checks


if __name__ == "__main__":
    pass