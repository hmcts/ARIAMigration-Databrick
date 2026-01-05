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
            (CategoryId = '38' AND isEvidenceFromOutsideUkOoc = 'Yes') 
            OR 
            (CategoryId != '38' AND isEvidenceFromOutsideUkOoc IS NULL)
        )"""
    )

    checks["valid_isEvidenceFromOutsideUkInCountry"] = (
        """(
            (CategoryId = '37' AND isEvidenceFromOutsideUkInCountry = 'Yes') 
            OR 
            (CategoryId != '37' AND isEvidenceFromOutsideUkInCountry IS NULL)
        )"""
    )

    checks["valid_isInterpreterServicesNeeded"] = (
        """(
            (Interpreter = 1 AND isInterpreterServicesNeeded = 'Yes')
            OR
            (Interpreter = 0 AND isInterpreterServicesNeeded = 'No')
        )"""
    )

    checks["valid_appellantInterpreterLanguageCategory"] = (
        """(
            (ARRAY_SIZE(appellantInterpreterLanguageCategory) = 0)
            OR
            (ARRAY_SIZE(ARRAY_EXCEPT(appellantInterpreterLanguageCategory, ARRAY('spokenLanguageInterpreter', 'signLanguageInterpreter'))) = 0)
        )"""
    )

    checks["valid_appellantInterpreterSpokenLanguage"] = (
        """(
            (
                (appellantInterpreterSpokenLanguage.languageRefData IS NULL)
            )
            OR
            (ARRAY_SIZE(appellantInterpreterSpokenLanguage.languageRefData) > 0)
            AND
            (ARRAY_SIZE(appellantInterpreterSpokenLanguage.list_items) > 0)
        )"""
    )

    checks["valid_appellantInterpreterSignLanguage"] = (
        """(
            (
                (appellantInterpreterSignLanguage.languageRefData IS NULL)
            )
            OR
            (ARRAY_SIZE(appellantInterpreterSignLanguage.languageRefData) > 0)
            AND
            (ARRAY_SIZE(appellantInterpreterSignLanguage.list_items) > 0)
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
            (CourtPreference NOT IN (0, 1, 2) AND singleSexCourt IS NULL) 
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
            (InCamera = 1 AND inCameraCourt = 'Yes')
            OR
            (InCamera = 0 AND inCameraCourt = 'No')
            OR
            (InCamera NOT IN (0, 1) AND inCameraCourt IS NULL) 
        )"""
    )
        
    checks["valid_inCameraCourtDescription"] = (
        """(
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
            (dv_representation = 'AIP' AND reasonsForAppealDecision = '"This is a migrated ARIA case. Please see the documents provided as part of the notice of appeal.')
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
        "(uploadHomeOfficeAppealResponseActionAvailable = 'Yes')"
    )

    return checks


def add_checks_document(checks={}):
    checks["valid_hearingRequirements"] = (
        "(hearingRequirements = '[]')"
    )

    return checks


if __name__ == "__main__":
    pass