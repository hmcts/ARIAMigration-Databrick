def add_checks(checks={}):
    checks = add_checks_substantive_decision(checks)
    checks = add_checks_general_default(checks)
    checks = add_checks_general(checks)
    checks = add_checks_hearing_details(checks)
    checks = add_checks_document(checks)

    return checks


def add_checks_hearing_details(checks={}):

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
    return checks

def add_checks_document(checks={}):
    checks["valid_caseBundles"] = (
        "(size(caseBundles) = 0) "
    )
    return checks

def add_checks_substantive_decision(checks={}):

    checks["valid_scheduleOfIssuesAgreement"] = ( "(scheduleOfIssuesAgreement = 'No')")

    checks["valid_scheduleOfIssuesDisagreementDescription"] = ( "(scheduleOfIssuesDisagreementDescription = 'This is a migrated ARIA case. Please see the documents for information on the schedule of issues.')")

    checks["valid_immigrationHistoryAgreement"] = ( "(immigrationHistoryAgreement = 'No')")

    checks["valid_immigrationHistoryDisagreementDescription"] = ( "(immigrationHistoryDisagreementDescription = 'This is a migrated ARIA case. Please see the documents for information on the immigration history.')")

    return checks

def add_checks_general_default(checks={}):

    checks["valid_hmcts"] = ( "(hmcts = '[userImage:hmcts.png]')")

    checks["valid_stitchingStatus"] = ( "(stitchingStatus = 'DONE')")

    checks["valid_bundleConfiguration"] = ( "(bundleConfiguration = 'iac-hearing-bundle-config.yaml')")

    checks["valid_decisionAndReasonsAvailable"] = ( "(decisionAndReasonsAvailable = 'No')")

    return checks



def add_checks_general(checks={}):

    checks["valid_bundleFileNamePrefix"] = (
        """
        (
            bundleFileNamePrefix = replace(CaseNo, '/', ' ') || '_' || Appellant_Name
        )
        """
    )

    return checks



if __name__ == "__main__":
    pass