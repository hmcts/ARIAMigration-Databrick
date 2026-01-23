def add_checks(checks={}):
    checks = add_checks_hearing_details(checks)
    checks = add_checks_document(checks)
    checks = add_checks_substantive_decision(checks)
    checks = add_checks_general_default(checks)
    checks = add_checks_general(checks)
    
    return checks


def add_checks_hearing_details(checks={}):

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

    checks["valid_listCaseHearingCentre"] = (
        "(listCaseHearingCentre[0] = bronz_listCaseHearingCentre)"
    )

    checks["valid_listCaseHearingCentreAddress"] = (
        "(listCaseHearingCentreAddress = bronz_listCaseHearingCentreAddress)"
    )
    
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
            bundleFileNamePrefix = replace(CaseNo, '/', ' ') || '-' || Appellant_Name
        )
        """
    )

    return checks


if __name__ == "__main__":
    pass