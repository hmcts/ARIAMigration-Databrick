def add_checks(checks={}):
    checks = add_checks_hearing_actuals(checks)
    checks = add_checks_document(checks)
    checks = add_checks_substantive_decision(checks)
    checks = add_checks_general_default(checks)
    checks = add_checks_ftpa(checks)
    
    return checks


def add_checks_hearing_actuals(checks={}):

    checks["valid_actualCaseHearingLength"] = ("""
    (HearingDuration IS NULL AND
        element_at(actualCaseHearingLength, 'hours') IS NULL AND
        element_at(actualCaseHearingLength, 'minutes') IS NULL
    ) OR (
        HearingDuration IS NOT NULL AND
        element_at(actualCaseHearingLength, 'hours') >=0 AND
        element_at(actualCaseHearingLength, 'minutes') >=0
    )
    """)

    checks["valid_attendingJudge"] = (
        """
        (
            attendingJudge = concat(Adj_Determination_Title, ' ' ,'Adj_Determination_Forenames', ' ' ,'Adj_Determination_Surname')
        )
        """)
    
    return checks

def add_checks_substantive_decision(checks={}):

    checks["valid_sendDecisionsAndReasonsDate"] = """
        (
            
            DecisionDate IS NULL

            OR

           to_date(DecisionDate, 'dd/MM/yyyy') = sendDecisionsAndReasonsDate

        )
        """

    checks["valid_appealDate"] = """
    (
        
        DecisionDate IS NULL

        OR

        to_date(DecisionDate, 'dd/MM/yyyy') = appealDate

    )
    """

    checks["valid_anonymityOrder"] = ( "(anonymityOrder = 'No')")

    checks["valid_appealDecision"] = """
        (
        CASE
            WHEN Outcome = 1 THEN (appealDecision = 'Allowed')
            WHEN Outcome = 2 THEN (appealDecision = 'Dismissed')
            ELSE (appealDecision IS NULL)
            END
        )
        """
    checks["valid_isDecisionAllowed"] = """
        (
        CASE
            WHEN Outcome = 1 THEN (isDecisionAllowed = 'Allowed')
            WHEN Outcome = 2 THEN (isDecisionAllowed = 'Dismissed')
            ELSE (isDecisionAllowed IS NULL)
            END
        )
        """

    return checks

def add_checks_document(checks={}):
    checks["valid_finalDecisionAndReasonsDocuments"] = (
        "(size(finalDecisionAndReasonsDocuments) = 0)"
    )
    return checks


def add_checks_general_default(checks={}):

    checks["valid_appealDecisionAvailable"] = ( "(appealDecisionAvailable = 'Yes')")

    return checks

def add_checks_ftpa(checks={}):

    checks["valid_ftpaApplicationDeadline"] = """
        (
            
            DecisionDate IS NULL

            OR

            CASE
            WHEN CategoryId = 37 THEN date_add(to_date(DecisionDate, 'dd/MM/yyyy'), 14) = ftpaApplicationDeadline
            WHEN CategoryId = 38 THEN date_add(to_date(DecisionDate, 'dd/MM/yyyy'), 28) = ftpaApplicationDeadline
            ELSE to_date(DecisionDate, 'dd/MM/yyyy') = ftpaApplicationDeadline
            END
        )
        """

    return checks


if __name__ == "__main__":
    pass