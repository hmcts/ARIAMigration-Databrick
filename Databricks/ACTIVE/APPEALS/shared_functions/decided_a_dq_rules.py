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
            attendingJudge = concat(Adj_Determination_Title, ' ', Adj_Determination_Forenames, ' ', Adj_Determination_Surname)
        )
        """
    )

    
    return checks

def add_checks_substantive_decision(checks={}):


    checks["valid_sendDecisionsAndReasonsDate"] = """
    (
        -- allow missing output if that's acceptable; remove the next two lines if not
        sendDecisionsAndReasonsDate IS NULL
        OR trim(sendDecisionsAndReasonsDate) = ''
        OR
        -- compare dates after parsing both sides
        to_date(
            to_timestamp(DecisionDate, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX')
        ) = to_date(trim(sendDecisionsAndReasonsDate), 'dd/MM/yyyy')
    )
    """


    
    checks["valid_appealDate"] = """
    (
        DecisionDate IS NULL
        OR
        to_date(DecisionDate, 'dd/MM/yyyy') = to_date(appealDate, 'dd/MM/yyyy')
    )
    """


    checks["valid_anonymityOrder"] = ( "(anonymityOrder = 'No')")


    
    checks["valid_appealDecision"] = """
    (
        CASE
            WHEN Outcome_SD = 1 THEN (appealDecision = 'Allowed')
            WHEN Outcome_SD = 2 THEN (appealDecision = 'Dismissed')
            ELSE (appealDecision IS NULL)
        END
    )
    """

   
   
    checks["valid_isDecisionAllowed"] = """
    (
        CASE
            WHEN Outcome_SD = 1 THEN (isDecisionAllowed = 'Allowed')
            WHEN Outcome_SD = 2 THEN (isDecisionAllowed = 'Dismissed')
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
            WHEN CategoryId = 37 THEN date_add(DecisionDate, 14) = to_date(ftpaApplicationDeadline, 'dd/MM/yyyy')
            WHEN CategoryId = 38 THEN date_add(DecisionDate, 28) = to_date(ftpaApplicationDeadline, 'dd/MM/yyyy')
            ELSE DecisionDate = to_date(ftpaApplicationDeadline, 'dd/MM/yyyy')
        END
    )
    """


    return checks


if __name__ == "__main__":
    pass