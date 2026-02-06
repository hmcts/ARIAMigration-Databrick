def add_checks(checks={}):
   
    checks = add_checks_ftpa(checks)

    checks = add_checks_ftpa_decided(checks)

    return checks


def add_checks_ftpa(checks={}):

    checks["valid_allocatedJudge"] = (
        """
        (
            (allocatedJudge IS NULL AND Adj_Title IS NULL AND Adj_Forenames IS NULL AND Adj_Surname IS NULL)
            OR
            (allocatedJudge = concat(Adj_Title, ' ', Adj_Forenames, ' ', Adj_Surname))
        )
        """
    )

    checks["valid_allocatedJudgeEdit"] = (
        """
        (
            (allocatedJudgeEdit IS NULL AND Adj_Title IS NULL AND Adj_Forenames IS NULL AND Adj_Surname IS NULL)
            OR
            (allocatedJudgeEdit = concat(Adj_Title, ' ', Adj_Forenames, ' ', Adj_Surname))
        )
        """
    )

    checks["valid_judgeAllocationExists"] = ( "(judgeAllocationExists = 'Yes')" )

    return checks


def add_checks_ftpa_decided(checks={}):

    # ---------------------------------------------------------
    # Helper mapping (Outcome -> type)
    # 30 -> granted, 31 -> refused, 14 -> notAdmitted
    # ---------------------------------------------------------
    checks["valid_ftpaApplicantType"] = (
        """
        (
            (Party = 1 AND ftpaApplicantType = 'appellant')
            OR
            (Party = 2 AND ftpaApplicantType = 'respondent')
            OR
            (Party IS NULL AND ftpaApplicantType IS NULL)
            OR
            (Party NOT IN (1,2) AND ftpaApplicantType IS NULL)
        )
        """
    )

    checks["valid_ftpaFirstDecision"] = (
        """
        (
            (Outcome = 30 AND ftpaFirstDecision = 'granted')
            OR
            (Outcome = 31 AND ftpaFirstDecision = 'refused')
            OR
            (Outcome = 14 AND ftpaFirstDecision = 'notAdmitted')
            OR
            (Outcome IS NULL AND ftpaFirstDecision IS NULL)
            OR
            (Outcome NOT IN (30,31,14) AND ftpaFirstDecision IS NULL)
        )
        """
    )

    checks["valid_ftpaFinalDecisionForDisplay"] = (
        """
        (
            (Outcome = 30 AND ftpaFinalDecisionForDisplay = 'Granted')
            OR
            (Outcome = 31 AND ftpaFinalDecisionForDisplay = 'Refused')
            OR
            (Outcome = 14 AND ftpaFinalDecisionForDisplay = 'Not admitted')
            OR
            (Outcome IS NULL AND ftpaFinalDecisionForDisplay IS NULL)
            OR
            (Outcome NOT IN (30,31,14) AND ftpaFinalDecisionForDisplay IS NULL)
        )
        """
    )

    # Decision dates are dd/MM/yyyy and only populated for matching Party
    checks["valid_ftpaAppellantDecisionDate"] = (
        """
        (
            (Party = 1 AND ftpaAppellantDecisionDate = date_format(DecisionDate, 'dd/MM/yyyy'))
            OR
            (Party <> 1 AND ftpaAppellantDecisionDate IS NULL)
            OR
            (Party IS NULL AND ftpaAppellantDecisionDate IS NULL)
        )
        """
    )

    checks["valid_ftpaRespondentDecisionDate"] = (
        """
        (
            (Party = 2 AND ftpaRespondentDecisionDate = date_format(DecisionDate, 'dd/MM/yyyy'))
            OR
            (Party <> 2 AND ftpaRespondentDecisionDate IS NULL)
            OR
            (Party IS NULL AND ftpaRespondentDecisionDate IS NULL)
        )
        """
    )

    # RJ outcome type only populated for matching party 
    checks["valid_ftpaAppellantRjDecisionOutcomeType"] = (
        """
        (
            (Party = 1 AND (
                (Outcome = 30 AND ftpaAppellantRjDecisionOutcomeType = 'granted')
                OR (Outcome = 31 AND ftpaAppellantRjDecisionOutcomeType = 'refused')
                OR (Outcome = 14 AND ftpaAppellantRjDecisionOutcomeType = 'notAdmitted')
                OR (Outcome IS NULL AND ftpaAppellantRjDecisionOutcomeType IS NULL)
                OR (Outcome NOT IN (30,31,14) AND ftpaAppellantRjDecisionOutcomeType IS NULL)
            ))
            OR
            (Party <> 1 AND ftpaAppellantRjDecisionOutcomeType IS NULL)
            OR
            (Party IS NULL AND ftpaAppellantRjDecisionOutcomeType IS NULL)
        )
        """
    )

    checks["valid_ftpaRespondentRjDecisionOutcomeType"] = (
        """
        (
            (Party = 2 AND (
                (Outcome = 30 AND ftpaRespondentRjDecisionOutcomeType = 'granted')
                OR (Outcome = 31 AND ftpaRespondentRjDecisionOutcomeType = 'refused')
                OR (Outcome = 14 AND ftpaRespondentRjDecisionOutcomeType = 'notAdmitted')
                OR (Outcome IS NULL AND ftpaRespondentRjDecisionOutcomeType IS NULL)
                OR (Outcome NOT IN (30,31,14) AND ftpaRespondentRjDecisionOutcomeType IS NULL)
            ))
            OR
            (Party <> 2 AND ftpaRespondentRjDecisionOutcomeType IS NULL)
            OR
            (Party IS NULL AND ftpaRespondentRjDecisionOutcomeType IS NULL)
        )
        """
    )

    checks["valid_isFtpaAppellantNoticeOfDecisionSetAside"] = (
        """
        (
            (Party = 1 AND isFtpaAppellantNoticeOfDecisionSetAside = 'No')
            OR
            (Party <> 1 AND isFtpaAppellantNoticeOfDecisionSetAside IS NULL)
            OR
            (Party IS NULL AND isFtpaAppellantNoticeOfDecisionSetAside IS NULL)
        )
        """
    )

    checks["valid_isFtpaRespondentNoticeOfDecisionSetAside"] = (
        """
        (
            (Party = 2 AND isFtpaRespondentNoticeOfDecisionSetAside = 'No')
            OR
            (Party <> 2 AND isFtpaRespondentNoticeOfDecisionSetAside IS NULL)
            OR
            (Party IS NULL AND isFtpaRespondentNoticeOfDecisionSetAside IS NULL)
        )
        """
    )
    
    checks["valid_judgeAllocationExists"] = ( "(judgeAllocationExists = 'Yes')")


    return checks


if __name__ == "__main__":
    pass
