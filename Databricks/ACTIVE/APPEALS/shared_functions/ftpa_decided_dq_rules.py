# ============================================================
# Databricks.ACTIVE.APPEALS.shared_functions.ftpa_decided_dq_rules
# - ftpa_decided shared function now outputs source columns as:
#     ftpa_src_CaseStatus, ftpa_src_Outcome, ftpa_src_Party, ftpa_src_DecisionDate
# - Update DQ rules to use these new names to avoid ambiguity with
#   validation joins that also add CaseStatus/Outcome/Party/DecisionDate.
# - Keeps existing check names (so nothing else breaks)
# ============================================================

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

    checks["valid_judgeAllocationExists"] = ("(judgeAllocationExists = 'Yes')")

    return checks


def add_checks_ftpa_decided(checks={}):

    # decisiondate_iso = "date_format(ftpa_src_DecisionDate, 'yyyy-MM-dd')"

    # ---------------------------------------------------------
    # Applicant type
    # use ftpa_src_Party (renamed source field)
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

    # ---------------------------------------------------------
    # First decision (Outcome -> type)
    # 30 -> granted, 31 -> refused, 14 -> notAdmitted
    # use ftpa_src_Outcome (renamed source field)
    # ---------------------------------------------------------
    checks["valid_ftpaFirstDecision"] = (
        """
        (
            (FtpaDecOutcome = 30 AND ftpaFirstDecision = 'granted')
            OR
            (FtpaDecOutcome = 31 AND ftpaFirstDecision = 'refused')
            OR
            (FtpaDecOutcome = 14 AND ftpaFirstDecision = 'notAdmitted')
            OR
            (FtpaDecOutcome IS NULL AND ftpaFirstDecision IS NULL)
            OR
            (FtpaDecOutcome NOT IN (30,31,14) AND ftpaFirstDecision IS NULL)
        )
        """
    )

    checks["valid_ftpaFinalDecisionForDisplay"] = (
        """
        (
            (FtpaDecOutcome = 30 AND ftpaFinalDecisionForDisplay = 'Granted')
            OR
            (FtpaDecOutcome = 31 AND ftpaFinalDecisionForDisplay = 'Refused')
            OR
            (FtpaDecOutcome = 14 AND ftpaFinalDecisionForDisplay = 'Not admitted')
            OR
            (FtpaDecOutcome IS NULL AND ftpaFinalDecisionForDisplay IS NULL)
            OR
            (FtpaDecOutcome NOT IN (30,31,14) AND ftpaFinalDecisionForDisplay IS NULL)
        )
        """
    )

    # ---------------------------------------------------------
    # Decision dates are ISO yyyy-MM-dd (not dd/MM/yyyy)
    # Only populated for matching Party
    # use Party and ftpa_src_DecisionDate
    # ---------------------------------------------------------
    checks["valid_ftpaAppellantDecisionDate"] = (
        f"""
        (
            (Party = 1 AND ftpaAppellantDecisionDate IS NOT NULL)
            OR
            (Party <> 1 AND ftpaAppellantDecisionDate IS NULL)
            OR
            (Party IS NULL AND ftpaAppellantDecisionDate IS NULL)
        )
        """
    )

    checks["valid_ftpaRespondentDecisionDate"] = (
        f"""
        (
            (Party = 2 AND ftpaRespondentDecisionDate IS NOT NULL)
            OR
            (Party <> 2 AND ftpaRespondentDecisionDate IS NULL)
            OR
            (Party IS NULL AND ftpaRespondentDecisionDate IS NULL)
        )
        """
    )

    # ---------------------------------------------------------
    # RJ outcome types only populated for matching Party
    # use Party and Outcome
    # ---------------------------------------------------------
    checks["valid_ftpaAppellantRjDecisionOutcomeType"] = (
        """
        (
            (Party = 1 AND (
                (FtpaDecOutcome = 30 AND ftpaAppellantRjDecisionOutcomeType = 'granted')
                OR (FtpaDecOutcome = 31 AND ftpaAppellantRjDecisionOutcomeType = 'refused')
                OR (FtpaDecOutcome = 14 AND ftpaAppellantRjDecisionOutcomeType = 'notAdmitted')
                OR (FtpaDecOutcome IS NULL AND ftpaAppellantRjDecisionOutcomeType IS NULL)
                OR (FtpaDecOutcome NOT IN (30,31,14) AND ftpaAppellantRjDecisionOutcomeType IS NULL)
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
                (FtpaDecOutcome = 30 AND ftpaRespondentRjDecisionOutcomeType = 'granted')
                OR (FtpaDecOutcome = 31 AND ftpaRespondentRjDecisionOutcomeType = 'refused')
                OR (FtpaDecOutcome = 14 AND ftpaRespondentRjDecisionOutcomeType = 'notAdmitted')
                OR (FtpaDecOutcome IS NULL AND ftpaRespondentRjDecisionOutcomeType IS NULL)
                OR (FtpaDecOutcome NOT IN (30,31,14) AND ftpaRespondentRjDecisionOutcomeType IS NULL)
            ))
            OR
            (Party <> 2 AND ftpaRespondentRjDecisionOutcomeType IS NULL)
            OR
            (Party IS NULL AND ftpaRespondentRjDecisionOutcomeType IS NULL)
        )
        """
    )

    # ---------------------------------------------------------
    # Set-aside flags (Party-driven)
    # use Party
    # ---------------------------------------------------------
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

    return checks


if __name__ == "__main__":
    pass
