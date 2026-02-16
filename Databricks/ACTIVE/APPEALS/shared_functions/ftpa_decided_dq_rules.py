# ============================================================
# Databricks.ACTIVE.APPEALS.shared_functions.ftpa_decided_dq_rules
# FTPA Decided - DQ Rules
#   - Decision/outcome fields: CaseStatus=39 AND Outcome IN (30,31,14)
#   - Set-aside flags: CaseStatus=39 (no outcome filter)
#   - Decision dates in ISO 8601 date format: yyyy-MM-dd
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
    # ---------------------------------------------------------
    # Helpers:
    # - Decision/outcome fields only apply when:
    #     CaseStatus = 39 AND Outcome IN (30,31,14)
    # - DecisionDate output format: yyyy-MM-dd (ISO date)
    # - Safe timestamp parsing for DecisionDate (string or timestamp)
    # ---------------------------------------------------------

    # Applicant type (only when cs39 + outcome in scope)
    checks["valid_ftpaApplicantType"] = (
        """
        (
            (
                CaseStatus = 39 AND Outcome IN (30,31,14)
                AND (
                    (Party = 1 AND ftpaApplicantType = 'appellant')
                    OR (Party = 2 AND ftpaApplicantType = 'respondent')
                    OR (Party NOT IN (1,2) AND ftpaApplicantType IS NULL)
                    OR (Party IS NULL AND ftpaApplicantType IS NULL)
                )
            )
            OR
            (
                NOT (CaseStatus = 39 AND Outcome IN (30,31,14))
                AND ftpaApplicantType IS NULL
            )
        )
        """
    )

    # First decision (only when cs39 + outcome in scope)
    checks["valid_ftpaFirstDecision"] = (
        """
        (
            (
                CaseStatus = 39 AND Outcome IN (30,31,14)
                AND (
                    (Outcome = 30 AND ftpaFirstDecision = 'granted')
                    OR (Outcome = 31 AND ftpaFirstDecision = 'refused')
                    OR (Outcome = 14 AND ftpaFirstDecision = 'notAdmitted')
                )
            )
            OR
            (
                NOT (CaseStatus = 39 AND Outcome IN (30,31,14))
                AND ftpaFirstDecision IS NULL
            )
        )
        """
    )

    # Final decision for display (only when cs39 + outcome in scope)
    checks["valid_ftpaFinalDecisionForDisplay"] = (
        """
        (
            (
                CaseStatus = 39 AND Outcome IN (30,31,14)
                AND (
                    (Outcome = 30 AND ftpaFinalDecisionForDisplay = 'Granted')
                    OR (Outcome = 31 AND ftpaFinalDecisionForDisplay = 'Refused')
                    OR (Outcome = 14 AND ftpaFinalDecisionForDisplay = 'Not admitted')
                )
            )
            OR
            (
                NOT (CaseStatus = 39 AND Outcome IN (30,31,14))
                AND ftpaFinalDecisionForDisplay IS NULL
            )
        )
        """
    )

    # DecisionDate derivation (ISO yyyy-MM-dd) only when cs39 + outcome in scope and Party matches
    # NOTE: Use safe parsing:
    #   coalesce(to_timestamp(pattern1), to_timestamp(pattern2), cast(timestamp))
    checks["valid_ftpaAppellantDecisionDate"] = (
        """
        (
            (
                CaseStatus = 39 AND Outcome IN (30,31,14)
                AND Party = 1
                AND ftpaAppellantDecisionDate = date_format(
                    coalesce(
                        to_timestamp(DecisionDate, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'),
                        to_timestamp(DecisionDate, 'yyyy-MM-dd''T''HH:mm:ss.SSSX'),
                        cast(DecisionDate as timestamp)
                    ),
                    'yyyy-MM-dd'
                )
            )
            OR
            (
                -- If not Party=1 or not in scope => must be NULL
                (Party <> 1 OR NOT (CaseStatus = 39 AND Outcome IN (30,31,14)) OR Party IS NULL)
                AND ftpaAppellantDecisionDate IS NULL
            )
        )
        """
    )

    checks["valid_ftpaRespondentDecisionDate"] = (
        """
        (
            (
                CaseStatus = 39 AND Outcome IN (30,31,14)
                AND Party = 2
                AND ftpaRespondentDecisionDate = date_format(
                    coalesce(
                        to_timestamp(DecisionDate, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'),
                        to_timestamp(DecisionDate, 'yyyy-MM-dd''T''HH:mm:ss.SSSX'),
                        cast(DecisionDate as timestamp)
                    ),
                    'yyyy-MM-dd'
                )
            )
            OR
            (
                (Party <> 2 OR NOT (CaseStatus = 39 AND Outcome IN (30,31,14)) OR Party IS NULL)
                AND ftpaRespondentDecisionDate IS NULL
            )
        )
        """
    )

    # RJ outcome types (only when cs39 + outcome in scope AND Party matches)
    checks["valid_ftpaAppellantRjDecisionOutcomeType"] = (
        """
        (
            (
                CaseStatus = 39 AND Outcome IN (30,31,14)
                AND Party = 1
                AND (
                    (Outcome = 30 AND ftpaAppellantRjDecisionOutcomeType = 'granted')
                    OR (Outcome = 31 AND ftpaAppellantRjDecisionOutcomeType = 'refused')
                    OR (Outcome = 14 AND ftpaAppellantRjDecisionOutcomeType = 'notAdmitted')
                )
            )
            OR
            (
                (Party <> 1 OR NOT (CaseStatus = 39 AND Outcome IN (30,31,14)) OR Party IS NULL)
                AND ftpaAppellantRjDecisionOutcomeType IS NULL
            )
        )
        """
    )

    checks["valid_ftpaRespondentRjDecisionOutcomeType"] = (
        """
        (
            (
                CaseStatus = 39 AND Outcome IN (30,31,14)
                AND Party = 2
                AND (
                    (Outcome = 30 AND ftpaRespondentRjDecisionOutcomeType = 'granted')
                    OR (Outcome = 31 AND ftpaRespondentRjDecisionOutcomeType = 'refused')
                    OR (Outcome = 14 AND ftpaRespondentRjDecisionOutcomeType = 'notAdmitted')
                )
            )
            OR
            (
                (Party <> 2 OR NOT (CaseStatus = 39 AND Outcome IN (30,31,14)) OR Party IS NULL)
                AND ftpaRespondentRjDecisionOutcomeType IS NULL
            )
        )
        """
    )

    # Set-aside flags (CaseStatus=39 only; NO outcome filter)
    checks["valid_isFtpaAppellantNoticeOfDecisionSetAside"] = (
        """
        (
            (
                CaseStatus = 39
                AND Party = 1
                AND isFtpaAppellantNoticeOfDecisionSetAside = 'No'
            )
            OR
            (
                (Party <> 1 OR CaseStatus <> 39 OR Party IS NULL OR CaseStatus IS NULL)
                AND isFtpaAppellantNoticeOfDecisionSetAside IS NULL
            )
        )
        """
    )

    checks["valid_isFtpaRespondentNoticeOfDecisionSetAside"] = (
        """
        (
            (
                CaseStatus = 39
                AND Party = 2
                AND isFtpaRespondentNoticeOfDecisionSetAside = 'No'
            )
            OR
            (
                (Party <> 2 OR CaseStatus <> 39 OR Party IS NULL OR CaseStatus IS NULL)
                AND isFtpaRespondentNoticeOfDecisionSetAside IS NULL
            )
        )
        """
    )

    return checks


if __name__ == "__main__":
    pass
