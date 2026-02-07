def add_checks(checks={}):
    checks = add_checks_ended(checks)
    
    return checks


def add_checks_ended(checks={}):

    checks["valid_endAppealOutcome"] = """(
        endAppealOutcome = endAppealOutcome_end
        OR
        (endAppealOutcome IS NULL AND endAppealOutcome_end IS NULL)
    )"""

    checks["valid_endAppealOutcomeReason"] = """(
        endAppealOutcomeReason = endAppealOutcomeReason_end
        OR
        (endAppealOutcomeReason IS NULL AND endAppealOutcomeReason_end IS NULL)
    )"""

    checks["valid_endAppealApproverType"] = """(
        CASE 
            WHEN CaseStatus_end = 46 THEN endAppealApproverType = 'Judge'
            ELSE endAppealApproverType = 'Case Worker'
        END
    )"""

    checks["valid_endAppealApproverName"] = """(
        CASE 
            WHEN CaseStatus_end = 46 THEN endAppealApproverName = concat(Adj_Determination_Title_end, ' ', Adj_Determination_Forenames_end, ' ', Adj_Determination_Surname_end)
            ELSE endAppealApproverName = 'This is a migrated ARIA case'
        END
    )"""

    checks["valid_endAppealDate"] = """(
        endAppealDate = decision_ts
    )"""

    checks["valid_stateBeforeEndAppeal"] = """(
        stateBeforeEndAppeal = stateBeforeEndAppeal_end
        OR
        (stateBeforeEndAppeal IS NULL AND stateBeforeEndAppeal_end IS NULL)
    )"""

    return checks



if __name__ == "__main__":
    pass