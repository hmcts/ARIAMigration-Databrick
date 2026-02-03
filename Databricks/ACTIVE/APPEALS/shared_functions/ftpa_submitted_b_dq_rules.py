def add_checks(checks={}):
    checks = add_checks_ftpa(checks)
    
    return checks



def add_checks_ftpa(checks={}):

    checks["valid_allocatedJudge"] = (
        """
        (   (allocatedJudge IS NULL AND Adj_Title IS NULL AND Adj_Forenames IS NULL AND Adj_Surname IS NULL)
            OR
            (allocatedJudge = concat(Adj_Title, ' ', Adj_Forenames, ' ', Adj_Surname))
        )
        """
    )

    checks["valid_allocatedJudgeEdit"] = (
        """
        (   (allocatedJudgeEdit IS NULL AND Adj_Title IS NULL AND Adj_Forenames IS NULL AND Adj_Surname IS NULL)
            OR
            (allocatedJudgeEdit = concat(Adj_Title, ' ', Adj_Forenames, ' ', Adj_Surname))
        )
        """
    )

    checks["valid_judgeAllocationExists"] = ( "(judgeAllocationExists = 'Yes')")

    return checks


if __name__ == "__main__":
    pass