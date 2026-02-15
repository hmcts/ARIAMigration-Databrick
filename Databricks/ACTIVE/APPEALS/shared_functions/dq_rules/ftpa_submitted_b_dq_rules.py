from .dq_rules import DQRulesBase


class ftpaSubmittedBDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_ftpa()

        return checks

    def get_checks_ftpa(self, checks={}):

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

        checks["valid_judgeAllocationExists"] = ("(judgeAllocationExists = 'Yes')")

        return checks
