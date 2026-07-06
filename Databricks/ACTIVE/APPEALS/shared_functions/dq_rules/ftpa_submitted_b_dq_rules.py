from .dq_rules import DQRulesBase


class ftpaSubmittedBDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_ftpa()

        return checks

    def get_checks_ftpa(self, checks={}):

        checks["valid_allocatedJudge"] = (
            """
            (
                (
                    dq_cs39_status = 39 AND 
                    (allocatedJudge <=> concat(Adj_Title, ' ', Adj_Forenames, ' ', Adj_Surname))
                )
                OR
                (
                    (dq_cs39_status <> 39 OR dq_cs39_status IS NULL) AND allocatedJudge IS NULL
                )
            )
            """
        )

        checks["valid_allocatedJudgeEdit"] = (
            """
            (
                (
                    dq_cs39_status = 39 AND 
                    (allocatedJudgeEdit <=> concat(Adj_Title, ' ', Adj_Forenames, ' ', Adj_Surname))
                )
                OR
                (
                    (dq_cs39_status <> 39 OR dq_cs39_status IS NULL) AND allocatedJudgeEdit IS NULL
                )
            )
                """
        )

        checks["valid_judgeAllocationExists"] = ("(judgeAllocationExists = 'Yes')")

        return checks
