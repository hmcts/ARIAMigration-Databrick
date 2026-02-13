from .dq_rules import DQRulesBase


class caseUnderReviewDQRules(DQRulesBase):

    def add_checks(self, checks={}):
        checks = self.add_base_checks(checks)

        return checks

    def add_base_checks(self, checks={}):

        # The rule checks: if StatusId is the max for the CaseNo and ((CaseStatus in (37,38)) or (CaseStatus == 26 and Outcome == 0)), then additionalInstructionsTribunalResponse must not be null
        checks["valid_additionalInstructionsTribunalResponse"] = """
            (
            (StatusId = max_StatusId AND
                (
                CaseStatus IN (37,38) OR
                (CaseStatus = 26 AND Outcome = 0)
                )
            )
            AND
            (additionalInstructionsTribunalResponse IS NOT NULL)
            AND
            (dv_representation = 'LR')
            )
        """

        return checks
