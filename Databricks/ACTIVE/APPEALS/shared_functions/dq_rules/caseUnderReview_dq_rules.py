from .dq_rules import DQRulesBase


class caseUnderReviewDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_base_checks()

        return checks

    def get_base_checks(self, checks={}):
        # The rule checks: if StatusId is the max for the CaseNo and CaseStatus is 26, then additionalInstructionsTribunalResponse must not be null
        checks["valid_additionalInstructionsTribunalResponse"] = """
            (
                (
                    (hr_CaseStatus <=> 26)
                    AND
                    (dv_representation <=> 'LR')
                    AND
                    (additionalInstructionsTribunalResponse IS NOT NULL)
                )
                OR
                (
                    (
                        (NOT(hr_CaseStatus <=> 26)
                        OR
                        (NOT(dv_representation <=> 'LR'))
                    )
                    AND
                    (additionalInstructionsTribunalResponse IS NULL)
                )
            )
        """

        return checks
