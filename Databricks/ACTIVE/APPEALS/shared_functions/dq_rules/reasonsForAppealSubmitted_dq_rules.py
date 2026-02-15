from .dq_rules import DQRulesBase


class reasonsForAppealSubmittedDQRules(DQRulesBase):

    def add_checks(self, checks={}):
        checks = self.add_base_checks(checks)

        return checks

    def add_base_checks(self, checks={}):

        checks["valid_additionalInstructionsTribunalResponse"] = (
            """
                (
                    (
                        (hr_CaseStatus <=> 26)
                        AND
                        (dv_representation <=> 'AIP')
                        AND
                        (additionalInstructionsTribunalResponse IS NOT NULL)
                    )
                    OR
                    (
                        (
                            (NOT(hr_CaseStatus <=> 26)
                            OR
                            (NOT(dv_representation <=> 'AIP'))
                        )
                        AND
                        (additionalInstructionsTribunalResponse IS NULL)
                    )
                )
            """
        )

        return checks
