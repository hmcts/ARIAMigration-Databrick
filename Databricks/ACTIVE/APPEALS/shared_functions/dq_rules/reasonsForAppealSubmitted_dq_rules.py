from .dq_rules import DQRulesBase


class reasonsForAppealSubmittedDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_base_checks()

        return checks

    def get_base_checks(self, checks={}):

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
                            (NOT(hr_CaseStatus <=> 26))
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
