from .dq_rules import DQRulesBase


class reasonsForAppealSubmittedDQRules(DQRulesBase):

    def add_checks(self, checks={}):
        checks = self.add_base_checks(checks)

        return checks

    def add_base_checks(self, checks={}):

        checks["valid_additionalInstructionsTribunalResponse"] = (
            "((additionalInstructionsTribunalResponse IS NOT NULL and CaseStatus = '26') OR (additionalInstructionsTribunalResponse IS NULL and CaseStatus != '26'))"
        )

        checks["valid_additionalInstructionsTribunalResponse"] = (
            """
            (
                (
                (StatusId <=> max_StatusId AND
                    (
                    CaseStatus IN (37,38) OR
                    (CaseStatus <=> 26 AND Outcome <=> 0)
                    )
                )
                AND
                (dv_representation <=> 'LR')
                AND
                (additionalInstructionsTribunalResponse IS NOT NULL)
                )
                OR
                (
                (
                    NOT(StatusId <=> max_StatusId)
                    OR 
                    CaseStatus NOT IN (37,38)
                    OR 
                    NOT(CaseStatus <=> 26 AND Outcome <=> 0)
                    OR
                    NOT(dv_representation <=> 'LR')
                )
                AND
                (additionalInstructionsTribunalResponse IS NULL)
                )
            )
            """
        )

        return checks
