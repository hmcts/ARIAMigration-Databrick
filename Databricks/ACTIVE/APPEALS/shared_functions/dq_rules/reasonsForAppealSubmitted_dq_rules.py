from .dq_rules import DQRulesBase


class reasonsForAppealSubmittedDQRules(DQRulesBase):

    def add_checks(self, checks={}):
        checks = self.add_base_checks(checks)

        return checks

    def add_base_checks(self, checks={}):

        checks["valid_additionalInstructionsTribunalResponse"] = (
            "((additionalInstructionsTribunalResponse IS NOT NULL and CaseStatus = '26') OR (additionalInstructionsTribunalResponse IS NULL and CaseStatus != '26'))"
        )

        return checks
