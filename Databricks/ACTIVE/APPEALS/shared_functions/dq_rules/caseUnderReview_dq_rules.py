from .dq_rules import DQRulesBase


class caseUnderReviewDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_base_checks()
        checks = checks | self.get_checks_case_state()

        return checks

    def get_checks_case_state(self, checks={}):

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
                    AND
                    (LENGTH(additionalInstructionsTribunalResponse) <= 2000)
                )
                OR
                (
                    (
                        (NOT(hr_CaseStatus <=> 26))
                        OR
                        (NOT(dv_representation <=> 'LR'))
                    )
                    AND
                    (additionalInstructionsTribunalResponse IS NULL)
                )
            )
        """

        checks["valid_markEvidenceAsReviewedActionAvailable"] = "(markEvidenceAsReviewedActionAvailable <=> 'Yes')"

        checks["valid_uploadAdditionalEvidenceActionAvailable"] = "(uploadAdditionalEvidenceActionAvailable <=> 'Yes')"

        checks["valid_uploadAdditionalEvidenceHomeOfficeActionAvailable"] = "(uploadAdditionalEvidenceHomeOfficeActionAvailable <=> 'Yes')"

        checks["valid_caseArgumentAvailable"] = (
            """(
                (dv_representation <=> 'LR' AND caseArgumentAvailable <=> 'Yes')
                OR
                (NOT(dv_representation <=> 'LR') AND caseArgumentAvailable IS NULL)
            )"""
        )

        return checks
