from .dq_rules import DQRulesBase


class awaitingEvidenceRespondentADQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_base_checks()
        checks = checks | self.get_checks_case_state()

        return checks

    def get_checks_case_state(self, checks={}):

        return checks

    def get_base_checks(self, checks={}):
        checks["valid_appellantFullName_not_null"] = "(appellantFullName IS NOT NULL)"

        checks["valid_directions"] = "(directions IS NOT NULL)"

        checks["valid_uploadHomeOfficeBundleAvailable"] = "(uploadHomeOfficeBundleAvailable <=> 'Yes')"

        checks["valid_changeDirectionDueDateActionAvailable"] = "(changeDirectionDueDateActionAvailable <=> 'Yes')"

        return checks
