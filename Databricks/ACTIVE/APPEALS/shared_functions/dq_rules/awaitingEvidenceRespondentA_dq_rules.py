from .dq_rules import DQRulesBase


class awaitingEvidenceRespondentADQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_base_checks()

        return checks

    def get_base_checks(self, checks={}):
        checks["valid_appellantFullName_not_null"] = "(appellantFullName IS NOT NULL)"

        checks["valid_directions_not_null"] = "(directions IS NOT NULL)"

        checks["valid_uploadHomeOfficeBundleAvailable"] = "(uploadHomeOfficeBundleAvailable <=> 'Yes')"

        return checks
