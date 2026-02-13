from Databricks.ACTIVE.APPEALS.shared_functions.dq_rules.dq_rules import DQRulesBase


class awaitingEvidenceRespondentADQRules(DQRulesBase):

    def add_checks(self, checks={}):
        checks = self.add_base_checks(checks)

        return checks

    def add_base_checks(self, checks={}):

        checks["valid_appellantFullName_not_null"] = "(appellantFullName IS NOT NULL)"

        checks["valid_directions_not_null"] = "(directions IS NOT NULL)"

        checks["valid_uploadHomeOfficeBundleAvailable"] = "(uploadHomeOfficeBundleAvailable <=> 'Yes')"

        return checks
