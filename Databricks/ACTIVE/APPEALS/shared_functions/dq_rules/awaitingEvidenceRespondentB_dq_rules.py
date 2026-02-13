from Databricks.ACTIVE.APPEALS.shared_functions.dq_rules.dq_rules import DQRulesBase


class awaitingEvidenceRespondentBDQRules(DQRulesBase):

    def add_checks(self, checks={}):
        checks = self.add_base_checks(checks)

        return checks

    def add_base_checks(self, checks={}):

        checks["valid_uploadHomeOfficeBundleActionAvailable"] = "(uploadHomeOfficeBundleActionAvailable <=> 'No')"

        checks["valid_respondentDocuments_not_null"] = "(respondentDocuments IS NOT NULL)"

        return checks
