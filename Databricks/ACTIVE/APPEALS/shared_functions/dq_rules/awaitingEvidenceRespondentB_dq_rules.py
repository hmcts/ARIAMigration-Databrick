from .dq_rules import DQRulesBase


class awaitingEvidenceRespondentBDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_base_checks()

        return checks

    def get_base_checks(self, checks={}):
        checks["valid_uploadHomeOfficeBundleActionAvailable"] = "(uploadHomeOfficeBundleActionAvailable <=> 'No')"

        checks["valid_respondentDocuments_not_null"] = "(respondentDocuments IS NOT NULL)"

        return checks
