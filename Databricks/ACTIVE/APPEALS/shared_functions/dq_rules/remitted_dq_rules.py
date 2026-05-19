from .dq_rules import DQRulesBase


class remittedDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_remitted()
        checks = checks | self.get_checks_document()
        checks = checks | self.get_checks_general_default()

        return checks

    def get_checks_remitted(self, checks={}):

        checks["valid_rehearingReason"] = ("""
        (
            rehearingReason = "Remitted" AND rehearingReason IS NOT NULL AND rehearingReason != ""
        )
        """)

        checks["valid_sourceOfRemittal"] = ("""
        (
            sourceOfRemittal = "Upper Tribunal" AND sourceOfRemittal IS NOT NULL AND sourceOfRemittal != ""
        )
        """)

        checks["valid_courtReferenceNumber"] = ("""
        (
            courtReferenceNumber = "This is a migrated ARIA case. Please refer to the documents."
        )
        """)

        checks["valid_appealRemittedDate"] = (
            """
            (
                (CaseStatus_rem IN (42, 43, 44) AND Outcome_rem = 86)
                AND
                appealRemittedDate <=> date_format(CAST(DecisionDate_rem AS timestamp), 'yyyy-MM-dd')
            )
            """)


        return checks

    def get_checks_document(self, checks={}):
        
        checks["valid_remittalDocuments"] = ("(size(remittalDocuments) = 0) ")
        checks["valid_uploadOtherRemittalDocs"] = ("(size(uploadOtherRemittalDocs) = 0) ")
        return checks


    def get_checks_general_default(self, checks={}):

        checks["valid_caseFlagSetAsideReheardExists"] = (
            """
                (caseFlagSetAsideReheardExists = 'Yes') 
                AND caseFlagSetAsideReheardExists IS NOT NULL
                AND caseFlagSetAsideReheardExists != ""
            """)

        return checks

