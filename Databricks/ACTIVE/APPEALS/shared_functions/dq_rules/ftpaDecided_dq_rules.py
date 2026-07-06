from .dq_rules import DQRulesBase

class ftpaDecidedDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_ftpaDecided()
        checks = checks | self.get_checks_ftpaGeneral()
        checks = checks | self.get_checks_ftpaDocuments()

        return checks

    def get_checks_ftpaDecided(self, checks={}):

        checks["valid_ftpaApplicantType"] = (
        """
        (
            (
                (cs_39_outcome_14_30_31 = 39 AND outcome_14_30_31_cs_39 IN (30,31,14))
                AND
                (
                    (cs39_party_14_30_31 = 1 AND ftpaApplicantType = 'appellant')
                    OR
                    (cs39_party_14_30_31 = 2 AND ftpaApplicantType = 'respondent')
                    OR
                    (cs39_party_14_30_31 NOT IN (1,2) AND ftpaApplicantType IS NULL)
                    OR
                    (cs39_party_14_30_31 IS NULL AND ftpaApplicantType IS NULL)
                )
            )
            OR
            (
                NOT (cs_39_outcome_14_30_31 = 39 OR outcome_14_30_31_cs_39 IN (30,31,14))
                AND ftpaApplicantType IS NULL
            )
            OR
            (
                (cs_39_outcome_14_30_31 IS NULL AND ftpaApplicantType IS NULL)
            )
        )
        """
        )

        checks["valid_ftpaFirstDecision"] = (
        """
        (
            (
                (cs_39_outcome_14_30_31 = 39 AND outcome_14_30_31_cs_39 IN (30,31,14))
                AND
                (
                    (outcome_14_30_31_cs_39 = 30 AND ftpaFirstDecision = 'granted')
                    OR
                    (outcome_14_30_31_cs_39 = 31 AND ftpaFirstDecision = 'refused')
                    OR
                    (outcome_14_30_31_cs_39 = 14 AND ftpaFirstDecision = 'notAdmitted')
                    OR
                    (outcome_14_30_31_cs_39 IS NULL AND ftpaFirstDecision IS NULL)
                    OR
                    (outcome_14_30_31_cs_39 NOT IN (30,31,14) AND ftpaFirstDecision IS NULL)
                )
            )
            OR
            (
                NOT (cs_39_outcome_14_30_31 = 39 OR outcome_14_30_31_cs_39 IN (30,31,14))
                AND ftpaFirstDecision IS NULL
            )
            OR
            (
                (cs_39_outcome_14_30_31 IS NULL AND ftpaFirstDecision IS NULL)
            )
        )
        """
        )

        checks["valid_ftpaFinalDecisionForDisplay"] = (
            """
            (
                (
                    (
                        (cs_39_46_outcome_30_31_14 = 39 AND outcome_14_30_31_cs_39_46 IN (30,31,14))
                        OR
                        (cs_39_46_outcome_30_31_14 = 46 AND outcome_14_30_31_cs_39_46 = 31)
                    )
                    AND
                    (
                        (outcome_14_30_31_cs_39_46 = 30 AND ftpaFinalDecisionForDisplay = 'granted')
                        OR
                        (outcome_14_30_31_cs_39_46 = 31 AND ftpaFinalDecisionForDisplay = 'refused')
                        OR
                        (outcome_14_30_31_cs_39_46 = 14 AND ftpaFinalDecisionForDisplay = 'notAdmitted')
                    )
                )
                OR
                (
                    NOT (
                        (cs_39_46_outcome_30_31_14 = 39 OR outcome_14_30_31_cs_39_46 IN (30,31,14))
                        OR
                        (cs_39_46_outcome_30_31_14 = 46 OR outcome_14_30_31_cs_39_46 = 31)
                    )
                    AND ftpaFinalDecisionForDisplay IS NULL
                )
                OR
                (
                (cs_39_46_outcome_30_31_14 IS NULL AND ftpaFinalDecisionForDisplay IS NULL)
                )
            )
            """
        )

        checks["valid_ftpaAppellantDecisionDate"] = (
        """
        (
            (
                (cs_39_outcome_14_30_31 = 39 AND outcome_14_30_31_cs_39 IN (30,31,14))
                AND
                (
                    (cs39_party_14_30_31 = 1 AND ftpaAppellantDecisionDate IS NOT NULL)
                    OR
                    (cs39_party_14_30_31 <> 1 AND ftpaAppellantDecisionDate IS NULL)
                    OR
                    (cs39_party_14_30_31 IS NULL AND ftpaAppellantDecisionDate IS NULL)
                )
            )
            OR
            (
                NOT (cs_39_outcome_14_30_31 = 39 OR outcome_14_30_31_cs_39 IN (30,31,14))
                AND ftpaAppellantDecisionDate IS NULL
            )
            OR
            (
                (cs_39_outcome_14_30_31 IS NULL AND ftpaAppellantDecisionDate IS NULL)
            )
        )
        """
        )

        checks["valid_ftpaRespondentDecisionDate"] = (
        """
        (
            (
                (cs_39_outcome_14_30_31 = 39 AND outcome_14_30_31_cs_39 IN (30,31,14))
                AND
                (
                    (cs39_party_14_30_31 = 2 AND ftpaRespondentDecisionDate IS NOT NULL)
                    OR
                    (cs39_party_14_30_31 <> 2 AND ftpaRespondentDecisionDate IS NULL)
                    OR
                    (cs39_party_14_30_31 IS NULL AND ftpaRespondentDecisionDate IS NULL)
                )
            )
            OR
            (
                NOT (cs_39_outcome_14_30_31 = 39 OR outcome_14_30_31_cs_39 IN (30,31,14))
                AND ftpaRespondentDecisionDate IS NULL
            )
            OR
            (
                (cs_39_outcome_14_30_31 IS NULL AND ftpaRespondentDecisionDate IS NULL)
            )
        )
        """
        )

        checks["valid_ftpaAppellantRjDecisionOutcomeType"] = (
            """
            (
                (
                    (
                        (cs_39_outcome_14_30_31 = 39 AND outcome_14_30_31_cs_39 IN (30,31,14))
                    )
                    AND
                    (
                        (outcome_14_30_31_cs_39 = 30 AND cs39_party_14_30_31 = 1 AND ftpaAppellantRjDecisionOutcomeType = 'granted')
                        OR
                        (outcome_14_30_31_cs_39 = 31 AND cs39_party_14_30_31 = 1 AND ftpaAppellantRjDecisionOutcomeType = 'refused')
                        OR
                        (outcome_14_30_31_cs_39 = 14 AND cs39_party_14_30_31 = 1 AND ftpaAppellantRjDecisionOutcomeType = 'notAdmitted')
                    )
                )
                OR
                (
                    NOT (
                        (cs_39_outcome_14_30_31 = 39 AND outcome_14_30_31_cs_39 IN (30,31,14) AND cs39_party_14_30_31 = 1)
                    )
                    AND ftpaAppellantRjDecisionOutcomeType IS NULL
                )
                OR
                (
                    (cs_39_outcome_14_30_31 IS NULL AND ftpaAppellantRjDecisionOutcomeType IS NULL)
                )
            )
            """
        )

        checks["valid_ftpaRespondentRjDecisionOutcomeType"] = (
            """
            (
                (
                    (
                        (cs_39_outcome_14_30_31 = 39 AND outcome_14_30_31_cs_39 IN (30,31,14))
                    )
                    AND
                    (
                        (outcome_14_30_31_cs_39 = 30 AND cs39_party_14_30_31 = 2 AND ftpaRespondentRjDecisionOutcomeType = 'granted')
                        OR
                        (outcome_14_30_31_cs_39 = 31 AND cs39_party_14_30_31 = 2 AND ftpaRespondentRjDecisionOutcomeType = 'refused')
                        OR
                        (outcome_14_30_31_cs_39 = 14 AND cs39_party_14_30_31 = 2 AND ftpaRespondentRjDecisionOutcomeType = 'notAdmitted')
                    )
                )
                OR
                (
                    NOT (
                        (cs_39_outcome_14_30_31 = 39 AND outcome_14_30_31_cs_39 IN (30,31,14) AND cs39_party_14_30_31 = 2)
                    )
                    AND ftpaRespondentRjDecisionOutcomeType IS NULL
                )
                OR
                (
                    (cs_39_outcome_14_30_31 IS NULL AND ftpaRespondentRjDecisionOutcomeType IS NULL)
                )
            )
            """
        )

        checks["valid_isFtpaAppellantNoticeOfDecisionSetAside"] = (
        """
        (
            (
                (dq_cs39_status = 39)
                AND
                (
                    (dq_cs39_party = 1 AND isFtpaAppellantNoticeOfDecisionSetAside = "No")
                    OR
                    (dq_cs39_party <> 1 AND isFtpaAppellantNoticeOfDecisionSetAside IS NULL)
                    OR
                    (dq_cs39_party IS NULL AND isFtpaAppellantNoticeOfDecisionSetAside IS NULL)
                )
            )
            OR
            (
                NOT (dq_cs39_status = 39)
                AND isFtpaAppellantNoticeOfDecisionSetAside IS NULL
            )
            OR
            (
                dq_cs39_status IS NULL AND isFtpaAppellantNoticeOfDecisionSetAside IS NULL
            )
        )
        """
        )

        checks["valid_isFtpaRespondentNoticeOfDecisionSetAside"] = (
        """
        (
            (
                (dq_cs39_status = 39)
                AND
                (
                    (dq_cs39_party = 2 AND isFtpaRespondentNoticeOfDecisionSetAside = "No")
                    OR
                    (dq_cs39_party <> 2 AND isFtpaRespondentNoticeOfDecisionSetAside IS NULL)
                    OR
                    (dq_cs39_party IS NULL AND isFtpaRespondentNoticeOfDecisionSetAside IS NULL)
                )
            )
            OR
            (
                NOT (dq_cs39_status = 39)
                AND isFtpaRespondentNoticeOfDecisionSetAside IS NULL
            )
            OR
            (
                dq_cs39_status IS NULL AND isFtpaRespondentNoticeOfDecisionSetAside IS NULL
            )
        )
        """
        )

        return checks
    
    def get_checks_ftpaGeneral(self, checks={}):

        checks["valid_isAppellantFtpaDecisionvisibletoAll"] = ("""(
            (dq_cs39_status = 39 AND Party IN (0,1) AND isAppellantFtpaDecisionvisibletoAll = "Yes")
            OR
            (isAppellantFtpaDecisionvisibletoAll IS NULL)
        )""")

        checks["valid_isRespondentFtpaDecisionvisibletoAll"] = ("""(
            (dq_cs39_status = 39 AND Party = 2 AND isRespondentFtpaDecisionvisibletoAll = "Yes")
            OR
            (isRespondentFtpaDecisionvisibletoAll IS NULL)
        )""")

        checks["valid_isDlrmSetAsideEnabled"] = ("""(
            isDlrmSetAsideEnabled = "Yes"
        )""")

        checks["valid_isFtpaAppellantDecided"] = ("""(
                   
                (dq_cs39_status <=> 39 AND isFtpaAppellantDecided <=> "Yes")
                OR
                ((dq_cs39_status != 39 OR dq_cs39_status IS NULL) AND isFtpaAppellantDecided IS NULL)

               )
               """)

        checks["valid_isFtpaRespondentDecided"] = ("""
            (
                   
                (dq_cs39_status <=> 39 AND isFtpaRespondentDecided <=> "Yes")
                OR
                ((dq_cs39_status != 39 OR dq_cs39_status IS NULL) AND isFtpaRespondentDecided IS NULL)

               )
        """)

        checks["valid_isReheardAppealEnabled"] = ("""(
            isReheardAppealEnabled = "Yes"
        )""")

        checks["valid_secondFtpaDecisionExists"] = ("""(
            (
                secondFtpaDecisionExists =
                CASE
                    WHEN cs46_o31 = 46 AND o31_cs46 = 31 THEN 'Yes'
                    ELSE 'No'
                END
            ))
            OR
            (
            (cs46_o31 = 46 AND o31_cs46 = 31 AND secondFtpaDecisionExists = "Yes")
            OR
            (cs46_o31 != 46 OR o31_cs46 = 31 AND secondFtpaDecisionExists = "No")
            OR
            (cs46_o31 = 46 OR o31_cs46 != 31 AND secondFtpaDecisionExists = "No")
            OR
            (cs46_o31 != 46 OR o31_cs46 != 31 AND secondFtpaDecisionExists = "No")    
            )
            """)

        return checks

    def get_checks_ftpaDocuments(self, checks={}):

        checks["valid_allFtpaAppellantDecisionDocs"] = ("""(
            (dq_cs39_status = 39 AND Party = 1 AND size(allFtpaAppellantDecisionDocs) = 0)
            OR
            (allFtpaAppellantDecisionDocs IS NULL)
        )""")

        checks["valid_allFtpaRespondentDecisionDocs"] = ("""(
            (dq_cs39_status = 39 AND Party = 2 AND size(allFtpaRespondentDecisionDocs) = 0)
            OR
            (allFtpaRespondentDecisionDocs IS NULL)
        )""")

        checks["valid_ftpaAppellantNoticeDocument"] = ("""(
            (dq_cs39_status = 39 AND Party = 1 AND size(ftpaAppellantNoticeDocument) = 0)
            OR
            (ftpaAppellantNoticeDocument IS NULL)
        )""")

        checks["valid_ftpaRespondentNoticeDocument"] = ("""(
            (dq_cs39_status = 39 AND Party = 2 AND size(ftpaRespondentNoticeDocument) = 0)
            OR
            (ftpaRespondentNoticeDocument IS NULL)
        )""")

        return checks
