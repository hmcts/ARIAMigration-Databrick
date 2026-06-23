from .dq_rules import DQRulesBase


class decidedADQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_hearing_actuals()
        checks = checks | self.get_checks_document()
        checks = checks | self.get_checks_substantive_decision()
        checks = checks | self.get_checks_general_default()
        checks = checks | self.get_checks_ftpa()

        return checks

    def get_checks_hearing_actuals(self, checks={}):

        checks["valid_actualCaseHearingLength"] = ("""
        
        (
            CaseStatus_SD IN (37,38,26) AND Outcome_SD IN (1,2)
            AND element_at(actualCaseHearingLength, 'hours') =
                CAST(FLOOR(CAST(HearingDuration AS INT) / 60) AS STRING)
            AND element_at(actualCaseHearingLength, 'minutes') =
                CAST(CAST(HearingDuration AS INT) % 60 AS STRING)
            )
        """)

        checks["valid_attendingJudge"] = (
            """
            (   (attendingJudge IS NULL AND Adj_Determination_Title IS NULL AND Adj_Determination_Forenames IS NULL AND Adj_Determination_Surname IS NULL)
                OR
                (attendingJudge = concat(Adj_Determination_Title, ' ', Adj_Determination_Forenames, ' ', Adj_Determination_Surname))
            )
            """
        )

        return checks

    def get_checks_substantive_decision(self, checks={}):

        checks["valid_sendDecisionsAndReasonsDate"] = """
        (
            CaseStatus_SD IN (37,38,26) AND Outcome_SD IN (1,2)
            AND 
            to_date(
                to_timestamp(DecisionDate, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX')
            ) = to_date(trim(sendDecisionsAndReasonsDate), 'yyyy-MM-dd')
        )
        """

        checks["valid_appealDate"] = """
        (
            CaseStatus_SD IN (37,38,26) AND Outcome_SD IN (1,2)
            AND 
            to_date(DecisionDate, 'yyyy-MM-dd') = to_date(appealDate, 'yyyy-MM-dd')
        )
        """

        checks["valid_anonymityOrder"] = ("(anonymityOrder = 'No')")

        checks["valid_appealDecision"] = """
        (
            
            CaseStatus_SD IN (37,38,26) AND Outcome_SD IN (1,2)
            AND 
            CASE
                WHEN Outcome_SD = 1 THEN (appealDecision = 'Allowed')
                WHEN Outcome_SD = 2 THEN (appealDecision = 'Dismissed')
            END
        )
        """

        checks["valid_isDecisionAllowed"] = """
        (
            CaseStatus_SD IN (37,38,26) AND Outcome_SD IN (1,2)
            AND 
            CASE
                WHEN Outcome_SD = 1 THEN (isDecisionAllowed = 'allowed')
                WHEN Outcome_SD = 2 THEN (isDecisionAllowed = 'dismissed')
                ELSE (isDecisionAllowed IS NULL)
            END
        )
        """

        return checks

    def get_checks_document(self, checks={}):
        checks["valid_finalDecisionAndReasonsDocuments"] = (
            "(size(finalDecisionAndReasonsDocuments) = 0 AND finalDecisionAndReasonsDocuments IS NOT NULL)" 
        )
        return checks

    def get_checks_general_default(self, checks={}):

        checks["valid_appealDecisionAvailable"] = ("(appealDecisionAvailable = 'Yes')")

        return checks

    def get_checks_ftpa(self, checks={}):
        
        checks["valid_ftpaApplicationDeadline"] = """
        (
            (
                CASE
                    WHEN Outcome_SD IN (1, 2)
                        AND CaseStatus_SD IN (37, 38, 26)
                        AND CategoryId = 37
                    THEN
                        to_date(ftpaApplicationDeadline) = to_date(date_add(DecisionDate, 14))

                    WHEN Outcome_SD IN (1, 2)
                        AND CaseStatus_SD IN (37, 38, 26)
                        AND CategoryId = 38
                    THEN
                        to_date(ftpaApplicationDeadline) = to_date(date_add(DecisionDate, 28))
                END
            )
            OR
            (
                (
                    (Outcome_SD IS NULL OR Outcome_SD NOT IN (1,2))
                    OR (CaseStatus_SD IS NULL OR CaseStatus_SD NOT IN (37,38,26))
                    OR (CategoryId IS NULL OR CategoryId NOT IN (37,38))
                )
                AND ftpaApplicationDeadline IS NULL
            )
        )
        """
        return checks

