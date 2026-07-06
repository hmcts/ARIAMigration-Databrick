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
                    (
                        CaseStatus_SD IN (37,38,26)
                        AND Outcome_SD IN (1,2)
                        AND element_at(actualCaseHearingLength, 'hours') =
                            CAST(FLOOR(CAST(HearingDuration AS INT) / 60) AS STRING)
                        AND element_at(actualCaseHearingLength, 'minutes') =
                            CAST(CAST(HearingDuration AS INT) % 60 AS STRING)
                    )
                    OR
                    (
                        (CaseStatus_SD NOT IN (37,38,26) OR Outcome_SD NOT IN (1,2))
                        AND element_at(actualCaseHearingLength, 'hours') IS NULL
                        AND element_at(actualCaseHearingLength, 'minutes') IS NULL
                    )
                    OR
                    (
                        (CaseStatus_SD IS NOT NULL OR Outcome_SD IS NULL)
                        AND element_at(actualCaseHearingLength, 'hours') IS NULL
                        AND element_at(actualCaseHearingLength, 'minutes') IS NULL
                    )
                    OR
                    (
                        hearingDuration IS NULL
                        AND element_at(actualCaseHearingLength, 'hours') IS NULL
                        AND element_at(actualCaseHearingLength, 'minutes') IS NULL
                    )
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
            (
                CaseStatus_SD IN (37,38,26) AND Outcome_SD IN (1,2)
                AND 
                to_date(
                    to_timestamp(DecisionDate_decided, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX')
                ) = to_date(trim(sendDecisionsAndReasonsDate), 'yyyy-MM-dd')
            )
            OR
            (
                (CaseStatus_SD NOT IN (37,38,26) OR Outcome_SD NOT IN (1,2))
                AND appealDate IS NULL
            )
            OR
            (
                (CaseStatus_SD IS NULL OR Outcome_SD IS NULL)
                AND appealDate IS NULL
            )
        )
        """

        checks["valid_appealDate"] = """
        (
            (
                CaseStatus_SD IN (37,38,26) AND Outcome_SD IN (1,2)
                AND 
                to_date(DecisionDate_decided, 'yyyy-MM-dd') = to_date(appealDate, 'yyyy-MM-dd')
            )
            OR
            (
                (CaseStatus_SD NOT IN (37,38,26) OR Outcome_SD NOT IN (1,2))
                AND appealDate IS NULL
            )
            OR
            (
                (CaseStatus_SD IS NULL OR Outcome_SD IS NULL)
                AND appealDate IS NULL
            )
        )
        """

        checks["valid_anonymityOrder"] = ("(anonymityOrder = 'No')")

        checks["valid_appealDecision"] = """
        (
            (
                CaseStatus_SD IN (37,38,26) AND Outcome_SD IN (1,2)
                AND 
                CASE
                    WHEN Outcome_SD = 1 THEN (appealDecision = 'Allowed')
                    WHEN Outcome_SD = 2 THEN (appealDecision = 'Dismissed')
                END
            )
            OR
            (
                (CaseStatus_SD NOT IN (37,38,26) OR Outcome_SD NOT IN (1,2))
                AND appealDecision IS NULL
            )
            OR
            (
                (CaseStatus_SD IS NULL OR Outcome_SD IS NULL)
                AND appealDecision IS NULL
            )
        )
        """

        checks["valid_isDecisionAllowed"] = """
        (
            (
                CaseStatus_SD IN (37,38,26) AND Outcome_SD IN (1,2)
                AND 
                CASE
                    WHEN Outcome_SD = 1 THEN (isDecisionAllowed = 'allowed')
                    WHEN Outcome_SD = 2 THEN (isDecisionAllowed = 'dismissed')
                    ELSE (isDecisionAllowed IS NULL)
                END
            )
            OR
            (
                (CaseStatus_SD NOT IN (37,38,26) OR Outcome_SD NOT IN (1,2))
                AND isDecisionAllowed IS NULL
            )
            OR
            (
                (CaseStatus_SD IS NULL OR Outcome_SD IS NULL)
                AND isDecisionAllowed IS NULL
            )
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
                    WHEN INorOUT = 'IN'
                    THEN
                        to_date(ftpaApplicationDeadline) = to_date(date_add(DecisionDate_decided, 14))

                    WHEN INorOUT = 'OOC'
                    THEN
                        to_date(ftpaApplicationDeadline) = to_date(date_add(DecisionDate_decided, 28))

                    WHEN INorOUT = 'OOC' OR INorOUT IS NULL
                    THEN
                        ftpaApplicationDeadline IS NULL
                END
            )
            OR
            (
                ftpaApplicationDeadline IS NULL AND (stage_detained = "OOC" OR stage_category = "OOC" OR stage_country = "OOC" 
                OR stage_postcode = "OOC" OR stage_address = "OOC")
            )
            OR
            (
                ftpaApplicationDeadline IS NULL AND DecisionDate_decided IS NULL
            )
        )
        """

        return checks

