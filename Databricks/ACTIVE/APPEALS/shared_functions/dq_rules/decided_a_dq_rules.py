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
        (HearingDuration IS NULL AND
            element_at(actualCaseHearingLength, 'hours') IS NULL AND
            element_at(actualCaseHearingLength, 'minutes') IS NULL
        ) OR (
            HearingDuration IS NOT NULL AND
            element_at(actualCaseHearingLength, 'hours') >=0 AND
            element_at(actualCaseHearingLength, 'minutes') >=0
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
            -- allow missing output if that's acceptable; remove the next two lines if not
            sendDecisionsAndReasonsDate IS NULL
            OR trim(sendDecisionsAndReasonsDate) = ''
            OR
            -- compare dates after parsing both sides
            to_date(
                to_timestamp(DecisionDate, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX')
            ) = to_date(trim(sendDecisionsAndReasonsDate), 'yyyy-MM-dd')
        )
        """

        checks["valid_appealDate"] = """
        (
            DecisionDate IS NULL
            OR
            to_date(DecisionDate, 'yyyy-MM-dd') = to_date(appealDate, 'yyyy-MM-dd')
        )
        """

        checks["valid_anonymityOrder"] = ("(anonymityOrder = 'No')")

        checks["valid_appealDecision"] = """
        (
            CASE
                WHEN Outcome_SD = 1 THEN (appealDecision = 'Allowed')
                WHEN Outcome_SD = 2 THEN (appealDecision = 'Dismissed')
                ELSE (appealDecision IS NULL)
            END
        )
        """

        checks["valid_isDecisionAllowed"] = """
        (
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
            "(size(finalDecisionAndReasonsDocuments) = 0)"
        )
        return checks

    def get_checks_general_default(self, checks={}):

        checks["valid_appealDecisionAvailable"] = ("(appealDecisionAvailable = 'Yes')")

        return checks

    def get_checks_ftpa(self, checks={}):

        checks["valid_ftpaApplicationDeadline"] = """
            (
                (DecisionDate IS NULL AND ftpaApplicationDeadline IS NULL)
                OR
                CASE
                    WHEN Outcome_SD IN (1, 2) AND CaseStatus_SD IN (37, 38, 26) AND CategoryId = 37 THEN
                        date_add(DecisionDate, 14) = to_date(ftpaApplicationDeadline)
                    WHEN Outcome_SD IN (1, 2) AND CaseStatus_SD IN (37, 38, 26) AND CategoryId = 38 THEN
                        date_add(DecisionDate, 28) = to_date(ftpaApplicationDeadline)
                    ELSE
                        date_add(DecisionDate, 0) = to_date(ftpaApplicationDeadline)
                END
            )
            """

        return checks

