from .dq_rules import DQRulesBase

class ftpaDecidedDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_checks_ftpaDecided()

        return checks

    def get_checks_ftpaDecided(self, checks={}):

        checks["valid_ftpaApplicantType"] = (
            """
            (
                (Party = 1 AND ftpaApplicantType = 'appellant')
                OR
                (Party = 2 AND ftpaApplicantType = 'respondent')
                OR
                (Party IS NULL AND ftpaApplicantType IS NULL)
                OR
                (Party NOT IN (1,2) AND ftpaApplicantType IS NULL)
            )
            """
        )


        checks["valid_ftpaFirstDecision"] = (
            """
            (
                (FtpaDecOutcome = 30 AND ftpaFirstDecision = 'granted')
                OR
                (FtpaDecOutcome = 31 AND ftpaFirstDecision = 'refused')
                OR
                (FtpaDecOutcome = 14 AND ftpaFirstDecision = 'notAdmitted')
                OR
                (FtpaDecOutcome IS NULL AND ftpaFirstDecision IS NULL)
                OR
                (FtpaDecOutcome NOT IN (30,31,14) AND ftpaFirstDecision IS NULL)
            )
            """
        )

        checks["valid_ftpaFinalDecisionForDisplay"] = (
            """
            (
                (FtpaDecOutcome = 30 AND ftpaFinalDecisionForDisplay = 'granted')
                OR
                (FtpaDecOutcome = 31 AND ftpaFinalDecisionForDisplay = 'refused')
                OR
                (FtpaDecOutcome = 14 AND ftpaFinalDecisionForDisplay = 'notAdmitted')
                OR
                (FtpaDecOutcome IS NULL AND ftpaFinalDecisionForDisplay IS NULL)
                OR
                (FtpaDecOutcome NOT IN (30,31,14) AND ftpaFinalDecisionForDisplay IS NULL)
            )
            """
        )

        checks["valid_ftpaAppellantDecisionDate"] = (
            """
            (
                (Party = 1 AND ftpaAppellantDecisionDate IS NOT NULL)
                OR
                (Party <> 1 AND ftpaAppellantDecisionDate IS NULL)
                OR
                (Party IS NULL AND ftpaAppellantDecisionDate IS NULL)
            )
            """
        )

        checks["valid_ftpaRespondentDecisionDate"] = (
            """
            (
                (Party = 2 AND ftpaRespondentDecisionDate IS NOT NULL)
                OR
                (Party <> 2 AND ftpaRespondentDecisionDate IS NULL)
                OR
                (Party IS NULL AND ftpaRespondentDecisionDate IS NULL)
            )
            """
        )

        checks["valid_ftpaAppellantRjDecisionOutcomeType"] = (
            """
            (
                (FtpaDecOutcome = 30 AND ftpaAppellantRjDecisionOutcomeType = 'granted')
                OR (FtpaDecOutcome = 31 AND ftpaAppellantRjDecisionOutcomeType = 'refused')
                OR (FtpaDecOutcome = 14 AND ftpaAppellantRjDecisionOutcomeType = 'notAdmitted')
                OR (FtpaDecOutcome IS NULL AND ftpaAppellantRjDecisionOutcomeType IS NULL)
                OR (FtpaDecOutcome NOT IN (30,31,14) AND ftpaAppellantRjDecisionOutcomeType IS NULL)
            )
            """
        )

        checks["valid_ftpaRespondentRjDecisionOutcomeType"] = (
            """
            (
                (FtpaDecOutcome = 30 AND ftpaRespondentRjDecisionOutcomeType = 'granted')
                OR (FtpaDecOutcome = 31 AND ftpaRespondentRjDecisionOutcomeType = 'refused')
                OR (FtpaDecOutcome = 14 AND ftpaRespondentRjDecisionOutcomeType = 'notAdmitted')
                OR (FtpaDecOutcome IS NULL AND ftpaRespondentRjDecisionOutcomeType IS NULL)
                OR (FtpaDecOutcome NOT IN (30,31,14) AND ftpaRespondentRjDecisionOutcomeType IS NULL)
            )
            """
        )

        checks["valid_isFtpaAppellantNoticeOfDecisionSetAside"] = (
            """
            (
                (Party = 1 AND isFtpaAppellantNoticeOfDecisionSetAside = 'No')
                OR
                (Party <> 1 AND isFtpaAppellantNoticeOfDecisionSetAside IS NULL)
                OR
                (Party IS NULL AND isFtpaAppellantNoticeOfDecisionSetAside IS NULL)
            )
            """
        )

        checks["valid_isFtpaRespondentNoticeOfDecisionSetAside"] = (
            """
            (
                (Party = 2 AND isFtpaRespondentNoticeOfDecisionSetAside = 'No')
                OR
                (Party <> 2 AND isFtpaRespondentNoticeOfDecisionSetAside IS NULL)
                OR
                (Party IS NULL AND isFtpaRespondentNoticeOfDecisionSetAside IS NULL)
            )
            """
        )

        return checks
