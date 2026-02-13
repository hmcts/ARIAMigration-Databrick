from .dq_rules import DQRulesBase


class appealSubmittedDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_base_checks()

        return checks

    def get_base_checks(self, checks={}):
        checks["valid_paymentStatus"] = (
            """(
                (
                    (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA','EU','HU','PA'))
                    AND
                    (paymentStatus <=> (
                        IF(
                        (
                            (
                                AGGREGATE(
                                    TRANSFORM(valid_transactionList, x ->
                                    CASE
                                        WHEN (CAST(x.SumBalance AS INT) = 1 AND NOT(ARRAY_CONTAINS(lu_ref_txn, x.TransactionId)))
                                        THEN x.Amount
                                        ELSE 0
                                    END
                                    ),
                                    CAST(0 AS DECIMAL(19, 4)), (acc, x) -> CAST(acc + x AS DECIMAL(19, 4))
                                ) > 0
                            )
                            OR
                            (
                                (
                                    AGGREGATE(
                                    TRANSFORM(valid_transactionList, x ->
                                        CASE
                                        WHEN (CAST(x.SumBalance AS INT) = 1 AND NOT(ARRAY_CONTAINS(lu_ref_txn, x.TransactionId)))
                                        THEN x.Amount
                                        ELSE 0
                                        END
                                    ),
                                    CAST(0 AS DECIMAL(19, 4)), (acc, x) -> CAST(acc + x AS DECIMAL(19, 4))
                                    ) = 0
                                )
                                AND
                                (
                                    ELEMENT_AT(
                                        ARRAY_SORT(
                                            FILTER(
                                                valid_transactionList, x -> CAST(x.SumBalance AS INT) = 1 AND NOT(ARRAY_CONTAINS(lu_ref_txn, x.TransactionId))
                                            ), (a, b) -> b.TransactionId - a.TransactionId
                                        ), 1
                                    ).TransactionTypeId = 19
                                )
                            )
                        ),
                        'Payment pending',
                        'Paid'
                        )
                    ))
                )
                OR
                (
                    (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA','EU','HU','PA'))
                    AND
                    (paymentStatus IS NULL)
                )
            )"""
        )

        checks["valid_paAppealTypePaymentOption"] = (
            """(
                (
                    dv_representation <=> 'LR' AND dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('PA')
                    AND paAppealTypePaymentOption IS NOT NULL AND paAppealTypePaymentOption IN ('payLater')
                )
                OR
                (
                (
                    NOT(dv_representation <=> 'LR')
                    OR
                    dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('PA')
                )
                AND (paAppealTypePaymentOption IS NULL)
                )
            )"""
        )

        checks["valid_paAppealTypeAipPaymentOption"] = (
            """(
                (
                    dv_representation = 'AIP' AND dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('PA')
                    AND paAppealTypeAipPaymentOption IS NOT NULL AND paAppealTypeAipPaymentOption IN ('payLater')
                )
                OR
                (
                (
                    NOT(dv_representation <=> 'AIP')
                    OR
                    dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('PA')
                )
                AND (paAppealTypeAipPaymentOption IS NULL)
                )
            )"""
        )

        checks["valid_rpDcAppealHearingOption"] = (
            """(
                (
                (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('DC','RP'))
                AND
                (
                    (VisitVisaType <=> 1 AND rpDcAppealHearingOption <=> 'decisionWithoutHearing')
                    OR
                    (VisitVisaType <=> 2 AND rpDcAppealHearingOption <=> 'decisionWithHearing')
                )
                )
                OR
                (
                (
                    (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('DC','RP'))
                    OR
                    (VisitVisaType IS NULL OR VisitVisaType NOT IN (1, 2))
                )
                AND
                (rpDcAppealHearingOption IS NULL)
                )
            )"""
        )

        checks["valid_paidDate"] = (
            "((dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA') AND paidDate IS NOT NULL) OR (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA') AND paidDate IS NULL))"
        )

        checks["valid_paidAmount"] = (
            """(
                (
                    (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA'))
                    AND
                    (paidAmount <=> (
                        CAST(ABS(CAST(AGGREGATE(
                        TRANSFORM(valid_transactionList, x ->
                            CASE
                            WHEN (CAST(x.SumTotalPay AS INT) = 1 AND NOT(ARRAY_CONTAINS(lu_ref_txn, x.TransactionId)))
                            THEN x.Amount
                            ELSE 0
                            END
                        ),
                        CAST(0 AS DECIMAL(19, 4)), (acc, x) -> CAST(acc + x AS DECIMAL(19, 4))
                        ) AS INT)) AS STRING)
                    ))
                )
                OR
                (
                    (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA'))
                    AND
                    (paidAmount IS NULL)
                )
            )"""
        )

        checks["valid_additionalPaymentInfo"] = (
            "((dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA') AND additionalPaymentInfo <=> 'This is an ARIA Migrated Case. The payment was made in ARIA and the payment history can be found in the case notes.') OR (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA') AND additionalPaymentInfo IS NULL))"
        )

        checks["valid_paymentDescription"] = (
            """(
                (
                (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA'))
                AND
                (
                    (VisitVisaType <=> 1 AND paymentDescription <=> 'Appeal determined without a hearing')
                    OR
                    (VisitVisaType <=> 2 AND paymentDescription <=> 'Appeal determined with a hearing')
                )
                )
                OR
                (
                (
                    (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA'))
                    OR
                    (VisitVisaType IS NULL OR VisitVisaType NOT IN (1, 2))
                )
                AND
                (paymentDescription IS NULL)
                )
            )"""
        )

        checks["valid_remissionDecision"] = (
            """(
                (
                (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA'))
                AND
                (
                    (PaymentRemissionGranted = 1 AND remissionDecision <=> 'approved')
                    OR
                    (PaymentRemissionGranted = 2 AND remissionDecision <=> 'rejected')
                )
                )
                OR
                (
                (
                    (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA'))
                    OR
                    (PaymentRemissionGranted IS NULL OR PaymentRemissionGranted NOT IN (1, 2))
                )
                AND
                (remissionDecision IS NULL)
                )
            )"""
        )

        checks["valid_remissionDecisionReason"] = (
            """(
                (
                (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA'))
                AND
                (
                    (PaymentRemissionGranted = 1 AND remissionDecisionReason <=> 'This is a migrated case. The remission was granted.')
                    OR
                    (PaymentRemissionGranted = 2 AND remissionDecisionReason <=> 'This is a migrated case. The remission was rejected.')
                )
                )
                OR
                (
                (
                    (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA'))
                    OR
                    (PaymentRemissionGranted IS NULL OR PaymentRemissionGranted NOT IN (1, 2))
                )
                AND
                (remissionDecisionReason IS NULL)
                )
            )"""
        )

        checks["valid_amountRemitted"] = (
            """(
                (
                    (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA') AND PaymentRemissionGranted <=> 1)
                    AND
                    (amountRemitted <=> (
                        CAST(ABS(CAST(AGGREGATE(
                        TRANSFORM(valid_transactionList, x ->
                            CASE
                            WHEN (x.TransactionTypeId <=> 5 AND NOT(x.Status <=> 3))
                            THEN x.Amount
                            ELSE 0
                            END
                        ),
                        CAST(0 AS DECIMAL(19, 4)), (acc, x) -> CAST(acc + x AS DECIMAL(19, 4))
                        ) AS INT)) AS STRING)
                    ))
                )
                OR
                (
                    (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA') OR NOT(PaymentRemissionGranted <=> 1))
                    AND
                    (amountRemitted IS NULL)
                )
            )"""
        )

        checks["valid_amountLeftToPay"] = (
            """(
                (
                    (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA') AND PaymentRemissionGranted <=> 1)
                    AND
                    (amountLeftToPay <=> (
                        CAST(CAST(AGGREGATE(
                        TRANSFORM(valid_transactionList, x ->
                            CASE
                            WHEN (CAST(x.SumTotalFee AS INT) = 1 AND NOT(ARRAY_CONTAINS(lu_ref_txn, x.TransactionId)))
                            THEN x.Amount
                            ELSE 0
                            END
                        ),
                        CAST(0 AS DECIMAL(19, 4)), (acc, x) -> CAST(acc + x AS DECIMAL(19, 4))
                        ) AS INT) AS STRING)
                    ))
                )
                OR
                (
                    (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA') OR NOT(PaymentRemissionGranted <=> 1))
                    AND
                    (amountLeftToPay IS NULL)
                )
            )"""
        )

        checks["valid_caseNotes"] = (
            "(caseNotes IS NOT NULL)"
        )

        return checks
