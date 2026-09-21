from .dq_rules import DQRulesBase


class appealSubmittedDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_base_checks()

        return checks

    def get_base_checks(self, checks={}):

        checks["valid_paymentStatus"] = (
            """(
                (
                    dv_CCDAppealType IS NOT NULL
                    AND dv_CCDAppealType IN ('EA','EU','HU','PA')
                )
                AND (
                    paymentStatus <=> (
                        IF(
                            (
                                AGGREGATE(
                                    TRANSFORM(
                                        FILTER(
                                            valid_transactionList,
                                            x -> CAST(x.SumBalance AS INT) = 1
                                                AND NOT EXISTS(
                                                    valid_transactionList,
                                                    r -> r.TransactionTypeId IN (6, 19)
                                                        AND r.ReferringTransactionId = x.TransactionId
                                                )
                                        ),
                                        x -> x.Amount
                                    ),
                                    CAST(0 AS DECIMAL(19,4)),
                                    (acc, x) -> CAST(acc + x AS DECIMAL(19,4))
                                ) > 0
                            )
                            OR
                            (
                                AGGREGATE(
                                    TRANSFORM(
                                        FILTER(
                                            valid_transactionList,
                                            x -> CAST(x.SumBalance AS INT) = 1
                                                AND NOT EXISTS(
                                                    valid_transactionList,
                                                    r -> r.TransactionTypeId IN (6, 19)
                                                        AND r.ReferringTransactionId = x.TransactionId
                                                )
                                        ),
                                        x -> x.Amount
                                    ),
                                    CAST(0 AS DECIMAL(19,4)),
                                    (acc, x) -> CAST(acc + x AS DECIMAL(19,4))
                                ) = 0
                                AND
                                (
                                    ELEMENT_AT(
                                        ARRAY_SORT(
                                            FILTER(
                                                valid_transactionList,
                                                x -> CAST(x.SumBalance AS INT) = 1
                                                    AND NOT EXISTS(
                                                        valid_transactionList,
                                                        r -> r.TransactionTypeId IN (6, 19)
                                                            AND r.ReferringTransactionId = x.TransactionId
                                                    )
                                            ),
                                            (a, b) -> b.TransactionId - a.TransactionId
                                        ),
                                        1
                                    ).TransactionTypeId = 19
                                )
                            ),
                            'Payment pending',
                            'Paid'
                        )
                    )
                )
            )
            OR
            (
                (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA','EU','HU','PA'))
                AND paymentStatus IS NULL
            )
            """
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
            """(
                (
                    dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')
                    AND SIZE(FILTER(valid_transactionList, x -> x.TransactionTypeId = 3 AND NOT ARRAY_CONTAINS(lu_ref_txn, x.TransactionId))) > 0
                )
                OR
                (
                    (
                        dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA')
                        OR NOT SIZE(FILTER(valid_transactionList, x -> x.TransactionTypeId = 3 AND NOT ARRAY_CONTAINS(lu_ref_txn, x.TransactionId))) > 0
                    )
                    AND paidDate IS NULL
                )
            )"""
        )

        checks["valid_paidAmount"] = (
            """(
                (
                    (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA'))
                    AND SIZE(FILTER(valid_transactionList, x -> x.TransactionTypeId = 3 AND NOT ARRAY_CONTAINS(lu_ref_txn, x.TransactionId))) > 0
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
                        ) AS INT)) * 100 AS STRING)
                    ))
                )
                OR
                (
                    (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA'))
                    AND NOT SIZE(FILTER(valid_transactionList, x -> x.TransactionTypeId = 3 AND NOT ARRAY_CONTAINS(lu_ref_txn, x.TransactionId))) > 0
                    AND
                    (paidAmount IS NULL)
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
            """(
                (
                    dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')
                    AND SIZE(FILTER(valid_transactionList, x -> x.TransactionTypeId = 3 AND NOT ARRAY_CONTAINS(lu_ref_txn, x.TransactionId))) > 0
                    AND additionalPaymentInfo <=> 'This is an ARIA Migrated Case. The payment was made in ARIA and the payment history can be found in the case notes.'
                )
                OR
                (
                    dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')
                    AND NOT SIZE(FILTER(valid_transactionList, x -> x.TransactionTypeId = 3 AND NOT ARRAY_CONTAINS(lu_ref_txn, x.TransactionId))) > 0
                    AND additionalPaymentInfo IS NULL
                )
                OR
                (
                    (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA'))
                    AND additionalPaymentInfo IS NULL
                )
            )"""
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
                    dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')
                    AND
                    (
                        (
                            (
                                PaymentRemissionGranted <=> 1
                                OR (
                                    (PaymentRemissionGranted IS NULL OR PaymentRemissionGranted = 0)
                                    AND COALESCE(SIZE(FILTER(valid_transactionList, x -> x.TransactionTypeId = 5)), 0) > 0
                                )
                            )
                            AND remissionDecision <=> 'approved'
                        )
                        OR
                        (
                            (
                                PaymentRemissionGranted <=> 2
                                OR (
                                    NOT (dv_CCDAppealType <=> 'PA')
                                    AND (PaymentRemissionGranted IS NULL OR PaymentRemissionGranted = 0)
                                    AND COALESCE(SIZE(FILTER(valid_transactionList, x -> x.TransactionTypeId = 5)), 0) = 0
                                )
                            )
                            AND remissionDecision <=> 'rejected'
                        )
                        OR
                        (
                            dv_CCDAppealType <=> 'PA'
                            AND (PaymentRemissionGranted IS NULL OR PaymentRemissionGranted = 0)
                            AND COALESCE(SIZE(FILTER(valid_transactionList, x -> x.TransactionTypeId = 5)), 0) = 0
                            AND remissionDecision IS NULL
                        )
                    )
                )
                OR
                (
                    (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA'))
                    AND remissionDecision IS NULL
                )
            )"""
        )

        checks["valid_remissionDecisionReason"] = (
            """(
                (
                    dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')
                    AND
                    (
                        (
                            (
                                PaymentRemissionGranted <=> 1
                                OR (
                                    (PaymentRemissionGranted IS NULL OR PaymentRemissionGranted = 0)
                                    AND COALESCE(SIZE(FILTER(valid_transactionList, x -> x.TransactionTypeId = 5)), 0) > 0
                                )
                            )
                            AND remissionDecisionReason <=> 'This is a migrated case. The remission was granted.'
                        )
                        OR
                        (
                            (
                                PaymentRemissionGranted <=> 2
                                OR (
                                    NOT (dv_CCDAppealType <=> 'PA')
                                    AND (PaymentRemissionGranted IS NULL OR PaymentRemissionGranted = 0)
                                    AND COALESCE(SIZE(FILTER(valid_transactionList, x -> x.TransactionTypeId = 5)), 0) = 0
                                )
                            )
                            AND remissionDecisionReason <=> 'This is a migrated case. The remission was rejected.'
                        )
                        OR
                        (
                            dv_CCDAppealType <=> 'PA'
                            AND (PaymentRemissionGranted IS NULL OR PaymentRemissionGranted = 0)
                            AND COALESCE(SIZE(FILTER(valid_transactionList, x -> x.TransactionTypeId = 5)), 0) = 0
                            AND remissionDecisionReason IS NULL
                        )
                    )
                )
                OR
                (
                    (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA'))
                    AND remissionDecisionReason IS NULL
                )
            )"""
        )

        checks["valid_amountRemitted"] = (
            """(
                        (
                dv_CCDAppealType IS NOT NULL
                AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')
                AND PaymentRemissionGranted <=> 1
                AND amountRemitted <=> (
                    CAST(
                        ABS(
                            CAST(
                                AGGREGATE(
                                    TRANSFORM(
                                        valid_transactionList,
                                        x ->
                                            CASE
                                                WHEN x.TransactionTypeId = 5
                                                    AND x.Status <> 3
                                                THEN x.Amount * 100
                                                ELSE 0
                                            END
                                    ),
                                    CAST(0 AS DECIMAL(19,4)),
                                    (acc, x) -> CAST(acc + x AS DECIMAL(19,4))
                                ) AS INT
                            )
                        ) AS STRING
                    )
                )
            )
            OR
            (
                (
                    dv_CCDAppealType IS NULL
                    OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA')
                    OR NOT (PaymentRemissionGranted <=> 1)
                    OR SIZE(FILTER(valid_transactionList, t -> t.TransactionTypeId = 5)) <=> 0
                )
                AND amountRemitted IS NULL
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
                        TRANSFORM(valid_transactionList1, x ->
                            CASE
                            WHEN (CAST(x.SumTotalFee AS INT) = 1 AND NOT(ARRAY_CONTAINS(lu_ref_txn1, x.TransactionId)))
                            THEN x.Amount
                            ELSE 0
                            END
                        ),
                        CAST(0 AS DECIMAL(19, 4)), (acc, x) -> CAST(acc + x AS DECIMAL(19, 4))
                        ) AS INT) *100 AS STRING)
                    ))
                )
                OR
                (
                    (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA') AND PaymentRemissionGranted <=> 1)
                    AND
                    (
                        NOT EXISTS(
                            valid_transactionList1,
                            x -> CAST(x.SumTotalFee AS INT) = 1 AND NOT(ARRAY_CONTAINS(lu_ref_txn1, x.TransactionId))
                        )
                        AND amountLeftToPay IS NULL
                    )
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

        # ARIADM-1920 (remissionTypes extended join)
        checks["valid_remissionType_in_list"] = (
            """(
                (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA') AND remissionType IS NOT NULL AND remissionType IN ('noRemission', 'hoWaiverRemission', 'helpWithFees', 'exceptionalCircumstancesRemission'))
                OR
                (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA') AND remissionType IS NULL)
                OR
                (
                    NOT (PaymentRemissionReason <=> PaymentRemissionReason_remAPS)
                    AND remissionType IS NULL
                )
            )"""
        )

        checks["valid_remissionClaim_in_list"] = (
            """(
                (
                    (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA'))
                    AND
                    (remissionClaim IS NOT NULL AND remissionClaim IN ('asylumSupport', 'legalAid', 'section17', 'section20', 'homeOfficeWaiver'))
                )
                OR
                (
                    (dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA') OR lu_remissionClaim_remAPS <=> 'OMIT')
                    AND remissionClaim IS NULL
                )
                OR
                (
                    NOT (PaymentRemissionReason <=> PaymentRemissionReason_remAPS)
                    AND remissionClaim IS NULL
                )
            )"""
        )

        checks["valid_feeRemissionType_not_null"] = (
            """(
                (dv_CCDAppealType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA') AND feeRemissionType IS NOT NULL)
                OR
                ((dv_CCDAppealType IS NULL OR dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA') OR lu_feeRemissionType_remAPS <=> 'OMIT') AND feeRemissionType IS NULL)
            )"""
        )

        checks["valid_completeCaseReviewDate"] = (
            """(
                NOT (ariaDesiredState <=> 'appealSubmitted' OR ariaDesiredState <=> 'pendingPayment')
                OR
                completeCaseReviewDate <=> date_format(DateLodged, 'yyyy-MM-dd')
            )"""
        )

        checks["valid_ariaDesiredState"] = "(ariaDesiredState <=> 'appealSubmitted')"

        # homeOfficeDetails: only populated for PA / RP appeals
        ho_conditions = "COALESCE(dv_CCDAppealType IN ('PA', 'RP'), FALSE)"

        checks["valid_homeOfficeSearchStatus"] = (
            f"(({ho_conditions} AND homeOfficeSearchStatus <=> 'SUCCESS') OR (NOT {ho_conditions} AND homeOfficeSearchStatus IS NULL))"
        )

        checks["valid_homeOfficeSearchNoMatch"] = (
            f"(({ho_conditions} AND homeOfficeSearchNoMatch <=> 'NO_MATCH') OR (NOT {ho_conditions} AND homeOfficeSearchNoMatch IS NULL))"
        )

        checks["valid_matchingAppellantDetailsFound"] = (
            f"(({ho_conditions} AND matchingAppellantDetailsFound <=> 'No') OR (NOT {ho_conditions} AND matchingAppellantDetailsFound IS NULL))"
        )

        checks["valid_homeOfficeAppellantsList"] = f"""
            (
                (
                    {ho_conditions}
                    AND homeOfficeAppellantsList.value.code <=> 'NoMatch'
                    AND homeOfficeAppellantsList.value.label <=> 'No Match'
                    AND size(homeOfficeAppellantsList.list_items) = 1
                    AND homeOfficeAppellantsList.list_items[0].code <=> 'NoMatch'
                    AND homeOfficeAppellantsList.list_items[0].label <=> 'No Match'
                )
                OR
                (NOT {ho_conditions} AND homeOfficeAppellantsList IS NULL)
            )
        """

        checks["valid_homeOfficeCaseStatusData"] = f"""
            (
                (
                    {ho_conditions}
                    AND homeOfficeCaseStatusData.applicationStatus.roleSubType.code <=> 'No match'
                    AND homeOfficeCaseStatusData.applicationStatus.roleType.code <=> 'No match'
                    AND size(homeOfficeCaseStatusData.applicationStatus.ccdHomeOfficeMetadata) = 0
                    AND size(homeOfficeCaseStatusData.applicationStatus.ccdRejectionReasons) = 0
                    AND homeOfficeCaseStatusData.displayAppellantDetailsTitle <=> '<h2>Appellant details</h2>'
                    AND homeOfficeCaseStatusData.displayApplicationDetailsTitle <=> '<h2>Application details</h2>'
                    AND homeOfficeCaseStatusData.displayDateOfBirth <=> 'No match'
                    AND homeOfficeCaseStatusData.person.fullName <=> 'No match'
                    AND homeOfficeCaseStatusData.person.familyName <=> 'No match'
                    AND homeOfficeCaseStatusData.person.givenName <=> 'No match'
                    AND homeOfficeCaseStatusData.person.gender.code <=> 'No match'
                    AND homeOfficeCaseStatusData.person.nationality.code <=> 'No match'
                    AND homeOfficeCaseStatusData.person.dayOfBirth <=> 0
                    AND homeOfficeCaseStatusData.person.monthOfBirth <=> 0
                    AND homeOfficeCaseStatusData.person.yearOfBirth <=> 0
                )
                OR
                (NOT {ho_conditions} AND homeOfficeCaseStatusData IS NULL)
            )
        """

        return checks
