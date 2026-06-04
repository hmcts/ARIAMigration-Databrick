from pyspark.sql.functions import (
    col,
    when,
    lit,
    array,
    struct,
    collect_list,
    max as spark_max,
    date_format,
    abs,
    sum as sum_,
    from_json,
)
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

from . import paymentPending as PP


###############################################################
#########         paymentType Function              ###########
###############################################################


def paymentType(silver_m1, silver_m4):
    payment_content, payment_audit = PP.paymentType(silver_m1)

    conditions_all = col("dv_CCDAppealType").isin(["EA", "EU", "HU", "PA"])

    filtered_rows = silver_m4.alias("t").join(
        silver_m4.filter(~col("TransactionTypeId").isin(6, 19))
        .select("ReferringTransactionId")
        .where(col("ReferringTransactionId").isNotNull())
        .distinct()
        .alias("r"),
        col("t.TransactionId") == col("r.ReferringTransactionId"),
        "left_anti",
    )

    valid_cases = (
        filtered_rows.groupBy("CaseNo")
        .agg(
            sum_(when(col("TransactionTypeId") == 3, 1).otherwise(0)).alias(
                "type3_count"
            )
        )
        .filter(col("type3_count") > 0)
        .select("CaseNo")
    )

    final_filtered_df = filtered_rows.join(valid_cases, "CaseNo", "inner")

    ref_txn_df = (
        silver_m4.filter(col("TransactionTypeId").isin(6, 19))
        .select("ReferringTransactionId")
        .where(col("ReferringTransactionId").isNotNull())
        .distinct()
    )

    payment_status = (
        silver_m4.alias("m4")
        .join(ref_txn_df.alias("r_txn"),
            col("m4.TransactionId") == col("r_txn.ReferringTransactionId"),
            "left_anti")
        .filter(col("SumBalance") == True)
        .groupBy("CaseNo")
        .agg(
            sum_("Amount").alias("SumAmount"),
            spark_max(col("TransactionId")).alias("MaxTransactionId"),
        )
        .alias("max")
        .join(
            silver_m4.alias("type").select("CaseNo", "TransactionTypeId", "TransactionId"),
            on=(
                (col("max.CaseNo") == col("type.CaseNo")) &
                (col("max.MaxTransactionId") == col("type.TransactionId"))
            ),
        )
        .withColumn(
            "paymentStatus",
            when(col("SumAmount") > 0, lit("Payment pending"))
            .when(
                (col("SumAmount") == 0) & (col("type.TransactionTypeId") == 19),
                lit("Payment pending"),
            )
            .otherwise(lit("Paid")),
        )
        .select("max.CaseNo", "paymentStatus")
    )

    paid_amount = (
        final_filtered_df.filter(col("SumTotalPay") == True)
        .groupBy("CaseNo")
        .agg(
            abs(sum_(col("Amount"))).alias("paidAmount"),
            collect_list(col("Amount")).alias("amountList"),
        )
    )

    payment_content_final = (
        payment_content.alias("payment_content")
        .join(silver_m1, ["CaseNo"], "left")
        .join(paid_amount, ["CaseNo"], "left")
        .join(payment_status.alias("payment_status"), ["CaseNo"], "left")
        .join(valid_cases.withColumn("has_valid_txn", lit(True)), ["CaseNo"], "left")
        .select(
            "payment_content.*",
            when(
                (col("dv_CCDAppealType") == "PA") & (col("dv_representation") == "LR"),
                lit("payLater"),
            ).alias("paAppealTypePaymentOption"),
            when(
                (col("dv_CCDAppealType") == "PA") & (col("dv_representation") == "AIP"),
                lit("payLater"),
            ).alias("paAppealTypeAipPaymentOption"),
            when(
                (col("dv_CCDAppealType").isin(["DC", "RP"]))
                & (col("VisitVisatype") == 1),
                "decisionWithoutHearing",
            )
            .when(
                (col("dv_CCDAppealType").isin(["DC", "RP"]))
                & (col("VisitVisatype") == 2),
                "decisionWithHearing",
            )
            .alias("rpDcAppealHearingOption"),
            when(
                conditions_all & col("has_valid_txn"), date_format(col("DateCorrectFeeReceived"), "yyyy-MM-dd")
            ).alias("paidDate"),
            when(
                conditions_all & col("has_valid_txn"),
                when(col("paidAmount").isNotNull(), col("paidAmount").cast(IntegerType()).cast(StringType()))
                .otherwise(lit("0")),
            ).alias("paidAmount"),
            when(
                conditions_all & col("has_valid_txn"),
                lit(
                    "This is an ARIA Migrated Case. The payment was made in ARIA and the payment history can be found in the case notes."
                ),
            )
            .otherwise(lit(None))
            .alias("additionalPaymentInfo"),
            when(
                conditions_all,
                (
                    when(
                        col("payment_status.paymentStatus").isNotNull(),
                        col("payment_status.paymentStatus"),
                    ).otherwise(lit("Paid"))
                ),
            )
            .otherwise(lit(None))
            .alias("dv_paymentStatus"),
        )
    .select(
            "CaseNo",
            "feeAmountGbp",
            "feeDescription",
            "feeWithHearing",
            "feeWithoutHearing",
            "paymentDescription",
            "feePaymentAppealType",
            col("dv_paymentStatus").alias("paymentStatus"),
            "feeVersion",
            "decisionHearingFeeOption",
            "hasServiceRequestAlready",
            "paAppealTypePaymentOption",
            "paAppealTypeAipPaymentOption",
            "rpDcAppealHearingOption",
            "paidDate",
            "paidAmount",
            "additionalPaymentInfo"
        )
    ).distinct()

    payment_audit_final = (
        payment_audit.alias("audit")
        .join(paid_amount.alias("paid_amount"), ["CaseNo"], "left")
        .join(payment_content_final.alias("payment_content"), ["CaseNo"], "left")
        .join(silver_m1, ["CaseNo"], "left")
        .select(
            "audit.*",
            # paAppealTypePaymentOption
            array(struct(lit("dv_CCDAppealType"), lit("dv_representation"))).alias(
                "paAppealTypePaymentOption_inputFields"
            ),
            array(struct(col("dv_CCDAppealType"), col("dv_representation"))).alias(
                "paAppealTypePaymentOption_inputValues"
            ),
            col("payment_content.paAppealTypePaymentOption"),
            lit("yes").alias("paAppealTypePaymentOption_Transformation"),
            # paAppealTypeAipPaymentOption
            array(struct(lit("dv_CCDAppealType"), lit("dv_representation"))).alias(
                "paAppealTypeAipPaymentOption_inputFields"
            ),
            array(struct(col("dv_CCDAppealType"), col("dv_representation"))).alias(
                "paAppealTypeAipPaymentOption_inputValues"
            ),
            col("payment_content.paAppealTypeAipPaymentOption"),
            lit("yes").alias("paAppealTypeAipPaymentOption_Transformation"),
            # rpDcAppealHearingOption
            array(struct(lit("dv_CCDAppealType"), lit("VisitVisaType"))).alias(
                "rpDcAppealHearingOption_inputFields"
            ),
            array(struct(col("dv_CCDAppealType"), col("VisitVisaType"))).alias(
                "rpDcAppealHearingOption_inputValues"
            ),
            col("payment_content.rpDcAppealHearingOption"),
            lit("yes").alias("rpDcAppealHearingOption_Transformation"),
            # paidDate
            array(struct(lit("dv_CCDAppealType"), lit("DateCorrectFeeReceived"))).alias(
                "paidDate_inputFields"
            ),
            array(struct(col("dv_CCDAppealType"), col("DateCorrectFeeReceived"))).alias(
                "paidDate_inputValues"
            ),
            col("payment_content.paidDate"),
            lit("yes").alias("paidDate_Transformation"),
            # paidAmount
            array(struct(lit("dv_CCDAppealType"), lit("paid_amount"))).alias(
                "paidAmount_inputFields"
            ),
            array(struct(col("dv_CCDAppealType"), col("paid_amount.amountList"))).alias(
                "paidAmount_inputValues"
            ),
            col("payment_content.paidAmount"),
            lit("yes").alias("paidAmount_Transformation"),
        )
    )

    return payment_content_final, payment_audit_final


################################################################
##########        remissionTypes Function            ###########
################################################################


def remissionTypes(silver_m1, bronze_remission_lookup_df, silver_m4):
    df_final, df_audit = PP.remissionTypes(
        silver_m1, bronze_remission_lookup_df, silver_m4
    )
    conditions_all = col("dv_CCDAppealType").isin(["EA", "EU", "HU", "PA"])

    amount_remitted = (
        silver_m4.filter((col("TransactionTypeId") == 5) & (col("Status") != 3))
        .groupBy("CaseNo")
        .agg(
            abs(sum_(col("Amount"))).alias("amountRemitted"),
            collect_list(col("Amount")).alias("amountRemittedList"),
        )
    )

    ref_txn_df = (
        silver_m4.filter(col("TransactionTypeId").isin(6, 19))
        .select("ReferringTransactionId")
        .where(col("ReferringTransactionId").isNotNull())
        .distinct()
    )

    # Use left anti join to remove matches
    filtered_df = (
        silver_m4.alias("m4")
        .join(
            ref_txn_df.alias("ref_txn"),
            col("m4.TransactionId") == col("ref_txn.ReferringTransactionId"),
            "left_anti",
        )
        .select("CaseNo", "TransactionId", "TransactionTypeId", "Amount", "SumTotalFee")
    )

    amount_left_to_pay = (
        filtered_df.filter(col("SumTotalFee") == True)
        .groupBy("CaseNo")
        .agg(
            sum_(col("Amount")).alias("amountLeftToPay"),
            collect_list(col("Amount")).alias("amountLeftToPayList"),
        )
    )

    df_final = (
        df_final.alias("source")
        .join(silver_m1, ["CaseNo"], "left")
        .join(amount_remitted, ["CaseNo"], "left")
        .join(amount_left_to_pay, ["CaseNo"], "left")
        .withColumn(
            "remissionDecision",
            when(
                (conditions_all) & (col("PaymentRemissionGranted") == 1),
                lit("approved"),
            ).when(
                (conditions_all) & (col("PaymentRemissionGranted") == 2),
                lit("rejected"),
            ),
        )
        .withColumn(
            "remissionDecisionReason",
            when(
                (conditions_all) & (col("PaymentRemissionGranted") == 1),
                lit("This is a migrated case. The remission was granted."),
            ).when(
                (conditions_all) & (col("PaymentRemissionGranted") == 2),
                lit("This is a migrated case. The remission was rejected."),
            ),
        )
        .withColumn(
            "amountRemitted",
            when(
                (conditions_all) & (col("PaymentRemissionGranted") == 1),
                when(col("amountRemitted").isNotNull(), col("amountRemitted") * 100)
                .otherwise(lit(None))
                .cast(IntegerType())
                .cast(StringType()),
            ),
        )
        .withColumn(
            "amountLeftToPay",
            when(
                (conditions_all) & (col("PaymentRemissionGranted") == 1),
                when(col("amountLeftToPay").isNotNull(), col("amountLeftToPay"))
                .otherwise(lit(None))
                .cast(IntegerType())
                .cast(StringType()),
            ),
        )
        .select(
            "source.*",
            "remissionDecision",
            "remissionDecisionReason",
            "amountRemitted",
            "amountLeftToPay",
        )
    ).distinct()

    common_inputFields = [lit("dv_CCDAppealType"), lit("dv_representation")]
    common_inputValues = [
        col("m1_audit.dv_CCDAppealType"),
        col("m1_audit.dv_representation"),
    ]

    df_audit = (
        df_audit.alias("audit")
        .join(df_final.alias("content"), ["CaseNo"], "left")
        .join(amount_remitted.alias("amount_remitted"), ["CaseNo"], "left")
        .join(amount_left_to_pay.alias("amount_left_to_pay"), ["CaseNo"], "left")
        .join(silver_m1.alias("m1_audit"), ["CaseNo"], "left")
        .select(
            "audit.*",
            array(struct(*common_inputFields, lit("content.remissionDecision"))).alias(
                "remissionDecision_inputFields"
            ),
            array(struct(*common_inputValues, col("content.remissionDecision"))).alias(
                "remissionDecision_inputValues"
            ),
            col("content.remissionDecision"),
            lit("yes").alias("remissionDecision_Transformed"),
            array(
                struct(*common_inputFields, lit("content.remissionDecisionReason"))
            ).alias("remissionDecisionReason_inputFields"),
            array(
                struct(*common_inputValues, col("content.remissionDecisionReason"))
            ).alias("remissionDecisionReason_inputValues"),
            col("content.remissionDecisionReason"),
            lit("yes").alias("remissionDecisionReason_Transformed"),
            array(
                struct(*common_inputFields, lit("amount_remitted.amountRemittedList"))
            ).alias("amountRemitted_inputFields"),
            array(
                struct(*common_inputValues, col("amount_remitted.amountRemittedList"))
            ).alias("amountRemitted_inputValues"),
            col("content.amountRemitted"),
            lit("yes").alias("amountRemitted_Transformed"),
            array(
                struct(
                    *common_inputFields, lit("amount_left_to_pay.amountLeftToPayList")
                )
            ).alias("amountLeftToPay_inputFields"),
            array(
                struct(
                    *common_inputValues, col("amount_left_to_pay.amountLeftToPayList")
                )
            ).alias("amountLeftToPay_inputValues"),
            col("content.amountLeftToPay"),
            lit("yes").alias("amountLeftToPay_Transformed"),
        )
    )

    return df_final, df_audit


###############################################################
#########          homeOffice extended              ###########
###############################################################
def homeOfficeDetails(silver_m1, silver_m2, silver_c, bronze_HORef_cleansing):
    df_final, df_audit = PP.homeOfficeDetails(
        silver_m1, silver_m2, silver_c, bronze_HORef_cleansing
    )

    homeOfficeAppellantList_schema = (
        StructType([
            StructField("list_items", ArrayType(StructType([
                StructField("code", StringType(), True),
                StructField("label", StringType(), True)
            ])), True),
            StructField("value", StructType([
                StructField("code", StringType(), True),
                StructField("label", StringType(), True)
            ]), True)
        ])
    )

    homOfficeCaseStatusDate_schema = (
        StructType([
            StructField("applicationStatus", StructType([
                StructField("ccdHomeOfficeMetadata", ArrayType(StringType()), True),
                StructField("ccdRejectionReasons", ArrayType(StringType()), True),
                StructField("roleSubType", StructType([
                    StructField("code", StringType(), True),
                    StructField("description", StringType(), True)
                ]), True),
                StructField("roleType", StructType([
                    StructField("code", StringType(), True),
                    StructField("description", StringType(), True)
                ]), True)
            ]), True),
            StructField("displayAppellantDetailsTitle", StringType(), True),
            StructField("displayApplicationDetailsTitle", StringType(), True),
            StructField("displayDateOfBirth", StringType(), True),
            StructField("person", StructType([
                StructField("dayOfBirth", IntegerType(), True),
                StructField("familyName", StringType(), True),
                StructField("fullName", StringType(), True),
                StructField("gender", StructType([
                    StructField("code", StringType(), True),
                    StructField("description", StringType(), True)
                ]), True),
                StructField("givenName", StringType(), True),
                StructField("monthOfBirth", IntegerType(), True),
                StructField("nationality", StructType([
                    StructField("code", StringType(), True),
                    StructField("description", StringType(), True)
                ]), True),
                StructField("yearOfBirth", IntegerType(), True)
            ]), True)
        ])
    )

    condition = col("dv_CCDAppealType").isin(["PA", "RP"])

    df_final = (
        df_final.alias("source")
        .join(silver_m1.select("CaseNo", "dv_CCDAppealType"), ["CaseNo"], "left")
        .withColumn("homeOfficeSearchStatus", when(condition, lit("SUCCESS")).otherwise(lit(None)))
        .withColumn("homeOfficeSearchNoMatch", when(condition, lit("NO_MATCH")).otherwise(lit(None)))
        .withColumn("matchingAppellantDetailsFound", when(condition, lit("No")).otherwise(lit(None)))
        .withColumn("homeOfficeAppellantsList", when(condition, from_json(lit("""
            {
                "list_items":[{"code":"NoMatch","label":"No Match"}],
                "value":{"code":"NoMatch","label":"No Match"}
            }
        """), homeOfficeAppellantList_schema)).otherwise(lit(None)))
        .withColumn("homeOfficeCaseStatusDate", when(condition, from_json(lit("""
            {
                "applicationStatus": {
                    "ccdHomeOfficeMetadata": [],
                    "ccdRejectionReasons": [],
                    "roleSubType": {"code": "No match", "description": "No match"},
                    "roleType": {"code": "No match", "description": "No match"}
                },
                "displayAppellantDetailsTitle": "<h2>Appellant details</h2>",
                "displayApplicationDetailsTitle": "<h2>Application details</h2>",
                "displayDateOfBirth": "No match",
                "person": {
                    "dayOfBirth": 0,
                    "familyName": "No match",
                    "fullName": "No match",
                    "gender": {"code": "No match", "description": "No match"},
                    "givenName": "No match",
                    "monthOfBirth": 0,
                    "nationality": {"code": "No match", "description": "No match"},
                    "yearOfBirth": 0
                }
            }
        """), homOfficeCaseStatusDate_schema)).otherwise(lit(None)))
        .select(
            "source.*",
            "homeOfficeSearchStatus",
            "homeOfficeSearchNoMatch",
            "matchingAppellantDetailsFound",
            "homeOfficeAppellantsList",
            "homeOfficeCaseStatusDate",
        )
    )

    common_inputFields = [lit("dv_CCDAppealType"), lit("dv_representation")]
    common_inputValues = [
        col("m1_audit.dv_CCDAppealType"),
        col("m1_audit.dv_representation"),
    ]

    df_audit = (
        df_audit.alias("audit")
        .join(df_final.alias("content"), ["CaseNo"], "left")
        .join(silver_m1.alias("m1_audit"), ["CaseNo"], "left")
        .select(
            "audit.*",
            array(struct(*common_inputFields)).alias("homeOfficeSearchStatus_inputFields"),
            array(struct(*common_inputValues)).alias("homeOfficeSearchStatus_inputValues"),
            col("content.homeOfficeSearchStatus"),
            lit("yes").alias("homeOfficeSearchStatus_Transformed"),
            array(struct(*common_inputFields)).alias("homeOfficeSearchNoMatch_inputFields"),
            array(struct(*common_inputValues)).alias("homeOfficeSearchNoMatch_inputValues"),
            col("content.homeOfficeSearchNoMatch"),
            lit("yes").alias("homeOfficeSearchNoMatch_Transformed"),
            array(struct(*common_inputFields)).alias("matchingAppellantDetailsFound_inputFields"),
            array(struct(*common_inputValues)).alias("matchingAppellantDetailsFound_inputValues"),
            col("content.matchingAppellantDetailsFound"),
            lit("yes").alias("matchingAppellantDetailsFound_Transformed"),
            array(struct(*common_inputFields)).alias("homeOfficeAppellantsList_inputFields"),
            array(struct(*common_inputValues)).alias("homeOfficeAppellantsList_inputValues"),
            col("content.homeOfficeAppellantsList"),
            lit("yes").alias("homeOfficeAppellantsList_Transformed"),
            array(struct(*common_inputFields)).alias("homeOfficeCaseStatusDate_inputFields"),
            array(struct(*common_inputValues)).alias("homeOfficeCaseStatusDate_inputValues"),
            col("content.homeOfficeCaseStatusDate"),
            lit("yes").alias("homeOfficeCaseStatusDate_Transformed")
        )
    )

    return df_final, df_audit


if __name__ == "__main__":
    pass
