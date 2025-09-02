from datetime import datetime
import re
import string
import pycountry
import pandas as pd

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    current_timestamp, collect_set, first, array_contains, 
    size, udf, coalesce, concat_ws, concat, trim, year,split,abs,sum as F_sum
)

from uk_postcodes_parsing import fix, postcode_utils

from Active_Functions import paymentPending


###############################################################
#########         paymentType Function              ###########
###############################################################

original_paymentType = paymentPending.paymentType 

def appealSubmitted_paymentType(silver_m1, silver_m4):
    payment_content, payment_audit = original_paymentType(silver_m1)

    conditions_all = col("dv_CCDAppealType").isin(["EA", "EU", "HU", "PA"])

    payment_common_inputFields = [lit("VisitVisatype")]
    payment_common_inputValues = [col("VisitVisatype")]

    paid_amount = silver_m4.groupBy("CaseNo").agg(
        abs(F_sum(col("Amount"))).alias("paidAmount"),
        collect_list(col("Amount")).alias("amountList")
    )

    payment_content_final = payment_content.alias("payment_content").join(silver_m1, ["CaseNo"], "left").join(paid_amount, ["CaseNo"], "left").select(
        "payment_content.*",
        when((col("dv_CCDAppealType") == "PA") & (col("dv_representation") == "LR"), "payLater")
            .otherwise("unknown").alias("paAppealTypePaymentOption"),
        when((col("dv_CCDAppealType") == "PA") & (col("dv_representation") == "AIP"), "payLater")
            .otherwise("unknown").alias("paAppealTypeAipPaymentOption"),
        when((col("dv_CCDAppealType").isin(["DC", "RP"])) & (col("VisitVisatype") == 1), "decisionWithoutHearing")
            .when((col("dv_CCDAppealType").isin(["DC", "RP"])) & (col("VisitVisatype") == 2), "decisionWithHearing")
            .otherwise("unknown").alias("rpDcAppealHearingOption"),
        when(conditions_all, date_format(col("DateCorrectFeeReceived"), "yyyy-MM-dd")).otherwise("unknown").alias("paidDate"),
        when(conditions_all, col("paidAmount")).otherwise("unknown").alias("paidAmount"),
        when(conditions_all,lit("This is an ARIA Migrated Case. The payment was made in ARIA and the payment history can be found in the case notes.")).otherwise("unknown").alias("additionalPaymentInfo")
    ).select(
        "CaseNo",
        "feeAmountGbp",
        "feeDescription",
        "feeWithHearing",
        "feeWithoutHearing",
        "paymentDescription",
        "feePaymentAppealType",
        "paymentStatus",
        "feeVersion",
        "decisionHearingFeeOption",
        "hasServiceRequestAlready",
        "paAppealTypePaymentOption",
        "paAppealTypeAipPaymentOption",
        "rpDcAppealHearingOption",
        "paidDate",
        "paidAmount",
        "additionalPaymentInfo",
        "paymentDescription"
    )

    payment_audit_final = payment_audit.alias("audit").join(paid_amount.alias("paid_amount"), ["CaseNo"], "left").join(payment_content_final.alias("payment_content"), ["CaseNo"], "left").join(silver_m1, ["CaseNo"], "left").select( "audit.*",
        # paAppealTypePaymentOption
        array(struct(lit("dv_CCDAppealType"), lit("dv_representation"))).alias("paAppealTypePaymentOption_inputFields"),
        array(struct(col("dv_CCDAppealType"), col("dv_representation"))).alias("paAppealTypePaymentOption_inputValues"),
        col("payment_content.paAppealTypePaymentOption"),
        lit("yes").alias("paAppealTypePaymentOption_Transformation"),

        # paAppealTypeAipPaymentOption
        array(struct(lit("dv_CCDAppealType"), lit("dv_representation"))).alias("paAppealTypeAipPaymentOption_inputFields"),
        array(struct(col("dv_CCDAppealType"), col("dv_representation"))).alias("paAppealTypeAipPaymentOption_inputValues"),
        col("payment_content.paAppealTypeAipPaymentOption"),
        lit("yes").alias("paAppealTypeAipPaymentOption_Transformation"),

        # rpDcAppealHearingOption
        array(struct(lit("dv_CCDAppealType"), lit("VisitVisatype"))).alias("rpDcAppealHearingOption_inputFields"),
        array(struct(col("dv_CCDAppealType"), col("VisitVisatype"))).alias("rpDcAppealHearingOption_inputValues"),
        col("payment_content.rpDcAppealHearingOption"),
        lit("yes").alias("rpDcAppealHearingOption_Transformation"),

        # paidDate
        array(struct(lit("DateCorrectFeeReceived"))).alias("paidDate_inputFields"),
        array(struct(col("DateCorrectFeeReceived"))).alias("paidDate_inputValues"),
        col("payment_content.paidDate"),
        lit("yes").alias("paidDate_Transformation"),

        # paidAmount
        array(struct(lit("paid_amount"))).alias("paidAmount_inputFields"),
        array(struct(col("paid_amount.amountList"))).alias("paidAmount_inputValues"),
        col("payment_content.paidAmount"),
        lit("yes").alias("paidAmount_Transformation")
    )

    return payment_content_final, payment_audit_final

################################################################
##########        remissionTypes Function            ###########
################################################################

original_remissionTypes = paymentPending.remissionTypes 

def appealSubmitted_remissionTypes(silver_m1, bronze_remission_lookup_df, silver_m4):

    df_final, df_audit = original_remissionTypes(silver_m1, bronze_remission_lookup_df)

    conditions_remissionTypes = col("dv_CCDAppealType").isin("EA", "EU", "HU", "PA")
    conditions = (col("dv_representation").isin('LR', 'AIP')) & (col("lu_appealType").isNotNull())

    amount_remitted = silver_m4.filter((col("TransactionTypeId") == 5) & (col("Status") != 3)).groupBy("CaseNo").agg(abs(F_sum(col("Amount"))).alias("amountRemitted"),collect_list(col("Amount")).alias("amountRemittedList"))

    referring_transactions = silver_m4.filter(col("TransactionTypeId").isin(6, 19)).select("ReferringTransactionId").distinct()
    referring_transaction_ids = [row.ReferringTransactionId for row in referring_transactions.collect()]
    amount_left_to_pay = silver_m4.filter((col("SumTotalFee") == 1) & (~col("TransactionID").isin(referring_transaction_ids))).groupBy("CaseNo").agg(F_sum(col("Amount")).alias("amountLeftToPay"),collect_list(col("Amount")).alias("amountLeftToPayList"))

    fields_list = [
    "source.remissionType",
    "source.remissionClaim",
    "source.feeRemissionType",
    "source.exceptionalCircumstances",
    "source.helpWithFeesReferenceNumber",
    "source.legalAidAccountNumber",
    "source.asylumSupportReference",
    "source.remissionDecision",
    "source.remissionDecisionReason",
    "source.amountRemitted",
    "source.amountLeftToPay"
    ]

    df_final = df_final.alias("source").join(silver_m1, ["CaseNo"], "left").join(amount_remitted, ["CaseNo"], "left").join(amount_left_to_pay, ["CaseNo"], "left").withColumn(
        "remissionDecision",
        when(col("PaymentRemissionGranted") == 1, "approved")
        .when(col("PaymentRemissionGranted") == 2, "rejected")
        .otherwise(None)
    ).withColumn(
        "remissionDecisionReason",
        when(col("PaymentRemissionGranted") == 1, "This is a migrated case. The remission was granted.")
        .when(col("PaymentRemissionGranted") == 2, "This is a migrated case. The remission was rejected.")
        .otherwise(None)
    ).withColumn(
        "amountRemitted",
        when(col("PaymentRemissionGranted") == 1, col("amountRemitted"))
        .otherwise(None)
    ).withColumn(
        "amountLeftToPay",
        when(col("PaymentRemissionGranted") == 1, col("amountLeftToPay"))
        .otherwise(None)
    ).select("source.*","remissionDecision","remissionDecisionReason","amountRemitted","amountLeftToPay")

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("m1_audit.dv_representation"), col("m1_audit.lu_appealType")]

    df_audit = df_audit.alias('audit').join(df_final.alias("content"),["CaseNo"],"left").join(amount_remitted.alias("amount_remitted"), ["CaseNo"], "left").join(amount_left_to_pay.alias("amount_left_to_pay"), ["CaseNo"], "left").join(silver_m1.alias("m1_audit"),["CaseNo"],"left").select( "audit.*",

        array(struct(*common_inputFields, lit("content.remissionDecision"))).alias("remissionDecision_inputFields"),
        array(struct(*common_inputValues, col("content.remissionDecision"))).alias("remissionDecision_inputValues"),
        col("content.remissionDecision"),
        lit("yes").alias("remissionDecision_Transformed"),

        array(struct(*common_inputFields, lit("content.remissionDecisionReason"))).alias("remissionDecisionReason_inputFields"),
        array(struct(*common_inputValues, col("content.remissionDecisionReason"))).alias("remissionDecisionReason_inputValues"),
        col("content.remissionDecisionReason"),
        lit("yes").alias("remissionDecisionReason_Transformed"),

        array(struct(*common_inputFields, lit("amount_remitted.amountRemittedList"))).alias("amountRemitted_inputFields"),
        array(struct(*common_inputValues, col("amount_remitted.amountRemittedList"))).alias("amountRemitted_inputValues"),
        col("content.amountRemitted"),
        lit("yes").alias("amountRemitted_Transformed"),

        array(struct(*common_inputFields, lit("amount_left_to_pay.amountLeftToPayList"))).alias("amountLeftToPay_inputFields"),
        array(struct(*common_inputValues, col("amount_left_to_pay.amountLeftToPayList"))).alias("amountLeftToPay_inputValues"),
        col("content.amountLeftToPay"),
        lit("yes").alias("amountLeftToPay_Transformed")
    )

    return df_final, df_audit


################################################################
##########        Import all Functions            ###########
################################################################


if __name__ == "__main__":
    pass
