import logging
from shared_functions.dq_rules import (
    paymentpending_dq_rules, appealSubmitted_dq_rules, awaitingEvidenceRespondentA_dq_rules, awaitingEvidenceRespondentB_dq_rules,
    caseUnderReview_dq_rules, reasonsForAppealSubmitted_dq_rules, listing_dq_rules, prepareforhearing_dq_rules, decision_dq_rules,
    decided_a_dq_rules, ftpa_submitted_b_dq_rules, ftpa_submitted_a_dq_rules, ftpaDecided_dq_rules, ended_dq_rules
)
from pyspark.sql import Window
from pyspark.sql.functions import coalesce, col, collect_list, lit, row_number, struct
from pyspark.sql.types import ArrayType, LongType


logger = logging.getLogger(__name__)


def build_rule_expression(rules: dict) -> str:
    """
    Joins multiple rule expressions into one combined SQL expression.
    """
    if len(rules.values()) == 0:
        logger.warning("No DQ rules found.")
        return ""

    return "({0})".format(" AND ".join(f"({rule})" for rule in rules.values()))


def base_DQRules(state: str = "paymentPending"):
    """
    Return a dictionary of the DQ rules to be used in the expectations
    """

    checks = {}

    state_flow = build_state_flow(state, [state])

    # Add all checks in state, errors if state not in mapping.
    for state_to_process in state_flow:
        checks = checks | add_state_dq_rules(state_to_process)

    return checks


def add_state_dq_rules(state: str) -> dict:
    dq_rules = {
        "paymentPending": paymentpending_dq_rules.paymentPendingDQRules().get_checks(),
        "appealSubmitted": appealSubmitted_dq_rules.appealSubmittedDQRules().get_checks(),
        "awaitingRespondentEvidence(a)": awaitingEvidenceRespondentA_dq_rules.awaitingEvidenceRespondentADQRules().get_checks(),
        "awaitingRespondentEvidence(b)": awaitingEvidenceRespondentB_dq_rules.awaitingEvidenceRespondentBDQRules().get_checks(),
        "caseUnderReview": caseUnderReview_dq_rules.caseUnderReviewDQRules().get_checks(),
        "reasonsForAppealSubmitted": reasonsForAppealSubmitted_dq_rules.reasonsForAppealSubmittedDQRules().get_checks(),
        "listing": listing_dq_rules.listingDQRules().get_checks(),
        "prepareForHearing": prepareforhearing_dq_rules.prepareForHearingDQRules().get_checks(),
        "decision": decision_dq_rules.decisionDQRules().get_checks(),
        "decided(a)": decided_a_dq_rules.decidedADQRules().get_checks(),
        "ftpaSubmitted(a)": ftpa_submitted_a_dq_rules.ftpaSubmittedADQRules().get_checks(),
        "ftpaSubmitted(b)": ftpa_submitted_b_dq_rules.ftpaSubmittedBDQRules().get_checks(),
        "ftpaDecided": ftpaDecided_dq_rules.ftpaDecidedDQRules().get_checks(),
        "ended": ended_dq_rules.endedDQRules().get_checks(),
        "remitted": {}
    }

    return dq_rules.get(state, {})


def previous_state_map(state: str):
    previous_state = {
        "appealSubmitted":               "paymentPending",
        "awaitingRespondentEvidence(a)": "appealSubmitted",
        "awaitingRespondentEvidence(b)": "awaitingRespondentEvidence(a)",
        "caseUnderReview":               "awaitingRespondentEvidence(b)",
        "reasonsForAppealSubmitted":     "awaitingRespondentEvidence(b)",
        "listing":                       "awaitingRespondentEvidence(b)",
        "prepareForHearing":             "listing",
        "decision":                      "prepareHearing",
        "decided(a)":                    "decision",
        "ftpaSubmitted(a)":              "decided(a)",
        "ftpaSubmitted(b)":              "ftpaSubmitted(a)",
        "ftpaDecided":                   "ftpaSubmitted(b)",
        "ended":                         "ftpaDecided",
        "remitted":                      "ended"
    }

    return previous_state.get(state, None)


# Implement DQ checks based on previous state
def build_state_flow(state: str, flow: list):
    # Define the previous state for the given state
    previous_state = previous_state_map(state)
    if previous_state is None:
        return flow
    return build_state_flow(previous_state, [previous_state] + flow)


def build_dq_rules_dependencies(df_final, silver_m1, silver_m2, silver_m3, silver_m4, silver_m6, silver_c,
                                bronze_countries_postal_lookup_df, bronze_HORef_cleansing, bronze_remission_lookup_df,
                                bronze_interpreter_languages, bronze_listing_location):

    # Base inputs
    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    valid_representation = silver_m1.select(
        col("CaseNo"), col("dv_representation"), col("dv_CCDAppealType"), col("lu_appealType"), col("CasePrefix"),
        col("CaseRep_Address5"), col("CaseRep_Postcode"), col("MainRespondentId"), col("HORef"),
        col("Sponsor_Authorisation"), col("Sponsor_Name"), col("RepresentativeId"), col("lu_countryCode"), col("lu_appellantNationalitiesDescription")
    )
    valid_appealant_address = silver_m2.select(
        col("CaseNo"), col("Appellant_Address1"), col("Appellant_Address2"), col("Appellant_Address3"), col("Appellant_Address4"),
        col("Appellant_Address5"), col("Appellant_Postcode"), col("Appellant_Email"), col("Appellant_Telephone"), col("FCONumber"), col("Appellant_Name")
    ).filter(col("Relationship").isNull())
    valid_catagoryid_list = silver_c.groupBy("CaseNo").agg(collect_list("CategoryId").alias("valid_categoryIdList"))
    valid_country_list = bronze_countries_postal_lookup_df.select(
        col("countryGovUkOocAdminJ").alias("valid_countryGovUkOocAdminJ")
    ).distinct()
    valid_HORef_cleansing = bronze_HORef_cleansing.select(
        col("CaseNo"), coalesce(col("HORef"), col("FCONumber")).alias("lu_HORef")
    )
    valid_reasonDescription = (
        silver_m1.alias("m1")
            .join(bronze_remission_lookup_df, on=["PaymentRemissionReason", "PaymentRemissionRequested"], how="left")
            .select(
                "CaseNo", "VisitVisaType", "PaymentRemissionGranted", "ReasonDescription",
                col("remissionClaim").alias("lu_remissionClaim"), col("feeRemissionType").alias("lu_feeRemissionType")
            )
    )

    # appealSubmitted - payment and remission
    lu_ref_txn = (
        silver_m4.alias("m4").filter(col("TransactionTypeId").isin(6, 19)).distinct()
            .select("ReferringTransactionId")
            .where(col("ReferringTransactionId").isNotNull())
            .rdd.flatMap(lambda x: x)
            .collect()
    ) or []
    valid_payment_type = (
        silver_m1.alias("m1").join(silver_m4.alias("m4"), on=["CaseNo"])
            .groupBy("CaseNo").agg(
                collect_list(struct(
                    "m4.Amount", "m4.TransactionId", "m4.ReferringTransactionId", "m4.TransactionTypeId",
                    "m4.Status", "m4.SumBalance", "m4.SumTotalPay", "m4.SumTotalFee"
                )).alias("valid_transactionList")
            ).withColumn("lu_ref_txn", lit(lu_ref_txn).cast(ArrayType(LongType())))
    )

    # case under review and reason for appeal submitted - hearing response
    # cur_rfas_hearing_response_window_spec =  & ((col("CaseStatus").isin(37, 38)) | ((col("CaseStatus").eqNullSafe(26)) & (col("Outcome").eqNullSafe(0))))
    valid_caseStatus_cur_rfas = (
        silver_m3
            .filter((col("CaseStatus").isin(37, 38)) | ((col("CaseStatus").eqNullSafe(26)) & (col("Outcome").eqNullSafe(0))))
            .select(col("CaseNo"), col("CaseStatus").alias("hr_CaseStatus"), col("StatusId").alias("hr_StatusId"),
                    row_number().over(window_spec).alias("rn")).filter((col("rn") == 1))
            .drop("rn")
    )

    # listing - languages
    valid_interpreter_caseStatus = silver_m3.filter(
        (((col("CaseStatus").isin(37, 38)) & (col("Outcome").isin(0, 27, 37, 39, 40, 50))) | ((col("CaseStatus") == 26) & (col("Outcome").isin(40, 52))))
    ).select(
        col("CaseNo"), col("CaseStatus"), col("StatusId"), col("Outcome"), col("AdditionalLanguageId"), row_number().over(window_spec).alias("rn")
    ).filter(col("rn").eqNullSafe(1)).drop("rn")
    valid_interpreter_statusId = valid_interpreter_caseStatus.select(col("CaseNo"), col("CaseStatus").alias("lang_CaseStatus"), col("StatusId").alias("lang_StatusId"), col("Outcome").alias("lang_Outcome"))
    valid_hearing_requirements = silver_m1.select(col("CaseNo"), col("Interpreter"), col("CourtPreference"), col("InCamera"))
    valid_languages = (
        silver_m1.alias("m1")
            .join(valid_interpreter_caseStatus.alias("m3"), on="CaseNo", how="left")
            .join(bronze_interpreter_languages.alias("lu_language"), on=(col("m1.LanguageId") == col("lu_language.LanguageId")), how="left")
            .join(bronze_interpreter_languages.alias("lu_additional_language"), on=(col("m3.AdditionalLanguageId") == col("lu_additional_language.LanguageId")), how="left")
            .select(
                col("CaseNo"),
                col("m1.LanguageId").alias("LanguageId"),
                col("m3.AdditionalLanguageId").alias("AdditionalLanguageId"),
                col("lu_language.appellantInterpreterLanguageCategory").alias("valid_languageCategory"),
                col("lu_additional_language.appellantInterpreterLanguageCategory").alias("valid_additionalLanguageCategory"),
                col("lu_language.languageCode").alias("valid_languageCode"),
                col("lu_additional_language.languageCode").alias("valid_additionalLanguageCode"),
                col("lu_language.languageLabel").alias("valid_languageLabel"),
                col("lu_additional_language.languageLabel").alias("valid_additionalLanguageLabel"),
                col("lu_language.manualEntry").alias("valid_manualEntry"),
                col("lu_additional_language.manualEntry").alias("valid_additionalManualEntry"),
                col("lu_language.manualEntryDescription").alias("valid_manualEntryDescription"),
                col("lu_additional_language.manualEntryDescription").alias("valid_additionalManualEntryDescription")
            )
    )

    # prepare for hearing - hearing response
    df_m3_validation = (
        silver_m3
            .filter(col("CaseStatus").isin(37, 38))
            .withColumn("row_number", row_number().over(window_spec))
            .filter(col("row_number") == 1).drop("row_number")
            .select(
                "CaseNo", "HearingCentre", "TimeEstimate", "HearingDate", "HearingType", "CourtName", "ListType", "ListTypeId", "StartTime",
                "Judge1FT_Surname", "Judge2FT_Surname", "Judge3FT_Surname", "Judge1FT_Forenames", "Judge2FT_Forenames", "Judge3FT_Forenames",
                "Judge1FT_Title", "Judge2FT_Title", "Judge3FT_Title", "CourtClerk_Surname", "CourtClerk_Forenames", "CourtClerk_Title"
            )
    )

    valid_preparforhearing = (
        silver_m1.select("CaseNo")
            .join(df_m3_validation, on="CaseNo", how="left")
            .join(bronze_listing_location
                .select(col("ListedCentre"), col("locationCode"), col("locationLabel"), col("listCaseHearingCentre").alias("bronze_listCaseHearingCentre"), col("listCaseHearingCentreAddress").alias("bronze_listCaseHearingCentreAddress")),
                on=col("HearingCentre") == col("ListedCentre"), how="left")
            .drop("HearingCentre")
    )

    # decided(a) - hearing actuals
    valid_decided_outcome = (
        silver_m3.filter(col("CaseStatus").isin(37, 38, 26) & col("Outcome").isin(1, 2))
            .withColumn("row_number", row_number().over(window_spec))
            .filter(col("row_number") == 1)
            .select(col("CaseNo"), col("Outcome").alias("Outcome_SD"), col("HearingDuration"), col("Adj_Determination_Title"),
                    col("Adj_Determination_Forenames"), col("Adj_Determination_Surname"), col("DecisionDate"))
    )

    # ftpaSubmitted - ftpa
    valid_ftpa = (
        silver_m3.filter(col("CaseStatus").isin(39))
            .withColumn("row_number", row_number().over(window_spec))
            .filter(col("row_number") == 1)
            .select(col("CaseNo"), col("Party"), col("OutOfTime"), col("DateReceived"),
                    col("Adj_Title"), col("Adj_Forenames"), col("Adj_Surname"))
            .distinct()
    )

    #ftpa submitted
    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(37, 38))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    silver_m3_filtered_casestatus = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    valid_preparforhearing = (
        silver_m1.select("CaseNo")
            .join(df_m3_validation, on="CaseNo", how="left")
            .join(bronze_listing_location.select(col("ListedCentre"),col("locationCode"),col("locationLabel"),col("listCaseHearingCentre").alias("bronze_listCaseHearingCentre"),col("listCaseHearingCentreAddress").alias("bronze_listCaseHearingCentreAddress")), on=col("HearingCentre") == col("ListedCentre"), how="left")
            .drop("HearingCentre")
    )
    
    #ftpaDecided - outcome in 30,31,14. status in 39,46
    silver_m3_filtered_fptaDec = silver_m3.filter(col("CaseStatus").isin([39,46]) & col("Outcome").isin([30, 31, 14]))
    ftpaDecided_outcome = silver_m3_filtered_fptaDec.withColumn("row_number", row_number().over(window_spec))
    cs_39_46_outcome_14_30_31 = ftpaDecided_outcome.filter(col("row_number") == 1).select(col("CaseNo"), col("Outcome").alias("outcome_14_30_31_cs_39_46"), col("CaseStatus").alias("cs_39_46_outcome_30_31_14"))

    #ftpaDecided - status in 39
    silver_m3_filtered_cs39 = silver_m3.filter(col("CaseStatus") == 39)
    cs39_ranked = (silver_m3_filtered_cs39.withColumn("row_number", row_number().over(window_spec)))
    valid_cs39 = (cs39_ranked.filter(col("row_number") == 1).select(
                                                col("CaseNo"),
                                                col("CaseStatus").alias("dq_cs39_status"),
                                                col("Outcome").alias("dq_cs39_outcome"),
                                                col("Party").alias("dq_cs39_party")))
    
    #ftpaDecided - status in 39. outcome in 14, 30, 31
    silver_m3_filtered_cs39_out14_30_31 = silver_m3.filter(col("CaseStatus").isin([39]) & col("Outcome").isin([30, 31, 14]))
    silver_m3_filtered_cs39_out14_30_31_ranked = silver_m3_filtered_cs39_out14_30_31.withColumn("row_number", row_number().over(window_spec))
    cs39_out14_30_31_outcome = silver_m3_filtered_cs39_out14_30_31_ranked.filter(col("row_number") == 1
                                                ).select(col("CaseNo"), col("Outcome").alias("outcome_14_30_31_cs_39"), 
                                                col("CaseStatus").alias("cs_39_outcome_14_30_31"),
                                                col("Party").alias("cs39_party_14_30_31"))

    return (
        df_final
            .join(valid_representation, on="CaseNo", how="left")
            .join(valid_country_list, on=col("CaseRep_Address5") == col("valid_countryGovUkOocAdminJ"), how="left")
            .join(valid_catagoryid_list, on="CaseNo", how="left")
            .join(valid_appealant_address, on="CaseNo", how="left")
            .join(valid_HORef_cleansing, on="CaseNo", how="left")
            .join(valid_reasonDescription, on="CaseNo", how="left")
            .join(valid_payment_type, on="CaseNo", how="left")
            .join(valid_caseStatus_cur_rfas, on="CaseNo", how="left")
            .join(valid_interpreter_statusId, on="CaseNo", how="left")
            .join(valid_hearing_requirements, on="CaseNo", how="left")
            .join(valid_languages, on="CaseNo", how="left")
            .join(valid_preparforhearing, on="CaseNo", how="left")
            .join(valid_decided_outcome, on="CaseNo", how="left")
            .join(valid_ftpa, on="CaseNo", how="left")
            .join(cs_39_46_outcome_14_30_31, on="CaseNo", how="left")
            .join(valid_cs39, on="CaseNo", how="left")
            .join(cs39_out14_30_31_outcome, on="CaseNo", how="left")
    )


if __name__ == "__main__":
    pass
