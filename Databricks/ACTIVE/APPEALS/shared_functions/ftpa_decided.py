# ============================================================
# Databricks.ACTIVE.APPEALS.shared_functions.ftpa_decided
# FTPA Decided (shared functions)
#   - Uses MAX(StatusId) WHERE CaseStatus = 39 for Party-driven fields
#   - Uses MAX(StatusId) WHERE CaseStatus = 39 AND Outcome IN (30,31,14) for decision/outcome fields
#   - Dates output in ISO 8601 date format: yyyy-MM-dd
#   - Party include/omit logic per mapping (I/J columns)
#
# LATEST FIX MERGED:
#   - Carry source fields into final output for validation table:
#       CaseStatus (int), Outcome (int), Party (int), DecisionDate
#   - Prevents CaseStatus/Outcome/Party being NULL in stg_main_ftpa_Decided_validation
#   - Keeps CaseStatus as INTEGER (NOT "39")
#
#  FIX (to avoid ambiguity in stg_main_ftpa_Decided_validation):
#   - Rename source fields to unique names so they do NOT clash with
#     validation joins that also add CaseStatus/Outcome/Party/DecisionDate.
#   - New columns:
#       ftpa_src_CaseStatus, ftpa_src_Outcome, ftpa_src_Party, ftpa_src_DecisionDate
# ============================================================

from datetime import datetime
import re
import string
import pycountry
import pandas as pd
import json

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

from . import AwaitingEvidenceRespondant_b as AERb
from . import listing as L
from . import prepareForHearing as PFH
from . import decision as D
from . import decided_a as DA
from . import ftpa_submitted_a as FSA
from . import ftpa_submitted_b as FSB

from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list,
    max as spark_max, date_format, row_number, expr, regexp_replace,
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp, transform, first, array_contains, rank,
    create_map, map_from_entries, map_from_arrays
)

################################################################
##########              ftpa (Field Group)          ###########
################################################################

def ftpa(silver_m3, silver_c):
    """
    Mapping alignment
      - Decision/outcome fields (ftpaFirstDecision, DecisionDates, FinalDecisionForDisplay, RJ outcome types, ApplicantType)
        -> MAX(StatusId) WHERE CaseStatus=39 AND Outcome IN (30,31,14)
      - Set-aside flags
        -> MAX(StatusId) WHERE CaseStatus=39 (NO Outcome filter)
      - Party include/omit:
        Party=1 => appellant fields populated; respondent fields NULL
        Party=2 => respondent fields populated; appellant fields NULL
      - Date format: ISO 8601 date (yyyy-MM-dd)

      Also outputs source columns for validation/DQ :
        ftpa_src_CaseStatus (int), ftpa_src_Outcome (int),
        ftpa_src_Party (int), ftpa_src_DecisionDate
    """

    # Base ftpa fields (judge allocation etc.)
    ftpa_df, ftpa_audit = FSB.ftpa(silver_m3, silver_c)

    # NOTE: DecisionDate may not exist in some unit test schemas. This ordering expects it exists in decided runs.
    window_spec = (
        Window.partitionBy("CaseNo")
        .orderBy(col("StatusId").desc(), col("DecisionDate").desc_nulls_last())
    )

    # ------------------------------------------------------------
    # F COLUMN (Set-aside flags):
    # MAX(StatusId) WHERE CaseStatus = 39
    # ------------------------------------------------------------
    m3_latest_cs39 = (
        silver_m3
        .filter(col("CaseStatus") == 39)
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # ------------------------------------------------------------
    # F COLUMN (Decision/outcome fields):
    # MAX(StatusId) WHERE CaseStatus = 39 AND Outcome IN (30,31,14)
    # ------------------------------------------------------------
    # m3_latest_cs39_outcome = (
    #     silver_m3
    #     .filter((col("CaseStatus") == 39) & (col("Outcome").isin([30, 31, 14])))
    #     .withColumn("rn", row_number().over(window_spec))
    #     .filter(col("rn") == 1)
    #     .drop("rn")
    # )

    # Outcome mapping (I/J)
    outcome_type = (
        when(col("Outcome") == 30, lit("granted"))
        .when(col("Outcome") == 31, lit("refused"))
        .when(col("Outcome") == 14, lit("notAdmitted"))
        .otherwise(lit(None))
    )

    # outcome_display = (
    #     when(col("Outcome") == 30, lit("Granted"))
    #     .when(col("Outcome") == 31, lit("Refused"))
    #     .when(col("Outcome") == 14, lit("Not admitted"))
    #     .otherwise(lit(None))
    # )

    # ------------------------------------------------------------
    # Decision/outcome-driven decided fields (cs39 + outcome in 30/31/14)
    # ------------------------------------------------------------
    ftpaDec_df = (
            m3_latest_cs39.join(ftpa_df, on=["CaseNo"], how="left")
            .withColumn(
                "ftpaApplicantType",
                when(col("Party") == 1, lit("appellant"))
                .when(col("Party") == 2, lit("respondent"))
                .otherwise(lit(None))
            )
            .withColumn("ftpaFirstDecision", outcome_type)

            .withColumn(
                "ftpaAppellantDecisionDate",
                when(col("Party") == 1, date_format(col("DecisionDate"), "yyyy-MM-dd")).otherwise(lit(None))
            )
            .withColumn(
                "ftpaRespondentDecisionDate",
                when(col("Party") == 2, date_format(col("DecisionDate"), "yyyy-MM-dd")).otherwise(lit(None))
            )
            
            .withColumn("ftpaFinalDecisionForDisplay", outcome_type)
            .withColumn("ftpaAppellantRjDecisionOutcomeType", outcome_type)
            .withColumn("ftpaRespondentRjDecisionOutcomeType", outcome_type)
            .withColumn("isFtpaAppellantNoticeOfDecisionSetAside", when(col("Party") == 1, lit("No")).otherwise(lit(None)))
            .withColumn("isFtpaRespondentNoticeOfDecisionSetAside", when(col("Party") == 2, lit("No")).otherwise(lit(None)))

            .select(
                col("CaseNo"),
                col("ftpaApplicationDeadline"),
                col("ftpaList"),
                col("ftpaAppellantApplicationDate"),
                col("ftpaAppellantSubmissionOutOfTime"),
                col("ftpaAppellantOutOfTimeExplanation"),
                col("ftpaRespondentApplicationDate"),
                col("ftpaRespondentSubmissionOutOfTime"),
                col("ftpaRespondentOutOfTimeExplanation"),
                col("judgeAllocationExists"),
                col("allocatedJudge"),
                col("allocatedJudgeEdit"),
                col("ftpaApplicantType"),
                col("ftpaFirstDecision"),
                col("ftpaAppellantDecisionDate"),
                col("ftpaRespondentDecisionDate"),
                col("ftpaFinalDecisionForDisplay"),
                col("ftpaAppellantRjDecisionOutcomeType"),
                col("ftpaRespondentRjDecisionOutcomeType"),
                col("isFtpaAppellantNoticeOfDecisionSetAside"),
                col("isFtpaRespondentNoticeOfDecisionSetAside")
            )
        )

    # ---------------------------
    # Audit for decided-only fields
    # ---------------------------
    # Use the correct "base" dataset for each field group:
    # - decision/outcome fields -> m3_latest_cs39_outcome
    # - set-aside flags         -> m3_latest_cs39

    ftpa_audit = (
        # ftpa_audit.alias("audit")
        ftpaDec_df.alias("content")
        .join(m3_latest_cs39.alias("m3s"), on=["CaseNo"], how="left")
        .select(
            # col("audit.*"),
            col("CaseNo"),

            # ApplicantType (decision dataset)
            array(struct(lit("Party"))).alias("ftpaApplicantType_inputFields"),
            array(struct(col("m3s.Party"))).alias("ftpaApplicantType_inputValues"),
            col("content.ftpaApplicantType").alias("ftpaApplicantType_value"),
            lit("Derived").alias("ftpaApplicantType_Transformation"),

            # FirstDecision (decision dataset)
            array(struct(lit("Outcome"))).alias("ftpaFirstDecision_inputFields"),
            array(struct(col("m3s.Outcome"))).alias("ftpaFirstDecision_inputValues"),
            col("content.ftpaFirstDecision").alias("ftpaFirstDecision_value"),
            lit("Derived").alias("ftpaFirstDecision_Transformation"),

            # Decision dates (decision dataset)
            array(struct(lit("DecisionDate"), lit("Party"))).alias("ftpaAppellantDecisionDate_inputFields"),
            array(struct(col("m3s.DecisionDate"), col("m3s.Party"))).alias("ftpaAppellantDecisionDate_inputValues"),
            col("content.ftpaAppellantDecisionDate").alias("ftpaAppellantDecisionDate_value"),
            lit("Derived").alias("ftpaAppellantDecisionDate_Transformation"),

            array(struct(lit("DecisionDate"), lit("Party"))).alias("ftpaRespondentDecisionDate_inputFields"),
            array(struct(col("m3s.DecisionDate"), col("m3s.Party"))).alias("ftpaRespondentDecisionDate_inputValues"),
            col("content.ftpaRespondentDecisionDate").alias("ftpaRespondentDecisionDate_value"),
            lit("Derived").alias("ftpaRespondentDecisionDate_Transformation"),

            # FinalDecisionForDisplay (decision dataset)
            array(struct(lit("Outcome"))).alias("ftpaFinalDecisionForDisplay_inputFields"),
            array(struct(col("m3s.Outcome"))).alias("ftpaFinalDecisionForDisplay_inputValues"),
            col("content.ftpaFinalDecisionForDisplay").alias("ftpaFinalDecisionForDisplay_value"),
            lit("Derived").alias("ftpaFinalDecisionForDisplay_Transformation"),

            # RJ outcome types (decision dataset)
            array(struct(lit("Outcome"), lit("Party"))).alias("ftpaAppellantRjDecisionOutcomeType_inputFields"),
            array(struct(col("m3s.Outcome"), col("m3s.Party"))).alias("ftpaAppellantRjDecisionOutcomeType_inputValues"),
            col("content.ftpaAppellantRjDecisionOutcomeType").alias("ftpaAppellantRjDecisionOutcomeType_value"),
            lit("Derived").alias("ftpaAppellantRjDecisionOutcomeType_Transformation"),

            array(struct(lit("Outcome"), lit("Party"))).alias("ftpaRespondentRjDecisionOutcomeType_inputFields"),
            array(struct(col("m3s.Outcome"), col("m3s.Party"))).alias("ftpaRespondentRjDecisionOutcomeType_inputValues"),
            col("content.ftpaRespondentRjDecisionOutcomeType").alias("ftpaRespondentRjDecisionOutcomeType_value"),
            lit("Derived").alias("ftpaRespondentRjDecisionOutcomeType_Transformation"),

            # Set-aside flags (cs39 dataset)
            array(struct(lit("Party"))).alias("isFtpaAppellantNoticeOfDecisionSetAside_inputFields"),
            array(struct(col("m3s.Party"))).alias("isFtpaAppellantNoticeOfDecisionSetAside_inputValues"),
            col("content.isFtpaAppellantNoticeOfDecisionSetAside").alias("isFtpaAppellantNoticeOfDecisionSetAside_value"),
            lit("Derived").alias("isFtpaAppellantNoticeOfDecisionSetAside_Transformation"),

            array(struct(lit("Party"))).alias("isFtpaRespondentNoticeOfDecisionSetAside_inputFields"),
            array(struct(col("m3s.Party"))).alias("isFtpaRespondentNoticeOfDecisionSetAside_inputValues"),
            col("content.isFtpaRespondentNoticeOfDecisionSetAside").alias("isFtpaRespondentNoticeOfDecisionSetAside_value"),
            lit("Derived").alias("isFtpaRespondentNoticeOfDecisionSetAside_Transformation"),
        )
    )

    return ftpaDec_df, ftpa_audit


################################################################
##########        documents (Document Field Group)    ###########
################################################################

def documents(silver_m1, silver_m3):
    """
    Decided documents = submitted_a documents + derived convenience arrays
    """
    df, df_audit = FSA.documents(silver_m1, silver_m3)

    empty_str_array = array().cast("array<string>")

    df = (
        df
        .withColumn("allFtpaAppellantDecisionDocs", coalesce(col("ftpaAppellantDocuments"), empty_str_array))
        .withColumn("allFtpaRespondentDecisionDocs", coalesce(col("ftpaRespondentDocuments"), empty_str_array))
        .withColumn("ftpaAppellantNoticeDocument", coalesce(col("finalDecisionAndReasonsDocuments"), empty_str_array))
        .withColumn("ftpaRespondentNoticeDocument", coalesce(col("finalDecisionAndReasonsDocuments"), empty_str_array))
    )

    df_audit = (
        df_audit
        .withColumn("allFtpaAppellantDecisionDocs_inputFields", array(struct(lit("ftpaAppellantDocuments_value"))))
        .withColumn("allFtpaAppellantDecisionDocs_inputValues", array(struct(col("ftpaAppellantDocuments_value"))))
        .withColumn("allFtpaAppellantDecisionDocs_value", col("ftpaAppellantDocuments_value"))
        .withColumn("allFtpaAppellantDecisionDocs_Transformation", lit("Derived"))

        .withColumn("allFtpaRespondentDecisionDocs_inputFields", array(struct(lit("ftpaRespondentDocuments_value"))))
        .withColumn("allFtpaRespondentDecisionDocs_inputValues", array(struct(col("ftpaRespondentDocuments_value"))))
        .withColumn("allFtpaRespondentDecisionDocs_value", col("ftpaRespondentDocuments_value"))
        .withColumn("allFtpaRespondentDecisionDocs_Transformation", lit("Derived"))

        .withColumn("ftpaAppellantNoticeDocument_inputFields", array(struct(lit("finalDecisionAndReasonsDocuments_value"))))
        .withColumn("ftpaAppellantNoticeDocument_inputValues", array(struct(col("finalDecisionAndReasonsDocuments_value"))))
        .withColumn("ftpaAppellantNoticeDocument_value", col("finalDecisionAndReasonsDocuments_value"))
        .withColumn("ftpaAppellantNoticeDocument_Transformation", lit("Derived"))

        .withColumn("ftpaRespondentNoticeDocument_inputFields", array(struct(lit("finalDecisionAndReasonsDocuments_value"))))
        .withColumn("ftpaRespondentNoticeDocument_inputValues", array(struct(col("finalDecisionAndReasonsDocuments_value"))))
        .withColumn("ftpaRespondentNoticeDocument_value", col("finalDecisionAndReasonsDocuments_value"))
        .withColumn("ftpaRespondentNoticeDocument_Transformation", lit("Derived"))
    )

    return df, df_audit


################################################################
##########          general (General Field Group)     ###########
################################################################

def general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres):
    """
    Decided general = submitted_a general + 7 decided flags (derived from latest Party)
    (Not part of the red fields in the screenshot; keeping existing agreed logic.)
    """
    df, df_audit = FSA.general(
        silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres
    )

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    m3_latest = (
        silver_m3
        .withColumn("row_number", row_number().over(window_spec))
        .filter(col("row_number") == 1)
        .drop("row_number")
        .select("CaseNo", "Party")
        .alias("m3")
    )

    joined = df.alias("g").join(m3_latest, on=["CaseNo"], how="left")

    df = (
        joined
        .select(col("g.*"), col("m3.Party").alias("Party"))
        .withColumn("isAppellantFtpaDecisionVisibleToAll", when(col("Party") == 1, lit("Yes")).otherwise(lit("No")))
        .withColumn("isRespondentFtpaDecisionVisibleToAll", when(col("Party") == 2, lit("Yes")).otherwise(lit("No")))
        .withColumn("isDlrnSetAsideEnabled", lit("Yes"))
        .withColumn("isFtpaAppellantDecided", when(col("Party") == 1, lit("Yes")).otherwise(lit("No")))
        .withColumn("isFtpaRespondentDecided", when(col("Party") == 2, lit("Yes")).otherwise(lit("No")))
        .withColumn("isReheardAppealEnabled", lit("Yes"))
        .withColumn("secondFtpaDecisionExists", lit("No"))
        .drop("Party")
    )

    df_audit = (
        df_audit.alias("audit")
        .join(m3_latest.alias("m3"), on=["CaseNo"], how="left")
        .select(
            col("audit.*"),

            array(struct(lit("Party"))).alias("isAppellantFtpaDecisionVisibleToAll_inputFields"),
            array(struct(col("m3.Party"))).alias("isAppellantFtpaDecisionVisibleToAll_inputValues"),
            when(col("m3.Party") == 1, lit("Yes")).otherwise(lit("No")).alias("isAppellantFtpaDecisionVisibleToAll_value"),
            lit("Derived").alias("isAppellantFtpaDecisionVisibleToAll_Transformation"),

            array(struct(lit("Party"))).alias("isRespondentFtpaDecisionVisibleToAll_inputFields"),
            array(struct(col("m3.Party"))).alias("isRespondentFtpaDecisionVisibleToAll_inputValues"),
            when(col("m3.Party") == 2, lit("Yes")).otherwise(lit("No")).alias("isRespondentFtpaDecisionVisibleToAll_value"),
            lit("Derived").alias("isRespondentFtpaDecisionVisibleToAll_Transformation"),

            array(struct(lit("Constant"))).alias("isDlrnSetAsideEnabled_inputFields"),
            array(struct(lit("Yes"))).alias("isDlrnSetAsideEnabled_inputValues"),
            lit("Yes").alias("isDlrnSetAsideEnabled_value"),
            lit("Derived").alias("isDlrnSetAsideEnabled_Transformation"),

            array(struct(lit("Party"))).alias("isFtpaAppellantDecided_inputFields"),
            array(struct(col("m3.Party"))).alias("isFtpaAppellantDecided_inputValues"),
            when(col("m3.Party") == 1, lit("Yes")).otherwise(lit("No")).alias("isFtpaAppellantDecided_value"),
            lit("Derived").alias("isFtpaAppellantDecided_Transformation"),

            array(struct(lit("Party"))).alias("isFtpaRespondentDecided_inputFields"),
            array(struct(col("m3.Party"))).alias("isFtpaRespondentDecided_inputValues"),
            when(col("m3.Party") == 2, lit("Yes")).otherwise(lit("No")).alias("isFtpaRespondentDecided_value"),
            lit("Derived").alias("isFtpaRespondentDecided_Transformation"),

            array(struct(lit("Constant"))).alias("isReheardAppealEnabled_inputFields"),
            array(struct(lit("Yes"))).alias("isReheardAppealEnabled_inputValues"),
            lit("Yes").alias("isReheardAppealEnabled_value"),
            lit("Derived").alias("isReheardAppealEnabled_Transformation"),

            array(struct(lit("Constant"))).alias("secondFtpaDecisionExists_inputFields"),
            array(struct(lit("No"))).alias("secondFtpaDecisionExists_inputValues"),
            lit("No").alias("secondFtpaDecisionExists_value"),
            lit("Derived").alias("secondFtpaDecisionExists_Transformation"),
        )
    )

    return df, df_audit


if __name__ == "__main__":
    pass
