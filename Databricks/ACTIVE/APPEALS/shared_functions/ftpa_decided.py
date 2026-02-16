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
    Also outputs source columns for validation/DQ:
        CaseStatus (int), Outcome (int), Party (int), DecisionDate
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
    m3_latest_cs39_outcome = (
        silver_m3
        .filter((col("CaseStatus") == 39) & (col("Outcome").isin([30, 31, 14])))
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # Safe DecisionDate parsing (string OR timestamp)
    decisiondate_ts = coalesce(
        F.to_timestamp(col("DecisionDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),  # +00:00
        F.to_timestamp(col("DecisionDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),    # Z or +01
        col("DecisionDate").cast("timestamp")                                 # already timestamp
    )

    # Outcome mapping (I/J)
    outcome_type = (
        when(col("Outcome") == 30, lit("granted"))
        .when(col("Outcome") == 31, lit("refused"))
        .when(col("Outcome") == 14, lit("notAdmitted"))
        .otherwise(lit(None))
    )

    outcome_display = (
        when(col("Outcome") == 30, lit("Granted"))
        .when(col("Outcome") == 31, lit("Refused"))
        .when(col("Outcome") == 14, lit("Not admitted"))
        .otherwise(lit(None))
    )

    # ------------------------------------------------------------
    # Decision/outcome-driven decided fields (cs39 + outcome in 30/31/14)
    # ------------------------------------------------------------
    decision_fields = (
        m3_latest_cs39_outcome
        .withColumn(
            "ftpaApplicantType",
            when(col("Party") == 1, lit("appellant"))
            .when(col("Party") == 2, lit("respondent"))
            .otherwise(lit(None))
        )
        .withColumn("ftpaFirstDecision", outcome_type)
        .withColumn("ftpaFinalDecisionForDisplay", outcome_display)

        # Dates are ISO 8601 date only (yyyy-MM-dd)
        .withColumn(
            "ftpaAppellantDecisionDate",
            when(col("Party") == 1, date_format(decisiondate_ts, "yyyy-MM-dd")).otherwise(lit(None))
        )
        .withColumn(
            "ftpaRespondentDecisionDate",
            when(col("Party") == 2, date_format(decisiondate_ts, "yyyy-MM-dd")).otherwise(lit(None))
        )

        .withColumn(
            "ftpaAppellantRjDecisionOutcomeType",
            when(col("Party") == 1, outcome_type).otherwise(lit(None))
        )
        .withColumn(
            "ftpaRespondentRjDecisionOutcomeType",
            when(col("Party") == 2, outcome_type).otherwise(lit(None))
        )
        .select(
            col("CaseNo"),
            col("ftpaApplicantType"),
            col("ftpaFirstDecision"),
            col("ftpaAppellantDecisionDate"),
            col("ftpaRespondentDecisionDate"),
            col("ftpaFinalDecisionForDisplay"),
            col("ftpaAppellantRjDecisionOutcomeType"),
            col("ftpaRespondentRjDecisionOutcomeType"),
        )
    )

    # ------------------------------------------------------------
    # Party-driven set-aside flags (cs39 only; NO outcome filter)
    # ------------------------------------------------------------
    set_aside_fields = (
        m3_latest_cs39
        .withColumn(
            "isFtpaAppellantNoticeOfDecisionSetAside",
            when(col("Party") == 1, lit("No")).otherwise(lit(None))
        )
        .withColumn(
            "isFtpaRespondentNoticeOfDecisionSetAside",
            when(col("Party") == 2, lit("No")).otherwise(lit(None))
        )
        .select(
            col("CaseNo"),
            col("isFtpaAppellantNoticeOfDecisionSetAside"),
            col("isFtpaRespondentNoticeOfDecisionSetAside"),
        )
    )

    # ------------------------------------------------------------
    # SOURCE FIELDS FOR VALIDATION / DQ TABLE
    # Ensure final output contains:
    #   CaseStatus (int), Outcome (int), Party (int), DecisionDate
    # Outcome/DecisionDate come from outcome-filtered row when present.
    # Party uses outcome-filtered Party if present else cs39 Party.
    # CaseStatus remains integer 39 from cs39 set.
    # ------------------------------------------------------------
    m3_source_fields = (
        m3_latest_cs39.alias("cs39")
        .join(
            m3_latest_cs39_outcome
            .select(
                col("CaseNo").alias("CaseNo"),
                col("Outcome").alias("Outcome_outcome"),
                col("DecisionDate").alias("DecisionDate_outcome"),
                col("Party").alias("Party_outcome"),
            )
            .alias("cs39o"),
            on="CaseNo",
            how="left",
        )
        .select(
            col("cs39.CaseNo").alias("CaseNo"),
            col("cs39.CaseStatus").cast("int").alias("CaseStatus"),
            col("cs39o.Outcome_outcome").cast("int").alias("Outcome"),
            coalesce(col("cs39o.Party_outcome"), col("cs39.Party")).cast("int").alias("Party"),
            coalesce(col("cs39o.DecisionDate_outcome"), col("cs39.DecisionDate")).alias("DecisionDate"),
        )
    )

    # Final decided content (merge all)
    silver_m3_content = (
        decision_fields
        .join(set_aside_fields, on="CaseNo", how="left")
        .join(m3_source_fields, on="CaseNo", how="left")  # ✅ added
    )

    # ------------------------------------------------------------
    # FALLBACK (UNIT TEST SAFE):
    # If base FSB.ftpa() returns 0 rows, return decided-only content.
    # ------------------------------------------------------------
    if ftpa_df.limit(1).count() == 0:
        return silver_m3_content, ftpa_audit.limit(0)

    # Join decided fields onto base ftpa_df
    ftpa_df = (
        ftpa_df.alias("ftpa")
        .join(silver_m3_content.alias("m3"), on=["CaseNo"], how="left")
        .select(
            col("ftpa.*"),

            # ✅ source columns for validation
            col("m3.CaseStatus").alias("CaseStatus"),
            col("m3.Outcome").alias("Outcome"),
            col("m3.Party").alias("Party"),
            col("m3.DecisionDate").alias("DecisionDate"),

            # decided derived fields
            col("m3.ftpaApplicantType").alias("ftpaApplicantType"),
            col("m3.ftpaFirstDecision").alias("ftpaFirstDecision"),
            col("m3.ftpaAppellantDecisionDate").alias("ftpaAppellantDecisionDate"),
            col("m3.ftpaRespondentDecisionDate").alias("ftpaRespondentDecisionDate"),
            col("m3.ftpaFinalDecisionForDisplay").alias("ftpaFinalDecisionForDisplay"),
            col("m3.ftpaAppellantRjDecisionOutcomeType").alias("ftpaAppellantRjDecisionOutcomeType"),
            col("m3.ftpaRespondentRjDecisionOutcomeType").alias("ftpaRespondentRjDecisionOutcomeType"),
            col("m3.isFtpaAppellantNoticeOfDecisionSetAside").alias("isFtpaAppellantNoticeOfDecisionSetAside"),
            col("m3.isFtpaRespondentNoticeOfDecisionSetAside").alias("isFtpaRespondentNoticeOfDecisionSetAside"),
        )
    )

    # ---------------------------
    # Audit for decided-only fields
    # ---------------------------
    # Use the correct "base" dataset for each field group:
    # - decision/outcome fields -> m3_latest_cs39_outcome
    # - set-aside flags         -> m3_latest_cs39
    ftpa_audit = (
        ftpa_audit.alias("audit")
        .join(ftpa_df.alias("ftpa"), on=["CaseNo"], how="left")
        .join(m3_latest_cs39_outcome.alias("m3d"), on=["CaseNo"], how="left")
        .join(m3_latest_cs39.alias("m3s"), on=["CaseNo"], how="left")
        .join(silver_m3_content.alias("m3"), on=["CaseNo"], how="left")
        .select(
            col("audit.*"),

            # ApplicantType (decision dataset)
            array(struct(lit("Party"))).alias("ftpaApplicantType_inputFields"),
            array(struct(col("m3d.Party"))).alias("ftpaApplicantType_inputValues"),
            col("m3.ftpaApplicantType").alias("ftpaApplicantType_value"),
            lit("Derived").alias("ftpaApplicantType_Transformation"),

            # FirstDecision (decision dataset)
            array(struct(lit("Outcome"))).alias("ftpaFirstDecision_inputFields"),
            array(struct(col("m3d.Outcome"))).alias("ftpaFirstDecision_inputValues"),
            col("m3.ftpaFirstDecision").alias("ftpaFirstDecision_value"),
            lit("Derived").alias("ftpaFirstDecision_Transformation"),

            # Decision dates (decision dataset)
            array(struct(lit("DecisionDate"), lit("Party"))).alias("ftpaAppellantDecisionDate_inputFields"),
            array(struct(col("m3d.DecisionDate"), col("m3d.Party"))).alias("ftpaAppellantDecisionDate_inputValues"),
            col("m3.ftpaAppellantDecisionDate").alias("ftpaAppellantDecisionDate_value"),
            lit("Derived").alias("ftpaAppellantDecisionDate_Transformation"),

            array(struct(lit("DecisionDate"), lit("Party"))).alias("ftpaRespondentDecisionDate_inputFields"),
            array(struct(col("m3d.DecisionDate"), col("m3d.Party"))).alias("ftpaRespondentDecisionDate_inputValues"),
            col("m3.ftpaRespondentDecisionDate").alias("ftpaRespondentDecisionDate_value"),
            lit("Derived").alias("ftpaRespondentDecisionDate_Transformation"),

            # FinalDecisionForDisplay (decision dataset)
            array(struct(lit("Outcome"))).alias("ftpaFinalDecisionForDisplay_inputFields"),
            array(struct(col("m3d.Outcome"))).alias("ftpaFinalDecisionForDisplay_inputValues"),
            col("m3.ftpaFinalDecisionForDisplay").alias("ftpaFinalDecisionForDisplay_value"),
            lit("Derived").alias("ftpaFinalDecisionForDisplay_Transformation"),

            # RJ outcome types (decision dataset)
            array(struct(lit("Outcome"), lit("Party"))).alias("ftpaAppellantRjDecisionOutcomeType_inputFields"),
            array(struct(col("m3d.Outcome"), col("m3d.Party"))).alias("ftpaAppellantRjDecisionOutcomeType_inputValues"),
            col("m3.ftpaAppellantRjDecisionOutcomeType").alias("ftpaAppellantRjDecisionOutcomeType_value"),
            lit("Derived").alias("ftpaAppellantRjDecisionOutcomeType_Transformation"),

            array(struct(lit("Outcome"), lit("Party"))).alias("ftpaRespondentRjDecisionOutcomeType_inputFields"),
            array(struct(col("m3d.Outcome"), col("m3d.Party"))).alias("ftpaRespondentRjDecisionOutcomeType_inputValues"),
            col("m3.ftpaRespondentRjDecisionOutcomeType").alias("ftpaRespondentRjDecisionOutcomeType_value"),
            lit("Derived").alias("ftpaRespondentRjDecisionOutcomeType_Transformation"),

            # Set-aside flags (cs39 dataset)
            array(struct(lit("Party"))).alias("isFtpaAppellantNoticeOfDecisionSetAside_inputFields"),
            array(struct(col("m3s.Party"))).alias("isFtpaAppellantNoticeOfDecisionSetAside_inputValues"),
            col("m3.isFtpaAppellantNoticeOfDecisionSetAside").alias("isFtpaAppellantNoticeOfDecisionSetAside_value"),
            lit("Derived").alias("isFtpaAppellantNoticeOfDecisionSetAside_Transformation"),

            array(struct(lit("Party"))).alias("isFtpaRespondentNoticeOfDecisionSetAside_inputFields"),
            array(struct(col("m3s.Party"))).alias("isFtpaRespondentNoticeOfDecisionSetAside_inputValues"),
            col("m3.isFtpaRespondentNoticeOfDecisionSetAside").alias("isFtpaRespondentNoticeOfDecisionSetAside_value"),
            lit("Derived").alias("isFtpaRespondentNoticeOfDecisionSetAside_Transformation"),
        )
    )

    return ftpa_df, ftpa_audit

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

################################################################
##########        documents (Document Field Group)    ###########
################################################################

def documents(silver_m1, silver_m3):

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


    df, df_audit = FSA.general(
        silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres
    )

    # We need latest status row per CaseNo to apply Party-based include/omit rules
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

    # Added ONLY the 7 new columns 
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

    # Added ONLY new audit columns
    df_audit = (
        df_audit.alias("audit")
        .join(m3_latest.alias("m3"), on=["CaseNo"], how="left")
        .select(
            col("audit.*"),

            # ApplicantType (decision dataset)
            array(struct(lit("Party"))).alias("ftpaApplicantType_inputFields"),
            array(struct(col("m3d.Party"))).alias("ftpaApplicantType_inputValues"),
            col("m3.ftpaApplicantType").alias("ftpaApplicantType_value"),
            lit("Derived").alias("ftpaApplicantType_Transformation"),

            # FirstDecision (decision dataset)
            array(struct(lit("Outcome"))).alias("ftpaFirstDecision_inputFields"),
            array(struct(col("m3d.Outcome"))).alias("ftpaFirstDecision_inputValues"),
            col("m3.ftpaFirstDecision").alias("ftpaFirstDecision_value"),
            lit("Derived").alias("ftpaFirstDecision_Transformation"),

            # Decision dates (decision dataset)
            array(struct(lit("DecisionDate"), lit("Party"))).alias("ftpaAppellantDecisionDate_inputFields"),
            array(struct(col("m3d.DecisionDate"), col("m3d.Party"))).alias("ftpaAppellantDecisionDate_inputValues"),
            col("m3.ftpaAppellantDecisionDate").alias("ftpaAppellantDecisionDate_value"),
            lit("Derived").alias("ftpaAppellantDecisionDate_Transformation"),

            array(struct(lit("DecisionDate"), lit("Party"))).alias("ftpaRespondentDecisionDate_inputFields"),
            array(struct(col("m3d.DecisionDate"), col("m3d.Party"))).alias("ftpaRespondentDecisionDate_inputValues"),
            col("m3.ftpaRespondentDecisionDate").alias("ftpaRespondentDecisionDate_value"),
            lit("Derived").alias("ftpaRespondentDecisionDate_Transformation"),

            # FinalDecisionForDisplay (decision dataset)
            array(struct(lit("Outcome"))).alias("ftpaFinalDecisionForDisplay_inputFields"),
            array(struct(col("m3d.Outcome"))).alias("ftpaFinalDecisionForDisplay_inputValues"),
            col("m3.ftpaFinalDecisionForDisplay").alias("ftpaFinalDecisionForDisplay_value"),
            lit("Derived").alias("ftpaFinalDecisionForDisplay_Transformation"),

            # RJ outcome types (decision dataset)
            array(struct(lit("Outcome"), lit("Party"))).alias("ftpaAppellantRjDecisionOutcomeType_inputFields"),
            array(struct(col("m3d.Outcome"), col("m3d.Party"))).alias("ftpaAppellantRjDecisionOutcomeType_inputValues"),
            col("m3.ftpaAppellantRjDecisionOutcomeType").alias("ftpaAppellantRjDecisionOutcomeType_value"),
            lit("Derived").alias("ftpaAppellantRjDecisionOutcomeType_Transformation"),

            array(struct(lit("Outcome"), lit("Party"))).alias("ftpaRespondentRjDecisionOutcomeType_inputFields"),
            array(struct(col("m3d.Outcome"), col("m3d.Party"))).alias("ftpaRespondentRjDecisionOutcomeType_inputValues"),
            col("m3.ftpaRespondentRjDecisionOutcomeType").alias("ftpaRespondentRjDecisionOutcomeType_value"),
            lit("Derived").alias("ftpaRespondentRjDecisionOutcomeType_Transformation"),

            # Set-aside flags (cs39 dataset)
            array(struct(lit("Party"))).alias("isFtpaAppellantNoticeOfDecisionSetAside_inputFields"),
            array(struct(col("m3s.Party"))).alias("isFtpaAppellantNoticeOfDecisionSetAside_inputValues"),
            col("m3.isFtpaAppellantNoticeOfDecisionSetAside").alias("isFtpaAppellantNoticeOfDecisionSetAside_value"),
            lit("Derived").alias("isFtpaAppellantNoticeOfDecisionSetAside_Transformation"),

            array(struct(lit("Party"))).alias("isFtpaRespondentNoticeOfDecisionSetAside_inputFields"),
            array(struct(col("m3s.Party"))).alias("isFtpaRespondentNoticeOfDecisionSetAside_inputValues"),
            col("m3.isFtpaRespondentNoticeOfDecisionSetAside").alias("isFtpaRespondentNoticeOfDecisionSetAside_value"),
            lit("Derived").alias("isFtpaRespondentNoticeOfDecisionSetAside_Transformation"),
        )
    )

    return ftpa_df, ftpa_audit


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
