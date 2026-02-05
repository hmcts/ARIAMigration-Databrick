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
##########              ftpa          ###########
################################################################

def ftpa(silver_m3, silver_c):

    # Base FTPSUBMITTED(b) output (already contains judgeAllocationExists/allocatedJudge/etc)
    ftpa_df, ftpa_audit = FSB.ftpa(silver_m3, silver_c)

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(39))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    # Outcome mapping
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


    silver_m3_content = (
        silver_m3_max_statusid
            .withColumn(
                "ftpaApplicantType",
                when(col("Party") == 1, lit("appellant"))
                .when(col("Party") == 2, lit("respondent"))
                .otherwise(lit(None))
            )
            .withColumn("ftpaFirstDecision", outcome_type)
            .withColumn(
                "ftpaAppellantDecisionDate",
                when(col("Party") == 1, date_format(col("DecisionDate"), "dd/MM/yyyy")).otherwise(lit(None))
            )
            .withColumn(
                "ftpaRespondentDecisionDate",
                when(col("Party") == 2, date_format(col("DecisionDate"), "dd/MM/yyyy")).otherwise(lit(None))
            )
            .withColumn("ftpaFinalDecisionForDisplay", outcome_display)
            .withColumn(
                "ftpaAppellantRjDecisionOutcomeType",
                when(col("Party") == 1, outcome_type).otherwise(lit(None))
            )
            .withColumn(
                "ftpaRespondentRjDecisionOutcomeType",
                when(col("Party") == 2, outcome_type).otherwise(lit(None))
            )
         
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
                col("ftpaApplicantType"),
                col("ftpaFirstDecision"),
                col("ftpaAppellantDecisionDate"),
                col("ftpaRespondentDecisionDate"),
                col("ftpaFinalDecisionForDisplay"),
                col("ftpaAppellantRjDecisionOutcomeType"),
                col("ftpaRespondentRjDecisionOutcomeType"),
                col("isFtpaAppellantNoticeOfDecisionSetAside"),
                col("isFtpaRespondentNoticeOfDecisionSetAside"),
            )
    )

 
    joined = (
        ftpa_df.alias("ftpa")
            .join(silver_m3_content.alias("m3"), on=["CaseNo"], how="left")
    )

    ftpa_df = (
        joined.select(
            col("ftpa.*"),

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


    ftpa_audit = (
        ftpa_audit.alias("audit")
            .join(ftpa_df.alias("ftpa"), on=["CaseNo"], how="left")
            .join(silver_m3_max_statusid.alias("m3base"), on=["CaseNo"], how="left")
            .join(silver_m3_content.alias("m3"), on=["CaseNo"], how="left")
            .select(
                col("audit.*"),

                # --- NEW fields only ---
                array(struct(lit("Party"))).alias("ftpaApplicantType_inputFields"),
                array(struct(col("m3base.Party"))).alias("ftpaApplicantType_inputValues"),
                col("m3.ftpaApplicantType").alias("ftpaApplicantType_value"),
                lit("Yes").alias("ftpaApplicantType_Transformation"),

                array(struct(lit("Outcome"))).alias("ftpaFirstDecision_inputFields"),
                array(struct(col("m3base.Outcome"))).alias("ftpaFirstDecision_inputValues"),
                col("m3.ftpaFirstDecision").alias("ftpaFirstDecision_value"),
                lit("Yes").alias("ftpaFirstDecision_Transformation"),

                array(struct(lit("DecisionDate"), lit("Party"))).alias("ftpaAppellantDecisionDate_inputFields"),
                array(struct(col("m3base.DecisionDate"), col("m3base.Party"))).alias("ftpaAppellantDecisionDate_inputValues"),
                col("m3.ftpaAppellantDecisionDate").alias("ftpaAppellantDecisionDate_value"),
                lit("Yes").alias("ftpaAppellantDecisionDate_Transformation"),

                array(struct(lit("DecisionDate"), lit("Party"))).alias("ftpaRespondentDecisionDate_inputFields"),
                array(struct(col("m3base.DecisionDate"), col("m3base.Party"))).alias("ftpaRespondentDecisionDate_inputValues"),
                col("m3.ftpaRespondentDecisionDate").alias("ftpaRespondentDecisionDate_value"),
                lit("Yes").alias("ftpaRespondentDecisionDate_Transformation"),

                array(struct(lit("Outcome"))).alias("ftpaFinalDecisionForDisplay_inputFields"),
                array(struct(col("m3base.Outcome"))).alias("ftpaFinalDecisionForDisplay_inputValues"),
                col("m3.ftpaFinalDecisionForDisplay").alias("ftpaFinalDecisionForDisplay_value"),
                lit("Yes").alias("ftpaFinalDecisionForDisplay_Transformation"),

                array(struct(lit("Outcome"), lit("Party"))).alias("ftpaAppellantRjDecisionOutcomeType_inputFields"),
                array(struct(col("m3base.Outcome"), col("m3base.Party"))).alias("ftpaAppellantRjDecisionOutcomeType_inputValues"),
                col("m3.ftpaAppellantRjDecisionOutcomeType").alias("ftpaAppellantRjDecisionOutcomeType_value"),
                lit("Yes").alias("ftpaAppellantRjDecisionOutcomeType_Transformation"),

                array(struct(lit("Outcome"), lit("Party"))).alias("ftpaRespondentRjDecisionOutcomeType_inputFields"),
                array(struct(col("m3base.Outcome"), col("m3base.Party"))).alias("ftpaRespondentRjDecisionOutcomeType_inputValues"),
                col("m3.ftpaRespondentRjDecisionOutcomeType").alias("ftpaRespondentRjDecisionOutcomeType_value"),
                lit("Yes").alias("ftpaRespondentRjDecisionOutcomeType_Transformation"),

                array(struct(lit("Party"))).alias("isFtpaAppellantNoticeOfDecisionSetAside_inputFields"),
                array(struct(col("m3base.Party"))).alias("isFtpaAppellantNoticeOfDecisionSetAside_inputValues"),
                col("m3.isFtpaAppellantNoticeOfDecisionSetAside").alias("isFtpaAppellantNoticeOfDecisionSetAside_value"),
                lit("Yes").alias("isFtpaAppellantNoticeOfDecisionSetAside_Transformation"),

                array(struct(lit("Party"))).alias("isFtpaRespondentNoticeOfDecisionSetAside_inputFields"),
                array(struct(col("m3base.Party"))).alias("isFtpaRespondentNoticeOfDecisionSetAside_inputValues"),
                col("m3.isFtpaRespondentNoticeOfDecisionSetAside").alias("isFtpaRespondentNoticeOfDecisionSetAside_value"),
                lit("Yes").alias("isFtpaRespondentNoticeOfDecisionSetAside_Transformation"),
            )
    )

    return ftpa_df, ftpa_audit


if __name__ == "__main__":
    pass
