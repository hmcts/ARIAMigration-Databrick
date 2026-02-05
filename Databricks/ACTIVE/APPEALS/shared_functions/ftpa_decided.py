from datetime import datetime
import re
import string
import pycountry
import pandas as pd
import json

from datetime import datetime
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
    collect_set, current_timestamp, transform, first, array_contains, rank, create_map, map_from_entries, map_from_arrays
)

################################################################
##########              ftpa          ###########
################################################################

def ftpa(silver_m3, silver_c):

    ftpa_df, ftpa_audit = FSB.ftpa(silver_m3, silver_c)

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(39))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

  
    outcome_text = (
        when(col("Outcome") == 30, lit("granted"))
        .when(col("Outcome") == 31, lit("refused"))
        .when(col("Outcome") == 14, lit("notAdmitted"))
        .otherwise(lit(None))
    )

    silver_m3_content = (
        silver_m3_max_statusid
            .withColumn("judgeAllocationExists", lit("Yes"))
            .withColumn("allocatedJudge", concat(col("Adj_Title"), lit(" "), col("Adj_Forenames"), lit(" "), col("Adj_Surname")))
            .withColumn("allocatedJudgeEdit", concat(col("Adj_Title"), lit(" "), col("Adj_Forenames"), lit(" "), col("Adj_Surname")))
            .withColumn(
                "ftpaApplicantType",
                when(col("Party") == 1, lit("appellant"))
                .when(col("Party") == 2, lit("respondent"))
                .otherwise(lit(None))
            )
            .withColumn("ftpaFirstDecision", outcome_text)
            .withColumn("ftpaFinalDecisionForDisplay", outcome_text)
            .withColumn("ftpaAppellantRJDecisionOutcomeType", outcome_text)
            .withColumn("ftpaRespondentRJDecisionOutcomeType", outcome_text)
            .withColumn(
                "ftpaAppellantDecisionDate",
                when(col("Party") == 1, col("DecisionDate")).otherwise(lit(None))
            )
            .withColumn(
                "ftpaRespondentDecisionDate",
                when(col("Party") == 2, col("DecisionDate")).otherwise(lit(None))
            )
            .withColumn(
                "isFtpaAppellantNoticeOfDecisionSetAside",
                when(col("Party") == 1, lit("Yes")).otherwise(lit("No"))
            )
            .withColumn(
                "isFtpaRespondentNoticeOfDecisionSetAside",
                when(col("Party") == 2, lit("Yes")).otherwise(lit("No"))
            )
            .select(
                col("CaseNo"),
                col("judgeAllocationExists"),
                col("allocatedJudge"),
                col("allocatedJudgeEdit"),
                col("ftpaApplicantType"),
                col("ftpaFirstDecision"),
                col("ftpaAppellantDecisionDate"),
                col("ftpaRespondentDecisionDate"),
                col("ftpaFinalDecisionForDisplay"),
                col("ftpaAppellantRJDecisionOutcomeType"),
                col("ftpaRespondentRJDecisionOutcomeType"),
                col("isFtpaAppellantNoticeOfDecisionSetAside"),
                col("isFtpaRespondentNoticeOfDecisionSetAside"),
            )
    )

    ftpa_df = (
        ftpa_df.alias("ftpa")
            .join(silver_m3_content.alias("m3"), on=["CaseNo"], how="left")
            .select(
                "ftpa.*",
                col("judgeAllocationExists"),
                col("allocatedJudge"),
                col("allocatedJudgeEdit"),
                col("ftpaApplicantType"),
                col("ftpaFirstDecision"),
                col("ftpaAppellantDecisionDate"),
                col("ftpaRespondentDecisionDate"),
                col("ftpaFinalDecisionForDisplay"),
                col("ftpaAppellantRJDecisionOutcomeType"),
                col("ftpaRespondentRJDecisionOutcomeType"),
                col("isFtpaAppellantNoticeOfDecisionSetAside"),
                col("isFtpaRespondentNoticeOfDecisionSetAside"),
            )
    )

    # Build the audit DataFrame (keeping your style + adding ONLY new fields)
    ftpa_audit = (
        ftpa_audit.alias("audit")
            .join(ftpa_df.alias("ftpa"), on=["CaseNo"], how="left")
            .join(silver_m3_max_statusid.alias("m3"), on=["CaseNo"], how="left")
            .select(
                "audit.*",
                array(struct(lit("judgeAllocationExists"))).alias("judgeAllocationExists_inputFields"),
                array(struct(lit("Null"))).alias("judgeAllocationExists_inputValues"),
                col("judgeAllocationExists").alias("judgeAllocationExists_value"),
                lit("Yes").alias("judgeAllocationExists_Transformation"),

                array(struct(lit("Adj_Title"), lit("Adj_Forenames"), lit("Adj_Surname"))).alias("allocatedJudge_inputFields"),
                array(struct(col("Adj_Title"), col("Adj_Forenames"), col("Adj_Surname"))).alias("allocatedJudge_inputValues"),
                col("allocatedJudge").alias("allocatedJudge_value"),
                lit("Yes").alias("allocatedJudge_Transformation"),

                array(struct(lit("Adj_Title"), lit("Adj_Forenames"), lit("Adj_Surname"))).alias("allocatedJudgeEdit_inputFields"),
                array(struct(col("Adj_Title"), col("Adj_Forenames"), col("Adj_Surname"))).alias("allocatedJudgeEdit_inputValues"),
                col("allocatedJudgeEdit").alias("allocatedJudgeEdite_value"),
                lit("Yes").alias("allocatedJudgeEdit_Transformation"),

                array(struct(lit("Party"))).alias("ftpaApplicantType_inputFields"),
                array(struct(col("Party"))).alias("ftpaApplicantType_inputValues"),
                col("ftpaApplicantType").alias("ftpaApplicantType_value"),
                lit("Yes").alias("ftpaApplicantType_Transformation"),

                array(struct(lit("Outcome"))).alias("ftpaFirstDecision_inputFields"),
                array(struct(col("Outcome"))).alias("ftpaFirstDecision_inputValues"),
                col("ftpaFirstDecision").alias("ftpaFirstDecision_value"),
                lit("Yes").alias("ftpaFirstDecision_Transformation"),

                array(struct(lit("DecisionDate"), lit("Party"))).alias("ftpaAppellantDecisionDate_inputFields"),
                array(struct(col("DecisionDate"), col("Party"))).alias("ftpaAppellantDecisionDate_inputValues"),
                col("ftpaAppellantDecisionDate").alias("ftpaAppellantDecisionDate_value"),
                lit("Yes").alias("ftpaAppellantDecisionDate_Transformation"),

                array(struct(lit("DecisionDate"), lit("Party"))).alias("ftpaRespondentDecisionDate_inputFields"),
                array(struct(col("DecisionDate"), col("Party"))).alias("ftpaRespondentDecisionDate_inputValues"),
                col("ftpaRespondentDecisionDate").alias("ftpaRespondentDecisionDate_value"),
                lit("Yes").alias("ftpaRespondentDecisionDate_Transformation"),

                array(struct(lit("Outcome"))).alias("ftpaFinalDecisionForDisplay_inputFields"),
                array(struct(col("Outcome"))).alias("ftpaFinalDecisionForDisplay_inputValues"),
                col("ftpaFinalDecisionForDisplay").alias("ftpaFinalDecisionForDisplay_value"),
                lit("Yes").alias("ftpaFinalDecisionForDisplay_Transformation"),

                array(struct(lit("Outcome"))).alias("ftpaAppellantRJDecisionOutcomeType_inputFields"),
                array(struct(col("Outcome"))).alias("ftpaAppellantRJDecisionOutcomeType_inputValues"),
                col("ftpaAppellantRJDecisionOutcomeType").alias("ftpaAppellantRJDecisionOutcomeType_value"),
                lit("Yes").alias("ftpaAppellantRJDecisionOutcomeType_Transformation"),

                array(struct(lit("Outcome"))).alias("ftpaRespondentRJDecisionOutcomeType_inputFields"),
                array(struct(col("Outcome"))).alias("ftpaRespondentRJDecisionOutcomeType_inputValues"),
                col("ftpaRespondentRJDecisionOutcomeType").alias("ftpaRespondentRJDecisionOutcomeType_value"),
                lit("Yes").alias("ftpaRespondentRJDecisionOutcomeType_Transformation"),

                array(struct(lit("Party"))).alias("isFtpaAppellantNoticeOfDecisionSetAside_inputFields"),
                array(struct(col("Party"))).alias("isFtpaAppellantNoticeOfDecisionSetAside_inputValues"),
                col("isFtpaAppellantNoticeOfDecisionSetAside").alias("isFtpaAppellantNoticeOfDecisionSetAside_value"),
                lit("Yes").alias("isFtpaAppellantNoticeOfDecisionSetAside_Transformation"),

                array(struct(lit("Party"))).alias("isFtpaRespondentNoticeOfDecisionSetAside_inputFields"),
                array(struct(col("Party"))).alias("isFtpaRespondentNoticeOfDecisionSetAside_inputValues"),
                col("isFtpaRespondentNoticeOfDecisionSetAside").alias("isFtpaRespondentNoticeOfDecisionSetAside_value"),
                lit("Yes").alias("isFtpaRespondentNoticeOfDecisionSetAside_Transformation"),
            )
    )

    return ftpa_df, ftpa_audit

################################################################
if __name__ == "__main__":
    pass
