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
                col("ftpaAppellantRjDecisionOutcomeType"),
                col("ftpaRespondentRjDecisionOutcomeType"),
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
