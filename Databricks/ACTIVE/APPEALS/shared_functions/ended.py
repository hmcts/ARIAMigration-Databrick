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

from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, regexp_replace,
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains,rank,create_map, map_from_entries, map_from_arrays
)


################################################################
##########              ended          ###########
################################################################

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def ended(silver_m3, bronze_ended_states):
    # 1) Normalize numeric types explicitly (in-place casts)
    df = (
        silver_m3
        .withColumn("CaseStatus", F.col("CaseStatus").cast("int"))
        .withColumn("Outcome", F.col("Outcome").cast("int"))
        .withColumn("StatusId", F.col("StatusId").cast("long"))
    )

    # 2) Build "exists in the same case" flag for sa.CaseStatus IN (10,51,52)
    has_10_51_52 = (
        df.groupBy("CaseNo")
          .agg(
              F.max(
                  F.when(F.col("CaseStatus").isin(10, 51, 52), F.lit(1)).otherwise(F.lit(0))
              ).alias("has_10_51_52_in_case")
          )
    )

    df_with_flag = df.join(has_10_51_52, on="CaseNo", how="left")

    # 3) Apply the full filter equivalent to your SQL WHERE
    cond = (
        (
            (F.col("CaseStatus") == 10) &
            F.col("Outcome").isin(80, 122, 25, 120, 2, 105, 13)
        ) |
        (
            # t.CaseStatus = 46 AND t.Outcome = 31 AND sa.CaseStatus IN (10,51,52)
            (F.col("CaseStatus") == 46) &
            (F.col("Outcome") == 31) &
            (F.col("has_10_51_52_in_case") == 1)
        ) |
        (
            (F.col("CaseStatus") == 26) &
            F.col("Outcome").isin(80, 13, 25)
        ) |
        (
            F.col("CaseStatus").isin(37, 38) &
            F.col("Outcome").isin(80, 13, 25, 72, 125)
        ) |
        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        ) |
        (
            (F.col("CaseStatus") == 51) &
            F.col("Outcome").isin(0, 94, 93)
        ) |
        (
            (F.col("CaseStatus") == 52) &
            F.col("Outcome").isin(91, 95)
        ) |
        (
            (F.col("CaseStatus") == 36) &
            F.col("Outcome").isin(1, 2, 25)
        )
    )

    filtered = df_with_flag.filter(cond)

    # 4) For each CaseNo, take the row with the MAX(StatusId) among the filtered rows
    w = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
    m3_net_df = (
        filtered
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn", "has_10_51_52_in_case")   # drop helper cols
    )

    # 5) Build decision_ts robustly and format end date
    silver_with_decision_ts = m3_net_df.withColumn(
        "decision_ts",
        F.coalesce(
            F.to_timestamp(F.col("DecisionDate")),  # already-ISO timestamps parse fine
            F.to_timestamp(F.col("DecisionDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("DecisionDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")
        )
    )

    # 6) Join with ended state and build final fields
    #    Assumes bronze_ended_states has int CaseStatus and Outcome too.
    ended_df = (
        silver_with_decision_ts.alias("m3")
        .join(bronze_ended_states.alias("es"), on=["CaseStatus", "Outcome"], how="left")
        .withColumn(
            "endAppealApproverType",
            F.when(F.col("CaseStatus") == 46, F.lit("Judge")).otherwise(F.lit("Case Worker"))
        )
        .withColumn(
            "endAppealApproverName",
            F.when(
                F.col("CaseStatus") == 46,
                F.concat(
                    F.col("Adj_Determination_Title"), F.lit(" "),
                    F.col("Adj_Determination_Forenames"), F.lit(" "),
                    F.col("Adj_Determination_Surname")
                )
            ).otherwise(F.lit("This is a migrated ARIA case"))
        )
        .withColumn("endAppealDate", F.date_format(F.col("decision_ts"), "dd/MM/yyyy"))
        .select(
            F.col("CaseNo"),
            F.col("es.endAppealOutcome"),
            F.col("es.endAppealOutcomeReason"),
            F.col("endAppealApproverType"),
            F.col("endAppealApproverName"),
            F.col("endAppealDate"),
            F.col("es.stateBeforeEndAppeal"),
        )
    )

    ended_audit = (
        ended_df.alias("content")
            .join(silver_with_decision_ts.alias("m3"), on="CaseNo", how="left")
            .join(bronze_ended_states.alias("es"), on=["CaseStatus", "Outcome"], how="left")
            .select(
                "content.CaseNo",
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"),lit("endAppealOutcome"))).alias("endAppealOutcome_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"),col("es.endAppealOutcome"))).alias("endAppealOutcome_inputValues"),
                col("content.endAppealOutcome").alias("endAppealOutcome_value"),
                lit("Yes").alias("endAppealOutcome_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"),lit("endAppealOutcomeReason"))).alias("endAppealOutcomeReason_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"),col("es.endAppealOutcomeReason"))).alias("endAppealOutcomeReason_inputValues"),
                col("content.endAppealOutcomeReason").alias("endAppealOutcomeReason_value"),
                lit("Yes").alias("endAppealOutcomeReason_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"),lit("endAppealApproverType"))).alias("endAppealApproverType_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"),lit("Null"))).alias("endAppealApproverType_inputValues"),
                col("endAppealApproverType").alias("endAppealApproverType_value"),
                lit("Yes").alias("endAppealApproverType_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"),lit("Adj_Determination_Title"),lit("Adj_Determination_Forenames"),lit("Adj_Determination_Surname"))).alias("endAppealApproverName_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"),col("Adj_Determination_Title"),col("Adj_Determination_Forenames"),col("Adj_Determination_Surname"))).alias("endAppealApproverName_inputValues"),
                col("endAppealApproverName").alias("endAppealApproverName_value"),
                lit("Yes").alias("endAppealApproverName_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"),lit("DecisionDate"))).alias("endAppealDate_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"),col("m3.DecisionDate"))).alias("endAppealDate_inputValues"),
                col("endAppealDate").alias("endAppealDate_value"),
                lit("Yes").alias("endAppealDate_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"),lit("stateBeforeEndAppeal"))).alias("stateBeforeEndAppeal_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"),col("es.stateBeforeEndAppeal"))).alias("stateBeforeEndAppeal_inputValues"),
                col("content.stateBeforeEndAppeal").alias("stateBeforeEndAppeal_value"),
                lit("Yes").alias("stateBeforeEndAppeal_Transformed"),
            )
    )

    return ended_df,ended_audit

################################################################
##########              documents          ###########
################################################################


def documents(silver_m1,silver_m3):

    documents_df, documents_audit = FSA.documents(silver_m1,silver_m3)

    df = (
        silver_m3
        .withColumn("CaseStatus", F.col("CaseStatus").cast("int"))
        .withColumn("Outcome", F.col("Outcome").cast("int"))
        .withColumn("StatusId", F.col("StatusId").cast("long"))
    )

    cond_state_2_3_4 = (
        (
            (F.col("CaseStatus") == 26) &
            (F.col("Outcome").isin(80, 25,13))
        ) |
        (
            # t.CaseStatus = 46 AND t.Outcome = 31 AND sa.CaseStatus IN (10,51,52)
            (F.col("CaseStatus").isin(37, 38)) &
            (F.col("Outcome").isin(80,13,25))
        ) |
        (
            (F.col("CaseStatus") == 38) &
            (F.col("Outcome") == 72)
        ) |

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    cond_state_3_4 = (
        (
            # t.CaseStatus = 46 AND t.Outcome = 31 AND sa.CaseStatus IN (10,51,52)
            (F.col("CaseStatus").isin(37, 38)) &
            (F.col("Outcome").isin(80,13,25))
        ) |
        (
            (F.col("CaseStatus") == 38) &
            (F.col("Outcome") == 72)
        ) |

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    cond_state_4 = (

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_filtered_state_2_3_4 = silver_m3.filter(cond_state_2_3_4)
    silver_m3_ranked_state_2_3_4 = silver_m3_filtered_state_2_3_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_2_3_4 = silver_m3_ranked_state_2_3_4.filter(col("row_number") == 1).drop("row_number")

    silver_m3_filtered_state_3_4 = silver_m3.filter(cond_state_3_4)
    silver_m3_ranked_state_3_4 = silver_m3_filtered_state_3_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_3_4 = silver_m3_ranked_state_3_4.filter(col("row_number") == 1).drop("row_number")

    silver_m3_filtered_state_4 = silver_m3.filter(cond_state_4)
    silver_m3_ranked_state_4 = silver_m3_filtered_state_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_4 = silver_m3_ranked_state_4.filter(col("row_number") == 1).drop("row_number")

    
    
    documents_df = (
        documents_df.alias("content")
        .join(silver_m3_max_statusid_state_2_3_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("respondentDocuments",when(cond_state_2_3_4, col("content.respondentDocuments")).otherwise(None))
        .select("content.*", "respondentDocuments")
    )

    documents_audit = (
        documents_df.alias("content")
        .join(silver_m3_max_statusid_state_2_3_4.alias("m3"), on="CaseNo", how="left")
        .select("content.CaseNo",
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("respondentDocuments_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("respondentDocuments_inputValues"),
                col("content.respondentDocuments").alias("respondentDocuments_value"),
                lit("Yes").alias("respondentDocuments_Transformed"),
                )
    )

    documents_df = (
        documents_df.alias("content")
        .join(silver_m3_max_statusid_state_3_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("hearingRequirements",when(cond_state_3_4, col("content.hearingRequirements")).otherwise(None))
        .select("content.*", "hearingRequirements")
    )

    documents_audit = (
        documents_df.alias("content")
        .join(documents_audit.alias("audit"), on="CaseNo", how="left")
        .join(silver_m3_max_statusid_state_3_4.alias("m3"), on="CaseNo", how="left")
        .select("audit.*",
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("hearingRequirements_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("hearingRequirements_inputValues"),
                col("content.hearingRequirements").alias("hearingRequirements_value"),
                lit("Yes").alias("hearingRequirements_Transformed"),
                )
    )

    documents_df = (
        documents_df.alias("content")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("hearingDocuments",when(cond_state_4, col("content.hearingDocuments")).otherwise(None))
        .withColumn("letterBundleDocuments",when(cond_state_4, col("content.letterBundleDocuments")).otherwise(None))
        .withColumn("caseBundles",when(cond_state_4, col("content.caseBundles")).otherwise(None))
        .withColumn("finalDecisionAndReasonsDocuments",when(cond_state_4, col("content.finalDecisionAndReasonsDocuments")).otherwise(None))
        .withColumn("ftpaAppellantDocuments",when(cond_state_4, col("content.ftpaAppellantDocuments")).otherwise(None))
        .withColumn("ftpaRespondentDocuments",when(cond_state_4, col("content.ftpaRespondentDocuments")).otherwise(None))
        .withColumn("ftpaAppellantGroundsDocuments",when(cond_state_4, col("content.ftpaAppellantGroundsDocuments")).otherwise(None))
        .withColumn("ftpaRespondentGroundsDocuments",when(cond_state_4, col("content.ftpaRespondentGroundsDocuments")).otherwise(None))
        .withColumn("ftpaAppellantEvidenceDocuments",when(cond_state_4, col("content.ftpaAppellantEvidenceDocuments")).otherwise(None))
        .withColumn("ftpaRespondentEvidenceDocuments",when(cond_state_4, col("content.ftpaRespondentEvidenceDocuments")).otherwise(None))
        .withColumn("ftpaAppellantOutOfTimeDocuments",when(cond_state_4, col("content.ftpaAppellantOutOfTimeDocuments")).otherwise(None))
        .withColumn("ftpaRespondentOutOfTimeDocuments",when(cond_state_4, col("content.ftpaRespondentOutOfTimeDocuments")).otherwise(None))
        .select("content.*"
                ,"hearingDocuments"
                ,"letterBundleDocuments"
                ,"caseBundles"
                ,"finalDecisionAndReasonsDocuments"
                ,"ftpaAppellantDocuments"
                ,"ftpaRespondentDocuments"
                ,"ftpaAppellantGroundsDocuments"
                ,"ftpaRespondentGroundsDocuments"
                ,"ftpaAppellantEvidenceDocuments"
                ,"ftpaRespondentEvidenceDocuments"
                ,"ftpaAppellantOutOfTimeDocuments"
                ,"ftpaRespondentOutOfTimeDocuments"
                )
    )

    documents_audit = (
        documents_df.alias("content")
        .join(documents_audit.alias("audit"), on="CaseNo", how="left")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .select("audit.*",
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("hearingDocuments_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("hearingDocuments_inputValues"),
                col("content.hearingDocuments").alias("hearingDocuments_value"),
                lit("Yes").alias("hearingDocuments_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("letterBundleDocuments_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("letterBundleDocuments_inputValues"),
                col("content.letterBundleDocuments").alias("letterBundleDocuments_value"),
                lit("Yes").alias("letterBundleDocuments_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("caseBundles_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("caseBundles_inputValues"),
                col("content.caseBundles").alias("caseBundles_value"),
                lit("Yes").alias("caseBundles_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("finalDecisionAndReasonsDocuments_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("finalDecisionAndReasonsDocuments_inputValues"),
                col("content.finalDecisionAndReasonsDocuments").alias("finalDecisionAndReasonsDocuments_value"),
                lit("Yes").alias("finalDecisionAndReasonsDocuments_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaAppellantDocuments_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaAppellantDocuments_inputValues"),
                col("content.ftpaAppellantDocuments").alias("ftpaAppellantDocuments_value"),
                lit("Yes").alias("ftpaAppellantDocuments_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaRespondentDocuments_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaRespondentDocuments_inputValues"),
                col("content.ftpaRespondentDocuments").alias("ftpaRespondentDocuments_value"),
                lit("Yes").alias("ftpaRespondentDocuments_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaAppellantGroundsDocuments_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaAppellantGroundsDocuments_inputValues"),
                col("content.ftpaAppellantGroundsDocuments").alias("ftpaAppellantGroundsDocuments_value"),
                lit("Yes").alias("ftpaAppellantGroundsDocuments_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaRespondentGroundsDocuments_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaRespondentGroundsDocuments_inputValues"),
                col("content.ftpaRespondentGroundsDocuments").alias("ftpaRespondentGroundsDocuments_value"),
                lit("Yes").alias("ftpaRespondentGroundsDocuments_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaAppellantEvidenceDocuments_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaAppellantEvidenceDocuments_inputValues"),
                col("content.ftpaAppellantEvidenceDocuments").alias("ftpaAppellantEvidenceDocuments_value"),
                lit("Yes").alias("ftpaAppellantEvidenceDocuments_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaRespondentEvidenceDocuments_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaRespondentEvidenceDocuments_inputValues"),
                col("content.ftpaRespondentEvidenceDocuments").alias("ftpaRespondentEvidenceDocuments_value"),
                lit("Yes").alias("ftpaRespondentEvidenceDocuments_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaAppellantOutOfTimeDocuments_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaAppellantOutOfTimeDocuments_inputValues"),
                col("content.ftpaAppellantOutOfTimeDocuments").alias("ftpaAppellantOutOfTimeDocuments_value"),
                lit("Yes").alias("ftpaAppellantOutOfTimeDocuments_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaRespondentOutOfTimeDocuments_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaRespondentOutOfTimeDocuments_inputValues"),
                col("content.ftpaRespondentOutOfTimeDocuments").alias("ftpaRespondentOutOfTimeDocuments_value"),
                lit("Yes").alias("ftpaRespondentOutOfTimeDocuments_Transformed"),

                )
    )


    return documents_df, documents_audit

################################################################
##########              hearingRequirements          ###########
################################################################

def hearingRequirements(silver_m1, silver_m3, silver_c, bronze_interpreter_languages):
    hearingRequirements_df, hearingRequirements_audit = L.hearingRequirements(silver_m1, silver_m3, silver_c, bronze_interpreter_languages)

    df = (
        silver_m3
        .withColumn("CaseStatus", F.col("CaseStatus").cast("int"))
        .withColumn("Outcome", F.col("Outcome").cast("int"))
        .withColumn("StatusId", F.col("StatusId").cast("long"))
    )

    cond_state_3_4 = (
        (
            # t.CaseStatus = 46 AND t.Outcome = 31 AND sa.CaseStatus IN (10,51,52)
            (F.col("CaseStatus").isin(37, 38)) &
            (F.col("Outcome").isin(80,13,25))
        ) |
        (
            (F.col("CaseStatus") == 38) &
            (F.col("Outcome") == 72)
        ) |

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    silver_m3_filtered_state_3_4 = silver_m3.filter(cond_state_3_4)
    silver_m3_ranked_state_3_4 = silver_m3_filtered_state_3_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_3_4 = silver_m3_ranked_state_3_4.filter(col("row_number") == 1).drop("row_number")


    hearingRequirements_df = (
        hearingRequirements_df.alias("content")
        .join(silver_m3_max_statusid_state_3_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("isAppellantAttendingTheHearing",when(cond_state_3_4, col("content.isAppellantAttendingTheHearing")).otherwise(None))
        .withColumn("isAppellantGivingOralEvidence",when(cond_state_3_4, col("content.isAppellantGivingOralEvidence")).otherwise(None))
        .withColumn("isWitnessesAttending",when(cond_state_3_4, col("content.isWitnessesAttending")).otherwise(None))
        .withColumn("isEvidenceFromOutsideUkOoc",when(cond_state_3_4, col("content.isEvidenceFromOutsideUkOoc")).otherwise(None))
        .withColumn("isEvidenceFromOutsideUkInCountry",when(cond_state_3_4, col("content.isEvidenceFromOutsideUkInCountry")).otherwise(None))
        .withColumn("isInterpreterServicesNeeded",when(cond_state_3_4, col("content.isInterpreterServicesNeeded")).otherwise(None))
        .withColumn("appellantInterpreterLanguageCategory",when(cond_state_3_4, col("content.appellantInterpreterLanguageCategory")).otherwise(None))
        .withColumn("appellantInterpreterSpokenLanguage",when(cond_state_3_4, col("content.appellantInterpreterSpokenLanguage")).otherwise(None))
        .withColumn("appellantInterpreterSignLanguage",when(cond_state_3_4, col("content.appellantInterpreterSignLanguage")).otherwise(None))
        .withColumn("isHearingRoomNeeded",when(cond_state_3_4, col("content.isHearingRoomNeeded")).otherwise(None))
        .withColumn("isHearingLoopNeeded",when(cond_state_3_4, col("content.isHearingLoopNeeded")).otherwise(None))
        .withColumn("remoteVideoCall",when(cond_state_3_4, col("content.remoteVideoCall")).otherwise(None))
        .withColumn("remoteVideoCallDescription",when(cond_state_3_4, col("content.remoteVideoCallDescription")).otherwise(None))
        .withColumn("physicalOrMentalHealthIssues",when(cond_state_3_4, col("content.physicalOrMentalHealthIssues")).otherwise(None))
        .withColumn("physicalOrMentalHealthIssuesDescription",when(cond_state_3_4, col("content.physicalOrMentalHealthIssuesDescription")).otherwise(None))
        .withColumn("pastExperiences",when(cond_state_3_4, col("content.pastExperiences")).otherwise(None))
        .withColumn("pastExperiencesDescription",when(cond_state_3_4, col("content.pastExperiencesDescription")).otherwise(None))
        .withColumn("multimediaEvidence",when(cond_state_3_4, col("content.multimediaEvidence")).otherwise(None))
        .withColumn("multimediaEvidenceDescription",when(cond_state_3_4, col("content.multimediaEvidenceDescription")).otherwise(None))
        .withColumn("singleSexCourt",when(cond_state_3_4, col("content.singleSexCourt")).otherwise(None))
        .withColumn("singleSexCourtType",when(cond_state_3_4, col("content.singleSexCourtType")).otherwise(None))
        .withColumn("singleSexCourtTypeDescription",when(cond_state_3_4, col("content.singleSexCourtTypeDescription")).otherwise(None))
        .withColumn("inCameraCourt",when(cond_state_3_4, col("content.inCameraCourt")).otherwise(None))
        .withColumn("inCameraCourtDescription",when(cond_state_3_4, col("content.inCameraCourtDescription")).otherwise(None))
        .withColumn("additionalRequests",when(cond_state_3_4, col("content.additionalRequests")).otherwise(None))
        .withColumn("additionalRequestsDescription",when(cond_state_3_4, col("content.additionalRequestsDescription")).otherwise(None))
        .withColumn("datesToAvoidYesNo",when(cond_state_3_4, col("content.datesToAvoidYesNo")).otherwise(None))
        .select("content.*"
                ,"isAppellantAttendingTheHearing"
                ,"isAppellantGivingOralEvidence"
                ,"isWitnessesAttending"
                ,"isEvidenceFromOutsideUkOoc"
                ,"isEvidenceFromOutsideUkInCountry"
                ,"isInterpreterServicesNeeded"
                ,"appellantInterpreterLanguageCategory"
                ,"appellantInterpreterSpokenLanguage"
                ,"appellantInterpreterSignLanguage"
                ,"isHearingRoomNeeded"
                ,"isHearingLoopNeeded"
                ,"remoteVideoCall"
                ,"remoteVideoCallDescription"
                ,"physicalOrMentalHealthIssues"
                ,"physicalOrMentalHealthIssuesDescription"
                ,"pastExperiences"
                ,"pastExperiencesDescription"
                ,"multimediaEvidence"
                ,"multimediaEvidenceDescription"
                ,"singleSexCourt"
                ,"singleSexCourtType"
                ,"singleSexCourtTypeDescription"
                ,"inCameraCourt"
                ,"inCameraCourtDescription"
                ,"additionalRequests"
                ,"additionalRequestsDescription"
                ,"datesToAvoidYesNo"
                )
    )

    hearingRequirements_audit = (
        hearingRequirements_df.alias("content")
        .join(silver_m3_max_statusid_state_3_4.alias("m3"), on="CaseNo", how="left")
        .select("content.CaseNo",
                
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isAppellantAttendingTheHearing_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isAppellantAttendingTheHearing_inputValues"),
                col("content.isAppellantAttendingTheHearing").alias("isAppellantAttendingTheHearing_value"),
                lit("Yes").alias("isAppellantAttendingTheHearing_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isAppellantGivingOralEvidence_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isAppellantGivingOralEvidence_inputValues"),
                col("content.isAppellantGivingOralEvidence").alias("isAppellantGivingOralEvidence_value"),
                lit("Yes").alias("isAppellantGivingOralEvidence_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isWitnessesAttending_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isWitnessesAttending_inputValues"),
                col("content.isWitnessesAttending").alias("isWitnessesAttending_value"),
                lit("Yes").alias("isWitnessesAttending_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isEvidenceFromOutsideUkOoc_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isEvidenceFromOutsideUkOoc_inputValues"),
                col("content.isEvidenceFromOutsideUkOoc").alias("isEvidenceFromOutsideUkOoc_value"),
                lit("Yes").alias("isEvidenceFromOutsideUkOoc_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isEvidenceFromOutsideUkInCountry_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isEvidenceFromOutsideUkInCountry_inputValues"),
                col("content.isEvidenceFromOutsideUkInCountry").alias("isEvidenceFromOutsideUkInCountry_value"),
                lit("Yes").alias("isEvidenceFromOutsideUkInCountry_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isInterpreterServicesNeeded_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isInterpreterServicesNeeded_inputValues"),
                col("content.isInterpreterServicesNeeded").alias("isInterpreterServicesNeeded_value"),
                lit("Yes").alias("isInterpreterServicesNeeded_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("appellantInterpreterLanguageCategory_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("appellantInterpreterLanguageCategory_inputValues"),
                col("content.appellantInterpreterLanguageCategory").alias("appellantInterpreterLanguageCategory_value"),
                lit("Yes").alias("appellantInterpreterLanguageCategory_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("appellantInterpreterSpokenLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("appellantInterpreterSpokenLanguage_inputValues"),
                col("content.appellantInterpreterSpokenLanguage").alias("appellantInterpreterSpokenLanguage_value"),
                lit("Yes").alias("appellantInterpreterSpokenLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("appellantInterpreterSignLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("appellantInterpreterSignLanguage_inputValues"),
                col("content.appellantInterpreterSignLanguage").alias("appellantInterpreterSignLanguage_value"),
                lit("Yes").alias("appellantInterpreterSignLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isHearingRoomNeeded_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isHearingRoomNeeded_inputValues"),
                col("content.isHearingRoomNeeded").alias("isHearingRoomNeeded_value"),
                lit("Yes").alias("isHearingRoomNeeded_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isHearingLoopNeeded_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isHearingLoopNeeded_inputValues"),
                col("content.isHearingLoopNeeded").alias("isHearingLoopNeeded_value"),
                lit("Yes").alias("isHearingLoopNeeded_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("remoteVideoCall_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("remoteVideoCall_inputValues"),
                col("content.remoteVideoCall").alias("remoteVideoCall_value"),
                lit("Yes").alias("remoteVideoCall_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("remoteVideoCallDescription_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("remoteVideoCallDescription_inputValues"),
                col("content.remoteVideoCallDescription").alias("remoteVideoCallDescription_value"),
                lit("Yes").alias("remoteVideoCallDescription_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("physicalOrMentalHealthIssues_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("physicalOrMentalHealthIssues_inputValues"),
                col("content.physicalOrMentalHealthIssues").alias("physicalOrMentalHealthIssues_value"),
                lit("Yes").alias("physicalOrMentalHealthIssues_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("physicalOrMentalHealthIssuesDescription_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("physicalOrMentalHealthIssuesDescription_inputValues"),
                col("content.physicalOrMentalHealthIssuesDescription").alias("physicalOrMentalHealthIssuesDescription_value"),
                lit("Yes").alias("physicalOrMentalHealthIssuesDescription_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("pastExperiences_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("pastExperiences_inputValues"),
                col("content.pastExperiences").alias("pastExperiences_value"),
                lit("Yes").alias("pastExperiences_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("pastExperiencesDescription_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("pastExperiencesDescription_inputValues"),
                col("content.pastExperiencesDescription").alias("pastExperiencesDescription_value"),
                lit("Yes").alias("pastExperiencesDescription_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("multimediaEvidence_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("multimediaEvidence_inputValues"),
                col("content.multimediaEvidence").alias("multimediaEvidence_value"),
                lit("Yes").alias("multimediaEvidence_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("multimediaEvidenceDescription_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("multimediaEvidenceDescription_inputValues"),
                col("content.multimediaEvidenceDescription").alias("multimediaEvidenceDescription_value"),
                lit("Yes").alias("multimediaEvidenceDescription_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("singleSexCourt_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("singleSexCourt_inputValues"),
                col("content.singleSexCourt").alias("singleSexCourt_value"),
                lit("Yes").alias("singleSexCourt_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("singleSexCourtType_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("singleSexCourtType_inputValues"),
                col("content.singleSexCourtType").alias("singleSexCourtType_value"),
                lit("Yes").alias("singleSexCourtType_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("singleSexCourtTypeDescription_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("singleSexCourtTypeDescription_inputValues"),
                col("content.singleSexCourtTypeDescription").alias("singleSexCourtTypeDescription_value"),
                lit("Yes").alias("singleSexCourtTypeDescription_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("inCameraCourt_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("inCameraCourt_inputValues"),
                col("content.inCameraCourt").alias("inCameraCourt_value"),
                lit("Yes").alias("inCameraCourt_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("inCameraCourtDescription_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("inCameraCourtDescription_inputValues"),
                col("content.inCameraCourtDescription").alias("inCameraCourtDescription_value"),
                lit("Yes").alias("inCameraCourtDescription_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("additionalRequests_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("additionalRequests_inputValues"),
                col("content.additionalRequests").alias("additionalRequests_value"),
                lit("Yes").alias("additionalRequests_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("additionalRequestsDescription_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("additionalRequestsDescription_inputValues"),
                col("content.additionalRequestsDescription").alias("additionalRequestsDescription_value"),
                lit("Yes").alias("additionalRequestsDescription_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("datesToAvoidYesNo_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("datesToAvoidYesNo_inputValues"),
                col("content.datesToAvoidYesNo").alias("adatesToAvoidYesNo_value"),
                lit("Yes").alias("datesToAvoidYesNo_Transformed"),

                )
    )

    return hearingRequirements_df, hearingRequirements_audit


################################################################
##########              hearingResponse          ###########
################################################################

def hearingResponse(silver_m1, silver_m3, silver_m6):

    hearingResponse_df, hearingResponse_audit = PFH.hearingResponse(silver_m1, silver_m3, silver_m6)

    df = (
        silver_m3
        .withColumn("CaseStatus", F.col("CaseStatus").cast("int"))
        .withColumn("Outcome", F.col("Outcome").cast("int"))
        .withColumn("StatusId", F.col("StatusId").cast("long"))
    )

    cond_state_4 = (

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())


    silver_m3_filtered_state_4 = silver_m3.filter(cond_state_4)
    silver_m3_ranked_state_4 = silver_m3_filtered_state_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_4 = silver_m3_ranked_state_4.filter(col("row_number") == 1).drop("row_number")


    hearingResponse_df = (
        hearingResponse_df.alias("content")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("isRemoteHearing",when(cond_state_4, col("content.isRemoteHearing")).otherwise(None))
        .withColumn("isAppealSuitableToFloat",when(cond_state_4, col("content.isAppealSuitableToFloat")).otherwise(None))
        .withColumn("isMultimediaAllowed",when(cond_state_4, col("content.isMultimediaAllowed")).otherwise(None))
        .withColumn("multimediaTribunalResponse",when(cond_state_4, col("content.multimediaTribunalResponse")).otherwise(None))
        .withColumn("multimediaDecisionForDisplay",when(cond_state_4, col("content.multimediaDecisionForDisplay")).otherwise(None))
        .withColumn("isInCameraCourtAllowed",when(cond_state_4, col("content.isInCameraCourtAllowed")).otherwise(None))
        .withColumn("inCameraCourtTribunalResponse",when(cond_state_4, col("content.inCameraCourtTribunalResponse")).otherwise(None))
        .withColumn("inCameraCourtDecisionForDisplay",when(cond_state_4, col("content.inCameraCourtDecisionForDisplay")).otherwise(None))
        .withColumn("isSingleSexCourtAllowed",when(cond_state_4, col("content.isSingleSexCourtAllowed")).otherwise(None))
        .withColumn("singleSexCourtTribunalResponse",when(cond_state_4, col("content.singleSexCourtTribunalResponse")).otherwise(None))
        .withColumn("singleSexCourtDecisionForDisplay",when(cond_state_4, col("content.singleSexCourtDecisionForDisplay")).otherwise(None))
        .withColumn("isVulnerabilitiesAllowed",when(cond_state_4, col("content.isVulnerabilitiesAllowed")).otherwise(None))
        .withColumn("vulnerabilitiesTribunalResponse",when(cond_state_4, col("content.vulnerabilitiesTribunalResponse")).otherwise(None))
        .withColumn("vulnerabilitiesDecisionForDisplay",when(cond_state_4, col("content.vulnerabilitiesDecisionForDisplay")).otherwise(None))
        .withColumn("isRemoteHearingAllowed",when(cond_state_4, col("content.isRemoteHearingAllowed")).otherwise(None))
        .withColumn("remoteVideoCallTribunalResponse",when(cond_state_4, col("content.remoteVideoCallTribunalResponse")).otherwise(None))
        .withColumn("remoteHearingDecisionForDisplay",when(cond_state_4, col("content.remoteHearingDecisionForDisplay")).otherwise(None))
        .withColumn("isAdditionalAdjustmentsAllowed",when(cond_state_4, col("content.isAdditionalAdjustmentsAllowed")).otherwise(None))
        .withColumn("additionalTribunalResponse",when(cond_state_4, col("content.additionalTribunalResponse")).otherwise(None))
        .withColumn("otherDecisionForDisplay",when(cond_state_4, col("content.otherDecisionForDisplay")).otherwise(None))
        .withColumn("isAdditionalInstructionAllowed",when(cond_state_4, col("content.isAdditionalInstructionAllowed")).otherwise(None))
        .withColumn("additionalInstructionsTribunalResponse",when(cond_state_4, col("content.additionalInstructionsTribunalResponse")).otherwise(None))
        .select("content.*"
                ,"isRemoteHearing"
                ,"isAppealSuitableToFloat"
                ,"isMultimediaAllowed"
                ,"multimediaTribunalResponse"
                ,"multimediaDecisionForDisplay"
                ,"isInCameraCourtAllowed"
                ,"inCameraCourtTribunalResponse"
                ,"inCameraCourtDecisionForDisplay"
                ,"isSingleSexCourtAllowed"
                ,"singleSexCourtTribunalResponse"
                ,"singleSexCourtDecisionForDisplay"
                ,"isVulnerabilitiesAllowed"
                ,"vulnerabilitiesTribunalResponse"
                ,"vulnerabilitiesDecisionForDisplay"
                ,"isRemoteHearingAllowed"
                ,"remoteVideoCallTribunalResponse"
                ,"remoteHearingDecisionForDisplay"
                ,"isAdditionalAdjustmentsAllowed"
                ,"additionalTribunalResponse"
                ,"otherDecisionForDisplay"
                ,"isAdditionalInstructionAllowed"
                ,"additionalInstructionsTribunalResponse"
                )
    )

    hearingResponse_audit = (
        hearingResponse_df.alias("content")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .select("content.CaseNo",
                
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isRemoteHearing_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isRemoteHearing_inputValues"),
                col("content.isRemoteHearing").alias("isRemoteHearing_value"),
                lit("Yes").alias("isRemoteHearing_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isAppealSuitableToFloat_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isAppealSuitableToFloat_inputValues"),
                col("content.isAppealSuitableToFloat").alias("isAppealSuitableToFloat_value"),
                lit("Yes").alias("isAppealSuitableToFloat_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isMultimediaAllowed_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isMultimediaAllowed_inputValues"),
                col("content.isMultimediaAllowed").alias("isMultimediaAllowed_value"),
                lit("Yes").alias("isMultimediaAllowed_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("multimediaTribunalResponse_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("multimediaTribunalResponse_inputValues"),
                col("content.multimediaTribunalResponse").alias("multimediaTribunalResponse_value"),
                lit("Yes").alias("multimediaTribunalResponse_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("multimediaDecisionForDisplay_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("multimediaDecisionForDisplay_inputValues"),
                col("content.multimediaDecisionForDisplay").alias("multimediaDecisionForDisplay_value"),
                lit("Yes").alias("multimediaDecisionForDisplay_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isInCameraCourtAllowed_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isInCameraCourtAllowed_inputValues"),
                col("content.isInCameraCourtAllowed").alias("isInCameraCourtAllowed_value"),
                lit("Yes").alias("isInCameraCourtAllowed_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("inCameraCourtTribunalResponse_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("inCameraCourtTribunalResponse_inputValues"),
                col("content.inCameraCourtTribunalResponse").alias("inCameraCourtTribunalResponse_value"),
                lit("Yes").alias("inCameraCourtTribunalResponse_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("inCameraCourtDecisionForDisplay_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("inCameraCourtDecisionForDisplay_inputValues"),
                col("content.inCameraCourtDecisionForDisplay").alias("inCameraCourtDecisionForDisplay_value"),
                lit("Yes").alias("inCameraCourtDecisionForDisplay_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isSingleSexCourtAllowed_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isSingleSexCourtAllowed_inputValues"),
                col("content.isSingleSexCourtAllowed").alias("isSingleSexCourtAllowed_value"),
                lit("Yes").alias("isSingleSexCourtAllowed_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("singleSexCourtTribunalResponse_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("singleSexCourtTribunalResponse_inputValues"),
                col("content.singleSexCourtTribunalResponse").alias("singleSexCourtTribunalResponse_value"),
                lit("Yes").alias("singleSexCourtTribunalResponse_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("singleSexCourtDecisionForDisplay_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("singleSexCourtDecisionForDisplay_inputValues"),
                col("content.singleSexCourtDecisionForDisplay").alias("singleSexCourtDecisionForDisplay_value"),
                lit("Yes").alias("singleSexCourtDecisionForDisplay_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isVulnerabilitiesAllowed_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isVulnerabilitiesAllowed_inputValues"),
                col("content.isVulnerabilitiesAllowed").alias("isVulnerabilitiesAllowed_value"),
                lit("Yes").alias("isVulnerabilitiesAllowed_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("vulnerabilitiesTribunalResponse_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("vulnerabilitiesTribunalResponse_inputValues"),
                col("content.vulnerabilitiesTribunalResponse").alias("vulnerabilitiesTribunalResponse_value"),
                lit("Yes").alias("vulnerabilitiesTribunalResponse_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("vulnerabilitiesDecisionForDisplay_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("vulnerabilitiesDecisionForDisplay_inputValues"),
                col("content.vulnerabilitiesDecisionForDisplay").alias("vulnerabilitiesDecisionForDisplay_value"),
                lit("Yes").alias("vulnerabilitiesDecisionForDisplay_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isRemoteHearingAllowed_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isRemoteHearingAllowed_inputValues"),
                col("content.isRemoteHearingAllowed").alias("isRemoteHearingAllowed_value"),
                lit("Yes").alias("isRemoteHearingAllowed_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("remoteVideoCallTribunalResponse_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("remoteVideoCallTribunalResponse_inputValues"),
                col("content.remoteVideoCallTribunalResponse").alias("remoteVideoCallTribunalResponse_value"),
                lit("Yes").alias("remoteVideoCallTribunalResponse_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("remoteHearingDecisionForDisplay_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("remoteHearingDecisionForDisplay_inputValues"),
                col("content.remoteHearingDecisionForDisplay").alias("remoteHearingDecisionForDisplay_value"),
                lit("Yes").alias("remoteHearingDecisionForDisplay_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isAdditionalAdjustmentsAllowed_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isAdditionalAdjustmentsAllowed_inputValues"),
                col("content.isAdditionalAdjustmentsAllowed").alias("isAdditionalAdjustmentsAllowed_value"),
                lit("Yes").alias("isAdditionalAdjustmentsAllowed_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("additionalTribunalResponse_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("additionalTribunalResponse_inputValues"),
                col("content.additionalTribunalResponse").alias("additionalTribunalResponse_value"),
                lit("Yes").alias("additionalTribunalResponse_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("otherDecisionForDisplay_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("otherDecisionForDisplay_inputValues"),
                col("content.otherDecisionForDisplay").alias("otherDecisionForDisplay_value"),
                lit("Yes").alias("otherDecisionForDisplay_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isAdditionalInstructionAllowed_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isAdditionalInstructionAllowed_inputValues"),
                col("content.isAdditionalInstructionAllowed").alias("isAdditionalInstructionAllowed_value"),
                lit("Yes").alias("isAdditionalInstructionAllowed_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("additionalInstructionsTribunalResponse_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("additionalInstructionsTribunalResponse_inputValues"),
                col("content.additionalInstructionsTribunalResponse").alias("additionalInstructionsTribunalResponse_value"),
                lit("Yes").alias("additionalInstructionsTribunalResponse_Transformed"),
        )
    )
    
    return hearingResponse_df, hearingResponse_audit

################################################################
##########              hearingDetails          ###########
################################################################



def hearingDetails(silver_m1,silver_m3,bronze_listing_location):

    hearingDetails_df, hearingDetails_audit = D.hearingDetails(silver_m1,silver_m3,bronze_listing_location)

    df = (
        silver_m3
        .withColumn("CaseStatus", F.col("CaseStatus").cast("int"))
        .withColumn("Outcome", F.col("Outcome").cast("int"))
        .withColumn("StatusId", F.col("StatusId").cast("long"))
    )

    cond_state_4 = (

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())


    silver_m3_filtered_state_4 = silver_m3.filter(cond_state_4)
    silver_m3_ranked_state_4 = silver_m3_filtered_state_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_4 = silver_m3_ranked_state_4.filter(col("row_number") == 1).drop("row_number")

    
    

    hearingDetails_df = (
        hearingDetails_df.alias("content")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("listingLength",when(cond_state_4, col("content.listingLength")).otherwise(None))
        .withColumn("hearingChannel",when(cond_state_4, col("content.hearingChannel")).otherwise(None))
        .withColumn("witnessDetails",when(cond_state_4, col("content.witnessDetails")).otherwise(None))
        .withColumn("listingLocation",when(cond_state_4, col("content.listingLocation")).otherwise(None))
        .withColumn("witness1InterpreterSignLanguage",when(cond_state_4, col("content.witness1InterpreterSignLanguage")).otherwise(None))
        .withColumn("witness2InterpreterSignLanguage",when(cond_state_4, col("content.witness2InterpreterSignLanguage")).otherwise(None))
        .withColumn("witness3InterpreterSignLanguage",when(cond_state_4, col("content.witness3InterpreterSignLanguage")).otherwise(None))
        .withColumn("witness4InterpreterSignLanguage",when(cond_state_4, col("content.witness4InterpreterSignLanguage")).otherwise(None))
        .withColumn("witness5InterpreterSignLanguage",when(cond_state_4, col("content.witness5InterpreterSignLanguage")).otherwise(None))
        .withColumn("witness6InterpreterSignLanguage",when(cond_state_4, col("content.witness6InterpreterSignLanguage")).otherwise(None))
        .withColumn("witness7InterpreterSignLanguage",when(cond_state_4, col("content.witness7InterpreterSignLanguage")).otherwise(None))
        .withColumn("witness8InterpreterSignLanguage",when(cond_state_4, col("content.witness8InterpreterSignLanguage")).otherwise(None))
        .withColumn("witness9InterpreterSignLanguage",when(cond_state_4, col("content.witness9InterpreterSignLanguage")).otherwise(None))
        .withColumn("witness10InterpreterSignLanguage",when(cond_state_4, col("content.witness10InterpreterSignLanguage")).otherwise(None))
        .withColumn("witness1InterpreterSpokenLanguage",when(cond_state_4, col("content.witness1InterpreterSpokenLanguage")).otherwise(None))
        .withColumn("witness2InterpreterSpokenLanguage",when(cond_state_4, col("content.witness2InterpreterSpokenLanguage")).otherwise(None))
        .withColumn("witness3InterpreterSpokenLanguage",when(cond_state_4, col("content.witness3InterpreterSpokenLanguage")).otherwise(None))
        .withColumn("witness4InterpreterSpokenLanguage",when(cond_state_4, col("content.witness4InterpreterSpokenLanguage")).otherwise(None))
        .withColumn("witness5InterpreterSpokenLanguage",when(cond_state_4, col("content.witness5InterpreterSpokenLanguage")).otherwise(None))
        .withColumn("witness6InterpreterSpokenLanguage",when(cond_state_4, col("content.witness6InterpreterSpokenLanguage")).otherwise(None))
        .withColumn("witness7InterpreterSpokenLanguage",when(cond_state_4, col("content.witness7InterpreterSpokenLanguage")).otherwise(None))
        .withColumn("witness8InterpreterSpokenLanguage",when(cond_state_4, col("content.witness8InterpreterSpokenLanguage")).otherwise(None))
        .withColumn("witness9InterpreterSpokenLanguage",when(cond_state_4, col("content.witness9InterpreterSpokenLanguage")).otherwise(None))
        .withColumn("witness10InterpreterSpokenLanguage",when(cond_state_4, col("content.witness10InterpreterSpokenLanguage")).otherwise(None))
        .withColumn("listCaseHearingLength",when(cond_state_4, col("content.listCaseHearingLength")).otherwise(None))
        .withColumn("listCaseHearingDate",when(cond_state_4, col("content.listCaseHearingDate")).otherwise(None))
        .withColumn("listCaseHearingCentre",when(cond_state_4, col("content.listCaseHearingCentre")).otherwise(None))
        .withColumn("listCaseHearingCentreAddress",when(cond_state_4, col("content.listCaseHearingCentreAddress")).otherwise(None))
        .select("content.*"
                ,"listingLength"
                ,"hearingChannel"
                ,"witnessDetails"
                ,"listingLocation"
                ,"witness1InterpreterSignLanguage"
                ,"witness2InterpreterSignLanguage"
                ,"witness3InterpreterSignLanguage"
                ,"witness4InterpreterSignLanguage"
                ,"witness5InterpreterSignLanguage"
                ,"witness6InterpreterSignLanguage"
                ,"witness7InterpreterSignLanguage"
                ,"witness8InterpreterSignLanguage"
                ,"witness9InterpreterSignLanguage"
                ,"witness10InterpreterSignLanguage"
                ,"witness1InterpreterSpokenLanguage"
                ,"witness2InterpreterSpokenLanguage"
                ,"witness3InterpreterSpokenLanguage"
                ,"witness4InterpreterSpokenLanguage"
                ,"witness5InterpreterSpokenLanguage"
                ,"witness6InterpreterSpokenLanguage"
                ,"witness7InterpreterSpokenLanguage"
                ,"witness8InterpreterSpokenLanguage"
                ,"witness9InterpreterSpokenLanguage"
                ,"witness10InterpreterSpokenLanguage"
                ,"listCaseHearingLength"
                ,"listCaseHearingDate"
                ,"listCaseHearingCentre"
                ,"listCaseHearingCentreAddress"
                )
    )

    hearingDetails_audit = (
        hearingDetails_df.alias("content")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .select("content.CaseNo",
                
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("listingLength_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("listingLength_inputValues"),
                col("content.listingLength").alias("listingLength_value"),
                lit("Yes").alias("listingLength_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("hearingChannel_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("hearingChannel_inputValues"),
                col("content.hearingChannel").alias("hearingChannel_value"),
                lit("Yes").alias("hearingChannel_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witnessDetails_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witnessDetails_inputValues"),
                col("content.witnessDetails").alias("witnessDetails_value"),
                lit("Yes").alias("witnessDetails_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("listingLocation_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("listingLocation_inputValues"),
                col("content.listingLocation").alias("listingLocation_value"),
                lit("Yes").alias("listingLocation_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness1InterpreterSignLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness1InterpreterSignLanguage_inputValues"),
                col("content.witness1InterpreterSignLanguage").alias("witness1InterpreterSignLanguage_value"),
                lit("Yes").alias("witness1InterpreterSignLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness2InterpreterSignLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness2InterpreterSignLanguage_inputValues"),
                col("content.witness2InterpreterSignLanguage").alias("witness2InterpreterSignLanguage_value"),
                lit("Yes").alias("witness2InterpreterSignLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness3InterpreterSignLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness3InterpreterSignLanguage_inputValues"),
                col("content.witness3InterpreterSignLanguage").alias("witness3InterpreterSignLanguage_value"),
                lit("Yes").alias("witness3InterpreterSignLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness4InterpreterSignLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness4InterpreterSignLanguage_inputValues"),
                col("content.witness4InterpreterSignLanguage").alias("witness4InterpreterSignLanguage_value"),
                lit("Yes").alias("witness4InterpreterSignLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness5InterpreterSignLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness5InterpreterSignLanguage_inputValues"),
                col("content.witness5InterpreterSignLanguage").alias("witness5InterpreterSignLanguage_value"),
                lit("Yes").alias("witness5InterpreterSignLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness6InterpreterSignLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness6InterpreterSignLanguage_inputValues"),
                col("content.witness6InterpreterSignLanguage").alias("witness6InterpreterSignLanguage_value"),
                lit("Yes").alias("witness6InterpreterSignLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness7InterpreterSignLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness7InterpreterSignLanguage_inputValues"),
                col("content.witness7InterpreterSignLanguage").alias("witness7InterpreterSignLanguage_value"),
                lit("Yes").alias("witness7InterpreterSignLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness8InterpreterSignLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness8InterpreterSignLanguage_inputValues"),
                col("content.witness8InterpreterSignLanguage").alias("witness8InterpreterSignLanguage_value"),
                lit("Yes").alias("witness8InterpreterSignLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness9InterpreterSignLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness9InterpreterSignLanguage_inputValues"),
                col("content.witness9InterpreterSignLanguage").alias("witness9InterpreterSignLanguage_value"),
                lit("Yes").alias("witness9InterpreterSignLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness10InterpreterSignLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness10InterpreterSignLanguage_inputValues"),
                col("content.witness10InterpreterSignLanguage").alias("witness10InterpreterSignLanguage_value"),
                lit("Yes").alias("witness10InterpreterSignLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness1InterpreterSpokenLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness1InterpreterSpokenLanguage_inputValues"),
                col("content.witness1InterpreterSpokenLanguage").alias("witness1InterpreterSpokenLanguage_value"),
                lit("Yes").alias("witness1InterpreterSpokenLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness2InterpreterSpokenLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness2InterpreterSpokenLanguage_inputValues"),
                col("content.witness2InterpreterSpokenLanguage").alias("witness2InterpreterSpokenLanguage_value"),
                lit("Yes").alias("witness2InterpreterSpokenLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness3InterpreterSpokenLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness3InterpreterSpokenLanguage_inputValues"),
                col("content.witness3InterpreterSpokenLanguage").alias("witness3InterpreterSpokenLanguage_value"),
                lit("Yes").alias("witness3InterpreterSpokenLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness4InterpreterSpokenLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness4InterpreterSpokenLanguage_inputValues"),
                col("content.witness4InterpreterSpokenLanguage").alias("witness4InterpreterSpokenLanguage_value"),
                lit("Yes").alias("witness4InterpreterSpokenLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness5InterpreterSpokenLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness5InterpreterSpokenLanguage_inputValues"),
                col("content.witness5InterpreterSpokenLanguage").alias("witness5InterpreterSpokenLanguage_value"),
                lit("Yes").alias("witness5InterpreterSpokenLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness6InterpreterSpokenLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness6InterpreterSpokenLanguage_inputValues"),
                col("content.witness6InterpreterSpokenLanguage").alias("witness6InterpreterSpokenLanguage_value"),
                lit("Yes").alias("witness6InterpreterSpokenLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness7InterpreterSpokenLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness7InterpreterSpokenLanguage_inputValues"),
                col("content.witness7InterpreterSpokenLanguage").alias("witness7InterpreterSpokenLanguage_value"),
                lit("Yes").alias("witness7InterpreterSpokenLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness8InterpreterSpokenLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness8InterpreterSpokenLanguage_inputValues"),
                col("content.witness8InterpreterSpokenLanguage").alias("witness8InterpreterSpokenLanguage_value"),
                lit("Yes").alias("witness8InterpreterSpokenLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness9InterpreterSpokenLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness9InterpreterSpokenLanguage_inputValues"),
                col("content.witness9InterpreterSpokenLanguage").alias("witness9InterpreterSpokenLanguage_value"),
                lit("Yes").alias("witness9InterpreterSpokenLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("witness10InterpreterSpokenLanguage_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("witness10InterpreterSpokenLanguage_inputValues"),
                col("content.witness10InterpreterSpokenLanguage").alias("witness10InterpreterSpokenLanguage_value"),
                lit("Yes").alias("witness10InterpreterSpokenLanguage_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("listCaseHearingLength_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("listCaseHearingLength_inputValues"),
                col("content.listCaseHearingLength").alias("listCaseHearingLength_value"),
                lit("Yes").alias("listCaseHearingLength_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("listCaseHearingDate_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("listCaseHearingDate_inputValues"),
                col("content.listCaseHearingDate").alias("listCaseHearingDate_value"),
                lit("Yes").alias("listCaseHearingDate_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("listCaseHearingCentre_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("listCaseHearingCentre_inputValues"),
                col("content.listCaseHearingCentre").alias("listCaseHearingCentre_value"),
                lit("Yes").alias("listCaseHearingCentre_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("listCaseHearingCentreAddress_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("listCaseHearingCentreAddress_inputValues"),
                col("content.listCaseHearingCentreAddress").alias("listCaseHearingCentreAddress_value"),
                lit("Yes").alias("listCaseHearingCentreAddress_Transformed"),
        )
    )

    return hearingDetails_df, hearingDetails_audit

################################################################
##########              substantiveDecision          ###########
################################################################


def substantiveDecision(silver_m1,silver_m3):

    substantiveDecision_df, substantiveDecision_audit = DA.substantiveDecision(silver_m1,silver_m3)

    df = (
        silver_m3
        .withColumn("CaseStatus", F.col("CaseStatus").cast("int"))
        .withColumn("Outcome", F.col("Outcome").cast("int"))
        .withColumn("StatusId", F.col("StatusId").cast("long"))
    )

    cond_state_4 = (

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    silver_m3_filtered_state_4 = silver_m3.filter(cond_state_4)
    silver_m3_ranked_state_4 = silver_m3_filtered_state_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_4 = silver_m3_ranked_state_4.filter(col("row_number") == 1).drop("row_number")

    
    substantiveDecision_df = (
        substantiveDecision_df.alias("content")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("scheduleOfIssuesAgreement",when(cond_state_4, col("content.scheduleOfIssuesAgreement")).otherwise(None))
        .withColumn("scheduleOfIssuesDisagreementDescription",when(cond_state_4, col("content.scheduleOfIssuesDisagreementDescription")).otherwise(None))
        .withColumn("immigrationHistoryAgreement",when(cond_state_4, col("content.immigrationHistoryAgreement")).otherwise(None))
        .withColumn("immigrationHistoryDisagreementDescription",when(cond_state_4, col("content.immigrationHistoryDisagreementDescription")).otherwise(None))
        .withColumn("sendDecisionsAndReasonsDate",when(cond_state_4, col("content.sendDecisionsAndReasonsDate")).otherwise(None))
        .withColumn("appealDate",when(cond_state_4, col("content.appealDate")).otherwise(None))
        .withColumn("appealDecision",when(cond_state_4, col("content.appealDecision")).otherwise(None))
        .withColumn("isDecisionAllowed",when(cond_state_4, col("content.isDecisionAllowed")).otherwise(None))
        .withColumn("anonymityOrder",when(cond_state_4, col("content.anonymityOrder")).otherwise(None))
        .select("content.*"
                ,"scheduleOfIssuesAgreement"
                ,"scheduleOfIssuesDisagreementDescription"
                ,"immigrationHistoryAgreement"
                ,"immigrationHistoryDisagreementDescription"
                ,"sendDecisionsAndReasonsDate"
                ,"appealDate"
                ,"appealDecision"
                ,"isDecisionAllowed"
                ,"anonymityOrder"
                )
    )

    substantiveDecision_audit = (
        substantiveDecision_df.alias("content")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .select("content.CaseNo",
                
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("scheduleOfIssuesAgreement_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("scheduleOfIssuesAgreement_inputValues"),
                col("content.scheduleOfIssuesAgreement").alias("scheduleOfIssuesAgreement_value"),
                lit("Yes").alias("scheduleOfIssuesAgreement_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("scheduleOfIssuesDisagreementDescription_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("scheduleOfIssuesDisagreementDescription_inputValues"),
                col("content.scheduleOfIssuesDisagreementDescription").alias("scheduleOfIssuesDisagreementDescription_value"),
                lit("Yes").alias("scheduleOfIssuesDisagreementDescription_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("immigrationHistoryAgreement_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("immigrationHistoryAgreement_inputValues"),
                col("content.immigrationHistoryAgreement").alias("immigrationHistoryAgreement_value"),
                lit("Yes").alias("immigrationHistoryAgreement_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("immigrationHistoryDisagreementDescription_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("immigrationHistoryDisagreementDescription_inputValues"),
                col("content.immigrationHistoryDisagreementDescription").alias("immigrationHistoryDisagreementDescription_value"),
                lit("Yes").alias("immigrationHistoryDisagreementDescription_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("sendDecisionsAndReasonsDate_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("sendDecisionsAndReasonsDate_inputValues"),
                col("content.sendDecisionsAndReasonsDate").alias("sendDecisionsAndReasonsDate_value"),
                lit("Yes").alias("sendDecisionsAndReasonsDate_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("appealDate_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("appealDate_inputValues"),
                col("content.appealDate").alias("appealDate_value"),
                lit("Yes").alias("appealDate_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("appealDecision_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("appealDecision_inputValues"),
                col("content.appealDecision").alias("appealDecision_value"),
                lit("Yes").alias("appealDecision_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isDecisionAllowed_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isDecisionAllowed_inputValues"),
                col("content.isDecisionAllowed").alias("isDecisionAllowed_value"),
                lit("Yes").alias("isDecisionAllowed_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("anonymityOrder_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("anonymityOrder_inputValues"),
                col("content.anonymityOrder").alias("anonymityOrder_value"),
                lit("Yes").alias("anonymityOrder_Transformed"),
        )
    )

    return substantiveDecision_df, substantiveDecision_audit


################################################################
##########              hearingActuals          ###########
################################################################


def hearingActuals(silver_m3):

    hearingActuals_df, hearingActuals_audit = DA.hearingActuals(silver_m3)

    df = (
        silver_m3
        .withColumn("CaseStatus", F.col("CaseStatus").cast("int"))
        .withColumn("Outcome", F.col("Outcome").cast("int"))
        .withColumn("StatusId", F.col("StatusId").cast("long"))
    )

    cond_state_4 = (

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    silver_m3_filtered_state_4 = silver_m3.filter(cond_state_4)
    silver_m3_ranked_state_4 = silver_m3_filtered_state_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_4 = silver_m3_ranked_state_4.filter(col("row_number") == 1).drop("row_number")

    
    hearingActuals_df = (
        hearingActuals_df.alias("content")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("attendingJudge",when(cond_state_4, col("content.attendingJudge")).otherwise(None))
        .withColumn("actualCaseHearingLength",when(cond_state_4, col("content.actualCaseHearingLength")).otherwise(None))
        .select("content.*"
                ,"attendingJudge"
                ,"actualCaseHearingLength"
                )
    )

    hearingActuals_audit = (
        hearingActuals_df.alias("content")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .select("content.CaseNo",
                
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("attendingJudge_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("attendingJudge_inputValues"),
                col("content.attendingJudge").alias("attendingJudge_value"),
                lit("Yes").alias("attendingJudge_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("actualCaseHearingLength_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("actualCaseHearingLength_inputValues"),
                col("content.actualCaseHearingLength").alias("actualCaseHearingLength_value"),
                lit("Yes").alias("actualCaseHearingLength_Transformed"),
        )
    )

    return hearingActuals_df, hearingActuals_audit

################################################################
##########              ftpa          ###########
################################################################


def ftpa(silver_m3, silver_c):

    ftpa_df, ftpa_audit = FSA.ftpa(silver_m3, silver_c)

    df = (
        silver_m3
        .withColumn("CaseStatus", F.col("CaseStatus").cast("int"))
        .withColumn("Outcome", F.col("Outcome").cast("int"))
        .withColumn("StatusId", F.col("StatusId").cast("long"))
    )

    cond_state_4 = (

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    silver_m3_filtered_state_4 = silver_m3.filter(cond_state_4)
    silver_m3_ranked_state_4 = silver_m3_filtered_state_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_4 = silver_m3_ranked_state_4.filter(col("row_number") == 1).drop("row_number")

    
    ftpa_df = (
        ftpa_df.alias("content")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("ftpaApplicationDeadline",when(cond_state_4, col("content.ftpaApplicationDeadline")).otherwise(None))
        .withColumn("ftpaList",when(cond_state_4, col("content.ftpaList")).otherwise(None))
        .withColumn("ftpaAppellantApplicationDate",when(cond_state_4, col("content.ftpaAppellantApplicationDate")).otherwise(None))
        .withColumn("ftpaAppellantSubmissionOutOfTime",when(cond_state_4, col("content.ftpaAppellantSubmissionOutOfTime")).otherwise(None))
        .withColumn("ftpaAppellantOutOfTimeExplanation",when(cond_state_4, col("content.ftpaAppellantOutOfTimeExplanation")).otherwise(None))
        .withColumn("ftpaRespondentApplicationDate",when(cond_state_4, col("content.ftpaRespondentApplicationDate")).otherwise(None))
        .withColumn("ftpaRespondentSubmissionOutOfTime",when(cond_state_4, col("content.ftpaRespondentSubmissionOutOfTime")).otherwise(None))
        .withColumn("ftpaRespondentOutOfTimeExplanation",when(cond_state_4, col("content.ftpaRespondentOutOfTimeExplanation")).otherwise(None))
        .select("content.*"
                ,"ftpaApplicationDeadline"
                ,"ftpaList"
                ,"ftpaAppellantApplicationDate"
                ,"ftpaAppellantSubmissionOutOfTime"
                ,"ftpaAppellantOutOfTimeExplanation"
                ,"ftpaRespondentApplicationDate"
                ,"ftpaRespondentSubmissionOutOfTime"
                ,"ftpaRespondentOutOfTimeExplanation"
                )
    )

    ftpa_audit = (
        ftpa_df.alias("content")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .select("content.CaseNo",
                
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaApplicationDeadline_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaApplicationDeadline_inputValues"),
                col("content.ftpaApplicationDeadline").alias("ftpaApplicationDeadline_value"),
                lit("Yes").alias("ftpaApplicationDeadline_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaList_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaList_inputValues"),
                col("content.ftpaList").alias("ftpaList_value"),
                lit("Yes").alias("ftpaList_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaAppellantApplicationDate_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaAppellantApplicationDate_inputValues"),
                col("content.ftpaAppellantApplicationDate").alias("ftpaAppellantApplicationDate_value"),
                lit("Yes").alias("ftpaAppellantApplicationDate_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaAppellantSubmissionOutOfTime_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaAppellantSubmissionOutOfTime_inputValues"),
                col("content.ftpaAppellantSubmissionOutOfTime").alias("ftpaAppellantSubmissionOutOfTime_value"),
                lit("Yes").alias("ftpaAppellantSubmissionOutOfTime_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaAppellantOutOfTimeExplanation_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaAppellantOutOfTimeExplanation_inputValues"),
                col("content.ftpaAppellantOutOfTimeExplanation").alias("ftpaAppellantOutOfTimeExplanation_value"),
                lit("Yes").alias("ftpaAppellantOutOfTimeExplanation_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaRespondentApplicationDate_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaRespondentApplicationDate_inputValues"),
                col("content.ftpaRespondentApplicationDate").alias("ftpaRespondentApplicationDate_value"),
                lit("Yes").alias("ftpaRespondentApplicationDate_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaRespondentSubmissionOutOfTime_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaRespondentSubmissionOutOfTime_inputValues"),
                col("content.ftpaRespondentSubmissionOutOfTime").alias("ftpaRespondentSubmissionOutOfTime_value"),
                lit("Yes").alias("ftpaRespondentSubmissionOutOfTime_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaRespondentOutOfTimeExplanation_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaRespondentOutOfTimeExplanation_inputValues"),
                col("content.ftpaRespondentOutOfTimeExplanation").alias("ftpaRespondentOutOfTimeExplanation_value"),
                lit("Yes").alias("ftpaRespondentOutOfTimeExplanation_Transformed"),
        )
    )

    return ftpa_df, ftpa_audit


################################################################
##########              generalDefault          ###########
################################################################


def generalDefault(silver_m1,silver_m3):

    generalDefault_df = FSA.generalDefault(silver_m1)

    df = (
        silver_m3
        .withColumn("CaseStatus", F.col("CaseStatus").cast("int"))
        .withColumn("Outcome", F.col("Outcome").cast("int"))
        .withColumn("StatusId", F.col("StatusId").cast("long"))
    )

    cond_state_2_3_4 = (
        (
            (F.col("CaseStatus") == 26) &
            (F.col("Outcome").isin(80, 25,13))
        ) |
        (
            # t.CaseStatus = 46 AND t.Outcome = 31 AND sa.CaseStatus IN (10,51,52)
            (F.col("CaseStatus").isin(37, 38)) &
            (F.col("Outcome").isin(80,13,25))
        ) |
        (
            (F.col("CaseStatus") == 38) &
            (F.col("Outcome") == 72)
        ) |

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    cond_state_3_4 = (
        (
            # t.CaseStatus = 46 AND t.Outcome = 31 AND sa.CaseStatus IN (10,51,52)
            (F.col("CaseStatus").isin(37, 38)) &
            (F.col("Outcome").isin(80,13,25))
        ) |
        (
            (F.col("CaseStatus") == 38) &
            (F.col("Outcome") == 72)
        ) |

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    cond_state_4 = (

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    silver_m3_filtered_state_2_3_4 = silver_m3.filter(cond_state_2_3_4)
    silver_m3_ranked_state_2_3_4 = silver_m3_filtered_state_2_3_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_2_3_4 = silver_m3_ranked_state_2_3_4.filter(col("row_number") == 1).drop("row_number")

    silver_m3_filtered_state_3_4 = silver_m3.filter(cond_state_3_4)
    silver_m3_ranked_state_3_4 = silver_m3_filtered_state_3_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_3_4 = silver_m3_ranked_state_3_4.filter(col("row_number") == 1).drop("row_number")

    silver_m3_filtered_state_4 = silver_m3.filter(cond_state_4)
    silver_m3_ranked_state_4 = silver_m3_filtered_state_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_4 = silver_m3_ranked_state_4.filter(col("row_number") == 1).drop("row_number")

    
    
    generalDefault_df = (
        generalDefault_df.alias("content")
        .join(silver_m3_max_statusid_state_2_3_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("directions",when(cond_state_2_3_4, col("content.directions")).otherwise(None))
        .withColumn("uploadHomeOfficeBundleAvailable",when(cond_state_2_3_4, col("content.uploadHomeOfficeBundleAvailable")).otherwise(None))
        .withColumn("uploadHomeOfficeBundleActionAvailable",when(cond_state_2_3_4, col("content.uploadHomeOfficeBundleActionAvailable")).otherwise(None))
        # .withColumn("caseArgumentAvailable",when(cond_state_2_3_4, col("content.caseArgumentAvailable")).otherwise(None))
        # .withColumn("reasonsForAppealDecision",when(cond_state_2_3_4, col("content.reasonsForAppealDecision")).otherwise(None))
        .select("content.*"
                ,"directions"
                ,"uploadHomeOfficeBundleAvailable"
                ,"uploadHomeOfficeBundleActionAvailable"
                )
    )

    # generalDefault_audit = (
    #     generalDefault_df.alias("content")
    #     .join(silver_m3_max_statusid_state_2_3_4.alias("m3"), on="CaseNo", how="left")
    #     .select("content.CaseNo",
    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("directions_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("directions_inputValues"),
    #             col("content.directions").alias("directions_value"),
    #             lit("Yes").alias("directions_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("uploadHomeOfficeBundleAvailable_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("uploadHomeOfficeBundleAvailable_inputValues"),
    #             col("content.uploadHomeOfficeBundleAvailable").alias("uploadHomeOfficeBundleAvailable_value"),
    #             lit("Yes").alias("uploadHomeOfficeBundleAvailable_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("uploadHomeOfficeBundleActionAvailable_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("uploadHomeOfficeBundleActionAvailable_inputValues"),
    #             col("content.uploadHomeOfficeBundleActionAvailable").alias("uploadHomeOfficeBundleActionAvailable_value"),
    #             lit("Yes").alias("uploadHomeOfficeBundleActionAvailable_Transformed"),

    #     )
    # )

    generalDefault_df = (
        generalDefault_df.alias("content")
        .join(silver_m3_max_statusid_state_3_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("appealReviewOutcome",when(cond_state_3_4, col("content.appealReviewOutcome")).otherwise(None))
        .withColumn("appealResponseAvailable",when(cond_state_3_4, col("content.appealResponseAvailable")).otherwise(None))
        .withColumn("reviewedHearingRequirements",when(cond_state_3_4, col("content.reviewedHearingRequirements")).otherwise(None))
        .withColumn("amendResponseActionAvailable",when(cond_state_3_4, col("content.amendResponseActionAvailable")).otherwise(None))
        .withColumn("currentHearingDetailsVisible",when(cond_state_3_4, col("content.currentHearingDetailsVisible")).otherwise(None))
        .withColumn("reviewResponseActionAvailable",when(cond_state_3_4, col("content.reviewResponseActionAvailable")).otherwise(None))
        .withColumn("reviewHomeOfficeResponseByLegalRep",when(cond_state_3_4, col("content.reviewHomeOfficeResponseByLegalRep")).otherwise(None))
        .withColumn("submitHearingRequirementsAvailable",when(cond_state_3_4, col("content.submitHearingRequirementsAvailable")).otherwise(None))
        .withColumn("uploadHomeOfficeAppealResponseActionAvailable",when(cond_state_3_4, col("content.uploadHomeOfficeAppealResponseActionAvailable")).otherwise(None))
        .select("content.*"
                ,"appealReviewOutcome"
                ,"appealResponseAvailable"
                ,"reviewedHearingRequirements"
                ,"amendResponseActionAvailable"
                ,"currentHearingDetailsVisible"
                ,"reviewResponseActionAvailable"
                ,"reviewHomeOfficeResponseByLegalRep"
                ,"submitHearingRequirementsAvailable"
                ,"uploadHomeOfficeAppealResponseActionAvailable"
                )
    )

    # generalDefault_audit = (
    #     generalDefault_df.alias("content")
    #     .join(generalDefault_audit.alias("audit"), on="CaseNo", how="left")
    #     .join(silver_m3_max_statusid_state_3_4.alias("m3"), on="CaseNo", how="left")
    #     .select("audit.*",
    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("appealReviewOutcome_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("appealReviewOutcome_inputValues"),
    #             col("content.appealReviewOutcome").alias("appealReviewOutcome_value"),
    #             lit("Yes").alias("appealReviewOutcome_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("appealResponseAvailable_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("appealResponseAvailable_inputValues"),
    #             col("content.appealResponseAvailable").alias("appealResponseAvailable_value"),
    #             lit("Yes").alias("appealResponseAvailable_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("reviewedHearingRequirements_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("reviewedHearingRequirements_inputValues"),
    #             col("content.reviewedHearingRequirements").alias("reviewedHearingRequirements_value"),
    #             lit("Yes").alias("reviewedHearingRequirements_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("amendResponseActionAvailable_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("amendResponseActionAvailable_inputValues"),
    #             col("content.amendResponseActionAvailable").alias("amendResponseActionAvailable_value"),
    #             lit("Yes").alias("amendResponseActionAvailable_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("currentHearingDetailsVisible_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("currentHearingDetailsVisible_inputValues"),
    #             col("content.currentHearingDetailsVisible").alias("currentHearingDetailsVisible_value"),
    #             lit("Yes").alias("currentHearingDetailsVisible_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("reviewResponseActionAvailable_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("reviewResponseActionAvailable_inputValues"),
    #             col("content.reviewResponseActionAvailable").alias("reviewResponseActionAvailable_value"),
    #             lit("Yes").alias("reviewResponseActionAvailable_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("reviewHomeOfficeResponseByLegalRep_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("reviewHomeOfficeResponseByLegalRep_inputValues"),
    #             col("content.reviewHomeOfficeResponseByLegalRep").alias("reviewHomeOfficeResponseByLegalRep_value"),
    #             lit("Yes").alias("reviewHomeOfficeResponseByLegalRep_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("submitHearingRequirementsAvailable_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("submitHearingRequirementsAvailable_inputValues"),
    #             col("content.submitHearingRequirementsAvailable").alias("submitHearingRequirementsAvailable_value"),
    #             lit("Yes").alias("submitHearingRequirementsAvailable_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("uploadHomeOfficeAppealResponseActionAvailable_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("uploadHomeOfficeAppealResponseActionAvailable_inputValues"),
    #             col("content.uploadHomeOfficeAppealResponseActionAvailable").alias("uploadHomeOfficeAppealResponseActionAvailable_value"),
    #             lit("Yes").alias("uploadHomeOfficeAppealResponseActionAvailable_Transformed"),

    #             )
    # )

    generalDefault_df = (
        generalDefault_df.alias("content")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("hmcts",when(cond_state_4, col("content.hmcts")).otherwise(None))
        .withColumn("stitchingStatus",when(cond_state_4, col("content.stitchingStatus")).otherwise(None))
        .withColumn("bundleConfiguration",when(cond_state_4, col("content.bundleConfiguration")).otherwise(None))
        .withColumn("appealDecisionAvailable",when(cond_state_4, col("content.appealDecisionAvailable")).otherwise(None))
        .withColumn("isFtpaListVisible",when(cond_state_4, col("content.isFtpaListVisible")).otherwise(None))
        .select("content.*"
                ,"hmcts"
                ,"stitchingStatus"
                ,"bundleConfiguration"
                ,"appealDecisionAvailable"
                ,"isFtpaListVisible"
                )
    )

    # generalDefault_audit = (
    #     generalDefault_df.alias("content")
    #     .join(generalDefault_audit.alias("audit"), on="CaseNo", how="left")
    #     .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
    #     .select("audit.*",
    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("hmcts_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("hmcts_inputValues"),
    #             col("content.hmcts").alias("hmcts_value"),
    #             lit("Yes").alias("hmcts_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("stitchingStatus_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("stitchingStatus_inputValues"),
    #             col("content.stitchingStatus").alias("stitchingStatus_value"),
    #             lit("Yes").alias("stitchingStatus_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("bundleConfiguration_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("bundleConfiguration_inputValues"),
    #             col("content.bundleConfiguration").alias("bundleConfiguration_value"),
    #             lit("Yes").alias("bundleConfiguration_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("appealDecisionAvailable_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("appealDecisionAvailable_inputValues"),
    #             col("content.appealDecisionAvailable").alias("appealDecisionAvailable_value"),
    #             lit("Yes").alias("appealDecisionAvailable_Transformed"),

    #             array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaListVisible_inputFields"),
    #             array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaListVisible_inputValues"),
    #             col("content.isFtpaListVisible").alias("isFtpaListVisible_value"),
    #             lit("Yes").alias("isFtpaListVisible_Transformed"),

    #     )
    # )

    return generalDefault_df

################################################################
##########              general          ###########
################################################################


def general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres):

    general_df, general_audit = FSA.general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres)

    df = (
        silver_m3
        .withColumn("CaseStatus", F.col("CaseStatus").cast("int"))
        .withColumn("Outcome", F.col("Outcome").cast("int"))
        .withColumn("StatusId", F.col("StatusId").cast("long"))
    )

    cond_state_2_3_4 = (
        (
            (F.col("CaseStatus") == 26) &
            (F.col("Outcome").isin(80, 25,13))
        ) |
        (
            # t.CaseStatus = 46 AND t.Outcome = 31 AND sa.CaseStatus IN (10,51,52)
            (F.col("CaseStatus").isin(37, 38)) &
            (F.col("Outcome").isin(80,13,25))
        ) |
        (
            (F.col("CaseStatus") == 38) &
            (F.col("Outcome") == 72)
        ) |

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    cond_state_4 = (

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        )
    )

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_filtered_state_2_3_4 = silver_m3.filter(cond_state_2_3_4)
    silver_m3_ranked_state_2_3_4 = silver_m3_filtered_state_2_3_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_2_3_4 = silver_m3_ranked_state_2_3_4.filter(col("row_number") == 1).drop("row_number")

    silver_m3_filtered_state_4 = silver_m3.filter(cond_state_4)
    silver_m3_ranked_state_4 = silver_m3_filtered_state_4.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid_state_4 = silver_m3_ranked_state_4.filter(col("row_number") == 1).drop("row_number")

    general_df = ( general_df.alias("content") .join( silver_m3_max_statusid_state_2_3_4.alias("m3"), on="CaseNo", how="left" )
                  .withColumn("caseArgumentAvailable", when(cond_state_2_3_4, col("content.caseArgumentAvailable")).otherwise(None))
                  .withColumn("reasonsForAppealDecision", when(cond_state_2_3_4, col("content.reasonsForAppealDecision")).otherwise(None))
                  .select( "content.*"
                          ,"caseArgumentAvailable"
                          ,"reasonsForAppealDecision" 
                          )
                  )
    
    general_audit = (
        general_df.alias("content")
        .join(silver_m3_max_statusid_state_2_3_4.alias("m3"), on="CaseNo", how="left")
        .select("content.CaseNo",
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("caseArgumentAvailable_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("caseArgumentAvailable_inputValues"),
                col("content.caseArgumentAvailable").alias("caseArgumentAvailable_value"),
                lit("Yes").alias("caseArgumentAvailable_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("reasonsForAppealDecision_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("reasonsForAppealDecision_inputValues"),
                col("content.reasonsForAppealDecision").alias("reasonsForAppealDecision_value"),
                lit("Yes").alias("reasonsForAppealDecision_Transformed"),
                )
    )
    
    general_df = ( general_df.alias("content") .join( silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left" )
                  .withColumn("bundleFileNamePrefix", when(cond_state_4, col("content.bundleFileNamePrefix")).otherwise(None)) 
                  .withColumn("ftpaAppellantSubmitted", when(cond_state_4, col("content.ftpaAppellantSubmitted")).otherwise(None)) 
                  .withColumn("isFtpaAppellantDocsVisibleInDecided", when(cond_state_4, col("content.isFtpaAppellantDocsVisibleInDecided")).otherwise(None)) 
                  .withColumn("isFtpaAppellantDocsVisibleInSubmitted", when(cond_state_4, col("content.isFtpaAppellantDocsVisibleInSubmitted")).otherwise(None)) 
                  .withColumn("isFtpaAppellantOotDocsVisibleInDecided", when(cond_state_4, col("content.isFtpaAppellantOotDocsVisibleInDecided")).otherwise(None)) 
                  .withColumn("isFtpaAppellantOotDocsVisibleInSubmitted", when(cond_state_4, col("content.isFtpaAppellantOotDocsVisibleInSubmitted"))
                              .otherwise(None)) 
                  .withColumn("isFtpaAppellantGroundsDocsVisibleInDecided", when(cond_state_4, col("content.isFtpaAppellantGroundsDocsVisibleInDecided"))
                              .otherwise(None)) 
                  .withColumn("isFtpaAppellantEvidenceDocsVisibleInDecided", when(cond_state_4, col("content.isFtpaAppellantEvidenceDocsVisibleInDecided"))
                              .otherwise(None)) 
                  .withColumn("isFtpaAppellantGroundsDocsVisibleInSubmitted", when(cond_state_4, col("content.isFtpaAppellantGroundsDocsVisibleInSubmitted"))
                              .otherwise(None)) 
                  .withColumn("isFtpaAppellantEvidenceDocsVisibleInSubmitted", when(cond_state_4, col("content.isFtpaAppellantEvidenceDocsVisibleInSubmitted"))
                              .otherwise(None)) 
                  .withColumn("isFtpaAppellantOotExplanationVisibleInDecided", when(cond_state_4, col("content.isFtpaAppellantOotExplanationVisibleInDecided"))
                              .otherwise(None)) 
                  .withColumn("isFtpaAppellantOotExplanationVisibleInSubmitted", when(cond_state_4, col("content.isFtpaAppellantOotExplanationVisibleInSubmitted"))
                              .otherwise(None)) 
                  .withColumn("ftpaRespondentSubmitted", when(cond_state_4, col("content.ftpaRespondentSubmitted")).otherwise(None)) 
                  .withColumn("isFtpaRespondentDocsVisibleInDecided", when(cond_state_4, col("content.isFtpaRespondentDocsVisibleInDecided")).otherwise(None)) 
                  .withColumn("isFtpaRespondentDocsVisibleInSubmitted", when(cond_state_4, col("content.isFtpaRespondentDocsVisibleInSubmitted")).otherwise(None)) 
                  .withColumn("isFtpaRespondentOotDocsVisibleInDecided", when(cond_state_4, col("content.isFtpaRespondentOotDocsVisibleInDecided")).otherwise(None)) 
                  .withColumn("isFtpaRespondentOotDocsVisibleInSubmitted", when(cond_state_4, col("content.isFtpaRespondentOotDocsVisibleInSubmitted"))
                              .otherwise(None)) 
                  .withColumn("isFtpaRespondentGroundsDocsVisibleInDecided", when(cond_state_4, col("content.isFtpaRespondentGroundsDocsVisibleInDecided"))
                              .otherwise(None)) 
                  .withColumn("isFtpaRespondentEvidenceDocsVisibleInDecided", when(cond_state_4, col("content.isFtpaRespondentEvidenceDocsVisibleInDecided"))
                              .otherwise(None)) 
                  .withColumn("isFtpaRespondentGroundsDocsVisibleInSubmitted", when(cond_state_4, col("content.isFtpaRespondentGroundsDocsVisibleInSubmitted"))
                              .otherwise(None)) 
                  .withColumn("isFtpaRespondentEvidenceDocsVisibleInSubmitted", when(cond_state_4, col("content.isFtpaRespondentEvidenceDocsVisibleInSubmitted"))
                              .otherwise(None)) 
                  .withColumn("isFtpaRespondentOotExplanationVisibleInDecided", when(cond_state_4, col("content.isFtpaRespondentOotExplanationVisibleInDecided"))
                              .otherwise(None)) 
                  .withColumn("isFtpaRespondentOotExplanationVisibleInSubmitted", when(cond_state_4, col("content.isFtpaRespondentOotExplanationVisibleInSubmitted"))
                              .otherwise(None))
                  .select( "content.*"
                          ,"bundleFileNamePrefix"
                          ,"ftpaAppellantSubmitted"
                          ,"isFtpaAppellantDocsVisibleInDecided"
                          ,"isFtpaAppellantDocsVisibleInSubmitted"
                          ,"isFtpaAppellantOotDocsVisibleInDecided"
                          ,"isFtpaAppellantOotDocsVisibleInSubmitted"
                          ,"isFtpaAppellantGroundsDocsVisibleInDecided"
                          ,"isFtpaAppellantEvidenceDocsVisibleInDecided"
                          ,"isFtpaAppellantGroundsDocsVisibleInSubmitted"
                          ,"isFtpaAppellantEvidenceDocsVisibleInSubmitted"
                          ,"isFtpaAppellantOotExplanationVisibleInDecided"
                          ,"isFtpaAppellantOotExplanationVisibleInSubmitted"
                          ,"ftpaRespondentSubmitted"
                          ,"isFtpaRespondentDocsVisibleInDecided"
                          ,"isFtpaRespondentDocsVisibleInSubmitted"
                          ,"isFtpaRespondentOotDocsVisibleInDecided"
                          ,"isFtpaRespondentOotDocsVisibleInSubmitted"
                          ,"isFtpaRespondentGroundsDocsVisibleInDecided"
                          ,"isFtpaRespondentEvidenceDocsVisibleInDecided"
                          ,"isFtpaRespondentGroundsDocsVisibleInSubmitted"
                          ,"isFtpaRespondentEvidenceDocsVisibleInSubmitted"
                          ,"isFtpaRespondentOotExplanationVisibleInDecided"
                          ,"isFtpaRespondentOotExplanationVisibleInSubmitted"
                          ,"caseArgumentAvailable"
                          ) 
                  )
    
    general_audit = (
        general_df.alias("content")
        .join(general_audit.alias("audit"), on="CaseNo", how="left")
        .join(silver_m3_max_statusid_state_4.alias("m3"), on="CaseNo", how="left")
        .select("audit.*",
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("bundleFileNamePrefix_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("bundleFileNamePrefix_inputValues"),
                col("content.bundleFileNamePrefix").alias("bundleFileNamePrefix_value"),
                lit("Yes").alias("bundleFileNamePrefix_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaAppellantSubmitted_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaAppellantSubmitted_inputValues"),
                col("content.ftpaAppellantSubmitted").alias("ftpaAppellantSubmitted_value"),
                lit("Yes").alias("ftpaAppellantSubmitted_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaAppellantDocsVisibleInDecided_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaAppellantDocsVisibleInDecided_inputValues"),
                col("content.isFtpaAppellantDocsVisibleInDecided").alias("isFtpaAppellantDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaAppellantDocsVisibleInDecided_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaAppellantDocsVisibleInSubmitted_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaAppellantDocsVisibleInSubmitted_inputValues"),
                col("content.isFtpaAppellantDocsVisibleInSubmitted").alias("isFtpaAppellantDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaAppellantDocsVisibleInSubmitted_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaAppellantOotDocsVisibleInDecided_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaAppellantOotDocsVisibleInDecided_inputValues"),
                col("content.isFtpaAppellantOotDocsVisibleInDecided").alias("isFtpaAppellantOotDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaAppellantOotDocsVisibleInDecided_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaAppellantOotDocsVisibleInSubmitted_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaAppellantOotDocsVisibleInSubmitted_inputValues"),
                col("content.isFtpaAppellantOotDocsVisibleInSubmitted").alias("isFtpaAppellantOotDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaAppellantOotDocsVisibleInSubmitted_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaAppellantGroundsDocsVisibleInDecided_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaAppellantGroundsDocsVisibleInDecided_inputValues"),
                col("content.isFtpaAppellantGroundsDocsVisibleInDecided").alias("isFtpaAppellantGroundsDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaAppellantGroundsDocsVisibleInDecided_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaAppellantEvidenceDocsVisibleInDecided_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaAppellantEvidenceDocsVisibleInDecided_inputValues"),
                col("content.isFtpaAppellantEvidenceDocsVisibleInDecided").alias("isFtpaAppellantEvidenceDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaAppellantEvidenceDocsVisibleInDecided_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaAppellantGroundsDocsVisibleInSubmitted_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaAppellantGroundsDocsVisibleInSubmitted_inputValues"),
                col("content.isFtpaAppellantGroundsDocsVisibleInSubmitted").alias("isFtpaAppellantGroundsDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaAppellantGroundsDocsVisibleInSubmitted_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaAppellantEvidenceDocsVisibleInSubmitted_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaAppellantEvidenceDocsVisibleInSubmitted_inputValues"),
                col("content.isFtpaAppellantEvidenceDocsVisibleInSubmitted").alias("isFtpaAppellantEvidenceDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaAppellantEvidenceDocsVisibleInSubmitted_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaAppellantOotExplanationVisibleInDecided_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaAppellantOotExplanationVisibleInDecided_inputValues"),
                col("content.isFtpaAppellantOotExplanationVisibleInDecided").alias("isFtpaAppellantOotExplanationVisibleInDecided_value"),
                lit("Yes").alias("isFtpaAppellantOotExplanationVisibleInDecided_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaAppellantOotExplanationVisibleInSubmitted_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaAppellantOotExplanationVisibleInSubmitted_inputValues"),
                col("content.isFtpaAppellantOotExplanationVisibleInSubmitted").alias("isFtpaAppellantOotExplanationVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaAppellantOotExplanationVisibleInSubmitted_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("ftpaRespondentSubmitted_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("ftpaRespondentSubmitted_inputValues"),
                col("content.ftpaRespondentSubmitted").alias("ftpaRespondentSubmitted_value"),
                lit("Yes").alias("ftpaRespondentSubmitted_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaRespondentDocsVisibleInDecided_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaRespondentDocsVisibleInDecided_inputValues"),
                col("content.isFtpaRespondentDocsVisibleInDecided").alias("isFtpaRespondentDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaRespondentDocsVisibleInDecided_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaRespondentDocsVisibleInSubmitted_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaRespondentDocsVisibleInSubmitted_inputValues"),
                col("content.isFtpaRespondentDocsVisibleInSubmitted").alias("isFtpaRespondentDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaRespondentDocsVisibleInSubmitted_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaRespondentOotDocsVisibleInDecided_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaRespondentOotDocsVisibleInDecided_inputValues"),
                col("content.isFtpaRespondentOotDocsVisibleInDecided").alias("isFtpaRespondentOotDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaRespondentOotDocsVisibleInDecided_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaRespondentOotDocsVisibleInSubmitted_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaRespondentOotDocsVisibleInSubmitted_inputValues"),
                col("content.isFtpaRespondentOotDocsVisibleInSubmitted").alias("isFtpaRespondentOotDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaRespondentOotDocsVisibleInSubmitted_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaRespondentGroundsDocsVisibleInDecided_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaRespondentGroundsDocsVisibleInDecided_inputValues"),
                col("content.isFtpaRespondentGroundsDocsVisibleInDecided").alias("isFtpaRespondentGroundsDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaRespondentGroundsDocsVisibleInDecided_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaRespondentEvidenceDocsVisibleInDecided_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaRespondentEvidenceDocsVisibleInDecided_inputValues"),
                col("content.isFtpaRespondentEvidenceDocsVisibleInDecided").alias("isFtpaRespondentEvidenceDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaRespondentEvidenceDocsVisibleInDecided_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaRespondentGroundsDocsVisibleInSubmitted_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaRespondentGroundsDocsVisibleInSubmitted_inputValues"),
                col("content.isFtpaRespondentGroundsDocsVisibleInSubmitted").alias("isFtpaRespondentGroundsDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaRespondentGroundsDocsVisibleInSubmitted_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaRespondentEvidenceDocsVisibleInSubmitted_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaRespondentEvidenceDocsVisibleInSubmitted_inputValues"),
                col("content.isFtpaRespondentEvidenceDocsVisibleInSubmitted").alias("isFtpaRespondentEvidenceDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaRespondentEvidenceDocsVisibleInSubmitted_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaRespondentOotExplanationVisibleInDecided_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaRespondentOotExplanationVisibleInDecided_inputValues"),
                col("content.isFtpaRespondentOotExplanationVisibleInDecided").alias("isFtpaRespondentOotExplanationVisibleInDecided_value"),
                lit("Yes").alias("isFtpaRespondentOotExplanationVisibleInDecided_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("isFtpaRespondentOotExplanationVisibleInSubmitted_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("isFtpaRespondentOotExplanationVisibleInSubmitted_inputValues"),
                col("content.isFtpaRespondentOotExplanationVisibleInSubmitted").alias("isFtpaRespondentOotExplanationVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaRespondentOotExplanationVisibleInSubmitted_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"))).alias("caseArgumentAvailable_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"))).alias("caseArgumentAvailable_inputValues"),
                col("content.caseArgumentAvailable").alias("caseArgumentAvailable_value"),
                lit("Yes").alias("caseArgumentAvailable_Transformed"),

        )
    )

    return general_df, general_audit


################################################################   

if __name__ == "__main__":
    pass