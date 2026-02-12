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

    documents_df = (
        documents_df.alias("content")
        .join(silver_m3_max_statusid_state_3_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("hearingRequirements",when(cond_state_3_4, col("content.hearingRequirements")).otherwise(None))
        .select("content.*", "hearingRequirements")
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
        .select("content.*", "hearingDocuments"
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
                ,"CaseStatus","Outcome")
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
                <"pastExperiencesDescription"
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
                ,"CaseStatus","Outcome"
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
                ,"CaseStatus","Outcome"
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
                ,"CaseStatus","Outcome"
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
                ,"CaseStatus","Outcome"
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
                ,"CaseStatus","Outcome"
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
                ,"CaseStatus","Outcome"
                )
    )

    return ftpa_df, ftpa_audit


################################################################
##########              generalDefault          ###########
################################################################


def generalDefault(silver_m1,silver_m3):

    generalDefault_df, generalDefault_audit = FSA.generalDefault(silver_m1)

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

    
    
    generalDefault_df = (
        generalDefault_df.alias("content")
        .join(silver_m3_max_statusid_state_2_3_4.alias("m3"), on="CaseNo", how="left")
        .withColumn("directions",when(cond_state_2_3_4, col("content.directions")).otherwise(None))
        .withColumn("uploadHomeOfficeBundleAvailable",when(cond_state_2_3_4, col("content.uploadHomeOfficeBundleAvailable")).otherwise(None))
        .withColumn("uploadHomeOfficeBundleActionAvailable",when(cond_state_2_3_4, col("content.uploadHomeOfficeBundleActionAvailable")).otherwise(None))
        .withColumn("caseArgumentAvailable",when(cond_state_2_3_4, col("content.caseArgumentAvailable")).otherwise(None))
        .withColumn("reasonsForAppealDecision",when(cond_state_2_3_4, col("content.reasonsForAppealDecision")).otherwise(None))
        .select("content.*"
                ,"directions"
                ,"uploadHomeOfficeBundleAvailable"
                ,"uploadHomeOfficeBundleActionAvailable"
                ,"caseArgumentAvailable"
                ,"reasonsForAppealDecision"
                ,"CaseStatus","Outcome"
                )
    )

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
                ,"CaseStatus","Outcome"
                )
    )

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
                ,"CaseStatus","Outcome"
                )
    )

    return generalDefault_df, generalDefault_audit

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
                          , "bundleFileNamePrefix"
                          , "ftpaAppellantSubmitted"
                          , "isFtpaAppellantDocsVisibleInDecided"
                          , "isFtpaAppellantDocsVisibleInSubmitted"
                          , "isFtpaAppellantOotDocsVisibleInDecided"
                          , "isFtpaAppellantOotDocsVisibleInSubmitted"
                          , "isFtpaAppellantGroundsDocsVisibleInDecided"
                          , "isFtpaAppellantEvidenceDocsVisibleInDecided"
                          , "isFtpaAppellantGroundsDocsVisibleInSubmitted"
                          , "isFtpaAppellantEvidenceDocsVisibleInSubmitted"
                          , "isFtpaAppellantOotExplanationVisibleInDecided"
                          , "isFtpaAppellantOotExplanationVisibleInSubmitted"
                          , "ftpaRespondentSubmitted"
                          , "isFtpaRespondentDocsVisibleInDecided"
                          , "isFtpaRespondentDocsVisibleInSubmitted"
                          , "isFtpaRespondentOotDocsVisibleInDecided"
                          , "isFtpaRespondentOotDocsVisibleInSubmitted"
                          , "isFtpaRespondentGroundsDocsVisibleInDecided"
                          , "isFtpaRespondentEvidenceDocsVisibleInDecided"
                          , "isFtpaRespondentGroundsDocsVisibleInSubmitted"
                          , "isFtpaRespondentEvidenceDocsVisibleInSubmitted"
                          , "isFtpaRespondentOotExplanationVisibleInDecided"
                          , "isFtpaRespondentOotExplanationVisibleInSubmitted"
                          , "CaseStatus", "Outcome" 
                          ) 
                  )

    return general_df, general_audit


################################################################   

if __name__ == "__main__":
    pass