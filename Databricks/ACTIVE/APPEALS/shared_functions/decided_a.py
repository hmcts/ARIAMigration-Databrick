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

from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, regexp_replace,
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains,rank,create_map, map_from_entries, map_from_arrays
)


################################################################
##########              documents          ###########
################################################################

def documents(silver_m1): 
    documents_df, documents_audit = D.documents(silver_m1)

    documents_df = documents_df.select("*",
                lit([]).cast("array<string>").alias("finalDecisionAndReasonsDocuments"))
    

    documents_audit = (
        documents_audit.alias("audit")
            .join(documents_df.alias("documents"), on="CaseNo", how="left")
            .select(
                "audit.*",
                array(struct( lit("finalDecisionAndReasonsDocuments"))).alias("finalDecisionAndReasonsDocuments_inputFields"),
                array(struct(lit("null"))).alias("finalDecisionAndReasonsDocumentss_inputValues"),
                col("finalDecisionAndReasonsDocuments").alias("finalDecisionAndReasonsDocuments_value"),
                lit("Yes").alias("finalDecisionAndReasonsDocuments_Transformed")
            )
    )
    return documents_df, documents_audit

################################################################

################################################################
##########              substantiveDecision          ###########
################################################################


def substantiveDecision(silver_m1,silver_m3):

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(37, 38,26) & col("Outcome").isin(1,2))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    # silver_m3_filtered_casestatus = silver_m3_ranked.filter(col("CaseStatus").isin(37, 38))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")


    substantiveDecision_df, substantiveDecision_audit = D.substantiveDecision(silver_m1)
    
    
    substantiveDecision_df = (
        substantiveDecision_df.alias("sd")
            .join(silver_m3_max_statusid.alias("m3"), on=["CaseNo"], how="left")
            .select(
                col("sd.*"),
                # Format dates as dd/MM/yyyy
                date_format(col("m3.DecisionDate"), "dd/MM/yyyy").alias("sendDecisionsAndReasonsDate"),
                date_format(col("m3.DecisionDate"), "dd/MM/yyyy").alias("appealDate"),

                # Outcome mapping
                when(col("m3.Outcome") == 1, "Allowed")
                    .when(col("m3.Outcome") == 2, "Dismissed")
                    .otherwise(None)
                    .alias("appealDecision"),

                when(col("m3.Outcome") == 1, "Allowed")
                    .when(col("m3.Outcome") == 2, "Dismissed")
                    .otherwise(None)
                    .alias("isDecisionAllowed"),

                lit("No").alias("anonymityOrder")
            )
    )


    substantiveDecision_audit = (
        substantiveDecision_audit.alias("audit")
        .join(silver_m3_max_statusid.alias("m3"), on=["CaseNo"], how="left")
        .join(substantiveDecision_df.alias("content"), on=["CaseNo"], how="left")
        .select(
            "audit.*",

            # ----- sendDecisionsAndReasonsDate -----
            array(struct(lit("DecisionDate"),lit("CaseStatus"),lit("Outcome"))).alias("sendDecisionsAndReasonsDate_inputFields"),
            array(struct(col("m3.DecisionDate"),col("CaseStatus"),col("Outcome"))).alias("sendDecisionsAndReasonsDate_inputValues"),
            col("content.sendDecisionsAndReasonsDate").alias("sendDecisionsAndReasonsDate"),
            lit("Yes").alias("sendDecisionsAndReasonsDate_Transformation"),

            # ----- appealDate -----
            array(struct(lit("DecisionDate"),lit("CaseStatus"),lit("Outcome"))).alias("appealDate_inputFields"),
            array(struct(col("m3.DecisionDate"),col("CaseStatus"),col("Outcome"))).alias("appealDate_inputValues"),
            col("content.appealDate").alias("appealDate"),
            lit("Yes").alias("appealDate_Transformation"),

            # ----- appealDecision -----
            array(struct(lit("CaseStatus"),lit("Outcome"))).alias("appealDecision_inputFields"),
            array(struct(col("CaseStatus"),col("Outcome"))).alias("appealDecision_inputValues"),
            col("content.appealDecision").alias("appealDecision"),
            lit("Yes").alias("appealDecision_Transformation"),

            # ----- isDecisionAllowed -----
            array(struct(lit("CaseStatus"),lit("Outcome"))).alias("isDecisionAllowed_inputFields"),
            array(struct(col("CaseStatus"),col("Outcome"))).alias("isDecisionAllowed_inputValues"),
            col("content.isDecisionAllowed").alias("isDecisionAllowed"),
            lit("Yes").alias("isDecisionAllowed_Transformation"),

            # ----- anonymityOrder -----
            array(struct(lit("anonymityOrder"))).alias("anonymityOrder_inputFields"),
            array(struct(lit("null"))).alias("anonymityOrder_inputValues"),
            col("content.anonymityOrder").alias("anonymityOrder"),
            lit("Yes").alias("anonymityOrder_Transformation"),
        )
    )

    return substantiveDecision_df, substantiveDecision_audit

################################################################

################################################################
##########              hearingActuals          ###########
################################################################

def hearingActuals(silver_m3):

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(37, 38,26) & col("Outcome").isin(1,2))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    # silver_m3_filtered_casestatus = silver_m3_ranked.filter(col("CaseStatus").isin(37, 38))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    hearingActuals_df = (
        silver_m3_max_statusid
        .withColumn(
            "actualCaseHearingLength",
            F.create_map(
                # key: "hours", value: hours calculation
                F.lit("hours"),
                F.when(col("HearingDuration").isNull(), F.lit(None).cast("int"))
                .otherwise(F.floor(col("HearingDuration").cast("int") / 60)),

                # key: "minutes", value: minutes calculation
                F.lit("minutes"),
                F.when(col("HearingDuration").isNull(), F.lit(None).cast("int"))
                .otherwise(col("HearingDuration").cast("int") % 60)
            )
        )
        .withColumn("attendingJudge",concat(col("Adj_Determination_Title"),lit(" "),col("Adj_Determination_Forenames"),lit(" "),col("Adj_Determination_Surname")))
        .select(
            col("CaseNo"),
            col("actualCaseHearingLength"),
            col("attendingJudge")
        )
    )

    hearingActuals_audit = hearingActuals_df.alias("ha").join(silver_m3_max_statusid.alias("m3"), on=["CaseNo"], how="left").select(
        col("CaseNo"),
        array(struct(lit("HearingDuration"),lit("Outcome"))).alias("actualCaseHearingLength_inputFields"),
        array(struct(col("HearingDuration"),col("Outcome"))).alias("actualCaseHearingLength_inputValues"),
        col("actualCaseHearingLength").alias("actualCaseHearingLength"),
        lit("Yes").alias("actualCaseHearingLength_Transformation"),

        array(struct(lit("Adj_Determination_Title"),lit("Adj_Determination_Forenames"),lit("Adj_Determination_Surname"),lit("Outcome"))).alias("attendingJudge_inputFields"),
        array(struct(col("Adj_Determination_Title"),col("Adj_Determination_Forenames"),col("Adj_Determination_Surname"),col("Outcome"))).alias("attendingJudge_inputValues"),
        col("attendingJudge").alias("attendingJudge"),
        lit("Yes").alias("attendingJudge_Transformation"),
    )

    return hearingActuals_df,hearingActuals_audit


################################################################

################################################################
##########              ftpa          ###########
################################################################

def ftpa(silver_m3,silver_c):

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(37, 38,26) & col("Outcome").isin(1,2))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    # silver_m3_filtered_casestatus = silver_m3_ranked.filter(col("CaseStatus").isin(37, 38))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    silver_c_filtered = silver_c.filter(col("CategoryId").isin(37, 38))
    
    ftpa_df = (
        silver_m3_max_statusid
            .join(silver_c_filtered, on=["CaseNo"], how="left")
            .select(
                col("CaseNo"),
                date_format(
                    when(col("CategoryId") == 37, F.date_add(col("DecisionDate"), 14))
                    .when(col("CategoryId") == 38, F.date_add(col("DecisionDate"), 28))
                    .otherwise(col("DecisionDate")),
                    "dd/MM/yyyy"
                ).alias("ftpaApplicationDeadline")
            )
    )

    # Build the audit DataFrame
    ftpa_audit = (
        ftpa_df.alias("ftpa")
            .join(silver_m3_max_statusid.alias("m3"), on=["CaseNo"], how="left")
            .join(silver_c_filtered.alias("c"), on=["CaseNo"], how="left")
            .select(
                col("CaseNo"),
                array(
                    struct(
                        lit("CategoryId"),
                        lit("CaseStatus"),
                        lit("DecisionDate"),
                        lit("Outcome")
                    )
                ).alias("ftpaApplicationDeadline_inputFields"),
                array(
                    struct(
                        col("CategoryId"),
                        col("CaseStatus"),
                        col("DecisionDate"),
                        col("Outcome")
                    )
                ).alias("ftpaApplicationDeadline_inputValues"),
                col("ftpaApplicationDeadline").alias("ftpaApplicationDeadline"),
                lit("Yes").alias("ftpaApplicationDeadline_Transformation"),
            )
    )


    return ftpa_df, ftpa_audit
################################################################

################################################################

################################################################
##########              generalDefault          ###########
################################################################

def generalDefault(silver_m1):

    general_df = D.generalDefault(silver_m1)

    general_df = (
        general_df
        .withColumn("appealDecisionAvailable", lit("Yes"))
    )

    return general_df
################################################################

################################################################   

if __name__ == "__main__":
    pass