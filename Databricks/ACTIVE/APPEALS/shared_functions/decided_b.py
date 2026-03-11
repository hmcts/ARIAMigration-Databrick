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
from . import ftpa_submitted_a as FSA
from . import ftpa_decided as FD

from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, regexp_replace,
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains,rank,create_map, map_from_entries, map_from_arrays
)


################################################################
##########        documents (Document Field Group)    ###########
################################################################

def documents(silver_m1,silver_m3): 
    documents_df, documents_audit = FD.documents(silver_m1,silver_m3)

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(39))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    silver_m3_content = (
    silver_m3_max_statusid
        .withColumn("allFtpaAppellantDecisionDocs", when(col("Party") == 1, lit([]).cast("array<string>")).otherwise(None))
        .withColumn("allFtpaRespondentDecisionDocs", when(col("Party") == 2, lit([]).cast("array<string>")).otherwise(None))
        .withColumn("allSetAsideDocs", lit([]).cast("array<string>"))
        .select(
            col("CaseNo"),
            col("allFtpaAppellantDecisionDocs"),
            col("ftpaRespondentDocuments"),
            col("allSetAsideDocs"),
        )   
    )

    documents_df = documents_df.join(silver_m3_content.alias("m3"), on="CaseNo", how="left")

    documents_audit = (
        documents_audit.alias("audit")
            .join(documents_df.alias("documents"), on="CaseNo", how="left")
            .join(silver_m3_max_statusid.alias("m3"), on="CaseNo", how="left")
            .select(
                "audit.*",
                array(struct(lit("Party"),lit("StatusId"))).alias("allFtpaAppellantDecisionDocs_inputFields"),
                array(struct(col("Party"),col("StatusId"))).alias("allFtpaAppellantDecisionDocs_inputValues"),
                col("documents.allFtpaAppellantDecisionDocs").alias("allFtpaAppellantDecisionDocs_value"),
                lit("Yes").alias("allFtpaAppellantDecisionDocs_Transformed"),

                array(struct(lit("Party"),lit("StatusId"))).alias("allFtpaRespondentDecisionDocs_inputFields"),
                array(struct(col("Party"),col("StatusId"))).alias("allFtpaRespondentDecisionDocs_inputValues"),
                col("documents.allFtpaRespondentDecisionDocs").alias("allFtpaRespondentDecisionDocs_value"),
                lit("Yes").alias("allFtpaRespondentDecisionDocs_Transformed"),

                array(struct(lit("Party"),lit("StatusId"))).alias("allSetAsideDocs_inputFields"),
                array(struct(col("Party"),col("StatusId"))).alias("allSetAsideDocs_inputValues"),
                col("documents.allSetAsideDocs").alias("allSetAsideDocs_value"),
                lit("Yes").alias("allSetAsideDocs_Transformed"),

            )
    )
    
    return documents_df, documents_audit


################################################################
##########              remittal                     ###########
################################################################


def remittal(silver_m3):
    # Window: highest StatusId per CaseNo
    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Filter: CaseStatus in (42, 43, 44) AND Outcome = 86
    # Note: use & (bitwise AND), not 'and', and avoid HTML-encoded &amp;
    silver_m3_filtered_casestatus = silver_m3.filter(
        col("CaseStatus").isin([42, 43, 44]) & (col("Outcome") == 86)
        # Alternatively: col("Outcome").isin([86])
    )

    # Rank and keep the highest StatusId per CaseNo
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn(
        "row_number", row_number().over(window_spec)
    )
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    # Build remittal content
    remittal_df = (
        silver_m3_max_statusid
        .withColumn("rehearingReason", lit("Remitted"))
        .withColumn("sourceOfRemittal", lit("Upper Tribunal"))
        .withColumn("appealRemittedDate", date_format(col("DecisionDate"), "yyyy-MM-dd")) 
        .withColumn("courtReferenceNumber", lit("This is a migrated ARIA case. Please refer to the documents."))
        .select(
            col("CaseNo"),
            col("rehearingReason"),
            col("sourceOfRemittal"),
            col("appealRemittedDate"),
            col("courtReferenceNumber"),
        )
    )

    # Build audit (content joined back to source)
    remittal_audit = (
        silver_m3_max_statusid.alias("m3")
        .join(remittal_df.alias("content"), on=["CaseNo"], how="left")
        .select(
            col("CaseNo"),

            # ---- rehearingReason ----
            array(struct(lit("rehearingReason"))).alias("rehearingReason_inputFields"),
            array(struct(lit("null"))).alias("rehearingReason_inputValues"),
            col("rehearingReason").alias("rehearingReason_value"),
            lit("Yes").alias("rehearingReason_Transformation"),

            # ---- sourceOfRemittal ----
            array(struct(lit("sourceOfRemittal"))).alias("sourceOfRemittal_inputFields"),
            array(struct(lit("null"))).alias("sourceOfRemittal_inputValues"),
            col("sourceOfRemittal").alias("sourceOfRemittal_value"),
            lit("Yes").alias("sourceOfRemittal_Transformation"),

            # ---- appealRemittedDate ----
            array(struct(lit("DecisionDate"),lit("CaseStatus"),lit("Outcome"))).alias("appealRemittedDate_inputFields"),
            array(struct(col("m3.DecisionDate"),col("CaseStatus"),col("Outcome"))).alias("appealRemittedDate_inputValues"),
            col("appealRemittedDate").alias("appealRemittedDate_value"),
            lit("Yes").alias("appealRemittedDate_Transformation"),

            # ---- courtReferenceNumber ----
            array(struct(lit("courtReferenceNumber"))).alias("courtReferenceNumber_inputFields"),
            array(struct(lit("null"))).alias("courtReferenceNumber_inputValues"),
            col("courtReferenceNumber").alias("courtReferenceNumber_value"),
            lit("Yes").alias("courtReferenceNumber_Transformation"),
        )
    )

    return remittal_df, remittal_audit


################################################################
##########              generalDefault          ###########
################################################################

def generalDefault(silver_m1):

    general_df = FSA.generalDefault(silver_m1)

    general_df = (
        general_df
        .withColumn("isDlrmSetAsideEnabled", lit("Yes"))
        .withColumn("isFtpaAppellantDecided", lit("Yes"))
        .withColumn("isFtpaRespondentDecided", lit("Yes"))
        .withColumn("isReheardAppealEnabled", lit("Yes"))
        .withColumn("secondFtpaDecisionExists", lit("No"))
        .withColumn("caseFlagSetAsideReheardExists", lit("Yes"))
    )

    return general_df
################################################################

################################################################   

if __name__ == "__main__":
    pass