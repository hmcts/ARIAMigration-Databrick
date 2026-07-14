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

from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, regexp_replace,
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains,rank,create_map, map_from_entries, map_from_arrays
)



################################################################
##########              hearingDetails          ###########
################################################################

def hearingDetails(silver_m1, silver_m3, bronze_listing_location):

    hearingDetails_df, hearingDetails_audit = PFH.hearingDetails(
        silver_m1, silver_m3, bronze_listing_location
    )

    return hearingDetails_df, hearingDetails_audit


################################################################
##########              documents          ###########
################################################################

def documents(silver_m1): 
    documents_df, documents_audit = PFH.documents(silver_m1)

    documents_df = (
        silver_m1.alias("m1")
        .join(
            documents_df.alias("content"),
            on="CaseNo",
            how="left"
        )
        .select(
            "m1.CaseNo",
            *[c for c in documents_df.columns if c != "CaseNo"],
            lit([]).cast("array<string>").alias("caseBundles")
        )
    )

    
    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("m1.dv_representation"), col("m1.lu_appealType")]
    

    documents_audit = (
        documents_audit.alias("audit")
            .join(documents_df.alias("documents"), on="CaseNo", how="left")
            .join(silver_m1.alias("m1"), on="CaseNo", how="left")
            .select(
                "audit.*",
                array(struct(*common_inputFields)).alias("caseBundles_inputFields"),
                array(struct(*common_inputValues)).alias("caseBundles_inputValues"),
                col("documents.caseBundles").alias("caseBundles_value"),
                lit("Yes").alias("caseBundles_Transformed")
            )
    )
    return documents_df, documents_audit

################################################################

################################################################
##########              substantiveDecision          ###########
################################################################


def substantiveDecision(silver_m1):
    substantiveDecision_df = (
        silver_m1
        .select(col("CaseNo"))
        .withColumn("scheduleOfIssuesAgreement", lit("No"))
        .withColumn(
            "scheduleOfIssuesDisagreementDescription",
            lit("This is a migrated ARIA case. Please see the documents for information on the schedule of issues.")
        )
        .withColumn("immigrationHistoryAgreement", lit("No"))
        .withColumn(
            "immigrationHistoryDisagreementDescription",
            lit("This is a migrated ARIA case. Please see the documents for information on the immigration history.")
        )
    )

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("audit.dv_representation"), col("audit.lu_appealType")]

    substantiveDecision_audit = silver_m1.alias("audit").join(substantiveDecision_df.alias("content"), on = ["CaseNo"], how = "left").select(
        col("CaseNo"),
        #-----# uploadTheAppealFormDocs #-----# 
        array(struct(*common_inputFields, lit("scheduleOfIssuesAgreement"))).alias("scheduleOfIssuesAgreement_inputFields"),
        array(struct(*common_inputValues, lit("null"))).alias("scheduleOfIssuesAgreement_inputValues"),
        col("scheduleOfIssuesAgreement"),
        lit("Yes").alias("scheduleOfIssuesAgreement_Transformation"),

        array(struct(*common_inputFields, lit("scheduleOfIssuesDisagreementDescription"))).alias("scheduleOfIssuesDisagreementDescription_inputFields"),
        array(struct(*common_inputValues, lit("null"))).alias("scheduleOfIssuesDisagreementDescription_inputValues"),
        col("scheduleOfIssuesDisagreementDescription"),
        lit("Yes").alias("scheduleOfIssuesDisagreementDescription_Transformation"),

        array(struct(*common_inputFields, lit("immigrationHistoryAgreement"))).alias("immigrationHistoryAgreement_inputFields"),
        array(struct(*common_inputValues, lit("null"))).alias("immigrationHistoryAgreement_inputValues"),
        col("immigrationHistoryAgreement"),
        lit("Yes").alias("immigrationHistoryAgreement_Transformation"),

        array(struct(*common_inputFields, lit("immigrationHistoryDisagreementDescription"))).alias("immigrationHistoryDisagreementDescription_inputFields"),
        array(struct(*common_inputValues, lit("null"))).alias("immigrationHistoryDisagreementDescription_inputValues"),
        col("immigrationHistoryDisagreementDescription"),
        lit("Yes").alias("immigrationHistoryDisagreementDescription_Transformation"),

    )
        
    return substantiveDecision_df, substantiveDecision_audit

################################################################

################################################################
##########              generalDefault          ###########
################################################################

def generalDefault(silver_m1):

    general_df = PFH.generalDefault(silver_m1)
    

    general_df = (
        general_df
        .withColumn("hmcts", lit("[userImage:hmcts.png]"))
        .withColumn("stitchingStatus", lit("DONE"))
        .withColumn("bundleConfiguration", lit("iac-hearing-bundle-config.yaml"))
        .withColumn("decisionAndReasonsAvailable", lit("No"))
    )

    return general_df
################################################################

################################################################
##########              general          ###########
################################################################

def general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres,bronze_detention_centres):

    general_df, general_audit = L.general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres,bronze_detention_centres)

    general_df = (
        silver_m1.alias("m1").join(general_df.alias("content"),on="CaseNo",how="left")
        .select("m1.CaseNo",
                *[c for c in general_df.columns if c != "CaseNo"],
                )
        )

    bundleFileNamePrefix_df = (
    silver_m1.alias("m1")
    .join(silver_m2.alias("m2"), on="CaseNo", how="left")
    .select("m1.CaseNo", "m2.Appellant_Name")
    .withColumn(
        "bundleFileNamePrefix",
        concat(
            regexp_replace(col("m1.CaseNo"), "/", " "),
            lit("-"),
            col("m2.Appellant_Name")
        )
    )
    )

    general_df =  general_df.join(bundleFileNamePrefix_df, on="CaseNo", how="left").drop("Appellant_Name")

    general_audit = (
        general_audit.alias("audit")
        .join(bundleFileNamePrefix_df.alias("bfp"), on="CaseNo", how="left")
        .select("audit.*",
            # bundleFileNamePrefix_inputFields
            array(struct(lit("CaseNo"),lit("Appellant_Name"))).alias("bundleFileNamePrefix_inputFields"),
            # bundleFileNamePrefix_inputValues
            array(struct(col("audit.CaseNo"),col("bfp.Appellant_Name"))).alias("bundleFileNamePrefix_inputValues"),
            # Transformed fields
            col("bfp.bundleFileNamePrefix").alias("bundleFileNamePrefix_value"),
            lit("Yes").alias("bundleFileNamePrefix_Transformed")
        )
    )

    return general_df, general_audit

################################################################   

if __name__ == "__main__":
    pass