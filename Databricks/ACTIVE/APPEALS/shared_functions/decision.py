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

def hearingDetails(silver_m1,silver_m3,bronze_listing_location):

    df_hearingDetails = PHF.hearingDetails(silver_m1,silver_m3,bronze_listing_location)


        # Define window partitioned by CaseNo and ordered by descending StatusId
    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(37, 38))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    # silver_m3_filtered_casestatus = silver_m3_ranked.filter(col("CaseStatus").isin(37, 38))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    silver_m3_filtered_casestatus = silver_m3_max_statusid

    silver_m3_filtered_casestatus = silver_m3_filtered_casestatus.alias("m3").join(
        bronze_listing_location.alias("location"),
        on=col("m3.HearingCentre") == col("location.ListedCentre"),
        how="left"
    ).withColumn(
        "listingLocation",
        F.create_map(
            F.lit("code"),
            F.when(col("location.ListedCentre").isNull(), F.lit(None).alias("code"))
                .otherwise(col("location.locationCode").alias("code")),
            F.lit("label"),
            F.when(col("location.ListedCentre").isNull(), F.lit(None).alias("code"))
                .otherwise(col("location.locationLabel").alias("label"))
    ))

    content_df = silver_m3_filtered_casestatus.withColumn(
        "listingLength",
        F.create_map(
                F.lit("hours"),
                F.when(col("TimeEstimate").isNull(), F.lit(None).cast("int").alias("hours"))
                .otherwise(F.floor(F.col("TimeEstimate").cast("int") / 60).alias("hours")),
                F.lit("minutes"),
                F.when(col("TimeEstimate").isNull(), F.lit(None).cast("int").alias("minutes"))
                .otherwise(F.col("TimeEstimate").cast("int") % 60).alias("minutes"))
        ).select(
            col("CaseNo").alias("CaseNo"),
            col("listingLength"),
            col("listingLocation"),
            col("TimeEstimate"),
            col("HearingCentre") 
        )
            # F.when(
        #     F.col("TimeEstimate").isNull(),
        #     # Create a struct with null hours and minutes when TimeEstimate is null
        #     F.struct(
        #         F.lit(None).cast("int").alias("hours"),
        #         F.lit(None).cast("int").alias("minutes")
        #     )
        # ).otherwise(
        #     # Compute hours and minutes from TimeEstimate (assumed to be minutes)
        #     F.struct(
        #         F.floor(F.col("TimeEstimate").cast("int") / 60).alias("hours"),
        #         (F.col("TimeEstimate").cast("int") % 60).alias("minutes")
        #     )
        # )
    df_hearingDetails = (
        silver_m1.alias("m1")
        .join(content_df.alias("m3_content"), ["CaseNo"], "left")
        .withColumn(
            "hearingChannel",
            F.create_map(
                F.lit("code"),
                F.when(col("m1.VisitVisaType") == 1, F.lit("ONPPRS"))
                .when(col("m1.VisitVisaType") == 2, F.lit("INTER"))
                .otherwise(F.lit(None).cast("string")),
                F.lit("label"),
                F.when(col("m1.VisitVisaType") == 1, F.lit("On The Papers"))
                .when(col("m1.VisitVisaType") == 2, F.lit("In Person"))
                .otherwise(F.lit(None).cast("string"))
            )
        )
    .withColumn("witnessDetails",lit([]).cast("array<string>"))
    .withColumn("witness1InterpreterSignLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness2InterpreterSignLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness3InterpreterSignLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness4InterpreterSignLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness5InterpreterSignLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness6InterpreterSignLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness7InterpreterSignLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness8InterpreterSignLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness9InterpreterSignLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness10InterpreterSignLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness1InterpreterSpokenLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness2InterpreterSpokenLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness3InterpreterSpokenLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness4InterpreterSpokenLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness5InterpreterSpokenLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness6InterpreterSpokenLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness7InterpreterSpokenLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness8InterpreterSpokenLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness9InterpreterSpokenLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .withColumn("witness10InterpreterSpokenLanguage", map_from_arrays(lit([]).cast("array<string>"), lit([]).cast("array<string>")).cast("map<string,string>"))
    .select(
    col("m1.CaseNo").alias("CaseNo"),
    col("listingLength"),
    col("hearingChannel"),
    col("witnessDetails"),
    col("listingLocation"),
    col("witness1InterpreterSignLanguage"), 
    col("witness2InterpreterSignLanguage"), 
    col("witness3InterpreterSignLanguage"), 
    col("witness4InterpreterSignLanguage"), 
    col("witness5InterpreterSignLanguage"), 
    col("witness6InterpreterSignLanguage"), 
    col("witness7InterpreterSignLanguage"), 
    col("witness8InterpreterSignLanguage"), 
    col("witness9InterpreterSignLanguage"),
    col("witness10InterpreterSignLanguage"), 
    col("witness1InterpreterSpokenLanguage"), 
    col("witness2InterpreterSpokenLanguage"), 
    col("witness3InterpreterSpokenLanguage"), 
    col("witness4InterpreterSpokenLanguage"), 
    col("witness5InterpreterSpokenLanguage"), 
    col("witness6InterpreterSpokenLanguage"), 
    col("witness7InterpreterSpokenLanguage"), 
    col("witness8InterpreterSpokenLanguage"), 
    col("witness9InterpreterSpokenLanguage"), 
    col("witness10InterpreterSpokenLanguage")
    )
    )

    df_audit_hearingDetails = (
        df_hearingDetails.alias("hd")
        .join(silver_m1.alias("m1"), ["CaseNo"], "left")
        .join(content_df.alias("m3"), ["CaseNo"], "left")
        .join(bronze_listing_location.alias("location"),on=col("m3.HearingCentre") == col("location.ListedCentre"), how="left")
        .select(
                "hd.CaseNo",
                # listingLength
                array(struct(lit("TimeEstimate"))).alias("listingLength_inputFields"),
                array(struct(col("TimeEstimate"))).alias("listingLength_inputValues"),
                col("hd.listingLength"),
                lit("Yes").alias("listingLength_Transformed"),
                # hearingChannel
                array(struct(lit("VisitVisaType"))).alias("hearingChannel_inputFields"),
                array(struct(col("m1.VisitVisaType"))).alias("hearingChannel_inputValues"),
                col("hd.hearingChannel"),
                lit("Yes").alias("hearingChannel_Transformed"),

                # # listingLocation
                
                # array(struct(lit("locationCode").alias("code"), lit("locationLabel").alias("label")).alias("listingLocation_inputFields")),
                # array(struct(col("location.locationCode"), col("location.locationLabel"))).alias("listingLocation_inputValues"),
                # col("hd.listingLocation"),
                # lit("Yes").alias("listingLocation_Transformed"),
        )
    )


    return df_hearingDetails, df_audit_hearingDetails

################################################################
##########              documents          ###########
################################################################

def documents(silver_m1): 
    documents_df, documents_audit = PFH.documents(silver_m1)

    documents_df = documents_df.select("*",
                lit([]).cast("array<string>").alias("caseBundles"))
    
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

    general_df = L.generalDefault(silver_m1)

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

def general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres):

    general_df, general_audit = L.general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres)

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
        .select(
            "audit.*",
            # bundleFileNamePrefix_inputFields
            array(
                struct(
                    lit("CaseNo"),
                    lit("Appellant_Name")
                )
            ).alias("bundleFileNamePrefix_inputFields"),

            # bundleFileNamePrefix_inputValues
            array(
                struct(
                    col("audit.CaseNo"),
                    col("bfp.Appellant_Name")
                )
            ).alias("bundleFileNamePrefix_inputValues"),

            # Transformed fields
            col("bfp.bundleFileNamePrefix").alias("bundleFileNamePrefix_value"),
            lit("Yes").alias("bundleFileNamePrefix_Transformed")
        )
    )

    return general_df, general_audit

################################################################   

if __name__ == "__main__":
    pass