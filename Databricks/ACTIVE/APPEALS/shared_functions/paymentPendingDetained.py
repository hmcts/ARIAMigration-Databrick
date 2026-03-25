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
from . import paymentPending as PP


from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, regexp_replace,
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains,rank,create_map, map_from_entries, map_from_arrays
)

################################################################
##########          Detained State Function          ###########
################################################################

def detainedState(silver_m1, silver_m2,bronze_detention_centres):

    joined_m1_m2 =(
        silver_m1.alias("m1")
        .join(silver_m2.alias("m2"), on="CaseNo", how="left")
        
    )

    detainedState_df = (
        joined_m1_m2.alias("m2").join(bronze_detention_centres.alias("det"), on="DetentionCentreId", how="left")
        .withColumn("appellantInDetention", when(col("m2.Detained").isin([1,2,4]), "Yes").otherwise(None))
        .withColumn("detentionFacility", when(col("m2.Detained") == 1, "prison")
                    .when(col("m2.Detained") == 1, "prison")
                    .when(col("m2.Detained") == 2, "immigrationRemovalCentre")
                    .when(col("m2.Detained") == 4, "other")
                    .otherwise(None))
        .withColumn("prisonNOMSNumber",
                    when((col("m2.Detained") != 1) & (col("m2.PrisonRef").isNotNull()),
                        struct(col("m2.PrisonRef").alias("prison"))
                    ).otherwise(None)
        )
        .withColumn("otherDetentionFacilityName",
            when((col("m2.Detained") == 4),
                    struct(coalesce(col("det.DetentionCentre"),col("m2.Appellant_Address1")).alias("other"))
            ).otherwise(None)
        )
        .withColumn("ircName",when((col("m2.Detained") == 2),col("det.ircName")).otherwise(None))
        .withColumn("releaseDateProvided",when((col("m2.Detained").isin(1,4)),lit("Yes")).otherwise(None))
        .withColumn("hasPendingBailApplications",when((col("m2.Detained") == 2),lit("NotSure")).otherwise(None))
        .withColumn("removalOrderOptions",when((col("m2.RemovalDate").isNotNull()),lit("Yes")).otherwise("No"))
        .withColumn("removalOrderDate",when((col("m2.RemovalDate").isNotNull()),date_format(col("m2.RemovalDate"), "yyyy-MM-dd")).otherwise(None))
        .withColumn("detentionBuilding",when((col("m2.Detained").isin(1,2)),col("det.detentionBuilding")).otherwise(None))
        .withColumn("detentionAddressLines",when((col("m2.Detained").isin(1,2)),col("det.detentionAddressLines")).otherwise(None))
        .withColumn("detentionPostcode",when((col("m2.Detained").isin(1,2)),col("det.detentionPostcode")).otherwise(None))

        .select(col("CaseNo"),
                col("appellantInDetention"),
                col("detentionFacility"),
                col("prisonName"),
                col("prisonNOMSNumber"),
                col("otherDetentionFacilityName"),
                col("ircName"),
                col("releaseDateProvided"),
                col("hasPendingBailApplications"),
                col("removalOrderOptions"),
                col("removalOrderDate"),
                col("detentionBuilding"),
                col("detentionAddressLines"),
                col("detentionPostcode"),
                )
    )

    detainedState_audit = (
        joined_m1_m2.alias("m1_m2")
        .join(bronze_detention_centres.alias("det"),col("m1_m2.DetentionCentreId") == col("det.DetentionCentreId"),"left")
        .join(detainedState_df.alias("content"), on="CaseNo", how="left")
            .select(
                # ariaDesiredState - ARIADM-797
                col("content.CaseNo"),
                array(struct(lit("Detained"))).alias("appellantInDetention_inputFields"),
                array(struct(col("m1_m2.Detained"))).alias("appellantInDetention_inputValues"),
                col("content.appellantInDetention"),
                lit("Yes").alias("appellantInDetention_Transformation"),

                array(struct(lit("Detained"))).alias("detentionFacility_inputFields"),
                array(struct(col("m1_m2.Detained"))).alias("detentionFacility_inputValues"),
                col("content.detentionFacility"),
                lit("Yes").alias("detentionFacility_Transformation"),

                array(struct(lit("Detained"), lit("PrisonRef"))).alias("prisonNOMSNumber_inputFields"),
                array(struct(col("m1_m2.Detained"), col("m1_m2.PrisonRef"))).alias("prisonNOMSNumber_inputValues"),
                col("content.prisonNOMSNumber"),
                lit("Yes").alias("prisonNOMSNumber_Transformation"),

                array(struct(lit("Detained"), lit("DetentionCentre"), lit("Appellant_Address1"))).alias("otherDetentionFacilityName_inputFields"),
                array(struct(
                    col("m1_m2.Detained"),
                    col("det.DetentionCentre"),
                    col("m1_m2.Appellant_Address1")
                )).alias("otherDetentionFacilityName_inputValues"),
                col("content.otherDetentionFacilityName"),
                lit("Yes").alias("otherDetentionFacilityName_Transformation"),

                array(struct(lit("Detained"), lit("ircName"))).alias("ircName_inputFields"),
                array(struct(col("m1_m2.Detained"), col("det.ircName"))).alias("ircName_inputValues"),
                col("content.ircName"),
                lit("Yes").alias("ircName_Transformation"),

                array(struct(lit("Detained"))).alias("releaseDateProvided_inputFields"),
                array(struct(col("m1_m2.Detained"))).alias("releaseDateProvided_inputValues"),
                col("content.releaseDateProvided"),
                lit("Yes").alias("releaseDateProvided_Transformation"),

                array(struct(lit("Detained"))).alias("hasPendingBailApplications_inputFields"),
                array(struct(col("m1_m2.Detained"))).alias("hasPendingBailApplications_inputValues"),
                col("content.hasPendingBailApplications"),
                lit("Yes").alias("hasPendingBailApplications_Transformation"),

                array(struct(lit("RemovalDate"))).alias("removalOrderOptions_inputFields"),
                array(struct(col("m1_m2.RemovalDate"))).alias("removalOrderOptions_inputValues"),
                col("content.removalOrderOptions"),
                lit("Yes").alias("removalOrderOptions_Transformation"),

                array(struct(lit("RemovalDate"))).alias("removalOrderDate_inputFields"),
                array(struct(col("m1_m2.RemovalDate"))).alias("removalOrderDate_inputValues"),
                col("content.removalOrderDate"),
                lit("Yes").alias("removalOrderDate_Transformation"),

                array(struct(lit("Detained"), lit("detentionBuilding"))).alias("detentionBuilding_inputFields"),
                array(struct(col("m1_m2.Detained"), col("det.detentionBuilding"))).alias("detentionBuilding_inputValues"),
                col("content.detentionBuilding"),
                lit("Yes").alias("detentionBuilding_Transformation"),

                array(struct(lit("Detained"), lit("detentionAddressLines"))).alias("detentionAddressLines_inputFields"),
                array(struct(col("m1_m2.Detained"), col("det.detentionAddressLines"))).alias("detentionAddressLines_inputValues"),
                col("content.detentionAddressLines"),
                lit("Yes").alias("detentionAddressLines_Transformation"),

                array(struct(lit("Detained"), lit("detentionPostcode"))).alias("detentionPostcode_inputFields"),
                array(struct(col("m1_m2.Detained"), col("det.detentionPostcode"))).alias("detentionPostcode_inputValues"),
                col("content.detentionPostcode"),
                lit("Yes").alias("detentionPostcode_Transformation")
            )
    )


    return detainedState_df, detainedState_audit

################################################################
##########              caseData grouping            ###########
################################################################

# caseData grouping
def caseData(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres, bronze_detention_centres):

    caseData_df, caseData_audit = PP.caseData(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres)

    caseData_df = caseData_df.drop("hearingCentre","staffLocation","caseManagementLocation","hearingCentreDynamicList","caseManagementLocationRefData","selectedHearingCentreRefData")

    joined_m1_m2 =(
        silver_m1.alias("m1")
        .join(silver_m2.alias("m2"), on="CaseNo", how="left")
        )
    
    caseData_df = (
        caseData_df.alias("content").join(joined_m1_m2.alias("m2"),on="CaseNo", how="left")
        .join(bronze_detention_centres.alias("det"), on="DetentionCentreId", how="left")
        .join(bronze_hearing_centres.alias("bhc"),col("m2.CentreId") == col("bhc.CentreId"),how="left")
        .withColumn("hearingCentre1", when(col("m2.Detained").isin(1,2),col("det.hearingCentre")).otherwise(col("bhc.hearingCentre")))
        .withColumn("staffLocation1", when(col("m2.Detained").isin(1,2),col("det.staffLocation")).otherwise(col("bhc.staffLocation")))
        # .withColumn("caseManagementLocation1", when(col("m2.Detained").isin(1,2),col("det.caseManagementLocation")).otherwise(col("bhc.caseManagementLocation")))
        .withColumn("hearingCentreDynamicList1", when(col("m2.Detained").isin(1,2),col("det.applicationChangeDesignatedHearingCentre")).otherwise(col("bhc.applicationChangeDesignatedHearingCentre")))
        # .withColumn("caseManagementLocationRefData", when(col("m2.Detained").isin(1,2),col("det.caseManagementLocationRefData")).otherwise("bhc.caseManagementLocationRefData"))
        .withColumn("selectedHearingCentreRefData1", when(col("m2.Detained").isin(1,2),col("det.selectedHearingCentreRefData")).otherwise(col("bhc.selectedHearingCentreRefData")))
        .select(
            "content.*",
            col("hearingCentre1").alias("hearingCentre"),
            col("staffLocation1").alias("staffLocation"),
            # col("caseManagementLocation1").alias("caseManagementLocation"),
            col("hearingCentreDynamicList1").alias("hearingCentreDynamicList"),
            # col("caseManagementLocationRefData"),
            col("selectedHearingCentreRefData1").alias("selectedHearingCentreRefData"),
        )

    )
    




    return caseData_df, caseData_audit

################################################################
##########              documents          ###########
################################################################

def documents(silver_m1,silver_m3): 
    documents_df, documents_audit = DA.documents(silver_m1)

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(39))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    silver_m3_content = (
    silver_m3_max_statusid
        .withColumn("ftpaAppellantDocuments", when(col("Party") == 1, lit([]).cast("array<string>")).otherwise(None))
        .withColumn("ftpaRespondentDocuments", when(col("Party") == 2, lit([]).cast("array<string>")).otherwise(None))
        .withColumn("ftpaAppellantGroundsDocuments", when(col("Party") == 1, lit([]).cast("array<string>")).otherwise(None))
        .withColumn("ftpaRespondentGroundsDocuments", when(col("Party") == 2, lit([]).cast("array<string>")).otherwise(None))
        .withColumn("ftpaAppellantEvidenceDocuments", when(col("Party") == 1, lit([]).cast("array<string>")).otherwise(None))
        .withColumn("ftpaRespondentEvidenceDocuments", when(col("Party") == 2, lit([]).cast("array<string>")).otherwise(None))
        .withColumn("ftpaAppellantOutOfTimeDocuments", when(col("Party") == 1, lit([]).cast("array<string>")).otherwise(None))
        .withColumn("ftpaRespondentOutOfTimeDocuments", when(col("Party") == 2, lit([]).cast("array<string>")).otherwise(None))
        .select(
            col("CaseNo"),
            col("ftpaAppellantDocuments"),
            col("ftpaRespondentDocuments"),
            col("ftpaAppellantGroundsDocuments"),
            col("ftpaRespondentGroundsDocuments"),
            col("ftpaAppellantEvidenceDocuments"),
            col("ftpaRespondentEvidenceDocuments"),
            col("ftpaAppellantOutOfTimeDocuments"),
            col("ftpaRespondentOutOfTimeDocuments")
        )   
    )
    
    documents_df = documents_df.join(silver_m3_content.alias("m3"), on="CaseNo", how="left")

    
    documents_audit = (
        documents_audit.alias("audit")
            .join(documents_df.alias("documents"), on="CaseNo", how="left")
            .join(silver_m3_max_statusid.alias("m3"), on="CaseNo", how="left")
            .select(
                "audit.*",
                array(struct(lit("Party"),lit("StatusId"))).alias("ftpaAppellantDocuments_inputFields"),
                array(struct(col("Party"),col("StatusId"))).alias("ftpaAppellantDocuments_inputValues"),
                col("documents.ftpaAppellantDocuments").alias("ftpaAppellantDocuments_value"),
                lit("Yes").alias("ftpaAppellantDocuments_Transformed"),

                array(struct(lit("Party"),lit("StatusId"))).alias("ftpaRespondentDocuments_inputFields"),
                array(struct(col("Party"),col("StatusId"))).alias("ftpaRespondentDocuments_inputValues"),
                col("documents.ftpaRespondentDocuments").alias("ftpaRespondentDocuments_value"),
                lit("Yes").alias("ftpaRespondentDocuments_Transformed"),

                array(struct(lit("Party"),lit("StatusId"))).alias("ftpaAppellantGroundsDocuments_inputFields"),
                array(struct(col("Party"),col("StatusId"))).alias("ftpaAppellantGroundsDocuments_inputValues"),
                col("documents.ftpaAppellantGroundsDocuments").alias("ftpaAppellantGroundsDocuments_value"),
                lit("Yes").alias("ftpaAppellantGroundsDocuments_Transformed"),
                
                array(struct(lit("Party"),lit("StatusId"))).alias("ftpaRespondentGroundsDocuments_inputFields"),
                array(struct(col("Party"),col("StatusId"))).alias("ftpaRespondentGroundsDocuments_inputValues"),
                col("documents.ftpaRespondentGroundsDocuments").alias("ftpaRespondentGroundsDocuments_value"),
                lit("Yes").alias("ftpaRespondentGroundsDocuments_Transformed"),

                array(struct(lit("Party"),lit("StatusId"))).alias("ftpaAppellantEvidenceDocuments_inputFields"),
                array(struct(col("Party"),col("StatusId"))).alias("ftpaAppellantEvidenceDocuments_inputValues"),
                col("documents.ftpaAppellantEvidenceDocuments").alias("ftpaAppellantEvidenceDocuments_value"),
                lit("Yes").alias("ftpaAppellantEvidenceDocuments_Transformed"),
                
                array(struct(lit("Party"),lit("StatusId"))).alias("ftpaRespondentEvidenceDocuments_inputFields"),
                array(struct(col("Party"),col("StatusId"))).alias("ftpaRespondentEvidenceDocuments_inputValues"),
                col("documents.ftpaRespondentEvidenceDocuments").alias("ftpaRespondentEvidenceDocuments_value"),
                lit("Yes").alias("ftpaRespondentEvidenceDocuments_Transformed"),

                array(struct(lit("Party"),lit("StatusId"))).alias("ftpaAppellantOutOfTimeDocuments_inputFields"),
                array(struct(col("Party"),col("StatusId"))).alias("ftpaAppellantOutOfTimeDocuments_inputValues"),
                col("documents.ftpaAppellantOutOfTimeDocuments").alias("ftpaAppellantOutOfTimeDocuments_value"),
                lit("Yes").alias("ftpaAppellantOutOfTimeDocuments_Transformed"),
                
                array(struct(lit("Party"),lit("StatusId"))).alias("ftpaRespondentOutOfTimeDocuments_inputFields"),
                array(struct(col("Party"),col("StatusId"))).alias("ftpaRespondentOutOfTimeDocuments_inputValues"),
                col("documents.ftpaRespondentOutOfTimeDocuments").alias("ftpaRespondentOutOfTimeDocuments_value"),
                lit("Yes").alias("ftpaRespondentOutOfTimeDocuments_Transformed"),
            )
    )
    return documents_df, documents_audit

################################################################

################################################################
##########              ftpa          ###########
################################################################

def ftpa(silver_m3,silver_c):

    ftpa_df,ftpa_audit = DA.ftpa(silver_m3,silver_c)

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(39))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    datereceived_ts = coalesce(
        F.to_timestamp(col("DateReceived"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),  # e.g., +00:00
        F.to_timestamp(col("DateReceived"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")     # e.g., Z or +01
    )

    silver_m3_content = (
        silver_m3_max_statusid
            # Appellant fields
            .withColumn(
                "ftpaAppellantApplicationDate",
                when(col("Party") == 1, date_format(datereceived_ts, "yyyy-MM-dd")).otherwise(None)
            )
            .withColumn(
                "ftpaAppellantSubmissionOutOfTime",
                when((col("Party") == 1) & (col("OutOfTime") == 1), lit("Yes"))
                .when((col("Party") == 1) & (col("OutOfTime") != 1), lit("No"))
                .otherwise(None)
            )
            .withColumn(
                "ftpaAppellantOutOfTimeExplanation",
                when((col("Party") == 1) & (col("OutOfTime") == 1),
                    lit("This is a migrated ARIA case. Please refer to the documents.")
                ).otherwise(None)
            )

            # Respondent fields
            .withColumn(
                "ftpaRespondentApplicationDate",
                when(col("Party") == 2, date_format(datereceived_ts, "yyyy-MM-dd")).otherwise(None)
            )
            .withColumn(
                "ftpaRespondentSubmissionOutOfTime",
                when((col("Party") == 2) & (col("OutOfTime") == 1), lit("Yes"))
                .when((col("Party") == 2) & (col("OutOfTime") != 1), lit("No"))
                .otherwise(None)
            )
            .withColumn(
                "ftpaRespondentOutOfTimeExplanation",
                when((col("Party") == 2) & (col("OutOfTime") == 1),
                    lit("This is a migrated ARIA case. Please refer to the documents.")
                ).otherwise(None)
            )

            # Build ftpaList depending on Party
            .withColumn(
                "ftpaList",
                when(
                    col("Party") == 1,
                    array(
                        struct(
                            lit("1").alias("id"),
                            struct(
                                lit("appellant").alias("ftpaApplicant"),
                                col("ftpaAppellantApplicationDate").alias("ftpaApplicationDate"),
                                lit([]).cast("array<string>").alias("ftpaGroundsDocuments"),
                                lit([]).cast("array<string>").alias("ftpaEvidenceDocuments"),
                                lit([]).cast("array<string>").alias("ftpaOutOfTimeDocuments"),
                                col("ftpaAppellantOutOfTimeExplanation").alias("ftpaOutOfTimeExplanation")
                            ).alias("value")
                        )
                    )
                ).when(
                    col("Party") == 2,
                    array(
                        struct(
                            lit("1").alias("id"),
                            struct(
                                lit("respondent").alias("ftpaApplicant"),
                                col("ftpaRespondentApplicationDate").alias("ftpaApplicationDate"),
                                lit([]).cast("array<string>").alias("ftpaGroundsDocuments"),
                                lit([]).cast("array<string>").alias("ftpaEvidenceDocuments"),
                                lit([]).cast("array<string>").alias("ftpaOutOfTimeDocuments"),
                                col("ftpaRespondentOutOfTimeExplanation").alias("ftpaOutOfTimeExplanation")
                            ).alias("value")
                        )
                    )
                ).otherwise(None)
            )

            .select(
                col("CaseNo"),
                col("ftpaList"),
                col("ftpaAppellantApplicationDate"),
                col("ftpaAppellantSubmissionOutOfTime"),
                col("ftpaAppellantOutOfTimeExplanation"),
                col("ftpaRespondentApplicationDate"),
                col("ftpaRespondentSubmissionOutOfTime"),
                col("ftpaRespondentOutOfTimeExplanation"),
            )
    )

    ftpa_df = (
        silver_m3_content.alias("m3")
            .join(ftpa_df.alias("ftpa"), on=["CaseNo"], how="left")
            .select(
                col("m3.CaseNo"),
                *[col(f"ftpa.{c}") for c in ftpa_df.columns if c != "CaseNo"],
                col("ftpaList"),
                col("ftpaAppellantApplicationDate"),
                col("ftpaAppellantSubmissionOutOfTime"),
                col("ftpaAppellantOutOfTimeExplanation"),
                col("ftpaRespondentApplicationDate"),
                col("ftpaRespondentSubmissionOutOfTime"),
                col("ftpaRespondentOutOfTimeExplanation"),
            )
    )

    # Build the audit DataFrame
    ftpa_audit = (
        ftpa_audit.alias("audit")
            .join(ftpa_df.alias("ftpa"), on=["CaseNo"], how="left")
            .join(silver_m3_max_statusid.alias("m3"), on=["CaseNo"], how="left")
            .select(
                "audit.*",
                array(struct(lit("DateReceived"),lit("Party"),lit("OutOfTime"))).alias("ftpaList_inputFields"),
                array(struct(col("DateReceived"),col("Party"),col("OutOfTime"))).alias("ftpaList_inputValues"),
                col("ftpaList").alias("ftpaList_value"),
                lit("Yes").alias("ftpaList_Transformation"),

                array(struct(lit("DateReceived"),lit("Party"),lit("OutOfTime"))).alias("ftpaAppellantApplicationDate_inputFields"),
                array(struct(col("DateReceived"),col("Party"),col("OutOfTime"))).alias("ftpaAppellantApplicationDate_inputValues"),
                col("ftpaAppellantApplicationDate").alias("ftpaAppellantApplicationDate_value"),
                lit("Yes").alias("ftpaAppellantApplicationDate_Transformation"),

                array(struct(lit("DateReceived"),lit("Party"),lit("OutOfTime"))).alias("ftpaAppellantSubmissionOutOfTime_inputFields"),
                array(struct(col("DateReceived"),col("Party"),col("OutOfTime"))).alias("ftpaAppellantSubmissionOutOfTime_inputValues"),
                col("ftpaAppellantSubmissionOutOfTime").alias("ftpaAppellantSubmissionOutOfTime_value"),
                lit("Yes").alias("ftpaAppellantSubmissionOutOfTime_Transformation"),

                array(struct(lit("DateReceived"),lit("Party"),lit("OutOfTime"))).alias("ftpaAppellantOutOfTimeExplanation_inputFields"),
                array(struct(col("DateReceived"),col("Party"),col("OutOfTime"))).alias("ftpaAppellantOutOfTimeExplanation_inputValues"),
                col("ftpaAppellantOutOfTimeExplanation").alias("ftpaAppellantOutOfTimeExplanation_value"),
                lit("Yes").alias("ftpaAppellantOutOfTimeExplanation_Transformation"),

                array(struct(lit("DateReceived"),lit("Party"),lit("OutOfTime"))).alias("ftpaRespondentApplicationDate_inputFields"),
                array(struct(col("DateReceived"),col("Party"),col("OutOfTime"))).alias("ftpaRespondentApplicationDate_inputValues"),
                col("ftpaRespondentApplicationDate").alias("ftpaRespondentApplicationDate_value"),
                lit("Yes").alias("ftpaRespondentApplicationDate_Transformation"),

                array(struct(lit("DateReceived"),lit("Party"),lit("OutOfTime"))).alias("ftpaRespondentSubmissionOutOfTime_inputFields"),
                array(struct(col("DateReceived"),col("Party"),col("OutOfTime"))).alias("ftpaRespondentSubmissionOutOfTime_inputValues"),
                col("ftpaRespondentSubmissionOutOfTime").alias("ftpaRespondentSubmissionOutOfTime_value"),
                lit("Yes").alias("ftpaRespondentSubmissionOutOfTime_Transformation"),

                array(struct(lit("DateReceived"),lit("Party"),lit("OutOfTime"))).alias("ftpaRespondentOutOfTimeExplanation_inputFields"),
                array(struct(col("DateReceived"),col("Party"),col("OutOfTime"))).alias("ftpaRespondentOutOfTimeExplanation_inputValues"),
                col("ftpaRespondentOutOfTimeExplanation").alias("ftpaRespondentOutOfTimeExplanation_value"),
                lit("Yes").alias("ftpaRespondentOutOfTimeExplanation_Transformation"),
            )
    )
       
    return ftpa_df, ftpa_audit
################################################################

################################################################

################################################################
##########              general          ###########
################################################################

def general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres):

    general_df,general_audit = D.general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres)

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())
    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(39))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    silver_m3_content = (
        silver_m3_max_statusid

            # ---------------------------
            # Appellant FTPA visibility
            # ---------------------------
            .withColumn("ftpaAppellantSubmitted",
                        when(col("Party") == 1, lit("Yes")).otherwise(None))

            .withColumn("isFtpaAppellantDocsVisibleInDecided",
                        when(col("Party") == 1, lit("No")).otherwise(None))

            .withColumn("isFtpaAppellantDocsVisibleInSubmitted",
                        when(col("Party") == 1, lit("Yes")).otherwise(None))

            .withColumn("isFtpaAppellantOotDocsVisibleInDecided",
                        when((col("Party") == 1) & (col("OutOfTime") == 1), lit("No"))
                        .otherwise(None))

            .withColumn("isFtpaAppellantOotDocsVisibleInSubmitted",
                        when((col("Party") == 1) & (col("OutOfTime") == 1), lit("Yes"))
                        .otherwise(None))

            .withColumn("isFtpaAppellantGroundsDocsVisibleInDecided",
                        when(col("Party") == 1, lit("No")).otherwise(None))

            .withColumn("isFtpaAppellantEvidenceDocsVisibleInDecided",
                        when(col("Party") == 1, lit("No")).otherwise(None))

            .withColumn("isFtpaAppellantGroundsDocsVisibleInSubmitted",
                        when(col("Party") == 1, lit("Yes")).otherwise(None))

            .withColumn("isFtpaAppellantEvidenceDocsVisibleInSubmitted",
                        when(col("Party") == 1, lit("Yes")).otherwise(None))

            .withColumn("isFtpaAppellantOotExplanationVisibleInDecided",
                        when((col("Party") == 1) & (col("OutOfTime") == 1), lit("No"))
                        .otherwise(None))

            .withColumn("isFtpaAppellantOotExplanationVisibleInSubmitted",
                        when((col("Party") == 1) & (col("OutOfTime") == 1), lit("Yes"))
                        .otherwise(None))

            # ---------------------------
            # Respondent FTPA visibility
            # ---------------------------
            .withColumn("ftpaRespondentSubmitted",
                        when(col("Party") == 2, lit("Yes")).otherwise(None))

            .withColumn("isFtpaRespondentDocsVisibleInDecided",
                        when(col("Party") == 2, lit("No")).otherwise(None))

            .withColumn("isFtpaRespondentDocsVisibleInSubmitted",
                        when(col("Party") == 2, lit("Yes")).otherwise(None))

            .withColumn("isFtpaRespondentOotDocsVisibleInDecided",
                        when((col("Party") == 2) & (col("OutOfTime") == 1), lit("No"))
                        .otherwise(None))

            .withColumn("isFtpaRespondentOotDocsVisibleInSubmitted",
                        when((col("Party") == 2) & (col("OutOfTime") == 1), lit("Yes"))
                        .otherwise(None))

            .withColumn("isFtpaRespondentGroundsDocsVisibleInDecided",
                        when(col("Party") == 2, lit("No")).otherwise(None))

            .withColumn("isFtpaRespondentEvidenceDocsVisibleInDecided",
                        when(col("Party") == 2, lit("No")).otherwise(None))

            .withColumn("isFtpaRespondentGroundsDocsVisibleInSubmitted",
                        when(col("Party") == 2, lit("Yes")).otherwise(None))

            .withColumn("isFtpaRespondentEvidenceDocsVisibleInSubmitted",
                        when(col("Party") == 2, lit("Yes")).otherwise(None))

            .withColumn("isFtpaRespondentOotExplanationVisibleInDecided",
                        when((col("Party") == 2) & (col("OutOfTime") == 1), lit("No"))
                        .otherwise(None))

            .withColumn("isFtpaRespondentOotExplanationVisibleInSubmitted",
                        when((col("Party") == 2) & (col("OutOfTime") == 1), lit("Yes"))
                        .otherwise(None))
    )

    # Select only the needed columns
    silver_m3_content = silver_m3_content.select(
        "CaseNo",
        "ftpaAppellantSubmitted",
        "isFtpaAppellantDocsVisibleInDecided",
        "isFtpaAppellantDocsVisibleInSubmitted",
        "isFtpaAppellantOotDocsVisibleInDecided",
        "isFtpaAppellantOotDocsVisibleInSubmitted",
        "isFtpaAppellantGroundsDocsVisibleInDecided",
        "isFtpaAppellantEvidenceDocsVisibleInDecided",
        "isFtpaAppellantGroundsDocsVisibleInSubmitted",
        "isFtpaAppellantEvidenceDocsVisibleInSubmitted",
        "isFtpaAppellantOotExplanationVisibleInDecided",
        "isFtpaAppellantOotExplanationVisibleInSubmitted",

        "ftpaRespondentSubmitted",
        "isFtpaRespondentDocsVisibleInDecided",
        "isFtpaRespondentDocsVisibleInSubmitted",
        "isFtpaRespondentOotDocsVisibleInDecided",
        "isFtpaRespondentOotDocsVisibleInSubmitted",
        "isFtpaRespondentGroundsDocsVisibleInDecided",
        "isFtpaRespondentEvidenceDocsVisibleInDecided",
        "isFtpaRespondentGroundsDocsVisibleInSubmitted",
        "isFtpaRespondentEvidenceDocsVisibleInSubmitted",
        "isFtpaRespondentOotExplanationVisibleInDecided",
        "isFtpaRespondentOotExplanationVisibleInSubmitted",
    )

    general_df = (
        general_df.alias("gen")
            .join(silver_m3_content.alias("m3"), on=["CaseNo"], how="left")
            .select(
                col("gen.*"),

                col("m3.ftpaAppellantSubmitted"),
                col("m3.isFtpaAppellantDocsVisibleInDecided"),
                col("m3.isFtpaAppellantDocsVisibleInSubmitted"),
                col("m3.isFtpaAppellantOotDocsVisibleInDecided"),
                col("m3.isFtpaAppellantOotDocsVisibleInSubmitted"),
                col("m3.isFtpaAppellantGroundsDocsVisibleInDecided"),
                col("m3.isFtpaAppellantEvidenceDocsVisibleInDecided"),
                col("m3.isFtpaAppellantGroundsDocsVisibleInSubmitted"),
                col("m3.isFtpaAppellantEvidenceDocsVisibleInSubmitted"),
                col("m3.isFtpaAppellantOotExplanationVisibleInDecided"),
                col("m3.isFtpaAppellantOotExplanationVisibleInSubmitted"),

                col("m3.ftpaRespondentSubmitted"),
                col("m3.isFtpaRespondentDocsVisibleInDecided"),
                col("m3.isFtpaRespondentDocsVisibleInSubmitted"),
                col("m3.isFtpaRespondentOotDocsVisibleInDecided"),
                col("m3.isFtpaRespondentOotDocsVisibleInSubmitted"),
                col("m3.isFtpaRespondentGroundsDocsVisibleInDecided"),
                col("m3.isFtpaRespondentEvidenceDocsVisibleInDecided"),
                col("m3.isFtpaRespondentGroundsDocsVisibleInSubmitted"),
                col("m3.isFtpaRespondentEvidenceDocsVisibleInSubmitted"),
                col("m3.isFtpaRespondentOotExplanationVisibleInDecided"),
                col("m3.isFtpaRespondentOotExplanationVisibleInSubmitted"),
            )
    )

    general_audit = (
        general_audit.alias("audit")
            .join(general_df.alias("gen"), on=["CaseNo"], how="left")
            .join(silver_m3_max_statusid.alias("m3"), on=["CaseNo"], how="left")
            .select(
                "audit.*",

                # -------------------------------------------------------------
                # 1. ftpaAppellantSubmitted
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("OutOfTime"))).alias("ftpaAppellantSubmitted_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("ftpaAppellantSubmitted_inputValues"),
                col("ftpaAppellantSubmitted").alias("ftpaAppellantSubmitted_value"),
                lit("Yes").alias("ftpaAppellantSubmitted_Transformation"),

                # -------------------------------------------------------------
                # 2. isFtpaAppellantDocsVisibleInDecided
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaAppellantDocsVisibleInDecided_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaAppellantDocsVisibleInDecided_inputValues"),
                col("isFtpaAppellantDocsVisibleInDecided").alias("isFtpaAppellantDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaAppellantDocsVisibleInDecided_Transformation"),

                # -------------------------------------------------------------
                # 3. isFtpaAppellantDocsVisibleInSubmitted
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaAppellantDocsVisibleInSubmitted_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaAppellantDocsVisibleInSubmitted_inputValues"),
                col("isFtpaAppellantDocsVisibleInSubmitted").alias("isFtpaAppellantDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaAppellantDocsVisibleInSubmitted_Transformation"),

                # -------------------------------------------------------------
                # 4. isFtpaAppellantOotDocsVisibleInDecided
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaAppellantOotDocsVisibleInDecided_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaAppellantOotDocsVisibleInDecided_inputValues"),
                col("isFtpaAppellantOotDocsVisibleInDecided").alias("isFtpaAppellantOotDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaAppellantOotDocsVisibleInDecided_Transformation"),

                # -------------------------------------------------------------
                # 5. isFtpaAppellantOotDocsVisibleInSubmitted
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaAppellantOotDocsVisibleInSubmitted_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaAppellantOotDocsVisibleInSubmitted_inputValues"),
                col("isFtpaAppellantOotDocsVisibleInSubmitted").alias("isFtpaAppellantOotDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaAppellantOotDocsVisibleInSubmitted_Transformation"),

                # -------------------------------------------------------------
                # 6. isFtpaAppellantGroundsDocsVisibleInDecided
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaAppellantGroundsDocsVisibleInDecided_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaAppellantGroundsDocsVisibleInDecided_inputValues"),
                col("isFtpaAppellantGroundsDocsVisibleInDecided").alias("isFtpaAppellantGroundsDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaAppellantGroundsDocsVisibleInDecided_Transformation"),

                # -------------------------------------------------------------
                # 7. isFtpaAppellantEvidenceDocsVisibleInDecided
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaAppellantEvidenceDocsVisibleInDecided_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaAppellantEvidenceDocsVisibleInDecided_inputValues"),
                col("isFtpaAppellantEvidenceDocsVisibleInDecided").alias("isFtpaAppellantEvidenceDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaAppellantEvidenceDocsVisibleInDecided_Transformation"),

                # -------------------------------------------------------------
                # 8. isFtpaAppellantGroundsDocsVisibleInSubmitted
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaAppellantGroundsDocsVisibleInSubmitted_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaAppellantGroundsDocsVisibleInSubmitted_inputValues"),
                col("isFtpaAppellantGroundsDocsVisibleInSubmitted").alias("isFtpaAppellantGroundsDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaAppellantGroundsDocsVisibleInSubmitted_Transformation"),

                # -------------------------------------------------------------
                # 9. isFtpaAppellantEvidenceDocsVisibleInSubmitted
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaAppellantEvidenceDocsVisibleInSubmitted_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaAppellantEvidenceDocsVisibleInSubmitted_inputValues"),
                col("isFtpaAppellantEvidenceDocsVisibleInSubmitted").alias("isFtpaAppellantEvidenceDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaAppellantEvidenceDocsVisibleInSubmitted_Transformation"),

                # -------------------------------------------------------------
                # 10. isFtpaAppellantOotExplanationVisibleInDecided
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaAppellantOotExplanationVisibleInDecided_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaAppellantOotExplanationVisibleInDecided_inputValues"),
                col("isFtpaAppellantOotExplanationVisibleInDecided").alias("isFtpaAppellantOotExplanationVisibleInDecided_value"),
                lit("Yes").alias("isFtpaAppellantOotExplanationVisibleInDecided_Transformation"),

                # -------------------------------------------------------------
                # 11. isFtpaAppellantOotExplanationVisibleInSubmitted
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaAppellantOotExplanationVisibleInSubmitted_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaAppellantOotExplanationVisibleInSubmitted_inputValues"),
                col("isFtpaAppellantOotExplanationVisibleInSubmitted").alias("isFtpaAppellantOotExplanationVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaAppellantOotExplanationVisibleInSubmitted_Transformation"),


                # -------------------------------------------------------------
                # RESPONDENT FIELDS (12–21)
                # -------------------------------------------------------------

                # ftpaRespondentSubmitted
                array(struct(lit("Party"), lit("OutOfTime"))).alias("ftpaRespondentSubmitted_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("ftpaRespondentSubmitted_inputValues"),
                col("ftpaRespondentSubmitted").alias("ftpaRespondentSubmitted_value"),
                lit("Yes").alias("ftpaRespondentSubmitted_Transformation"),

                # isFtpaRespondentDocsVisibleInDecided
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaRespondentDocsVisibleInDecided_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaRespondentDocsVisibleInDecided_inputValues"),
                col("isFtpaRespondentDocsVisibleInDecided").alias("isFtpaRespondentDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaRespondentDocsVisibleInDecided_Transformation"),

                # isFtpaRespondentDocsVisibleInSubmitted
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaRespondentDocsVisibleInSubmitted_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaRespondentDocsVisibleInSubmitted_inputValues"),
                col("isFtpaRespondentDocsVisibleInSubmitted").alias("isFtpaRespondentDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaRespondentDocsVisibleInSubmitted_Transformation"),

                # isFtpaRespondentOotDocsVisibleInDecided
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaRespondentOotDocsVisibleInDecided_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaRespondentOotDocsVisibleInDecided_inputValues"),
                col("isFtpaRespondentOotDocsVisibleInDecided").alias("isFtpaRespondentOotDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaRespondentOotDocsVisibleInDecided_Transformation"),

                # isFtpaRespondentOotDocsVisibleInSubmitted
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaRespondentOotDocsVisibleInSubmitted_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaRespondentOotDocsVisibleInSubmitted_inputValues"),
                col("isFtpaRespondentOotDocsVisibleInSubmitted").alias("isFtpaRespondentOotDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaRespondentOotDocsVisibleInSubmitted_Transformation"),

                # isFtpaRespondentGroundsDocsVisibleInDecided
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaRespondentGroundsDocsVisibleInDecided_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaRespondentGroundsDocsVisibleInDecided_inputValues"),
                col("isFtpaRespondentGroundsDocsVisibleInDecided").alias("isFtpaRespondentGroundsDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaRespondentGroundsDocsVisibleInDecided_Transformation"),

                # isFtpaRespondentEvidenceDocsVisibleInDecided
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaRespondentEvidenceDocsVisibleInDecided_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaRespondentEvidenceDocsVisibleInDecided_inputValues"),
                col("isFtpaRespondentEvidenceDocsVisibleInDecided").alias("isFtpaRespondentEvidenceDocsVisibleInDecided_value"),
                lit("Yes").alias("isFtpaRespondentEvidenceDocsVisibleInDecided_Transformation"),

                # isFtpaRespondentGroundsDocsVisibleInSubmitted
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaRespondentGroundsDocsVisibleInSubmitted_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaRespondentGroundsDocsVisibleInSubmitted_inputValues"),
                col("isFtpaRespondentGroundsDocsVisibleInSubmitted").alias("isFtpaRespondentGroundsDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaRespondentGroundsDocsVisibleInSubmitted_Transformation"),

                # isFtpaRespondentEvidenceDocsVisibleInSubmitted
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaRespondentEvidenceDocsVisibleInSubmitted_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaRespondentEvidenceDocsVisibleInSubmitted_inputValues"),
                col("isFtpaRespondentEvidenceDocsVisibleInSubmitted").alias("isFtpaRespondentEvidenceDocsVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaRespondentEvidenceDocsVisibleInSubmitted_Transformation"),

                # isFtpaRespondentOotExplanationVisibleInDecided
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaRespondentOotExplanationVisibleInDecided_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaRespondentOotExplanationVisibleInDecided_inputValues"),
                col("isFtpaRespondentOotExplanationVisibleInDecided").alias("isFtpaRespondentOotExplanationVisibleInDecided_value"),
                lit("Yes").alias("isFtpaRespondentOotExplanationVisibleInDecided_Transformation"),

                # isFtpaRespondentOotExplanationVisibleInSubmitted
                array(struct(lit("Party"), lit("OutOfTime"))).alias("isFtpaRespondentOotExplanationVisibleInSubmitted_inputFields"),
                array(struct(col("Party"), col("OutOfTime"))).alias("isFtpaRespondentOotExplanationVisibleInSubmitted_inputValues"),
                col("isFtpaRespondentOotExplanationVisibleInSubmitted").alias("isFtpaRespondentOotExplanationVisibleInSubmitted_value"),
                lit("Yes").alias("isFtpaRespondentOotExplanationVisibleInSubmitted_Transformation"),
            )
    )

    return general_df, general_audit

################################################################

################################################################
##########              generalDefault          ###########
################################################################

def generalDefault(silver_m1):

    general_df = DA.generalDefault(silver_m1)

    general_df = (
        general_df
        .withColumn("isFtpaListVisible", lit("Yes"))
    )

    return general_df
################################################################

################################################################   

if __name__ == "__main__":
    pass