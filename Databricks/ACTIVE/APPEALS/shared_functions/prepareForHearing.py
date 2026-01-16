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

from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains,rank,create_map, map_from_entries, map_from_arrays
)


################################################################
##########              hearingResponse          ###########
################################################################

def hearingResponse(silver_m1, silver_m3, silver_m6):

    # # Define window partitioned by CaseNo and ordered by descending StatusId
    # window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # # Add row_number to get the row with the highest StatusId per CaseNo
    # silver_m3_ranked = silver_m3.withColumn("row_number", row_number().over(window_spec))
    # silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")
    # silver_m3_filtered_casestatus = silver_m3_max_statusid.filter(col("CaseStatus").isin(37, 38))

    window = Window.partitionBy("CaseNo").orderBy(F.desc("StatusId"))
    df_stg = silver_m3.filter((F.col("CaseStatus").isin([37,38]))).withColumn("rn",F.row_number().over(window)).filter(F.col("rn") == 1).drop(F.col("rn"))

    m3_df = df_stg.withColumn("CourtClerkFull",
                                            when((col("CourtClerk_Surname").isNotNull()) & (col("CourtClerk_Surname") != ""), concat_ws(" ", col("CourtClerk_Surname"), col("CourtClerk_Forenames"),
        when((col("CourtClerk_Title").isNotNull()) & (col("CourtClerk_Title") != ""),
            concat(lit("("), col("CourtClerk_Title"), lit(")"))).otherwise(lit(None))))
            ).withColumn("isAppealSuitableToFloat",when(col("ListTypeId") == 5, lit("Yes")).otherwise("No"))
 
    stg_m6 = silver_m6.withColumn("Transformed_Required",when(F.col("Required") == 0, lit('Not Required')).when(F.col("Required") == 1, lit('Required')))

    final_df = m3_df.join(silver_m1, ["CaseNo"], "left").join(stg_m6, ["CaseNo"], "left").withColumn("CaseNo", trim(col("CaseNo"))
                    ).withColumn("Hearing Centre",
                                when(col("HearingCentre").isNull(), "N/A").otherwise(col("HearingCentre")) #ListedCentre
                    ).withColumn("Hearing Date",
                                when(col("HearingDate").isNull(), "N/A").otherwise(col("HearingDate")) #KeyDate
                    ).withColumn("Hearing Type",
                                when(col("HearingType").isNull(), "N/A").otherwise(col("HearingType"))
                    ).withColumn("Court",
                                when(col("CourtName").isNull(), "N/A").otherwise(col("CourtName"))
                    ).withColumn("List Type",
                                when(col("ListType").isNull(), "N/A").otherwise(col("ListType"))
                    ).withColumn("List Start Time",
                                when(col("StartTime").isNull(), "N/A").otherwise(col("StartTime"))
                    ).withColumn("Judge First Tier",
                                when(coalesce(col("Judge1FT_Surname"), col("Judge2FT_Surname"), col("Judge3FT_Surname")).isNotNull(),
                                trim(concat_ws(" ",
                                when(col("Judge1FT_Surname").isNotNull(),
                                    concat_ws(" ", col("Judge1FT_Surname"), col("Judge1FT_Forenames"),
                                    when(col("Judge1FT_Title").isNotNull() & (col("Judge1FT_Title") != ""),
                                        concat(lit("("), col("Judge1FT_Title"), lit(")"))).otherwise(lit("")))).otherwise(lit("")),
 
                                when(col("Judge2FT_Surname").isNotNull(),
                                    concat_ws(" ", col("Judge2FT_Surname"), col("Judge2FT_Forenames"),
                                        when(col("Judge2FT_Title").isNotNull() & (col("Judge2FT_Title") != ""),
                                            concat(lit("("), col("Judge2FT_Title"), lit(")"))).otherwise(lit("")))).otherwise(lit("")),
                
                                when(col("Judge3FT_Surname").isNotNull(),
                                    concat_ws(" ", col("Judge3FT_Surname"), col("Judge3FT_Forenames"),
                                        when(col("Judge3FT_Title").isNotNull() & (col("Judge3FT_Title") != ""),
                                            concat(lit("("), col("Judge3FT_Title"), lit(")"))).otherwise(lit("")))
                                    ).otherwise(lit(""))))
                                ).otherwise(lit(None))
                                
                    ).withColumn("Start Time",
                                when(col("StartTime").isNull(), "N/A").otherwise(col("StartTime"))
                    ).withColumn("Estimated Duration",
                                when(col("TimeEstimate").isNull(), "N/A").otherwise(col("TimeEstimate").cast("string"))
                    ).withColumn("Required/Incompatible Judicial Officers", concat_ws(" ", col("Judge_Surname"), col("Judge_Forenames")
                    , when(col("Judge_Title").isNotNull(),"("),
                    col("Judge_Title"),
                    when(col("Judge_Title").isNotNull(),")"),
                    when(col("Transformed_Required").isNotNull(),":"), col("Transformed_Required"))
                    ).withColumn("Notes",
                                when(col("Notes").isNull(), "N/A").otherwise(col("Notes"))
                    ).withColumn("additionalInstructionsTribunalResponse",
                                concat(
                                lit("Listed details from ARIA: "),
                                lit("\n Hearing Centre: "), coalesce(col("Hearing Centre"), lit("N/A")),
                                lit("\n Hearing Date: "), coalesce(col("Hearing Date"), lit("N/A")),
                                lit("\n Hearing Type: "), coalesce(col("Hearing Type"), lit("N/A")),
                                lit("\n Court: "), coalesce(col("Court"), lit("N/A")),
                                lit("\n List Type: "), coalesce(col("ListType"), lit("N/A")),
                                lit("\n List Start Time: "), coalesce(col("List Start Time"), lit("N/A")),
                                lit("\n Judge First Tier: "), coalesce(col("Judge First Tier"), lit('')),
                                lit("\n Court Clerk / Usher: "), expr("coalesce(nullif(concat_ws(', ', CourtClerkFull), ''), 'N/A')"),
                                lit("\n Start Time: "), coalesce(col("Start Time"), lit("N/A")),
                                lit("\n Estimated Duration: "), coalesce(col("Estimated Duration"), lit("N/A")),
                                lit("\n Required/Incompatible Judicial Officers: "), coalesce(col("Required/Incompatible Judicial Officers"), lit("N/A")),
                                lit("\n Notes: "), coalesce(col("Notes"), lit("N/A"))
                            )
                                
                    )
    content_df = final_df.select(col("CaseNo"),col("additionalInstructionsTribunalResponse"),col("isAppealSuitableToFloat"),col("ListTypeId"))


    df_hearingResponse = (
        silver_m1.alias("m1")
        .join(content_df.alias("m3"), ["CaseNo"], "left")
        # .join(silver_m6.alias("m6"), ["CaseNo"], "left")
        # .join(content_df.alias("m3_content"), ["CaseNo"], "left")
        .withColumn("isRemoteHearing", lit("No"))
        .withColumn("isMultimediaAllowed", lit("Granted"))
        .withColumn("multimediaTribunalResponse", lit("This is a migrated ARIA case. Please refer to the documents."))
        .withColumn("multimediaDecisionForDisplay", lit("Granted - This is a migrated ARIA case. Please refer to the documents."))
        .withColumn(
            "isInCameraCourtAllowed",
            when(col("m1.InCamera") == True, lit("Granted")).otherwise(lit(None))
        )
        .withColumn(
            "inCameraCourtTribunalResponse",
            when(col("m1.InCamera") == True,
                 lit("This is a migrated ARIA case. Please refer to the documents."))
            .otherwise(lit(None))
        )
        .withColumn(
            "inCameraCourtDecisionForDisplay",
            when(col("m1.InCamera") == True,
                 lit("Granted - This is a migrated ARIA case. Please refer to the documents."))
            .otherwise(lit(None))
        )
        .withColumn(
            "isSingleSexCourtAllowed",
            when(col("m1.CourtPreference").isin([1, 2]), lit("Granted")).otherwise(lit(None))
        )
        .withColumn(
            "singleSexCourtTribunalResponse",
            when(col("m1.CourtPreference").isin([1, 2]),
                 lit("This is a migrated ARIA case. Please refer to the documents."))
            .otherwise(lit(None))
        )
        .withColumn(
            "singleSexCourtDecisionForDisplay",
            when(col("m1.CourtPreference").isin([1, 2]),
                 lit("Granted - This is a migrated ARIA case. Please refer to the documents."))
            .otherwise(lit(None))
        )
        .withColumn("isVulnerabilitiesAllowed", lit("Granted"))
        .withColumn("vulnerabilitiesTribunalResponse", lit("This is a migrated ARIA case. Please refer to the documents."))
        .withColumn("vulnerabilitiesDecisionForDisplay", lit("Granted - This is a migrated ARIA case. Please refer to the documents."))
        .withColumn("isRemoteHearingAllowed", lit("Granted"))
        .withColumn("remoteVideoCallTribunalResponse", lit("This is a migrated ARIA case. Please refer to the documents."))
        .withColumn("remoteHearingDecisionForDisplay", lit("Granted - This is a migrated ARIA case. Please refer to the documents."))
        .withColumn("isAdditionalAdjustmentsAllowed", lit("Granted"))
        .withColumn("additionalTribunalResponse", lit("This is a migrated ARIA case. Please refer to the documents."))
        .withColumn("otherDecisionForDisplay", lit("Granted - This is a migrated ARIA case. Please refer to the documents."))
        .withColumn("isAdditionalInstructionAllowed", lit("Yes"))
        .select(
        col("m1.CaseNo").alias("CaseNo"),
        col("isRemoteHearing"),
        col("m3.isAppealSuitableToFloat"),
        col("isMultimediaAllowed"),
        col("multimediaTribunalResponse"),
        col("multimediaDecisionForDisplay"),
        col("isInCameraCourtAllowed"),
        col("inCameraCourtTribunalResponse"),
        col("inCameraCourtDecisionForDisplay"),
        col("isSingleSexCourtAllowed"),
        col("singleSexCourtTribunalResponse"),
        col("singleSexCourtDecisionForDisplay"),
        col("isVulnerabilitiesAllowed"),
        col("vulnerabilitiesTribunalResponse"),
        col("vulnerabilitiesDecisionForDisplay"),
        col("isRemoteHearingAllowed"),
        col("remoteVideoCallTribunalResponse"),
        col("remoteHearingDecisionForDisplay"),
        col("isAdditionalAdjustmentsAllowed"),
        col("additionalTribunalResponse"),
        col("otherDecisionForDisplay"),
        col("isAdditionalInstructionAllowed"),
        col("m3.additionalInstructionsTribunalResponse")
        )
    )

    df_audit_hearingResponse = (
        df_hearingResponse.alias("hr")
        .join(silver_m1.alias("m1"), ["CaseNo"], "left")
        .join(content_df.alias("m3"), ["CaseNo"], "left")
        .join(final_df.alias("f"), ["CaseNo"], "left")
        .select(
                "hr.CaseNo",
                # isRemoteHearing
                array(struct()).alias("isRemoteHearing_inputFields"),
                array(struct()).alias("isRemoteHearing_inputValues"),
                col("hr.isRemoteHearing"),
                lit("Yes").alias("isRemoteHearing_Transformed"),
                # isAppealSuitableToFloat
                array(struct(lit("ListTypeId"))).alias("isAppealSuitableToFloat_inputFields"),
                array(struct(col("m3.ListTypeId"))).alias("isAppealSuitableToFloat_inputValues"),
                col("hr.isAppealSuitableToFloat"),
                lit("Yes").alias("isAppealSuitableToFloat_Transformed"),
                # isMultimediaAllowed
                array(struct()).alias("isMultimediaAllowed_inputFields"),
                array(struct()).alias("isMultimediaAllowed_inputValues"),
                col("hr.isMultimediaAllowed"),
                lit("Yes").alias("isMultimediaAllowed_Transformed"),
                # multimediaTribunalResponse
                array(struct()).alias("multimediaTribunalResponse_inputFields"),
                array(struct()).alias("multimediaTribunalResponse_inputValues"),
                col("hr.multimediaTribunalResponse"),
                lit("Yes").alias("multimediaTribunalResponse_Transformed"),
                # multimediaDecisionForDisplay
                array(struct()).alias("multimediaDecisionForDisplay_inputFields"),
                array(struct()).alias("multimediaDecisionForDisplay_inputValues"),
                col("hr.multimediaDecisionForDisplay"),
                lit("Yes").alias("multimediaDecisionForDisplay_Transformed"),
                # isInCameraCourtAllowed
                array(struct(lit("InCamera"))).alias("isInCameraCourtAllowed_inputFields"),
                array(struct(col("m1.InCamera"))).alias("isInCameraCourtAllowed_inputValues"),
                col("hr.isInCameraCourtAllowed"),
                lit("Yes").alias("isInCameraCourtAllowed_Transformed"),
                # inCameraCourtTribunalResponse
                array(struct(lit("InCamera"))).alias("inCameraCourtTribunalResponse_inputFields"),
                array(struct(col("m1.InCamera"))).alias("inCameraCourtTribunalResponse_inputValues"),
                col("hr.inCameraCourtTribunalResponse"),
                lit("Yes").alias("inCameraCourtTribunalResponse_Transformed"),
                # inCameraCourtDecisionForDisplay
                array(struct(lit("InCamera"))).alias("inCameraCourtDecisionForDisplay_inputFields"),
                array(struct(col("m1.InCamera"))).alias("inCameraCourtDecisionForDisplay_inputValues"),
                col("hr.inCameraCourtDecisionForDisplay"),
                lit("Yes").alias("inCameraCourtDecisionForDisplay_Transformed"),
                # isSingleSexCourtAllowed
                array(struct(lit("CourtPreference"))).alias("isSingleSexCourtAllowed_inputFields"),
                array(struct(col("m1.CourtPreference"))).alias("isSingleSexCourtAllowed_inputValues"),
                col("hr.isSingleSexCourtAllowed"),
                lit("Yes").alias("isSingleSexCourtAllowed_Transformed"),
                # singleSexCourtTribunalResponse
                array(struct(lit("CourtPreference"))).alias("singleSexCourtTribunalResponse_inputFields"),
                array(struct(col("m1.CourtPreference"))).alias("singleSexCourtTribunalResponse_inputValues"),
                col("hr.singleSexCourtTribunalResponse"),
                lit("Yes").alias("singleSexCourtTribunalResponse_Transformed"),
                # singleSexCourtDecisionForDisplay
                array(struct(lit("CourtPreference"))).alias("singleSexCourtDecisionForDisplay_inputFields"),
                array(struct(col("m1.CourtPreference"))).alias("singleSexCourtDecisionForDisplay_inputValues"),
                col("hr.singleSexCourtDecisionForDisplay"),
                lit("Yes").alias("singleSexCourtDecisionForDisplay_Transformed"),
                # isVulnerabilitiesAllowed
                array(struct()).alias("isVulnerabilitiesAllowed_inputFields"),
                array(struct()).alias("isVulnerabilitiesAllowed_inputValues"),
                col("hr.isVulnerabilitiesAllowed"),
                lit("Yes").alias("isVulnerabilitiesAllowed_Transformed"),
                # vulnerabilitiesTribunalResponse
                array(struct()).alias("vulnerabilitiesTribunalResponse_inputFields"),
                array(struct()).alias("vulnerabilitiesTribunalResponse_inputValues"),
                col("hr.vulnerabilitiesTribunalResponse"),
                lit("Yes").alias("vulnerabilitiesTribunalResponse_Transformed"),
                # vulnerabilitiesDecisionForDisplay
                array(struct()).alias("vulnerabilitiesDecisionForDisplay_inputFields"),
                array(struct()).alias("vulnerabilitiesDecisionForDisplay_inputValues"),
                col("hr.vulnerabilitiesDecisionForDisplay"),
                lit("Yes").alias("vulnerabilitiesDecisionForDisplay_Transformed"),
                # isRemoteHearingAllowed
                array(struct()).alias("isRemoteHearingAllowed_inputFields"),
                array(struct()).alias("isRemoteHearingAllowed_inputValues"),
                col("hr.isRemoteHearingAllowed"),
                lit("Yes").alias("isRemoteHearingAllowed_Transformed"),
                # remoteVideoCallTribunalResponse
                array(struct()).alias("remoteVideoCallTribunalResponse_inputFields"),
                array(struct()).alias("remoteVideoCallTribunalResponse_inputValues"),
                col("hr.remoteVideoCallTribunalResponse"),
                lit("Yes").alias("remoteVideoCallTribunalResponse_Transformed"),
                # isAdditionalAdjustmentsAllowed
                array(struct()).alias("isAdditionalAdjustmentsAllowed_inputFields"),
                array(struct()).alias("isAdditionalAdjustmentsAllowed_inputValues"),
                col("hr.isAdditionalAdjustmentsAllowed"),
                lit("Yes").alias("isAdditionalAdjustmentsAllowed_Transformed"),
                # additionalTribunalResponse
                array(struct()).alias("additionalTribunalResponse_inputFields"),
                array(struct()).alias("additionalTribunalResponse_inputValues"),
                col("hr.additionalTribunalResponse"),
                lit("Yes").alias("additionalTribunalResponse_Transformed"),
                # otherDecisionForDisplay
                array(struct()).alias("otherDecisionForDisplay_inputFields"),
                array(struct()).alias("otherDecisionForDisplay_inputValues"),
                col("hr.otherDecisionForDisplay"),
                lit("Yes").alias("otherDecisionForDisplay_Transformed"),
                # isAdditionalInstructionAllowed
                array(struct()).alias("isAdditionalInstructionAllowed_inputFields"),
                array(struct()).alias("isAdditionalInstructionAllowed_inputValues"),
                col("hr.isAdditionalInstructionAllowed"),
                lit("Yes").alias("isAdditionalInstructionAllowed_Transformed"),
                # additionalInstructionsTribunalResponse
                # array(struct()).alias("additionalInstructionsTribunalResponse_inputFields"),
                # array(struct()).alias("additionalInstructionsTribunalResponse_inputValues"),
                # col("hr.additionalInstructionsTribunalResponse"),
                # lit("Yes").alias("additionalInstructionsTribunalResponse_Transformed"),
                array(
                struct(
                    lit("Hearing Centre").alias("field"),
                    array(lit("HearingCentre")).alias("source_columns")
                ),
                struct(
                    lit("Hearing Date").alias("field"),
                    array(lit("HearingDate")).alias("source_columns")
                ),
                struct(
                    lit("Hearing Type").alias("field"),
                    array(lit("HearingType")).alias("source_columns")
                ),
                struct(
                    lit("Court").alias("field"),
                    array(lit("CourtName")).alias("source_columns")
                ),
                struct(
                    lit("List Type").alias("field"),
                    array(lit("ListType")).alias("source_columns")
                ),
                struct(
                    lit("List Start Time").alias("field"),
                    array(lit("StartTime")).alias("source_columns")
                ),
                struct(
                    lit("Judge First Tier").alias("field"),
                    array(
                        lit("Judge1FT_Surname"), lit("Judge1FT_Forenames"), lit("Judge1FT_Title"),
                        lit("Judge2FT_Surname"), lit("Judge2FT_Forenames"), lit("Judge2FT_Title"),
                        lit("Judge3FT_Surname"), lit("Judge3FT_Forenames"), lit("Judge3FT_Title")
                    ).alias("source_columns")
                ),
                struct(
                    lit("Start Time").alias("field"),
                    array(lit("StartTime")).alias("source_columns")
                ),
                struct(
                    lit("Estimated Duration").alias("field"),
                    array(lit("TimeEstimate")).alias("source_columns")
                ),
                struct(
                    lit("Required/Incompatible Judicial Officers").alias("field"),
                    array(lit("Judge_Surname"), lit("Judge_Forenames"), lit("Judge_Title"), lit("Transformed_Required")).alias("source_columns")
                ),
                struct(
                    lit("Notes").alias("field"),
                    array(lit("Notes")).alias("source_columns")
                ),
                struct(
                    lit("Court Clerk / Usher").alias("field"),
                    array(lit("CourtClerk_Surname"), lit("CourtClerk_Forenames"), lit("CourtClerk_Title")).alias("source_columns")
                ),
                struct(
                    lit("dv_representation").alias("field"),
                    array(lit("dv_representation")).alias("source_columns")
                ),
                # struct(
                #     lit("dv_CCDAppealType").alias("field"),
                #     array(lit("dv_CCDAppealType")).alias("source_columns")
                # )
            ).alias("additionalInstructionsTribunalResponse_inputFields"),
            
            array(
                struct(lit("Hearing Centre").alias("field"), col("f.`Hearing Centre`").cast("string").alias("value")),
                struct(lit("Hearing Date").alias("field"), col("f.`Hearing Date`").cast("string").alias("value")),
                struct(lit("Hearing Type").alias("field"), col("f.`Hearing Type`").cast("string").alias("value")),
                struct(lit("Court").alias("field"), col("f.`Court`").cast("string").alias("value")),
                struct(lit("List Type").alias("field"), col("f.`List Type`").cast("string").alias("value")),
                struct(lit("List Start Time").alias("field"), col("f.`List Start Time`").cast("string").alias("value")),
                struct(lit("Judge First Tier").alias("field"), col("f.`Judge First Tier`").cast("string").alias("value")),
                struct(lit("Start Time").alias("field"), col("f.`Start Time`").cast("string").alias("value")),
                struct(lit("Estimated Duration").alias("field"), col("f.`Estimated Duration`").cast("string").alias("value")),
                struct(lit("Required/Incompatible Judicial Officers").alias("field"), col("f.`Required/Incompatible Judicial Officers`").cast("string").alias("value")),
                struct(lit("Notes").alias("field"), col("f.`Notes`").cast("string").alias("value")),
                struct(lit("Court Clerk / Usher").alias("field"), col("f.CourtClerkFull").cast("string").alias("value")),
                struct(lit("dv_representation").alias("field"), col("m1.`dv_representation`").cast("string").alias("value")),
                # struct(lit("dv_CCDAppealType").alias("field"), col("m1.`dv_CCDAppealType`").cast("string").alias("value"))
            ).alias("additionalInstructionsTribunalResponse_inputValues")

        )
    )

    return df_hearingResponse, df_audit_hearingResponse

################################################################
##########              hearingDetails          ###########
################################################################

def hearingDetails(silver_m1,silver_m3,bronze_listing_location):
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
    documents_df, documents_audit = L.documents(silver_m1)

    documents_df = documents_df.select("*",
                lit([]).cast("array<string>").alias("hearingDocuments"),
                lit([]).cast("array<string>").alias("letterBundleDocuments"))
    return documents_df, documents_audit

################################################################

if __name__ == "__main__":
    pass