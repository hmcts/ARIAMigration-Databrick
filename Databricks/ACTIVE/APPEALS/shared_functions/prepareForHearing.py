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

from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)

from uk_postcodes_parsing import fix, postcode_utils

################################################################
##########              hearingResponse          ###########
################################################################


def hearingResponse(silver_m1, silver_m3, silver_m6):
    df = (
        silver_m1.alias("m1")
        .join(silver_m3.alias("m3"), ["CaseNo"], "left")
        .join(silver_m6.alias("m6"), ["CaseNo"], "left")
        .withColumn(
            "isInCameraCourtAllowed",
            when(col("m1.InCamera") == True, lit("Granted")).otherwise(lit(None))
        )
        .withColumn(
            "inCameraCourtTribunalResponse",
            when(
                col("m1.InCamera") == True,
                lit("This is a migrated ARIA case. Please refer to the documents.")
            ).otherwise(lit(None))
        )
        .withColumn(
            "inCameraCourtDecisionForDisplay",
            when(
                col("m1.InCamera") == True,
                lit("Granted - This is a migrated ARIA case. Please refer to the documents.")
            ).otherwise(lit(None))
        )
        .withColumn(
            "isSingleSexCourtAllowed",
            when(
                (col("m1.CourtPreference").isin([1, 2])),
                lit("Granted")
            ).otherwise(lit(None))
        )
        .withColumn(
            "singleSexCourtTribunalResponse",
            when(
                (col("m1.CourtPreference").isin([1, 2])),
                lit("This is a migrated ARIA case. Please refer to the documents.")
            ).otherwise(lit(None))
        )
        .withColumn(
            "singleSexCourtDecisionForDisplay",
            when(
                (col("m1.CourtPreference").isin([1, 2])),
                lit("Granted - This is a migrated ARIA case. Please refer to the documents.")
            ).otherwise(lit(None))
        )
        .withColumn(
            "hearingChannel",
            when(
                col("m1.VisitVisaType") == 1,
                struct(lit("ONPPRS").alias("channelCode"), lit("On the papers").alias("channelLabel"))
            )
            .when(
                col("m1.VisitVisaType") == 2,
                struct(lit("INTER").alias("channelCode"), lit("In Person").alias("channelLabel"))
            )
            .otherwise(lit(None))
        )
        .withColumn(lit([]).cast("array<string>").alias("witnessDetails"))
        .withColumn(lit({}).cast("map<string,string>").alias("uploadTheAppealFormDocs"))

    )
    return df


# AppealType grouping
def appealType(silver_m1):
    conditions = (col("dv_representation").isin('LR', 'AIP')) & (col("lu_appealType").isNotNull())
    lr_condition = (col("dv_representation") == "LR") & (col("lu_appealType").isNotNull())
    aip_condition = (col("dv_representation") == "AIP") & (col("lu_appealType").isNotNull())

    df = silver_m1.select(
        col("CaseNo"), 
        when(
            conditions,
            col("lu_appealType")
        ).otherwise(None).alias("appealType"),
        when(
            conditions,
            col("lu_hmctsCaseCategory")
        ).otherwise(None).alias("hmctsCaseCategory"),
        when(
            conditions,
            col("CaseNo")
        ).otherwise(None).alias("appealReferenceNumber"),
        when(
            conditions,
            col("lu_appealTypeDescription")
        ).otherwise(None).alias("appealTypeDescription"),
        when(
            lr_condition,
            col("lu_caseManagementCategory")
        ).otherwise(None).alias("caseManagementCategory"),
        when(
            aip_condition,
            lit("Yes")
        ).otherwise(lit(None)).alias("isAppealReferenceNumberAvailable"),
        when(
            conditions,
            lit("")
        ).otherwise(lit(None)).alias("ccdReferenceNumberForDisplay")
    )

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("audit.dv_representation"), col("audit.lu_appealType")]

    df_audit = silver_m1.alias("audit").join(df.alias("content"), ["CaseNo"], "left").select(
        col("CaseNo"),

        #Audit appealType
        array(struct(*common_inputFields)).alias("appealType_inputFields"),
        array(struct(*common_inputValues)).alias("appealType_inputValues"),
        col("content.appealType"),
        lit("yes").alias("appealType_Transformation"),

        #Audit appealReferenceNumber
        array(struct(*common_inputFields, lit("CaseNo"))).alias("appealReferenceNumber_inputfields"),
        array(struct(*common_inputValues, col("CaseNo"))).alias("appealReferenceNumber_inputvalues"),
        col("content.appealReferenceNumber"),
        lit("no").alias("appealReferenceNumber_Transformation"),

        #Audit hmctsCaseCategory
        array(struct(*common_inputFields, lit("lu_hmctsCaseCategory"))).alias("hmctsCaseCategory_inputfields"),
        array(struct(*common_inputValues, col("audit.lu_hmctsCaseCategory"))).alias("hmctsCaseCategory_inputvalues"),
        col("content.hmctsCaseCategory"),
        lit("yes").alias("hmctsCaseCategory_Transformation"),

        #Audit appealTypeDescription
        array(struct(*common_inputFields, lit("lu_appealTypeDescription"))).alias("appealTypeDescription_inputFields"),
        array(struct(*common_inputValues, col("audit.lu_appealTypeDescription"))).alias("appealTypeDescription_inputValues"),
        col("content.appealTypeDescription"),
        lit("yes").alias("appealTypeDescription_Transformation"),

        #Audit caseManagementCategory
        array(struct(*common_inputFields, lit("lu_caseManagementCategory"))).alias("caseManagementCategory_inputFields"),
        array(struct(*common_inputValues, col("audit.lu_caseManagementCategory"))).alias("caseManagementCategory_inputValues"),
        col("content.caseManagementCategory"),
        lit("yes").alias("caseManagementCategory_Transformation"),

        #Audit isAppealReferenceNumberAvailable
        array(struct(*common_inputFields)).alias("isAppealReferenceNumberAvailable_inputFields"),
        array(struct(*common_inputValues)).alias("isAppealReferenceNumberAvailable_inputValues"),
        col("content.isAppealReferenceNumberAvailable"),
        lit("yes").alias("isAppealReferenceNumberAvailable_Transformation"),

        #Audit ccdReferenceNumberForDisplay
        array(struct(*common_inputFields)).alias("ccdReferenceNumberForDisplay_inputFields"),
        array(struct(*common_inputValues)).alias("ccdReferenceNumberForDisplay_inputValues"),
        col("content.ccdReferenceNumberForDisplay"),
        lit("yes").alias("ccdReferenceNumberForDisplay_Transformation")
    )

    return df, df_audit

################################################################
##########              caseData grouping            ###########
################################################################

# caseData grouping
def caseData(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres):
    # Define the window specification
    window_spec = Window.partitionBy("CaseNo").orderBy(col("HistoryId").desc())

    # Define the base DataFrame
    silver_h = silver_h.filter(col("HistType") == 6)

    # Define the common filter condition
    common_filter = ~col("Comment").like("%Castle Park Storage%") & ~col("Comment").like("%Field House%") & ~col("Comment").like("%UT (IAC)%")

    # Define the function to get the latest history record based on the filter
    def get_latest_history(df, filter_condition):
        return df.filter(filter_condition).withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).select(
            col("CaseNo"),
            col("Comment"),
            split(col("Comment"), ',')[0].alias("der_prevFileLocation")
        )

    # Create DataFrames for each location
    derived_history_df = get_latest_history(silver_h, common_filter)

    # Define common columns for hearingCentreDynamicList and caseManagementLocationRefData
    common_columns = {
        "hearingCentreDynamicList": struct(
            struct(
                col("locationCode").alias("code"),
                col("locationLabel").alias("label")
            ).alias("value"),
            array(
                struct(lit("227101").alias("code"), lit("Newport Tribunal Centre - Columbus House").alias("label")),
                struct(lit("231596").alias("code"), lit("Birmingham Civil And Family Justice Centre").alias("label")),
                struct(lit("28837").alias("code"), lit("Harmondsworth Tribunal Hearing Centre").alias("label")),
                struct(lit("366559").alias("code"), lit("Atlantic Quay - Glasgow").alias("label")),
                struct(lit("366796").alias("code"), lit("Newcastle Civil And Family Courts And Tribunals Centre").alias("label")),
                struct(lit("386417").alias("code"), lit("Hatton Cross Tribunal Hearing Centre").alias("label")),
                struct(lit("512401").alias("code"), lit("Manchester Tribunal Hearing Centre - Piccadilly Exchange").alias("label")),
                struct(lit("649000").alias("code"), lit("Yarls Wood Immigration And Asylum Hearing Centre").alias("label")),
                struct(lit("698118").alias("code"), lit("Bradford Tribunal Hearing Centre").alias("label")),
                struct(lit("765324").alias("code"), lit("Taylor House Tribunal Hearing Centre").alias("label"))
            ).alias("list_items")
        ),
        "caseManagementLocationRefData": struct(
            lit("1").alias("region"),
            struct(
                struct(
                    col("locationCode").alias("code"),
                    col("locationLabel").alias("label")
                ).alias("value"),
                array(
                    struct(lit("227101").alias("code"), lit("Newport Tribunal Centre - Columbus House").alias("label")),
                    struct(lit("231596").alias("code"), lit("Birmingham Civil And Family Justice Centre").alias("label")),
                    struct(lit("28837").alias("code"), lit("Harmondsworth Tribunal Hearing Centre").alias("label")),
                    struct(lit("366559").alias("code"), lit("Atlantic Quay - Glasgow").alias("label")),
                    struct(lit("366796").alias("code"), lit("Newcastle Civil And Family Courts And Tribunals Centre").alias("label")),
                    struct(lit("386417").alias("code"), lit("Hatton Cross Tribunal Hearing Centre").alias("label")),
                    struct(lit("512401").alias("code"), lit("Manchester Tribunal Hearing Centre - Piccadilly Exchange").alias("label")),
                    struct(lit("649000").alias("code"), lit("Yarls Wood Immigration And Asylum Hearing Centre").alias("label")),
                    struct(lit("698118").alias("code"), lit("Bradford Tribunal Hearing Centre").alias("label")),
                    struct(lit("765324").alias("code"), lit("Taylor House Tribunal Hearing Centre").alias("label"))
                ).alias("list_items")
            ).alias("baseLocation")
        )
    }

    # Join result_df with bronze_derive_hearing_centres to derive derive_hearing_centres
    bronze_derive_hearing_centres = bronze_derive_hearing_centres.withColumn(
        "hearingCentreDynamicList",
        when(
            col("locationCode").isNotNull() & col("locationLabel").isNotNull(),
            common_columns["hearingCentreDynamicList"]
        ).otherwise(None)
    ).withColumn(
        "caseManagementLocationRefData",
        when(
            col("locationCode").isNotNull() & col("locationLabel").isNotNull(),
            common_columns["caseManagementLocationRefData"]
        ).otherwise(None)
    )

    # Join result_df with bronze_hearing_centres to derive hearingCentre
    bronze_hearing_centres = bronze_hearing_centres.withColumn(
        "hearingCentreDynamicList",
        when(
            col("locationCode").isNotNull() & col("locationLabel").isNotNull(),
            common_columns["hearingCentreDynamicList"]
        ).otherwise(None)
    ).withColumn(
        "caseManagementLocationRefData",
        when(
            col("locationCode").isNotNull() & col("locationLabel").isNotNull(),
            common_columns["caseManagementLocationRefData"]
        ).otherwise(None)
    )

    silver_m2 = silver_m2.filter(col("Relationship").isNull())

    # Define postcode mappings
    postcode_mappings = {
        "bradford": ["BD", "DN", "HD", "HG", "HU", "HX", "LS", "S", "WF", "YO"],
        "manchester": ["BB", "BL", "CH", "CW", "FY", "LL", "ST", "L", "LA", "M", "OL", "PR", "SK", "WA", "WN"],
        "newport": ["BA", "BS", "CF", "DT", "EX", "HR", "LD", "NP", "PL", "SA", "SN", "SP", "TA", "TQ", "TR"],
        "taylorHouse": ["AL", "BN", "BR", "CB", "CM", "CO", "CR", "CT", "DA", "E", "EC", "EN", "IG", "IP", "ME", "N", "NR", "NW", "RH", "RM", "SE", "SG", "SS", "TN", "W", "WC"],
        "newcastle": ["CA", "DH", "DL", "NE", "SR", "TS"],
        "birmingham": ["B", "CV", "DE", "DY", "GL", "HP", "LE", "LN", "LU", "MK", "NG", "NN", "OX", "PE", "RG", "SY", "TF", "WD", "WR", "WS", "WV"],
        "hattonCross": ["BH", "GU", "HA", "KT", "PO", "SL", "SM", "SO", "SW", "TW", "UB"],
        "glasgow": ["AB", "DD", "DG", "EH", "FK", "G", "HS", "IV", "KA", "KW", "KY", "ML", "PA", "PH", "TD", "ZE", "BT"]
    }

    # Function to map postcodes to hearing centres
    def map_postcode_to_hearing_centre(postcode):
        if postcode is None:
            return None
        postcode = postcode.replace(" ", "").upper()
        first2 = postcode[:2]
        first1 = postcode[:1]
        for centre, codes in postcode_mappings.items():
            # Try 2-char match first
            if any(first2 == code for code in codes if len(code) == 2):
                return centre
        for centre, codes in postcode_mappings.items():
            # Then try 1-char match
            if any(first1 == code for code in codes if len(code) == 1):
                return centre
        return None

    # Register the UDF
    map_postcode_to_hearing_centre_udf = udf(map_postcode_to_hearing_centre, StringType())

    ##################################################################################################

    result_with_hearing_centre_df = silver_m1.alias("m1").join(
            bronze_hearing_centres.alias("bhc"),
            col("m1.CentreId") == col("bhc.CentreId"),
            how="left").join(derived_history_df.alias("h"),
                                ((col("m1.CaseNo") == col("h.CaseNo")) &
                                (col("m1.CentreId").isin(77,476,101,55,296,13,79,522,406,517,37))),
                                how="left") \
                                .join(bronze_hearing_centres.alias("bhc2"),
                                (col("h.der_prevFileLocation") == col("bhc2.prevFileLocation")),
                                 how="left").join(silver_m2.alias("m2"), col("m1.CaseNo") == col("m2.CaseNo"), how="left") \
                                .withColumn("map_postcode_to_hearing_centre", 
                                           when((col("h.der_prevFileLocation").isin("Arnhem House","Arnhem House (Exceptions)","Loughborough","North Shields (Kings Court)","Not known at this time") | col("h.der_prevFileLocation").isNull()),
                                           coalesce(map_postcode_to_hearing_centre_udf(coalesce(col('m1.Rep_Postcode'),col('m1.CaseRep_Postcode'),('m2.Appellant_Postcode')))
                                           ,lit("newport"))).otherwise(lit("newport"))) \
                                .join(bronze_derive_hearing_centres.alias("bhc3"), col("map_postcode_to_hearing_centre") == col("bhc3.hearingCentre"), how="left") \
                                .select(
        col("m1.CaseNo"),
        col('bhc.Conditions').alias("lu_conditions"),
        col('bhc.hearingCentre').alias("dv_dhc_hearingCentre"),
        col("h.der_prevFileLocation").alias("dv_prevFileLocation"),
        col('m1.Rep_Postcode'),
        col('m1.CaseRep_Postcode'),
        col("m2.Appellant_Postcode"),
        col("m1.CentreId"),
        col("bhc.prevFileLocation").alias("dv_dhc_prevFileLocation"),
        col("bhc2.hearingCentre").alias("dv_dhc2_hearingCentre"),

        col("bhc2.staffLocation").alias("dv_dhc2_staffLocation"),
        col("bhc3.staffLocation").alias("dv_dhc3_staffLocation"),
        col("bhc2.caseManagementLocation").alias("dv_dhc2_caseManagementLocation"),
        col("bhc3.caseManagementLocation").alias("dv_dhc3_caseManagementLocation"),
        col("bhc2.hearingCentreDynamicList").alias("dv_dhc2_hearingCentreDynamicList"),
        col("bhc3.hearingCentreDynamicList").alias("dv_dhc3_hearingCentreDynamicList"),
        col("bhc2.caseManagementLocationRefData").alias("dv_dhc2_caseManagementLocationRefData"),
        col("bhc3.caseManagementLocationRefData").alias("dv_dhc3_caseManagementLocationRefData"),
        col("bhc2.selectedHearingCentreRefData").alias("dv_dhc2_selectedHearingCentreRefData"),
        col("bhc3.selectedHearingCentreRefData").alias("dv_dhc3_selectedHearingCentreRefData"),
        col("map_postcode_to_hearing_centre"),
        
        when(((col('bhc.Conditions').isNull()) | (col('bhc.Conditions') == 'NO MAPPING REQUIRED')), col('bhc.hearingCentre'))
        .when((col("h.der_prevFileLocation").isin("Arnhem House","Arnhem House (Exceptions)","Loughborough","North Shields (Kings Court)","Not known at this time") | col("h.der_prevFileLocation").isNull()) ,col("map_postcode_to_hearing_centre"))
        .when(col("h.der_prevFileLocation").isin('Castle Park Storage','Field House', 'Field House (TH)','UT (IAC) Cardiff CJC','UT (IAC) Hearing in Field House','UT (IAC) Hearing in Man CJC'), lit(None)).otherwise(col("bhc2.hearingCentre")).alias("hearingCentre"),


        when(((col('bhc.Conditions').isNull()) | (col('bhc.Conditions') == 'NO MAPPING REQUIRED')), col('bhc.staffLocation'))
        .when((col("h.der_prevFileLocation").isin("Arnhem House","Arnhem House (Exceptions)","Loughborough","North Shields (Kings Court)","Not known at this time")| col("h.der_prevFileLocation").isNull()),col("bhc3.staffLocation"))
        .when(col("h.der_prevFileLocation").isin('Castle Park Storage','Field House', 'Field House (TH)','UT (IAC) Cardiff CJC','UT (IAC) Hearing in Field House','UT (IAC) Hearing in Man CJC'), lit(None)).otherwise(col("bhc2.staffLocation"))
         .alias("staffLocation"),


         when(((col('bhc.Conditions').isNull()) | (col('bhc.Conditions') == 'NO MAPPING REQUIRED')), col('bhc.caseManagementLocation'))
        .when((col("h.der_prevFileLocation").isin("Arnhem House","Arnhem House (Exceptions)","Loughborough","North Shields (Kings Court)","Not known at this time") | col("h.der_prevFileLocation").isNull()),col("bhc3.caseManagementLocation"))
        .when(col("h.der_prevFileLocation").isin('Castle Park Storage','Field House', 'Field House (TH)','UT (IAC) Cardiff CJC','UT (IAC) Hearing in Field House','UT (IAC) Hearing in Man CJC'), lit(None)).otherwise(col("bhc2.caseManagementLocation"))
        .alias("caseManagementLocation"),

        when(((col('bhc.Conditions').isNull()) | (col('bhc.Conditions') == 'NO MAPPING REQUIRED')), col('bhc.hearingCentreDynamicList'))
        .when((col("h.der_prevFileLocation").isin("Arnhem House","Arnhem House (Exceptions)","Loughborough","North Shields (Kings Court)","Not known at this time") | col("h.der_prevFileLocation").isNull()),col("bhc3.hearingCentreDynamicList"))
        .when(col("h.der_prevFileLocation").isin('Castle Park Storage','Field House', 'Field House (TH)','UT (IAC) Cardiff CJC','UT (IAC) Hearing in Field House','UT (IAC) Hearing in Man CJC'), lit(None)).otherwise(col("bhc2.hearingCentreDynamicList"))
        .alias("hearingCentreDynamicList"),

        when(((col('bhc.Conditions').isNull()) | (col('bhc.Conditions') == 'NO MAPPING REQUIRED')), col('bhc.caseManagementLocationRefData'))
        .when((col("h.der_prevFileLocation").isin("Arnhem House","Arnhem House (Exceptions)","Loughborough","North Shields (Kings Court)","Not known at this time") | col("h.der_prevFileLocation").isNull()),col("bhc3.caseManagementLocationRefData"))
        .when(col("h.der_prevFileLocation").isin('Castle Park Storage','Field House', 'Field House (TH)','UT (IAC) Cardiff CJC','UT (IAC) Hearing in Field House','UT (IAC) Hearing in Man CJC'), lit(None)).otherwise(col("bhc2.caseManagementLocationRefData"))
        .alias("caseManagementLocationRefData"),

         when(((col('bhc.Conditions').isNull()) | (col('bhc.Conditions') == 'NO MAPPING REQUIRED')), col('bhc.selectedHearingCentreRefData'))
        .when((col("h.der_prevFileLocation").isin("Arnhem House","Arnhem House (Exceptions)","Loughborough","North Shields (Kings Court)","Not known at this time") | col("h.der_prevFileLocation").isNull()),col("bhc3.selectedHearingCentreRefData"))
        .when(col("h.der_prevFileLocation").isin('Castle Park Storage','Field House', 'Field House (TH)','UT (IAC) Cardiff CJC','UT (IAC) Hearing in Field House','UT (IAC) Hearing in Man CJC'), lit(None)).otherwise(col("bhc2.selectedHearingCentreRefData"))
        .alias("selectedHearingCentreRefData")
    )

    # Filter silver_m3 to get rows with max StatusId and Outcome is not null
    # Define window partitioned by CaseNo and ordered by descending StatusId
    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_ranked = silver_m3.withColumn("row_num", row_number().over(window_spec))

    # Filter the top-ranked rows where Outcome is not null
    silver_m3_filtered = silver_m3_ranked.filter(
        (col("row_num") == 1) & (col("Outcome").isNotNull())
    ).select(
        col("CaseNo"),
        lit("Yes").alias("recordedOutOfTimeDecision"), col("Outcome")
    )

    conditions = (col("dv_representation").isin('LR', 'AIP')) & (col("lu_appealType").isNotNull())

    df = silver_m1.alias("m1").join(
        silver_m3_filtered.alias("m3"),
        on="CaseNo",
        how="left"
    ).join(
        result_with_hearing_centre_df.alias("hearing"),
        on="CaseNo",
        how="left"
    ).withColumn(
        "appellantsRepresentation", when(((col("m1.dv_representation") == "LR") &  (col("lu_appealType").isNotNull())), "No").when(((col("m1.dv_representation") == "AIP") & (col("lu_appealType").isNotNull())), "Yes").otherwise(None)
    ).withColumn(
        "submissionOutOfTime", when(col("OutOfTimeIssue") == 1, lit("Yes")).otherwise(lit("No"))
    ).withColumn(
        "adminDeclaration1", lit(["hasDeclared"])
    ).withColumn(
        "appealWasNotSubmittedReason", when(((col("m1.dv_representation") == "LR") & (col("lu_appealType").isNotNull())), "This is an ARIA Migrated Case.").otherwise(None)
    ).withColumn(
        "applicationOutOfTimeExplanation", when(col("OutOfTimeIssue") == 1, "This is a migrated ARIA case. Please refer to the documents.").otherwise(None)
    ).withColumn(
        "appealSubmissionDate", date_format(col("m1.DateLodged"), "yyyy-MM-dd")
    ).withColumn(
        "appealSubmissionInternalDate", date_format(col("m1.DateLodged"), "yyyy-MM-dd")
    ).withColumn(
        "tribunalReceivedDate", date_format(col("m1.DateAppealReceived"), "yyyy-MM-dd")
    ).select(
        "CaseNo", 
        col("appellantsRepresentation"),
        when(conditions, col("submissionOutOfTime")).otherwise(None).alias("submissionOutOfTime"),
        when(conditions, col("m3.recordedOutOfTimeDecision")).otherwise(None).alias("recordedOutOfTimeDecision"),
        when(conditions, col("applicationOutOfTimeExplanation")).otherwise(None).alias("applicationOutOfTimeExplanation"), 
        when(conditions, col("hearing.hearingCentre")).otherwise(None).alias("hearingCentre"),
        when(conditions, col("hearing.staffLocation")).otherwise(None).alias("staffLocation"),
        when(conditions, col("hearing.caseManagementLocation")).otherwise(None).alias("caseManagementLocation"),
        when(conditions, col("hearing.hearingCentreDynamicList")).otherwise(None).alias("hearingCentreDynamicList"),
        when(conditions, col("hearing.caseManagementLocationRefData")).otherwise(None).alias("caseManagementLocationRefData"),
        when(conditions, col("hearing.selectedHearingCentreRefData")).otherwise(None).alias("selectedHearingCentreRefData"),
        col("appealWasNotSubmittedReason"),
        when(conditions, col("adminDeclaration1")).otherwise(None).alias("adminDeclaration1"),    
        when(conditions, col("appealSubmissionDate")).otherwise(None).alias("appealSubmissionDate"), 
        when(conditions, col("appealSubmissionInternalDate")).otherwise(None).alias("appealSubmissionInternalDate"),
        when(conditions, col("tribunalReceivedDate")).otherwise(None).alias("tribunalReceivedDate"),
        when(conditions, lit([]).cast("array<int>")).otherwise(None).alias("caseLinks"), 
        when(conditions, lit("NotSure")).otherwise(None).alias("hasOtherAppeals")
    )


    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("audit.dv_representation"), col("audit.lu_appealType")]

    df_audit = silver_m1.alias("audit").join(df.alias("content"), ["CaseNo"],"left") \
                        .join(silver_m3_filtered.alias("m3"), ["CaseNo"],"left") \
                        .join(result_with_hearing_centre_df.alias("hearing"),["CaseNo"],"left").select(

    col("CaseNo"),

    #Audit appellantsRepresentation
    array(struct(*common_inputFields)).alias("appellantsRepresentation_inputFields"),
    array(struct(*common_inputValues)).alias("appellantsRepresentation_inputValues"),
    col("content.appellantsRepresentation"),
    lit("yes").alias("appellantsRepresentation_Transformation"),

    #Audit submissionOutOfTime
    array(struct(*common_inputFields,lit("OutOfTimeIssue"))).alias("submissionOutOfTime_inputFields"),
    array(struct(*common_inputValues,col("audit.OutOfTimeIssue"))).alias("submissionOutOfTime_inputValues"),
    col("content.submissionOutOfTime"),
    lit("yes").alias("submissionOutOfTime_Transformation"),

    #Audit recordedOutOfTimeDecision
    array(struct(*common_inputFields,lit("Outcome"))).alias("recordedOutOfTimeDecision_inputFields"),
    array(struct(*common_inputValues,col("m3.Outcome"))).alias("recordedOutOfTimeDecision_inputValues"),
    col("content.recordedOutOfTimeDecision"),
    lit("yes").alias("recordedOutOfTimeDecision_Transformation"),

    #Audit applicationOutOfTimeExplanation
    array(struct(*common_inputFields,lit("OutOfTimeIssue"))).alias("applicationOutOfTimeExplanation_inputFields"),
    array(struct(*common_inputValues,col("audit.OutOfTimeIssue"))).alias("applicationOutOfTimeExplanation_inputValues"),
    col("content.applicationOutOfTimeExplanation"),
    lit("yes").alias("applicationOutOfTimeExplanation_Transformation"),

    #Audit hearingCentre
    array(struct(*common_inputFields,lit("lu_hearingCentre"),lit("lu_conditions"),lit("dv_prevFileLocation"),lit("Rep_Postcode"),lit("CaseRep_Postcode"),lit("Appellant_Postcode"),lit("CentreId"),lit("dv_dhc2_hearingCentre"))).alias("hearingCentre_inputFields"),
    array(struct(*common_inputValues,col("audit.lu_hearingCentre"),col("hearing.lu_conditions"),col("hearing.dv_prevFileLocation"),col("hearing.Rep_Postcode"),col("hearing.CaseRep_Postcode"),col("hearing.Appellant_Postcode"),col("hearing.CentreId"),col("hearing.dv_dhc2_hearingCentre"))).alias("hearingCentre_inputValues"),
    col("content.hearingCentre"),
    lit("yes").alias("hearingCentre_Transformation"),



    #Audit staffLocation
    array(struct(*common_inputFields,lit("lu_staffLocation"),lit("lu_conditions"),lit("dv_prevFileLocation"),lit("Rep_Postcode"),lit("CaseRep_Postcode"),lit("Appellant_Postcode"),lit("CentreId"),lit("dv_dhc2_staffLocation"),lit("dv_dhc3_staffLocation"))).alias("staffLocation_inputFields"),
    array(struct(*common_inputValues,col("audit.lu_staffLocation"),col("hearing.lu_conditions"),col("hearing.dv_prevFileLocation"),col("hearing.Rep_Postcode"),col("hearing.CaseRep_Postcode"),col("hearing.Appellant_Postcode"),col("hearing.CentreId"),col("hearing.dv_dhc2_staffLocation"),lit("hearing.dv_dhc3_staffLocation"))).alias("staffLocation_inputValues"),
    col("content.staffLocation"),
    lit("yes").alias("staffLocation_Transformation"),



    #Audit caseManagementLocation
    array(struct(*common_inputFields,lit("lu_hearingCentre"),lit("lu_conditions"),lit("dv_prevFileLocation"),lit("Rep_Postcode"),lit("CaseRep_Postcode"),lit("Appellant_Postcode"),lit("CentreId"),lit("dv_dhc2_caseManagementLocation"),lit("dv_dhc3_caseManagementLocation"))).alias("caseManagementLocation_inputFields"),
    array(struct(*common_inputValues,col("audit.lu_caseManagementLocation"),col("hearing.lu_conditions"),col("hearing.dv_prevFileLocation"),col("hearing.Rep_Postcode"),col("hearing.CaseRep_Postcode"),col("hearing.Appellant_Postcode"),col("hearing.CentreId"),col("hearing.dv_dhc2_caseManagementLocation"),col("hearing.dv_dhc3_caseManagementLocation"))).alias("caseManagementLocation_inputValues"),
    col("content.caseManagementLocation"),
    lit("yes").alias("caseManagementLocation_Transformation"),

    #Audit hearingCentreDynamicList
    array(struct(*common_inputFields,lit("dv_hearingCentreDynamicList"),lit("lu_conditions"),lit("dv_prevFileLocation"),lit("Rep_Postcode"),lit("CaseRep_Postcode"),lit("Appellant_Postcode"),lit("CentreId"),lit("dv_dhc2_hearingCentreDynamicList"),lit("dv_dhc3_hearingCentreDynamicList"))).alias("hearingCentreDynamicList_inputFields"),
    array(struct(*common_inputValues,col("audit.dv_hearingCentreDynamicList"),col("hearing.lu_conditions"),col("hearing.dv_prevFileLocation"),col("hearing.Rep_Postcode"),col("hearing.CaseRep_Postcode"),col("hearing.Appellant_Postcode"),col("hearing.CentreId"),col("hearing.dv_dhc2_hearingCentreDynamicList"),col("hearing.dv_dhc3_hearingCentreDynamicList"))).alias("hearingCentreDynamicList_inputValues"),
    col("content.hearingCentreDynamicList"),
    lit("yes").alias("hearingCentreDynamicList_Transformation"),

    #Audit caseManagementLocationRefData
    array(struct(*common_inputFields,lit("dv_caseManagementLocationRefData"),lit("lu_conditions"),lit("dv_prevFileLocation"),lit("Rep_Postcode"),lit("CaseRep_Postcode"),lit("Appellant_Postcode"),lit("CentreId"),lit("dv_dhc2_caseManagementLocationRefData"),lit("dv_dhc3_caseManagementLocationRefData"))).alias("caseManagementLocationRefData_inputFields"),
    array(struct(*common_inputValues,col("audit.dv_caseManagementLocationRefData"),col("hearing.lu_conditions"),col("hearing.dv_prevFileLocation"),col("hearing.Rep_Postcode"),col("hearing.CaseRep_Postcode"),col("hearing.Appellant_Postcode"),col("hearing.CentreId"),col("hearing.dv_dhc2_caseManagementLocationRefData"),col("hearing.dv_dhc3_caseManagementLocationRefData"))).alias("caseManagementLocationRefData_inputValues"),
    col("content.caseManagementLocationRefData"),
    lit("yes").alias("caseManagementLocationRefData_Transformation"),

    #Audit selectedHearingCentreRefData
    array(struct(*common_inputFields,lit("lu_selectedHearingCentreRefData"),lit("dv_prevFileLocation"),lit("Rep_Postcode"),lit("CaseRep_Postcode"),lit("Appellant_Postcode"),lit("CentreId"),lit("dv_dhc2_caseManagementLocationRefData"),lit("dv_dhc3_caseManagementLocationRefData"))).alias("selectedHearingCentreRefData_inputFields"),
    array(struct(*common_inputValues,col("audit.lu_selectedHearingCentreRefData"),col("hearing.lu_conditions"),col("hearing.dv_prevFileLocation"),col("hearing.Rep_Postcode"),col("hearing.CaseRep_Postcode"),col("hearing.Appellant_Postcode"),col("hearing.CentreId"),col("hearing.dv_dhc2_selectedHearingCentreRefData"),col("hearing.dv_dhc3_selectedHearingCentreRefData"))).alias("selectedHearingCentreRefData_inputValues"),
    col("content.selectedHearingCentreRefData"),
    lit("yes").alias("selectedHearingCentreRefData_Transformation"),

    #Audit appealWasNotSubmittedReason
    array(struct(*common_inputFields)).alias("appealWasNotSubmittedReason_inputFields"),
    array(struct(*common_inputValues)).alias("appealWasNotSubmittedReason_inputValues"),
    col("content.appealWasNotSubmittedReason"),
    lit("yes").alias("appealWasNotSubmittedReason_Transformation"),

    # Audit adminDeclaration1
    array(struct(*common_inputFields)).alias("adminDeclaration1_inputFields"),
    array(struct(*common_inputValues)).alias("adminDeclaration1_inputValues"),
    col("content.adminDeclaration1"),
    lit("yes").alias("adminDeclaration1_Transformation"),

    # Audit appealSubmissionDate
    array(struct(*common_inputFields,lit("DateLodged"))).alias("appealSubmissionDate_inputFields"),
    array(struct(*common_inputValues,col("audit.DateLodged"))).alias("appealSubmissionDate_inputValues"),
    col("content.appealSubmissionDate"),
    lit("yes").alias("appealSubmissionDate_Transformation"),

    # Audit appealSubmissionInternalDate
    array(struct(*common_inputFields,lit("DateLodged"))).alias("appealSubmissionInternalDate_inputFields"),
    array(struct(*common_inputValues,col("audit.DateLodged"))).alias("appealSubmissionInternalDate_inputValues"),
    col("content.appealSubmissionInternalDate"),
    lit("yes").alias("appealSubmissionInternalDate_Transformation"),

    # Audit tribunalReceivedDate
    array(struct(*common_inputFields,lit("DateLodged"))).alias("tribunalReceivedDate_inputFields"),
    array(struct(*common_inputValues,col("audit.DateLodged"))).alias("tribunalReceivedDate_inputValues"),
    col("content.tribunalReceivedDate"),
    lit("yes").alias("tribunalReceivedDate_Transformation"),

    # Audit caseLinks
    array(struct(*common_inputFields)).alias("caseLinks_inputFields"),
    array(struct(*common_inputValues)).alias("caseLinks_inputValues"),
    col("content.caseLinks"),
    lit("yes").alias("caseLinks_Transformation"),

    # Audit hasOtherAppeals
    array(struct(*common_inputFields)).alias("hasOtherAppeals_inputFields"),
    array(struct(*common_inputValues)).alias("hasOtherAppeals_inputValues"),
    col("content.hasOtherAppeals"),
    lit("yes").alias("hasOtherAppeals_Transformation")

    )

    return df, df_audit

################################################################
##########           flagsLabels grouping            ###########
################################################################

## flagsLabels function
def flagsLabels(silver_m1, silver_m2, silver_c):
    ## caseFlags lookup used for conditional mapping- data comes from Data-Mapping-ARIA-CCD mapping document in the sheet 'APPENDIX-Categories'
    case_flag_lookup = {
        7:  {"name": "Urgent case", "code": "CF0007", "comment": None, "hearing": "No"},
        41: {"name": "Other", "code": "OT0001", "comment": "Expedite", "hearing": "Yes"},
        31: {"name": "Other", "code": "OT0001", "comment": "Expedite", "hearing": "Yes"},
        32: {"name": "Other", "code": "OT0001", "comment": "Expedite", "hearing": "Yes"},
        8:  {"name": "Other", "code": "OT0001", "comment": "Reclassified RFT", "hearing": "Yes"},
        24: {"name": "Other", "code": "OT0001", "comment": "EEA Family Permit", "hearing": "Yes"},
        25: {"name": "RRO (Restricted Reporting Order / Anonymisation)", "code": "CF0012", "comment": None, "hearing": "Yes"}
    }

    ## appellantLevelFlags look up used for conditional mapping- data comes from Data-Mapping-ARIA-CCD mapping document in the sheet 'APPENDIX-Categories'
    appellant_flag_lookup = {
        9:  {"name": "Unaccompanied minor", "code": "PF0013", "comment": None, "hearing": "No"},
        17: {"name": "Foreign national offender", "code": "PF0012", "comment": None, "hearing": "Yes"},
        29: {"name": "Foreign national offender", "code": "PF0012", "comment": None, "hearing": "Yes"},
        30: {"name": "Foreign national offender", "code": "PF0012", "comment": None, "hearing": "Yes"},
        39: {"name": "Foreign national offender", "code": "PF0012", "comment": None, "hearing": "Yes"},
        40: {"name": "Foreign national offender", "code": "PF0012", "comment": None, "hearing": "Yes"},
        48: {"name": "Foreign national offender", "code": "PF0012", "comment": None, "hearing": "Yes"},
        19: {"name": "Foreign national offender", "code": "PF0012", "comment": None, "hearing": "Yes"}
    }

    silver_m2 = silver_m2.filter(col("Relationship").isNull())

    ## aliases for input tables- silver_m1, silver_m2, silver_c variables created in Transformation: stg_payment_pending_ccd_json_generator
    m1 = silver_m1.alias("m1")
    m2 = silver_m2.alias("m2")
    c = silver_c.alias("c")

    # Join tables needed for transformation
    joined = m1.join(c, on="CaseNo", how="left").join(m2, on="CaseNo", how="left")

    # Grouping by CaseNo to ensure one row per CaseNo, and collecting fields needed for mapping logic
    grouped = joined.groupBy("CaseNo").agg(
        # Collecting all CategoryId into an array to avoid multiples rows per CaseNo
        collect_set("CategoryId").alias("CategoryIds"),
        first("HORef", ignorenulls=True).alias("HORef"),
        first("CasePrefix", ignorenulls=True).alias("CasePrefix"),
        first("dv_representation", ignorenulls=True).alias("dv_representation"),
        first("Detained", ignorenulls=True).alias("Detained"),
        first("lu_appealType", ignorenulls=True).alias("lu_appealType")
    )

    ## Building caseFlags struct based off format shown in the mapping document in APPENDIX-Categories sheet
    def make_flag_struct(name, code, comment, hearing):
        return struct(
            lit(expr("uuid()")).alias("id"),
            struct(
                lit(name).alias("name"),
                array(struct(expr("uuid()").alias("id"), lit("Case").alias("value"))).alias("path"),
                lit("Active").alias("status"),
                lit(code).alias("flagCode"),
                lit(comment).cast("string").alias("flagComment"),
                date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("dateTimeCreated"),
                lit(hearing).alias("hearingRelevant")
                ).alias("value")
        )
    
    ## Building appellantLevelFlags struct based off format shown in the mapping document in APPENDIX-Categories sheet
    def make_appellant_flag_struct(name, code, comment, hearing):
        return struct(
            lit(expr("uuid()")).alias("id"),
            struct(
                lit(name).alias("name"),
                array(struct(expr("uuid()").alias("id"), lit("Party").alias("value"))).alias("path"),
                lit("Active").alias("status"),
                lit(code).alias("flagCode"),
                lit(comment).cast("string").alias("flagComment"),
                date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("dateTimeCreated"),
                lit(hearing).alias("hearingRelevant")
                ).alias("value")
        )

    ## Creating list of caseFlags for each row based on the conditions in the caseFlags lookup and the extra condition where the field 'HORef' (from silver_m1) is used
    def generate_case_flag_details(col_category_ids, col_horef):
        flags = []
        for cat_id, data in case_flag_lookup.items():
            flags.append((array_contains(col_category_ids, lit(cat_id)), make_flag_struct(**data)))
        flags.append((col_horef.isNotNull(), make_flag_struct("Other", "OT0001", "Dropped Case", "Yes")))

        exprs = [when(cond, array(flag)).otherwise(array()) for cond, flag in flags]
        return F.flatten(F.array(*exprs))
    
    ## Creating list of appellantLevelFlags for each row based on conditions in the appellantLevelFlags lookup and the extra condition where the field 'Detained' (from silver_m2) is used
    def generate_appellant_flag_details(col_category_ids, col_detained):
        flags = []
        for cat_id, data in appellant_flag_lookup.items():
            flags.append((array_contains(col_category_ids, lit(cat_id)), make_appellant_flag_struct(**data)))
        flags.append((col_detained.isin(1,2,4), make_appellant_flag_struct("Detained individual", "PF0019", None, "No")))

        exprs = [when(cond, array(flag)).otherwise(array()) for cond, flag in flags]
        return F.flatten(F.array(*exprs))

    ## Applying flag generators
    grouped = grouped.withColumn("caseFlagDetails", generate_case_flag_details(col("CategoryIds"), col("HORef")))
    grouped = grouped.withColumn("appellantFlagDetails", generate_appellant_flag_details(col("CategoryIds"), col("Detained")))
    ## Formatting caseFlags into final structure or Null/None if no conditions are met
    grouped = grouped.withColumn(
        "caseFlags",
        when(size(col("caseFlagDetails")) > 0,
            struct(
                col("caseFlagDetails").alias("details")
                # lit(None).cast("string").alias("groupId"),
                # lit(None).cast("string").alias("visibility")
            )
        ).otherwise(lit(None))
    )
    ## Formatting appellantLevelFlags into final structure or Null/None if no conditions are met
    grouped = grouped.withColumn(
        "appellantLevelFlags",
        when(F.size("appellantFlagDetails") > 0,
            struct(
                col("appellantFlagDetails").alias("details"),
                # lit(None).cast("string").alias("groupId"),
                lit("Functional PostDeployment").alias("partyName"),
                lit("Appellant").alias("roleOnCase"),
                # lit(None).cast("string").alias("visibility")
            )
        ).otherwise(lit(None))
    )
    ## Adding extra flagsLabels fields with conditions
    grouped = grouped.withColumn("s94bStatus", lit("No"))
    ####### ARIADM-709 ticket condition here #######
    # grouped = grouped.withColumn("journeyType", when(col("dv_representation") == "AIP", lit("aip")).otherwise(None))
    grouped = grouped.withColumn("isAdmin", lit("Yes"))
    ####### ARIADM-710 ticket condition here #######
    grouped = grouped.withColumn("isAriaMigratedFeeExemption", when(col("CasePrefix") == "DA", lit("Yes")).otherwise(lit("No")))
    grouped = grouped.withColumn("isEjp", lit("No"))

    ## Only returning fields where base conditions are met 
    conditions = (col("dv_representation").isin("LR", "AIP")) & (col("lu_appealType").isNotNull())
    df = grouped.select(
        col("CaseNo"),
        # col("CategoryIds"),
        # col("Detained"),
        when(conditions, col("caseFlags")).otherwise(None).alias("caseFlags"),
        when(conditions, col("appellantLevelFlags")).otherwise(None).alias("appellantLevelFlags"),
        when(conditions, col("s94bStatus")).otherwise(None).alias("s94bStatus"),
        # col("journeyType"),
        when(conditions, col("isAdmin")).otherwise(None).alias("isAdmin"),
        when(conditions, col("isAriaMigratedFeeExemption")).otherwise(None).alias("isAriaMigratedFeeExemption"),
        when(conditions, col("isEjp")).otherwise(None).alias("isEjp")
    )

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("audit.dv_representation"), col("audit.lu_appealType")]

    df_audit = silver_m1.alias("audit").join(df.alias("content"), ["CaseNo"], "left") \
                        .join(silver_m2.alias("m2"), ["CaseNo"], "left") \
                        .join(grouped.alias("grp"), ["CaseNo"], "left").select(
        col("CaseNo"),

        #Audit caseFlags
        array(struct(*common_inputFields, lit("CategoryIds"), lit("HORef"))).alias("caseFlags_inputFields"),
        array(struct(*common_inputValues, col("grp.CategoryIds"), col("audit.HORef"))).alias("caseFlags_inputValues"),
        col("content.caseFlags"),
        lit("yes").alias("caseFlags_Transformation"),

        #Audit appellantLevelFlags
        array(struct(*common_inputFields, lit("CategoryIds"), lit("Detained"))).alias("appellantLevelFlags_inputFields"),
        array(struct(*common_inputValues, col("grp.CategoryIds"), col("m2.Detained"))).alias("appellantLevelFlags_inputValues"),
        col("content.appellantLevelFlags"),
        lit("yes").alias("appellantLevelFlags_Transformation"),

        #Audit s94bStatus
        array(struct(*common_inputFields)).alias("s94bStatus_inputFields"),
        array(struct(*common_inputValues)).alias("s94bStatus_inputValues"),
        col("content.s94bStatus"),
        lit("yes").alias("s94bStatus_Transformation"),

        #Audit journeyType
        # array(struct(*common_inputFields)).alias("journeyType_inputFields"),
        # array(struct(*common_inputValues)).alias("journeyType_inputValues"),
        # col("content.journeyType"),
        # lit("yes").alias("journeyType_Transformation"),

        #Audit isAdmin
        array(struct(*common_inputFields)).alias("isAdmin_inputFields"),
        array(struct(*common_inputValues)).alias("isAdmin_inputValues"),
        col("content.isAdmin"),
        lit("yes").alias("isAdmin_Transformation"),

        #Audit isAriaMigratedFeeExemption
        array(struct(*common_inputFields, lit("CasePrefix"))).alias("isAriaMigratedFeeExemption_inputFields"),
        array(struct(*common_inputValues, col("audit.CasePrefix"))).alias("isAriaMigratedFeeExemption_inputValues"),
        col("content.isAriaMigratedFeeExemption"),
        lit("yes").alias("isAriaMigratedFeeExemption_Transformation"),

        #Audit isEjp
        array(struct(*common_inputFields)).alias("isEjp_inputFields"),
        array(struct(*common_inputValues)).alias("isEjp_inputValues"),
        col("content.isEjp"),
        lit("yes").alias("isEjp_Transformation"),
    )
                        
    return df, df_audit
    
################################################################
##########           cleanEmail Function             ###########
################################################################


def cleanEmail(email):
    if email is None:
        return None
    
    email = re.sub(r"\s+", "", email)             # Remove all whitespace
    email = re.sub(r"\s", "", email)              # 2. Remove internal whitespace
    email = re.sub(r"^\.", "", email)             # 3. Remove leading .
    email = re.sub(r"\.$", "", email)             # 4. Remove trailing .
    email = re.sub(r"^,", "", email)              # 5. Remove leading ,
    email = re.sub(r",$", "", email)              # 6. Remove trailing ,
    email = re.sub(r"^[()]", "", email)           # 7. Remove leading parenthesis
    email = re.sub(r"[()]$", "", email)           # 8. Remove trailing parenthesis
    email = re.sub(r"^:", "", email)              # 9. Remove leading colon
    email = re.sub(r":$", "", email)              #10. Remove trailing colon
    email = re.sub(r"^\*", "", email)             #11. Remove leading asterisk
    email = re.sub(r"\*$", "", email)             #12. Remove trailing asterisk
    email = re.sub(r"^;", "", email)              #13. Remove leading semicolon
    email = re.sub(r";$", "", email)              #14. Remove trailing semicolon
    email = re.sub(r"^\?", "", email)             #15. Remove leading question
    email = re.sub(r"\?$", "", email)             #16. Remove trailing question
    email = email.strip()                         # 1. Trim spaces

    return email

# Register the UDF
cleanEmailUDF = udf(cleanEmail, StringType())

################################################################
##########           cleanPhoneNumber Function       ###########
################################################################

def cleanPhoneNumber(PhoneNumber):
    if PhoneNumber is None:
        return None
    
    
    PhoneNumber = re.sub(r"\s+", "", PhoneNumber)             # 1. Remove internal whitespace
    PhoneNumber = re.sub(r"\s", "", PhoneNumber)              # 2. Remove internal whitespace
    PhoneNumber = re.sub(r"^\.", "", PhoneNumber)             # 3. Remove leading .
    PhoneNumber = re.sub(r"\.$", "", PhoneNumber)             # 4. Remove trailing .
    PhoneNumber = re.sub(r"^,", "", PhoneNumber)              # 5. Remove leading ,
    PhoneNumber = re.sub(r",$", "", PhoneNumber)              # 6. Remove trailing ,
    PhoneNumber = re.sub(r"^[()]", "", PhoneNumber)           # 7. Remove leading parenthesis
    PhoneNumber = re.sub(r"[()]$", "", PhoneNumber)           # 8. Remove trailing parenthesis
    PhoneNumber = re.sub(r"^:", "", PhoneNumber)              # 9. Remove leading colon
    PhoneNumber = re.sub(r":$", "", PhoneNumber)              #10. Remove trailing colon
    PhoneNumber = re.sub(r"^\*", "", PhoneNumber)             #11. Remove leading asterisk
    PhoneNumber = re.sub(r"\*$", "", PhoneNumber)             #12. Remove trailing asterisk
    PhoneNumber = re.sub(r"^;", "", PhoneNumber)              #13. Remove leading semicolon
    PhoneNumber = re.sub(r";$", "", PhoneNumber)              #14. Remove trailing semicolon
    PhoneNumber = re.sub(r"^\?", "", PhoneNumber)             #15. Remove leading question
    PhoneNumber = re.sub(r"\?$", "", PhoneNumber)             #16. Remove trailing question
    PhoneNumber = re.sub(r"[.\-]", "", PhoneNumber)           #17. Remove internal dots and dashes
    PhoneNumber = re.sub(r"^0044", "+44", PhoneNumber)        #18. Change starting 0044 to +44
    PhoneNumber = re.sub(r"\(0\)", "", PhoneNumber)           #19. Remove (0) if +44 or 44 exists in the number
    PhoneNumber = re.sub(r"^44", "+44", PhoneNumber)          #20. Add missing + if number starts with 44 (but not already +)
    PhoneNumber = PhoneNumber.strip()                         #21. Trim spaces
    PhoneNumber = re.sub(r"[)]", "", PhoneNumber)               # Removing closing parenthesis 

    return PhoneNumber


#Register the UDF
phoneNumberUDF = udf(cleanPhoneNumber, StringType())


################################################################
##########           legalRepDetails Function        ###########
################################################################

##Create legalRepDetails fields

def legalRepDetails(silver_m1):
    conditions_legalRepDetails = (col("dv_representation") == 'LR') & (col("lu_appealType").isNotNull())

    df = silver_m1.alias("m1").filter(conditions_legalRepDetails).withColumn(
        "legalRepGivenName",
        coalesce(col("Contact"), col("Rep_Name"), col("CaseRep_Name"))   #If contact is null use RepName, if RepName null use CaseRepName
    ).withColumn(
        "legalRepFamilyNamePaperJ",
        coalesce(col("Rep_Name"), col("CaseRep_Name"))                     #If RepName is null use CaseRepName
    ).withColumn(
        "legalRepCompanyPaperJ",
        coalesce(col("Rep_Name"), col("CaseRep_Name"))                     #If RepName is null use CaseRepName
    ).withColumn(
        "localAuthorityPolicy",
        lit(json.dumps({
            "Organisation": {},
            "OrgPolicyCaseAssignedRole": "[LEGALREPRESENTATIVE]"
        }))
    ).withColumn(
    "legalRepEmail",
    cleanEmailUDF(
        coalesce(col("Rep_Email"), col("CaseRep_Email"), col("CaseRep_FileSpecific_Email")))
    ).withColumn(
        "legalRepAddressUK",               
        when(col("RepresentativeId") == 0, concat_ws(" ",      #If Representiative (solicitor) lives outside UK
        col("CaseRep_Address1"), col("CaseRep_Address2"),         #Concat non-null fields by space
        col("CaseRep_Address3"), col("CaseRep_Address4"),
        col("CaseRep_Address5"), col("CaseRep_Postcode")
        
        )).otherwise(concat_ws(" ",                              #If representative lives inside UK
        col("Rep_Address1"), col("Rep_Address2"),                   #Concat non-null fields by space
        col("Rep_Address3"), col("Rep_Address4"),
        col("Rep_Address5"), col("Rep_Postcode")
    ))

    ## Apply UDFs to determine if the legalRep has a UK address - 1. pull postcode. UK postcode? 
    ).withColumn(
    "ukPostcode",
    getUkPostcodeUDF(col("CaseRep_Postcode"))

    ## Apply UDFs to determine if the legalRep has a UK address - 2. Pull country. Is the country United Kingdom?
    ).withColumn(
    "countryFromAddress",
    getCountryFromAddressUDF(col("legalRepAddressUK"))

    ## Apply UDFs to determine if the legalRep has a UK address - 3. Is the Postcode or country from the UK?
    ).withColumn(
    "legalRepHasAddress",
    legalRepHasAddressUDF(
        col("RepresentativeId"),
        col("ukPostcode"),
        "countryFromAddress"
    )
                                                                             
    ).withColumn("legalRepAddressUK", col("legalRepAddressUK")

    ).withColumn("oocAddressLine1",                                            #If Address1 is null use 2. If 1,2 null use 3
                when((col("CaseRep_Address1").isNotNull()), col("CaseRep_Address1")
                      ).otherwise(coalesce(col("CaseRep_Address2"),
                          col("CaseRep_Address3"), col("CaseRep_Address4"),
                          col("CaseRep_Address5"))
                          )
                 
    ).withColumn("oocAddressLine2", 
                when((col("CaseRep_Address2").isNotNull()), col("CaseRep_Address2")
                      ).otherwise(coalesce(col("CaseRep_Address3"),
                          col("CaseRep_Address4"), col("CaseRep_Address5"))
                )
                 
    ).withColumn("oocAddressLine3",
                when((col("CaseRep_Address3").isNotNull()), concat(col("CaseRep_Address3"), lit(", "), col("CaseRep_Address4"))
                      ).otherwise(col("CaseRep_Address4")
                )
    
    ).withColumn("oocAddressLine4",
                when((col("CaseRep_Address5").isNotNull()), concat(col("CaseRep_Address5"), lit(", "), col("CaseRep_Postcode"))
                      ).otherwise(col("CaseRep_Postcode")
                )
                 
    ).withColumn(
        "oocLrCountryGovUkAdminJ",
        getCountryLRUDF(col("legalRepAddressUK"))     #Use UDF getCountryLR(). Sample data indicates function works.
                 
    ).select(
        col("CaseNo"),
        "legalRepGivenName",
        "legalRepFamilyNamePaperJ",
        "legalRepCompanyPaperJ",
        "localAuthorityPolicy",
        "legalRepEmail",
        "legalRepAddressUK",         
        "legalRepHasAddress",
        "oocAddressLine1",
        "oocAddressLine2",
        "oocAddressLine3",
        "oocAddressLine4",
        "oocLrCountryGovUkAdminJ"
    )

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("m1_audit.dv_representation"), col("m1_audit.lu_appealType")]

    df_audit = silver_m1.alias("m1_audit").join(df.alias("content"), on = ["CaseNo"], how = "left").filter(conditions_legalRepDetails).select(
        col("CaseNo"),

        ## legalRepGivenName - ARIADM-767
        array(struct(*common_inputFields ,lit("m1_audit.Contact"), lit("m1_audit.Rep_Name"), lit("m1_audit.CaseRep_Name"))).alias("legalRepGivenName_inputFields"),
        array(struct(*common_inputValues ,col("m1_audit.Contact"), col("m1_audit.Rep_Name"), col("m1_audit.CaseRep_Name"))).alias("legalRepGivenName_inputValues"),
        col("content.legalRepGivenName"),
        lit("yes").alias("legalRepGivenName_Transformed"),

        # ## legalRepFamilyNamePaperJ - ARIADM-767
        array(struct(*common_inputFields ,lit("m1_audit.Rep_Name"), lit("m1_audit.CaseRep_Name"))).alias("legalRepFamilyNamePaperJ_inputFields"),
        array(struct(*common_inputValues ,col("m1_audit.Rep_Name"), col("m1_audit.CaseRep_Name"))).alias("legalRepFamilyNamePaperJ_inputValues"),
        col("content.legalRepFamilyNamePaperJ"),
        lit("yes").alias("legalRepFamilyNamePaperJ_Transformed"),

        # ## legalRepCompanyPaperJ - ARIADM-767
        array(struct(*common_inputFields ,lit("m1_audit.Rep_Name"), lit("m1_audit.CaseRep_Name"))).alias("legalRepCompanyPaperJ_inputFields"),
        array(struct(*common_inputValues ,col("m1_audit.Rep_Name"), col("m1_audit.CaseRep_Name"))).alias("legalRepCompanyPaperJ_inputValues"),
        col("content.legalRepCompanyPaperJ"),
        lit("yes").alias("legalRepCompanyPaperJ_Transformed"),

        # ## localAuthorityPolicy - ARIADM-767
        array(struct(*common_inputFields)).alias("localAuthorityPolicy_inputFields"),
        array(struct(*common_inputValues)).alias("localAuthorityPolicy_inputValues"),
        col("content.localAuthorityPolicy"),
        lit("no").alias("localAuthorityPolicy_Transformed"),

        # ## legalRepEmail - ARIADM-771
        array(struct(*common_inputFields, lit("m1_audit.Rep_Email"), lit("m1_audit.CaseRep_Email"), lit("m1_audit.CaseRep_FileSpecific_Email"))).alias("legalRepEmail_inputFields"),
        array(struct(*common_inputValues, col("m1_audit.Rep_Email"), col("m1_audit.CaseRep_Email"), col("m1_audit.CaseRep_FileSpecific_Email"))).alias("legalRepEmail_inputValues"),
        col("content.legalRepEmail"), 
        lit("yes").alias("legalRepEmail_Transformed"),

        ## legalRepAddressUK - ARIADM-769 - Check both CaseRepAddress and RepAddress
        array(struct(*common_inputFields, lit("m1_audit.Rep_Address1"), lit("m1_audit.Rep_Address2"), lit("m1_audit.Rep_Address3"),lit("m1_audit.Rep_Address4"),lit("m1_audit.Rep_Address5"),lit("m1_audit.Rep_Postcode"),lit("m1_audit.CaseRep_Address1"), lit("m1_audit.CaseRep_Address2"), lit("m1_audit.CaseRep_Address3"),lit("m1_audit.CaseRep_Address4"),lit("m1_audit.CaseRep_Address5"),lit("m1_audit.CaseRep_Postcode") )).alias("legalRepAddressUK_inputFields"),
        array(struct(*common_inputValues, col("m1_audit.Rep_Address1"), col("m1_audit.Rep_Address2"), col("m1_audit.Rep_Address3"),col("m1_audit.Rep_Address4"),col("m1_audit.Rep_Address5"),col("m1_audit.Rep_Postcode"), col("m1_audit.CaseRep_Address1"), col("m1_audit.CaseRep_Address2"), col("m1_audit.CaseRep_Address3"),col("m1_audit.CaseRep_Address4"),col("m1_audit.CaseRep_Address5"),col("m1_audit.CaseRep_Postcode"))).alias("legalRepAddressUK_inputValues"),
        col("content.legalRepAddressUK"), 
        lit("yes").alias("legalRepAddressUK_Transformed"),

        ## legalRepHasAddress - ARIADM-769
        array(struct(*common_inputFields, lit("m1_audit.CaseRep_Postcode"), lit("content.legalRepAddressUK"), lit("m1_audit.RepresentativeId"))).alias("legalRepHasAddress_inputFields"),
        array(struct(*common_inputValues, col("m1_audit.CaseRep_Postcode"), col("content.legalRepAddressUK"), col("m1_audit.RepresentativeId"))).alias("legalRepHasAddress_inputValues"),
        col("content.legalRepHasAddress"), 
        lit("yes").alias("legalRepHasAddress_Transformed"),

        ## oocAddressLine1 - ARIADM-769
        array(struct(*common_inputFields, lit("m1_audit.CaseRep_Address1"), lit("m1_audit.CaseRep_Address2"), lit("m1_audit.CaseRep_Address3"),lit("m1_audit.CaseRep_Address4"),lit("m1_audit.CaseRep_Address5"),lit("m1_audit.CaseRep_Postcode"))).alias("oocAddressLine1_inputFields"),
        array(struct(*common_inputValues, col("m1_audit.CaseRep_Address1"), col("m1_audit.CaseRep_Address2"), col("m1_audit.CaseRep_Address3"),col("m1_audit.CaseRep_Address4"),col("m1_audit.CaseRep_Address5"),col("m1_audit.CaseRep_Postcode"))).alias("oocAddressLine1_inputValues"),
        col("content.oocAddressLine1"), 
        lit("yes").alias("oocAddressLine1_Transformed"),

        ## oocAddressLine2 - ARIADM-769
        array(struct(*common_inputFields, lit("m1_audit.CaseRep_Address2"), lit("m1_audit.CaseRep_Address3"),lit("m1_audit.CaseRep_Address4"),lit("m1_audit.CaseRep_Address5"),lit("m1_audit.CaseRep_Postcode"))).alias("oocAddressLine2_inputFields"),
        array(struct(*common_inputValues, col("m1_audit.CaseRep_Address2"), col("m1_audit.CaseRep_Address3"),col("m1_audit.CaseRep_Address4"),col("m1_audit.CaseRep_Address5"),col("m1_audit.CaseRep_Postcode"))).alias("oocAddressLine2_inputValues"),
        col("content.oocAddressLine2"), 
        lit("yes").alias("oocAddressLine2_Transformed"),

        ## oocAddressLine3 - ARIADM-769
        array(struct(*common_inputFields, lit("m1_audit.CaseRep_Address3"),lit("m1_audit.CaseRep_Address4"),lit("m1_audit.CaseRep_Address5"),lit("m1_audit.CaseRep_Postcode"))).alias("oocAddressLine3_inputFields"),
        array(struct(*common_inputValues, col("m1_audit.CaseRep_Address3"),col("m1_audit.CaseRep_Address4"),col("m1_audit.CaseRep_Address5"),col("m1_audit.CaseRep_Postcode"))).alias("oocAddressLine3_inputValues"),
        col("content.oocAddressLine3"), 
        lit("yes").alias("oocAddressLine3_Transformed"),

        ## oocAddressLine4 - ARIADM-769
        array(struct(*common_inputFields, lit("m1_audit.CaseRep_Address4"),lit("m1_audit.CaseRep_Address5"),lit("m1_audit.CaseRep_Postcode"))).alias("oocAddressLine4_inputFields"),
        array(struct(*common_inputValues, col("m1_audit.CaseRep_Address4"),col("m1_audit.CaseRep_Address5"),col("m1_audit.CaseRep_Postcode"))).alias("oocAddressLine4_inputValues"),
        col("content.oocAddressLine4"), 
        lit("yes").alias("oocAddressLine4_Transformed"),

        ## oocLrCountryGovUkAdminJ - ARIADM-769
        array(struct(*common_inputFields, lit("content.oocAddressLine1"),lit("content.oocAddressLine2"),
        lit("content.oocAddressLine3"),lit("content.oocAddressLine4"), lit("m1_audit.CaseRep_Postcode"))).alias("oocLrCountryGovUkAdminJ_inputFields"),
        array(struct(*common_inputValues, col("content.oocAddressLine1"),col("content.oocAddressLine2"),
        col("content.oocAddressLine3"),col("content.oocAddressLine4"), col("m1_audit.CaseRep_Postcode"))).alias("oocLrCountryGovUkAdminJ_inputValues"),
        col("content.oocLrCountryGovUkAdminJ"), 
        lit("yes").alias("oocLrCountryGovUkAdminJ_Transformed"),
    )

    return df, df_audit

################################################################
##########           generalAddress Function         ###########
################################################################

# Step 1: list of countries
all_countries = pycountry.countries
lower_countries = [country.name.lower() for country in all_countries]

translator = str.maketrans('', '', string.punctuation)

# Step 2: UDF to combine address fields
def makeFullAddress(
    a1, a2, a3, a4, a5, postcode
):
    return ', '.join(map(str, [a1, a2, a3, a4, a5, postcode]))

makeFullAddressUDF = udf(makeFullAddress, StringType())

# Step 3: UDF to check if postcode is UK (simplified)
def getUkPostcode(postcode):
    try:
        if postcode is None or str(postcode).strip() == "":
            return "False"
        
        # Step 1: Check if postcode is valid
        if postcode_utils.is_valid(postcode):
            return "True"
        
        # Step 2: Try fixing the postcode if not valid
        clean_postcode = fix(postcode)
        if postcode_utils.is_valid(clean_postcode):
            return "True"

    except Exception as e:
        print(f"Error processing {postcode}: {e}")
        pass

    return "False"

getUkPostcodeUDF = udf(getUkPostcode, StringType())

# Step 4: UDF to find a country from address

def getCountryFromAddress(address):
    countries_matched = []
    low_address = address.lower()

    translator = str.maketrans('', '', string.punctuation)

    split_address = low_address.split(' ')
    for word in reversed(split_address):
        if word.strip() in lower_countries:
            countries_matched.append(word.strip().capitalize())
            break

    if len(countries_matched) > 0:
        return ', '.join(countries_matched)

    for word in reversed(split_address):
        clean_word = word.translate(translator)
        if clean_word.lower().strip() in lower_countries:
            countries_matched.append(clean_word.strip().capitalize())
            break

    if len(countries_matched) > 0:
        return ', '.join(countries_matched)

    split_address = low_address.split(' ')
    for word in reversed(split_address):
        clean_word = word.translate(translator)
        if clean_word.strip() in lower_countries:
            countries_matched.append(clean_word.strip().capitalize())
            break

    if len(countries_matched) > 0:
        return ', '.join(countries_matched)

    split_address = re.split(r'[,\.\s]+', low_address)
    for word in split_address:
        clean_word = word.translate(translator)
        if clean_word.strip() in lower_countries:
            countries_matched.append(clean_word.strip().capitalize())
            break

    return ', '.join(countries_matched)

getCountryFromAddressUDF = udf(getCountryFromAddress, StringType())

# Step 5: UDF to decide if LR address is UK
def legalRepHasAddress(repId, clean_postcode, full_address):
    if repId > 0:
        return "Yes"

    if repId == 0:
        if clean_postcode == "True":
            return "Yes"
        elif clean_postcode == "False":
            country = getCountryFromAddress(full_address)
            if country == "United Kingdom":
                return "Yes"
            elif country != "United Kingdom":
                return "No"
            elif country == '':
                return "Yes"
    return "No"

legalRepHasAddressUDF = udf(legalRepHasAddress, StringType())

# ###################### UDF function to replace get_country_LR ######################

def getCountryLR(full_address):

    if full_address is None:
        return ''
    
    address_clean = full_address.lower().translate(translator)
    
    for idx, lower_country in enumerate(lower_countries):
        if lower_country in address_clean:
            return lower_country.title()
    
    return ''

getCountryLRUDF = udf(getCountryLR, StringType())

################################################################
##########         appellantDetails Function         ###########
################################################################

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import pandas as pd

def getCountryApp(country, ukPostcodeAppellant, appellantFullAddress, Appellant_Postcode):
    countryFromAddress = []
    try:
        country = country
        uk_postcode = ukPostcodeAppellant
        full_address = appellantFullAddress

        if (country is not None and country != 0) or (country is not None and pd.notna(country)):
            countryFromAddress.append(str(country))
            return ', '.join(countryFromAddress)
        else:
            if uk_postcode is True or uk_postcode == "True":
                countryFromAddress.append('GB')
                return ', '.join(countryFromAddress)
            if uk_postcode is False or uk_postcode == "False":
                searched_country = getCountryFromAddress(full_address)
                countryFromAddress.append(searched_country)
                return ', '.join(countryFromAddress)
        return ', '.join(countryFromAddress)
    except Exception:
        return None

getCountryApp_udf = udf(getCountryApp, StringType())


# AppealType grouping
def appellantDetails(silver_m1, silver_m2, silver_c,bronze_countryFromAddress,bronze_HORef_cleansing):
    conditions = (col("dv_representation").isin('LR', 'AIP')) & (col("lu_appealType").isNotNull())

    # Create DataFrame with CaseNo and list of CategoryId
    silver_c_grouped = silver_c.groupBy("CaseNo").agg(collect_list(col("CategoryId")).alias("CategoryIdList"))

    # Create DataFrame with CaseNo and collection of lu_countryCode from silver_m1
    silver_m1_country_grouped = silver_m1.groupBy("CaseNo").agg(
    transform(
        collect_list(col("lu_countryCode")),
        lambda code: struct(
            expr("uuid()").alias("id"),
            struct(code.alias("code")).alias("value")
        )
    ).alias("appellantNationalities"),
    collect_list(col("lu_countryCode")).alias("lu_countryCodeList")
    )

    # isAppellantMinor: BirthDate > (DateLodged - 18 years) using year subtraction
    is_minor_expr = when(
        conditions & ((datediff(col("DateLodged"), col("BirthDate")) / 365.25) < 18),
        lit("Yes")
    ).when(conditions, lit("No")).otherwise(None)

    # DeportationOrderOptions logic
    deportation_category_ids = [17, 19, 29, 30, 31, 32, 41, 48]
    deportation_condition = (
        (col("CategoryIdList").isNotNull() & (expr(f"size(array_intersect(CategoryIdList, array({','.join(map(str, deportation_category_ids))})) )") > 0)) |
        (col("dv_CCDAppealType") == "DA") |
        (col("DeportationDate").isNotNull()) |
        (col("RemovalDate").isNotNull())
    )

    # appellantInUk logic
    appellant_in_uk_expr = when(
        conditions & (expr("array_contains(CategoryIdList, 37)")), lit("Yes")
    ).when(
        conditions & (expr("array_contains(CategoryIdList, 38)")), lit("No")
    ).otherwise(None)

    # appealOutOfCountry logic
    appeal_out_of_country_expr = when(
        conditions & (expr("array_contains(CategoryIdList, 38)")), lit("Yes")
    ).otherwise(None)

    # appellantStateless logic
    appellant_stateless_expr = when(
        conditions & (col("AppellantCountryId") == 211), lit("isStateless")
    ).when(
        conditions, lit("hasNationality")
    ).otherwise(None)
    
    # appellantHasFixedAddress logic
    appellant_has_fixed_address_expr = when(
        conditions & expr("array_contains(CategoryIdList, 37)"), lit("Yes")
    ).otherwise(None)

    # appellantHasFixedAddressAdminJ logic: Only include if CategoryIdList contains 38
    appellant_has_fixed_address_adminj_expr = when(conditions &
        expr("array_contains(CategoryIdList, 38)"), lit("Yes")
    ).otherwise(None) 

    # appellantAddress logic
    # Only include if CategoryIdList contains 37 and conditions
    include_appellant_address = (conditions & expr("array_contains(CategoryIdList, 37)") & 
                                    (coalesce(col("Appellant_Address1"), col("Appellant_Address2"), col("Appellant_Address3"), col("Appellant_Address4"), col("Appellant_Address5"), col("Appellant_Postcode")).isNotNull()))
    appellant_address_struct = when(
        include_appellant_address,
        struct(
            # AddressLine1 is mandatory in CCD, fallback logic
            coalesce(
                col("Appellant_Address1"),
                col("Appellant_Address2"),
                col("Appellant_Address3"),
                col("Appellant_Address4"),
                col("Appellant_Address5")
            ).alias("AddressLine1"),
            col("Appellant_Address2").alias("AddressLine2"),
            lit("").alias("AddressLine3"),
            col("Appellant_Address3").alias("PostTown"),
            col("Appellant_Address4").alias("County"),
            col("Appellant_Address5").alias("Country"),
            col("Appellant_Postcode").alias("PostCode")
        )
    ).otherwise(None)

    # addressLine1AdminJ: mandatory for OOC, fallback through address fields
    address_line1_adminj_expr = when(
        conditions & expr("array_contains(CategoryIdList, 38)"),
        coalesce(
            col("Appellant_Address1"),
            col("Appellant_Address2"),
            col("Appellant_Address3"),
            col("Appellant_Address4"),
            col("Appellant_Address5"),
            col("Appellant_Postcode")
        )
    ).otherwise(None)

    # addressLine2AdminJ: mandatory for OOC, fallback through address fields (skip the one used for addressLine1AdminJ)
    address_line2_adminj_expr = when(
        conditions & expr("array_contains(CategoryIdList, 38)"),
        coalesce(
            # Exclude the value used for addressLine1AdminJ
            when(col("Appellant_Address1").isNull(), None).otherwise(
                coalesce(
                    when(col("Appellant_Address2") != col("Appellant_Address1"), col("Appellant_Address2")),
                    when(col("Appellant_Address3") != col("Appellant_Address1"), col("Appellant_Address3")),
                    when(col("Appellant_Address4") != col("Appellant_Address1"), col("Appellant_Address4")),
                    when(col("Appellant_Address5") != col("Appellant_Address1"), col("Appellant_Address5")),
                    when(col("Appellant_Postcode") != col("Appellant_Address1"), col("Appellant_Postcode"))
                )
            ),
            when(col("Appellant_Address2").isNull() & col("Appellant_Address1").isNull(), None).otherwise(
                coalesce(
                    when(col("Appellant_Address3") != col("Appellant_Address2"), col("Appellant_Address3")),
                    when(col("Appellant_Address4") != col("Appellant_Address2"), col("Appellant_Address4")),
                    when(col("Appellant_Address5") != col("Appellant_Address2"), col("Appellant_Address5")),
                    when(col("Appellant_Postcode") != col("Appellant_Address2"), col("Appellant_Postcode"))
                )
            ),
            when(col("Appellant_Address3").isNull() & col("Appellant_Address2").isNull() & col("Appellant_Address1").isNull(), None).otherwise(
                coalesce(
                    when(col("Appellant_Address4") != col("Appellant_Address3"), col("Appellant_Address4")),
                    when(col("Appellant_Address5") != col("Appellant_Address3"), col("Appellant_Address5")),
                    when(col("Appellant_Postcode") != col("Appellant_Address3"), col("Appellant_Postcode"))
                )
            ),
            when(col("Appellant_Address4").isNull() & col("Appellant_Address3").isNull() & col("Appellant_Address2").isNull() & col("Appellant_Address1").isNull(), None).otherwise(
                coalesce(
                    when(col("Appellant_Address5") != col("Appellant_Address4"), col("Appellant_Address5")),
                    when(col("Appellant_Postcode") != col("Appellant_Address4"), col("Appellant_Postcode"))
                )
            ),
            when(col("Appellant_Address5").isNull() & col("Appellant_Address4").isNull() & col("Appellant_Address3").isNull() & col("Appellant_Address2").isNull() & col("Appellant_Address1").isNull(), None).otherwise(
                coalesce(
                    when(col("Appellant_Postcode") != col("Appellant_Address5"), col("Appellant_Postcode"))
                )
            )
        )
    ).otherwise(None)

    address_line3_adminj_expr = when(
    conditions & expr("array_contains(CategoryIdList, 38)") & (col("Appellant_Address3").isNotNull() | col("Appellant_Address4").isNotNull()),
    concat_ws(
        ", ",
        col("Appellant_Address3"), col("Appellant_Address4")
    )).otherwise(None)

    # addressLine4AdminJ logic
    # IF CategoryId IN [38] = Include; ELSE OMIT
    # AppellantAddress5 + ', ' + AppellantPostcode
    address_line4_adminj_expr = when(
        conditions & expr("array_contains(CategoryIdList, 38)") & (col("Appellant_Address5").isNotNull() | col("Appellant_Postcode").isNotNull()),
        concat_ws(",",
            col("Appellant_Address5"), col("Appellant_Postcode")
        )
    ).otherwise(None)


    
    # Join bronze_HORef_cleansing to get CleansedHORef using CaseNo and coalesce(HORef, FCONumber)
    bronze_cleansing = bronze_HORef_cleansing.select(
        col("CaseNo"),
        coalesce(col("HORef"), col("FCONumber")).alias("lu_HORef")
    )

    # oocAppealAdminJ logic
    ooc_appeal_adminj_expr = when(
        conditions & expr("array_contains(CategoryIdList, 38)"),
        when(
              (col("lu_HORef").like("%GWF%")) |
              (col("HORef").like("%GWF%")) |
              (col("FCONumber").like("%GWF%"))
        
        , lit("entryClearanceDecision")).otherwise(lit("none"))
    ).otherwise(None)

    silver_m2 = silver_m2.filter(col("Relationship").isNull())

    silver_m2_derived = silver_m2.withColumn(
                                        "appellantFullAddress",
                                        concat_ws(",",
                                            col("Appellant_Address1"),
                                            col("Appellant_Address2"),
                                            col("Appellant_Address3"),
                                            col("Appellant_Address4"),
                                            col("Appellant_Address5"),
                                            col("Appellant_Postcode")
                                        )
                                    ).withColumn(
                                        "ukPostcodeAppellant",
                                        getUkPostcodeUDF(col("Appellant_Postcode"))
                                    ).withColumn(
                                        "dv_countryGovUkOocAdminJ",
                                        getCountryApp_udf(
                                            col("lu_countryGovUkOocAdminJ").alias("country"),
                                            col("ukPostcodeAppellant"),
                                            col("appellantFullAddress"),
                                            col("Appellant_Postcode")
                                        )
                                    )


    bronze_countries_countryFromAddress = bronze_countryFromAddress.withColumn("lu_cfa_countryGovUkOocAdminJ",col("countryGovUkOocAdminJ")).withColumn("lu_cfa_contryFromAddress", col("countryFromAddress"))

    silver_m2_derived = silver_m2_derived.alias('main').join(bronze_countries_countryFromAddress.alias('cfa'), col("main.dv_countryGovUkOocAdminJ") == col("cfa.lu_cfa_contryFromAddress"), "left").select("main.*",  when( col("lu_cfa_contryFromAddress").isNotNull(),col("lu_cfa_countryGovUkOocAdminJ"))
          .otherwise(col("dv_countryGovUkOocAdminJ")).alias("countryGovUkOocAdminJ"))
    
    country_gov_uk_ooc_adminj_expr = when(
        conditions & expr("array_contains(CategoryIdList, 38)"),
        col("countryGovUkOocAdminJ")
    ).otherwise(None)
          

    # # display(df)

    # df.select("CaseNo", "appellantFullAddress", "ukPostcodeAppellant", 
    #       when( col("lu_cfa_contryFromAddress").isNotNull(),col("lu_cfa_countryGovUkOocAdminJ"))
    #       .otherwise(col("countryGovUkOocAdminJ")).alias("countryGovUkOocAdminJ")
    #       ).display()

    # internalAppellantEmail logic
    internal_appellant_email_expr = when(
        conditions & col("Appellant_Email").isNotNull(), cleanEmailUDF(col("Appellant_Email"))
    ).otherwise(None)

    # internalAppellantMobileNumber logic
    internal_appellant_mobile_number_expr = when(
        conditions & col("Appellant_Telephone").isNotNull(), phoneNumberUDF(col("Appellant_Telephone"))
    ).otherwise(None)

    # mobileNumber logic (same as internalAppellantMobileNumber)
    mobile_number_expr = internal_appellant_mobile_number_expr

    # email logic (same as internalAppellantEmail)
    email_expr = internal_appellant_email_expr

    df = silver_m1.alias("silver_m1") \
        .join(silver_m2_derived, ["CaseNo"], "left") \
        .join(silver_c_grouped, ["CaseNo"], "left") \
        .join(silver_m1_country_grouped, ["CaseNo"], "left") \
        .join(bronze_cleansing, ["CaseNo"], "left") \
        .select(
            col("CaseNo"),
            when(
                conditions,
                col("Appellant_Name")
            ).otherwise(None).alias("appellantFamilyName"),
            when(
                conditions,
                col("Appellant_Forenames")
            ).otherwise(None).alias("appellantGivenNames"),
            when(conditions, 
                 concat(col("Appellant_Forenames"), lit(" "), col("Appellant_Name"))
            ).otherwise(None).alias("appellantNameForDisplay"),
            when(conditions,
                 date_format(col("BirthDate"),"yyyy-MM-dd")
            ).otherwise(None).alias("appellantDateOfBirth"),
            is_minor_expr.alias("isAppellantMinor"),
            when(conditions,
                 concat(col("Appellant_Forenames"), lit(" "), col("Appellant_Name"))
            ).otherwise(None).alias("caseNameHmctsInternal"),
            when(conditions,
                 concat(col("Appellant_Forenames"), lit(" "), col("Appellant_Name"))
            ).otherwise(None).alias("hmctsCaseNameInternal"),
            internal_appellant_email_expr.alias("internalAppellantEmail"),
            email_expr.alias("email"),
            internal_appellant_mobile_number_expr.alias("internalAppellantMobileNumber"),
            mobile_number_expr.alias("mobileNumber"),
            appellant_in_uk_expr.alias("appellantInUk"),
            appeal_out_of_country_expr.alias("appealOutOfCountry"),
            ooc_appeal_adminj_expr.alias("oocAppealAdminJ"),
            appellant_has_fixed_address_expr.alias("appellantHasFixedAddress"),
            appellant_has_fixed_address_adminj_expr.alias("appellantHasFixedAddressAdminJ"),
            appellant_address_struct.alias("appellantAddress"),
            address_line1_adminj_expr.alias("addressLine1AdminJ"),
            address_line2_adminj_expr.alias("addressLine2AdminJ"),
            address_line3_adminj_expr.alias("addressLine3AdminJ"),
            address_line4_adminj_expr.alias("addressLine4AdminJ"),
            country_gov_uk_ooc_adminj_expr.alias("countryGovUkOocAdminJ"),
            appellant_stateless_expr.alias("appellantStateless"),
            when(
                conditions,
                when(
                    (size(col("appellantNationalities")) == 0) |
                    (array_contains(expr("transform(appellantNationalities, x -> x.value.code)"), "NO MAPPING REQUIRED")),
                    lit(None)
                ).otherwise(col("appellantNationalities"))
            ).otherwise(lit(None)).alias("appellantNationalities"),
            when(conditions, col("lu_appellantNationalitiesDescription")).otherwise(None).alias("appellantNationalitiesDescription"),
            when(conditions & deportation_condition ,
                lit("Yes")
            ).when(conditions, lit("No")).otherwise(None).alias("deportationOrderOptions")
            # internalAppellantEmail: IF AppellantEmail IS NULL = OMIT; ELSE Data from ARIA
            # internalAppellantMobileNumber: IF AppellantTelephone IS NULL = OMIT; ELSE Data from ARIA
            
            # mobileNumber: same as internalAppellantMobileNumber, with conditions
            # email: same as internalAppellantEmail, with conditions
        ).distinct()

    # new_order = ["appellantFamilyName", "appellantGivenNames","appellantNameForDisplay", "appellantDateOfBirth", "isAppellantMinor", "caseNameHmctsInternal","hmctsCaseNameInternal", "internalAppellantEmail", "email", "internalAppellantMobileNumber", "mobileNumber", "appellantInUk", "appealOutOfCountry", "oocAppealAdminJ", "appellantHasFixedAddress", "appellantHasFixedAddressAdminJ", "appellantAddress", "addressLine1AdminJ", "addressLine2AdminJ", "addressLine3AdminJ", "addressLine4AdminJ", "countryGovUkOocAdminJ", "appellantStateless", "appellantNationalities", "appellantNationalitiesDescription", "deportationOrderOptions"]

    # df = df.select(*new_order)

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("audit.dv_representation"), col("audit.lu_appealType")]

    df_audit = silver_m1.join(silver_m2_derived, ["CaseNo"], "left") \
        .alias("audit") \
        .join(silver_c_grouped, ["CaseNo"], "left") \
        .join(silver_m1_country_grouped, ["CaseNo"], "left") \
        .join(df.alias("content"), ["CaseNo"],"left") \
        .join(bronze_cleansing.alias("content_c"), ["CaseNo"],"left") \
        .select(
            col("CaseNo"),

            #Audit appellantFamilyName
            array(struct(*common_inputFields, lit("Appellant_Name"))).alias("appellantFamilyName_inputFields"),
            array(struct(*common_inputValues, col("Appellant_Name"))).alias("appellantFamilyName_inputValues"),
            col("content.appellantFamilyName"),
            lit("no").alias("appellantFamilyName_Transformation"),

            #Audit appellantGivenNames
            array(struct(*common_inputFields, lit("Appellant_Forenames"))).alias("appellantGivenNames_inputfields"),
            array(struct(*common_inputValues, col("Appellant_Forenames"))).alias("appellantGivenNames_inputvalues"),
            col("content.appellantGivenNames"),
            lit("no").alias("appellantGivenNames_Transformation"),

            #Audit appellantNameForDisplay
            array(struct(*common_inputFields,lit("Appellant_Name"), lit("Appellant_Forenames"))).alias("appellantNameForDisplay_inputfields"),
            array(struct(*common_inputValues,col("Appellant_Name"), col("Appellant_Forenames"))).alias("appellantNameForDisplay_inputvalues"),
            col("content.appellantNameForDisplay"),
            lit("no").alias("appellantNameForDisplay_Transformation"),

            #Audit appellantDateOfBirth
            array(struct(*common_inputFields,lit("BirthDate"))).alias("appellantDateOfBirth_inputfields"),
            array(struct(*common_inputValues,col("BirthDate"))).alias("appellantDateOfBirth_inputvalues"),
            col("content.appellantDateOfBirth"),
            lit("no").alias("appellantDateOfBirth_Transformation"),

            #Audit caseNameHmctsInternal
            array(struct(*common_inputFields,lit("Appellant_Name"), lit("Appellant_Forenames"))).alias("caseNameHmctsInternal_inputfields"),
            array(struct(*common_inputValues,col("Appellant_Name"), col("Appellant_Forenames"))).alias("caseNameHmctsInternal_inputvalues"),
            col("content.caseNameHmctsInternal"),
            lit("no").alias("caseNameHmctsInternal_Transformation"),

            #Audit hmctsCaseNameInternal
            array(struct(*common_inputFields,lit("Appellant_Name"), lit("Appellant_Forenames"))).alias("hmctsCaseNameInternal_inputfields"),
            array(struct(*common_inputValues,col("Appellant_Name"), col("Appellant_Forenames"))).alias("hmctsCaseNameInternal_inputvalues"),
            col("content.hmctsCaseNameInternal"),
            lit("no").alias("hmctsCaseNameInternal_Transformation"),

            # Audit deportationOrderOptions
            array(struct(*common_inputFields, lit("CategoryId"), lit("dv_CCDAppealType"), lit("DeportationDate"), lit("RemovalDate"))).alias("deportationOrderOptions_inputfields"),
            array(struct(*common_inputValues, col("CategoryIdList"), col("dv_CCDAppealType"), col("DeportationDate"), col("RemovalDate"))).alias("deportationOrderOptions_inputvalues"),
            col("content.deportationOrderOptions"),
            lit("yes").alias("deportationOrderOptions_Transformation"),

            # Audit appellantInUk
            array(struct(*common_inputFields, lit("CategoryId"))).alias("appellantInUk_inputfields"),
            array(struct(*common_inputValues, col("CategoryIdList"))).alias("appellantInUk_inputvalues"),
            col("content.appellantInUk"),
            lit("yes").alias("appellantInUk_Transformation"),

            # Audit appealOutOfCountry
            array(struct(*common_inputFields, lit("CategoryId"))).alias("appealOutOfCountry_inputfields"),
            array(struct(*common_inputValues, col("CategoryIdList"))).alias("appealOutOfCountry_inputvalues"),
            col("content.appealOutOfCountry"),
            lit("yes").alias("appealOutOfCountry_Transformation"),

            # Audit isAppellantMinor
            array(struct(*common_inputFields, lit("BirthDate"), lit("DateLodged"))).alias("isAppellantMinor_inputfields"),
            array(struct(*common_inputValues, col("BirthDate"), col("DateLodged"))).alias("isAppellantMinor_inputvalues"),
            col("content.isAppellantMinor"),
            lit("yes").alias("isAppellantMinor_Transformation"),

            # Audit appellantStateless
            array(struct(*common_inputFields, lit("AppellantCountryId"))).alias("appellantStateless_inputfields"),
            array(struct(*common_inputValues, col("AppellantCountryId"))).alias("appellantStateless_inputvalues"),
            col("content.appellantStateless"),
            lit("yes").alias("appellantStateless_Transformation"),

            # Audit appellantNationalities
            array(struct(*common_inputFields, lit("lu_countryCodeList"))).alias("appellantNationalities_inputfields"),
            array(struct(*common_inputValues, col("lu_countryCodeList"))).alias("appellantNationalities_inputvalues"),
            col("content.appellantNationalities"),
            lit("yes").alias("appellantNationalities_Transformation"),

            # Audit appellantNationalitiesDescription
            array(struct(*common_inputFields, lit("lu_appellantNationalitiesDescription"))).alias("lu_appellantNationalitiesDescription_inputfields"),
            array(struct(*common_inputValues, col("appellantNationalitiesDescription"))).alias("appellantNationalitiesDescription_inputvalues"),
            col("content.appellantNationalitiesDescription"),
            lit("yes").alias("appellantNationalitiesDescription_Transformation"),

            # Audit appellantHasFixedAddress
            array(struct(*common_inputFields, lit("CategoryIdList"))).alias("appellantHasFixedAddress_inputfields"),
            array(struct(*common_inputValues, col("CategoryIdList"))).alias("appellantHasFixedAddress_inputvalues"),
            col("content.appellantHasFixedAddress"),
            lit("yes").alias("appellantHasFixedAddress_Transformation"),

            # Audit appellantAddress
            array(struct(*common_inputFields, lit("Appellant_Address1"), lit("Appellant_Address2"), lit("Appellant_Address3"), lit("Appellant_Address4"), lit("Appellant_Address5"), lit("Appellant_Postcode"), lit("CategoryIdList"))).alias("appellantAddress_inputfields"),
            array(struct(*common_inputValues, col("Appellant_Address1"), col("Appellant_Address2"), col("Appellant_Address3"), col("Appellant_Address4"), col("Appellant_Address5"), col("Appellant_Postcode"), col("CategoryIdList"))).alias("appellantAddress_inputvalues"),
            col("content.appellantAddress"),
            lit("yes").alias("appellantAddress_Transformation"),



            # Audit oocAppealAdminJ
            array(struct(*common_inputFields, lit("CategoryIdList"), lit("HORef"),lit("FCONumber"),lit("lu_HORef"))).alias("oocAppealAdminJ_inputfields"),
            array(struct(*common_inputValues, col("CategoryIdList"), col("HORef"),col("FCONumber"),col("content_c.lu_HORef"))).alias("oocAppealAdminJ_inputvalues"),
            col("content.oocAppealAdminJ"),
            lit("yes").alias("oocAppealAdminJ_Transformation"),

            # Audit appellantHasFixedAddressAdminJ
            array(struct(*common_inputFields,lit("CategoryIdList"))).alias("appellantHasFixedAddressAdminJ_inputfields"),
            array(struct(*common_inputValues, col("CategoryIdList"))).alias("appellantHasFixedAddressAdminJ_inputvalues"),
            col("content.appellantHasFixedAddressAdminJ"),
            lit("yes").alias("appellantHasFixedAddressAdminJ_Transformation"),

            # Audit addressLine1AdminJ
            array(struct(*common_inputFields, lit("Appellant_Address1"),lit("Appellant_Address2"), lit("Appellant_Address3"), lit("Appellant_Address4"), lit("Appellant_Address5"), lit("Appellant_Postcode"), lit("CategoryIdList"))).alias("addressLine1AdminJ_inputfields"),
            array(struct(*common_inputValues, col("Appellant_Address1"),col("Appellant_Address2"), col("Appellant_Address3"), col("Appellant_Address4"), col("Appellant_Address5"), col("Appellant_Postcode"), col("CategoryIdList"))).alias("addressLine1AdminJ_inputvalues"),
            col("content.addressLine1AdminJ"),
            lit("yes").alias("addressLine1AdminJ_Transformation"),

            # Audit addressLine2AdminJ
            array(struct(*common_inputFields, lit("Appellant_Address2"), lit("Appellant_Address3"), lit("Appellant_Address4"), lit("Appellant_Address5"), lit("Appellant_Postcode"), lit("CategoryIdList"))).alias("addressLine2AdminJ_inputfields"),
            array(struct(*common_inputValues, col("Appellant_Address2"), col("Appellant_Address3"), col("Appellant_Address4"), col("Appellant_Address5"), col("Appellant_Postcode"), col("CategoryIdList"))).alias("addressLine2AdminJ_inputvalues"),
            col("content.addressLine2AdminJ"),
            lit("yes").alias("addressLine2AdminJ_Transformation"),

            # Audit addressLine3AdminJ
            array(struct(*common_inputFields, lit("Appellant_Address3"), lit("Appellant_Address4"), lit("CategoryIdList"))).alias("addressLine3AdminJ_inputfields"),
            array(struct(*common_inputValues, col("Appellant_Address3"), col("Appellant_Address4"), col("CategoryIdList"))).alias("addressLine3AdminJ_inputvalues"),
            col("content.addressLine3AdminJ"),
            lit("yes").alias("addressLine3AdminJ_Transformation"),

            # Audit addressLine4AdminJ
            array(struct(*common_inputFields, lit("Appellant_Address5"), lit("Appellant_Postcode"), lit("CategoryIdList"))).alias("addressLine4AdminJ_inputfields"),
            array(struct(*common_inputValues, col("Appellant_Address5"), col("Appellant_Postcode"), col("CategoryIdList"))).alias("addressLine4AdminJ_inputvalues"),
            col("content.addressLine4AdminJ"),
            lit("yes").alias("addressLine4AdminJ_Transformation"),

            # Audit countryGovUkOocAdminJ
            array(struct(*common_inputFields, lit("AppellantCountryId"), lit("ukPostcodeAppellant"), lit("appellantFullAddress"), lit("CategoryIdList"))).alias("countryGovUkOocAdminJ_inputfields"),
            array(struct(*common_inputValues, col("AppellantCountryId"), col("ukPostcodeAppellant"), col("appellantFullAddress"), col("CategoryIdList"))).alias("countryGovUkOocAdminJ_inputvalues"),
            col("content.countryGovUkOocAdminJ"),
            lit("yes").alias("countryGovUkOocAdminJ_Transformation"),

             # Audit internalAppellantEmail
            array(struct(*common_inputFields, lit("Appellant_Email"))).alias("internalAppellantEmail_inputfields"),
            array(struct(*common_inputValues, col("Appellant_Email"))).alias("internalAppellantEmail_inputvalues"),
            col("content.internalAppellantEmail"),
            lit("yes").alias("internalAppellantEmail_Transformation"),

            # Audit internalAppellantMobileNumber
            array(struct(*common_inputFields, lit("Appellant_Telephone"))).alias("internalAppellantMobileNumber_inputfields"),
            array(struct(*common_inputValues, col("Appellant_Telephone"))).alias("internalAppellantMobileNumber_inputvalues"),
            col("content.internalAppellantMobileNumber"),
            lit("yes").alias("internalAppellantMobileNumber_Transformation"),

            # Audit mobileNumber (same as internalAppellantMobileNumber)
            array(struct(*common_inputFields, lit("Appellant_Telephone"))).alias("mobileNumber_inputfields"),
            array(struct(*common_inputValues, col("Appellant_Telephone"))).alias("mobileNumber_inputvalues"),
            col("content.mobileNumber"),
            lit("yes").alias("mobileNumber_Transformation"),

            # Audit email (same as internalAppellantEmail)
            array(struct(*common_inputFields, lit("Appellant_Email"))).alias("email_inputfields"),
            array(struct(*common_inputValues, col("Appellant_Email"))).alias("email_inputvalues"),
            col("content.email"),
            lit("yes").alias("email_Transformation"),
        ).distinct()
    
    return df, df_audit



################################################################
##########            homeOffice Function            ###########
################################################################

# Function to clean reference numbers (HORef, FCONumber) according to business rules
def cleanReferenceNumber(ref):
    """
    Cleans a reference number string according to the following rules:
    - If ref is None, return None.
    - If ref starts with 'GWF', return as is (no cleaning).
    - Remove all spaces and special characters except '/'.
    - If ref contains '/', split into prefix and suffix:
        - If prefix contains no digits, ignore it.
        - Extract digits from prefix and suffix.
        - If suffix has <=3 digits, remove leading zeros.
        - Concatenate prefix digits and suffix digits.
    - If ref does not contain '/', extract all digits.
    - If the cleaned result is 8 digits, pad with leading zero to make 9 digits.
    - If the cleaned result is not 9 digits, return None.
    - Only return if cleaned result is exactly 9 digits.
    """
    if ref is None:
        return None
    ref = ref.strip()
    if ref.startswith("GWF"):
        return ref
    # Remove all spaces and special chars except /
    ref = re.sub(r"[^A-Za-z0-9/]", "", ref)
    # Split on /
    parts = ref.split("/")
    if len(parts) == 2:
        prefix, suffix = parts
        # If prefix is not all digits, ignore it (for ISLAMABAD/123456789, NEWDELHI/12345678, etc.)
        if not re.search(r"\d", prefix):
            prefix_digits = ""
        else:
            prefix_digits = re.sub(r"\D", "", prefix)
            if not prefix_digits and prefix:
                prefix_digits = prefix
        suffix_digits = re.sub(r"\D", "", suffix)
        if not suffix_digits:
            return None
        # Remove leading zeros for suffix if <=3 digits
        if len(suffix_digits) <= 3:
            try:
                suffix_digits = str(int(suffix_digits))
            except:
                return None
        cleaned = prefix_digits + suffix_digits
    elif len(parts) == 1:
        digits = re.sub(r"\D", "", ref)
        cleaned = digits
    else:
        return None
    # If cleaned is 8 digits, pad to 9
    if len(cleaned) == 8:
        cleaned = "0" + cleaned
    elif len(cleaned) != 9:
        return None
    if re.fullmatch(r"\d{9}", cleaned):
        return cleaned
    return None

# Register the cleaning function as a Spark UDF
cleanReferenceNumberUDF = udf(cleanReferenceNumber, StringType())

# # Example usage: Clean HORef in silver_m1 and display results where HORef is not null and length > 9
# silver_m1.withColumn("CleanedHORef", cleanReferenceNumberUDF(col("HORef"))).filter(col("HORef").isNotNull() & (F.length(col("HORef")) > 9)).select("HORef", "CleanedHORef").display()

# # Example usage: Clean FCONumber in silver_m2 and display results where FCONumber is not null and length > 9
# silver_m2.withColumn("CleanedFCONumber", cleanReferenceNumberUDF(col("FCONumber"))).filter(col("FCONumber").isNotNull() & (F.length(col("FCONumber")) > 9)).select("FCONumber", "CleanedFCONumber").display()

# # after len(HORef) > 9 tHEN NULL
# # IF starts with GWF the no cleaning eg GWF064069891
# # remove charters and special char and if after / we like 001 its 1 and 022 its 22 and if its till = 9 the no more clening and if 8 then add leading 0 else null

# # eg - A1222233/3(as 012222333) A14222233/3 (as 142222333 ) T1175081/2 (as 011750812)
# # eg - ISLAMABAD/123456789 as (123456789) NEW DELHI/12345678 (as 012345678 )
# # M1699430/002 (as 016994302 )
# # B1985113/2 (as 019851132 )
# # M0104326/010 (as 010432610 )
# # F3009964/2 (M0104326/010)
# # UKLPA/146205 (INVALID make it null)
# # TN2/5197349 ( AS 025197349 )
# # SHEFF1/4732574 (014732574)
# # B1977303/002 (019773032)

# bronze_HORef_cleansing = spark.table("ariadm_active_appeals.bronze_HORef_cleansing")

# # Join bronze_HORef_cleansing to get CleansedHORef using CaseNo and coalesce(HORef, FCONumber)
# bronze_cleansing = bronze_HORef_cleansing.select(
#     col("CaseNo"),
#     coalesce(col("HORef"), col("FCONumber")).alias("CleansedHORef"),
#     col("HORef"),
#     col("FCONumber"),
#     cleanReferenceNumberUDF(col("HORef")).alias("CleanedHORef"),
#     cleanReferenceNumberUDF(col("FCONumber")).alias("CleanedFCONumber")
# )

# display(bronze_cleansing.filter((~col("HORef").contains("GWF")) & (col("HORef").isNotNull()) & (F.length(col("HORef")) > 9)).select("HORef", "CleanedHORef"))
# display(bronze_cleansing.filter((~col("FCONumber").contains("GWF")) & (col("FCONumber").isNotNull()) & (F.length(col("FCONumber")) > 9)).select("FCONumber", "CleanedFCONumber"))


def homeOfficeDetails(silver_m1, silver_m2, silver_c, bronze_HORef_cleansing):
    conditions = (
        col("dv_representation").isin('LR', 'AIP')
    ) & (
        col("lu_appealType").isNotNull()
    )

    # Create DataFrame with CaseNo and list of CategoryId
    silver_c_grouped = silver_c.groupBy("CaseNo").agg(
        collect_list(col("CategoryId")).alias("CategoryIdList"),
        first("CategoryId", ignorenulls=True).alias("CategoryId")
    )

    # Join bronze_HORef_cleansing to get CleansedHORef using CaseNo and coalesce(HORef, FCONumber)
    bronze_cleansing = bronze_HORef_cleansing.select(
        col("CaseNo"),
        coalesce(col("HORef"), col("FCONumber")).alias("lu_HORef")
    )

    # homeOfficeDecisionDate
    home_office_decision_date_expr = when(
        expr("array_contains(CategoryIdList, 37)") & col("DateOfApplicationDecision").isNotNull(),
        date_format(col("DateOfApplicationDecision"), "yyyy-MM-dd")
    )
    # IF CategoryId IN [38] AND  IF CleansedHORef & M1.HORef & M2.FCONumber NOT LIKE '%GWF%' = Include; ELSE OMIT | ISO 8601 Standard
    # decisionLetterReceivedDate
    decision_letter_received_date_expr = when(
    (col("lu_HORef").like("%GWF%")) |
    (col("HORef").like("%GWF%")) |
    (col("FCONumber").like("%GWF%")),
    lit(None)
    ).when(
        (expr("array_contains(CategoryIdList, 38)")) &
        (col("DateOfApplicationDecision").isNotNull()),
        date_format(col("DateOfApplicationDecision"), "yyyy-MM-dd")
    ).otherwise(None)



    # IF CategoryId IN [38] AND  IF lu_HORef & M1.HORef & M2.FCONumber LIKE '%GWF%' = Include;; ELSE OMIT | ISO 8601 Standard
    # dateEntryClearanceDecision
    date_entry_clearance_decision_expr = when(
        (expr("array_contains(CategoryIdList, 38)")) &
       (
           (col("lu_HORef").like("%GWF%")) |
            (col("HORef").like("%GWF%")) |
            (col("FCONumber").like("%GWF%")) 
        ) &
        (col("DateOfApplicationDecision").isNotNull()),
        date_format(col("DateOfApplicationDecision"), "yyyy-MM-dd")
    ).otherwise(None)

    
    # homeOfficeReferenceNumber
    # IF CategoryId IN [38] AND IF CleansedHORef OR M1.HORef OR M2.FCONumber LIKE '%GWF%' = OMIT; ELSE Include
    # IF CleansedHORef IS NULL USE HORef; IF HORef IS NULL USE FCONumber
    # homeOfficeReferenceNumber logic
    home_office_reference_number_expr = when(
        expr("array_contains(CategoryIdList, 38)") &
        (
            col("lu_HORef").like("%GWF%") |
            col("HORef").like("%GWF%") |
            col("FCONumber").like("%GWF%")
        ),
        lit(None)
    ).when(
        expr("array_contains(CategoryIdList, 38)"),
        coalesce(
            cleanReferenceNumberUDF(col("lu_HORef")),
            cleanReferenceNumberUDF(col("HORef")),
            cleanReferenceNumberUDF(col("FCONumber"))
        )
    ).otherwise(lit(None))

    # IF CategoryId IN [38] AND  IF CleansedHORef OR M1.HORef OR M2.FCONumber LIKE '%GWF%' = Include; ELSE OMIT
    # IF CleansedHORef IS NULL USE HORef; IF HORef IS NULL USE FCONumber
    # gwfReferenceNumber logic
    gwf_reference_number_expr = when(
        expr("array_contains(CategoryIdList, 38)") 
        & ( col("lu_HORef").like("%GWF%") |
            col("HORef").like("%GWF%") |
            col("FCONumber").like("%GWF%")),
        coalesce(
            cleanReferenceNumberUDF(col("lu_HORef")),
            cleanReferenceNumberUDF(col("HORef")),
            cleanReferenceNumberUDF(col("FCONumber"))
        )
    ).otherwise(
        lit(None)
    )

    # IF CategoryId IN [38] AND  IF CleansedHORef & M1.HORef & M2.FCONumber LIKE '%GWF%' = Include; ELSE OMIT
    # IF CleansedHORef IS NULL USE HORef; IF HORef IS NULL USE FCONumber

    # gwfReferenceNumber logic
    gwf_reference_number_expr = when(
        expr("array_contains(CategoryIdList, 38)") 
        & ( col("lu_HORef").like("%GWF%") |
            col("HORef").like("%GWF%") |
            col("FCONumber").like("%GWF%")),
        coalesce(
            cleanReferenceNumberUDF(col("lu_HORef")),
            cleanReferenceNumberUDF(col("HORef")),
            cleanReferenceNumberUDF(col("FCONumber"))
        )
    ).otherwise(
        lit(None)
    )

    silver_m2 = silver_m2.filter(col("Relationship").isNull())

    df = (
        silver_m1
        .join(silver_c_grouped, ["CaseNo"], "left")
        .join(silver_m2, ["CaseNo"], "left")
        .join(bronze_cleansing, ["CaseNo"], "left")
        .filter(conditions)
        .select(
            col("CaseNo"),
            # col("CategoryIdList"),
            home_office_decision_date_expr.alias("homeOfficeDecisionDate"),
            decision_letter_received_date_expr.alias("decisionLetterReceivedDate"),
            date_entry_clearance_decision_expr.alias("dateEntryClearanceDecision"),
            home_office_reference_number_expr.alias("homeOfficeReferenceNumber"),
            gwf_reference_number_expr.alias("gwfReferenceNumber"),
            lit("Yes").alias("isHomeOfficeIntegrationEnabled"),
            lit("Yes").alias("homeOfficeNotificationsEligible")
            # col("HORef"),
            # col("FCONumber"),
            # col("lu_HORef")
        )
    )

    common_inputFields = [lit("lu_appealType"), lit("dv_representation")]
    common_inputValues = [col("audit.lu_appealType"), col("audit.dv_representation")]

    df_audit = (
        silver_m1.alias("audit")
        .join(silver_m2.alias("audit_m2"), ["CaseNo"], "left")
        .join(df.alias("content"), ["CaseNo"], "left")
        .join(silver_c_grouped.alias("audit_c"), ["CaseNo"], "left")
        .join(bronze_cleansing.alias("content_c"), ["CaseNo"], "left")
        .select(
            col("CaseNo"),
            array(struct(*common_inputFields, lit("audit_c.CategoryIdList"))).alias("homeOfficeDecisionDate_inputFields"),
            array(struct(*common_inputValues, col("audit_c.CategoryIdList"))).alias("homeOfficeDecisionDate_inputValues"),
            col("content.homeOfficeDecisionDate"),
            lit("yes").alias("homeOfficeDecisionDate_Transformed"),

            array(struct(*common_inputFields, lit("audit_c.CategoryIdList"),  lit("content_c.lu_HORef"), lit("audit.HORef"), lit("audit_m2.FCONumber"))).alias("decisionLetterReceivedDate_inputFields"),
            array(struct(*common_inputValues, col("audit_c.CategoryIdList"), col("content_c.lu_HORef"), col("audit.HORef"), col("audit_m2.FCONumber"))).alias("decisionLetterReceivedDate_inputValues"),
            col("content.decisionLetterReceivedDate"),
            lit("yes").alias("decisionLetterReceivedDate_Transformed"),

            array(struct(*common_inputFields, lit("audit_c.CategoryIdList"), lit("content_c.lu_HORef"), lit("audit.HORef"), lit("audit_m2.FCONumber"))).alias("dateEntryClearanceDecision_inputFields"),
            array(struct(*common_inputValues, col("audit_c.CategoryIdList"), col("content_c.lu_HORef"), col("audit.HORef"), col("audit_m2.FCONumber"))).alias("dateEntryClearanceDecision_inputValues"),
            col("content.dateEntryClearanceDecision"),
            lit("yes").alias("dateEntryClearanceDecision_Transformed"),

            array(struct(*common_inputFields, lit("audit_c.CategoryIdList"), lit("content_c.lu_HORef"), lit("audit.HORef"), lit("audit_m2.FCONumber"))).alias("homeOfficeReferenceNumber_inputFields"),
            array(struct(*common_inputValues, col("audit_c.CategoryIdList"), col("content_c.lu_HORef"), col("audit.HORef"), col("audit_m2.FCONumber"))).alias("homeOfficeReferenceNumber_inputValues"),
            col("content.homeOfficeReferenceNumber"),
            lit("yes").alias("homeOfficeReferenceNumber_Transformed"),

            array(struct(*common_inputFields, lit("audit_c.CategoryIdList"), lit("content_c.lu_HORef"), lit("audit.HORef"), lit("audit_m2.FCONumber"))).alias("gwfReferenceNumber_inputFields"),
            array(struct(*common_inputValues, col("audit_c.CategoryIdList"), col("content_c.lu_HORef"), col("audit.HORef"), col("audit_m2.FCONumber"))).alias("gwfReferenceNumber_inputValues"),
            col("content.gwfReferenceNumber"),
            lit("yes").alias("gwfReferenceNumber_Transformed"),
        
            #isHomeOfficeIntegrationEnabled - ARIADM-797
            array(struct(*common_inputFields, lit("isHomeOfficeIntegrationEnabled"))).alias("isHomeOfficeIntegrationEnabled_inputFields"),
            array(struct(*common_inputValues, lit("null"))).alias("isHomeOfficeIntegrationEnabled_inputValues"),
            col("content.isHomeOfficeIntegrationEnabled"),
            lit("no").alias("isHomeOfficeIntegrationEnabled_Transformation"),

            #homeOfficeNotificationsEligible - ARIADM-797
            array(struct(*common_inputFields, lit("homeOfficeNotificationsEligible"))).alias("homeOfficeNotificationsEligible_inputFields"),
            array(struct(*common_inputValues, lit("null"))).alias("homeOfficeNotificationsEligible_inputValues"),
            col("content.homeOfficeNotificationsEligible"),
            lit("no").alias("homeOfficeNotificationsEligible_Transformation")
        )
    )

    return df, df_audit


################################################################
##########         paymentType Function              ###########
################################################################


def paymentType(silver_m1):
    conditions_all = col("dv_CCDAppealType").isin(["EA", "EU", "HU", "PA"])

    payment_content = silver_m1.filter(conditions_all).select(
        col("CaseNo"),
        when(col("VisitVisatype") == 1, 8000)
            .when(col("VisitVisatype") == 2, 14000)
            .otherwise("unknown").alias("feeAmountGbp"),
        when(col("VisitVisatype") == 1, "Notice of Appeal - appellant consents without hearing A")
            .when(col("VisitVisatype") == 2, "Appeal determined with a hearing")
            .otherwise("unknown").alias("feeDescription"),
        when(col("VisitVisatype") == 1, None)
            .when(col("VisitVisatype") == 2, 140)
            .otherwise("unknown").alias("feeWithHearing"),
        when(col("VisitVisatype") == 1, 80)
            .when(col("VisitVisatype") == 2, None)
            .otherwise("unknown").alias("feeWithoutHearing"),
        when(col("VisitVisatype") == 1, "Appeal determined without a hearing")
            .when(col("VisitVisatype") == 2, "Appeal determined with a hearing")
            .otherwise("unknown").alias("paymentDescription"),
        lit("Yes").alias("feePaymentAppealType"),
        lit("Payment pending").alias("paymentStatus"),
        lit("2").alias("feeVersion"),
        when(col("VisitVisatype") == 1, "decisionWithoutHearing")
            .when(col("VisitVisatype") == 2, "decisionWithHearing")
            .otherwise("unknown").alias("decisionHearingFeeOption"),
        lit("No").alias("hasServiceRequestAlready")
    )

    payment_common_inputFields = [lit("VisitVisatype")]
    payment_common_inputValues = [col("VisitVisatype")]

    payment_audit = silver_m1.filter(conditions_all).join(payment_content, on="CaseNo").filter(conditions_all).select(
        col("CaseNo"),

        # feeAmountGbp
        array(struct(*payment_common_inputFields)).alias("feeAmountGbp_inputFields"),
        array(struct(*payment_common_inputValues)).alias("feeAmountGbp_inputValues"),
        col("feeAmountGbp"),
        lit("yes").alias("feeAmountGbp_Transformation"),

        # feeDescription
        array(struct(*payment_common_inputFields)).alias("feeDescription_inputFields"),
        array(struct(*payment_common_inputValues)).alias("feeDescription_inputValues"),
        col("feeDescription"),
        lit("yes").alias("feeDescription_Transformation"),

        # feeWithHearing
        array(struct(*payment_common_inputFields)).alias("feeWithHearing_inputFields"),
        array(struct(*payment_common_inputValues)).alias("feeWithHearing_inputValues"),
        col("feeWithHearing"),
        lit("yes").alias("feeWithHearing_Transformation"),

        # feeWithoutHearing
        array(struct(*payment_common_inputFields)).alias("feeWithoutHearing_inputFields"),
        array(struct(*payment_common_inputValues)).alias("feeWithoutHearing_inputValues"),
        col("feeWithoutHearing"),
        lit("yes").alias("feeWithoutHearing_Transformation"),

        # paymentDescription
        array(struct(*payment_common_inputFields)).alias("paymentDescription_inputFields"),
        array(struct(*payment_common_inputValues)).alias("paymentDescription_inputValues"),
        col("paymentDescription"),
        lit("yes").alias("paymentDescription_Transformation"),

        # feePaymentAppealType (literal, so empty arrays)
        array().cast("array<struct<dummy:string>>").alias("feePaymentAppealType_inputFields"),
        array().cast("array<struct<dummy:string>>").alias("feePaymentAppealType_inputValues"),
        col("feePaymentAppealType"),
        lit("yes").alias("feePaymentAppealType_Transformation"),

        # paymentStatus (literal, so empty arrays)
        array().cast("array<struct<dummy:string>>").alias("paymentStatus_inputFields"),
        array().cast("array<struct<dummy:string>>").alias("paymentStatus_inputValues"),
        col("paymentStatus"),
        lit("yes").alias("paymentStatus_Transformation"),

        # feeVersion (literal, so empty arrays)
        array().cast("array<struct<dummy:string>>").alias("feeVersion_inputFields"),
        array().cast("array<struct<dummy:string>>").alias("feeVersion_inputValues"),
        col("feeVersion"),
        lit("yes").alias("feeVersion_Transformation"),

        # decisionHearingFeeOption
        array(struct(*payment_common_inputFields)).alias("decisionHearingFeeOption_inputFields"),
        array(struct(*payment_common_inputValues)).alias("decisionHearingFeeOption_inputValues"),
        col("decisionHearingFeeOption"),
        lit("yes").alias("decisionHearingFeeOption_Transformation"),

        # hasServiceRequestAlready (literal, so empty arrays)
        array().cast("array<struct<dummy:string>>").alias("hasServiceRequestAlready_inputFields"),
        array().cast("array<struct<dummy:string>>").alias("hasServiceRequestAlready_inputValues"),
        col("hasServiceRequestAlready"),
        lit("yes").alias("hasServiceRequestAlready_Transformation")
    )

    return payment_content, payment_audit


################################################################
##########            partyID Function               ###########
################################################################


# Create UDF to generate UUID for relevant field as a string type
uuid4_udf = udf(lambda: str(uuid.uuid4()), StringType())

def partyID(silver_m1, silver_m3,silver_c):
    conditions_all = (col("lu_appealType").isNotNull())
    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_ranked = silver_m3.withColumn("row_num", row_number().over(window_spec))

    # Filter the top-ranked rows where Outcome is not null
    silver_m3_filtered = silver_m3_ranked.filter(
        (col("row_num") == 1) & (col("Outcome").isNotNull())
    ).select(col("CaseNo"))

    # Create DataFrame with CaseNo and list of CategoryId
    silver_c_grouped = silver_c.groupBy("CaseNo").agg(collect_list(col("categoryId")).alias("CategoryIdList"))

    df = silver_m1.alias("m1").join(silver_m3_filtered.alias("m3"), on="CaseNo", how="left").join(silver_c_grouped.alias("c"), ["CaseNo"], "left"
    ).withColumn(
        "appellantsRepresentation", when(((col("m1.dv_representation") == "LR") &  (col("lu_appealType").isNotNull())), "No").when(((col("m1.dv_representation") == "AIP") & (col("lu_appealType").isNotNull())), "Yes").otherwise(None)
    ).withColumn(
        "appellantPartyId",
        when(conditions_all, expr("uuid()")).otherwise(expr("uuid()"))  #use expr() to ensure a unique UUID per row
    ).withColumn(
        "legalRepIndividualPartyId",
        when((col("appellantsRepresentation") == 'No'), expr("uuid()")).otherwise(None) #If appelRep = LR (no) then valid
    ).withColumn(
        "legalRepOrganisationPartyId",
        when((col("appellantsRepresentation") == 'No'), expr("uuid()")).otherwise(None) #If appelRep = LR (no) then valid
    ).withColumn(
        "sponsorPartyId",
        when(conditions_all & col("m1.Sponsor_Name").isNotNull() & (array_contains(col("CategoryIdList"), 38)), expr("uuid()")).otherwise(None) 
    ).select(
        col("m1.CaseNo"),
        col("appellantPartyId"),
        col("legalRepIndividualPartyId"),
        col("legalRepOrganisationPartyId"),
        col("sponsorPartyId")
    ).distinct()

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("audit.dv_representation"), col("audit.lu_appealType")]

    df_audit = silver_m1.join(silver_c, ["CaseNo"], "left").alias("audit").join(df.alias("content"), ["CaseNo"],"left").select(
        col("CaseNo"),

        #Audit appellantPartyId - ARIADM-779
        array(struct(*common_inputFields, lit("content.appellantPartyId"))).alias("appellantPartyId_inputFields"),
        array(struct(*common_inputValues, col("content.appellantPartyId"))).alias("appellantPartyId_inputValues"),
        col("content.appellantPartyId"),
        lit("yes").alias("appellantPartyId_Transformation"),

        #Audit legalRepIndividualPartyId - ARIADM-779
        array(struct(*common_inputFields, lit("content.legalRepIndividualPartyId"))).alias("legalRepIndividualPartyId_inputFields"),
        array(struct(*common_inputValues, col("content.legalRepIndividualPartyId"))).alias("legalRepIndividualPartyId_inputValues"),
        col("content.legalRepIndividualPartyId"),
        lit("yes").alias("legalRepIndividualPartyId_Transformation"),

        #Audit legalRepOrganisationPartyId - ARIADM-779
        array(struct(*common_inputFields, lit("content.legalRepOrganisationPartyId"))).alias("legalRepOrganisationPartyId_inputFields"),
        array(struct(*common_inputValues, col("content.legalRepOrganisationPartyId"))).alias("legalRepOrganisationPartyId_inputValues"),
        col("content.legalRepOrganisationPartyId"),
        lit("yes").alias("legalRepOrganisationPartyId_Transformation"),

        #Audit sponsorPartyId - ARIADM-779
        array(struct(*common_inputFields, lit("content.sponsorPartyId"))).alias("sponsorPartyId_inputFields"),
        array(struct(*common_inputValues, col("content.sponsorPartyId"))).alias("sponsorPartyId_inputValues"),
        col("content.sponsorPartyId"),
        lit("yes").alias("sponsorPartyId_Transformation")
    ).distinct()

    return df, df_audit


################################################################
##########        remissionTypes Function            ###########
################################################################


def remissionTypes(silver_m1, bronze_remission_lookup_df, silver_m4):

    #Including conditions specified in mapping document (column H)
    conditions_remissionTypes = col("dv_CCDAppealType").isin("EA", "EU", "HU", "PA")
    conditions = (col("dv_representation").isin('LR', 'AIP')) & (col("lu_appealType").isNotNull())
    

    # No need for all logic in ticket. The remission table exists in bronze. Left join on the unique field. Rename "Omit" to null. Results are correct from the samples I can see below.
    df = silver_m1.alias("m1").filter(conditions_remissionTypes & conditions).join(bronze_remission_lookup_df, on=["PaymentRemissionReason","PaymentRemissionRequested"], how="left").join(silver_m4, on=["CaseNo"], how="left"
        ).withColumn(
        "remissionType",
        col("remissionType")
    ).withColumn(
        "remissionClaim",
        when(col("remissionClaim") == lit("OMIT"), None).otherwise(col("remissionClaim"))
    ).withColumn(
        "feeRemissionType",
        when(col("feeRemissionType") == lit("OMIT"), None).otherwise(col("feeRemissionType"))
    ).withColumn(
        "exceptionalCircumstances",
        when(col("exceptionalCircumstances") == lit("OMIT"), None).otherwise(col("feeRemissionType"))

    ).withColumn(
        "legalAidAccountNumber",
        when(col("legalAidAccountNumber") == lit("OMIT"), None                               #When set to OMIT, replace with NULL
        ).when(col("legalAidAccountNumber") == lit("M1.LSCReference; ELSE IF NULL 'Unknown'"),    #If record matches the string
        when(col("m1.LSCReference").isNotNull(), col("m1.LSCReference")).otherwise(lit("Unknown")) #Perform logic in the string
    ).otherwise(col("legalAidAccountNumber"))

    ).withColumn(
        "asylumSupportReference",
        when(col("asylumSupportReference") == lit("OMIT"), None                                  #When set to OMIT, replace with NULL
        ).when(col("asylumSupportReference") == lit("M1.ASFReferenceNo ELSE IF NULL 'Unknown'"),      #If record matches the string
        when(col("m1.ASFReferenceNo").isNotNull(), col("m1.ASFReferenceNo")).otherwise(lit("Unknown")) #Perform logic in the string
    ).otherwise(col("asylumSupportReference"))
        
    ).withColumn(
        "helpWithFeesReferenceNumber",
        when(col("helpWithFeesReferenceNumber") == lit("OMIT"), None                             #As above
        ).when(col("helpWithFeesReferenceNumber") == lit("M1.PaymentRemissionReasonNote; ELSE IF NULL 'Unknown'"),
        when(col("m1.PaymentRemissionReasonNote").isNotNull(), col("m1.PaymentRemissionReasonNote")).otherwise(lit("Unknown"))
        ).otherwise(col("helpWithFeesReferenceNumber"))

    ).select(
        col("CaseNo"),
        "remissionType",
        "remissionClaim",
        "feeRemissionType",
        "exceptionalCircumstances",
        "legalAidAccountNumber",
        "asylumSupportReference",
        "helpWithFeesReferenceNumber"
    ).distinct()

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("m1_audit.dv_representation"), col("m1_audit.lu_appealType")]


    df_audit = silver_m1.alias("m1_audit").filter(conditions_remissionTypes).join(df.alias("content"), on = ["CaseNo"], how = "left").select(
        col("CaseNo"),

        ## remissionType - ARIADM-784
        array(struct(*common_inputFields ,lit("content.remissionType"))).alias("remissionType_inputFields"),
        array(struct(*common_inputValues ,col("content.remissionType"))).alias("remissionType_inputValues"),
        col("content.remissionType"),
        lit("yes").alias("remissionType_Transformed"),

        ## remissionClaim - ARIADM-784
        array(struct(*common_inputFields ,lit("content.remissionClaim"))).alias("remissionClaim_inputFields"),
        array(struct(*common_inputValues ,col("content.remissionClaim"))).alias("remissionClaim_inputValues"),
        col("content.remissionClaim"),
        lit("yes").alias("remissionClaim_Transformed"),

        ## feeRemissionType - ARIADM-784
        array(struct(*common_inputFields ,lit("content.feeRemissionType"))).alias("feeRemissionType_inputFields"),
        array(struct(*common_inputValues ,col("content.feeRemissionType"))).alias("feeRemissionType_inputValues"),
        col("content.feeRemissionType"),
        lit("yes").alias("feeRemissionType_Transformed"),

        ## exceptionalCircumstances - ARIADM-786
        array(struct(*common_inputFields ,lit("content.exceptionalCircumstances"))).alias("exceptionalCircumstances_inputFields"),
        array(struct(*common_inputValues ,col("content.exceptionalCircumstances"))).alias("exceptionalCircumstances_inputValues"),
        col("content.exceptionalCircumstances"),
        lit("yes").alias("exceptionalCircumstances_Transformed"),

        ## legalAidAccountNumber - ARIADM-786
        array(struct(*common_inputFields ,lit("content.legalAidAccountNumber"), lit("LSCReference"))).alias("legalAidAccountNumber_inputFields"),
        array(struct(*common_inputValues ,col("content.legalAidAccountNumber"), col("LSCReference"))).alias("legalAidAccountNumber_inputValues"),
        col("content.legalAidAccountNumber"),
        lit("yes").alias("legalAidAccountNumber_Transformed"),

        ## asylumSupportReference - ARIADM-786
        array(struct(*common_inputFields ,lit("content.asylumSupportReference"), lit("ASFReferenceNo"))).alias("asylumSupportReference_inputFields"),
        array(struct(*common_inputValues ,col("content.asylumSupportReference"), col("ASFReferenceNo"))).alias("asylumSupportReference_inputValues"),
        col("content.asylumSupportReference"),
        lit("yes").alias("asylumSupportReference_Transformed"),

        ## helpWithFeesReferenceNumber - ARIADM-786
        array(struct(*common_inputFields ,lit("content.helpWithFeesReferenceNumber"), lit("PaymentRemissionReasonNote"))).alias("helpWithFeesReferenceNumber_inputFields"),
        array(struct(*common_inputValues ,col("content.helpWithFeesReferenceNumber"), col("PaymentRemissionReasonNote"))).alias("helpWithFeesReferenceNumber_inputValues"),
        col("content.helpWithFeesReferenceNumber"),
        lit("yes").alias("helpWithFeesReferenceNumber_Transformed")
    ).distinct()

    return df, df_audit
    
################################################################
##########        sponsorDetails Function            ###########
################################################################

def sponsorDetails(silver_m1, silver_c):
    m1 = silver_m1.alias("m1")
    c = silver_c.alias("c")

    joined = m1.join(c, on='CaseNo', how="left")

    grouped = joined.groupBy("CaseNo").agg(
        collect_list("CategoryId").alias("CategoryIdList"),
        first("Sponsor_Name", ignorenulls=True).alias("Sponsor_Name"),
        first("Sponsor_Forenames", ignorenulls=True).alias("Sponsor_Forenames"),
        first("Sponsor_Address1", ignorenulls=True).alias("Sponsor_Address1"),
        first("Sponsor_Address2", ignorenulls=True).alias("Sponsor_Address2"),
        first("Sponsor_Address3", ignorenulls=True).alias("Sponsor_Address3"),
        first("Sponsor_Address4", ignorenulls=True).alias("Sponsor_Address4"),
        first("Sponsor_Address5", ignorenulls=True).alias("Sponsor_Address5"),
        first("Sponsor_Postcode", ignorenulls=True).alias("Sponsor_Postcode"),
        first("Sponsor_Authorisation", ignorenulls=True).alias("Sponsor_Authorisation"),
        first("Sponsor_Email", ignorenulls=True).alias("Sponsor_Email"),
        first("Sponsor_Telephone", ignorenulls=True).alias("Sponsor_Telephone")
    )

    sponsor_condition = (array_contains(col("CategoryIdList"), 38) & col("Sponsor_Name").isNotNull())

    grouped = grouped.withColumn(
    "hasSponsor",
        when(sponsor_condition, lit("Yes")).otherwise(lit("No"))
    ).withColumn(
        "sponsorGivenNames",
        when((sponsor_condition), col("Sponsor_Forenames")).otherwise(lit(None))
    ).withColumn(
        "sponsorFamilyName",
        when((sponsor_condition), col("Sponsor_Name")).otherwise(lit(None))
    ).withColumn(
        "sponsorAuthorisation",
        when((sponsor_condition) & col("Sponsor_Authorisation") == lit("true"), lit("Yes"))
        .when((sponsor_condition) & col("Sponsor_Authorisation") != lit("true"), lit("No")).otherwise(lit(None))
    ).withColumn(
        "sponsorAddress",
        when(
            (sponsor_condition),
            trim(
                concat_ws(
                    ", ",
                    col("Sponsor_Address1"),
                    col("Sponsor_Address2"),
                    col("Sponsor_Address3"),
                    col("Sponsor_Address4"),
                    col("Sponsor_Address5"),
                    col("Sponsor_Postcode")
                )
            )
        )
    ).withColumn(
        "sponsorEmailAdminJ",
        when((sponsor_condition),
             cleanEmailUDF(col("Sponsor_Email")))
    ).withColumn(
        "sponsorMobileNumberAdminJ",
        when((sponsor_condition),
             phoneNumberUDF(col("Sponsor_Telephone")))
    )

    df = grouped.select(
        "CaseNo",
        "hasSponsor",
        "sponsorGivenNames",
        "sponsorFamilyName",
        "sponsorAddress",
        "sponsorEmailAdminJ",
        "sponsorMobileNumberAdminJ",
        "sponsorAuthorisation",
    )
    # .where((sponsor_condition) & (col("hasSponsor") == lit("Yes")))

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("audit.dv_representation"), col("audit.lu_appealType")]

    df_audit = silver_m1.alias("audit").join(df.alias("content"), ["CaseNo"], "left"
            ).join(grouped.alias("grp"), ["CaseNo"], "left").select(
        col("CaseNo"),
        
        #audit hasSponsor
        array(struct(*common_inputFields, lit("audit.Sponsor_Name"), lit("grp.CategoryIdList"))).alias("hasSponsor_inputFields"),
        array(struct(*common_inputValues, col("audit.Sponsor_Name"), col("grp.CategoryIdList"))).alias("hasSponsor_inputValues"),
        col("content.hasSponsor"),
        lit("yes").alias("hasSponsor_Transformation"),

        #audit sponsorGivenNames
        array(struct(*common_inputFields, lit("audit.Sponsor_Forenames"), lit("grp.CategoryIdList"))).alias("sponsorGivenName_inputFields"),
        array(struct(*common_inputValues, col("audit.Sponsor_Forenames"), col("grp.CategoryIdList"))).alias("sponsorGivenName_inputValues"),
        col("content.sponsorGivenNames"),
        lit("yes").alias("sponsorGivenNames_Transformation"),

        #audit sponsorFamilyName
        array(struct(*common_inputFields, lit("audit.Sponsor_Name"), lit("grp.CategoryIdList"))).alias("sponsorFamilyName_inputFields"),
        array(struct(*common_inputValues, col("audit.Sponsor_Name"), col("grp.CategoryIdList"))).alias("sponsorFamilyName_inputValues"),
        col("content.sponsorFamilyName"),
        lit("yes").alias("sponsorFamilyName_Transformation"),

        #audit sponsorAddress
        array(struct(*common_inputFields, lit("audit.Sponsor_Address1"), lit("audit.Sponsor_Address2"), lit("audit.Sponsor_Address3"), lit("audit.Sponsor_Address4"), lit("audit.Sponsor_Address5"), lit("audit.Sponsor_Postcode"), lit("grp.CategoryIdList"))).alias("sponsorAddress.inputFields"),
        array(struct(*common_inputValues, col("audit.Sponsor_Address1"), col("audit.Sponsor_Address2"), col("audit.Sponsor_Address3"), col("audit.Sponsor_Address4"), col("audit.Sponsor_Address5"), col("audit.Sponsor_Postcode"), col("grp.CategoryIdList"))).alias("sponsorAddress.inputValues"),
        col("content.sponsorAddress"),
        lit("yes").alias("sponsorAddress_Transformation"),

        #audit sponsorAuthorisation
        array(struct(*common_inputFields, lit("audit.Sponsor_Authorisation"), lit("grp.CategoryIdList"))).alias("sponsorAuthorisation_inputFields"),
        array(struct(*common_inputValues, col("audit.Sponsor_Authorisation"), col("grp.CategoryIdList"))).alias("sponsorAuthorisation_inputValues"),
        col("content.sponsorAuthorisation"),
        lit("yes").alias("sponsorAuthorisation_Transformation"),

        #audit sponsorEmailAdminJ
        array(struct(*common_inputFields, lit("audit.Sponsor_Email"), lit("grp.CategoryIdList"))).alias("sponsorEmailAdminJ_inputFields"),
        array(struct(*common_inputValues, col("audit.Sponsor_Email"), col("grp.CategoryIdList"))).alias("sponsorEmailAdminJ_inputValues"),
        col("content.sponsorEmailAdminJ"),
        lit("yes").alias("sponsorEmailAdminJ_Transformation"),

        #audit sponsorMobileNumberAdminJ
        array(struct(*common_inputFields, lit("audit.Sponsor_Telephone"), lit("grp.CategoryIdList"))).alias("sponsorMobileNumberAdminJ_inputFields"),
        array(struct(*common_inputValues, col("audit.Sponsor_Telephone"), col("grp.CategoryIdList"))).alias("sponsorMobileNumberAdminJ_inputValues"),
        col("content.sponsorMobileNumberAdminJ"),
        lit("yes").alias("sponsorMobileNumberAdminJ_Transformation")
    )

    return df, df_audit

################################################################
##########             General Function              ###########
################################################################

def general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres):
    conditions_general = (col("dv_representation").isin('LR','AIP')) & (col("lu_appealType").isNotNull())

        # Define the window specification
    window_spec = Window.partitionBy("CaseNo").orderBy(col("HistoryId").desc())

    # Define the base DataFrame
    silver_h = silver_h.filter(col("HistType") == 6)

    # Define the common filter condition
    common_filter = ~col("Comment").like("%Castle Park Storage%") & ~col("Comment").like("%Field House%") & ~col("Comment").like("%UT (IAC)%")

    # Define the function to get the latest history record based on the filter
    def get_latest_history(df, filter_condition):
        return df.filter(filter_condition).withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).select(
            col("CaseNo"),
            col("Comment"),
            split(col("Comment"), ',')[0].alias("der_prevFileLocation")
        )

    # Create DataFrames for each location
    derived_history_df = get_latest_history(silver_h, common_filter)

    # Define common columns for hearingCentreDynamicList and caseManagementLocationRefData
    common_columns = {
        "hearingCentreDynamicList": struct(
            struct(
                col("locationCode").alias("code"),
                col("locationLabel").alias("label")
            ).alias("value"),
            array(
                struct(lit("227101").alias("code"), lit("Newport Tribunal Centre - Columbus House").alias("label")),
                struct(lit("231596").alias("code"), lit("Birmingham Civil And Family Justice Centre").alias("label")),
                struct(lit("28837").alias("code"), lit("Harmondsworth Tribunal Hearing Centre").alias("label")),
                struct(lit("366559").alias("code"), lit("Atlantic Quay - Glasgow").alias("label")),
                struct(lit("366796").alias("code"), lit("Newcastle Civil And Family Courts And Tribunals Centre").alias("label")),
                struct(lit("386417").alias("code"), lit("Hatton Cross Tribunal Hearing Centre").alias("label")),
                struct(lit("512401").alias("code"), lit("Manchester Tribunal Hearing Centre - Piccadilly Exchange").alias("label")),
                struct(lit("649000").alias("code"), lit("Yarls Wood Immigration And Asylum Hearing Centre").alias("label")),
                struct(lit("698118").alias("code"), lit("Bradford Tribunal Hearing Centre").alias("label")),
                struct(lit("765324").alias("code"), lit("Taylor House Tribunal Hearing Centre").alias("label"))
            ).alias("list_items")
        ),
        "caseManagementLocationRefData": struct(
            lit("1").alias("region"),
            struct(
                struct(
                    col("locationCode").alias("code"),
                    col("locationLabel").alias("label")
                ).alias("value"),
                array(
                    struct(lit("227101").alias("code"), lit("Newport Tribunal Centre - Columbus House").alias("label")),
                    struct(lit("231596").alias("code"), lit("Birmingham Civil And Family Justice Centre").alias("label")),
                    struct(lit("28837").alias("code"), lit("Harmondsworth Tribunal Hearing Centre").alias("label")),
                    struct(lit("366559").alias("code"), lit("Atlantic Quay - Glasgow").alias("label")),
                    struct(lit("366796").alias("code"), lit("Newcastle Civil And Family Courts And Tribunals Centre").alias("label")),
                    struct(lit("386417").alias("code"), lit("Hatton Cross Tribunal Hearing Centre").alias("label")),
                    struct(lit("512401").alias("code"), lit("Manchester Tribunal Hearing Centre - Piccadilly Exchange").alias("label")),
                    struct(lit("649000").alias("code"), lit("Yarls Wood Immigration And Asylum Hearing Centre").alias("label")),
                    struct(lit("698118").alias("code"), lit("Bradford Tribunal Hearing Centre").alias("label")),
                    struct(lit("765324").alias("code"), lit("Taylor House Tribunal Hearing Centre").alias("label"))
                ).alias("list_items")
            ).alias("baseLocation")
        )
    }

    # Join result_df with bronze_derive_hearing_centres to derive derive_hearing_centres
    bronze_derive_hearing_centres = bronze_derive_hearing_centres.withColumn(
        "hearingCentreDynamicList",
        when(
            col("locationCode").isNotNull() & col("locationLabel").isNotNull(),
            common_columns["hearingCentreDynamicList"]
        ).otherwise(None)
    ).withColumn(
        "caseManagementLocationRefData",
        when(
            col("locationCode").isNotNull() & col("locationLabel").isNotNull(),
            common_columns["caseManagementLocationRefData"]
        ).otherwise(None)
    )

    # Join result_df with bronze_hearing_centres to derive hearingCentre
    bronze_hearing_centres = bronze_hearing_centres.withColumn(
        "hearingCentreDynamicList",
        when(
            col("locationCode").isNotNull() & col("locationLabel").isNotNull(),
            common_columns["hearingCentreDynamicList"]
        ).otherwise(None)
    ).withColumn(
        "caseManagementLocationRefData",
        when(
            col("locationCode").isNotNull() & col("locationLabel").isNotNull(),
            common_columns["caseManagementLocationRefData"]
        ).otherwise(None)
    )

    silver_m2 = silver_m2.filter(col("Relationship").isNull())

    # Define postcode mappings
    postcode_mappings = {
        "bradford": ["BD", "DN", "HD", "HG", "HU", "HX", "LS", "S", "WF", "YO"],
        "manchester": ["BB", "BL", "CH", "CW", "FY", "LL", "ST", "L", "LA", "M", "OL", "PR", "SK", "WA", "WN"],
        "newport": ["BA", "BS", "CF", "DT", "EX", "HR", "LD", "NP", "PL", "SA", "SN", "SP", "TA", "TQ", "TR"],
        "taylorHouse": ["AL", "BN", "BR", "CB", "CM", "CO", "CR", "CT", "DA", "E", "EC", "EN", "IG", "IP", "ME", "N", "NR", "NW", "RH", "RM", "SE", "SG", "SS", "TN", "W", "WC"],
        "newcastle": ["CA", "DH", "DL", "NE", "SR", "TS"],
        "birmingham": ["B", "CV", "DE", "DY", "GL", "HP", "LE", "LN", "LU", "MK", "NG", "NN", "OX", "PE", "RG", "SY", "TF", "WD", "WR", "WS", "WV"],
        "hattonCross": ["BH", "GU", "HA", "KT", "PO", "SL", "SM", "SO", "SW", "TW", "UB"],
        "glasgow": ["AB", "DD", "DG", "EH", "FK", "G", "HS", "IV", "KA", "KW", "KY", "ML", "PA", "PH", "TD", "ZE", "BT"]
    }

    # Function to map postcodes to hearing centres
    def map_postcode_to_hearing_centre(postcode):
        if postcode is None:
            return None
        postcode = postcode.replace(" ", "").upper()
        first2 = postcode[:2]
        first1 = postcode[:1]
        for centre, codes in postcode_mappings.items():
            # Try 2-char match first
            if any(first2 == code for code in codes if len(code) == 2):
                return centre
        for centre, codes in postcode_mappings.items():
            # Then try 1-char match
            if any(first1 == code for code in codes if len(code) == 1):
                return centre
        return None

    # Register the UDF
    map_postcode_to_hearing_centre_udf = udf(map_postcode_to_hearing_centre, StringType())

    ##################################################################################################

    result_with_hearing_centre_df = silver_m1.alias("m1").join(
            bronze_hearing_centres.alias("bhc"),
            col("m1.CentreId") == col("bhc.CentreId"),
            how="left").join(derived_history_df.alias("h"),
                                ((col("m1.CaseNo") == col("h.CaseNo")) &
                                (col("m1.CentreId").isin(77,476,101,55,296,13,79,522,406,517,37))),
                                how="left") \
                                .join(bronze_hearing_centres.alias("bhc2"),
                                (col("h.der_prevFileLocation") == col("bhc2.prevFileLocation")),
                                 how="left").join(silver_m2.alias("m2"), col("m1.CaseNo") == col("m2.CaseNo"), how="left") \
                                .withColumn("map_postcode_to_hearing_centre", 
                                           when((col("h.der_prevFileLocation").isin("Arnhem House","Arnhem House (Exceptions)","Loughborough","North Shields (Kings Court)","Not known at this time") | col("h.der_prevFileLocation").isNull()),
                                           coalesce(map_postcode_to_hearing_centre_udf(coalesce(col('m1.Rep_Postcode'),col('m1.CaseRep_Postcode'),('m2.Appellant_Postcode')))
                                           ,lit("newport"))).otherwise(lit("newport"))) \
                                .join(bronze_derive_hearing_centres.alias("bhc3"), col("map_postcode_to_hearing_centre") == col("bhc3.hearingCentre"), how="left") \
                                .withColumn("isServiceRequestTabVisibleConsideringRemissions",
        when(
            (col("PaymentRemissionRequested").isNull()) | (col("PaymentRemissionRequested") == lit('2')),
            "Yes"
        ).otherwise("No")
        ).select(
        col("m1.CaseNo"),
        # "isServiceRequestTabVisibleConsideringRemissions",
        col('bhc.Conditions').alias("lu_conditions"),
        col('bhc.hearingCentre').alias("dv_dhc_hearingCentre"),
        col("h.der_prevFileLocation").alias("dv_prevFileLocation"),
        col('m1.Rep_Postcode'),
        col('m1.CaseRep_Postcode'),
        col("m2.Appellant_Postcode"),
        col("m1.CentreId"),
        col("bhc.prevFileLocation").alias("dv_dhc_prevFileLocation"),
        col("bhc2.hearingCentre").alias("dv_dhc2_hearingCentre"),

        col("bhc2.applicationChangeDesignatedHearingCentre").alias("dv_dhc2_applicationChangeDesignatedHearingCentre"),
        col("bhc3.applicationChangeDesignatedHearingCentre").alias("dv_dhc3_applicationChangeDesignatedHearingCentre"),

        when(((col('bhc.Conditions').isNull()) | (col('bhc.Conditions') == 'NO MAPPING REQUIRED')), col('bhc.applicationChangeDesignatedHearingCentre'))
        .when((col("h.der_prevFileLocation").isin("Arnhem House","Arnhem House (Exceptions)","Loughborough","North Shields (Kings Court)","Not known at this time")| col("h.der_prevFileLocation").isNull()),col("bhc3.applicationChangeDesignatedHearingCentre"))
        .when(col("h.der_prevFileLocation").isin('Castle Park Storage','Field House', 'Field House (TH)','UT (IAC) Cardiff CJC','UT (IAC) Hearing in Field House','UT (IAC) Hearing in Man CJC'), lit(None)).otherwise(col("bhc2.applicationChangeDesignatedHearingCentre"))
         .alias("applicationChangeDesignatedHearingCentre")
        )
        
    df = silver_m1.join(result_with_hearing_centre_df, ["CaseNo"], "left").filter(conditions_general).withColumn(
        "isServiceRequestTabVisibleConsideringRemissions",
        when(
            (col("PaymentRemissionRequested").isNull()) | (col("PaymentRemissionRequested") == lit('2')),
            "Yes"
        ).otherwise("No")
        ).select(
            col("CaseNo"),
            "isServiceRequestTabVisibleConsideringRemissions",
            "applicationChangeDesignatedHearingCentre"
        )

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("m1_audit.dv_representation"), col("m1_audit.lu_appealType")]

    df_audit = (
        silver_m1.alias("m1_audit")
        .join(df.alias("content"), ["CaseNo"], "left"
        ).join(result_with_hearing_centre_df.alias("hearing"), ["CaseNo"], "left")
        .select(
            col("CaseNo"),
            # isServiceRequestTabVisibleConsideringRemissions - ARIADM-767
            array(
                struct(
                    *common_inputFields,
                    lit("m1_audit.PaymentRemissionRequested"),
                    lit("content.isServiceRequestTabVisibleConsideringRemissions")
                )
            ).alias("isServiceRequestTabVisibleConsideringRemissions_inputFields"),
            array(
                struct(
                    *common_inputValues,
                    col("m1_audit.PaymentRemissionRequested"),
                    col("content.isServiceRequestTabVisibleConsideringRemissions")
                )
            ).alias("isServiceRequestTabVisibleConsideringRemissions_inputValues"),
            col("content.isServiceRequestTabVisibleConsideringRemissions"),
            lit("yes").alias("isServiceRequestTabVisibleConsideringRemissions_Transformed"),
            # Audit staffLocation
            array(
                struct(
                    *common_inputFields,
                    lit("lu_applicationChangeDesignatedHearingCentre"),
                    lit("lu_conditions"),
                    lit("dv_prevFileLocation"),
                    lit("Rep_Postcode"),
                    lit("CaseRep_Postcode"),
                    lit("Appellant_Postcode"),
                    lit("CentreId"),
                    lit("dv_dhc2_applicationChangeDesignatedHearingCentre"),
                    lit("dv_dhc3_applicationChangeDesignatedHearingCentre")
                )
            ).alias("applicationChangeDesignatedHearingCentre_inputFields"),
            array(
                struct(
                    *common_inputValues,
                    col("m1_audit.lu_applicationChangeDesignatedHearingCentre"),
                    col("hearing.lu_conditions"),
                    col("hearing.dv_prevFileLocation"),
                    col("hearing.Rep_Postcode"),
                    col("hearing.CaseRep_Postcode"),
                    col("hearing.Appellant_Postcode"),
                    col("hearing.CentreId"),
                    col("hearing.dv_dhc2_applicationChangeDesignatedHearingCentre"),
                    lit("hearing.dv_dhc3_applicationChangeDesignatedHearingCentre")
                )
            ).alias("applicationChangeDesignatedHearingCentre_inputValues"),
            col("content.applicationChangeDesignatedHearingCentre"),
            lit("yes").alias("applicationChangeDesignatedHearingCentre_Transformation"),
        )
    )

    return df, df_audit

################################################################
##########         General Default Function          ###########
################################################################

def generalDefault(silver_m1):

    conditions_generalDefault = (col("dv_representation").isin('LR','AIP')) & (col("lu_appealType").isNotNull())

    df = silver_m1.filter(conditions_generalDefault
                ).withColumn("notificationsSent", lit([]).cast("array<string>")
                ).withColumn("submitNotificationStatus", lit("")
                ).withColumn("isFeePaymentEnabled", lit("Yes")
                ).withColumn("isRemissionsEnabled", lit("Yes")
                ).withColumn("isOutOfCountryEnabled", lit("Yes")
                ).withColumn("isIntegrated", lit("No")
                ).withColumn("isNabaEnabled", lit("No")
                ).withColumn("isNabaAdaEnabled", lit("Yes")
                ).withColumn("isNabaEnabledOoc", lit("No")
                ).withColumn("isCaseUsingLocationRefData", lit("Yes")
                ).withColumn("hasAddedLegalRepDetails", lit("Yes")
                ).withColumn("autoHearingRequestEnabled", lit("No")
                ).withColumn("isDlrmFeeRemissionEnabled", lit("Yes")
                ).withColumn("isDlrmFeeRefundEnabled", lit("Yes")
                ).withColumn("sendDirectionActionAvailable", lit("Yes")
                ).withColumn("changeDirectionDueDateActionAvailable", lit("No")
                ).withColumn("markEvidenceAsReviewedActionAvailable", lit("No")
                ).withColumn("uploadAddendumEvidenceActionAvailable", lit("No")
                ).withColumn("uploadAdditionalEvidenceActionAvailable", lit("No")
                ).withColumn("displayMarkAsPaidEventForPartialRemission", lit("No")
                ).withColumn("haveHearingAttendeesAndDurationBeenRecorded", lit("No")
                ).withColumn("markAddendumEvidenceAsReviewedActionAvailable", lit("No")
                ).withColumn("uploadAddendumEvidenceLegalRepActionAvailable", lit("No")
                ).withColumn("uploadAddendumEvidenceHomeOfficeActionAvailable", lit("No")
                ).withColumn("uploadAddendumEvidenceAdminOfficerActionAvailable", lit("No")
                ).withColumn("uploadAdditionalEvidenceHomeOfficeActionAvailable", lit("No")                
    ).select(
        col("CaseNo"),
        # lit("").alias("ccdReferenceNumberForDisplay"), Done as part of AppealType 
        # lit("No").alias("hasOtherAppeals"), Done as part of payment pending
        # array(lit("hasDeclared")).alias("adminDeclaration1"), Done as part of caseData
        # lit("No").alias("s94bStatus"), Done as part of payment pending
        # lit("Yes").alias("isAdmin"), Done as part of payment pending
        # lit("No").alias("isEjp"), Done as part of payment pending
        # "isHomeOfficeIntegrationEnabled", Done as part of home office
        # "homeOfficeNotificationsEligible", done as part of home office
        # "ariaDesiredState", done as part of case data
        # "ariaMigrationTaskDueDays", done as part of case data
        "notificationsSent",
        "submitNotificationStatus",
        "isFeePaymentEnabled",
        "isRemissionsEnabled",
        "isOutOfCountryEnabled",
        "isIntegrated",
        "isNabaEnabled",
        "isNabaAdaEnabled",
        "isNabaEnabledOoc",
        "isCaseUsingLocationRefData",
        "hasAddedLegalRepDetails",
        "autoHearingRequestEnabled",
        "isDlrmFeeRemissionEnabled",
        "isDlrmFeeRefundEnabled",
        "sendDirectionActionAvailable",
        "changeDirectionDueDateActionAvailable",
        "markEvidenceAsReviewedActionAvailable",
        "uploadAddendumEvidenceActionAvailable",
        "uploadAdditionalEvidenceActionAvailable",
        "displayMarkAsPaidEventForPartialRemission",
        "haveHearingAttendeesAndDurationBeenRecorded",
        "markAddendumEvidenceAsReviewedActionAvailable",
        "uploadAddendumEvidenceLegalRepActionAvailable",
        "uploadAddendumEvidenceHomeOfficeActionAvailable",
        "uploadAddendumEvidenceAdminOfficerActionAvailable",
        "uploadAdditionalEvidenceHomeOfficeActionAvailable"
    )

    return df

################################################################
##########              Documents Function           ###########
################################################################

def documents(silver_m1):

    documents_content = silver_m1.select(
        "CaseNo",
        lit([]).cast("array<string>").alias("uploadTheAppealFormDocs"),
        lit([]).cast("array<string>").alias("caseNotes"),
        lit([]).cast("array<string>").alias("tribunalDocuments"),
        lit([]).cast("array<string>").alias("legalRepresentativeDocuments")
    )

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("audit.dv_representation"), col("audit.lu_appealType")]

    documents_audit = silver_m1.alias("audit").join(documents_content.alias("content"), on = ["CaseNo"], how = "left").select(
        col("CaseNo"),

        #-----# uploadTheAppealFormDocs #-----# 
        array(struct(*common_inputFields, lit("uploadTheAppealFormDocs"))).alias("uploadTheAppealFormDocs_inputFields"),
        array(struct(*common_inputValues, lit("null"))).alias("uploadTheAppealFormDocs_inputValues"),
        col("uploadTheAppealFormDocs"),
        lit("no").alias("uploadTheAppealFormDocs_Transformation"),

        #-----# caseNotes #-----# 
        array(struct(*common_inputFields, lit("caseNotes"))).alias("caseNotes_inputFields"),
        array(struct(*common_inputValues, lit("null"))).alias("caseNotes_inputValues"),
        col("caseNotes"),
        lit("no").alias("caseNotes_Transformation"),

        #-----# tribunalDocuments #-----# 
        array(struct(*common_inputFields, lit("tribunalDocuments"))).alias("tribunalDocuments_inputFields"),
        array(struct(*common_inputValues, lit("null"))).alias("tribunalDocuments_inputValues"),
        col("tribunalDocuments"),
        lit("no").alias("tribunalDocuments_Transformation"),

        #-----# legalRepresentativeDocuments #-----# 
        array(struct(*common_inputFields, lit("legalRepresentativeDocuments"))).alias("legalRepresentativeDocuments_inputFields"),
        array(struct(*common_inputValues, lit("null"))).alias("legalRepresentativeDocuments_inputValues"),
        col("legalRepresentativeDocuments"),
        lit("no").alias("legalRepresentativeDocuments_Transformation")
    )

    return documents_content, documents_audit

################################################################
##########             Case State Function           ###########
################################################################

def caseState(silver_m1, desiredState):
    """
    desiredState = is the current state of the case, e.g. 'Awaiting Appeal Lodgement'
    """

    df = silver_m1.select(
        "CaseNo", 
        lit(desiredState).alias("ariaDesiredState"),
        lit("14").alias("ariaMigrationTaskDueDays")
    )

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("audit.dv_representation"), col("audit.lu_appealType")]

    df_audit = silver_m1.alias("audit").join(df.alias("content"), on = ["CaseNo"], how = "left").select(col("CaseNo"),
  
    #ariaDesiredState - ARIADM-797
    array(struct(*common_inputFields, lit("ariaDesiredState"))).alias("ariaDesiredState_inputFields"),
    array(struct(*common_inputValues, lit("null"))).alias("ariaDesiredState_inputValues"),
    col("content.ariaDesiredState"),
    lit("no").alias("ariaDesiredState_Transformation"),

    #ariaMigrationTaskDueDays - ARIADM-797
    array(struct(*common_inputFields, lit("ariaMigrationTaskDueDays"))).alias("ariaMigrationTaskDueDays_inputFields"),
    array(struct(*common_inputValues, lit("null"))).alias("ariaMigrationTaskDueDays_inputValues"),
    col("content.ariaMigrationTaskDueDays"),
    lit("no").alias("ariaMigrationTaskDueDays_Transformation")
    )

    return df,df_audit

if __name__ == "__main__":
    pass