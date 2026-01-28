import importlib.util
from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    current_timestamp, collect_set, first, array_contains, 
    size, udf, coalesce, concat_ws, concat, trim, year,split,datediff, size, nullif, desc
)
from pyspark.sql import functions as F
from pyspark.sql import Window
from . import paymentPending as PP
from . import appealSubmitted as APS
from . import AwaitingEvidenceRespondant_a as AERa
from . import AwaitingEvidenceRespondant_a as AERb
from . import caseUnderReview as CUR


def appealType(silver_m1): 
    df_appealType, df_audit_appealType = PP.appealType(silver_m1) 
    
    df_appealType = df_appealType.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_appealType, df_audit_appealType

def caseData(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres): 
    df_caseData, df_audit_caseData = PP.caseData(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres) 
    
    df_caseData = df_caseData.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_caseData, df_audit_caseData

def flagsLabels(silver_m1, silver_m2, silver_c): 
    df_flagsLabels, df_audit_flagsLabels = PP.flagsLabels(silver_m1, silver_m2, silver_c)
    
    df_flagsLabels = df_flagsLabels.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_flagsLabels, df_audit_flagsLabels

def legalRepDetails(silver_m1, bronze_countryFromAddress): 
    df_legalRepDetails, df_audit_legalRepDetails = PP.legalRepDetails(silver_m1, bronze_countryFromAddress)
    
    df_legalRepDetails = df_legalRepDetails.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_legalRepDetails, df_audit_legalRepDetails

def appellantDetails(silver_m1, silver_m2, silver_c, bronze_countryFromAddress,bronze_HORef_cleansing): 
    df_appellantDetails, df_audit_apellantDetails = AERa.appellantDetails(silver_m1, silver_m2, silver_c, bronze_countryFromAddress,bronze_HORef_cleansing)
    
    df_appellantDetails = df_appellantDetails.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_appellantDetails, df_audit_apellantDetails

def homeOfficeDetails(silver_m1, silver_m2, silver_c, bronze_HORef_cleansing): 
    df_homeOfficeDetails, df_audit_homeOfficeDetails = PP.homeOfficeDetails(silver_m1, silver_m2, silver_c, bronze_HORef_cleansing)
    
    df_homeOfficeDetails = df_homeOfficeDetails.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_homeOfficeDetails, df_audit_homeOfficeDetails

def paymentType(silver_m1, silver_m4): 
    df_paymentType, df_audit_paymentType = APS.paymentType(silver_m1, silver_m4)
    
    df_paymentType = df_paymentType.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_paymentType, df_audit_paymentType

def partyID(silver_m1, silver_m3, silver_c): 
    df_partyID, df_audit_partyID = PP.partyID(silver_m1, silver_m3, silver_c)
    
    df_partyID = df_partyID.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_partyID, df_audit_partyID

def remissionTypes(silver_m1, bronze_remission_lookup_df,silver_m4): 
    df_remissionTypes, df_audit_remissionTypes = APS.remissionTypes(silver_m1, bronze_remission_lookup_df,silver_m4)
    
    df_remissionTypes = df_remissionTypes.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_remissionTypes, df_audit_remissionTypes

def sponsorDetails(silver_m1, silver_c): 
    df_sponsorDetails, df_audit_sponsorDetails = PP.sponsorDetails(silver_m1, silver_c)
    
    df_sponsorDetails = df_sponsorDetails.join(
        silver_m1.select("CaseNo", "dv_representation"), ["CaseNo"], "left"
    ).select(*df_sponsorDetails.columns).where(col("dv_representation") == 'AIP').distinct()
        
    return df_sponsorDetails, df_audit_sponsorDetails

def hearingResponse(silver_m1,silver_m3, silver_m6):
    
    window = Window.partitionBy("CaseNo").orderBy(desc("StatusId"))
    df_stg = silver_m3.filter((F.col("CaseStatus").isin([37,38])) | (F.col("CaseStatus") == 26) & (F.col("Outcome") == 0)).withColumn("rn",F.row_number().over(window)).filter(F.col("rn") == 1).drop(F.col("rn"))
 
 
    m3_df = df_stg.withColumn("CourtClerkFull",
                                            when((col("CourtClerk_Surname").isNotNull()) & (col("CourtClerk_Surname") != ""), concat_ws(" ", col("CourtClerk_Surname"), col("CourtClerk_Forenames"),
        when((col("CourtClerk_Title").isNotNull()) & (col("CourtClerk_Title") != ""),
            concat(lit("("), col("CourtClerk_Title"), lit(")"))).otherwise(lit(None))))
            )
 
    stg_m6 = silver_m6.withColumn("Transformed_Required",when(F.col("Required") == '0', lit('Not Required')).when(F.col("Required") == '1', lit('Required')))
 
 
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
                                when(col("TimeEstimate").isNull(), "N/A").otherwise(col("TimeEstimate"))
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
                                lit("\n Court Clerk / Usher: "), coalesce(nullif(concat_ws(", ", col("CourtClerkFull")), lit("")), lit("N/A")),
                                lit("\n Start Time: "), coalesce(col("Start Time"), lit("N/A")),
                                lit("\n Estimated Duration: "), coalesce(col("Estimated Duration"), lit("N/A")),
                                lit("\n Required/Incompatible Judicial Officers: "), coalesce(col("Required/Incompatible Judicial Officers"), lit("N/A")),
                                lit("\n Notes: "), coalesce(col("Notes"), lit("N/A"))
                            )
                                
                    )
    additionalInstructionsTribunalResponse_schema_dict = {
        "Hearing Centre": ["HearingCentre"],
        "Hearing Date": ["HearingDate"],
        "Hearing Type": ["HearingType"],
        "Court": ["CourtName"],
        "List Type": ["ListType"],
        "List Start Time": ["StartTime"],
        "Judge First Tier": [
            "Judge1FT_Surname", "Judge1FT_Forenames", "Judge1FT_Title",
            "Judge2FT_Surname", "Judge2FT_Forenames", "Judge2FT_Title",
            "Judge3FT_Surname", "Judge3FT_Forenames", "Judge3FT_Title"
        ],
        "Start Time": ["StartTime"],
        "Estimated Duration": ["TimeEstimate"],
        "Required/Incompatible Judicial Officers": [
            "Judge_Surname", "Judge_Forenames", "Judge_Title", "Transformed_Required"
        ],
        "Notes": ["Notes"],
        "Court Clerk / Usher": [
            "CourtClerk_Surname", "CourtClerk_Forenames", "CourtClerk_Title"
        ]
    }
 
    content_df = final_df.select(
        col("CaseNo"),
        col("additionalInstructionsTribunalResponse")).where(col('dv_representation') == 'AIP')
 
    df_audit = final_df.alias("f").join(silver_m1.alias("m1"), col("m1.CaseNo") == col("f.CaseNo"), "left").select(
        col("f.CaseNo"),
        col("f.additionalInstructionsTribunalResponse"),
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
            struct(
                lit("dv_CCDAppealType").alias("field"),
                array(lit("dv_CCDAppealType")).alias("source_columns")
            )
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
            struct(lit("dv_representation").alias("field"), col("m1.dv_representation").cast("string").alias("value")),
            struct(lit("dv_CCDAppealType").alias("field"), col("m1.dv_CCDAppealType").cast("string").alias("value"))
        ).alias("additionalInstructionsTribunalResponse_inputValues")
    )
    return content_df, df_audit

def general(silver_m1, silver_m2, silver_m3, silver_h, bnronze_hearing_centres, bronze_derive_hearing_centres): 
    df_general, df_audit_general = PP.general(silver_m1, silver_m2, silver_m3, silver_h, bnronze_hearing_centres, bronze_derive_hearing_centres)
    
    df_general = df_general.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_general, df_audit_general

def generalDefault(silver_m1): 
    df_generalDefault = CUR.generalDefault(silver_m1)
    
    df_generalDefault = df_generalDefault.withColumn("changeDirectionDueDateActionAvailable", lit("Yes")
                                        ).withColumn("markEvidenceAsReviewedActionAvailable", lit("Yes")
                                        ).withColumn("uploadAdditionalEvidenceActionAvailable", lit("Yes")
                                        ).withColumn("uploadAdditionalEvidenceHomeOfficeActionAvailable",lit("Yes")
                                        ).withColumn("uploadHomeOfficeBundleAvailable", lit("Yes")
                                
                ).select('*',
                # lit("No").alias("uploadHomeOfficeBundleActionAvailable"),
                lit("This is a migrated ARIA case. Please see the documents provided as part of the notice of appeal.").alias("reasonsForAppealDecision")
                ).where(col("dv_representation") == 'AIP').distinct()

    return df_generalDefault

def documents(silver_m1): 
    AERb_documents = AERb.documents
    df_documents, df_audit_documents = AERb_documents(silver_m1)
    
    df_documents = df_documents.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_documents, df_audit_documents

if __name__ == "__main__":
    pass
