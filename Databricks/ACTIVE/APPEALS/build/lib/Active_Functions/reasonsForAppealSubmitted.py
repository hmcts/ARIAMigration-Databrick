import importlib.util
from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    current_timestamp, collect_set, first, array_contains, 
    size, udf, coalesce, concat_ws, concat, trim, year,split,datediff, size, nullif
)
from . import paymentPending as PP
from . import appealSubmitted as APS
from . import AwaitingEvidenceRespondant_a as AERa
from . import AwaitingEvidenceRespondant_a as AERb


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

def legalRepDetails(silver_m1): 
    df_legalRepDetails, df_audit_legalRepDetails = PP.legalRepDetails(silver_m1)
    
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

def hearingResponse(silver_m1, silver_m3, silver_m6):

    conditions = (col("dv_representation") == 'AIP') & (col("dv_CCDAppealType").isNotNull())
    row_conditions = silver_m3.groupBy("CaseNo").agg(spark_max("StatusId").alias("max_StatusId"))
    m3_conditions = (col("CaseStatus") == 26) #IF status = 26 = include else OMIT
    m6_conditions = when(col("Required") == '0', lit('Not Required')).when(col("Required") == '1', lit('Required'))

    court_clerk_agg = silver_m3.withColumn("CourtClerkFull", 
                                           when((col("CourtClerk_Surname").isNotNull()) & (col("CourtClerk_Surname") != ""), concat_ws(" ", col("CourtClerk_Surname"), col("CourtClerk_Forenames"),
        when((col("CourtClerk_Title").isNotNull()) & (col("CourtClerk_Title") != ""),
            concat(lit("("), col("CourtClerk_Title"), lit(")"))).otherwise(lit(None))))
            ).groupby("CaseNo"
            ).agg(when(size(collect_list("CourtClerkFull")) > 0, collect_list("CourtClerkFull")).otherwise(lit(None)).alias("Court Clerk / Usher"))
    
    df = silver_m1.join(silver_m3, ["CaseNo"], "left"
                    ).join(silver_m6, ["CaseNo"], "left"
                    ).join(court_clerk_agg, ["CaseNo"], "left"
                    ).join(row_conditions, ["CaseNo"], "left"
                ).withColumn("CaseNo", col("CaseNo")
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
                ).withColumn("TransformationRequired", m6_conditions
                ).withColumn("Required/Incompatible Judicial Officers", concat_ws(" ", col("Judge_Surname"), col                ("Judge_Forenames"), col("Judge_Title"), "TransformationRequired")
                ).withColumn("Notes",
                            when(col("Notes").isNull(), "N/A").otherwise(col("Notes"))
                ).withColumn("valid_statusId",
                            when(
                                col("CaseStatus").isin("37", "38") | ((col("CaseStatus") == 26) & (col("Outcome") == 0)),
                                col("max_StatusId")
                            ).otherwise(lit(None))
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
                            lit("\n Court Clerk / Usher: "), coalesce(nullif(concat_ws(", ", col("Court Clerk / Usher")), lit("")), lit("N/A")),
                            lit("\n Start Time: "), coalesce(col("Start Time"), lit("N/A")),
                            lit("\n Estimated Duration: "), coalesce(col("Estimated Duration"), lit("N/A")),
                            lit("\n Required/Incompatible Judicial Officers: "), coalesce(col("Required/Incompatible Judicial Officers"), lit("N/A")),
                            lit("\n Notes: "), coalesce(col("Notes"), lit("N/A"))
                        )
                             
                ).select(
                    col("CaseNo"),
                    "additionalInstructionsTribunalResponse"
                ).where(col("valid_statusId").isNotNull() & conditions & m3_conditions).distinct()
            
    common_inputFields = [lit("dv_representation"), lit("dv_CCDAppealType")]
    common_inputValues = [col("m1_audit.dv_representation"), col("m1_audit.dv_CCDAppealType")]

    df_audit = (
        df.alias("content")
        .join(silver_m1.alias("m1_audit"), ["CaseNo"], "left")
        .join(silver_m3.alias("m3_audit"), ["CaseNo"], "left")
        .join(silver_m6.alias("m6_audit"), ["CaseNo"], "left")
        .join(court_clerk_agg, ["CaseNo"], "left")
        .join(row_conditions, ["CaseNo"], "left")
        .select(
            col("CaseNo"),

            array(
                struct(
                    lit("HearingCentre"),
                    lit("HearingDate"),
                    lit("HearingType"),
                    lit("CourtName"),
                    lit("ListType"),
                    lit("StartTime"),
                    lit("Judge1FT_Surname"),
                    lit("Judge2FT_Surname"),
                    lit("Judge3FT_Surname"),
                    lit("Court Clerk / Usher"),
                    lit("TimeEstimate"),
                    lit("Judge_Surname"),
                    lit("Judge_Forenames"),
                    lit("Judge_Title"),
                    lit("Notes")
                )
            ).alias("additionalInstructionsTribunalResponse_inputFields"),

            array(
                struct(
                    col("m3_audit.HearingCentre").cast("string"),
                    col("m3_audit.HearingDate").cast("string"),
                    col("m3_audit.HearingType").cast("string"),
                    col("m3_audit.CourtName").cast("string"),
                    col("m3_audit.ListType").cast("string"),
                    col("m3_audit.StartTime").cast("string"),
                    col("m3_audit.Judge1FT_Surname").cast("string"),
                    col("m3_audit.Judge2FT_Surname").cast("string"),
                    col("m3_audit.Judge3FT_Surname").cast("string"),
                    col("Court Clerk / Usher").cast("string"),
                    col("m3_audit.TimeEstimate").cast("string"),
                    col("m6_audit.Judge_Surname").cast("string"),
                    col("m6_audit.Judge_Forenames").cast("string"),
                    col("m6_audit.Judge_Title").cast("string"),
                    col("m3_audit.Notes").cast("string")
                )
            ).alias("additionalInstructionsTribunalResponse_inputValues"),
            col("content.additionalInstructionsTribunalResponse"),
            lit("yes").alias("additionalInstructionsTribunalResponse_Transformed")
        )
    ).distinct()
    
    return df, df_audit

def general(silver_m1): 
    df_general, df_audit_general = PP.general(silver_m1)
    
    df_general = df_general.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_general, df_audit_general

def generalDefault(silver_m1): 
    df_generalDefault = AERb.generalDefault(silver_m1)
    
    df_generalDefault = df_generalDefault.withColumn("changeDirectionDueDateActionAvailable", lit("Yes")
                                        ).withColumn("markEvidenceAsReviewedActionAvailable", lit("Yes")
                                        ).withColumn("uploadAdditionalEvidenceActionAvailable", lit("Yes")
                                        ).withColumn("uploadAdditionalEvidenceHomeOfficeActionAvailable",lit("Yes")
                                        ).withColumn("uploadHomeOfficeBundleAvailable", lit("Yes")
                                
                ).select('*',
                lit("No").alias("uploadHomeOfficeBundleActionAvailable"),
                lit("This is a migrated ARIA case. Please see the documents provided as part of the notice of appeal.").alias("reasonsForAppealDecision")
                ).where(col("dv_representation") == 'AIP').distinct()

    return df_generalDefault

def documents(silver_m1): 
    AERb_documents = AERb.documents
    df_documents, df_audit_documents = AERb_documents(silver_m1)
    
    df_documents = df_documents.select('*').where(col("dv_representation") == 'AIP').distinct()
    
    return df_documents, df_audit_documents

def caseState(silver_m1): 
    df_caseState, df_audit_caseState = original_caseState(silver_m1, "reasonForAppealSubmitted")
    
    df_caseState = df_caseState.select(*df_caseState.columns).where(col("dv_representation") == 'AIP').distinct()
    
    return df_caseState, df_audit_caseState



