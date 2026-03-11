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

    documents_df, documents_audit = FSA.documents(silver_m1,silver_m3)

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
            col("allFtpaRespondentDecisionDocs"),
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
##########              setAside                     ###########
################################################################


def setAside(silver_m1, silver_m3, silver_m6):
    # Window: highest StatusId per CaseNo
    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Filter: CaseStatus in (42, 43, 44) AND Outcome = 86
    # Note: use & (bitwise AND), not 'and', and avoid HTML-encoded &amp;
    silver_m3_filtered_casestatus = silver_m3.filter(
        col("CaseStatus").isin([39]) #& (col("Outcome") == 86)
        # Alternatively: col("Outcome").isin([86])
    )

    # Rank and keep the highest StatusId per CaseNo
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn(
        "row_number", row_number().over(window_spec)
    )
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    # Build remittal content
    setaside_df = (
        silver_m1.alias("m1").join(silver_m3_max_statusid.alias("m3"), on="CaseNo", how="left")
        .withColumn("reasonRehearingRule32", lit("Set aside and to be reheard under rule 32"))
        .withColumn("rule32ListingAdditionalIns", lit("This is an ARIA Migrated case. Please refer to the documents for any additional listing instructions."))
        .withColumn("updateTribunalDecisionList", lit("underRule32"))
        .withColumn("ftpaFinalDecisionRemadeRule32", lit(""))
        .withColumn("updateTribunalDecisionDateRule32", date_format(col("DecisionDate"), "yyyy-MM-dd")) 
        .withColumn("ftpaAppellantDecisionRemadeRule32Text", when(col("Party") == 1, lit("This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.")).otherwise(None)) 
        .withColumn("ftpaRespondentDecisionRemadeRule32Text", when(col("Party") == 2, lit("This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.")).otherwise(None)) 
        
        .select(
            col("CaseNo"),
            col("reasonRehearingRule32"),
            col("rule32ListingAdditionalIns"),
            col("updateTribunalDecisionList"),
            col("ftpaFinalDecisionRemadeRule32"),
            col("updateTribunalDecisionDateRule32"),
            col("ftpaAppellantDecisionRemadeRule32Text"),
            col("ftpaRespondentDecisionRemadeRule32Text"),
        )
    )

    # Build audit (content joined back to source)
    setaside_audit = (
        silver_m1.alias("m1").join(silver_m3_max_statusid.alias("m3"), on="CaseNo", how="left")
        .join(setaside_df.alias("content"), on=["CaseNo"], how="left")
        .select(
            col("m1.CaseNo"),

            # ---- reasonRehearingRule32 ----
            array(struct(lit("reasonRehearingRule32"))).alias("reasonRehearingRule32_inputFields"),
            array(struct(lit("null"))).alias("reasonRehearingRule32_inputValues"),
            col("reasonRehearingRule32").alias("reasonRehearingRule32_value"),
            lit("Yes").alias("reasonRehearingRule32_Transformation"),

            # ---- rule32ListingAdditionalIns ----
            array(struct(lit("rule32ListingAdditionalIns"))).alias("rule32ListingAdditionalIns_inputFields"),
            array(struct(lit("null"))).alias("rule32ListingAdditionalIns_inputValues"),
            col("rule32ListingAdditionalIns").alias("rule32ListingAdditionalIns_value"),
            lit("Yes").alias("rule32ListingAdditionalIns_Transformation"),

            # ---- updateTribunalDecisionList ----
            array(struct(lit("updateTribunalDecisionList"))).alias("updateTribunalDecisionList_inputFields"),
            array(struct(lit("null"))).alias("updateTribunalDecisionList_inputValues"),
            col("updateTribunalDecisionList").alias("updateTribunalDecisionList_value"),
            lit("Yes").alias("updateTribunalDecisionList_Transformation"),

            # ---- ftpaFinalDecisionRemadeRule32 ----
            array(struct(lit("ftpaFinalDecisionRemadeRule32"))).alias("ftpaFinalDecisionRemadeRule32_inputFields"),
            array(struct(lit("null"))).alias("ftpaFinalDecisionRemadeRule32_inputValues"),
            col("ftpaFinalDecisionRemadeRule32").alias("ftpaFinalDecisionRemadeRule32_value"),
            lit("Yes").alias("ftpaFinalDecisionRemadeRule32_Transformation"),

            # ---- updateTribunalDecisionDateRule32 ----
            array(struct(lit("DecisionDate"),lit("CaseStatus"),lit("Outcome"))).alias("updateTribunalDecisionDateRule32_inputFields"),
            array(struct(col("m3.DecisionDate"),col("CaseStatus"),col("Outcome"))).alias("updateTribunalDecisionDateRule32_inputValues"),
            col("updateTribunalDecisionDateRule32").alias("updateTribunalDecisionDateRule32_value"),
            lit("Yes").alias("updateTribunalDecisionDateRule32_Transformation"),

            # ---- ftpaAppellantDecisionRemadeRule32Text ----
            array(struct(lit("Party"),lit("CaseStatus"),lit("Outcome"))).alias("ftpaAppellantDecisionRemadeRule32Text_inputFields"),
            array(struct(col("m3.Party"),col("CaseStatus"),col("Outcome"))).alias("ftpaAppellantDecisionRemadeRule32Text_inputValues"),
            col("ftpaAppellantDecisionRemadeRule32Text").alias("ftpaAppellantDecisionRemadeRule32Text_value"),
            lit("Yes").alias("ftpaAppellantDecisionRemadeRule32Text_Transformation"),

            # ---- ftpaRespondentDecisionRemadeRule32Text ----
            array(struct(lit("Party"),lit("CaseStatus"),lit("Outcome"))).alias("ftpaRespondentDecisionRemadeRule32Text_inputFields"),
            array(struct(col("m3.Party"),col("CaseStatus"),col("Outcome"))).alias("ftpaRespondentDecisionRemadeRule32Text_inputValues"),
            col("ftpaRespondentDecisionRemadeRule32Text").alias("ftpaRespondentDecisionRemadeRule32Text_value"),
            lit("Yes").alias("ftpaRespondentDecisionRemadeRule32Text_Transformation"),

        )
    )
    return setaside_df, setaside_audit


################################################################
##########              ftpa          ###########
################################################################

def ftpa(silver_m3,silver_c):

    ftpa_df,ftpa_audit = FSA.ftpa(silver_m3,silver_c)

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(39))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    silver_m3_content = (
        silver_m3_max_statusid
        .withColumn("ftpaFirstDecision",lit("remadeRule32"))
        .withColumn("ftpaFinalDecisionForDisplay",lit("undecided"))
        .withColumn("ftpaApplicantType",
            when(col("Party") == 1, lit("appellant")).when(col("Party") == 2, lit("respondent")).otherwise(None))
        .withColumn("ftpaAppellantDecisionDate", when(col("Party") == 1,date_format(col("DecisionDate"), "yyyy-MM-dd")).otherwise(None))
        .withColumn("ftpaRespondentDecisionDate", when(col("Party") == 2,date_format(col("DecisionDate"), "yyyy-MM-dd")).otherwise(None))
        .withColumn("ftpaAppellantRjDecisionOutcomeType", when(col("Party") == 1,lit("remadeRule32")).otherwise(None))
        .withColumn("ftpaRespondentRjDecisionOutcomeType", when(col("Party") == 2,lit("remadeRule32")).otherwise(None))
        )

    ftpa_df = (
        ftpa_df.alias("ftpa")
            .join(silver_m3_content.alias("m3"), on=["CaseNo"], how="left")
            .select(
                    "ftpa.*",
                    col("ftpaFirstDecision"),
                    col("ftpaFinalDecisionForDisplay"),
                    col("ftpaApplicantType"),
                    col("ftpaAppellantDecisionDate"),
                    col("ftpaRespondentDecisionDate"),
                    col("ftpaAppellantRjDecisionOutcomeType"),
                    col("ftpaRespondentRjDecisionOutcomeType"),
            )
    )

    # Build the audit DataFrame
    ftpa_audit = (
        ftpa_audit.alias("audit")
            .join(ftpa_df.alias("ftpa"), on=["CaseNo"], how="left")
            .join(silver_m3_max_statusid.alias("m3"), on=["CaseNo"], how="left")
            .select(
                "audit.*",
                array(struct(lit("ftpaFirstDecision"),lit("Party"),lit("CaseStatus"))).alias("ftpaFirstDecision_inputFields"),
                array(struct(lit("Null"),col("Party"),col("CaseStatus"))).alias("ftpaFirstDecision_inputValues"),
                col("ftpaFirstDecision").alias("ftpaFirstDecision_value"),
                lit("Yes").alias("ftpaFirstDecision_Transformation"),

                array(struct(lit("ftpaFinalDecisionForDisplay"),lit("Party"),lit("CaseStatus"))).alias("ftpaFinalDecisionForDisplay_inputFields"),
                array(struct(lit("Null"),col("Party"),col("CaseStatus"))).alias("ftpaFinalDecisionForDisplay_inputValues"),
                col("ftpaFinalDecisionForDisplay").alias("ftpaFinalDecisionForDisplay_value"),
                lit("Yes").alias("ftpaFinalDecisionForDisplay_Transformation"),

                array(struct(lit("Party"),lit("CaseStatus"))).alias("ftpaApplicantType_inputFields"),
                array(struct(col("Party"),col("CaseStatus"))).alias("ftpaApplicantType_inputValues"),
                col("ftpaApplicantType").alias("ftpaApplicantType_value"),
                lit("Yes").alias("ftpaApplicantType_Transformation"),

                array(struct(lit("Party"),lit("CaseStatus"),lit("DecisionDate"))).alias("ftpaAppellantDecisionDate_inputFields"),
                array(struct(col("Party"),col("CaseStatus"),col("DecisionDate"))).alias("ftpaAppellantDecisionDate_inputValues"),
                col("ftpaAppellantDecisionDate").alias("ftpaAppellantDecisionDate_value"),
                lit("Yes").alias("ftpaAppellantDecisionDate_Transformation"),

                array(struct(lit("Party"),lit("CaseStatus"),lit("DecisionDate"))).alias("ftpaRespondentDecisionDate_inputFields"),
                array(struct(col("Party"),col("CaseStatus"),col("DecisionDate"))).alias("ftpaRespondentDecisionDate_inputValues"),
                col("ftpaRespondentDecisionDate").alias("ftpaRespondentDecisionDate_value"),
                lit("Yes").alias("ftpaRespondentDecisionDate_Transformation"),

                array(struct(lit("Party"),lit("CaseStatus"))).alias("ftpaAppellantRjDecisionOutcomeType_inputFields"),
                array(struct(col("Party"),col("CaseStatus"))).alias("ftpaAppellantRjDecisionOutcomeType_inputValues"),
                col("ftpaAppellantRjDecisionOutcomeType").alias("ftpaAppellantRjDecisionOutcomeType_value"),
                lit("Yes").alias("ftpaAppellantRjDecisionOutcomeType_Transformation"),

                array(struct(lit("Party"),lit("CaseStatus"))).alias("ftpaRespondentRjDecisionOutcomeType_inputFields"),
                array(struct(col("Party"),col("CaseStatus"))).alias("ftpaRespondentRjDecisionOutcomeType_inputValues"),
                col("ftpaRespondentRjDecisionOutcomeType").alias("ftpaRespondentRjDecisionOutcomeType_value"),
                lit("Yes").alias("ftpaRespondentRjDecisionOutcomeType_Transformation"),
            )
    )

    return ftpa_df, ftpa_audit


################################################################
##########              general          ###########
################################################################

def general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres):

    general_df,general_audit = FSA.general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres)

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())
    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(39))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")

    silver_m3_content = (
        silver_m3_max_statusid
            .withColumn("isFtpaAppellantDecided",
                        when(col("Party") == 1, lit("Yes")).otherwise(None))
            .withColumn("isFtpaRespondentDecided",
                        when(col("Party") == 2, lit("No")).otherwise(None))
    )

    general_df = (
        general_df.alias("gen")
            .join(silver_m3_content.alias("m3"), on=["CaseNo"], how="left")
            .select(
                col("gen.*"),
                col("isFtpaAppellantDecided"),
                col("isFtpaRespondentDecided"),
            )
    )

    general_audit = (
        general_audit.alias("audit")
            .join(general_df.alias("gen"), on=["CaseNo"], how="left")
            .join(silver_m3_max_statusid.alias("m3"), on=["CaseNo"], how="left")
            .select(
                "audit.*",

                # -------------------------------------------------------------
                # 1. isFtpaAppellantDecided
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("CaseStatus"))).alias("isFtpaAppellantDecided_inputFields"),
                array(struct(col("m3.Party"), col("m3.CaseStatus"))).alias("isFtpaAppellantDecided_inputValues"),
                col("isFtpaAppellantDecided").alias("isFtpaAppellantDecided_value"),
                lit("Yes").alias("isFtpaAppellantDecided_Transformation"),

                # -------------------------------------------------------------
                # 2. isFtpaRespondentDecided
                # -------------------------------------------------------------
                array(struct(lit("Party"), lit("CaseStatus"))).alias("isFtpaRespondentDecided_inputFields"),
                array(struct(col("m3.Party"), col("m3.CaseStatus"))).alias("isFtpaRespondentDecided_inputValues"),
                col("isFtpaRespondentDecided").alias("isFtpaRespondentDecided_value"),
                lit("Yes").alias("isFtpaRespondentDecided_Transformation"),

            )
    )
    return general_df, general_audit

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