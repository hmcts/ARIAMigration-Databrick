# active_common_utils notebook
def greet(name):
    return f"Hello, {name}! Welcome to Databricks."

from pyspark.sql.functions import col, lit, array, struct

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

        #Audit isAppealReferenceNumberAvailable
        array(struct(*common_inputFields)).alias("ccdReferenceNumberForDisplay_inputFields"),
        array(struct(*common_inputValues)).alias("ccdReferenceNumberForDisplay_inputValues"),
        col("content.ccdReferenceNumberForDisplay"),
        lit("yes").alias("ccdReferenceNumberForDisplay_Transformation")

    )
    
    return df, df_audit

    if __name__ == "__main__":
    