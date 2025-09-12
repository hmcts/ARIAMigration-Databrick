import importlib.util
from pyspark.sql.functions import col, struct, lit, concat, array
from . import paymentPending as PP
from . import appealSubmitted as APS


##########################
# Appellant Details
##########################

def appellantDetails(silver_m1, silver_m2, silver_c,bronze_countryFromAddress,bronze_HORef_cleansing): 
    df_apellantDetails, df_audit_appellantDetails = PP.appellantDetails(silver_m1, silver_m2, silver_c,bronze_countryFromAddress,bronze_HORef_cleansing)
    
    # Update column changeDirectionDueDateActionAvailable and add two new colums per mapping document 
    df_apellantDetails = df_apellantDetails.withColumn("appellantFullName", concat(col("appellantGivenNames"), lit(" "), col("appellantFamilyName")))

    ### Create new column order to ensure the appellantFullName gets inserted to the correct location
    new_order = ["CaseNo", "appellantFamilyName", "appellantGivenNames", "appellantFullName","appellantNameForDisplay", "appellantDateOfBirth", "isAppellantMinor", "caseNameHmctsInternal","hmctsCaseNameInternal", "internalAppellantEmail", "email", "internalAppellantMobileNumber", "mobileNumber", "appellantInUk", "appealOutOfCountry", "oocAppealAdminJ", "appellantHasFixedAddress", "appellantHasFixedAddressAdminJ", "appellantAddress", "addressLine1AdminJ", "addressLine2AdminJ", "addressLine3AdminJ", "addressLine4AdminJ", "countryGovUkOocAdminJ", "appellantStateless", "appellantNationalities", "appellantNationalitiesDescription", "deportationOrderOptions"]

    df_apellantDetails = df_apellantDetails.select(*new_order).distinct()

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("m1.dv_representation"), col("m1.lu_appealType")]

    df_audit_appellantDetails = (
    silver_m1.alias("m1")
    .join(df_apellantDetails.alias("ae"), ["CaseNo"], "left")
    .select(
        col("m1.CaseNo"),

        # Family name audit
        array(struct(*common_inputFields, lit("appellantFamilyName"))).alias("appellantFamilyName_inputfields"),
        array(struct(*common_inputValues, col("ae.appellantFamilyName"))).alias("appellantFamilyName_inputvalues"),
        col("ae.appellantFamilyName"),
        lit("yes").alias("appellantFamilyName_Transformation"),

        # Given names audit
        array(struct(*common_inputFields, lit("appellantGivenNames"))).alias("appellantGivenNames_inputfields"),
        array(struct(*common_inputValues, col("ae.appellantGivenNames"))).alias("appellantGivenNames_inputvalues"),
        col("ae.appellantGivenNames"),
        lit("yes").alias("appellantGivenNames_Transformation"),

        # Full name audit
        array(struct(*common_inputFields, lit("appellantGivenNames"), lit("appellantFamilyName"))).alias("appellantFullName_inputfields"),
        array(struct(*common_inputValues, col("ae.appellantGivenNames"), col("ae.appellantFamilyName"))).alias("appellantFullName_inputvalues"),
        col("ae.appellantFullName"),
        lit("yes").alias("appellantFullName_Transformation"),

        # Name for display
        array(struct(*common_inputFields, lit("appellantNameForDisplay"))).alias("appellantNameForDisplay_inputfields"),
        array(struct(*common_inputValues, col("ae.appellantNameForDisplay"))).alias("appellantNameForDisplay_inputvalues"),
        col("ae.appellantNameForDisplay"),
        lit("yes").alias("appellantNameForDisplay_Transformation"),

        # DOB
        array(struct(*common_inputFields, lit("appellantDateOfBirth"))).alias("appellantDateOfBirth_inputfields"),
        array(struct(*common_inputValues, col("ae.appellantDateOfBirth"))).alias("appellantDateOfBirth_inputvalues"),
        col("ae.appellantDateOfBirth"),
        lit("yes").alias("appellantDateOfBirth_Transformation"),

        # isMinor
        array(struct(*common_inputFields, lit("isAppellantMinor"))).alias("isAppellantMinor_inputfields"),
        array(struct(*common_inputValues, col("ae.isAppellantMinor"))).alias("isAppellantMinor_inputvalues"),
        col("ae.isAppellantMinor"),
        lit("yes").alias("isAppellantMinor_Transformation"),

        # hmcts internal case name
        array(struct(*common_inputFields, lit("caseNameHmctsInternal"))).alias("caseNameHmctsInternal_inputfields"),
        array(struct(*common_inputValues, col("ae.caseNameHmctsInternal"))).alias("caseNameHmctsInternal_inputvalues"),
        col("ae.caseNameHmctsInternal"),
        lit("yes").alias("caseNameHmctsInternal_Transformation"),

        array(struct(*common_inputFields, lit("hmctsCaseNameInternal"))).alias("hmctsCaseNameInternal_inputfields"),
        array(struct(*common_inputValues, col("ae.hmctsCaseNameInternal"))).alias("hmctsCaseNameInternal_inputvalues"),
        col("ae.hmctsCaseNameInternal"),
        lit("yes").alias("hmctsCaseNameInternal_Transformation"),

        # Emails
        array(struct(*common_inputFields, lit("internalAppellantEmail"))).alias("internalAppellantEmail_inputfields"),
        array(struct(*common_inputValues, col("ae.internalAppellantEmail"))).alias("internalAppellantEmail_inputvalues"),
        col("ae.internalAppellantEmail"),
        lit("yes").alias("internalAppellantEmail_Transformation"),

        array(struct(*common_inputFields, lit("email"))).alias("email_inputfields"),
        array(struct(*common_inputValues, col("ae.email"))).alias("email_inputvalues"),
        col("ae.email"),
        lit("yes").alias("email_Transformation"),

        # Mobiles
        array(struct(*common_inputFields, lit("internalAppellantMobileNumber"))).alias("internalAppellantMobileNumber_inputfields"),
        array(struct(*common_inputValues, col("ae.internalAppellantMobileNumber"))).alias("internalAppellantMobileNumber_inputvalues"),
        col("ae.internalAppellantMobileNumber"),
        lit("yes").alias("internalAppellantMobileNumber_Transformation"),

        array(struct(*common_inputFields, lit("mobileNumber"))).alias("mobileNumber_inputfields"),
        array(struct(*common_inputValues, col("ae.mobileNumber"))).alias("mobileNumber_inputvalues"),
        col("ae.mobileNumber"),
        lit("yes").alias("mobileNumber_Transformation"),

        # In UK / Out of country
        array(struct(*common_inputFields, lit("appellantInUk"))).alias("appellantInUk_inputfields"),
        array(struct(*common_inputValues, col("ae.appellantInUk"))).alias("appellantInUk_inputvalues"),
        col("ae.appellantInUk"),
        lit("yes").alias("appellantInUk_Transformation"),

        array(struct(*common_inputFields, lit("appealOutOfCountry"))).alias("appealOutOfCountry_inputfields"),
        array(struct(*common_inputValues, col("ae.appealOutOfCountry"))).alias("appealOutOfCountry_inputvalues"),
        col("ae.appealOutOfCountry"),
        lit("yes").alias("appealOutOfCountry_Transformation"),

        array(struct(*common_inputFields, lit("oocAppealAdminJ"))).alias("oocAppealAdminJ_inputfields"),
        array(struct(*common_inputValues, col("ae.oocAppealAdminJ"))).alias("oocAppealAdminJ_inputvalues"),
        col("ae.oocAppealAdminJ"),
        lit("yes").alias("oocAppealAdminJ_Transformation"),

        # Address
        array(struct(*common_inputFields, lit("appellantHasFixedAddress"))).alias("appellantHasFixedAddress_inputfields"),
        array(struct(*common_inputValues, col("ae.appellantHasFixedAddress"))).alias("appellantHasFixedAddress_inputvalues"),
        col("ae.appellantHasFixedAddress"),
        lit("yes").alias("appellantHasFixedAddress_Transformation"),

        array(struct(*common_inputFields, lit("appellantHasFixedAddressAdminJ"))).alias("appellantHasFixedAddressAdminJ_inputfields"),
        array(struct(*common_inputValues, col("ae.appellantHasFixedAddressAdminJ"))).alias("appellantHasFixedAddressAdminJ_inputvalues"),
        col("ae.appellantHasFixedAddressAdminJ"),
        lit("yes").alias("appellantHasFixedAddressAdminJ_Transformation"),

        array(struct(*common_inputFields, lit("appellantAddress"))).alias("appellantAddress_inputfields"),
        array(struct(*common_inputValues, col("ae.appellantAddress"))).alias("appellantAddress_inputvalues"),
        col("ae.appellantAddress"),
        lit("yes").alias("appellantAddress_Transformation"),

        array(struct(*common_inputFields, lit("addressLine1AdminJ"))).alias("addressLine1AdminJ_inputfields"),
        array(struct(*common_inputValues, col("ae.addressLine1AdminJ"))).alias("addressLine1AdminJ_inputvalues"),
        col("ae.addressLine1AdminJ"),
        lit("yes").alias("addressLine1AdminJ_Transformation"),

        array(struct(*common_inputFields, lit("addressLine2AdminJ"))).alias("addressLine2AdminJ_inputfields"),
        array(struct(*common_inputValues, col("ae.addressLine2AdminJ"))).alias("addressLine2AdminJ_inputvalues"),
        col("ae.addressLine2AdminJ"),
        lit("yes").alias("addressLine2AdminJ_Transformation"),

        array(struct(*common_inputFields, lit("addressLine3AdminJ"))).alias("addressLine3AdminJ_inputfields"),
        array(struct(*common_inputValues, col("ae.addressLine3AdminJ"))).alias("addressLine3AdminJ_inputvalues"),
        col("ae.addressLine3AdminJ"),
        lit("yes").alias("addressLine3AdminJ_Transformation"),

        array(struct(*common_inputFields, lit("addressLine4AdminJ"))).alias("addressLine4AdminJ_inputfields"),
        array(struct(*common_inputValues, col("ae.addressLine4AdminJ"))).alias("addressLine4AdminJ_inputvalues"),
        col("ae.addressLine4AdminJ"),
        lit("yes").alias("addressLine4AdminJ_Transformation"),

        array(struct(*common_inputFields, lit("countryGovUkOocAdminJ"))).alias("countryGovUkOocAdminJ_inputfields"),
        array(struct(*common_inputValues, col("ae.countryGovUkOocAdminJ"))).alias("countryGovUkOocAdminJ_inputvalues"),
        col("ae.countryGovUkOocAdminJ"),
        lit("yes").alias("countryGovUkOocAdminJ_Transformation"),

        # Identity
        array(struct(*common_inputFields, lit("appellantStateless"))).alias("appellantStateless_inputfields"),
        array(struct(*common_inputValues, col("ae.appellantStateless"))).alias("appellantStateless_inputvalues"),
        col("ae.appellantStateless"),
        lit("yes").alias("appellantStateless_Transformation"),

        array(struct(*common_inputFields, lit("appellantNationalities"))).alias("appellantNationalities_inputfields"),
        array(struct(*common_inputValues, col("ae.appellantNationalities"))).alias("appellantNationalities_inputvalues"),
        col("ae.appellantNationalities"),
        lit("yes").alias("appellantNationalities_Transformation"),

        array(struct(*common_inputFields, lit("appellantNationalitiesDescription"))).alias("appellantNationalitiesDescription_inputfields"),
        array(struct(*common_inputValues, col("ae.appellantNationalitiesDescription"))).alias("appellantNationalitiesDescription_inputvalues"),
        col("ae.appellantNationalitiesDescription"),
        lit("yes").alias("appellantNationalitiesDescription_Transformation"),

        array(struct(*common_inputFields, lit("deportationOrderOptions"))).alias("deportationOrderOptions_inputfields"),
        array(struct(*common_inputValues, col("ae.deportationOrderOptions"))).alias("deportationOrderOptions_inputvalues"),
        col("ae.deportationOrderOptions"),
        lit("yes").alias("deportationOrderOptions_Transformation")

        ).distinct()
    )

    return df_apellantDetails, df_audit_appellantDetails

def generalDefault(silver_m1): 
    df_generalDefault = PP.generalDefault(silver_m1) 
    
    df_generalDefault = (
        df_generalDefault.select("*",
                                 lit([]).cast("array<string>").alias('directions'),
                                 lit("Yes").alias("uploadHomeOfficeBundleAvailable"))
    )
            
    return df_generalDefault

#AERb does not pull through from AERb during RFPAS......... Hence, pull from here. No need to update audit as default value.
def documents(silver_m1):

    documents_df, documents_audit = PP.documents(silver_m1) 

    documents_df = (documents_df.select("*",
                                        lit([]).cast("array<string>").alias("respondentDocuments")).where(col("dv_representation") == 'AIP'))

    return documents_df, documents_audit

if __name__ == "__main__":
    pass

