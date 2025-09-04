import importlib.util
from pyspark.sql.functions import col, struct, lit, concat
from . import paymentPending as PP


# Exact path to your .py file
# file_path = "/Workspace/Users/andrew.mcdevitt@hmcts.net/active_common_utils.py"
# file_path = os.getcwd() + "/paymentPending.py"
# module_name = "paymentPending"

# # Load the module explicitly
# spec = importlib.util.spec_from_file_location(module_name, file_path)            # Load the module
# paymentPending = importlib.util.module_from_spec(spec)                      # Build the module
# spec.loader.exec_module(paymentPending)                                     # Execute the module

##########################
# Appellant Details
##########################

def appellantDetails(silver_m1, silver_m2, silver_c, bronze_countryFromAddress): 
    df_awaitingEvidenceRespondent_a, df_audit_awaitingEvidenceRespondent_a = PP.appellantDetails(silver_m1, silver_m2, silver_c, bronze_countryFromAddress) 
    
    # Update column changeDirectionDueDateActionAvailable and add two new colums per mapping document 
    df_awaitingEvidenceRespondent_a = df_awaitingEvidenceRespondent_a.withColumn("appellantFullName", concat(col("appellantGivenNames"), lit(" "), col("appellantFamilyName")))

    ### Create new column order to ensure the appellantFullName gets inserted to the correct location
    new_order = ["CaseNo", "appellantFamilyName", "appellantGivenNames", "appellantFullName","appellantNameForDisplay", "appellantDateOfBirth", "isAppellantMinor", "caseNameHmctsInternal","hmctsCaseNameInternal", "internalAppellantEmail", "email", "internalAppellantMobileNumber", "mobileNumber", "appellantInUk", "appealOutOfCountry", "oocAppealAdminJ", "appellantHasFixedAddress", "appellantHasFixedAddressAdminJ", "appellantAddress", "addressLine1AdminJ", "addressLine2AdminJ", "addressLine3AdminJ", "addressLine4AdminJ", "countryGovUkOocAdminJ", "appellantStateless", "appellantNationalities", "appellantNationalitiesDescription", "deportationOrderOptions"]

    df_awaitingEvidenceRespondent_a = df_awaitingEvidenceRespondent_a.select(*new_order)
    
    return df_awaitingEvidenceRespondent_a, df_audit_awaitingEvidenceRespondent_a

def generalDefault(silver_m1): 
    paymentPending_generalDefault_content, paymentPending_generalDefault_audit = PP.generalDefault(silver_m1) 
    
    # Update column changeDirectionDueDateActionAvailable and add two new colums per mapping document 
    df_awaitingEvidenceRespondent_b = (
        paymentPending_generalDefault_content
        .withColumn("directions", lit([]).cast("array<string>").alias("directions") )
        .withColumn("uploadHomeOfficeBundleAvailable", lit("Yes"))
    )
            
    return paymentPending_generalDefault_content, paymentPending_generalDefault_audit

def homeOfficeDetails(silver_m1, silver_m2, silver_c, bronze_HORef_cleansing):
    df, df_audit = PP.homeOfficeDetails(silver_m1, silver_m2, silver_c, bronze_HORef_cleansing)
    return df, df_audit

if __name__ == "__main__":
    pass

