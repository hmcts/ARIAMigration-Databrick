from datetime import datetime
import re
import string
import pycountry
import pandas as pd
import json

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType,StructType,StructField
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

def detained(silver_m1, silver_m2,bronze_detention_centres):

    joined_m1_m2 =(
        silver_m1.alias("m1")
        .join(silver_m2.alias("m2"), on="CaseNo", how="left")
        
    )

    detained_df = (
        joined_m1_m2.alias("m2").join(bronze_detention_centres.alias("det"), on="DetentionCentreId", how="left")
        .withColumn("appellantInDetention", when(col("m2.Detained").isin([1,2,4]), "Yes").otherwise(lit('No')))
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

    detained_audit = (
        joined_m1_m2.alias("m1_m2")
        .join(bronze_detention_centres.alias("det"),col("m1_m2.DetentionCentreId") == col("det.DetentionCentreId"),"left")
        .join(detained_df.alias("content"), on="CaseNo", how="left")
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

    return detained_df, detained_audit

################################################################
##########              caseData grouping            ###########
################################################################

# caseData grouping
def caseData(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres, bronze_detention_centres):

    case_mgmt_schema = StructType([
    StructField("region", StringType(), True),
    StructField("baseLocation", StringType(), True)
    ])

    bronze_detention_centres = bronze_detention_centres.withColumn(
        "caseManagementLocation",
        F.from_json(col("caseManagementLocation"), case_mgmt_schema)
    )

    caseData_df, caseData_audit = PP.caseData(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres)

    # caseData_df = caseData_df.drop("hearingCentre","staffLocation","caseManagementLocation","hearingCentreDynamicList","caseManagementLocationRefData","selectedHearingCentreRefData")

    joined_m1_m2 =(
        silver_m1.alias("m1")
        .join(silver_m2.alias("m2"), on="CaseNo", how="left")
        )
    
    caseData_df = (
        caseData_df.alias("content").join(joined_m1_m2.alias("m2"),on="CaseNo", how="left")
        .join(bronze_detention_centres.alias("det"), on="DetentionCentreId", how="left")
        .join(bronze_hearing_centres.alias("bhc"),on=col("m2.CentreId") == col("bhc.CentreId"),how="left")
        .withColumn("hearingCentre1", when(col("m2.Detained").isin(1,2),col("det.hearingCentre")).otherwise(col("content.hearingCentre")))
        .withColumn("staffLocation1", when(col("m2.Detained").isin(1,2),col("det.staffLocation")).otherwise(col("content.staffLocation")))
        .withColumn("caseManagementLocation1", when(col("m2.Detained").isin(1,2),col("det.caseManagementLocation")).otherwise(col("content.caseManagementLocation")))
        .withColumn("hearingCentreDynamicList1", when(col("m2.Detained").isin(1,2),
                                                      struct(
                                                            struct(
                                                                col("det.locationCode").alias("code"),
                                                                col("det.locationLabel").alias("label")
                                                            ).alias("value"),array(
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
                                                    ).alias("list_items")))
                    .otherwise(col("content.hearingCentreDynamicList"))
        )
        .withColumn("caseManagementLocationRefData1", 
                    when(col("m2.Detained").isin(1,2),struct(
            lit("1").alias("region"),
            struct(
                struct(
                    col("det.locationCode").alias("code"),
                    col("det.locationLabel").alias("label")
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
        ))
                    
                    .otherwise(col("content.caseManagementLocationRefData")))
        .withColumn("selectedHearingCentreRefData1", 
                    when(col("m2.Detained").isin(1,2),col("det.selectedHearingCentreRefData")).otherwise(col("content.selectedHearingCentreRefData")))
        .drop("hearingCentre","staffLocation","caseManagementLocation","hearingCentreDynamicList","caseManagementLocationRefData","selectedHearingCentreRefData")
        .select(
            "content.*",
            col("hearingCentre1").alias("hearingCentre"),
            col("staffLocation1").alias("staffLocation"),
            col("caseManagementLocation1").alias("caseManagementLocation"),
            col("hearingCentreDynamicList1").alias("hearingCentreDynamicList"),
            col("caseManagementLocationRefData1").alias("caseManagementLocationRefData"),
            col("selectedHearingCentreRefData1").alias("selectedHearingCentreRefData"),
        )
    )
    
    return caseData_df, caseData_audit

################################################################
##########             General Function              ###########
################################################################

def general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres, bronze_detention_centres):

    general_df, general_audit = PP.general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres)

    # general_df = general_df.drop("applicationChangeDesignatedHearingCentre")

    joined_m1_m2 =(
        silver_m1.alias("m1")
        .join(silver_m2.alias("m2"), on="CaseNo", how="left")
        )
    
    general_df = (
        general_df.alias("content").join(joined_m1_m2.alias("m2"),on="CaseNo", how="left")
        .join(bronze_detention_centres.alias("det"), on="DetentionCentreId", how="left")
        .join(bronze_hearing_centres.alias("bhc"),on=col("m2.CentreId") == col("bhc.CentreId"),how="left")
        .withColumn("applicationChangeDesignatedHearingCentre1", when(col("m2.Detained").isin(1,2),col("det.applicationChangeDesignatedHearingCentre"))
                    .otherwise(col("content.applicationChangeDesignatedHearingCentre")))
        .drop(col("content.applicationChangeDesignatedHearingCentre"))
        .select("content.*",
                col("applicationChangeDesignatedHearingCentre1").alias("applicationChangeDesignatedHearingCentre")
                )
    )

    return general_df, general_audit

################################################################
##########             appellantDetails Function              ###########
################################################################

def appellantDetails(silver_m1, silver_m2, silver_c, bronze_countryFromAddress, bronze_HORef_cleansing):

    appellantDetails_df, appellantDetails_audit = PP.appellantDetails(silver_m1, silver_m2, silver_c, bronze_countryFromAddress, bronze_HORef_cleansing)

    appellantDetails_df = appellantDetails_df.drop("appellantInUk","appealOutOfCountry","appellantHasFixedAddress")
    # appellantDetails_audit = appellantDetails_audit.drop("appellantAddress")

    silver_c_grouped = silver_c.groupBy("CaseNo").agg(collect_list(col("CategoryId")).alias("CategoryIdList"))

    
    appellantDetails_df = (
        appellantDetails_df.alias("content")
        .join(silver_m2.alias("m2"), on="CaseNo", how="left")
        .join(silver_c_grouped.alias("mc"), on="CaseNo", how="left")

        # -----------------------------
        # appellantInUk logic
        # -----------------------------
        .withColumn(
            "appellantInUk",
            when(col("m2.Detained").isin(1, 2, 4) | expr("array_contains(CategoryIdList, 37)"), "Yes")
            .when(expr("array_contains(CategoryIdList, 38)"), "No")
            .otherwise(None)
        )

        # -----------------------------
        # appealOutOfCountry logic
        # -----------------------------
        .withColumn(
            "appealOutOfCountry",
            when(col("m2.Detained").isin(1, 2, 4) | expr("array_contains(CategoryIdList, 37)"), "No")
            .when(expr("array_contains(CategoryIdList, 38)"), "Yes")
            .otherwise(None)
        )

        # -----------------------------
        # appellantHasFixedAddress logic
        # -----------------------------
        .withColumn(
            "appellantHasFixedAddress",
            when(col("m2.Detained").isin(1, 2), None)
            .when(expr("array_contains(CategoryIdList, 37)"), "Yes")
            .otherwise(None)
        )

        # -----------------------------
        # appellantAddress logic
        # -----------------------------
        .withColumn(
            "appellantAddress1",
            when(col("m2.Detained").isin(1, 2), None)
            .when(expr("array_contains(CategoryIdList, 37)"), col("content.appellantAddress"))
            .otherwise(None)
        )

        .drop("appellantAddress")

        # -----------------------------
        # Final select
        # -----------------------------
        .select(
            "content.*",
            col("appellantInUk"),
            col("appealOutOfCountry"),
            col("appellantHasFixedAddress"),
            col("appellantAddress1").alias("appellantAddress"),
        )
    )

    return appellantDetails_df, appellantDetails_audit

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


# Register the UDF
phoneNumberUDF = udf(cleanPhoneNumber, StringType())


################################################################
##########    mobile phone number regex check.       ###########
################################################################

def filterMobilePhoneNumber(PhoneNumber):
    if PhoneNumber is None:
        return None

    cleanedPhoneNumber = cleanPhoneNumber(PhoneNumber)

    mobile_number_pattern = re.compile("^((\\+44(\\s\\(0\\)\\s|\\s0\\s|\\s)?)|0)7\\d{3}(\\s)?\\d{6}$")

    if mobile_number_pattern.match(cleanedPhoneNumber):
        return cleanedPhoneNumber
    else:
        return None


filterMobilePhoneNumberUDF = udf(filterMobilePhoneNumber, StringType())

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

    category_condition = (array_contains(col("CategoryIdList"), 38))
    name_condition = col("Sponsor_Name").isNotNull()

    grouped = grouped.withColumn("hasSponsor", when(name_condition, lit("Yes")).otherwise("No")
    ).withColumn("sponsorGivenNames", when(name_condition, col("Sponsor_Forenames")).otherwise(lit(None))
    ).withColumn("sponsorFamilyName", when((name_condition), col("Sponsor_Name")).otherwise(lit(None))
    ).withColumn("sponsorAuthorisation",when(name_condition,when(col("Sponsor_Authorisation") == True, lit("Yes")).otherwise(lit("No")))
    ).withColumn("sponsorAddress",when(name_condition,
            struct(
                coalesce(
                    col("Sponsor_Address1"),
                    col("Sponsor_Address2"),
                    col("Sponsor_Address3"),
                    col("Sponsor_Address4"),
                    col("Sponsor_Address5")
                ).alias("AddressLine1"),
                coalesce(col("Sponsor_Address2"), lit("")).alias("AddressLine2"),
                lit("").alias("AddressLine3"),
                coalesce(col("Sponsor_Address3"), lit("")).alias("PostTown"),
                coalesce(col("Sponsor_Address4"), lit("")).alias("County"),
                coalesce(col("Sponsor_Address5"), lit("")).alias("Country"),
                coalesce(col("Sponsor_Postcode"), lit("")).alias("PostCode")
            )
        )
    ).withColumn("sponsorEmailAdminJ",when((name_condition),cleanEmailUDF(col("Sponsor_Email")))
    ).withColumn("sponsorMobileNumberAdminJ",when((name_condition),filterMobilePhoneNumberUDF(col("Sponsor_Telephone")))
    ).withColumn("sponsorNameForDisplay",when(name_condition,concat(col("Sponsor_Forenames"), lit(" "), col("Sponsor_Name"))).otherwise(lit(None))
    ).withColumn("sponsorAddressForDisplay",
        when(
            name_condition,
            concat_ws(
                "\r\n",
                col("Sponsor_Address1"),
                col("Sponsor_Address2"),
                col("Sponsor_Address3"),
                col("Sponsor_Address4"),
                col("Sponsor_Address5"),
                col("Sponsor_Postcode")
            )
        ).otherwise(lit(None))
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
        "sponsorNameForDisplay",
        "sponsorAddressForDisplay"
    )

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
        lit("yes").alias("sponsorMobileNumberAdminJ_Transformation"),

        #audit sponsorNameForDisplay
        array(struct(*common_inputFields, lit("audit.Sponsor_Name"), lit("Sponsor_Forenames"))).alias("sponsorNameForDisplay_inputFields"),
        array(struct(*common_inputValues, col("audit.Sponsor_Name"), col("audit.Sponsor_Forenames"))).alias("sponsorNameForDisplay_inputValues"),
        col("content.sponsorNameForDisplay"),
        lit("yes").alias("sponsorNameForDisplay_Transformation"),

        #audit sponsorAddressForDisplay
        array(struct(*common_inputFields, lit("audit.Sponsor_Address1"), lit("audit.Sponsor_Address2"), lit("audit.Sponsor_Address3"), lit("audit.Sponsor_Address4"), lit("audit.Sponsor_Address5"), lit("audit.Sponsor_Postcode"))).alias("sponsorAddressForDisplay_inputFields"),
        array(struct(*common_inputValues, col("audit.Sponsor_Address1"), col("audit.Sponsor_Address2"), col("audit.Sponsor_Address3"), col("audit.Sponsor_Address4"), col("audit.Sponsor_Address5"), col("audit.Sponsor_Postcode"))).alias("sponsorAddressForDisplay_inputValues"),
        col("content.sponsorAddressForDisplay"),
        lit("yes").alias("sponsorAddressForDisplay_Transformation"),
    )

    return df, df_audit


################################################################

################################################################   

if __name__ == "__main__":
    pass