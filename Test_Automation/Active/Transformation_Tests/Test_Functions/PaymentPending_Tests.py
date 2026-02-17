from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains, explode,
    year, month, dayofmonth, countDistinct, count
)

from functools import reduce
from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    ArrayType,
    BooleanType
)

import re

#Import Test Results class
from models.test_result import TestResult

#Not working to set default value,
# TestResult.DEFAULT_TEST_FROM_STATE = "paymentPending"

#Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "paymentPending"

#######################
#sponsorDetails Init Code
#######################
def test_sponsorDetails_init(json, M1_bronze, C):
    try:
        json_sd = json.select(
            "appealReferenceNumber",
            "hasSponsor",
            "sponsorGivenNames",
            "sponsorFamilyName",
            "sponsorAuthorisation",
            "appealWasNotSubmittedReason",
            "sponsorAddress",
            "sponsorEmailAdminJ",
            "sponsorMobileNumberAdminJ"
        )

        M1_sd =  M1_bronze.select(
            "CaseNo",
            "Sponsor_Authorisation",
            "Sponsor_Forenames",
            "Sponsor_Name",
            "Sponsor_Address1",
            "Sponsor_Address2",
            "Sponsor_Address3",
            "Sponsor_Address4",
            "Sponsor_Address5",
            "Sponsor_Postcode",
            "Sponsor_Email",
            "Sponsor_Telephone"
        )

        C = C.select(
            "CaseNo",
            "CategoryId"
        )

        test_df_sd = json_sd.join(
            C,
            C.CaseNo == json_sd.appealReferenceNumber,
            "inner"
        )

        test_df_sd = test_df_sd.join(
            M1_sd,
            M1_sd.CaseNo == test_df_sd.appealReferenceNumber,
            "inner"
        )

        test_df_sd = test_df_sd.select(
            "appealReferenceNumber",
            "CategoryId",
            "hasSponsor",
            "sponsorGivenNames",
            "sponsorFamilyName",
            "sponsorAuthorisation",
            "Sponsor_Authorisation",
            "appealWasNotSubmittedReason",
            "sponsorAddress",
            "Sponsor_Forenames",
            "Sponsor_Name",
            "Sponsor_Address1",
            "Sponsor_Address2",
            "Sponsor_Address3",
            "Sponsor_Address4",
            "Sponsor_Address5",
            "Sponsor_Postcode",
            "sponsorEmailAdminJ",
            "sponsorMobileNumberAdminJ",
            "Sponsor_Email",
            "Sponsor_Telephone"
        )

        test_df_sd = (
            test_df_sd
            .groupBy("appealReferenceNumber")
            .agg(
                collect_list("CategoryId").alias("CategoryIds"),
                first("hasSponsor").alias("hasSponsor"),
                first("sponsorGivenNames").alias("sponsorGivenNames"),
                first("sponsorFamilyName").alias("sponsorFamilyName"),
                first("sponsorAuthorisation").alias("sponsorAuthorisation"),
                first("Sponsor_Authorisation").alias("Sponsor_Authorisation"),
                first("appealWasNotSubmittedReason").alias("appealWasNotSubmittedReason"),
                first("sponsorAddress").alias("sponsorAddress"),
                first("Sponsor_Forenames").alias("Sponsor_Forenames"),
                first("Sponsor_Name").alias("Sponsor_Name"),
                first("Sponsor_Address1").alias("Sponsor_Address1"),
                first("Sponsor_Address2").alias("Sponsor_Address2"),
                first("Sponsor_Address3").alias("Sponsor_Address3"),
                first("Sponsor_Address4").alias("Sponsor_Address4"),
                first("Sponsor_Address5").alias("Sponsor_Address5"),
                first("Sponsor_Postcode").alias("Sponsor_Postcode"),
                first("sponsorEmailAdminJ").alias("sponsorEmailAdminJ"),
                first("sponsorMobileNumberAdminJ").alias("sponsorMobileNumberAdminJ"),
                first("Sponsor_Email").alias("SponsorEmail"),
                first("Sponsor_Telephone").alias("SponsorTelephone")
            )
        )

        return test_df_sd, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("hasSponsor", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state )

# CategoryId 38 means out of country case, so we need to check that sponsor fields are not populated if 38 is not included, rather than vice versa, and hasSponsor should always be no

# hasSponsor - "IF CategoryId IN [38] = Include; ELSE OMIT, IF SponsorName IS NOT NULL = Yes; ELSE No"
#######################
# hasSponsor - IF CategoryId not in 38 and hasSponsor not omitted 
#######################
def test_hasSponsor_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(~(array_contains(col("CategoryIds"), 38))).count() == 0:
        return TestResult("hasSponsor", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac1_hasSponsor_test1 = test_df_sd.filter(
    (~(array_contains(col("CategoryIds"), 38))) & (col("hasSponsor").isNotNull())
    )

    if ac1_hasSponsor_test1.count() != 0:
        return TestResult("hasSponsor","FAIL", f"hasSponsor acceptance criteria 1 - failed: {str(ac1_hasSponsor_test1.count())} cases have been found where the CategoryIds do not contain 38, but hasSponsor has been included.",test_from_state)
    else:
        return TestResult("hasSponsor","PASS", f"hasSponsor acceptance criteria 1 passed, all cases where the CategoryIds do not contain 38 have hasSponsor omitted.",test_from_state)

#######################
# hasSponsor - "IF CategoryId IN [38] + Sponsor_Name is Null and hasSponsor != No
#######################
def test_hasSponsor_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(array_contains(col("CategoryIds"), 38) & (col("Sponsor_Name").isNull())).count() == 0:
        return TestResult("hasSponsor", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac1_hasSponsor_test2 = test_df_sd.filter(
     (array_contains(col("CategoryIds"), 38) & col("Sponsor_Name").isNull()) & (col("hasSponsor") != "No")
    )

    if ac1_hasSponsor_test2.count() != 0:
        return TestResult("hasSponsor", "FAIL", f"hasSponsor acceptance criteria 2 - failed: {str(ac1_hasSponsor_test2.count())} cases have been found where Sponsor_Name is null and hasSponsor is null or not equal to No",test_from_state)
    else:
        return TestResult("hasSponsor", "PASS", f"hasSponsor acceptance criteria 2 passed, all cases where Sponsor_Name is null have hasSponsor set to No",test_from_state)

#######################
# hasSponsor - "IF CategoryId IN [38] + Sponsor_Name is not Null and hasSponsor != Yes
#######################
def test_hasSponsor_test3(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(array_contains(col("CategoryIds"), 38) & (col("Sponsor_Name").isNotNull())).count() == 0:
        return TestResult("hasSponsor", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac1_hasSponsor_test3 = test_df_sd.filter(
     (array_contains(col("CategoryIds"), 38) & col("Sponsor_Name").isNotNull()) & (col("hasSponsor") != "Yes")
    )    

    if ac1_hasSponsor_test3.count() != 0:
        return TestResult("hasSponsor", "FAIL", f"hasSponsor acceptance criteria 3 - failed: {str(ac1_hasSponsor_test3.count())} cases have been found where Sponsor_Name is not null and hasSponsor is null or not equal to Yes",test_from_state)
    else:
        return TestResult("hasSponsor", "PASS", f"hasSponsor acceptance criteria 3 passed, all cases where Sponsor_Name is not null have hasSponsor set to Yes",test_from_state)

# IF CategoryId IN [38] = Include; ELSE OMIT, IF SponsorName IS NOT NULL = Include; ELSE OMIT, IF Sponsor_Authorised IS 1 = Yes; ELSE No    
#######################
#sponsorGivenNames - IF CategoryId not in 38 and sponsorGivenNames not omitted 
#######################
def test_sponsorGivenNames_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(~(array_contains(col("CategoryIds"), 38))).count() == 0:
        return TestResult("sponsorGivenNames", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac2_sponsorGivenNames_test1 = test_df_sd.filter(
    (~(array_contains(col("CategoryIds"), 38))) & (col("sponsorGivenNames").isNotNull())
    )

    if ac2_sponsorGivenNames_test1.count() != 0:
        return TestResult("sponsorGivenNames", "FAIL", f"sponsorGivenNames acceptance criteria 1 - failed: {str(ac2_818_test1.count())} cases have been found where the CategoryIds do not contain 38, but sponsorGivenNames has not been omitted.",test_from_state)
    else:
        return TestResult("sponsorGivenNames", "PASS", f"sponsorGivenNames acceptance criteria 1 passed, all cases where the CategoryIds do not contain 38 have sponsorGivenNames omitted.",test_from_state)

#######################
#sponsorGivenNames - If CategoryId in 38 + Sponsor_Forenames is Null and sponsorGivenNames is not omitted
#######################
def test_sponsorGivenNames_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(array_contains(col("CategoryIds"), 38) & col("Sponsor_Forenames").isNull()).count() == 0:
        return TestResult("sponsorGivenNames", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac2_sponsorGivenNames_test2 = test_df_sd.filter(
    (array_contains(col("CategoryIds"), 38) & col("Sponsor_Forenames").isNull()) & (col("sponsorGivenNames").isNotNull())
    )

    if ac2_sponsorGivenNames_test2.count() != 0:
        return TestResult("sponsorGivenNames", "FAIL", f"sponsorGivenNames acceptance criteria 2 - failed: {str(ac2_sponsorGivenNames_test2.count())} cases have been found where Sponsor_Forenames from ARIA is null, but sponsorGivenNames has a value.",test_from_state)
    else:
        return TestResult("sponsorGivenNames", "PASS", f"sponsorGivenNames acceptance criteria 2 passed, all cases where Sponsor_Forenames is null, sponsorGivenNames are also null.",test_from_state)

#######################
#sponsorGivenNames - If CategoryId in 38 + Sponsor_Forenames is not Null and sponsorGivenNames is omitted
#######################
def test_sponsorGivenNames_test3(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(array_contains(col("CategoryIds"), 38) & col("Sponsor_Forenames").isNotNull()).count() == 0:
        return TestResult("sponsorGivenNames", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac2_sponsorGivenNames_test3 = test_df_sd.filter(
    (array_contains(col("CategoryIds"), 38) & (col("Sponsor_Forenames").isNotNull())) & (col("sponsorGivenNames") != col("Sponsor_Forenames"))
    )

    if ac2_sponsorGivenNames_test3.count() != 0:
        return TestResult("sponsorGivenNames", "FAIL", f"sponsorGivenNames acceptance criteria 3 - failed: {str(ac2_sponsorGivenNames_test3.count())} cases have been found where Sponsor_Forenames from ARIA is not null, but sponsorGivenNames does not have a matching value.",test_from_state), ac2_sponsorGivenNames_test3
    else:
        return TestResult("sponsorGivenNames", "PASS", f"sponsorGivenNames acceptance criteria 3 passed, all cases between Sponsor_Forenames and sponsorGivenNames are matching.",test_from_state)

# IF CategoryId IN [38] = Include; ELSE OMIT, IF SponsorName IS NOT NULL = Include; ELSE OMIT
#######################
#sponsorFamilyName - If CategoryId not in 38 and sponsorFamilyName not omitted
#######################
def test_sponsorFamilyName_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(~array_contains(col("CategoryIds"), 38)).count() == 0:
        return TestResult("sponsorFamilyName", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac3_sponsorFamilyName_test1 = test_df_sd.filter(
    (~(array_contains(col("CategoryIds"), 38))) & (col("sponsorFamilyName").isNotNull())
    )

    if ac3_sponsorFamilyName_test1.count() != 0:
        return TestResult("sponsorFamilyName", "FAIL", f"sponsorFamilyName acceptance criteria 1 - failed: {str(ac3_sponsorFamilyName_test1.count())} cases have been found where the CategoryIds do not contain 38, but sponsorFamilyName has not been omitted.",test_from_state)
    else:
        return TestResult("sponsorFamilyName", "PASS", f"sponsorFamilyName cceptance criteria 1 passed, all cases where the CategoryIds do not contain 38 have sponsorFamilyName omitted.",test_from_state)

#######################
#sponsorFamilyName - If CategoryId in 38 + Sponsor_Name is Null and sponsorFamilyName is not omitted
#######################
def test_sponsorFamilyName_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(array_contains(col("CategoryIds"), 38) & col("Sponsor_Name").isNull()).count() == 0:
        return TestResult("sponsorFamilyName", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac3_sponsorFamilyName_test2 = test_df_sd.filter(
    (array_contains(col("CategoryIds"), 38) & col("Sponsor_Name").isNull()) & (col("sponsorFamilyName").isNotNull())
    )

    if ac3_sponsorFamilyName_test2.count() != 0:
        return TestResult("sponsorFamilyName","FAIL", f"sponsorFamilyName acceptance criteria 2 - failed: {str(ac3_sponsorFamilyName_test2.count())} cases have been found where Sponsor_Name from ARIA is null, but sponsorFamilyName has a value.",test_from_state)
    else:
        return TestResult("sponsorFamilyName","PASS", f"sponsorFamilyName acceptance criteria 2 passed, all cases where Sponsor_Name is null, sponsorFamilyName are also null.",test_from_state)

#######################
# sponsorFamilyName - If CategoryId in 38 + Sponsor_Name is not Null and sponsorFamilyName is omitted
#######################
def test_sponsorFamilyName_test3(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(array_contains(col("CategoryIds"), 38) & col("Sponsor_Name").isNotNull()).count() == 0:
        return TestResult("sponsorFamilyName", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac3_sponsorFamilyName_test3 = test_df_sd.filter(
    (array_contains(col("CategoryIds"), 38) & (col("Sponsor_Name").isNotNull())) & (col("sponsorFamilyName") != col("Sponsor_Name"))
    )

    if ac3_sponsorFamilyName_test3.count() != 0:
        return TestResult("sponsorFamilyName", "FAIL", f"sponsorFamilyName acceptance criteria 3 - failed: {str(ac3_sponsorFamilyName_test3.count())} cases have been found where Sponsor_Name from ARIA is not null, but sponsorFamilyName does not have a matching value.",test_from_state), ac3_sponsorFamilyName_test3
    else:
        return TestResult("sponsorFamilyName", "PASS", f"sponsorFamilyName acceptance criteria 3 passed, all cases between Sponsor_Name and sponsorFamilyName are matching.",test_from_state)

# IF CategoryId IN [38] = Include; ELSE OMIT, IF SponsorName IS NOT NULL = Include; ELSE OMIT, IF Sponsor_Authorised IS 1 = Yes; ELSE No
#######################
#sponsorAuthorisation - If CategoryId not in 38 and sponsorAuthorisation is not omitted
#######################
def test_sponsorAuthorisation_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(~array_contains(col("CategoryIds"), 38)).count() == 0:
        return TestResult("sponsorAuthorisation", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac4_sponsorAuthorisation_test1 = test_df_sd.filter(
    (~(array_contains(col("CategoryIds"), 38))) & (col("sponsorAuthorisation").isNotNull())
    )

    if ac4_sponsorAuthorisation_test1.count() != 0:
        return TestResult("sponsorAuthorisation", "FAIL", f"sponsorAuthorisation acceptance criteria 1 - failed: {str(ac4_sponsorAuthorisation_test1.count())} cases have been found where the CategoryIds do not contain 38, but sponsorAuthorisation has not been omitted.",test_from_state), ac4_sponsorAuthorisation_test1
    else:
        return TestResult("sponsorAuthorisation", "PASS", f"sponsorAuthorisation acceptance criteria 1 passed, all cases where the CategoryIds do not contain 38 have sponsorAuthorisation omitted.",test_from_state)

#######################
#sponsorAuthorisation - IF CategoryId IN [38] + Sponsor_Authorised IS 1 + Sponsor_Name is not null and sponsorAuthorisation != Yes
#######################
def test_sponsorAuthorisation_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(
        (array_contains(col("CategoryIds"), 38)) & 
        (col("Sponsor_Authorisation") == "True") & 
        (col("Sponsor_Name").isNotNull())
        ).count() == 0:
        return TestResult("sponsorAuthorisation", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac4_sponsorAuthorisation_test2 = test_df_sd.filter(
    (
        (array_contains(col("CategoryIds"), 38)) & 
        (col("Sponsor_Authorisation") == "True") & 
        (col("Sponsor_Name").isNotNull()) 
    )& 
    (col("sponsorAuthorisation") != "Yes")
    )

    if ac4_sponsorAuthorisation_test2.count() != 0:
        return TestResult("sponsorAuthorisation", "FAIL", f"sponsorAuthorisation acceptance criteria 2 - failed: {str(ac4_sponsorAuthorisation_test2.count())} cases have been found where CategoryId is 38, Sponsor_Authorised is True and Sponsor_Name is not Null but sponsorAuthorisation does not equal Yes.",test_from_state)
    else:
        return TestResult("sponsorAuthorisation", "PASS", f"sponsorAuthorisation acceptance criteria 2 passed, all cases where Sponsor_Name from ARIA is null and Sponsor_Name is not Null, sponsorAuthorisation matches.",test_from_state)
    
#######################
#sponsorAuthorisation - IF CategoryId IN [38] + Sponsor_Authorised IS 0 + Sponsor_Name is not null and sponsorAuthorisation != No
#######################
def test_sponsorAuthorisation_test3(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(
        (array_contains(col("CategoryIds"), 38)) & 
        (col("Sponsor_Authorisation") == "False") & 
        (col("Sponsor_Name").isNotNull())
        ).count() == 0:
        return TestResult("sponsorAuthorisation", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac4_sponsorAuthorisation_test3 = test_df_sd.filter(
    (
        (array_contains(col("CategoryIds"), 38)) & 
        ((col("Sponsor_Authorisation")) == "False") & 
        (col("Sponsor_Name").isNotNull()) 
    )& 
    (col("sponsorAuthorisation") != "No")
    )

    if ac4_sponsorAuthorisation_test3.count() != 0:
        return TestResult("sponsorAuthorisation", "FAIL", f"sponsorAuthorisation acceptance criteria 3 - failed: {str(ac4_sponsorAuthorisation_test3.count())} cases have been found where CategoryId is 38, Sponsor_Authorisation is False and Sponsor_Name is not Null but sponsorAuthorisation does not match.",test_from_state)
    else:
        return TestResult("sponsorAuthorisation", "PASS", f"sponsorAuthorisation acceptance criteria 3 passed, all cases where Sponsor_Name from ARIA is no and Sponsor_Name is not Null, sponsorAuthorisation matches.",test_from_state)
    
#######################
#sponsorAuthorisation - IF CategoryId IN [38] + SponsorName IS NULL and sponsorAuthorisation is omitted
#######################
def test_sponsorAuthorisation_test4(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(array_contains(col("CategoryIds"), 38) & col("Sponsor_Name").isNull()).count() == 0:
        return TestResult("sponsorAuthorisation", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac4_sponsorAuthorisation_test4 = test_df_sd.filter(
    (array_contains(col("CategoryIds"), 38) & col("Sponsor_Name").isNull()) & (col("sponsorAuthorisation").isNotNull())
    )

    if ac4_sponsorAuthorisation_test4.count() != 0:
        return TestResult("sponsorAuthorisation", "FAIL", f"sponsorAuthorisation acceptance criteria 4 - failed: {str(ac4_sponsorAuthorisation_test4.count())} cases have been found where CategoryId is 38, Sponsor_Name is Null but sponsorAuthorisation has not been omitted.",test_from_state), ac4_sponsorAuthorisation_test4
    else:
        return TestResult("sponsorAuthorisation", "PASS", f"sponsorAuthorisation acceptance criteria 4 passed, all cases where CategoryId is 38, Sponsor_Name is Null have sponsorAuthorisation correctly omitted.",test_from_state)

#######################
# sponsorAddress - If CategoryId is not 38 and sponsorAddress has not been omitted
#######################
def test_sponsorAddress_ac1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(~array_contains(col("CategoryIds"), 38)).count() == 0:
        return TestResult("sponsorAddress", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac1_sponsorAddress = test_df_sd.filter(
    (~(array_contains(col("CategoryIds"), 38))) & (col("sponsorAddress") != "No")
    )

    if ac1_sponsorAddress.count() != 0:
        return TestResult("sponsorAddress", "FAIL", f"sponsorAddress acceptance criteria 1 - failed: {str(ac1_820.count())} cases have been found where the CategoryIds do not contain 38, but sponsorAddress has not been omitted.",test_from_state)
    else:
        return TestResult("sponsorAddress", "PASS", f"sponsorAddress acceptance criteria 1 passed, all cases where the CategoryIds do not contain 38 have sponsorAddress omitted.",test_from_state)

#######################
#sponsorAddress - If CategoryId is 38 + Sponsor_Name is null and sponsorEmailAdminJ not omitted
#######################
def test_sponsorAddress_ac2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("Sponsor_Name").isNull())
        ).count() == 0:
        return TestResult("sponsorAddress", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac2_sponsorAddress = test_df_sd.filter(
    (
        (array_contains(col("CategoryIds"), 38)) & 
        (col("Sponsor_Name")).isNull()
    ) & 
        (col("sponsorAddress").isNotNull()) & (trim(col("sponsorAddress")) != "")
    )

    if ac2_sponsorAddress.count() != 0:
        return TestResult("sponsorAddress", "FAIL", f"sponsorAddress acceptance criteria 2 - failed: {str(ac2_sponsorAddress.count())} cases have been found where CategoryId is 38, Sponsor_Name is null but sponsorAddress has not been omitted.",test_from_state)
    else:
        return TestResult("sponsorAddress", "PASS", f"sponsorAddress acceptance criteria 2 passed, all cases where CategoryId is 38 and Sponsor_Name is null have sponsorAddress omitted.",test_from_state)
    
#######################
#sponsorAddress - If CategoryId in 38 + Sponsor_Name is not Null and sponsorAddress is omitted
#######################
def test_sponsorAddress_ac3(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("Sponsor_Name").isNotNull())
        ).count() == 0:
        return TestResult("sponsorAddress", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac3_sponsorAddress = test_df_sd.filter(
    (
        (array_contains(col("CategoryIds"), 38)) & 
        (col("Sponsor_Name")).isNotNull()
    ) & 
        col("sponsorAddress").isNull()
    )

    if ac3_sponsorAddress.count() != 0:
        return TestResult("sponsorAddress", "FAIL", f"sponsorAddress acceptance criteria 3 - failed: {str(ac3_sponsorAddress.count())} cases have been found where ",test_from_state)
    else:
        return TestResult("sponsorAddress", "PASS", f"sponsorAddress acceptance criteria 3 passed, all cases where",test_from_state)

#######################
#sponsorAddress - sponsorAddress collection is mapped using ARIA Sponsor_Address1, Sponsor_Address2, Sponsor_Address3, Sponsor_Address4, Sponsor_Address5, Sponsor_Postcode as per in the mapping document APPENDIX-Address
#######################
def test_sponsorAddress_ac4(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(col("sponsorAddress").isNotNull()).count() == 0:
        return TestResult("sponsorAddress", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac4_sponsorAddress = test_df_sd.filter(
    ((col("sponsorAddress").isNotNull() & (col("hasSponsor") == "Yes") & (array_contains(col("CategoryIds"), 38))) &
        (col("sponsorAddress") != concat_ws(
            ", ",
            col("Sponsor_Address1"), 
            col("Sponsor_Address2"), 
            col("Sponsor_Address3"), 
            col("Sponsor_Address4"), 
            col("Sponsor_Address5"), 
            col("Sponsor_Postcode"))
        ))
    )

    if ac4_sponsorAddress.count() != 0:
        return TestResult("sponsorAddress", "FAIL", f"sponsorAddress acceptance criteria 4 - failed: {str(ac4_sponsorAddress.count())} cases have been found where the JSON sponsorAddress field and the ARIA Sponsor_Address fields do not match.",test_from_state)
    else:
        return TestResult("sponsorAddress", "PASS", f"sponsorAddress acceptance criteria 4 passed, all cases have a matching JSON sponsorAddress field and ARIA Sponsor_Address fields.",test_from_state)
    
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

#######################
#sponsorEmailAdminJ - If CategoryId not 38 and sponsorEmailAdminJ not omitted
#######################
def test_sponsorEmailAdminJ_ac1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(
        (~array_contains(col("CategoryIds"), 38))
        ).count() == 0:
        return TestResult("sponsorEmailAdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac1_sponsorEmailAdminJ = test_df_sd.filter(
    (~array_contains(col("CategoryIds"), 38)) & 
    (col("sponsorEmailAdminJ").isNotNull())
    )

    if ac1_sponsorEmailAdminJ.count() != 0:
        return TestResult("sponsorEmailAdminJ", "FAIL", f"sponsorEmailAdminJ acceptance criteria 1 failed: {str(ac1_sponsorEmailAdminJ.count())} cases have been found where the CategoryIds do not contain 38, but sponsorEmailAdminJ has not been omitted.",test_from_state)
    else:
        return TestResult("sponsorEmailAdminJ", "PASS", f"sponsorEmailAdminJ acceptance criteria 1 passed, all cases where the CategoryIds do not contain 38 have sponsorEmailAdminJ omitted.",test_from_state)

#######################
#sponsorEmailAdminJ - If CategoryId is 38 + Sponsor_Name is null and sponsorEmailAdminJ not omitted
#######################
def test_sponsorEmailAdminJ_ac2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("Sponsor_Name").isNull())
        ).count() == 0:
        return TestResult("sponsorEmailAdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac2_sponsorEmailAdminJ = test_df_sd.filter(
    (
    (array_contains(col("CategoryIds"), 38)) &
    (col("Sponsor_Name").isNull())
    ) &
    col("sponsorEmailAdminJ").isNotNull()
    )

    if ac2_sponsorEmailAdminJ.count() != 0:
        return TestResult("sponsorEmailAdminJ", "FAIL", f"sponsorEmailAdminJ acceptance criteria 2 failed: {str(ac2_sponsorEmailAdminJ.count())} cases have been found where the CategoryIds contain 38 and Sponsor_Name is null, but sponsorEmailAdminJ has not been omitted.",test_from_state)
    else:
        return TestResult("sponsorEmailAdminJ", "PASS", f"sponsorEmailAdminJ acceptance criteria 2 passed, all cases where the CategoryIds contain 38 and Sponsor_Name is null have sponsorEmailAdminJ omitted.",test_from_state)

#######################
#sponsorEmailAdminJ - If CategoryId is 38 + Sponsor_Name is not null and sponsorEmailAdminJ omitted
#######################
def test_sponsorEmailAdminJ_ac3(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("Sponsor_Name").isNotNull())
        ).count() == 0:
        return TestResult("sponsorEmailAdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    clean_email_udf = udf(cleanEmail, StringType())

    test_df_sd = test_df_sd.withColumn(
        "sponsor_email_cleaned", 
        clean_email_udf(col("SponsorEmail"))
    )
    
    ac3_sponsorEmailAdminJ = test_df_sd.filter(
    (array_contains(col("CategoryIds"), 38)) &
    (col("Sponsor_Name").isNotNull()) &
    (~col("sponsorEmailAdminJ").eqNullSafe(col("sponsor_email_cleaned")))
    )

    if ac3_sponsorEmailAdminJ.count() != 0:
        return TestResult("sponsorEmailAdminJ", "FAIL", f"sponsorEmailAdminJ acceptance criteria 3 failed: {str(ac3_sponsorEmailAdminJ.count())} cases have been found where the CategoryIds contain 38 and Sponsor_Name is not null, but sponsorEmailAdminJ does not match the ARIA value.",test_from_state), ac3_sponsorEmailAdminJ
    else:
        return TestResult("sponsorEmailAdminJ", "PASS", f"sponsorEmailAdminJ acceptance criteria 3 passed, all cases where the CategoryIds contain 38 and Sponsor_Name is not null have sponsorEmailAdminJ matching.",test_from_state)

#######################
#sponsorMobileNumberAdminJ - If CategoryId not 38 and sponsorMobileNumberAdminJ not omitted
#######################
def test_sponsorMobileNumberAdminJ_ac1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(
        (~array_contains(col("CategoryIds"), 38))
        ).count() == 0:
        return TestResult("sponsorMobileNumberAdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac1_sponsorMobileNumberAdminJ = test_df_sd.filter(
    (~array_contains(col("CategoryIds"), 38)) & 
    (col("sponsorMobileNumberAdminJ").isNotNull())
    )

    if ac1_sponsorMobileNumberAdminJ.count() != 0:
        return TestResult("ac1_sponsorMobileNumberAdminJ", "FAIL", f"sponsorMobileNumberAdminJ acceptance criteria 1 failed: {str(ac1_sponsorMobileNumberAdminJ.count())} cases have been found where the CategoryIds do not contain 38, but sponsorMobileNumberAdminJ has not been omitted.",test_from_state)
    else:
        return TestResult("ac1_sponsorMobileNumberAdminJ", "PASS", f"sponsorMobileNumberAdminJ acceptance criteria 1 passed, all cases where the CategoryIds do not contain 38 have sponsorMobileNumberAdminJ omitted.",test_from_state)

#######################
#sponsorMobileNumberAdminJ - If CategoryId is 38 + Sponsor_Name is null and sponsorMobileNumberAdminJ not omitted
#######################
def test_sponsorMobileNumberAdminJ_ac2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("Sponsor_Name").isNull())
        ).count() == 0:
        return TestResult("sponsorMobileNumberAdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac2_sponsorMobileNumberAdminJ = test_df_sd.filter(
    (
    (array_contains(col("CategoryIds"), 38)) &
    (col("Sponsor_Name").isNull())
    ) &
    col("sponsorMobileNumberAdminJ").isNotNull()
    )

    if ac2_sponsorMobileNumberAdminJ.count() != 0:
        return TestResult("sponsorMobileNumberAdminJ", "FAIL", f"sponsorMobileNumberAdminJ acceptance criteria 2 failed: {str(ac2_sponsorMobileNumberAdminJ.count())} cases have been found where the CategoryIds contain 38 and Sponsor_Name is null, but sponsorMobileNumberAdminJ has not been omitted.",test_from_state)
    else:
        return TestResult("sponsorMobileNumberAdminJ", "PASS", f"sponsorMobileNumberAdminJ acceptance criteria 2 passed, all cases where the CategoryIds contain 38 and Sponsor_Name is null have sponsorMobileNumberAdminJ omitted.",test_from_state)

#######################
#sponsorMobileNumberAdminJ - If CategoryId is 38 + Sponsor_Name is not null and sponsorMobileNumberAdminJ omitted
#######################
def test_sponsorMobileNumberAdminJ_ac3(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("Sponsor_Name").isNotNull())
        ).count() == 0:
        return TestResult("sponsorMobileNumberAdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    clean_phone_udf = udf(cleanPhoneNumber, StringType())

    test_df_sd = test_df_sd.withColumn(
        "sponsor_phone_cleaned", 
        clean_phone_udf(col("SponsorTelephone"))
    )
    
    ac3_sponsorMobileNumberAdminJ = test_df_sd.filter(
    (array_contains(col("CategoryIds"), 38)) &
    (col("Sponsor_Name").isNotNull()) &
    (~col("sponsorMobileNumberAdminJ").eqNullSafe(col("sponsor_phone_cleaned")))
    )

    if ac3_sponsorMobileNumberAdminJ.count() != 0:
        return TestResult("sponsorMobileNumberAdminJ", "FAIL", f"sponsorMobileNumberAdminJ acceptance criteria 3 failed: {str(ac3_sponsorMobileNumberAdminJ.count())} cases have been found where the CategoryIds contain 38 and Sponsor_Name is not null, but sponsorMobileNumberAdminJ does not match the ARIA value.",test_from_state), ac3_sponsorMobileNumberAdminJ
    else:
        return TestResult("sponsorMobileNumberAdminJ", "PASS", f"sponsorMobileNumberAdminJ acceptance criteria 3 passed, all cases where the CategoryIds contain 38 and Sponsor_Name is not null have sponsorMobileNumberAdminJ matching.",test_from_state)

#######################
#sponsorEmailAdminJ - Check fields have been cleansed correctly
#######################
def test_sponsorEmailAdminJ_ac4(test_df_sd):
    clean_email_udf = udf(cleanEmail, StringType())

    test_df_sd = test_df_sd.withColumn(
        "sponsor_email_cleaned", 
        clean_email_udf(col("sponsorEmailAdminJ"))
    )

    ac4_sponsorEmailAdminJ = test_df_sd.filter(
    (
        (col("sponsorEmailAdminJ") != col("sponsor_email_cleaned"))
    ))

    if ac4_sponsorEmailAdminJ.count() != 0:
        return TestResult("sponsorEmailAdminJ", "FAIL", f"sponsorEmailAdminJ acceptance criteria 4 failed: {str(ac4_sponsorEmailAdminJ.count())} cases have been found where the sponsorEmailAdminJ has not been cleansed properly.",test_from_state)
    else:
        return TestResult("sponsorEmailAdminJ", "PASS", f"sponsorEmailAdminJ acceptance criteria 4 passed, all cases have sponsorEmailAdminJ cleansed properly.",test_from_state)

#######################
#sponsorMobileNumberAdminJ - Check fields have been cleansed correctly
#######################
def test_sponsorMobileNumberAdminJ_ac4(test_df_sd):
    clean_phone_udf = udf(cleanPhoneNumber, StringType())

    test_df_sd = test_df_sd.withColumn(
        "sponsor_telephone_cleaned", 
        clean_phone_udf(col("sponsorMobileNumberAdminJ"))
    )

    ac4_sponsorMobileNumberAdminJ = test_df_sd.filter(
    (
        (col("sponsorMobileNumberAdminJ") != col("sponsor_telephone_cleaned"))
    ))

    if ac4_sponsorMobileNumberAdminJ.count() != 0:
        return TestResult("sponsorMobileNumberAdminJ", "FAIL", f"sponsorMobileNumberAdminJ acceptance criteria 4 failed: {str(ac4_sponsorMobileNumberAdminJ.count())} cases have been found where the sponsorMobileNumberAdminJ has not been cleansed properly.",test_from_state)
    else:
        return TestResult("sponsorMobileNumberAdminJ", "PASS", f"sponsorMobileNumberAdminJ acceptance criteria 4 passed, all cases have sponsorMobileNumberAdminJ cleansed properly.",test_from_state)

############################################################################################

#######################
#appealType Init Code
#######################
def test_appealType_init(json, M1_silver, C, bat):
    try:
        json_appealType = json.select(
            "appealReferenceNumber",
            "appealType",
            "hmctsCaseCategory",
            "appealTypeDescription",
            "caseManagementCategory"
        )

        M1_silver_appealType = M1_silver.select(
            "CaseNo",
            "CasePrefix",
            "dv_representation"
        )

        C = C.select(
            "CaseNo",
            "CategoryId",
        )

        test_df_appealType = json_appealType.join(
            M1_silver_appealType,
            json_appealType["appealReferenceNumber"] == M1_silver_appealType["CaseNo"],
            "inner"
        )

        test_df_appealType = test_df_appealType.join(
            C,
            test_df_appealType["appealReferenceNumber"] == C["CaseNo"],
            "inner"
        )

        test_df_appealType = test_df_appealType.join(
            bat,
            bat["appealType"] == json_appealType["appealType"],
            how="left"
        )

        test_df_appealType = test_df_appealType.select(
            M1_silver_appealType["CaseNo"],
            M1_silver_appealType["CasePrefix"],
            C["CategoryId"],
            bat["CCDAppealType"],
            json_appealType["appealType"],
            json_appealType["hmctsCaseCategory"],
            json_appealType["appealTypeDescription"],
            json_appealType["caseManagementCategory"],
            json_appealType["appealReferenceNumber"],
            M1_silver_appealType["dv_representation"]
        )
        
        return test_df_appealType, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("appealType", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state )

def test_appealType_case_mapping(
    test_df,
    CasePrefix,
    CCDAppealType,
    expected_appealType,
    expected_hmctsCaseCategory,
    expected_appealTypeDescription,
    expected_caseManagementCategory_label
):
    output_lines = []
    test_passed = True

    # Filter rows that match the "Given" conditions
    filtered_data = test_df.where(
        (col("CasePrefix") == CasePrefix) &
        (col("CCDAppealType") == CCDAppealType)
    )

    if filtered_data.count() == 0:
        return False, f"*No matching test data* for CasePrefix={CasePrefix}, CCDAppealType={CCDAppealType}", 0

    # Check each row's actual values
    mismatches = filtered_data.where(
        (col("appealType") != expected_appealType) |
        (col("hmctsCaseCategory") != expected_hmctsCaseCategory) |
        (col("appealTypeDescription") != expected_appealTypeDescription)
    )

    if mismatches.count() > 0:
        test_passed = False
        output_lines.append("*Mismatch in appealType, hmctsCaseCategory, or appealTypeDescription*")
    else:
        output_lines.append("All static fields match as expected.")

    # Validate representation logic
    aip_invalid = filtered_data.where(
        (col("dv_representation") == "AIP") &
        (col("caseManagementCategory").isNotNull())
    )

    lr_invalid = filtered_data.where(
        (col("dv_representation") == "LR") &
        (col("caseManagementCategory").isNull())
    )

    if aip_invalid.count() > 0:
        test_passed = False
        output_lines.append("AIP rows should have null caseManagementCategory")

    if lr_invalid.count() > 0:
        test_passed = False
        output_lines.append("LR rows should have non-null caseManagementCategory")

    # Check for caseManagementCategory label if provided
    if expected_caseManagementCategory_label:

        # Explode list_items to access individual label entries
        exploded = filtered_data.withColumn("cm_label", explode(col("caseManagementCategory.list_items.label")))
        
        # Check if expected label is in any row
        label_matches = exploded.where(col("cm_label") == expected_caseManagementCategory_label)

        if label_matches.count() == 0:
            test_passed = False
            output_lines.append(f"No matching label '{expected_caseManagementCategory_label}' found in caseManagementCategory.list_items.label.")
        else:
            output_lines.append(f"caseManagementCategory contains label: '{expected_caseManagementCategory_label}'")


    return test_passed, "\n".join(output_lines), filtered_data.count()

#######################
#AC1 - Given CasePrefix = "DA" and CCDAppealType = "EA"
#######################
def test_appealType_ac1(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "DA",
        CCDAppealType = "EA",
        expected_appealType = "refusalOfEu",
        expected_hmctsCaseCategory = "EEA",
        expected_appealTypeDescription = "Refusal of application under the EEA regulations",
        expected_caseManagementCategory_label = "Refusal of application under the EEA regulations"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for DA EA " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for DA EA " + output_lines, test_from_state)
    
#######################
#AC2 - Given CasePrefix = "DC" and CCDAppealType = "DC"
#######################
def test_appealType_ac2(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "DC",
        CCDAppealType = "DC",
        expected_appealType = "deprivation",
        expected_hmctsCaseCategory = "DoC",
        expected_appealTypeDescription = "Deprivation of citizenship",
        expected_caseManagementCategory_label = "Deprivation of citizenship"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for DC DC " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for DC DC " +  output_lines, test_from_state)
        
#######################
#AC3 - Given CasePrefix = "EA" and CCDAppealType = "EA"
#######################
def test_appealType_ac3(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "EA",
        CCDAppealType = "EA",
        expected_appealType = "refusalOfEu",
        expected_hmctsCaseCategory = "EEA",
        expected_appealTypeDescription = "Refusal of application under the EEA regulations",
        expected_caseManagementCategory_label = "Refusal of application under the EEA regulations"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for EA EA " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for EA EA " + output_lines, test_from_state)
    
#######################
#AC4 - Given CasePrefix = "EA" and CCDAppealType = "EU"
#######################
def test_appealType_ac4(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "EA",
        CCDAppealType = "EU",
        expected_appealType = "euSettlementScheme",
        expected_hmctsCaseCategory = "EU Settlement Scheme",
        expected_appealTypeDescription = "EU Settlement Scheme",
        expected_caseManagementCategory_label = "EU Settlement Scheme"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed,  "Test for EA EU " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed,  "Test for EA EU " + output_lines, test_from_state)

#######################
#AC5 - Given CasePrefix = "HU" and CCDAppealType = "HU"
#######################
def test_appealType_ac5(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "HU",
        CCDAppealType = "HU",
        expected_appealType = "refusalOfHumanRights",
        expected_hmctsCaseCategory = "Human rights",
        expected_appealTypeDescription = "Refusal of a human rights claim",
        expected_caseManagementCategory_label = "Refusal of a human rights claim"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for HU HU " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for HU HU " + output_lines, test_from_state)

#######################
#AC6 - Given CasePrefix = "IA" and CCDAppealType = "HU"
#######################
def test_appealType_ac6(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "IA",
        CCDAppealType = "HU",
        expected_appealType = "refusalOfHumanRights",
        expected_hmctsCaseCategory = "Human rights",
        expected_appealTypeDescription = "Refusal of a human rights claim",
        expected_caseManagementCategory_label = "Refusal of a human rights claim"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for IA HU " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for IA HU " + output_lines, test_from_state)

#######################
#AC7 - Given CasePrefix = "LD" and CCDAppealType = "DC"
#######################
def test_appealType_ac7(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "LD",
        CCDAppealType = "DC",
        expected_appealType = "deprivation",
        expected_hmctsCaseCategory = "Human rights",
        expected_appealTypeDescription = "Deprivation of citizenship",
        expected_caseManagementCategory_label = "Deprivation of citizenship"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for LD DC " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for LD DC " + output_lines, test_from_state)
    
#######################
#AC8 - Given CasePrefix = "LE" and CCDAppealType = "EA"
#######################
def test_appealType_ac8(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "LE",
        CCDAppealType = "EA",
        expected_appealType = "refusalOfEu",
        expected_hmctsCaseCategory = "EEA",
        expected_appealTypeDescription = "Refusal of application under the EEA regulations",
        expected_caseManagementCategory_label = "Refusal of application under the EEA regulations"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for LE EA " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed,  "Test for LE EA " + output_lines, test_from_state)
    
#######################
#AC9 - Given CasePrefix = "LH" and CCDAppealType = "HU"
#######################
def test_appealType_ac9(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "LH",
        CCDAppealType = "HU",
        expected_appealType = "refusalOfHumanRights",
        expected_hmctsCaseCategory = "Human rights",
        expected_appealTypeDescription = "Refusal of a human rights claim",
        expected_caseManagementCategory_label = "Refusal of a human rights claim"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for LH HU " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for LH HU " + output_lines, test_from_state)
    
#######################
#AC10 - Given CasePrefix = "LH" and CCDAppealType = "HU"
#######################
def test_appealType_ac10(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "LP",
        CCDAppealType = "PA",
        expected_appealType = "protection",
        expected_hmctsCaseCategory = "Protection",
        expected_appealTypeDescription = "Refusal of a protection claim",
        expected_caseManagementCategory_label = "Refusal of a protection claim"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for LP PA " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for LP PA " + output_lines, test_from_state)

#######################
#AC11 - Given CasePrefix = "LR" and CCDAppealType = "RP"
#######################
def test_appealType_ac11(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "LR",
        CCDAppealType = "RP",
        expected_appealType = "revocationOfProtection",
        expected_hmctsCaseCategory = "Revocation",
        expected_appealTypeDescription = "Revocation of a protection status",
        expected_caseManagementCategory_label = "Revocation of a protection status"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for LR RP " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for LR RP " + output_lines, test_from_state)

#######################
#AC12 - Given CasePrefix = "PA" and CCDAppealType = "PA"
#######################
def test_appealType_ac12(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "PA",
        CCDAppealType = "PA",
        expected_appealType = "protection",
        expected_hmctsCaseCategory = "Protection",
        expected_appealTypeDescription = "Refusal of protection claim",
        expected_caseManagementCategory_label = "Refusal of protection claim"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for PA PA " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for PA PA " +  output_lines, test_from_state)
    
#######################
#AC13 - Given CasePrefix = "RP" and CCDAppealType = "RP"
#######################
def test_appealType_ac13(test_df):
    test_passed, output_lines, count = test_appealType_case_mapping(
        test_df,
        CasePrefix = "RP",
        CCDAppealType = "RP",
        expected_appealType = "protection",
        expected_hmctsCaseCategory = "Protection",
        expected_appealTypeDescription = "Refusal of protection claim",
        expected_caseManagementCategory_label = "Refusal of protection claim"
    )

    if test_passed == True:
        test_passed = "PASS"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for RP RP " + output_lines, test_from_state)
    else:
        test_passed = "FAIL"
        return TestResult("appealType, hmctsCaseCategory, appealTypeDescription, caseManagementCategory_label", test_passed, "Test for RP RP " + output_lines, test_from_state)
    
#######################
#appealReferenceNumber - If M1.CaseNo != appealReferenceNumber
#######################
def test_appealReferenceNumber_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("CaseNo").isNotNull()
        ).count() == 0:
        return TestResult("appealReferenceNumber", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac1_appealReferenceNumber = test_df.filter(
        col("CaseNo") != col("appealReferenceNumber")
    )    

    if ac1_appealReferenceNumber.count() != 0:
        return TestResult("appealReferenceNumber", "FAIL", f"appealReferenceNumber acceptance criteria failed: {str(ac1_appealReferenceNumber.count())} cases have been found where appealReferenceNumber & M1.CaseNo do not match.",test_from_state)
    else:
        return TestResult("appealReferenceNumber", "PASS", f"appealReferenceNumber acceptance criteria passed, all cases have matching appealReferenceNumber & M1.CaseNo rows.",test_from_state)

############################################################################################

#######################
#flagLabels Init Code
#######################
def test_flags_init(json, M1_bronze, C):
    try:
        json_flags = json.select(
            "AppealReferenceNumber",
            "caseFlags",
            "appellantLevelFlags",
            "isAriaMigratedFeeExemption"
        )

        C = C.select(
            "CaseNo",
            "CategoryId"
        )

        M1_flags = M1_bronze.select(
            "CaseNo",
            "CasePrefix"
        )

        json_flags = json_flags.join(
            C,
            json_flags.AppealReferenceNumber == C.CaseNo,
            "inner"
        )


        json_flags = json_flags.join(
            M1_flags,
            json_flags.AppealReferenceNumber == M1_flags.CaseNo,
            "inner"
        )

        test_df = (
            json_flags
            .groupBy("AppealReferenceNumber")
            .agg(
                collect_list("CategoryId").alias("CategoryIds"),
                first("caseFlags").alias("caseFlags"),
                first("appellantLevelFlags").alias("appellantLevelFlags"),
                first("isAriaMigratedFeeExemption").alias("isAriaMigratedFeeExemption"),
                first("CasePrefix").alias("CasePrefix")
            )
        )

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("flagLabels", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state)
    
#######################
# caseFlags Test
#######################
def test_caseFlags(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("caseFlags").isNotNull())
        ).count() == 0:
        return TestResult("caseFlags", "FAIL", "NO RECORDS TO TEST",test_from_state)

    # caseFlag setup
    case_flag_lookup = {
        7:  {"name": "Urgent case", "code": "CF0007", "comment": None, "hearing": "No"},
        41: {"name": "Other", "code": "OT0001", "comment": "Expedite", "hearing": "Yes"},
        31: {"name": "Other", "code": "OT0001", "comment": "Expedite", "hearing": "Yes"},
        32: {"name": "Other", "code": "OT0001", "comment": "Expedite", "hearing": "Yes"},
        8:  {"name": "Other", "code": "OT0001", "comment": "Reclassified RFT", "hearing": "Yes"},
        24: {"name": "Other", "code": "OT0001", "comment": "EEA Family Permit", "hearing": "Yes"},
        25: {"name": "RRO (Restricted Reporting Order / Anonymisation)", "code": "CF0012", "comment": None, "hearing": "Yes"}
    }

    exploded_caseflags = (
    test_df.select(
        "AppealReferenceNumber",
        "CategoryIds",
        col("caseFlags.details")[0].alias("caseflag_detail_0")
    ).select(
        "AppealReferenceNumber",
        "CategoryIds",
        col("caseflag_detail_0.value.name").alias("caseflag_name"),
        col("caseflag_detail_0.value.flagCode").alias("caseflag_code"),
        col("caseflag_detail_0.value.flagComment").alias("caseflag_comment"),
        col("caseflag_detail_0.value.hearingRelevant").alias("caseflag_hearing")
    ))

    # Test logic
    combine_results = lambda df1, df2: df1.union(df2)
    case_flag_mismatch_dfs = []
    for categoryid, expected in case_flag_lookup.items():
        mismatch_df = exploded_caseflags.filter(
            array_contains(col("CategoryIds"), lit(categoryid)) &
            (
                (col("caseflag_name") != lit(expected["name"])) |
                (col("caseflag_code") != lit(expected["code"])) |
                (col("caseflag_comment").cast("string") != lit(expected["comment"]).cast("string")) | 
                (col("caseflag_hearing") != lit(expected["hearing"]))  
            )
        )
        case_flag_mismatch_dfs.append(mismatch_df)

    ac_case_flag = reduce(combine_results, case_flag_mismatch_dfs)

    # Output
    if ac_case_flag.count() != 0:
        return TestResult("caseFlags","FAIL", f"caseFlags acceptance criteria failed: {str(ac_case_flag.count())} cases have been found where the caseFlag details seem to be mapped incorrectly.",test_from_state)
    else:
        return TestResult("caseFlags","PASS", f"caseFlags acceptance criteria passed, all the caseFlag details are mapped correctly.",test_from_state)


#######################
# appellantFlags Test
#######################
def test_appellantFlags(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("appellantLevelFlags").isNotNull())
        ).count() == 0:
        return TestResult("appellantLevelFlags", "FAIL", "NO RECORDS TO TEST",test_from_state)

    # appellantFlag setup
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

    exploded_appellantflags = (
    test_df.select(
        "AppealReferenceNumber",
        "CategoryIds",
        col("appellantLevelFlags.details")[0].alias("appellantflag_detail_0")
    ).select(
        "AppealReferenceNumber",
        "CategoryIds",
        col("appellantflag_detail_0.value.name").alias("appellantflag_name"),
        col("appellantflag_detail_0.value.flagCode").alias("appellantflag_code"),
        col("appellantflag_detail_0.value.hearingRelevant").alias("appellantflag_hearing")
    ))

    # Test logic
    combine_results = lambda df1, df2: df1.union(df2)
    appellant_flag_mismatch_dfs = []
    for categoryid, expected in appellant_flag_lookup.items():
        mismatch_df = exploded_appellantflags.filter(
            array_contains(col("CategoryIds"), lit(categoryid)) &
            (
                (col("appellantflag_name") != lit(expected["name"])) |
                (col("appellantflag_code") != lit(expected["code"])) |
                (col("appellantflag_hearing") != lit(expected["hearing"]))  
            )
        )
        appellant_flag_mismatch_dfs.append(mismatch_df)

    ac_appellant_flag = reduce(combine_results, appellant_flag_mismatch_dfs)

    # Output
    if ac_appellant_flag.count() != 0:
        return TestResult("appellantLevelFlags","FAIL", f"appellantLevelFlags acceptance criteria failed: {str(ac_appellant_flag.count())} cases have been found where the appellantLevelFlags details seem to be mapped incorrectly.",test_from_state)
    else:
        return TestResult("appellantLevelFlags","PASS", f"appellantLevelFlags acceptance criteria passed, all the appellantLevelFlags details are mapped correctly.",test_from_state)

#######################
# isAriaMigratedFeeExemption test 1 - If M1.CasePrefix = DA and isAriaMigratedFeeExemption != Yes
#######################
def test_isAriaMigratedFeeExemption_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("CasePrefix") == "DA")
        ).count() == 0:
        return TestResult("isAriaMigratedFeeExemption", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac1_isAriaMigratedFeeExemption = test_df.filter(
    (col("CasePrefix") == "DA") &
    (col("isAriaMigratedFeeExemption") != "Yes")
    )

    if ac1_isAriaMigratedFeeExemption.count() != 0:
        return TestResult("isAriaMigratedFeeExemption", "FAIL", f"isAriaMigratedFeeExemption acceptance criteria failed: {str(ac1_isAriaMigratedFeeExemption.count())} cases have been found where CasePrefix is 'DA' and isAriaMigratedFeeExemption is not Yes.",test_from_state)
    else:
        return TestResult("isAriaMigratedFeeExemption", "PASS", f"isAriaMigratedFeeExemption acceptance criteria passed, all cases where CasePrefix is 'DA' have isAriaMigratedFeeExemption equal to Yes.",test_from_state)
    
#######################
# isAriaMigratedFeeExemption test 2 - If M1.CasePrefix != DA and isAriaMigratedFeeExemption != No
#######################
def test_isAriaMigratedFeeExemption_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("CasePrefix") != "DA")
        ).count() == 0:
        return TestResult("isAriaMigratedFeeExemption", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac2_isAriaMigratedFeeExemption = test_df.filter(
    (col("CasePrefix") != "DA") &
    (col("isAriaMigratedFeeExemption") != "No")
    )

    if ac2_isAriaMigratedFeeExemption.count() != 0:
        return TestResult("isAriaMigratedFeeExemption", "FAIL", f"isAriaMigratedFeeExemption acceptance criteria failed: {str(ac2_isAriaMigratedFeeExemption.count())} cases have been found where CasePrefix is not 'DA' and isAriaMigratedFeeExemption is not No.",test_from_state)
    else:
        return TestResult("isAriaMigratedFeeExemption", "PASS", f"isAriaMigratedFeeExemption acceptance criteria passed, all cases where CasePrefix is not 'DA' have isAriaMigratedFeeExemption equal to No.",test_from_state)

############################################################################################
##############################################

#######################
#appellantDetails Init Code
#######################
def test_appellantdetails_init(json, M2_bronze, M1_bronze, C):
    try:
        # 1. Prepare DataFrames
        json_df = json.select(
            "AppealReferenceNumber", "appellantFamilyName", "appellantGivenNames",
            "appellantNameForDisplay", "appellantDateOfBirth", "isAppellantMinor",
            "caseNameHmctsInternal", "hmctsCaseNameInternal", "internalAppellantEmail",
            "email", 
            # "internalAppellantMobileNumber", "mobileNumber", 
            "appellantInUk", "appealOutOfCountry", "oocAppealAdminJ",
            "appellantAddress", "addressLine1AdminJ", "addressLine2AdminJ",
            "addressLine3AdminJ", "addressLine4AdminJ", "countryGovUkOocAdminJ",
            "appellantStateless", "appellantNationalities", 
            "appellantNationalitiesDescription", "deportationOrderOptions"
        )

        # 2. Prepare Join Tables (Renaming or selecting specific keys)
        c_df = C.select(col("CaseNo").alias("C_CaseNo"), "CategoryId")
        
        m2_df = M2_bronze.select(
            col("CaseNo").alias("M2_CaseNo"), 
            "Appellant_Name", "Appellant_Forenames", "BirthDate", "FCONumber", "Appellant_Email", 
            "Appellant_Telephone", "Appellant_Address1", "Appellant_Address2", "Appellant_Address3",
            "Appellant_Address4", "Appellant_Address5", "Appellant_Postcode", "AppellantCountryId"
        )

        m1_df = M1_bronze.select(
            col("CaseNo").alias("M1_CaseNo"), 
            "CasePrefix", "HORef", "DateLodged", "NationalityId", "DeportationDate", "RemovalDate"
        )

        # 3. Perform Joins using the unique aliases to avoid ambiguity
        # Join M2
        combined_df = json_df.join(
            m2_df, 
            json_df.AppealReferenceNumber == m2_df.M2_CaseNo, 
            how="inner"
        )

        # Join M1
        combined_df = combined_df.join(
            m1_df, 
            combined_df.AppealReferenceNumber == m1_df.M1_CaseNo, 
            how="inner"
        )

        # Join C (Critical for CategoryIds!)
        combined_df = combined_df.join(
            c_df, 
            combined_df.AppealReferenceNumber == c_df.C_CaseNo, 
            how="inner"
        )

        # 4. Aggregate
        test_df = (
            combined_df
            .groupBy("AppealReferenceNumber")
            .agg(
                collect_list("CategoryId").alias("CategoryIds"),
                first("appellantFamilyName").alias("appellantFamilyName"),
                first("appellantGivenNames").alias("appellantGivenNames"),
                first("appellantNameForDisplay").alias("appellantNameForDisplay"),
                first("appellantDateOfBirth").alias("appellantDateOfBirth"),
                first("isAppellantMinor").alias("isAppellantMinor"),
                first("caseNameHmctsInternal").alias("caseNameHmctsInternal"),
                first("hmctsCaseNameInternal").alias("hmctsCaseNameInternal"),
                # first("internalAppellantMobileNumber").alias("internalAppellantMobileNumber"),
                # first("mobileNumber").alias("mobileNumber"),
                first("internalAppellantEmail").alias("internalAppellantEmail"),
                first("email").alias("email"),
                first("appellantInUk").alias("appellantInUk"),
                first("appealOutOfCountry").alias("appealOutOfCountry"),
                first("oocAppealAdminJ").alias("oocAppealAdminJ"),
                first("addressLine1AdminJ").alias("addressLine1AdminJ"),
                first("addressLine2AdminJ").alias("addressLine2AdminJ"),
                first("addressLine3AdminJ").alias("addressLine3AdminJ"),
                first("addressLine4AdminJ").alias("addressLine4AdminJ"),
                first("countryGovUkOocAdminJ").alias("countryGovUkOocAdminJ"),
                first("appellantStateless").alias("appellantStateless"),
                first("appellantNationalities").alias("appellantNationalities"),
                first("appellantNationalitiesDescription").alias("appellantNationalitiesDescription"),
                first("deportationOrderOptions").alias("deportationOrderOptions"),
                first("CasePrefix").alias("CasePrefix"),
                first("HORef").alias("HORef"),
                first("Appellant_Name").alias("Appellant_Name"),
                first("Appellant_Forenames").alias("Appellant_Forenames"),
                first("BirthDate").alias("BirthDate"),
                first("DateLodged").alias("DateLodged"),
                first("FCONumber").alias("FCONumber"),
                first("Appellant_Email").alias("Appellant_Email"),
                first("Appellant_Telephone").alias("Appellant_Telephone"),
                first("appellantAddress").alias("appellantAddress"),
                first("Appellant_Address1").alias("Appellant_Address1"),
                first("Appellant_Address2").alias("Appellant_Address2"),
                first("Appellant_Address3").alias("Appellant_Address3"),
                first("Appellant_Address4").alias("Appellant_Address4"),
                first("Appellant_Address5").alias("Appellant_Address5"),
                first("Appellant_Postcode").alias("Appellant_Postcode"),
                first("AppellantCountryId").alias("AppellantCountryId"),
                first("NationalityId").alias("NationalityId"),
                first("DeportationDate").alias("DeportationDate"),
                first("RemovalDate").alias("RemovalDate")
            )
        )
        
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("appellantDetails", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state)
    
#######################
#appellantFamilyName
#######################
def test_appellantFamilyName(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("Appellant_Name").isNotNull())
        ).count() == 0:
        return TestResult("appellantFamilyName", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantFamilyName = test_df.filter(
    col("Appellant_Name") != col("appellantFamilyName")
    )

    if ac_appellantFamilyName.count() != 0:
        return TestResult("appellantFamilyName", "FAIL", f"appellantFamilyName acceptance criteria failed: {str(ac_appellantFamilyName.count())} cases have been found where appellantFamilyName and M2.Appellant_Name do not match.",test_from_state)
    else:
        return TestResult("appellantFamilyName", "PASS", f"appellantFamilyName acceptance criteria passed, all cases have matching appellantFamilyName and M2.Appellant_Name.",test_from_state)

#######################
#appellantGivenNames
#######################
def test_appellantGivenNames(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("Appellant_Forenames").isNotNull())
        ).count() == 0:
        return TestResult("appellantGivenNames", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantGivenNames = test_df.filter(
    col("Appellant_Forenames") != col("appellantGivenNames")
    )

    if ac_appellantGivenNames.count() != 0:
        return TestResult("appellantGivenNames", "FAIL", f"appellantGivenNames acceptance criteria failed: {str(ac_appellantGivenNames.count())} cases have been found where appellantGivenNames and M2.Appellant_Name do not match.",test_from_state)
    else:
        return TestResult("appellantGivenNames", "PASS", f"appellantGivenNames acceptance criteria passed, all cases have matching appellantGivenNames and M2.Appellant_Name.",test_from_state)

#######################
#appellantNameForDisplay 
#######################
def test_appellantNameForDisplay(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("Appellant_Forenames").isNotNull()) &
        (col("Appellant_Name").isNotNull())
        ).count() == 0:
        return TestResult("appellantNameForDisplay", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantNameForDisplay = test_df.filter(
        col("appellantNameForDisplay") != (col("Appellant_Forenames") + col("Appellant_Name"))
    )

    if ac_appellantNameForDisplay.count() != 0:
        return TestResult("appellantNameForDisplay", "FAIL", f"appellantNameForDisplay acceptance criteria failed: {str(ac_appellantNameForDisplay.count())} cases have been found where appellantNameForDisplay does not equal M2.Appellant_Forenames + M2.Appellant_Name do not match.",test_from_state)
    else:
        return TestResult("appellantNameForDisplay", "PASS", f"appellantNameForDisplay acceptance criteria passed, all cases have matching appellantGivenNames and M2.Appellant_Forenames + M2.Appellant_Name.",test_from_state)

#######################
#appellantDateOfBirth
#######################
def test_appellantDateOfBirth(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("BirthDate").isNotNull())
        ).count() == 0:
        return TestResult("appellantDateOfBirth", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantNameForDisplay = test_df.filter(
        col("appellantDateOfBirth") != (col("BirthDate"))
    )

    if ac_appellantNameForDisplay.count() != 0:
        return TestResult("appellantDateOfBirth", "FAIL", f"appellantDateOfBirth acceptance criteria failed: {str(ac_appellantNameForDisplay.count())} cases have been found where appellantNameForDisplay and M2.BirthDate do not match.",test_from_state)
    else:
        return TestResult("appellantDateOfBirth", "PASS", f"appellantDateOfBirth acceptance criteria passed, all cases have matching appellantGivenNames and M2.BirthDate.",test_from_state)

#######################
#isAppellantMinor
#######################
def test_isAppellantMinor(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("BirthDate").isNotNull()) &
        (col("DateLodged").isNotNull())
        ).count() == 0:
        return TestResult("isAppellantMinor", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    # Step 1 — basic year difference
    year_difference = year(col("DateLodged")) - year(col("BirthDate"))

    # Step 2 — check if birthday hasn't happened yet in the appeal year
    birthday_not_this_year = (
        (month(col("DateLodged")) < month(col("BirthDate"))) |
        (
            (month(col("DateLodged")) == month(col("BirthDate"))) &
            (dayofmonth(col("DateLodged")) < dayofmonth(col("BirthDate")))
        )
    )

    # Step 3 — adjust year_diff by subtracting 1 if birthday_not_yet is True
    age_exact = year_difference - when(birthday_not_this_year, 1).otherwise(0)

    test_df = test_df.withColumn("CalculatedAge", age_exact)

    test_df = test_df.filter(
        ((col("isAppellantMinor") == "No")  & (age_exact < 18)) |
        ((col("isAppellantMinor") == "Yes") & (age_exact >= 18))
    )

    ac_isAppellantMinor = test_df.select(countDistinct("appealReferenceNumber")).first()[0]

    if ac_isAppellantMinor != 0:
        return TestResult("isAppellantMinor", "FAIL", f"isAppellantMinor acceptance criteria failed: {str(ac_isAppellantMinor)} cases have been found where appellants under 18 may have been marked as not minor, or some appellants over 18 have been marked as minor.",test_from_state)
    else:
        return TestResult("isAppellantMinor", "PASS", f"isAppellantMinor acceptance criteria passed, every appellant under/over 18 years of age at the time of appeal case creation has isAppellantMinor = Yes/No respectively.",test_from_state)

#######################
#caseNameHmctsInternal 
#######################
def test_caseNameHmctsInternal(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("Appellant_Forenames").isNotNull()) &
        (col("Appellant_Name").isNotNull())
        ).count() == 0:
        return TestResult("caseNameHmctsInternal", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_caseNameHmctsInternal = test_df.filter(
        col("caseNameHmctsInternal") != (col("Appellant_Forenames") + col("Appellant_Name"))
    )

    if ac_caseNameHmctsInternal.count() != 0:
        return TestResult("caseNameHmctsInternal", "FAIL", f"caseNameHmctsInternal acceptance criteria failed: {str(ac_caseNameHmctsInternal.count())} cases have been found where caseNameHmctsInternal does not equal M2.Appellant_Forenames + M2.Appellant_Name do not match.",test_from_state)
    else:
        return TestResult("caseNameHmctsInternal", "PASS", f"caseNameHmctsInternal acceptance criteria passed, all cases have matching appellantGivenNames and M2.Appellant_Forenames + M2.Appellant_Name.",test_from_state)

#######################
#hmctsCaseNameInternal 
#######################
def test_hmctsCaseNameInternal(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("Appellant_Forenames").isNotNull()) &
        (col("Appellant_Name").isNotNull())
        ).count() == 0:
        return TestResult("hmctsCaseNameInternal", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_hmctsCaseNameInternal = test_df.filter(
        col("hmctsCaseNameInternal") != (col("Appellant_Forenames") + col("Appellant_Name"))
    )

    if ac_hmctsCaseNameInternal.count() != 0:
        return TestResult("hmctsCaseNameInternal", "FAIL", f"hmctsCaseNameInternal acceptance criteria failed: {str(ac_hmctsCaseNameInternal.count())} cases have been found where hmctsCaseNameInternal does not equal M2.Appellant_Forenames + M2.Appellant_Name do not match.",test_from_state)
    else:
        return TestResult("hmctsCaseNameInternal", "PASS", f"hmctsCaseNameInternal acceptance criteria passed, all cases have matching appellantGivenNames and M2.Appellant_Forenames + M2.Appellant_Name.",test_from_state)

#######################
#internalAppellantEmail - If M2.AppellantEmail is not Null and internalAppellantEmail != M2.AppellantEmail
#######################
def test_internalAppellantEmail_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("Appellant_Email").isNotNull())
        ).count() == 0:
        return TestResult("internalAppellantEmail", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_internalAppellantEmail = test_df.filter(
        (col("Appellant_Email").isNotNull()) & (~col("internalAppellantEmail").eqNullSafe(col("Appellant_Email")))
    )

    if ac_internalAppellantEmail.count() != 0:
        return TestResult("internalAppellantEmail", "FAIL", f"internalAppellantEmail acceptance criteria failed: {str(ac_internalAppellantEmail.count())} cases have been found where internalAppellantEmail does not equal M2.Appellant_Email" ,test_from_state)
    else:
        return TestResult("internalAppellantEmail", "PASS", f"internalAppellantEmail acceptance criteria passed, all cases have matching internalAppellantEmail and M2.Appellant_Email.",test_from_state)

def test_internalAppellantEmail_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("Appellant_Email").isNull())
        ).count() == 0:
        return TestResult("internalAppellantEmail", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_internalAppellantEmail = test_df.filter(
        (col("Appellant_Email").isNull()) & (~col("internalAppellantEmail").eqNullSafe(col("Appellant_Email")))
    )

    if ac_internalAppellantEmail.count() != 0:
        return TestResult("internalAppellantEmail", "FAIL", f"internalAppellantEmail acceptance criteria failed: {str(ac_internalAppellantEmail.count())} cases have been found where internalAppellantEmail does not equal M2.Appellant_Email" ,test_from_state)
    else:
        return TestResult("internalAppellantEmail", "PASS", f"internalAppellantEmail acceptance criteria passed, all cases have matching internalAppellantEmail and M2.Appellant_Email.",test_from_state)

#######################
#email
#######################
def test_email_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("Appellant_Email").isNotNull())
        ).count() == 0:
        return TestResult("internalAppellantEmail", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_email = test_df.filter(
        (col("Appellant_Email").isNotNull()) & (~col("internalAppellantEmail").eqNullSafe(col("email")))
    )

    if ac_email.count() != 0:
        return TestResult("email", "FAIL", f"email acceptance criteria failed: {str(ac_email.count())} cases have been found where email does not equal M2.Appellant_Email" ,test_from_state)
    else:
        return TestResult("email", "PASS", f"email acceptance criteria passed, all cases have matching email and M2.Appellant_Email.",test_from_state)
    
def test_email_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("Appellant_Email").isNull())
        ).count() == 0:
        return TestResult("internalAppellantEmail", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_email = test_df.filter(
        (col("Appellant_Email").isNull()) & (~col("internalAppellantEmail").eqNullSafe(col("email")))
    )

    if ac_email.count() != 0:
        return TestResult("email", "FAIL", f"email acceptance criteria failed: {str(ac_email.count())} cases have been found where email does not equal M2.Appellant_Email" ,test_from_state)
    else:
        return TestResult("email", "PASS", f"email acceptance criteria passed, all cases have matching email and M2.Appellant_Email.",test_from_state)

#######################
#internalAppellantMobileNumber
#######################
def test_internalAppellantMobileNumber_ac1(json, M2_bronze):
    try:
        json_df = json.select(
            "AppealReferenceNumber", "internalAppellantMobileNumber"
        )
        
        m2_df = M2_bronze.select(
            col("CaseNo").alias("M2_CaseNo"), 
            "Appellant_Telephone"
        )

        test_df = json_df.join(
            m2_df, 
            json_df.AppealReferenceNumber == m2_df.M2_CaseNo, 
            how="inner"
        )
        
        #Check we have Records To test
        if test_df.filter(
            (col("Appellant_Telephone").isNotNull())
            ).count() == 0:
            return TestResult("internalAppellantMobileNumber", "FAIL", "NO RECORDS TO TEST",test_from_state)
        
        ac_internalAppellantMobileNumber = test_df.filter(
            ~col("internalAppellantMobileNumber").eqNullSafe(col("Appellant_Telephone"))
        )

        if ac_internalAppellantMobileNumber.count() != 0:
            return TestResult("ac_internalAppellantMobileNumber", "FAIL", f"internalAppellantMobileNumber acceptance criteria failed: {str(ac_internalAppellantMobileNumber.count())} cases have been found where internalAppellantMobileNumber does not equal M2.Appellant_Telephone" ,test_from_state)
        else:
            return TestResult("internalAppellantMobileNumber", "PASS", f"internalAppellantMobileNumber acceptance criteria passed, all cases have matching ac_internalAppellantMobileNumber and M2.Appellant_Telephone.",test_from_state)
        
    except Exception as e:
        error_message = str(e)        
        return TestResult("internalAppellantMobileNumber", "FAIL",f"Failed to Setup Data for internalAppellantMobileNumber: Error : {error_message[:300]}",test_from_state)
    
def test_internalAppellantMobileNumber_ac2(json, M2_bronze):
    try:
        json_df = json.select(
            "AppealReferenceNumber", "internalAppellantMobileNumber"
        )
        
        m2_df = M2_bronze.select(
            col("CaseNo").alias("M2_CaseNo"), 
            "Appellant_Telephone"
        )

        test_df = json_df.join(
            m2_df, 
            json_df.AppealReferenceNumber == m2_df.M2_CaseNo, 
            how="inner"
        )
        
        #Check we have Records To test
        if test_df.filter(
            (col("Appellant_Telephone").isNull())
            ).count() == 0:
            return TestResult("internalAppellantMobileNumber", "FAIL", "NO RECORDS TO TEST",test_from_state)
        
        ac_internalAppellantMobileNumber = test_df.filter(
            (col("Appellant_Telephone").isNull()) & (col("internalAppellantMobileNumber").isNotNull())
        )

        if ac_internalAppellantMobileNumber.count() != 0:
            return TestResult("ac_internalAppellantMobileNumber", "FAIL", f"internalAppellantMobileNumber acceptance criteria failed: {str(ac_internalAppellantMobileNumber.count())} cases have been found where internalAppellantMobileNumber does not equal M2.Appellant_Telephone" ,test_from_state)
        else:
            return TestResult("internalAppellantMobileNumber", "PASS", f"internalAppellantMobileNumber acceptance criteria passed, all cases have matching ac_internalAppellantMobileNumber and M2.Appellant_Telephone.",test_from_state)
        
    except Exception as e:
        error_message = str(e)        
        return TestResult("internalAppellantMobileNumber", "FAIL",f"Failed to Setup Data for internalAppellantMobileNumber: Error : {error_message[:300]}",test_from_state)

#######################
#mobileNumber
#######################
def test_mobileNumber_ac1(json, M2_bronze):
    try:
        json_df = json.select(
            "AppealReferenceNumber", "mobileNumber"
        )
        
        m2_df = M2_bronze.select(
            col("CaseNo").alias("M2_CaseNo"), 
            "Appellant_Telephone"
        )

        test_df = json_df.join(
            m2_df, 
            json_df.AppealReferenceNumber == m2_df.M2_CaseNo, 
            how="inner"
        )
        
        #Check we have Records To test
        if test_df.filter(
            (col("Appellant_Telephone").isNotNull())
            ).count() == 0:
            return TestResult("mobileNumber", "FAIL", "NO RECORDS TO TEST",test_from_state)
        
        ac_mobileNumber = test_df.filter(
            ~col("mobileNumber").eqNullSafe(col("Appellant_Telephone"))
        )

        if ac_mobileNumber.count() != 0:
            return TestResult("mobileNumber", "FAIL", f"mobileNumber acceptance criteria failed: {str(ac_mobileNumber.count())} cases have been found where mobileNumber does not equal M2.Appellant_Telephone" ,test_from_state)
        else:
            return TestResult("mobileNumber", "PASS", f"mobileNumber acceptance criteria passed, all cases have matching mobileNumber and M2.Appellant_Telephone.",test_from_state)
        
    except Exception as e:
        error_message = str(e)        
        return TestResult("mobileNumber", "FAIL",f"Failed to Setup Data for mobileNumber: Error : {error_message[:300]}",test_from_state)
    
def test_mobileNumber_ac2(json, M2_bronze):
    try:
        json_df = json.select(
            "AppealReferenceNumber", "mobileNumber"
        )
        
        m2_df = M2_bronze.select(
            col("CaseNo").alias("M2_CaseNo"), 
            "Appellant_Telephone"
        )

        test_df = json_df.join(
            m2_df, 
            json_df.AppealReferenceNumber == m2_df.M2_CaseNo, 
            how="inner"
        )
        
        #Check we have Records To test
        if test_df.filter(
            (col("Appellant_Telephone").isNull())
            ).count() == 0:
            return TestResult("mobileNumber", "FAIL", "NO RECORDS TO TEST",test_from_state)
        
        ac_mobileNumber = test_df.filter(
            (col("Appellant_Telephone").isNull()) & (col("mobileNumber").isNotNull())
        )

        if ac_mobileNumber.count() != 0:
            return TestResult("mobileNumber", "FAIL", f"mobileNumber acceptance criteria failed: {str(ac_mobileNumber.count())} cases have been found where mobileNumber does not equal M2.Appellant_Telephone" ,test_from_state)
        else:
            return TestResult("mobileNumber", "PASS", f"mobileNumber acceptance criteria passed, all cases have matching mobileNumber and M2.Appellant_Telephone.",test_from_state)
        
    except Exception as e:
        error_message = str(e)        
        return TestResult("mobileNumber", "FAIL",f"Failed to Setup Data for mobileNumber: Error : {error_message[:300]}",test_from_state)

#######################
#appellantInUk - If CategoryId is 37 and appellantInUk != Yes
#######################
def test_appellantInUk_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 37))
        ).count() == 0:
        return TestResult("appellantInUk", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantInUk_ac1 = test_df.filter(
        (array_contains(col("CategoryIds"), 37)) & (col("appellantInUk") != "Yes")
    )

    if ac_appellantInUk_ac1.count() != 0:
        return TestResult("appellantInUk", "FAIL", f"appellantInUk acceptance criteria failed: {str(ac_appellantInUk_ac1.count())} cases have been found where CategoryId is 37 and appellantInUk != Yes" ,test_from_state)
    else:
        return TestResult("appellantInUk", "PASS", f"appellantInUk acceptance criteria passed, all cases where CategoryId is 37 have appellantInUk = Yes",test_from_state)
    
#######################
#appellantInUk - If CategoryId is 38 and appellantInUk != No
#######################
def test_appellantInUk_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38))
        ).count() == 0:
        return TestResult("appellantInUk", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantInUk_ac2 = test_df.filter(
        (array_contains(col("CategoryIds"), 38)) & (col("appellantInUk") != "No")
    )

    if ac_appellantInUk_ac2.count() != 0:
        return TestResult("appellantInUk", "FAIL", f"appellantInUk acceptance criteria failed: {str(ac_appellantInUk_ac2.count())} cases have been found where CategoryId is 37 and appellantInUk != No" ,test_from_state)
    else:
        return TestResult("appellantInUk", "PASS", f"appellantInUk acceptance criteria passed, all cases where CategoryId is 37 have appellantInUk = No",test_from_state)

#######################
#appealOutOfCountry
#######################
def test_appealOutOfCountry_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        ~(array_contains(col("CategoryIds"), 38))
        ).count() == 0:
        return TestResult("appealOutOfCountry", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appealOutOfCountry = test_df.filter(
        (~(array_contains(col("CategoryIds"), 38))) & (col("appealOutOfCountry").isNotNull())
    )

    if ac_appealOutOfCountry.count() != 0:
        return TestResult("appealOutOfCountry", "FAIL", f"appealOutOfCountry acceptance criteria failed: {str(ac_appealOutOfCountry.count())} cases have been found where CategoryId is not 38 and appealOutOfCountry is not omitted" ,test_from_state)
    else:
        return TestResult("appealOutOfCountry", "PASS", f"appealOutOfCountry acceptance criteria passed, where CategoryId is not 38, appealOutOfCountry is always omitted.",test_from_state)
    
def test_appealOutOfCountry_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        array_contains(col("CategoryIds"), 38)
        ).count() == 0:
        return TestResult("appealOutOfCountry", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appealOutOfCountry = test_df.filter(
        ((array_contains(col("CategoryIds"), 38))) & (col("appealOutOfCountry") != "Yes")
    )

    if ac_appealOutOfCountry.count() != 0:
        return TestResult("appealOutOfCountry", "FAIL", f"appealOutOfCountry acceptance criteria failed: {str(ac_appealOutOfCountry.count())} cases have been found where CategoryId is 38 and appealOutOfCountry is not Yes" ,test_from_state)
    else:
        return TestResult("appealOutOfCountry", "PASS", f"appealOutOfCountry acceptance criteria passed, where CategoryId is 38, appealOutOfCountry is always Yes.",test_from_state)
    
#######################
#oocAppealAdminJ - If CategoryId is not in 38 and oocAppealAdminJ not omitted
#######################
def test_oocAppealAdminJ_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        ~(array_contains(col("CategoryIds"), 38))
        ).count() == 0:
        return TestResult("oocAppealAdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_oocAppealAdminJ = test_df.filter(
        (~(array_contains(col("CategoryIds"), 38))) & (col("oocAppealAdminJ").isNotNull())
    )

    if ac_oocAppealAdminJ.count() != 0:
        return TestResult("oocAppealAdminJ", "FAIL", f"oocAppealAdminJ acceptance criteria failed: {str(ac_oocAppealAdminJ.count())} cases have been found where CategoryId is not 38 and oocAppealAdminJ is not omitted" ,test_from_state)
    else:
        return TestResult("oocAppealAdminJ", "PASS", f"oocAppealAdminJ acceptance criteria passed, where CategoryId is not 38, oocAppealAdminJ is always omitted.",test_from_state)

#######################
#oocAppealAdminJ - If CategoryId is in 38 + M1.HORef LIKE '%GWF%'  and  oocAppealAdminJ != ‘entryClearanceDecision’
#######################
def test_oocAppealAdminJ_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("HORef").contains("GWF"))
        ).count() == 0:
        return TestResult("oocAppealAdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_oocAppealAdminJ = test_df.filter(
    (
        (array_contains(col("CategoryIds"), 38)) &
        (col("HORef").contains("GWF"))
    ) & 
        (col("oocAppealAdminJ") != "entryClearanceDecision")
    )

    if ac_oocAppealAdminJ.count() != 0:
        return TestResult("oocAppealAdminJ", "FAIL", f"oocAppealAdminJ acceptance criteria failed: {str(ac_oocAppealAdminJ.count())} cases have been found where CategoryId is 38 + HORef contains GWF and oocAppealAdminJ is not 'entryClearanceDecision'." ,test_from_state)
    else:
        return TestResult("oocAppealAdminJ", "PASS", f"oocAppealAdminJ acceptance criteria passed, where CategoryId is 38 + HORef contains GWF, oocAppealAdminJ is always 'entryClearanceDecision'.",test_from_state)

#######################
#oocAppealAdminJ - If CategoryId is in 38 + M2.FCONumber LIKE '%GWF%'  and  oocAppealAdminJ != ‘entryClearanceDecision’
#######################
def test_oocAppealAdminJ_ac3(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("FCONumber").contains("GWF"))
        ).count() == 0:
        return TestResult("oocAppealAdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_oocAppealAdminJ = test_df.filter(
    (
        (array_contains(col("CategoryIds"), 38)) &
        (col("FCONumber").contains("GWF"))
    ) & 
        (col("oocAppealAdminJ") != "entryClearanceDecision")
    )

    if ac_oocAppealAdminJ.count() != 0:
        return TestResult("oocAppealAdminJ", "FAIL", f"oocAppealAdminJ acceptance criteria failed: {str(ac_oocAppealAdminJ.count())} cases have been found where CategoryId is 38 + FCONumber contains GWF and oocAppealAdminJ is not 'entryClearanceDecision'" ,test_from_state)
    else:
        return TestResult("oocAppealAdminJ", "PASS", f"oocAppealAdminJ acceptance criteria passed, where CategoryId is 38 + FCONumber contains GWF, oocAppealAdminJ is always 'entryClearanceDecision'.",test_from_state)

#######################
#oocAppealAdminJ - If CategoryId is in 38 + M1.HORef  NOT LIKE '%GWF%' & oocAppealAdminJ != ‘None
#######################
def test_oocAppealAdminJ_ac4(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38)) &
        ~(col("HORef").contains("GWF"))
        ).count() == 0:
        return TestResult("oocAppealAdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_oocAppealAdminJ = test_df.filter(
    (
        (array_contains(col("CategoryIds"), 38)) &
        (~(col("HORef").contains("GWF")))
    ) & 
        (col("oocAppealAdminJ") != "none")
    )

    if ac_oocAppealAdminJ.count() != 0:
        return TestResult("oocAppealAdminJ", "FAIL", f"oocAppealAdminJ acceptance criteria failed: {str(ac_oocAppealAdminJ.count())} cases have been found where CategoryId is 38 + HORef doesn't GWF and oocAppealAdminJ is not 'none''." ,test_from_state)
    else:
        return TestResult("oocAppealAdminJ", "PASS", f"oocAppealAdminJ acceptance criteria passed, where CategoryId is 38 + HORef doesn't contain GWF, oocAppealAdminJ is always 'none'.",test_from_state)

#######################
#oocAppealAdminJ - If CategoryId is in 38 + M2.FCONumber NOT LIKE '%GWF%' & oocAppealAdminJ != ‘None
#######################
def test_oocAppealAdminJ_ac5(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38)) &
        ~(col("FCONumber").contains("GWF"))
        ).count() == 0:
        return TestResult("oocAppealAdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_oocAppealAdminJ = test_df.filter(
    (
        (array_contains(col("CategoryIds"), 38)) &
        (~(col("FCONumber").contains("GWF")))
    ) & 
        (col("oocAppealAdminJ") != "none")
    )

    if ac_oocAppealAdminJ.count() != 0:
        return TestResult("oocAppealAdminJ", "FAIL", f"oocAppealAdminJ acceptance criteria failed: {str(ac_oocAppealAdminJ.count())} cases have been found where CategoryId is 38 + FCONumber doesn't contain GWF and oocAppealAdminJ is not 'none'" ,test_from_state)
    else:
        return TestResult("oocAppealAdminJ", "PASS", f"oocAppealAdminJ acceptance criteria passed, where CategoryId is 38 + FCONumber doesn't contain GWF, oocAppealAdminJ is always 'none'.",test_from_state)

#######################
#appellantAddress - If CategoryId not in 37 and appellantAddress not omitted
#######################
def test_appellantAddress_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        ~(array_contains(col("CategoryIds"), 37))
        ).count() == 0:
        return TestResult("appellantAddress", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantAddress= test_df.filter(
        (~(array_contains(col("CategoryIds"), 37))) & (col("appellantAddress").isNotNull())
    )

    if ac_appellantAddress.count() != 0:
        return TestResult("appellantAddress", "FAIL", f"appellantAddress acceptance criteria failed: {str(ac_appellantAddress.count())} cases have been found where CategoryId is not 37 and appellantAddress is not omitted" ,test_from_state)
    else:
        return TestResult("appellantAddress", "PASS", f"appellantAddress acceptance criteria passed, where CategoryId is not 37, appellantAddress is always omitted.",test_from_state)

#######################
#appellantAddress - Check appellantAddress is mapped with the correct ARIA data
#######################
def test_appellantAddress_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("appellantAddress").isNotNull())
        ).count() == 0:
        return TestResult("appellantAddress", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantAddress= test_df.filter(
        (~(col("appellantAddress.AddressLine1").isin(col("Appellant_Address1")))) &
        (~(col("appellantAddress.AddressLine2").isin(col("Appellant_Address2")))) &
        (~(col("appellantAddress.PostTown").isin(col("Appellant_Address3")))) &
        (~(col("appellantAddress.County").isin(col("Appellant_Address4")))) &
        (~(col("appellantAddress.Country").isin(col("Appellant_Address5")))) &
        (~(col("appellantAddress.PostCode").isin(col("Appellant_PostCode"))))
    )

    if ac_appellantAddress.count() != 0:
        return TestResult("appellantAddress", "FAIL", f"appellantAddress acceptance criteria failed: {str(ac_appellantAddress.count())} cases have been found where appellantAddress is not structured with the correct Appellant_Address ARIA fields." ,test_from_state)
    else:
        return TestResult("appellantAddress", "PASS", f"appellantAddress acceptance criteria passed, all cases have an appellantAddress which is structured with the correct Appellant_Address ARIA fields.",test_from_state)

#######################
#addressLine1AdminJ - If CategoryId not in 38 and addressLine1AdminJ not omitted
#######################
def test_addressLine1AdminJ_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (~(array_contains(col("CategoryIds"), 38)))
        ).count() == 0:
        return TestResult("addressLine1AdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_addressLine1AdminJ = test_df.filter(
        (~(array_contains(col("CategoryIds"), 38))) & (col("addressLine1AdminJ").isNotNull())
    )

    if ac_addressLine1AdminJ.count() != 0:
        return TestResult("addressLine1AdminJ", "FAIL", f"addressLine1AdminJ acceptance criteria failed: {str(ac_addressLine1AdminJ.count())} cases have been found where CategoryId is not 38 and addressLine1AdminJ is not omitted" ,test_from_state)
    else:
        return TestResult("addressLine1AdminJ", "PASS", f"addressLine1AdminJ acceptance criteria passed, all cases where CategoryId is not 38 have addressLine1AdminJ omitted",test_from_state)

#######################
#addressLine1AdminJ - If CategoryId in 38 and addressLine1AdminJ != AppellantAddress1
#######################
def test_addressLine1AdminJ_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("addressLine1AdminJ").isNotNull())
        ).count() == 0:
        return TestResult("addressLine1AdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_addressLine1AdminJ = test_df.filter(
        (array_contains(col("CategoryIds"), 38)) & 
    (
        col("addressLine1AdminJ") != col("Appellant_Address1")
    )
    )

    if ac_addressLine1AdminJ.count() != 0:
        return TestResult("addressLine1AdminJ", "FAIL", f"addressLine1AdminJ acceptance criteria failed: {str(ac_addressLine1AdminJ.count())} cases have been found where CategoryId is 38 and addressLine1AdminJ does not match Appellant_Address1" ,test_from_state)
    else:
        return TestResult("addressLine1AdminJ", "PASS", f"addressLine1AdminJ acceptance criteria passed, all cases where CategoryId is 38 have addressLine1AdminJ matching Appellant_Address1",test_from_state)

#######################
#addressLine2AdminJ - If CategoryId not in 38 and addressLine2AdminJ not omitted
#######################
def test_addressLine2AdminJ_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (~(array_contains(col("CategoryIds"), 38)))
        ).count() == 0:
        return TestResult("addressLine2AdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_addressLine2AdminJ = test_df.filter(
        (~(array_contains(col("CategoryIds"), 38))) & (col("addressLine2AdminJ").isNotNull())
    )

    if ac_addressLine2AdminJ.count() != 0:
        return TestResult("addressLine2AdminJ", "FAIL", f"addressLine2AdminJ acceptance criteria failed: {str(ac_addressLine2AdminJ.count())} cases have been found where CategoryId is not 38 and addressLine2AdminJ is not omitted" ,test_from_state)
    else:
        return TestResult("addressLine2AdminJ", "PASS", f"addressLine2AdminJ acceptance criteria passed, all cases where CategoryId is not 38 have addressLine2AdminJ omitted",test_from_state)
    
#######################
#addressLine2AdminJ - correct mapping and should not be null
#######################
def test_addressLine2AdminJ_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38)) 
        ).count() == 0:
        return TestResult("addressLine2AdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_addressLine2AdminJ = test_df.filter(
    (array_contains(col("CategoryIds"), 38)) & 
    (
        (~col("addressLine2AdminJ").eqNullSafe(
            coalesce(
                col("Appellant_Address2"),
                col("Appellant_Address3"),
                col("Appellant_Address4"),
                col("Appellant_Address5"),
                col("Appellant_Postcode")
            )
        )) | 
        (col("addressLine2AdminJ").isNull())
    )
    )

    if ac_addressLine2AdminJ.count() != 0:
        return TestResult("addressLine2AdminJ", "FAIL", f"addressLine2AdminJ acceptance criteria failed: {str(ac_addressLine2AdminJ.count())} cases have been found where addressLine2AdminJ is not mapped correctly." ,test_from_state)
    else:
        return TestResult("addressLine2AdminJ", "PASS", f"addressLine2AdminJ acceptance criteria passed, all cases where addressLine2AdminJ is not mapped correctly.",test_from_state)

#######################
#addressLine3AdminJ - If CategoryId not in 38 and addressLine3AdminJ not omitted
#######################
def test_addressLine3AdminJ_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (~(array_contains(col("CategoryIds"), 38)))
        ).count() == 0:
        return TestResult("addressLine3AdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_addressLine3AdminJ = test_df.filter(
        (~(array_contains(col("CategoryIds"), 38))) & (col("addressLine3AdminJ").isNotNull())
    )

    if ac_addressLine3AdminJ.count() != 0:
        return TestResult("addressLine3AdminJ", "FAIL", f"addressLine3AdminJ acceptance criteria failed: {str(ac_addressLine3AdminJ.count())} cases have been found where CategoryId is not 38 and addressLine3AdminJ is not omitted" ,test_from_state)
    else:
        return TestResult("addressLine3AdminJ", "PASS", f"addressLine3AdminJ acceptance criteria passed, all cases where CategoryId is not 38 have addressLine3AdminJ omitted",test_from_state)

#######################
#addressLine3AdminJ - If CategoryId in 38 and addressLine3AdminJ != AppellantAddress3 + ', ' + AppellantAddress4
#######################
def test_addressLine3AdminJ_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38))
        ).count() == 0:
        return TestResult("addressLine3AdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_addressLine3AdminJ = test_df.filter(
        (array_contains(col("CategoryIds"), 38)) & 
    (
        col("addressLine3AdminJ") != (col("Appellant_Address3") + col("Appellant_Address4"))
    )
    )

    if ac_addressLine3AdminJ.count() != 0:
        return TestResult("addressLine3AdminJ", "FAIL", f"addressLine3AdminJ acceptance criteria failed: {str(ac_addressLine3AdminJ.count())} cases have been found where CategoryId is 38 and addressLine3AdminJ is not equal to AppellantAddress3 + ', ' + AppellantAddress4" ,test_from_state)
    else:
        return TestResult("addressLine3AdminJ", "PASS", f"addressLine3AdminJ acceptance criteria passed, all cases where CategoryId is 38 have addressLine3AdminJ equal to AppellantAddress3 + ', ' + AppellantAddress4",test_from_state)

#######################
#addressLine4AdminJ - If CategoryId not in 38 and addressLine3AdminJ not omitted
#######################
def test_addressLine4AdminJ_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (~(array_contains(col("CategoryIds"), 38)))
        ).count() == 0:
        return TestResult("addressLine4AdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_addressLine4AdminJ = test_df.filter(
        (~(array_contains(col("CategoryIds"), 38))) & (col("addressLine4AdminJ").isNotNull())
    )

    if ac_addressLine4AdminJ.count() != 0:
        return TestResult("addressLine4AdminJ", "FAIL", f"addressLine4AdminJ acceptance criteria failed: {str(ac_addressLine4AdminJ.count())} cases have been found where CategoryId is not 38 and addressLine4AdminJ is not omitted" ,test_from_state)
    else:
        return TestResult("addressLine4AdminJ", "PASS", f"addressLine4AdminJ acceptance criteria passed, all cases where CategoryId is not 38 have addressLine4AdminJ omitted",test_from_state)

#######################
#addressLine4AdminJ - If CategoryId in 38 and addressLine4AdminJ != AppellantAddress4 + ', ' + AppellantPostcode
#######################
def test_addressLine4AdminJ_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38))
        ).count() == 0:
        return TestResult("addressLine4AdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_addressLine4AdminJ = test_df.filter(
        (array_contains(col("CategoryIds"), 38)) & 
    (
        col("addressLine4AdminJ") != (col("Appellant_Address4") + col("Appellant_Postcode"))
    )
    )

    if ac_addressLine4AdminJ.count() != 0:
        return TestResult("addressLine4AdminJ", "FAIL", f"addressLine4AdminJ acceptance criteria failed: {str(ac_addressLine4AdminJ.count())} cases have been found where CategoryId is 38 and addressLine4AdminJ is not equal to AppellantAddress3 + ', ' + AppellantPostcode" ,test_from_state)
    else:
        return TestResult("addressLine4AdminJ", "PASS", f"addressLine4AdminJ acceptance criteria passed, all cases where CategoryId is 38 have addressLine4AdminJ equal to AppellantAddress4 + ', ' + AppellantPostcode",test_from_state)

#######################
#countryGovUkOocAdminJ - If CategoryId not in 38 and addressLine3AdminJ not omitted
#######################
def test_countryGovUkOocAdminJ_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (~(array_contains(col("CategoryIds"), 38)))
        ).count() == 0:
        return TestResult("countryGovUkOocAdminJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_countryGovUkOocAdminJ = test_df.filter(
        (~(array_contains(col("CategoryIds"), 38))) & (col("countryGovUkOocAdminJ").isNotNull())
    )

    if ac_countryGovUkOocAdminJ.count() != 0:
        return TestResult("countryGovUkOocAdminJ", "FAIL", f"countryGovUkOocAdminJ acceptance criteria failed: {str(ac_countryGovUkOocAdminJ.count())} cases have been found where CategoryId is not 38 and countryGovUkOocAdminJ is not omitted" ,test_from_state)
    else:
        return TestResult("countryGovUkOocAdminJ", "PASS", f"countryGovUkOocAdminJ acceptance criteria passed, all cases where CategoryId is not 38 have countryGovUkOocAdminJ omitted",test_from_state)
    
#######################
#appellantStateless - If M2.AppellantCountryId is 211 and appellantStateless is not isStateless
#######################
def test_appellantStateless_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("AppellantCountryId") == "211")
        ).count() == 0:
        return TestResult("appellantStateless", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantStateless = test_df.filter(
        (col("AppellantCountryId") == "211") & (col("appellantStateless") != "isStateless")
    )

    if ac_appellantStateless.count() != 0:
        return TestResult("appellantStateless", "FAIL", f"appellantStateless acceptance criteria failed: {str(ac_appellantStateless.count())} cases have been found where M2.AppellantCountryId is 211 and appellantStateless is not isStateless" ,test_from_state)
    else:
        return TestResult("appellantStateless", "PASS", f"appellantStateless acceptance criteria passed, all cases where M2.AppellantCountryId is 211 have appellantStateless is isStateless",test_from_state)

#######################
#appellantStateless - If M2.AppellantCountryId is not 211 and appellantStateless is not hasNationality
#######################
def test_appellantStateless_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("AppellantCountryId") != "211")
        ).count() == 0:
        return TestResult("appellantStateless", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantStateless = test_df.filter(
        (col("AppellantCountryId") != "211") & (col("appellantStateless") != "hasNationality")
    )

    if ac_appellantStateless.count() != 0:
        return TestResult("appellantStateless", "FAIL", f"appellantStateless acceptance criteria failed: {str(ac_appellantStateless.count())} cases have been found where M2.AppellantCountryId is not 211 and appellantStateless is not hasNationality" ,test_from_state)
    else:
        return TestResult("appellantStateless", "PASS", f"appellantStateless acceptance criteria passed, all cases where M2.AppellantCountryId is not 211 have appellantStateless is hasNationality",test_from_state)
    
#######################
#appellantNationalities
#######################
def test_appellantNationalities(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("NationalityId").isNotNull())
        ).count() == 0:
        return TestResult("appellantNationalities", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantNationalities = test_df.filter(
    ((col("NationalityId") == 1) & ~(array_contains(col("appellantNationalities.value.code"), "AF"))) | 
    ((col("NationalityId") == 2) & ~(array_contains(col("appellantNationalities.value.code"), "AL"))) | 
    ((col("NationalityId") == 3) & ~(array_contains(col("appellantNationalities.value.code"), "DZ"))) | 
    ((col("NationalityId") == 4) & ~(array_contains(col("appellantNationalities.value.code"), "AD"))) | 
    ((col("NationalityId") == 5) & ~(array_contains(col("appellantNationalities.value.code"), "AO"))) | 
    ((col("NationalityId") == 6) & ~(array_contains(col("appellantNationalities.value.code"), "AG"))) | 
    ((col("NationalityId") == 7) & ~(array_contains(col("appellantNationalities.value.code"), "AR"))) | 
    ((col("NationalityId") == 8) & ~(array_contains(col("appellantNationalities.value.code"), "AM"))) | 
    ((col("NationalityId") == 9) & ~(array_contains(col("appellantNationalities.value.code"), "AU"))) | 
    ((col("NationalityId") == 10) & ~(array_contains(col("appellantNationalities.value.code"), "AT"))) | 
    ((col("NationalityId") == 11) & ~(array_contains(col("appellantNationalities.value.code"), "AZ"))) | 
    ((col("NationalityId") == 12) & ~(array_contains(col("appellantNationalities.value.code"), "BS"))) | 
    ((col("NationalityId") == 13) & ~(array_contains(col("appellantNationalities.value.code"), "BD"))) | 
    ((col("NationalityId") == 14) & ~(array_contains(col("appellantNationalities.value.code"), "BB"))) | 
    ((col("NationalityId") == 15) & ~(array_contains(col("appellantNationalities.value.code"), "BY"))) | 
    ((col("NationalityId") == 16) & ~(array_contains(col("appellantNationalities.value.code"), "BE"))) | 
    ((col("NationalityId") == 17) & ~(array_contains(col("appellantNationalities.value.code"), "BZ"))) | 
    ((col("NationalityId") == 18) & ~(array_contains(col("appellantNationalities.value.code"), "BJ"))) | 
    ((col("NationalityId") == 19) & ~(array_contains(col("appellantNationalities.value.code"), "BO"))) | 
    ((col("NationalityId") == 20) & ~(array_contains(col("appellantNationalities.value.code"), "BA"))) | 
    ((col("NationalityId") == 21) & ~(array_contains(col("appellantNationalities.value.code"), "BW"))) | 
    ((col("NationalityId") == 22) & ~(array_contains(col("appellantNationalities.value.code"), "BR"))) | 
    ((col("NationalityId") == 23) & ~(array_contains(col("appellantNationalities.value.code"), "BN"))) | 
    ((col("NationalityId") == 24) & ~(array_contains(col("appellantNationalities.value.code"), "BG"))) | 
    ((col("NationalityId") == 25) & ~(array_contains(col("appellantNationalities.value.code"), "BF"))) | 
    ((col("NationalityId") == 26) & ~(array_contains(col("appellantNationalities.value.code"), "MM"))) | 
    ((col("NationalityId") == 27) & ~(array_contains(col("appellantNationalities.value.code"), "BI"))) | 
    ((col("NationalityId") == 28) & ~(array_contains(col("appellantNationalities.value.code"), "CM"))) | 
    ((col("NationalityId") == 29) & ~(array_contains(col("appellantNationalities.value.code"), "CA"))) | 
    ((col("NationalityId") == 30) & ~(array_contains(col("appellantNationalities.value.code"), "CV"))) | 
    ((col("NationalityId") == 31) & ~(array_contains(col("appellantNationalities.value.code"), "CF"))) | 
    ((col("NationalityId") == 32) & ~(array_contains(col("appellantNationalities.value.code"), "TD"))) | 
    ((col("NationalityId") == 33) & ~(array_contains(col("appellantNationalities.value.code"), "CL"))) | 
    ((col("NationalityId") == 34) & ~(array_contains(col("appellantNationalities.value.code"), "CN"))) | 
    ((col("NationalityId") == 35) & ~(array_contains(col("appellantNationalities.value.code"), "CO"))) | 
    ((col("NationalityId") == 36) & ~(array_contains(col("appellantNationalities.value.code"), "KM"))) | 
    ((col("NationalityId") == 37) & ~(array_contains(col("appellantNationalities.value.code"), "CG"))) | 
    ((col("NationalityId") == 38) & ~(array_contains(col("appellantNationalities.value.code"), "CR"))) | 
    ((col("NationalityId") == 40) & ~(array_contains(col("appellantNationalities.value.code"), "HR"))) | 
    ((col("NationalityId") == 41) & ~(array_contains(col("appellantNationalities.value.code"), "CU"))) | 
    ((col("NationalityId") == 42) & ~(array_contains(col("appellantNationalities.value.code"), "CY"))) | 
    ((col("NationalityId") == 43) & ~(array_contains(col("appellantNationalities.value.code"), "CZ"))) | 
    ((col("NationalityId") == 44) & ~(array_contains(col("appellantNationalities.value.code"), "DK"))) | 
    ((col("NationalityId") == 45) & ~(array_contains(col("appellantNationalities.value.code"), "DJ"))) | 
    ((col("NationalityId") == 46) & ~(array_contains(col("appellantNationalities.value.code"), "DM"))) | 
    ((col("NationalityId") == 47) & ~(array_contains(col("appellantNationalities.value.code"), "DO"))) | 
    ((col("NationalityId") == 48) & ~(array_contains(col("appellantNationalities.value.code"), "EC"))) | 
    ((col("NationalityId") == 49) & ~(array_contains(col("appellantNationalities.value.code"), "EG"))) | 
    ((col("NationalityId") == 50) & ~(array_contains(col("appellantNationalities.value.code"), "SV"))) | 
    ((col("NationalityId") == 51) & ~(array_contains(col("appellantNationalities.value.code"), "GQ"))) | 
    ((col("NationalityId") == 52) & ~(array_contains(col("appellantNationalities.value.code"), "ER"))) | 
    ((col("NationalityId") == 53) & ~(array_contains(col("appellantNationalities.value.code"), "EE"))) | 
    ((col("NationalityId") == 54) & ~(array_contains(col("appellantNationalities.value.code"), "ET"))) | 
    ((col("NationalityId") == 55) & ~(array_contains(col("appellantNationalities.value.code"), "FJ"))) | 
    ((col("NationalityId") == 56) & ~(array_contains(col("appellantNationalities.value.code"), "FI"))) | 
    ((col("NationalityId") == 57) & ~(array_contains(col("appellantNationalities.value.code"), "FR"))) | 
    ((col("NationalityId") == 58) & ~(array_contains(col("appellantNationalities.value.code"), "GA"))) | 
    ((col("NationalityId") == 59) & ~(array_contains(col("appellantNationalities.value.code"), "GM"))) | 
    ((col("NationalityId") == 60) & ~(array_contains(col("appellantNationalities.value.code"), "GE"))) | 
    ((col("NationalityId") == 61) & ~(array_contains(col("appellantNationalities.value.code"), "DE"))) | 
    ((col("NationalityId") == 62) & ~(array_contains(col("appellantNationalities.value.code"), "GH"))) | 
    ((col("NationalityId") == 63) & ~(array_contains(col("appellantNationalities.value.code"), "GR"))) | 
    ((col("NationalityId") == 64) & ~(array_contains(col("appellantNationalities.value.code"), "GD"))) | 
    ((col("NationalityId") == 65) & ~(array_contains(col("appellantNationalities.value.code"), "GT"))) | 
    ((col("NationalityId") == 66) & ~(array_contains(col("appellantNationalities.value.code"), "GN"))) | 
    ((col("NationalityId") == 67) & ~(array_contains(col("appellantNationalities.value.code"), "GW"))) | 
    ((col("NationalityId") == 68) & ~(array_contains(col("appellantNationalities.value.code"), "GY"))) | 
    ((col("NationalityId") == 69) & ~(array_contains(col("appellantNationalities.value.code"), "HT"))) | 
    ((col("NationalityId") == 70) & ~(array_contains(col("appellantNationalities.value.code"), "VA"))) | 
    ((col("NationalityId") == 71) & ~(array_contains(col("appellantNationalities.value.code"), "HN"))) | 
    ((col("NationalityId") == 72) & ~(array_contains(col("appellantNationalities.value.code"), "HU"))) | 
    ((col("NationalityId") == 73) & ~(array_contains(col("appellantNationalities.value.code"), "IS"))) | 
    ((col("NationalityId") == 74) & ~(array_contains(col("appellantNationalities.value.code"), "IN"))) | 
    ((col("NationalityId") == 75) & ~(array_contains(col("appellantNationalities.value.code"), "ID"))) | 
    ((col("NationalityId") == 76) & ~(array_contains(col("appellantNationalities.value.code"), "IR"))) | 
    ((col("NationalityId") == 77) & ~(array_contains(col("appellantNationalities.value.code"), "IE"))) | 
    ((col("NationalityId") == 78) & ~(array_contains(col("appellantNationalities.value.code"), "IL"))) | 
    ((col("NationalityId") == 79) & ~(array_contains(col("appellantNationalities.value.code"), "IT"))) | 
    ((col("NationalityId") == 80) & ~(array_contains(col("appellantNationalities.value.code"), "JM"))) | 
    ((col("NationalityId") == 81) & ~(array_contains(col("appellantNationalities.value.code"), "JP"))) | 
    ((col("NationalityId") == 82) & ~(array_contains(col("appellantNationalities.value.code"), "PS"))) | 
    ((col("NationalityId") == 83) & ~(array_contains(col("appellantNationalities.value.code"), "JO"))) | 
    ((col("NationalityId") == 84) & ~(array_contains(col("appellantNationalities.value.code"), "KZ"))) | 
    ((col("NationalityId") == 85) & ~(array_contains(col("appellantNationalities.value.code"), "KE"))) | 
    ((col("NationalityId") == 86) & ~(array_contains(col("appellantNationalities.value.code"), "KI"))) | 
    ((col("NationalityId") == 87) & ~(array_contains(col("appellantNationalities.value.code"), "KR"))) | 
    ((col("NationalityId") == 88) & ~(array_contains(col("appellantNationalities.value.code"), "KW"))) | 
    ((col("NationalityId") == 89) & ~(array_contains(col("appellantNationalities.value.code"), "KG"))) | 
    ((col("NationalityId") == 90) & ~(array_contains(col("appellantNationalities.value.code"), "LA"))) | 
    ((col("NationalityId") == 91) & ~(array_contains(col("appellantNationalities.value.code"), "LV"))) | 
    ((col("NationalityId") == 92) & ~(array_contains(col("appellantNationalities.value.code"), "LB"))) | 
    ((col("NationalityId") == 93) & ~(array_contains(col("appellantNationalities.value.code"), "LS"))) | 
    ((col("NationalityId") == 94) & ~(array_contains(col("appellantNationalities.value.code"), "LR"))) | 
    ((col("NationalityId") == 95) & ~(array_contains(col("appellantNationalities.value.code"), "LY"))) | 
    ((col("NationalityId") == 97) & ~(array_contains(col("appellantNationalities.value.code"), "LT"))) | 
    ((col("NationalityId") == 98) & ~(array_contains(col("appellantNationalities.value.code"), "LU"))) | 
    ((col("NationalityId") == 99) & ~(array_contains(col("appellantNationalities.value.code"), "MK"))) | 
    ((col("NationalityId") == 100) & ~(array_contains(col("appellantNationalities.value.code"), "MG"))) | 
    ((col("NationalityId") == 101) & ~(array_contains(col("appellantNationalities.value.code"), "MW"))) | 
    ((col("NationalityId") == 102) & ~(array_contains(col("appellantNationalities.value.code"), "MY"))) | 
    ((col("NationalityId") == 103) & ~(array_contains(col("appellantNationalities.value.code"), "MV"))) | 
    ((col("NationalityId") == 104) & ~(array_contains(col("appellantNationalities.value.code"), "ML"))) | 
    ((col("NationalityId") == 105) & ~(array_contains(col("appellantNationalities.value.code"), "MT"))) | 
    ((col("NationalityId") == 106) & ~(array_contains(col("appellantNationalities.value.code"), "MH"))) | 
    ((col("NationalityId") == 107) & ~(array_contains(col("appellantNationalities.value.code"), "MR"))) | 
    ((col("NationalityId") == 108) & ~(array_contains(col("appellantNationalities.value.code"), "MU"))) | 
    ((col("NationalityId") == 109) & ~(array_contains(col("appellantNationalities.value.code"), "MX"))) | 
    ((col("NationalityId") == 110) & ~(array_contains(col("appellantNationalities.value.code"), "FM"))) | 
    ((col("NationalityId") == 111) & ~(array_contains(col("appellantNationalities.value.code"), "MD"))) | 
    ((col("NationalityId") == 112) & ~(array_contains(col("appellantNationalities.value.code"), "MC"))) | 
    ((col("NationalityId") == 113) & ~(array_contains(col("appellantNationalities.value.code"), "MN"))) | 
    ((col("NationalityId") == 114) & ~(array_contains(col("appellantNationalities.value.code"), "MA"))) | 
    ((col("NationalityId") == 115) & ~(array_contains(col("appellantNationalities.value.code"), "MZ"))) | 
    ((col("NationalityId") == 116) & ~(array_contains(col("appellantNationalities.value.code"), "NA"))) | 
    ((col("NationalityId") == 117) & ~(array_contains(col("appellantNationalities.value.code"), "NR"))) | 
    ((col("NationalityId") == 118) & ~(array_contains(col("appellantNationalities.value.code"), "NP"))) | 
    ((col("NationalityId") == 119) & ~(array_contains(col("appellantNationalities.value.code"), "NL"))) | 
    ((col("NationalityId") == 120) & ~(array_contains(col("appellantNationalities.value.code"), "NZ"))) | 
    ((col("NationalityId") == 121) & ~(array_contains(col("appellantNationalities.value.code"), "NI"))) | 
    ((col("NationalityId") == 122) & ~(array_contains(col("appellantNationalities.value.code"), "NE"))) | 
    ((col("NationalityId") == 123) & ~(array_contains(col("appellantNationalities.value.code"), "NG"))) | 
    ((col("NationalityId") == 124) & ~(array_contains(col("appellantNationalities.value.code"), "NO"))) | 
    ((col("NationalityId") == 125) & ~(array_contains(col("appellantNationalities.value.code"), "OM"))) | 
    ((col("NationalityId") == 126) & ~(array_contains(col("appellantNationalities.value.code"), "PK"))) | 
    ((col("NationalityId") == 127) & ~(array_contains(col("appellantNationalities.value.code"), "PW"))) | 
    ((col("NationalityId") == 128) & ~(array_contains(col("appellantNationalities.value.code"), "PA"))) | 
    ((col("NationalityId") == 129) & ~(array_contains(col("appellantNationalities.value.code"), "PG"))) | 
    ((col("NationalityId") == 130) & ~(array_contains(col("appellantNationalities.value.code"), "PY"))) | 
    ((col("NationalityId") == 131) & ~(array_contains(col("appellantNationalities.value.code"), "PE"))) | 
    ((col("NationalityId") == 132) & ~(array_contains(col("appellantNationalities.value.code"), "PH"))) | 
    ((col("NationalityId") == 133) & ~(array_contains(col("appellantNationalities.value.code"), "PL"))) | 
    ((col("NationalityId") == 134) & ~(array_contains(col("appellantNationalities.value.code"), "PT"))) | 
    ((col("NationalityId") == 135) & ~(array_contains(col("appellantNationalities.value.code"), "QA"))) | 
    ((col("NationalityId") == 136) & ~(array_contains(col("appellantNationalities.value.code"), "RO"))) | 
    ((col("NationalityId") == 137) & ~(array_contains(col("appellantNationalities.value.code"), "RU"))) | 
    ((col("NationalityId") == 138) & ~(array_contains(col("appellantNationalities.value.code"), "RW"))) | 
    ((col("NationalityId") == 139) & ~(array_contains(col("appellantNationalities.value.code"), "KN"))) | 
    ((col("NationalityId") == 140) & ~(array_contains(col("appellantNationalities.value.code"), "LC"))) | 
    ((col("NationalityId") == 141) & ~(array_contains(col("appellantNationalities.value.code"), "VC"))) | 
    ((col("NationalityId") == 142) & ~(array_contains(col("appellantNationalities.value.code"), "SM"))) | 
    ((col("NationalityId") == 143) & ~(array_contains(col("appellantNationalities.value.code"), "ST"))) | 
    ((col("NationalityId") == 144) & ~(array_contains(col("appellantNationalities.value.code"), "SA"))) | 
    ((col("NationalityId") == 145) & ~(array_contains(col("appellantNationalities.value.code"), "SN"))) | 
    ((col("NationalityId") == 146) & ~(array_contains(col("appellantNationalities.value.code"), "SC"))) | 
    ((col("NationalityId") == 147) & ~(array_contains(col("appellantNationalities.value.code"), "SL"))) | 
    ((col("NationalityId") == 148) & ~(array_contains(col("appellantNationalities.value.code"), "SG"))) | 
    ((col("NationalityId") == 149) & ~(array_contains(col("appellantNationalities.value.code"), "SK"))) | 
    ((col("NationalityId") == 150) & ~(array_contains(col("appellantNationalities.value.code"), "SI"))) | 
    ((col("NationalityId") == 152) & ~(array_contains(col("appellantNationalities.value.code"), "SO"))) | 
    ((col("NationalityId") == 153) & ~(array_contains(col("appellantNationalities.value.code"), "ZA"))) | 
    ((col("NationalityId") == 154) & ~(array_contains(col("appellantNationalities.value.code"), "ES"))) | 
    ((col("NationalityId") == 155) & ~(array_contains(col("appellantNationalities.value.code"), "LK"))) | 
    ((col("NationalityId") == 156) & ~(array_contains(col("appellantNationalities.value.code"), "SD"))) | 
    ((col("NationalityId") == 157) & ~(array_contains(col("appellantNationalities.value.code"), "SR"))) | 
    ((col("NationalityId") == 158) & ~(array_contains(col("appellantNationalities.value.code"), "SZ"))) | 
    ((col("NationalityId") == 159) & ~(array_contains(col("appellantNationalities.value.code"), "SE"))) | 
    ((col("NationalityId") == 160) & ~(array_contains(col("appellantNationalities.value.code"), "CH"))) | 
    ((col("NationalityId") == 161) & ~(array_contains(col("appellantNationalities.value.code"), "SY"))) | 
    ((col("NationalityId") == 162) & ~(array_contains(col("appellantNationalities.value.code"), "TJ"))) | 
    ((col("NationalityId") == 163) & ~(array_contains(col("appellantNationalities.value.code"), "TZ"))) | 
    ((col("NationalityId") == 164) & ~(array_contains(col("appellantNationalities.value.code"), "TH"))) | 
    ((col("NationalityId") == 165) & ~(array_contains(col("appellantNationalities.value.code"), "TG"))) | 
    ((col("NationalityId") == 166) & ~(array_contains(col("appellantNationalities.value.code"), "TO"))) | 
    ((col("NationalityId") == 167) & ~(array_contains(col("appellantNationalities.value.code"), "TT"))) | 
    ((col("NationalityId") == 168) & ~(array_contains(col("appellantNationalities.value.code"), "TN"))) | 
    ((col("NationalityId") == 169) & ~(array_contains(col("appellantNationalities.value.code"), "TR"))) | 
    ((col("NationalityId") == 170) & ~(array_contains(col("appellantNationalities.value.code"), "TM"))) | 
    ((col("NationalityId") == 171) & ~(array_contains(col("appellantNationalities.value.code"), "TV"))) | 
    ((col("NationalityId") == 172) & ~(array_contains(col("appellantNationalities.value.code"), "UG"))) | 
    ((col("NationalityId") == 173) & ~(array_contains(col("appellantNationalities.value.code"), "UA"))) | 
    ((col("NationalityId") == 174) & ~(array_contains(col("appellantNationalities.value.code"), "AE"))) | 
    ((col("NationalityId") == 175) & ~(array_contains(col("appellantNationalities.value.code"), "US"))) | 
    ((col("NationalityId") == 176) & ~(array_contains(col("appellantNationalities.value.code"), "UY"))) | 
    ((col("NationalityId") == 177) & ~(array_contains(col("appellantNationalities.value.code"), "UZ"))) | 
    ((col("NationalityId") == 178) & ~(array_contains(col("appellantNationalities.value.code"), "VU"))) | 
    ((col("NationalityId") == 179) & ~(array_contains(col("appellantNationalities.value.code"), "VE"))) | 
    ((col("NationalityId") == 180) & ~(array_contains(col("appellantNationalities.value.code"), "VN"))) | 
    ((col("NationalityId") == 181) & ~(array_contains(col("appellantNationalities.value.code"), "WS"))) | 
    ((col("NationalityId") == 182) & ~(array_contains(col("appellantNationalities.value.code"), "YE"))) | 
    ((col("NationalityId") == 183) & ~(array_contains(col("appellantNationalities.value.code"), "ME"))) | 
    ((col("NationalityId") == 184) & ~(array_contains(col("appellantNationalities.value.code"), "CD"))) | 
    ((col("NationalityId") == 185) & ~(array_contains(col("appellantNationalities.value.code"), "ZM"))) | 
    ((col("NationalityId") == 186) & ~(array_contains(col("appellantNationalities.value.code"), "ZW"))) | 
    ((col("NationalityId") == 187) & ~(array_contains(col("appellantNationalities.value.code"), "CI"))) | 
    ((col("NationalityId") == 188) & ~(array_contains(col("appellantNationalities.value.code"), "GB"))) | 
    ((col("NationalityId") == 189) & ~(array_contains(col("appellantNationalities.value.code"), "BH"))) | 
    ((col("NationalityId") == 190) & ~(array_contains(col("appellantNationalities.value.code"), "KH"))) | 
    ((col("NationalityId") == 191) & ~(array_contains(col("appellantNationalities.value.code"), "IQ"))) | 
    ((col("NationalityId") == 192) & ~(array_contains(col("appellantNationalities.value.code"), "MK"))) | 
    ((col("NationalityId") == 193) & ~(array_contains(col("appellantNationalities.value.code"), "BT"))) | 
    ((col("NationalityId") == 194) & ~(array_contains(col("appellantNationalities.value.code"), "PR"))) | 
    ((col("NationalityId") == 195) & ~(array_contains(col("appellantNationalities.value.code"), "HK"))) | 
    ((col("NationalityId") == 196) & ~(array_contains(col("appellantNationalities.value.code"), "BC"))) | 
    ((col("NationalityId") == 197) & ~(array_contains(col("appellantNationalities.value.code"), "IE"))) | 
    ((col("NationalityId") == 198) & ~(array_contains(col("appellantNationalities.value.code"), "PS"))) | 
    ((col("NationalityId") == 199) & ~(array_contains(col("appellantNationalities.value.code"), "CD"))) | 
    ((col("NationalityId") == 200) & ~(array_contains(col("appellantNationalities.value.code"), "TW"))) | 
    ((col("NationalityId") == 202) & ~(array_contains(col("appellantNationalities.value.code"), "EH"))) | 
    ((col("NationalityId") == 204) & ~(array_contains(col("appellantNationalities.value.code"), "TL"))) | 
    ((col("NationalityId") == 205) & ~(array_contains(col("appellantNationalities.value.code"), "MD"))) | 
    ((col("NationalityId") == 206) & ~(array_contains(col("appellantNationalities.value.code"), "KP"))) | 
    ((col("NationalityId") == 208) & ~(array_contains(col("appellantNationalities.value.code"), "RS"))) | 
    ((col("NationalityId") == 209) & ~(array_contains(col("appellantNationalities.value.code"), "ME"))) | 
    ((col("NationalityId") == 211) & ~(array_contains(col("appellantNationalities.value.code"), "ZZ"))) | 
    ((col("NationalityId") == 212) & ~(array_contains(col("appellantNationalities.value.code"), "TW"))) | 
    ((col("NationalityId") == 213) & ~(array_contains(col("appellantNationalities.value.code"), "KO")))
    )

    if ac_appellantNationalities.count() != 0:
        return TestResult("appellantNationalities", "FAIL", f"appellantNationalities acceptance criteria failed: {str(ac_appellantNationalities.count())} cases have been found where the appellantNationalities field does not match the NationalityId field entries." ,test_from_state)
    else:
        return TestResult("appellantNationalities", "PASS", f"appellantNationalities acceptance criteria passed, all appellantNationalities fields match the NationalityId field entries.",test_from_state)

#######################
#appellantNationalitiesDescription
#######################
def test_appellantNationalitiesDescription(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("NationalityId").isNotNull())
        ).count() == 0:
        return TestResult("appellantNationalitiesDescription", "FAIL", "NO RECORDS TO TEST",test_from_state)

    ac_appellantNationalitiesDescription = test_df.filter(
    ((col("NationalityId") == 1) & ~(col("appellantNationalitiesDescription").contains("Afghanistan"))) | 
    ((col("NationalityId") == 2) & ~(col("appellantNationalitiesDescription").contains("Albania"))) | 
    ((col("NationalityId") == 3) & ~(col("appellantNationalitiesDescription").contains("Algeria"))) | 
    ((col("NationalityId") == 4) & ~(col("appellantNationalitiesDescription").contains("Andorra"))) | 
    ((col("NationalityId") == 5) & ~(col("appellantNationalitiesDescription").contains("Angola"))) | 
    ((col("NationalityId") == 6) & ~(col("appellantNationalitiesDescription").contains("Antigua and Barbuda"))) | 
    ((col("NationalityId") == 7) & ~(col("appellantNationalitiesDescription").contains("Argentina"))) | 
    ((col("NationalityId") == 8) & ~(col("appellantNationalitiesDescription").contains("Armenia"))) | 
    ((col("NationalityId") == 9) & ~(col("appellantNationalitiesDescription").contains("Australia"))) | 
    ((col("NationalityId") == 10) & ~(col("appellantNationalitiesDescription").contains("Austria"))) | 
    ((col("NationalityId") == 11) & ~(col("appellantNationalitiesDescription").contains("Azerbaijan"))) | 
    ((col("NationalityId") == 12) & ~(col("appellantNationalitiesDescription").contains("Bahamas"))) | 
    ((col("NationalityId") == 13) & ~(col("appellantNationalitiesDescription").contains("Bangladesh"))) | 
    ((col("NationalityId") == 14) & ~(col("appellantNationalitiesDescription").contains("Barbados"))) | 
    ((col("NationalityId") == 15) & ~(col("appellantNationalitiesDescription").contains("Belarus"))) | 
    ((col("NationalityId") == 16) & ~(col("appellantNationalitiesDescription").contains("Belgium"))) | 
    ((col("NationalityId") == 17) & ~(col("appellantNationalitiesDescription").contains("Belize"))) | 
    ((col("NationalityId") == 18) & ~(col("appellantNationalitiesDescription").contains("Benin"))) | 
    ((col("NationalityId") == 19) & ~(col("appellantNationalitiesDescription").contains("Bolivia"))) | 
    ((col("NationalityId") == 20) & ~(col("appellantNationalitiesDescription").contains("Bosnia and Herzegovina"))) | 
    ((col("NationalityId") == 21) & ~(col("appellantNationalitiesDescription").contains("Botswana"))) | 
    ((col("NationalityId") == 22) & ~(col("appellantNationalitiesDescription").contains("Brazil"))) | 
    ((col("NationalityId") == 23) & ~(col("appellantNationalitiesDescription").contains("Brunei Darussalam"))) | 
    ((col("NationalityId") == 24) & ~(col("appellantNationalitiesDescription").contains("Bulgaria"))) | 
    ((col("NationalityId") == 25) & ~(col("appellantNationalitiesDescription").contains("Burkina Faso"))) | 
    ((col("NationalityId") == 26) & ~(col("appellantNationalitiesDescription").contains("Myanmar"))) | 
    ((col("NationalityId") == 27) & ~(col("appellantNationalitiesDescription").contains("Burundi"))) | 
    ((col("NationalityId") == 28) & ~(col("appellantNationalitiesDescription").contains("Cameroon"))) | 
    ((col("NationalityId") == 29) & ~(col("appellantNationalitiesDescription").contains("Canada"))) | 
    ((col("NationalityId") == 30) & ~(col("appellantNationalitiesDescription").contains("Cape Verde"))) | 
    ((col("NationalityId") == 31) & ~(col("appellantNationalitiesDescription").contains("Central African Republic"))) | 
    ((col("NationalityId") == 32) & ~(col("appellantNationalitiesDescription").contains("Chad"))) | 
    ((col("NationalityId") == 33) & ~(col("appellantNationalitiesDescription").contains("Chile"))) | 
    ((col("NationalityId") == 34) & ~(col("appellantNationalitiesDescription").contains("China"))) | 
    ((col("NationalityId") == 35) & ~(col("appellantNationalitiesDescription").contains("Colombia"))) | 
    ((col("NationalityId") == 36) & ~(col("appellantNationalitiesDescription").contains("Comoros"))) | 
    ((col("NationalityId") == 37) & ~(col("appellantNationalitiesDescription").contains("Congo (Brazzaville)"))) | 
    ((col("NationalityId") == 38) & ~(col("appellantNationalitiesDescription").contains("Costa Rica"))) | 
    ((col("NationalityId") == 40) & ~(col("appellantNationalitiesDescription").contains("Croatia"))) | 
    ((col("NationalityId") == 41) & ~(col("appellantNationalitiesDescription").contains("Cuba"))) | 
    ((col("NationalityId") == 42) & ~(col("appellantNationalitiesDescription").contains("Cyprus"))) | 
    ((col("NationalityId") == 43) & ~(col("appellantNationalitiesDescription").contains("Czech Republic"))) | 
    ((col("NationalityId") == 44) & ~(col("appellantNationalitiesDescription").contains("Denmark"))) | 
    ((col("NationalityId") == 45) & ~(col("appellantNationalitiesDescription").contains("Djibouti"))) | 
    ((col("NationalityId") == 46) & ~(col("appellantNationalitiesDescription").contains("Dominica"))) | 
    ((col("NationalityId") == 47) & ~(col("appellantNationalitiesDescription").contains("Dominican Republic"))) | 
    ((col("NationalityId") == 48) & ~(col("appellantNationalitiesDescription").contains("Ecuador"))) | 
    ((col("NationalityId") == 49) & ~(col("appellantNationalitiesDescription").contains("Egypt"))) | 
    ((col("NationalityId") == 50) & ~(col("appellantNationalitiesDescription").contains("El Salvador"))) | 
    ((col("NationalityId") == 51) & ~(col("appellantNationalitiesDescription").contains("Equatorial Guinea"))) | 
    ((col("NationalityId") == 52) & ~(col("appellantNationalitiesDescription").contains("Eritrea"))) | 
    ((col("NationalityId") == 53) & ~(col("appellantNationalitiesDescription").contains("Estonia"))) | 
    ((col("NationalityId") == 54) & ~(col("appellantNationalitiesDescription").contains("Ethiopia"))) | 
    ((col("NationalityId") == 55) & ~(col("appellantNationalitiesDescription").contains("Fiji"))) | 
    ((col("NationalityId") == 56) & ~(col("appellantNationalitiesDescription").contains("Finland"))) | 
    ((col("NationalityId") == 57) & ~(col("appellantNationalitiesDescription").contains("France"))) | 
    ((col("NationalityId") == 58) & ~(col("appellantNationalitiesDescription").contains("Gabon"))) | 
    ((col("NationalityId") == 59) & ~(col("appellantNationalitiesDescription").contains("Gambia"))) | 
    ((col("NationalityId") == 60) & ~(col("appellantNationalitiesDescription").contains("Georgia"))) | 
    ((col("NationalityId") == 61) & ~(col("appellantNationalitiesDescription").contains("Germany"))) | 
    ((col("NationalityId") == 62) & ~(col("appellantNationalitiesDescription").contains("Ghana"))) | 
    ((col("NationalityId") == 63) & ~(col("appellantNationalitiesDescription").contains("Greece"))) | 
    ((col("NationalityId") == 64) & ~(col("appellantNationalitiesDescription").contains("Grenada"))) | 
    ((col("NationalityId") == 65) & ~(col("appellantNationalitiesDescription").contains("Guatemala"))) | 
    ((col("NationalityId") == 66) & ~(col("appellantNationalitiesDescription").contains("Guinea"))) | 
    ((col("NationalityId") == 67) & ~(col("appellantNationalitiesDescription").contains("Guinea-Bissau"))) | 
    ((col("NationalityId") == 68) & ~(col("appellantNationalitiesDescription").contains("Guyana"))) | 
    ((col("NationalityId") == 69) & ~(col("appellantNationalitiesDescription").contains("Haiti"))) | 
    ((col("NationalityId") == 70) & ~(col("appellantNationalitiesDescription").contains("Holy See (Vatican City State)"))) | 
    ((col("NationalityId") == 71) & ~(col("appellantNationalitiesDescription").contains("Honduras"))) | 
    ((col("NationalityId") == 72) & ~(col("appellantNationalitiesDescription").contains("Hungary"))) | 
    ((col("NationalityId") == 73) & ~(col("appellantNationalitiesDescription").contains("Iceland"))) | 
    ((col("NationalityId") == 74) & ~(col("appellantNationalitiesDescription").contains("India"))) | 
    ((col("NationalityId") == 75) & ~(col("appellantNationalitiesDescription").contains("Indonesia"))) | 
    ((col("NationalityId") == 76) & ~(col("appellantNationalitiesDescription").contains("Iran, Islamic Republic of"))) | 
    ((col("NationalityId") == 77) & ~(col("appellantNationalitiesDescription").contains("Ireland"))) | 
    ((col("NationalityId") == 78) & ~(col("appellantNationalitiesDescription").contains("Israel"))) | 
    ((col("NationalityId") == 79) & ~(col("appellantNationalitiesDescription").contains("Italy"))) | 
    ((col("NationalityId") == 80) & ~(col("appellantNationalitiesDescription").contains("Jamaica"))) | 
    ((col("NationalityId") == 81) & ~(col("appellantNationalitiesDescription").contains("Japan"))) | 
    ((col("NationalityId") == 82) & ~(col("appellantNationalitiesDescription").contains("Palestinian Territory, Occupied"))) | 
    ((col("NationalityId") == 83) & ~(col("appellantNationalitiesDescription").contains("Jordan"))) | 
    ((col("NationalityId") == 84) & ~(col("appellantNationalitiesDescription").contains("Kazakhstan"))) | 
    ((col("NationalityId") == 85) & ~(col("appellantNationalitiesDescription").contains("Kenya"))) | 
    ((col("NationalityId") == 86) & ~(col("appellantNationalitiesDescription").contains("Kiribati"))) | 
    ((col("NationalityId") == 87) & ~(col("appellantNationalitiesDescription").contains("Korea, Republic of"))) | 
    ((col("NationalityId") == 88) & ~(col("appellantNationalitiesDescription").contains("Kuwait"))) | 
    ((col("NationalityId") == 89) & ~(col("appellantNationalitiesDescription").contains("Kyrgyzstan"))) | 
    ((col("NationalityId") == 90) & ~(col("appellantNationalitiesDescription").contains("Lao PDR"))) | 
    ((col("NationalityId") == 91) & ~(col("appellantNationalitiesDescription").contains("Latvia"))) | 
    ((col("NationalityId") == 92) & ~(col("appellantNationalitiesDescription").contains("Lebanon"))) | 
    ((col("NationalityId") == 93) & ~(col("appellantNationalitiesDescription").contains("Lesotho"))) | 
    ((col("NationalityId") == 94) & ~(col("appellantNationalitiesDescription").contains("Liberia"))) | 
    ((col("NationalityId") == 95) & ~(col("appellantNationalitiesDescription").contains("Libya"))) | 
    ((col("NationalityId") == 97) & ~(col("appellantNationalitiesDescription").contains("Lithuania"))) | 
    ((col("NationalityId") == 98) & ~(col("appellantNationalitiesDescription").contains("Luxembourg"))) | 
    ((col("NationalityId") == 99) & ~(col("appellantNationalitiesDescription").contains("Macedonia, Republic of"))) | 
    ((col("NationalityId") == 100) & ~(col("appellantNationalitiesDescription").contains("Madagascar"))) | 
    ((col("NationalityId") == 101) & ~(col("appellantNationalitiesDescription").contains("Malawi"))) | 
    ((col("NationalityId") == 102) & ~(col("appellantNationalitiesDescription").contains("Malaysia"))) | 
    ((col("NationalityId") == 103) & ~(col("appellantNationalitiesDescription").contains("Maldives"))) | 
    ((col("NationalityId") == 104) & ~(col("appellantNationalitiesDescription").contains("Mali"))) | 
    ((col("NationalityId") == 105) & ~(col("appellantNationalitiesDescription").contains("Malta"))) | 
    ((col("NationalityId") == 106) & ~(col("appellantNationalitiesDescription").contains("Marshall Islands"))) | 
    ((col("NationalityId") == 107) & ~(col("appellantNationalitiesDescription").contains("Mauritania"))) | 
    ((col("NationalityId") == 108) & ~(col("appellantNationalitiesDescription").contains("Mauritius"))) | 
    ((col("NationalityId") == 109) & ~(col("appellantNationalitiesDescription").contains("Mexico"))) | 
    ((col("NationalityId") == 110) & ~(col("appellantNationalitiesDescription").contains("Micronesia, Federated States of"))) | 
    ((col("NationalityId") == 111) & ~(col("appellantNationalitiesDescription").contains("Moldova"))) | 
    ((col("NationalityId") == 112) & ~(col("appellantNationalitiesDescription").contains("Monaco"))) | 
    ((col("NationalityId") == 113) & ~(col("appellantNationalitiesDescription").contains("Mongolia"))) | 
    ((col("NationalityId") == 114) & ~(col("appellantNationalitiesDescription").contains("Morocco"))) | 
    ((col("NationalityId") == 115) & ~(col("appellantNationalitiesDescription").contains("Mozambique"))) | 
    ((col("NationalityId") == 116) & ~(col("appellantNationalitiesDescription").contains("Namibia"))) | 
    ((col("NationalityId") == 117) & ~(col("appellantNationalitiesDescription").contains("Nauru"))) | 
    ((col("NationalityId") == 118) & ~(col("appellantNationalitiesDescription").contains("Nepal"))) | 
    ((col("NationalityId") == 119) & ~(col("appellantNationalitiesDescription").contains("Netherlands"))) | 
    ((col("NationalityId") == 120) & ~(col("appellantNationalitiesDescription").contains("New Zealand"))) | 
    ((col("NationalityId") == 121) & ~(col("appellantNationalitiesDescription").contains("Nicaragua"))) | 
    ((col("NationalityId") == 122) & ~(col("appellantNationalitiesDescription").contains("Niger"))) | 
    ((col("NationalityId") == 123) & ~(col("appellantNationalitiesDescription").contains("Nigeria"))) | 
    ((col("NationalityId") == 124) & ~(col("appellantNationalitiesDescription").contains("Norway"))) | 
    ((col("NationalityId") == 125) & ~(col("appellantNationalitiesDescription").contains("Oman"))) | 
    ((col("NationalityId") == 126) & ~(col("appellantNationalitiesDescription").contains("Pakistan"))) | 
    ((col("NationalityId") == 127) & ~(col("appellantNationalitiesDescription").contains("Palau"))) | 
    ((col("NationalityId") == 128) & ~(col("appellantNationalitiesDescription").contains("Panama"))) | 
    ((col("NationalityId") == 129) & ~(col("appellantNationalitiesDescription").contains("Papua New Guinea"))) | 
    ((col("NationalityId") == 130) & ~(col("appellantNationalitiesDescription").contains("Paraguay"))) | 
    ((col("NationalityId") == 131) & ~(col("appellantNationalitiesDescription").contains("Peru"))) | 
    ((col("NationalityId") == 132) & ~(col("appellantNationalitiesDescription").contains("Philippines"))) | 
    ((col("NationalityId") == 133) & ~(col("appellantNationalitiesDescription").contains("Poland"))) | 
    ((col("NationalityId") == 134) & ~(col("appellantNationalitiesDescription").contains("Portugal"))) | 
    ((col("NationalityId") == 135) & ~(col("appellantNationalitiesDescription").contains("Qatar"))) | 
    ((col("NationalityId") == 136) & ~(col("appellantNationalitiesDescription").contains("Romania"))) | 
    ((col("NationalityId") == 137) & ~(col("appellantNationalitiesDescription").contains("Russian Federation"))) | 
    ((col("NationalityId") == 138) & ~(col("appellantNationalitiesDescription").contains("Rwanda"))) | 
    ((col("NationalityId") == 139) & ~(col("appellantNationalitiesDescription").contains("Saint Kitts and Nevis"))) | 
    ((col("NationalityId") == 140) & ~(col("appellantNationalitiesDescription").contains("Saint Lucia"))) | 
    ((col("NationalityId") == 141) & ~(col("appellantNationalitiesDescription").contains("Saint Vincent and Grenadines"))) | 
    ((col("NationalityId") == 142) & ~(col("appellantNationalitiesDescription").contains("San Marino"))) | 
    ((col("NationalityId") == 143) & ~(col("appellantNationalitiesDescription").contains("Sao Tome and Principe"))) | 
    ((col("NationalityId") == 144) & ~(col("appellantNationalitiesDescription").contains("Saudi Arabia"))) | 
    ((col("NationalityId") == 145) & ~(col("appellantNationalitiesDescription").contains("Senegal"))) | 
    ((col("NationalityId") == 146) & ~(col("appellantNationalitiesDescription").contains("Seychelles"))) | 
    ((col("NationalityId") == 147) & ~(col("appellantNationalitiesDescription").contains("Sierra Leone"))) | 
    ((col("NationalityId") == 148) & ~(col("appellantNationalitiesDescription").contains("Singapore"))) | 
    ((col("NationalityId") == 149) & ~(col("appellantNationalitiesDescription").contains("Slovakia"))) | 
    ((col("NationalityId") == 150) & ~(col("appellantNationalitiesDescription").contains("Slovenia"))) | 
    ((col("NationalityId") == 152) & ~(col("appellantNationalitiesDescription").contains("Somalia"))) | 
    ((col("NationalityId") == 153) & ~(col("appellantNationalitiesDescription").contains("South Africa"))) | 
    ((col("NationalityId") == 154) & ~(col("appellantNationalitiesDescription").contains("Spain"))) | 
    ((col("NationalityId") == 155) & ~(col("appellantNationalitiesDescription").contains("Sri Lanka"))) | 
    ((col("NationalityId") == 156) & ~(col("appellantNationalitiesDescription").contains("Sudan"))) | 
    ((col("NationalityId") == 157) & ~(col("appellantNationalitiesDescription").contains("Suriname"))) | 
    ((col("NationalityId") == 158) & ~(col("appellantNationalitiesDescription").contains("Swaziland"))) | 
    ((col("NationalityId") == 159) & ~(col("appellantNationalitiesDescription").contains("Sweden"))) | 
    ((col("NationalityId") == 160) & ~(col("appellantNationalitiesDescription").contains("Switzerland"))) | 
    ((col("NationalityId") == 161) & ~(col("appellantNationalitiesDescription").contains("Syria"))) | 
    ((col("NationalityId") == 162) & ~(col("appellantNationalitiesDescription").contains("Tajikistan"))) | 
    ((col("NationalityId") == 163) & ~(col("appellantNationalitiesDescription").contains("Tanzania"))) | 
    ((col("NationalityId") == 164) & ~(col("appellantNationalitiesDescription").contains("Thailand"))) | 
    ((col("NationalityId") == 165) & ~(col("appellantNationalitiesDescription").contains("Togo"))) | 
    ((col("NationalityId") == 166) & ~(col("appellantNationalitiesDescription").contains("Tonga"))) | 
    ((col("NationalityId") == 167) & ~(col("appellantNationalitiesDescription").contains("Trinidad and Tobago"))) | 
    ((col("NationalityId") == 168) & ~(col("appellantNationalitiesDescription").contains("Tunisia"))) | 
    ((col("NationalityId") == 169) & ~(col("appellantNationalitiesDescription").contains("Turkey"))) | 
    ((col("NationalityId") == 170) & ~(col("appellantNationalitiesDescription").contains("Turkmenistan"))) | 
    ((col("NationalityId") == 171) & ~(col("appellantNationalitiesDescription").contains("Tuvalu"))) | 
    ((col("NationalityId") == 172) & ~(col("appellantNationalitiesDescription").contains("Uganda"))) | 
    ((col("NationalityId") == 173) & ~(col("appellantNationalitiesDescription").contains("Ukraine"))) | 
    ((col("NationalityId") == 174) & ~(col("appellantNationalitiesDescription").contains("United Arab Emirates"))) | 
    ((col("NationalityId") == 175) & ~(col("appellantNationalitiesDescription").contains("United States of America"))) | 
    ((col("NationalityId") == 176) & ~(col("appellantNationalitiesDescription").contains("Uruguay"))) | 
    ((col("NationalityId") == 177) & ~(col("appellantNationalitiesDescription").contains("Uzbekistan"))) | 
    ((col("NationalityId") == 178) & ~(col("appellantNationalitiesDescription").contains("Vanuatu"))) | 
    ((col("NationalityId") == 179) & ~(col("appellantNationalitiesDescription").contains("Venezuela"))) | 
    ((col("NationalityId") == 180) & ~(col("appellantNationalitiesDescription").contains("Viet Nam"))) | 
    ((col("NationalityId") == 181) & ~(col("appellantNationalitiesDescription").contains("Samoa"))) | 
    ((col("NationalityId") == 182) & ~(col("appellantNationalitiesDescription").contains("Yemen"))) | 
    ((col("NationalityId") == 183) & ~(col("appellantNationalitiesDescription").contains("Montenegro"))) | 
    ((col("NationalityId") == 184) & ~(col("appellantNationalitiesDescription").contains("Congo, Democratic Republic of the"))) | 
    ((col("NationalityId") == 185) & ~(col("appellantNationalitiesDescription").contains("Zambia"))) | 
    ((col("NationalityId") == 186) & ~(col("appellantNationalitiesDescription").contains("Zimbabwe"))) | 
    ((col("NationalityId") == 187) & ~(col("appellantNationalitiesDescription").contains("Côte d'Ivoire"))) | 
    ((col("NationalityId") == 188) & ~(col("appellantNationalitiesDescription").contains("United Kingdom"))) | 
    ((col("NationalityId") == 189) & ~(col("appellantNationalitiesDescription").contains("Bahrain"))) | 
    ((col("NationalityId") == 190) & ~(col("appellantNationalitiesDescription").contains("Cambodia"))) | 
    ((col("NationalityId") == 191) & ~(col("appellantNationalitiesDescription").contains("Iraq"))) | 
    ((col("NationalityId") == 192) & ~(col("appellantNationalitiesDescription").contains("Macedonia, Republic of"))) | 
    ((col("NationalityId") == 193) & ~(col("appellantNationalitiesDescription").contains("Bhutan"))) | 
    ((col("NationalityId") == 194) & ~(col("appellantNationalitiesDescription").contains("Puerto Rico"))) | 
    ((col("NationalityId") == 195) & ~(col("appellantNationalitiesDescription").contains("Hong Kong"))) | 
    ((col("NationalityId") == 196) & ~(col("appellantNationalitiesDescription").contains("British Overseas Citizen"))) | 
    ((col("NationalityId") == 197) & ~(col("appellantNationalitiesDescription").contains("Ireland"))) | 
    ((col("NationalityId") == 198) & ~(col("appellantNationalitiesDescription").contains("Palestinian Territory, Occupied"))) | 
    ((col("NationalityId") == 199) & ~(col("appellantNationalitiesDescription").contains("Congo, Democratic Republic of the"))) | 
    ((col("NationalityId") == 200) & ~(col("appellantNationalitiesDescription").contains("Taiwan"))) | 
    ((col("NationalityId") == 202) & ~(col("appellantNationalitiesDescription").contains("Western Sahara"))) | 
    ((col("NationalityId") == 204) & ~(col("appellantNationalitiesDescription").contains("Timor-Leste"))) | 
    ((col("NationalityId") == 205) & ~(col("appellantNationalitiesDescription").contains("Moldova"))) | 
    ((col("NationalityId") == 206) & ~(col("appellantNationalitiesDescription").contains("Korea, Democratic People's Republic of"))) | 
    ((col("NationalityId") == 208) & ~(col("appellantNationalitiesDescription").contains("Serbia"))) | 
    ((col("NationalityId") == 209) & ~(col("appellantNationalitiesDescription").contains("Montenegro"))) | 
    ((col("NationalityId") == 211) & ~(col("appellantNationalitiesDescription").contains("Stateless"))) | 
    ((col("NationalityId") == 212) & ~(col("appellantNationalitiesDescription").contains("Taiwan"))) | 
    ((col("NationalityId") == 213) & ~(col("appellantNationalitiesDescription").contains("Kosovo")))
    )   

    if ac_appellantNationalitiesDescription.count() != 0:
        return TestResult("appellantNationalitiesDescription", "FAIL", f"appellantNationalitiesDescription acceptance criteria failed: {str(ac_appellantNationalitiesDescription.count())} cases have been found where the appellantNationalitiesDescription field does not match the NationalityId field entries." ,test_from_state)
    else:
        return TestResult("appellantNationalitiesDescription", "PASS", f"appellantNationalitiesDescription acceptance criteria passed, all appellantNationalitiesDescription fields match the NationalityId field entries.",test_from_state)

#######################
#deportationOrderOptions - 'CategoryId' is in 17, 19, 29 ,30, 31, 32, 41, 48 and 'deportationOrderOptions' is not Yes
#######################
def test_deportationOrderOptions_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 17)) |
        (array_contains(col("CategoryIds"), 19)) |
        (array_contains(col("CategoryIds"), 29)) |
        (array_contains(col("CategoryIds"), 30)) |
        (array_contains(col("CategoryIds"), 31)) |
        (array_contains(col("CategoryIds"), 32)) |
        (array_contains(col("CategoryIds"), 41)) |
        (array_contains(col("CategoryIds"), 48))
        ).count() == 0:
        return TestResult("deportationOrderOptions", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_deportationOrderOptions = test_df.filter(
        (
        (array_contains(col("CategoryIds"), 17)) |
        (array_contains(col("CategoryIds"), 19)) |
        (array_contains(col("CategoryIds"), 29)) |
        (array_contains(col("CategoryIds"), 30)) |
        (array_contains(col("CategoryIds"), 31)) |
        (array_contains(col("CategoryIds"), 32)) |
        (array_contains(col("CategoryIds"), 41)) |
        (array_contains(col("CategoryIds"), 48))
        ) & (~col("deportationOrderOptions").isin("Yes"))
    )

    if ac_deportationOrderOptions.count() != 0:
        return TestResult("deportationOrderOptions", "FAIL", f"deportationOrderOptions acceptance criteria failed: {str(ac_appellantNationalitiesDescription.count())} cases have been found where CategoryId' is in 17, 19, 29 ,30, 31, 32, 41, 48 and 'deportationOrderOptions' is not Yes" ,test_from_state)
    else:
        return TestResult("deportationOrderOptions", "PASS", f"deportationOrderOptions acceptance criteria passed, when CategoryId' is in 17, 19, 29 ,30, 31, 32, 41, 48, 'deportationOrderOptions' is always Yes",test_from_state)

#######################
#deportationOrderOptions - 'CasePrefix' is 'DA' and 'deportationOrderOptions' is not Yes
#######################
def test_deportationOrderOptions_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("CasePrefix") == "DA"
        ).count() == 0:
        return TestResult("deportationOrderOptions", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_deportationOrderOptions = test_df.filter(
        (col("CasePrefix") == "DA") & (col("deportationOrderOptions") != ("Yes"))
    )

    if ac_deportationOrderOptions.count() != 0:
        return TestResult("deportationOrderOptions", "FAIL", f"deportationOrderOptions acceptance criteria failed: {str(ac_appellantNationalitiesDescription.count())} cases have been found where 'CasePrefix' is 'DA' and 'deportationOrderOptions' is not Yes." ,test_from_state)
    else:
        return TestResult("deportationOrderOptions", "PASS", f"deportationOrderOptions acceptance criteria passed, where CasePrefix' is 'DA', 'deportationOrderOptions' is always Yes",test_from_state)

#######################
#deportationOrderOptions - 'DeportationDate' is not null and 'deportationOrderOptions' is not Yes
#######################
def test_deportationOrderOptions_ac3(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("DeportationDate").isNotNull()
        ).count() == 0:
        return TestResult("deportationOrderOptions", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_deportationOrderOptions = test_df.filter(
        col("DeportationDate").isNotNull() & (col("deportationOrderOptions") != ("Yes"))
    )

    if ac_deportationOrderOptions.count() != 0:
        return TestResult("deportationOrderOptions", "FAIL", f"deportationOrderOptions acceptance criteria failed: {str(ac_appellantNationalitiesDescription.count())} cases have been found where 'DeportationDate' is not null and 'deportationOrderOptions' is not Yes" ,test_from_state)
    else:
        return TestResult("deportationOrderOptions", "PASS", f"deportationOrderOptions acceptance criteria passed, if 'DeportationDate' is not null, 'deportationOrderOptions' is always not Yes",test_from_state)
    
#######################
#deportationOrderOptions - 'RemovalDate' is not null and 'deportationOrderOptions' is not Yes
#######################
def test_deportationOrderOptions_ac4(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("RemovalDate").isNotNull()
        ).count() == 0:
        return TestResult("deportationOrderOptions", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_deportationOrderOptions = test_df.filter(
        col("RemovalDate").isNotNull() & (col("deportationOrderOptions") != ("Yes"))
    )

    if ac_deportationOrderOptions.count() != 0:
        return TestResult("deportationOrderOptions", "FAIL", f"deportationOrderOptions acceptance criteria failed: {str(ac_appellantNationalitiesDescription.count())} cases have been found where 'RemovalDate' is not null and 'deportationOrderOptions' is not Yes" ,test_from_state)
    else:
        return TestResult("deportationOrderOptions", "PASS", f"deportationOrderOptions acceptance criteria passed, if 'RemovalDate' is not null, 'deportationOrderOptions' is always not Yes",test_from_state)

############################################################################################

#######################
#partyIds Init Code
#######################
def test_partyIds_init(json, M1_silver):
    try:
        test_df = json.select(
            "AppealReferenceNumber",
            "appellantPartyId",
            "appellantsRepresentation",
            "legalRepIndividualPartyId",
            "legalRepOrganisationPartyId",
            "hasSponsor",
            "sponsorGivenNames",
            "sponsorPartyId"
        )

        M1_silver = M1_silver.select(
            "CaseNo",
            "dv_representation"
        )

        test_df = test_df.join(
            M1_silver,
            M1_silver.CaseNo == test_df.AppealReferenceNumber,
            "inner"
        )        
        
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("partyIds", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state)
    
#######################
#uuid checker
#######################  
import uuid
from pyspark.sql.types import BooleanType

def is_valid_uuid(val):
    if val is None:
        return True # We usually handle nulls separately
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False

is_valid_uuid_udf = udf(is_valid_uuid, BooleanType())
    
#######################
#duplicate_id_checker
#######################    
def duplicate_id_checker(input_id_column, input_data):
    # 1. Filter out nulls
    non_null_df = input_data.filter(col(input_id_column).isNotNull())

    # 2. Check for INVALID UUID formats
    invalid_uuids = non_null_df.withColumn(
        "is_valid", is_valid_uuid_udf(col(input_id_column))
    ).filter(col("is_valid") == False)
    
    invalid_count = invalid_uuids.count()

    # 3. Check for DUPLICATES (your existing logic)
    duplicate_instances = (
        non_null_df
        .groupBy(input_id_column)
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
    )
    duplicate_count = duplicate_instances.count()

    # 4. Combine results
    if duplicate_count == 0 and invalid_count == 0:
        return True, f"All IDs are unique and valid UUIDs.", duplicate_instances
    
    error_msg = ""
    if duplicate_count > 0:
        error_msg += f"Found {duplicate_count} duplicates. "
    if invalid_count > 0:
        error_msg += f"Found {invalid_count} invalid UUID formats."
        
    return False, error_msg, duplicate_instances


#######################
#appellantPartyId  
#######################
def test_appellantPartyId(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("appellantPartyId").isNotNull()) &
        ((col("dv_representation") == "LR") | (col("dv_representation") == "AIP")) & 
        (is_valid_uuid_udf(col("appellantPartyId")) == False)
        ).count() == 0:
        return TestResult("appellantPartyId", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    test_passed, appellantPartyId_result, duplicate_instances = duplicate_id_checker("appellantPartyId", test_df)

    if test_passed == True:
        return TestResult("appellantPartyId", "PASS", appellantPartyId_result, test_from_state)
    else:
        return TestResult("appellantPartyId", "FAIL", appellantPartyId_result, test_from_state)

#######################
#legalRepIndividualPartyId
#######################
def test_legalRepIndividualPartyId_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("appellantsRepresentation") == "Yes") &
        (col("dv_representation") == "LR")
        ).count() == 0:
        return TestResult("appellantPartyId", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_legalRepIndividualPartyId = test_df.filter(
        (col("appellantsRepresentation") == "Yes" & (col("dv_representation") == "LR")) & (col("legalRepIndividualPartyId").isNotNull())
    )

    if ac_legalRepIndividualPartyId.count() != 0:
        return TestResult("legalRepIndividualPartyId", "FAIL", f"legalRepIndividualPartyId acceptance criteria failed: {str(ac_legalRepIndividualPartyId.count())} cases have been found where 'appellantsRepresentation' is Yes and 'legalRepIndividualPartyId' is not null", test_from_state)
    else:
        return TestResult("legalRepIndividualPartyId", "PASS", f"legalRepIndividualPartyId acceptance criteria passed, where 'appellantsRepresentation' is Yes, all 'legalRepIndividualPartyId' is null", test_from_state)

#######################
#legalRepIndividualPartyId  
#######################
def test_legalRepIndividualPartyId_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("legalRepIndividualPartyId").isNotNull()) &
        (col("dv_representation") == "LR")
        ).count() == 0:
        return TestResult("legalRepIndividualPartyId", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    test_passed, legalRepIndividualPartyId_result, duplicate_instances = duplicate_id_checker("legalRepIndividualPartyId", test_df)

    if test_passed == True:
        return TestResult("legalRepIndividualPartyId", "PASS", legalRepIndividualPartyId_result, test_from_state)
    else:
        return TestResult("legalRepIndividualPartyId", "FAIL", legalRepIndividualPartyId_result, test_from_state)

#######################
#legalRepOrganisationPartyId
#######################
def test_legalRepOrganisationPartyId_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("appellantsRepresentation") == "Yes") &
        (col("dv_representation") == "LR")
        ).count() == 0:
        return TestResult("legalRepOrganisationPartyId", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_legalRepOrganisationPartyId = test_df.filter(
        (col("appellantsRepresentation") == "Yes") & (col("legalRepOrganisationPartyId").isNotNull())
    )

    if ac_legalRepOrganisationPartyId.count() != 0:
        return TestResult("legalRepOrganisationPartyId", "FAIL", f"legalRepOrganisationPartyId acceptance criteria failed: {str(ac_legalRepOrganisationPartyId.count())} cases have been found where 'appellantsRepresentation' is Yes and 'legalRepOrganisationPartyId' is not null", test_from_state)
    else:
        return TestResult("legalRepOrganisationPartyId", "PASS", f"legalRepOrganisationPartyId acceptance criteria passed, where 'appellantsRepresentation' is Yes, all 'legalRepOrganisationPartyId' is null", test_from_state)

#######################
#legalRepOrganisationPartyId  
#######################
def test_legalRepOrganisationPartyId_ac2(test_df):
    #Check we have Records To test
    test_df = test_df.filter(
        (col("legalRepOrganisationPartyId").isNotNull()) &
        (col("dv_representation") == "LR")
        )
    if test_df.count() == 0:
        return TestResult("legalRepOrganisationPartyId", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    test_passed, legalRepOrganisationPartyId_result, duplicate_instances = duplicate_id_checker("legalRepOrganisationPartyId", test_df)

    if test_passed == True:
        return TestResult("legalRepOrganisationPartyId", "PASS", legalRepOrganisationPartyId_result, test_from_state)
    else:
        return TestResult("legalRepOrganisationPartyId", "FAIL", legalRepOrganisationPartyId_result, test_from_state)

#######################
#sponsorPartyId
#######################
def test_sponsorPartyId_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("hasSponsor") != "Yes") &
        (col("sponsorGivenNames").isNull()) &
        ((col("dv_representation") == "LR") | (col("dv_representation") == "AIP"))
        ).count() == 0:
        return TestResult("sponsorPartyId", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_sponsorPartyId = test_df.filter(
        ((col("hasSponsor") != "Yes") & (col("sponsorGivenNames").isNull())) & 
        (col("sponsorPartyId").isNotNull())
    )

    if ac_sponsorPartyId.count() != 0:
        return TestResult("sponsorPartyId", "FAIL", f"sponsorPartyId acceptance criteria failed: {str(ac_sponsorPartyId.count())} cases have been found where 'hasSponsor' is not Yes and 'sponsorGivenNames' is null and 'sponsorPartyId' is not null", test_from_state)
    else:
        return TestResult("sponsorPartyId", "PASS", f"sponsorPartyId acceptance criteria passed, where 'hasSponsor' is not Yes and sponsorGivenNames' is null, all 'sponsorPartyId' are not null", test_from_state)

#######################
#sponsorPartyId  
#######################
def test_sponsorPartyId_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("sponsorPartyId").isNotNull()
        ).count() == 0:
        return TestResult("sponsorPartyId", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    test_passed, sponsorPartyId_result, duplicate_instances = duplicate_id_checker("sponsorPartyId", test_df)

    if test_passed == True:
        return TestResult("sponsorPartyId", "PASS", sponsorPartyId_result, test_from_state)
    else:
        return TestResult("sponsorPartyId", "FAIL", sponsorPartyId_result, test_from_state)
    
#######################
#sponsorPartyId  
#######################
def test_sponsorPartyId_ac3(test_df):
    #Check we have Records To test
    if test_df.filter(
        ~(array_contains(col("CategoryIds"), 38))
        ).count() == 0:
        return TestResult("sponsorPartyId", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_sponsorPartyId = test_df.filter(
        (~(array_contains(col("CategoryIds"), 38))) & (col("sponsorPartyId").isNotNull())
    )

    if ac_sponsorPartyId.count() != 0:
        return TestResult("sponsorPartyId", "FAIL", f"sponsorPartyId acceptance criteria failed: {str(ac_sponsorPartyId.count())} cases have been found where CategoryId is not 38 and 'sponsorPartyId' is not omitted", test_from_state)
    else:
        return TestResult("sponsorPartyId", "PASS", f"sponsorPartyId acceptance criteria passed, where 'CategoryId' is not 38 all 'sponsorPartyId' are omitted", test_from_state)

############################################################################################

#######################
#payment Init Code
#######################
def test_payment_init(json, M1_bronze):
    try:
        json = json.select(
            col("appealReferenceNumber"),
            col("feeAmountGbp"),
            col("feeDescription"),
            col("feeWithHearing"),
            col("feeWithoutHearing"),
            col("paymentDescription"),
            col("decisionHearingFeeOption")
        )

        M1_bronze = M1_bronze.select(
            col("CaseNo"),
            col("VisitVisaType")
        )
            
        test_df = json.join(
            M1_bronze,
            M1_bronze.CaseNo == json.appealReferenceNumber,
            how = "inner"
        )
        
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("payment", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state)
#######################
#feeAmountGbp - Where M1.VisitVisaType = 1 and feeAmountGbp != 8000 
#######################
def test_feeAmountGbp_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("VisitVisaType") == 1
        ).count() == 0:
        return TestResult("feeAmountGbp", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_feeAmountGbp = test_df.filter(
        (col("VisitVisaType") == 1) & (col("feeAmountGbp").cast(StringType()) != "8000")
    )

    if ac_feeAmountGbp.count() != 0:
        return TestResult("feeAmountGbp", "FAIL", f"feeAmountGbp acceptance criteria failed: {str(ac_feeAmountGbp.count())} cases have been found where M1.VisitVisaType = 1 and feeAmountGbp != 8000 " ,test_from_state)
    else:
        return TestResult("feeAmountGbp", "PASS", f"feeAmountGbp acceptance criteria passed, all cases where M1.VisitVisaType = 1 have feeAmountGbp = 8000",test_from_state)
    
#######################
#feeAmountGbp - Where M1.VisitVisaType = 2 and feeAmountGbp != 14000 
#######################
def test_feeAmountGbp_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("VisitVisaType") == 2
        ).count() == 0:
        return TestResult("feeAmountGbp", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_feeAmountGbp = test_df.filter(
        (col("VisitVisaType") == 2) & (col("feeAmountGbp").cast(StringType()) != "14000")
    )

    if ac_feeAmountGbp.count() != 0:
        return TestResult("feeAmountGbp", "FAIL", f"feeAmountGbp acceptance criteria failed: {str(ac_feeAmountGbp.count())} cases have been found where M1.VisitVisaType = 2 and feeAmountGbp != 14000 " ,test_from_state)
    else:
        return TestResult("feeAmountGbp", "PASS", f"feeAmountGbp acceptance criteria passed, all cases where M1.VisitVisaType = 2 have feeAmountGbp = 14000",test_from_state)

#######################
#feeDescription - Where M1.VisitVisaType = 1 and feeDescription != Notice of Appeal - appellant consents without hearing A
#######################
def test_feeDescription_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("VisitVisaType") == 1
        ).count() == 0:
        return TestResult("feeDescription", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_feeDescription = test_df.filter(
        (col("VisitVisaType") == 1) & (col("feeDescription") != "Notice of Appeal - appellant consents without hearing A")
    )

    if ac_feeDescription.count() != 0:
        return TestResult("feeDescription", "FAIL", f"feeDescription acceptance criteria failed: {str(ac_feeDescription.count())} cases have been found where M1.VisitVisaType = 1 and feeDescription != Notice of Appeal - appellant consents without hearing A" ,test_from_state)
    else:
        return TestResult("feeDescription", "PASS", f"feeDescription acceptance criteria passed, all cases where M1.VisitVisaType = 1 have feeDescription = Notice of Appeal - appellant consents without hearing A",test_from_state)

#######################
#feeDescription - Where M1.VisitVisaType = 2 and feeDescription != Appeal determined with a hearing 
#######################
def test_feeDescription_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("VisitVisaType") == 2
        ).count() == 0:
        return TestResult("feeDescription", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_feeDescription = test_df.filter(
        (col("VisitVisaType") == 2) & (col("feeDescription") != "Appeal determined with a hearing")
    )

    if ac_feeDescription.count() != 0:
        return TestResult("feeDescription", "FAIL", f"feeDescription acceptance criteria failed: {str(ac_feeDescription.count())} cases have been found where M1.VisitVisaType = 2 and feeDescription != Appeal determined with a hearing " ,test_from_state)
    else:
        return TestResult("feeDescription", "PASS", f"feeDescription acceptance criteria passed, all cases where M1.VisitVisaType = 1 have feeDescription = Appeal determined with a hearing ",test_from_state)
    
#######################
#feeWithHearing - Where M1.VisitVisaType = 1 and feeWithHearing not omitted
#######################
def test_feeWithHearing_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("VisitVisaType") == 1
        ).count() == 0:
        return TestResult("feeWithHearing", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_feeWithHearing = test_df.filter(
        (col("VisitVisaType") == 1) & (col("feeWithHearing").isNotNull())
    )

    if ac_feeWithHearing.count() != 0:
        return TestResult("feeWithHearing", "FAIL", f"feeWithHearing acceptance criteria failed: {str(ac_feeWithHearing.count())} cases have been found where M1.VisitVisaType = 1 and feeWithHearing not omitted" ,test_from_state)
    else:
        return TestResult("feeWithHearing", "PASS", f"feeWithHearing acceptance criteria passed, all cases where M1.VisitVisaType = 1 have feeWithHearing omitted",test_from_state)
    
#######################
#feeWithHearing - Where M1.VisitVisaType = 2 and feeWithHearing != 140
#######################
def test_feeWithHearing_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("VisitVisaType") == 2
        ).count() == 0:
        return TestResult("feeWithHearing", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_feeWithHearing = test_df.filter(
        (col("VisitVisaType") == 1) & (col("feeWithHearing").cast(StringType()) != "140")
    )

    if ac_feeWithHearing.count() != 0:
        return TestResult("feeWithHearing", "FAIL", f"feeWithHearing acceptance criteria failed: {str(ac_feeWithHearing.count())} cases have been found where M1.VisitVisaType = 1 and feeWithHearing != 140" ,test_from_state)
    else:
        return TestResult("feeWithHearing", "PASS", f"feeWithHearing acceptance criteria passed, all cases where M1.VisitVisaType = 1 have feeWithHearing = 140",test_from_state)

#######################
#feeWithoutHearing - Where M1.VisitVisaType = 1 and feeWithoutHearing != 80
#######################
def test_feeWithoutHearing_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("VisitVisaType") == 1
        ).count() == 0:
        return TestResult("feeWithoutHearing", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_feeWithoutHearing = test_df.filter(
        (col("VisitVisaType") == 1) & (col("feeWithoutHearing").cast(StringType()) != "80")
    )

    if ac_feeWithoutHearing.count() != 0:
        return TestResult("feeWithoutHearing", "FAIL", f"feeWithoutHearing acceptance criteria failed: {str(ac_feeWithoutHearing.count())} cases have been found where M1.VisitVisaType = 1 and feeWithoutHearing != 80" ,test_from_state)
    else:
        return TestResult("feeWithoutHearing", "PASS", f"feeWithoutHearing acceptance criteria passed, all cases where M1.VisitVisaType = 1 have feeWithHearing = 80",test_from_state)

#######################
#feeWithoutHearing - Where M1.VisitVisaType = 2 and feeWithoutHearing is not omitted
#######################
def test_feeWithoutHearing_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("VisitVisaType") == 2
        ).count() == 0:
        return TestResult("feeWithoutHearing", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_feeWithoutHearing = test_df.filter(
        (col("VisitVisaType") == 2) & (col("feeWithoutHearing").isNotNull())
    )

    if ac_feeWithoutHearing.count() != 0:
        return TestResult("feeWithoutHearing", "FAIL", f"feeWithoutHearing acceptance criteria failed: {str(ac_feeWithoutHearing.count())} cases have been found where M1.VisitVisaType = 1 and feeWithoutHearing is not omitted" ,test_from_state)
    else:
        return TestResult("feeWithoutHearing", "PASS", f"feeWithoutHearing acceptance criteria passed, all cases where M1.VisitVisaType = 2 have feeWithHearing omitted",test_from_state)

#######################
#paymentDescription - Where M1.VisitVisaType = 1 and paymentDescription != Appeal determined without a hearing
#######################
def test_paymentDescription_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("VisitVisaType") == 1
        ).count() == 0:
        return TestResult("paymentDescription", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_paymentDescription = test_df.filter(
        (col("VisitVisaType") == 1) & (col("paymentDescription") != "Appeal determined without a hearing")
    )

    if ac_paymentDescription.count() != 0:
        return TestResult("paymentDescription", "FAIL", f"paymentDescription acceptance criteria failed: {str(ac_paymentDescription.count())} cases have been found where M1.VisitVisaType = 1 and paymentDescription != Appeal determined without a hearing" ,test_from_state)
    else:
        return TestResult("paymentDescription", "PASS", f"paymentDescription acceptance criteria passed, all cases where M1.VisitVisaType = 1 have paymentDescription = Appeal determined without a hearing",test_from_state)

#######################
#paymentDescription - Where M1.VisitVisaType = 2 and paymentDescription != Appeal determined with a hearing
#######################
def test_paymentDescription_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("VisitVisaType") == 2
        ).count() == 0:
        return TestResult("paymentDescription", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_paymentDescription = test_df.filter(
        (col("VisitVisaType") == 2) & (col("paymentDescription") != "Appeal determined with a hearing")
    )

    if ac_paymentDescription.count() != 0:
        return TestResult("paymentDescription", "FAIL", f"paymentDescription acceptance criteria failed: {str(ac_paymentDescription.count())} cases have been found where M1.VisitVisaType = 2 and paymentDescription != Appeal determined with a hearing" ,test_from_state)
    else:
        return TestResult("paymentDescription", "PASS", f"paymentDescription acceptance criteria passed, all cases where M1.VisitVisaType = 2 have paymentDescription = Appeal determined with a hearing",test_from_state)

#######################
#decisionHearingFeeOption - Where M1.VisitVisaType = 1 and decisionHearingFeeOption != decisionWithoutHearing
#######################
def test_decisionHearingFeeOption_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("VisitVisaType") == 1
        ).count() == 0:
        return TestResult("decisionHearingFeeOption", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_decisionHearingFeeOption = test_df.filter(
        (col("VisitVisaType") == 1) & (col("decisionHearingFeeOption") != "decisionWithoutHearing")
    )

    if ac_decisionHearingFeeOption.count() != 0:
        return TestResult("decisionHearingFeeOption", "FAIL", f"decisionHearingFeeOption acceptance criteria failed: {str(ac_decisionHearingFeeOption.count())} cases have been found where M1.VisitVisaType = 1 and decisionHearingFeeOption != decisionWithoutHearing" ,test_from_state)
    else:
        return TestResult("decisionHearingFeeOption", "PASS", f"decisionHearingFeeOption acceptance criteria passed, all cases where M1.VisitVisaType = 1 have decisionHearingFeeOption = decisionWithoutHearing",test_from_state)
    
#######################
#decisionHearingFeeOption - Where M1.VisitVisaType = 2 and decisionHearingFeeOption != decisionWithHearing
#######################
def test_decisionHearingFeeOption_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("VisitVisaType") == 2
        ).count() == 0:
        return TestResult("decisionHearingFeeOption", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_decisionHearingFeeOption = test_df.filter(
        (col("VisitVisaType") == 2) & (col("decisionHearingFeeOption") != "decisionWithHearing")
    )

    if ac_decisionHearingFeeOption.count() != 0:
        return TestResult("decisionHearingFeeOption", "FAIL", f"decisionHearingFeeOption acceptance criteria failed: {str(ac_decisionHearingFeeOption.count())} cases have been found where M1.VisitVisaType = 2 and decisionHearingFeeOption != decisionWithHearing" ,test_from_state)
    else:
        return TestResult("decisionHearingFeeOption", "PASS", f"decisionHearingFeeOption acceptance criteria passed, all cases where M1.VisitVisaType = 2 have decisionHearingFeeOption = decisionWithHearing",test_from_state)
    
############################################################################################

#######################
#homeOffice Init Code
#######################
def test_homeOffice_init(json, C, M1_bronze, M2_bronze):
    try:
        json = json.select(
            "appealReferenceNumber",
            "decisionLetterReceivedDate",
            "dateEntryClearanceDecision",
            # "homeOfficeReferenceNumber",
            "gwfReferenceNumber",
            "homeOfficeDecisionDate",
            "oocAppealAdminJ"
        )

        C = C.select(
            "CaseNo",
            "CategoryId"
        )

        M1_bronze = M1_bronze.select(
            "CaseNo",
            "HORef",
            "DateOfApplicationDecision"
        )

        M2_bronze = M2_bronze.select(
            "CaseNo",
            "FCONumber"
        )

        test_df = json.join(
            M1_bronze,
            json["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        )

        test_df = test_df.join(
            M2_bronze,
            test_df["appealReferenceNumber"] == M2_bronze["CaseNo"],
            "inner"
        )

        test_df = test_df.join(
            C,
            test_df["appealReferenceNumber"] == C["CaseNo"],
            "inner"
        )

        test_df = test_df.select(
            "appealReferenceNumber",
            "oocAppealAdminJ",
            "decisionLetterReceivedDate",
            "dateEntryClearanceDecision",
            # "homeOfficeReferenceNumber",
            "gwfReferenceNumber",
            "homeOfficeDecisionDate",
            "DateOfApplicationDecision",
            "CategoryId",
            M1_bronze["HORef"],
            M2_bronze["FCONumber"]
        )

        test_df = (
            test_df
            .groupBy("appealReferenceNumber")
            .agg(
                collect_list("CategoryId").alias("CategoryIds"),
                first("oocAppealAdminJ").alias("oocAppealAdminJ"),
                first("decisionLetterReceivedDate").alias("decisionLetterReceivedDate"),
                first("dateEntryClearanceDecision").alias("dateEntryClearanceDecision"),
                # first("homeOfficeReferenceNumber").alias("homeOfficeReferenceNumber"),
                first("gwfReferenceNumber").alias("gwfReferenceNumber"),
                first("homeOfficeDecisionDate").alias("homeOfficeDecisionDate"),
                first("DateOfApplicationDecision").alias("DateOfApplicationDecision"),
                first(M1_bronze["HORef"]).alias("HORef_M1"),
                first(M2_bronze["FCONumber"]).alias("FCONumber")
            )
        )

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("homeOffice", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state)
    
#######################
#homeOfficeDecisionDate - If CategoryId not in 37 and homeOfficeDecisionDate not omitted
#######################
def test_homeOfficeDecisionDate_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (~(array_contains(col("CategoryIds"), 37)))
        ).count() == 0:
        return TestResult("homeOfficeDecisionDate", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_homeOfficeDecisionDate = test_df.filter(
        (~(array_contains(col("CategoryIds"), 37))) & col("homeOfficeDecisionDate").isNotNull()
    )

    if ac_homeOfficeDecisionDate.count() != 0:
        return TestResult("homeOfficeDecisionDate", "FAIL", f"homeOfficeDecisionDate acceptance criteria failed: {str(ac_homeOfficeDecisionDate.count())} cases have been found where CategoryId not in 37 and homeOfficeDecisionDate not omitted" ,test_from_state)
    else:
        return TestResult("homeOfficeDecisionDate", "PASS", f"homeOfficeDecisionDate acceptance criteria passed, all cases where CategoryId not in 37, homeOfficeDecisionDate is always omitted",test_from_state)

#######################
#homeOfficeDecisionDate - If CategoryId in 37 and homeOfficeDecisionDate != M1.DateOfApplicationDecision
#######################
def test_homeOfficeDecisionDate_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 37))
        ).count() == 0:
        return TestResult("homeOfficeDecisionDate", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_homeOfficeDecisionDate = test_df.filter(
        (array_contains(col("CategoryIds"), 37)) & (col("homeOfficeDecisionDate") != col("DateOfApplicationDecision"))
    )

    if ac_homeOfficeDecisionDate.count() != 0:
        return TestResult("homeOfficeDecisionDate", "FAIL", f"homeOfficeDecisionDate acceptance criteria failed: {str(ac_homeOfficeDecisionDate.count())} cases have been found where CategoryId in 37 and homeOfficeDecisionDate != M1.DateOfApplicationDecision" ,test_from_state)
    else:
        return TestResult("homeOfficeDecisionDate", "PASS", f"homeOfficeDecisionDate acceptance criteria passed, all cases where CategoryId in 37, homeOfficeDecisionDate is always equal to M1.DateOfApplicationDecision",test_from_state)

#######################
#decisionLetterReceivedDate - If CategoryId not in 38 and decisionLetterReceivedDate not omitted
#######################
def test_decisionLetterReceivedDate_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (~(array_contains(col("CategoryIds"), 38)))
        ).count() == 0:
        return TestResult("decisionLetterReceivedDate", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_decisionLetterReceivedDate = test_df.filter(
        (array_contains(col("CategoryIds"), 37)) & (col("decisionLetterReceivedDate").isNotNull())
    )

    if ac_decisionLetterReceivedDate.count() != 0:
        return TestResult("decisionLetterReceivedDate", "FAIL", f"decisionLetterReceivedDate acceptance criteria failed: {str(ac_decisionLetterReceivedDate.count())} cases have been found where CategoryId in 38 and decisionLetterReceivedDate not omitted" ,test_from_state)
    else:
        return TestResult("decisionLetterReceivedDate", "PASS", f"decisionLetterReceivedDate acceptance criteria passed, all cases where CategoryId in 38, decisionLetterReceivedDate is always omitted",test_from_state)

#######################
#decisionLetterReceivedDate - IF CategoryId in [38] + M1.HORef OR M2.FCONumber NOT LIKE '%GWF%' and decisionLetterReceivedDate != M1.DateOfApplicationDecision
#######################
def test_decisionLetterReceivedDate_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("HORef_M1").isNotNull()) & 
        (col("FCONumber").isNotNull())
        ).count() == 0:
        return TestResult("decisionLetterReceivedDate", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_decisionLetterReceivedDate = test_df.filter(
    (
        (array_contains(col("CategoryIds"), 38)) &
        (
            ~col("HORef_M1").rlike("GWF") | ~col("FCONumber").rlike("GWF")
        )
        
    ) & (col("decisionLetterReceivedDate") != col("DateOfApplicationDecision"))
    )

    if ac_decisionLetterReceivedDate.count() != 0:
        return TestResult("decisionLetterReceivedDate", "FAIL", f"decisionLetterReceivedDate acceptance criteria failed: {str(ac_decisionLetterReceivedDate.count())} cases have been found where CategoryId in [38] + M1.HORef OR M2.FCONumber NOT LIKE '%GWF%' and decisionLetterReceivedDate != M1.DateOfApplicationDecision" ,test_from_state)
    else:
        return TestResult("decisionLetterReceivedDate", "PASS", f"decisionLetterReceivedDate acceptance criteria passed, all cases where CategoryId in [38] + M1.HORef OR M2.FCONumber NOT LIKE '%GWF%', decisionLetterReceivedDate always equals M1.DateOfApplicationDecision",test_from_state)
    
#######################
#decisionLetterReceivedDate - IF CategoryId in [38] + M1.HORef OR M2.FCONumber LIKE '%GWF%' and decisionLetterReceivedDate not omitted
#######################
def test_decisionLetterReceivedDate_ac3(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("HORef_M1").isNotNull()) & 
        (col("FCONumber").isNotNull())
        ).count() == 0:
        return TestResult("decisionLetterReceivedDate", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_decisionLetterReceivedDate = test_df.filter(
    (
        (array_contains(col("CategoryIds"), 38)) &
        (
            col("HORef_M1").rlike("GWF") | col("FCONumber").rlike("GWF")
        )
        
    ) & (col("decisionLetterReceivedDate").isNotNull())
    )

    if ac_decisionLetterReceivedDate.count() != 0:
        return TestResult("decisionLetterReceivedDate", "FAIL", f"decisionLetterReceivedDate acceptance criteria failed: {str(ac_decisionLetterReceivedDate.count())} cases have been found where CategoryId in [38] + M1.HORef OR M2.FCONumber LIKE '%GWF%' and decisionLetterReceivedDate not omitted" ,test_from_state)
    else:
        return TestResult("decisionLetterReceivedDate", "PASS", f"decisionLetterReceivedDate acceptance criteria passed, all cases where CategoryId in [38] + M1.HORef OR M2.FCONumber NOT LIKE '%GWF%', decisionLetterReceivedDate always omitted",test_from_state)

#######################
#dateEntryClearanceDecision - If CategoryId not in 38 and dateEntryClearanceDecision not omitted
#######################
def test_dateEntryClearanceDecision_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (~(array_contains(col("CategoryIds"), 38)))
        ).count() == 0:
        return TestResult("dateEntryClearanceDecision", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_dateEntryClearanceDecision = test_df.filter(
        (array_contains(col("CategoryIds"), 38)) & (col("dateEntryClearanceDecision").isNotNull())
    )

    if ac_dateEntryClearanceDecision.count() != 0:
        return TestResult("dateEntryClearanceDecision", "FAIL", f"dateEntryClearanceDecision acceptance criteria failed: {str(ac_dateEntryClearanceDecision.count())} cases have been found where CategoryId in 38 and dateEntryClearanceDecision not omitted" ,test_from_state)
    else:
        return TestResult("dateEntryClearanceDecision", "PASS", f"dateEntryClearanceDecision acceptance criteria passed, all cases where CategoryId in 38, dateEntryClearanceDecision is always omitted",test_from_state)

#######################
#dateEntryClearanceDecision - IF CategoryId in [38] + M1.HORef OR M2.FCONumber LIKE '%GWF%' and dateEntryClearanceDecision != M1.DateOfApplicationDecision
#######################
def test_dateEntryClearanceDecision_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("HORef_M1").isNotNull()) & 
        (col("FCONumber").isNotNull())
        ).count() == 0:
        return TestResult("dateEntryClearanceDecision", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_dateEntryClearanceDecision = test_df.filter(
    (
        (array_contains(col("CategoryIds"), 38)) &
        (
            col("HORef_M1").rlike("GWF") | col("FCONumber").rlike("GWF")
        )
        
    ) & (col("dateEntryClearanceDecision") != col("DateOfApplicationDecision"))
    )

    if ac_dateEntryClearanceDecision.count() != 0:
        return TestResult("dateEntryClearanceDecision", "FAIL", f"dateEntryClearanceDecision acceptance criteria failed: {str(ac_dateEntryClearanceDecision.count())} cases have been found where CategoryId in [38] + M1.HORef OR M2.FCONumber LIKE '%GWF%' and dateEntryClearanceDecision != M1.DateOfApplicationDecision" ,test_from_state)
    else:
        return TestResult("dateEntryClearanceDecision", "PASS", f"dateEntryClearanceDecision acceptance criteria passed, all cases where CategoryId in [38] + M1.HORef OR M2.FCONumber LIKE '%GWF%', dateEntryClearanceDecision always equals M1.DateOfApplicationDecision",test_from_state)

#######################
#dateEntryClearanceDecision - IF CategoryId in [38] + CleansedHORef OR M1.HORef OR M2.FCONumber NOT LIKE '%GWF%' and dateEntryClearanceDecision not omitted
#######################
def test_dateEntryClearanceDecision_ac3(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("HORef_M1").isNotNull()) & 
        (col("FCONumber").isNotNull())
        ).count() == 0:
        return TestResult("dateEntryClearanceDecision", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_dateEntryClearanceDecision = test_df.filter(
    (
        (array_contains(col("CategoryIds"), 38)) &
        (
            ~col("HORef_M1").rlike("GWF") | ~col("FCONumber").rlike("GWF")
        )
        
    ) & (col("dateEntryClearanceDecision").isNotNull())
    )

    if ac_dateEntryClearanceDecision.count() != 0:
        return TestResult("dateEntryClearanceDecision", "FAIL", f"dateEntryClearanceDecision acceptance criteria failed: {str(ac_dateEntryClearanceDecision.count())} cases have been found where CategoryId in [38] + M1.HORef OR M2.FCONumber NOT LIKE '%GWF%' and dateEntryClearanceDecision is not omitted" ,test_from_state)
    else:
        return TestResult("dateEntryClearanceDecision", "PASS", f"dateEntryClearanceDecision acceptance criteria passed, all cases where CategoryId in [38] + M1.HORef OR M2.FCONumber NOT LIKE '%GWF%', dateEntryClearanceDecision always omitted",test_from_state)
    
#######################
#homeOfficeReferenceNumber Init Code
#######################
def test_homeOfficeReferenceNumber_init(json, C, M1_bronze, M2_bronze):
    try:
        json = json.select(
            "appealReferenceNumber",
            "homeOfficeReferenceNumber",
        )

        C = C.select(
            "CaseNo",
            "CategoryId"
        )

        M1_bronze = M1_bronze.select(
            "CaseNo",
            "HORef",
        )

        M2_bronze = M2_bronze.select(
            "CaseNo",
            "FCONumber"
        )

        test_df = json.join(
            M1_bronze,
            json["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        )

        test_df = test_df.join(
            M2_bronze,
            test_df["appealReferenceNumber"] == M2_bronze["CaseNo"],
            "inner"
        )

        test_df = test_df.select(
            "appealReferenceNumber",
            "homeOfficeReferenceNumber",
            "CategoryId",
            M1_bronze["HORef"],
            M2_bronze["FCONumber"]
        )

        ho_test_df = (
            test_df
            .groupBy("appealReferenceNumber")
            .agg(
                collect_list("CategoryId").alias("CategoryIds"),
                first("homeOfficeReferenceNumber").alias("homeOfficeReferenceNumber"),
                first(M1_bronze["HORef"]).alias("HORef_M1"),
                first(M2_bronze["FCONumber"]).alias("FCONumber")
            )
        )

        return ho_test_df, True
    except Exception as e:
        ho_test_df = None
        error_message = str(e)        
        return TestResult("homeOfficeReferenceNumber", "FAIL",f"Failed to Setup Data for Test - homeOfficeReferenceNumber does not exist in the payload",test_from_state), ho_test_df
    
def test_homeOfficeReferenceNumber_ac1(ho_test_df):
    if ho_test_df != None:
        #Check we have Records To test
        test_df = ho_test_df
        
        if test_df.filter(
            (array_contains(col("CategoryIds"), 38)) &
            (col("HORef_M1").isNotNull()) & 
            (col("FCONumber").isNotNull())
            ).count() == 0:
            return TestResult("homeOfficeReferenceNumber", "FAIL", "NO RECORDS TO TEST",test_from_state)
        
        ac_ref = test_df.filter(
            (array_contains(col("CategoryIds"), 38)) &
                (
                    ~col("HORef_M1").rlike("GWF") | ~col("FCONumber").rlike("GWF")
                )    
            & (col("homeOfficeReferenceNumber").isNotNull())
        )

        if ac_ref.count() != 0:
            return TestResult("homeOfficeReferenceNumber", "FAIL", f"homeOfficeReferenceNumber acceptance criteria failed: {str(ac_ref.count())} cases have been found where CategoryId in [38] + M1.HORef OR M2.FCONumber NOT LIKE '%GWF%' and homeOfficeReferenceNumber not omitted" ,test_from_state)
        else:
            return TestResult("homeOfficeReferenceNumber", "PASS", f"homeOfficeReferenceNumber acceptance criteria passed, all cases where CategoryId in [38] + M1.HORef OR M2.FCONumber NOT LIKE '%GWF%' and homeOfficeReferenceNumber omitted",test_from_state)
    else:
        return TestResult("homeOfficeReferenceNumber", "FAIL",f"Failed to Setup Data for Test - homeOfficeReferenceNumber does not exist in the payload",test_from_state)

def test_homeOfficeReferenceNumber_ac2(ho_test_df):
    if ho_test_df != None:
        test_df = ho_test_df

        if test_df.filter(
            col("HORef_M1").isNotNull()
            ).count() == 0:
            return TestResult("homeOfficeReferenceNumber", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
        ac_ref = test_df.filter(
            col("HORef_M1").eqNullSafe(col("homeOfficeReferenceNumber"))
        )

        if ac_ref.count() != 0:
            return TestResult("homeOfficeReferenceNumber", "FAIL", f"homeOfficeReferenceNumber acceptance criteria failed: {str(ac_gwfReferenceNumber.count())} cases have been found where M1.CleansedHORef IS not null and homeOfficeReferenceNumber != M1.CleansedHORef." ,test_from_state)
        else:
            return TestResult("homeOfficeReferenceNumber", "PASS", f"homeOfficeReferenceNumber acceptance criteria passed, all cases have M1.CleansedHORef matching homeOfficeReferenceNumber",test_from_state)
    else:
        return TestResult("homeOfficeReferenceNumber", "FAIL",f"Failed to Setup Data for Test - homeOfficeReferenceNumber does not exist in the payload",test_from_state)
    
def test_homeOfficeReferenceNumber_ac3(ho_test_df):
    if ho_test_df != None:
        test_df = ho_test_df

        if test_df.filter(
            (col("HORef_M1").isNull() & col("FCONumber").isNotNull()) 
            ).count() == 0:
            return TestResult("homeOfficeReferenceNumber", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
        test_df = test_df.filter(
            (col("HORef_M1").isNull() & col("FCONumber").isNotNull())
        )

        ac_ref = test_df.filter(
            col("FCONumber") != (col("homeOfficeReferenceNumber"))
        )

        if ac_gwfReferenceNumber.count() != 0:
            return TestResult("homeOfficeReferenceNumber", "FAIL", f"homeOfficeReferenceNumber acceptance criteria failed: {str(ac_ref.count())} cases have been found where M1.CleansedHORef IS null + M2.FCONumber is not Null and homeOfficeReferenceNumber != M2.FCONumber" ,test_from_state)
        else:
            return TestResult("homeOfficeReferenceNumber", "PASS", f"homeOfficeReferenceNumber acceptance criteria passed, all cases where M1.CleansedHORef IS null + M2.FCONumber is not Null have a matching homeOfficeReferenceNumber",test_from_state)
    else:
        return TestResult("homeOfficeReferenceNumber", "FAIL",f"Failed to Setup Data for Test - homeOfficeReferenceNumber does not exist in the payload",test_from_state)

#######################
#gwfReferenceNumber - IF CategoryId in [38] + M1.HORef OR M2.FCONumber NOT LIKE '%GWF%' and gwfReferenceNumber not omitted
#######################
def test_gwfReferenceNumber_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (array_contains(col("CategoryIds"), 38)) &
        (col("HORef_M1").isNotNull()) & 
        (col("FCONumber").isNotNull())
        ).count() == 0:
        return TestResult("gwfReferenceNumber", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_gwfReferenceNumber = test_df.filter(
        (array_contains(col("CategoryIds"), 38)) &
            (
                ~col("HORef_M1").rlike("GWF") | ~col("FCONumber").rlike("GWF")
            )    
         & (col("gwfReferenceNumber").isNotNull())
    )

    if ac_gwfReferenceNumber.count() != 0:
        return TestResult("gwfReferenceNumber", "FAIL", f"gwfReferenceNumber acceptance criteria failed: {str(ac_gwfReferenceNumber.count())} cases have been found where CategoryId in [38] + M1.HORef OR M2.FCONumber NOT LIKE '%GWF%' and gwfReferenceNumber not omitted" ,test_from_state)
    else:
        return TestResult("gwfReferenceNumber", "PASS", f"gwfReferenceNumber acceptance criteria passed, all cases where CategoryId in [38] + M1.HORef OR M2.FCONumber NOT LIKE '%GWF%' and gwfReferenceNumber omitted",test_from_state)
    
#######################
#gwfReferenceNumber - If M1.CleansedHORef IS not null and gwfReferenceNumber != M1.CleansedHORef 
#######################
def test_gwfReferenceNumber_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("HORef_M1").isNotNull()
        ).count() == 0:
        return TestResult("gwfReferenceNumber", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_gwfReferenceNumber = test_df.filter(
        col("HORef_M1").eqNullSafe(col("gwfReferenceNumber"))
    )

    if ac_gwfReferenceNumber.count() != 0:
        return TestResult("gwfReferenceNumber", "FAIL", f"gwfReferenceNumber acceptance criteria failed: {str(ac_gwfReferenceNumber.count())} cases have been found where M1.CleansedHORef IS not null and gwfReferenceNumber != M1.CleansedHORef." ,test_from_state)
    else:
        return TestResult("gwfReferenceNumber", "PASS", f"gwfReferenceNumber acceptance criteria passed, all cases have M1.CleansedHORef matching gwfReferenceNumber",test_from_state)

#######################
#gwfReferenceNumber - If M1.CleansedHORef IS null + M2.FCONumber is not Null and gwfReferenceNumber != M2.FCONumber
#######################
def test_gwfReferenceNumber_ac3(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("HORef_M1").isNull() & col("FCONumber").isNotNull()) 
        ).count() == 0:
        return TestResult("gwfReferenceNumber", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    test_df = test_df.filter(
        (col("HORef_M1").isNull() & col("FCONumber").isNotNull())
    )

    ac_gwfReferenceNumber = test_df.filter(
        col("FCONumber") != (col("gwfReferenceNumber"))
    )

    if ac_gwfReferenceNumber.count() != 0:
        return TestResult("gwfReferenceNumber", "FAIL", f"gwfReferenceNumber acceptance criteria failed: {str(ac_gwfReferenceNumber.count())} cases have been found where M1.CleansedHORef IS null + M2.FCONumber is not Null and gwfReferenceNumber != M2.FCONumber" ,test_from_state)
    else:
        return TestResult("gwfReferenceNumber", "PASS", f"gwfReferenceNumber acceptance criteria passed, all cases where M1.CleansedHORef IS null + M2.FCONumber is not Null have a matching gwfReferenceNumber",test_from_state)

############################################################################################

#######################
#remission Init code
#######################
def test_remission_init(json, M1_bronze):
    try:
        json = json.select(
            "appealReferenceNumber",
            "remissionType",
            "remissionClaim",
            "feeRemissionType"
            # "exceptionalCircumstances",
            # "legalAidAccountNumber",
            # "asylumSupportReference",
            # "helpWithFeesReferenceNumber",
        )

        M1_bronze = M1_bronze.select(
            "CaseNo",
            "PaymentRemissionRequested",
            "PaymentRemissionReason",
            # "ReasonDescription"
            "ASFReferenceNo",          
            "LSCReference",              
            "PaymentRemissionReasonNote"
        )

        test_df = json.join(
            M1_bronze,
            json["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        )

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("remission", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state)

def test_remission_mapping(
    test_df,
    PaymentRemissionRequested,
    PaymentRemissionReason,
    expected_remissionType,
    expected_remissionClaim,
    expected_feeRemissionType,
    # expected_exceptionalCircumstances,
    # expected_legalAidAccountNumber,
    # expected_asylumSupportReference,
    # expected_helpWithFeesReferenceNumber
):
    
    output_lines = []
    test_passed = True

    # Filter rows that match the "Given" conditions
    filtered_data = test_df.where(
        (col("PaymentRemissionRequested").eqNullSafe(PaymentRemissionRequested)) &
        (col("PaymentRemissionReason").eqNullSafe(PaymentRemissionReason))
    )

    if filtered_data.count() == 0:
        return False, f"*No matching test data* for PaymentRemissionRequested={PaymentRemissionRequested}, PaymentRemissionReason={PaymentRemissionReason}", 0

    # Check each row's actual values
    mismatches = filtered_data.where(
        (~col("remissionType").eqNullSafe(expected_remissionType)) |
        (~col("remissionClaim").eqNullSafe(expected_remissionClaim)) |
        (~col("feeRemissionType").eqNullSafe(expected_feeRemissionType)) 
        # (~col("exceptionalCircumstances") != expected_exceptionalCircumstances) |
        # (~col("legalAidAccountNumber") != expected_legalAidAccountNumber) |
        # (~col("asylumSupportReference") != expected_asylumSupportReference) |
        # (~col("helpWithFeesReferenceNumber") != expected_helpWithFeesReferenceNumber)
    )

    if mismatches.count() > 0:
        test_passed = False
        output_lines.append(f"*Mismatch present in one of the remission fields: remissionType, remissionClaim, feeRemissionType, exceptionalCircumstances, legalAidAccountNumber, asylumSupportReference, helpWithFeesReferenceNumber. {mismatches.count()} mismatches found.*")
    else:
        output_lines.append("All of the remission fields match defined requirements as expected: remissionType, remissionClaim, feeRemissionType, exceptionalCircumstances, legalAidAccountNumber, asylumSupportReference, helpWithFeesReferenceNumber.")

    return test_passed, "\n".join(output_lines), mismatches

#######################
# Remissions AC1 - Requested 0, Reason 0
#######################
def test_remission_ac1(test_df):
    test_passed, output_lines, mismatches = test_remission_mapping(
        test_df,
        PaymentRemissionRequested = 0,
        PaymentRemissionReason = 0,
        expected_remissionType = "noRemission",
        expected_remissionClaim = None,
        expected_feeRemissionType = None,
        # expected_exceptionalCircumstances = None,
        # expected_legalAidAccountNumber = None,
        # expected_asylumSupportReference = None,
        # expected_helpWithFeesReferenceNumber = None
    )
    
    status = "PASS" if test_passed else "FAIL"
    return TestResult("remissionType, remissionClaim, feeRemissionType", status, "Test for remissionType is noRemission (AC1): " + output_lines, test_from_state)

#######################
# Remissions AC2 - Requested 2, Reason 0
#######################
def test_remission_ac2(test_df):
    test_passed, output_lines, mismatches = test_remission_mapping(
        test_df,
        PaymentRemissionRequested = 2,
        PaymentRemissionReason = 0,
        expected_remissionType = "noRemission",
        expected_remissionClaim = None,
        expected_feeRemissionType = None,
        # expected_exceptionalCircumstances = None,
        # expected_legalAidAccountNumber = None,
        # expected_asylumSupportReference = None,
        # expected_helpWithFeesReferenceNumber = None
    )
    
    status = "PASS" if test_passed else "FAIL"
    return TestResult("remissionType, remissionClaim, feeRemissionType", status, "Test for remissionType is noRemission (AC2): " + output_lines, test_from_state)

#######################
# Remissions AC3 - Asylum Support
#######################
def test_remission_ac3(test_df):
    test_passed, output_lines, mismatches = test_remission_mapping(
        test_df,
        PaymentRemissionRequested = 1,
        PaymentRemissionReason = 1,
        expected_remissionType = "hoWaiverRemission",
        expected_remissionClaim = "asylumSupport",
        expected_feeRemissionType = "Asylum Support",
        # expected_exceptionalCircumstances = None,
        # expected_legalAidAccountNumber = None,
        # expected_asylumSupportReference = test_df["ASFReferenceNo"], 
        # expected_helpWithFeesReferenceNumber = None
    )
    
    status = "PASS" if test_passed else "FAIL"
    return TestResult("remissionType, remissionClaim, feeRemissionType", status, "Test for hoWaiverRemission (AC3): " + output_lines, test_from_state)

#######################
# Remissions AC4 - Oral Hearing Direction
#######################
def test_remission_ac4(test_df):
    test_passed, output_lines, mismatches = test_remission_mapping(
        test_df,
        PaymentRemissionRequested = 1,
        PaymentRemissionReason = 2,
        expected_remissionType = "exceptionalCircumstancesRemission",
        expected_remissionClaim = None,
        expected_feeRemissionType = None,
        # expected_exceptionalCircumstances = "This is a migrated ARIA case. The remission reason was Oral Hearing Direction. Please see the documents for further information.",
        # expected_legalAidAccountNumber = None,
        # expected_asylumSupportReference = None,
        # expected_helpWithFeesReferenceNumber = None
    )
    
    status = "PASS" if test_passed else "FAIL"
    return TestResult("remissionType, remissionClaim, feeRemissionType", status, "Test for exceptionalCircumstances (AC4): " + output_lines, test_from_state)

#######################
# Remissions AC5 - LAA Funded
#######################
def test_remission_ac5(test_df):
    test_passed, output_lines, mismatches = test_remission_mapping(
        test_df,
        PaymentRemissionRequested = 1,
        PaymentRemissionReason = 3,
        expected_remissionType = "hoWaiverRemission",
        expected_remissionClaim = "legalAid",
        expected_feeRemissionType = "Legal Aid",
        # expected_exceptionalCircumstances = None,
        # expected_legalAidAccountNumber = test_df["LSCReference"],
        # expected_asylumSupportReference = None,
        # expected_helpWithFeesReferenceNumber = None
    )
    
    status = "PASS" if test_passed else "FAIL"
    return TestResult("remissionType, remissionClaim, feeRemissionType", status, "Test for hoWaiverRemission (AC5): " + output_lines, test_from_state)

#######################
# Remissions AC6 - S17 Childrens Act
#######################
def test_remission_ac6(test_df):
    test_passed, output_lines, mismatches = test_remission_mapping(
        test_df,
        PaymentRemissionRequested = 1,
        PaymentRemissionReason = 4,
        expected_remissionType = "hoWaiverRemission",
        expected_remissionClaim = "section17",
        expected_feeRemissionType = "Section 17",
        # expected_exceptionalCircumstances = None,
        # expected_legalAidAccountNumber = None,
        # expected_asylumSupportReference = None,
        # expected_helpWithFeesReferenceNumber = None
    )
    
    status = "PASS" if test_passed else "FAIL"
    return TestResult("remissionType, remissionClaim, feeRemissionType", status, "Test for hoWaiverRemission (AC6): " + output_lines, test_from_state)

#######################
# Remissions AC7 - Convention Case
#######################
def test_remission_ac7(test_df):
    test_passed, output_lines, mismatches = test_remission_mapping(
        test_df,
        PaymentRemissionRequested = 1,
        PaymentRemissionReason = 5,
        expected_remissionType = "NO MAPPING REQUIRED",
        expected_remissionClaim = "NO MAPPING REQUIRED",
        expected_feeRemissionType = "NO MAPPING REQUIRED",
        # expected_exceptionalCircumstances = "NO MAPPING REQUIRED",
        # expected_legalAidAccountNumber = "NO MAPPING REQUIRED",
        # expected_asylumSupportReference = "NO MAPPING REQUIRED",
        # expected_helpWithFeesReferenceNumber = "NO MAPPING REQUIRED"
    )
    
    status = "PASS" if test_passed else "FAIL"
    return TestResult("remissionType, remissionClaim, feeRemissionType", status, "Test for NO MAPPING REQUIRED (AC7): " + output_lines, test_from_state)

#######################
# Remissions AC8 - Other
#######################
def test_remission_ac8(test_df):
    test_passed, output_lines, mismatches = test_remission_mapping(
        test_df,
        PaymentRemissionRequested = 1,
        PaymentRemissionReason = 6,
        expected_remissionType = "exceptionalCircumstancesRemission",
        expected_remissionClaim = None,
        expected_feeRemissionType = None,
        # expected_exceptionalCircumstances = "This is a migrated ARIA case. The remission reason was Other. Please see the documents for further information.",
        # expected_legalAidAccountNumber = None,
        # expected_asylumSupportReference = None,
        # expected_helpWithFeesReferenceNumber = None
    )
    
    status = "PASS" if test_passed else "FAIL"
    return TestResult("remissionType, remissionClaim, feeRemissionType", status, "Test for Other (AC8): " + output_lines, test_from_state)

#######################
# Remissions AC9 - S20 Childrens Act
#######################
def test_remission_ac9(test_df):
    test_passed, output_lines, mismatches = test_remission_mapping(
        test_df,
        PaymentRemissionRequested = 1,
        PaymentRemissionReason = 7,
        expected_remissionType = "hoWaiverRemission",
        expected_remissionClaim = "section20",
        expected_feeRemissionType = "Section 20",
        # expected_exceptionalCircumstances = None,
        # expected_legalAidAccountNumber = None,
        # expected_asylumSupportReference = None,
        # expected_helpWithFeesReferenceNumber = None
    )
    
    status = "PASS" if test_passed else "FAIL"
    return TestResult("remissionType, remissionClaim, feeRemissionType", status, "Test for hoWaiverRemission (AC9): " + output_lines, test_from_state)

#######################
# Remissions AC10 - Home Office Waiver
#######################
def test_remission_ac10(test_df):
    test_passed, output_lines, mismatches = test_remission_mapping(
        test_df,
        PaymentRemissionRequested = 1,
        PaymentRemissionReason = 8,
        expected_remissionType = "hoWaiverRemission",
        expected_remissionClaim = "homeOfficeWaiver",
        expected_feeRemissionType = "Home Office fee waiver",
        # expected_exceptionalCircumstances = None,
        # expected_legalAidAccountNumber = None,
        # expected_asylumSupportReference = None,
        # expected_helpWithFeesReferenceNumber = None
    )
    
    status = "PASS" if test_passed else "FAIL"
    return TestResult("remissionType, remissionClaim, feeRemissionType", status, "Test for hoWaiverRemission (AC10): " + output_lines, test_from_state)

#######################
# Remissions AC11 - Help with Fees
#######################
def test_remission_ac11(test_df):
    test_passed, output_lines, mismatches = test_remission_mapping(
        test_df,
        PaymentRemissionRequested = 1,
        PaymentRemissionReason = 9,
        expected_remissionType = "helpWithFees",
        expected_remissionClaim = None,
        expected_feeRemissionType = None,
        # expected_exceptionalCircumstances = None,
        # expected_legalAidAccountNumber = None,
        # expected_asylumSupportReference = None,
        # expected_helpWithFeesReferenceNumber = test_df["PaymentRemissionReasonNote"]
    )
    
    status = "PASS" if test_passed else "FAIL"
    return TestResult("remissionType, remissionClaim, feeRemissionType", status, "Test for helpWithFees (AC11): " + output_lines, test_from_state)

#######################
# Remissions AC12 - Paid on CCD
#######################
def test_remission_ac12(test_df):
    test_passed, output_lines, mismatches = test_remission_mapping(
        test_df,
        PaymentRemissionRequested = 1,
        PaymentRemissionReason = 10,
        expected_remissionType = "NO MAPPING REQUIRED",
        expected_remissionClaim = "NO MAPPING REQUIRED",
        expected_feeRemissionType = "NO MAPPING REQUIRED",
        # expected_exceptionalCircumstances = "NO MAPPING REQUIRED",
        # expected_legalAidAccountNumber = "NO MAPPING REQUIRED",
        # expected_asylumSupportReference = "NO MAPPING REQUIRED",
        # expected_helpWithFeesReferenceNumber = "NO MAPPING REQUIRED"
    )
    
    status = "PASS" if test_passed else "FAIL"
    return TestResult("remissionType, remissionClaim, feeRemissionType", status, "Test for NO MAPPING REQUIRED (AC12): " + output_lines, test_from_state)

############################################################################################

#######################
#caseData Init code
#######################
def test_caseData_init(json, M1_bronze, M1_silver):
    try:
        json = json.select(
            "appealReferenceNumber",
            "submissionOutOfTime",
            # "recordedOutOfTimeDecision",
            "appealSubmissionDate",
            "appealSubmissionInternalDate",
            "tribunalReceivedDate",
            "appellantsRepresentation"
        )

        M1_bronze = M1_bronze.select(
            "CaseNo",
            "DateLodged",
            "DateAppealReceived",
            "OutOfTimeIssue"
        )

        M1_silver = M1_silver.select(
            col("CaseNo").alias("CaseNo-silver"),
            "dv_representation"
        )

        test_df = json.join(
            M1_bronze,
            json["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        )

        test_df = test_df.join(
            M1_silver,
            json["appealReferenceNumber"] == M1_silver["CaseNo-silver"],
            "inner"
        )

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("caseData", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state)

#######################
#submissionOutOfTime - If OutOfTimeIssue is 1 and submissionOutOfTime != Yes
#######################
def test_submissionOutOfTime_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("OutOfTimeIssue") == "true"
        ).count() == 0:
        return TestResult("submissionOutOfTime", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_submissionOutOfTime = test_df.filter(
        (col("OutOfTimeIssue") == "true") & (col("submissionOutOfTime") != "Yes")
    )

    if ac_submissionOutOfTime.count() != 0:
        return TestResult("submissionOutOfTime", "FAIL", f"submissionOutOfTime acceptance criteria failed: {str(ac_submissionOutOfTime.count())} cases have been found where OutOfTimeIssue is true and submissionOutOfTime != Yes" ,test_from_state)
    else:
        return TestResult("submissionOutOfTime", "PASS", f"submissionOutOfTime acceptance criteria passed, all cases where OutOfTimeIssue is 1, submissionOutOfTime = Yes",test_from_state)

#######################
#submissionOutOfTime - If OutOfTimeIssue is 0 and submissionOutOfTime != No
#######################
def test_submissionOutOfTime_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("OutOfTimeIssue") == "false") 
        ).count() == 0:
        return TestResult("submissionOutOfTime", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_submissionOutOfTime = test_df.filter(
        ((col("OutOfTimeIssue") == "false") | (col("OutOfTimeIssue").isNull())) & 
        (col("submissionOutOfTime") != "No")
    )

    if ac_submissionOutOfTime.count() != 0:
        return TestResult("submissionOutOfTime", "FAIL", f"submissionOutOfTime acceptance criteria failed: {str(ac_submissionOutOfTime.count())} cases have been found where OutOfTimeIssue is false and submissionOutOfTime != No" ,test_from_state)
    else:
        return TestResult("submissionOutOfTime", "PASS", f"submissionOutOfTime acceptance criteria passed, all cases where OutOfTimeIssue is false, submissionOutOfTime = No",test_from_state)
    
#######################
#submissionOutOfTime - If OutOfTimeIssue is 0 or Null and submissionOutOfTime != No
#######################
def test_submissionOutOfTime_ac3(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("OutOfTimeIssue").isNull())
        ).count() == 0:
        return TestResult("submissionOutOfTime", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_submissionOutOfTime = test_df.filter(
        ((col("OutOfTimeIssue").isNull()) | (col("OutOfTimeIssue").isNull())) & 
        (col("submissionOutOfTime") != "No")
    )

    if ac_submissionOutOfTime.count() != 0:
        return TestResult("submissionOutOfTime", "FAIL", f"submissionOutOfTime acceptance criteria failed: {str(ac_submissionOutOfTime.count())} cases have been found where OutOfTimeIssue is null and submissionOutOfTime != No" ,test_from_state)
    else:
        return TestResult("submissionOutOfTime", "PASS", f"submissionOutOfTime acceptance criteria passed, all cases where OutOfTimeIssue is null, submissionOutOfTime = No",test_from_state)

#######################
#recordedOutOfTimeDecision - If M1.OutOfTimeIssue is 1 + M3.Outcome != 0 and recordedOutOfTimeDecision != Yes
#######################
def test_recordedOutOfTimeDecision_init(json, M1_bronze, M3_bronze):
    try:
        json = json.select(
            "appealReferenceNumber",
            "recordedOutOfTimeDecision"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "Outcome"
        ) 

        M1_bronze = M1_bronze.select(
            col("CaseNo").alias("CaseNo-M1"),
            "OutOfTimeIssue"
        )

        rod_test_df = json.join(
            M3_bronze,
            json["appealReferenceNumber"] == M3_bronze["CaseNo"],
            "inner"
        )

        rod_test_df = rod_test_df.join(
            M1_bronze,
            json["appealReferenceNumber"] == M1_silver["CaseNo-M1"],
            "inner"
        )

        return rod_test_df, True
    except Exception as e:
        rod_test_df = None
        error_message = str(e)        
        return TestResult("recordedOutOfTimeDecision", "FAIL",f"Failed to Setup Data for Test - recordedOutOfTimeDecision does not exist in the payload",test_from_state), rod_test_df
    
# If M1.OutOfTimeIssue is 1 + M3.Outcome != 0 and recordedOutOfTimeDecision != Yes
def test_recordedOutOfTimeDecision_ac1(rod_test_df):
    if rod_test_df != None:
        #Check we have Records To test
        if test_df.filter(
            (col("OutOfTimeIssue") == 1) &
            (col("Outcome") != 0)
            ).count() == 0:
            return TestResult("recordedOutOfTimeDecision", "FAIL", "NO RECORDS TO TEST",test_from_state)
        
        ac = test_df.filter(
        ((col("OutOfTimeIssue") == 1) & (col("Outcome") != 0)) & 
        col("recordedOutOfTimeDecision") != "Yes"
        )

        if ac.count() != 0:
            return TestResult("recordedOutOfTimeDecision", "FAIL", f"recordedOutOfTimeDecision acceptance criteria failed: {str(ac.count())} cases have been found where M1.OutOfTimeIssue is 1 + M3.Outcome != 0 and recordedOutOfTimeDecision != Yes",test_from_state)
        else:
            return TestResult("recordedOutOfTimeDecision", "PASS", f"recordedOutOfTimeDecision acceptance criteria passed, all cases where M1.OutOfTimeIssue is 1 + M3.Outcome != 0 have recordedOutOfTimeDecision = Yes",test_from_state)
    else:
        return TestResult("recordedOutOfTimeDecision", "FAIL",f"Failed to Setup Data for Test - recordedOutOfTimeDecision does not exist in the payload",test_from_state)
    
# If OutOfTimeIssue is 1 + Outcome = 0 and recordedOutOfTimeDecision is not omitted
def test_recordedOutOfTimeDecision_ac2(rod_test_df):
    if rod_test_df != None:
        #Check we have Records To test
        if test_df.filter(
            (col("OutOfTimeIssue") == 1) &
            (col("Outcome") == 0)
            ).count() == 0:
            return TestResult("recordedOutOfTimeDecision", "FAIL", "NO RECORDS TO TEST",test_from_state)
        
        ac = test_df.filter(
        ((col("OutOfTimeIssue") == 1) & (col("Outcome") == 0)) & 
        col("recordedOutOfTimeDecision").isNotNull()
        )

        if ac.count() != 0:
            return TestResult("recordedOutOfTimeDecision", "FAIL", f"recordedOutOfTimeDecision acceptance criteria failed: {str(ac.count())} cases have been found where M1.OutOfTimeIssue is 1 + M3.Outcome == 0 and recordedOutOfTimeDecision is not omitted",test_from_state)
        else:
            return TestResult("recordedOutOfTimeDecision", "PASS", f"recordedOutOfTimeDecision acceptance criteria passed, all cases where M1.OutOfTimeIssue is 1 + M3.Outcome = 0 have recordedOutOfTimeDecision is omitted",test_from_state)
    else:
        return TestResult("recordedOutOfTimeDecision", "FAIL",f"Failed to Setup Data for Test - recordedOutOfTimeDecision does not exist in the payload",test_from_state)
    
# If OutOfTimeIssue != 1 + Outcome != 0 and recordedOutOfTimeDecision is not omitted
def test_recordedOutOfTimeDecision_ac3(rod_test_df):
    if rod_test_df != None:
        #Check we have Records To test
        if test_df.filter(
            (col("OutOfTimeIssue") != 1) &
            (col("Outcome") != 0)
            ).count() == 0:
            return TestResult("recordedOutOfTimeDecision", "FAIL", "NO RECORDS TO TEST",test_from_state)
        
        ac = test_df.filter(
        ((col("OutOfTimeIssue") != 1) & (col("Outcome") != 0)) & 
        col("recordedOutOfTimeDecision").isNotNull()
        )

        if ac.count() != 0:
            return TestResult("recordedOutOfTimeDecision", "FAIL", f"recordedOutOfTimeDecision acceptance criteria failed: {str(ac.count())} cases have been found where M1.OutOfTimeIssue != 1 + M3.Outcome != 0 and recordedOutOfTimeDecision is not null",test_from_state)
        else:
            return TestResult("recordedOutOfTimeDecision", "PASS", f"recordedOutOfTimeDecision acceptance criteria passed, all cases where M1.OutOfTimeIssue != 1 + M3.Outcome != 0 have recordedOutOfTimeDecision is not null",test_from_state)
    else:
        return TestResult("recordedOutOfTimeDecision", "FAIL",f"Failed to Setup Data for Test - recordedOutOfTimeDecision does not exist in the payload",test_from_state)

#######################
#appealSubmissionDate - If M1.DateLodged != appealSubmissionDate
#######################
def test_appealSubmissionDate(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("DateLodged").isNotNull()
        ).count() == 0:
        return TestResult("appealSubmissionDate", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appealSubmissionDate = test_df.filter(
        col("appealSubmissionDate").eqNullSafe(("DateLodged"))
    )

    if ac_appealSubmissionDate.count() != 0:
        return TestResult("appealSubmissionDate", "FAIL", f"appealSubmissionDate acceptance criteria failed: {str(ac_appealSubmissionDate.count())} cases have been found where M1.DateLodged != appealSubmissionDate" ,test_from_state)
    else:
        return TestResult("appealSubmissionDate", "PASS", f"appealSubmissionDate acceptance criteria passed, all cases have M1.DateLodged = appealSubmissionDate",test_from_state)

#######################
#appealSubmissionInternalDate - If M1.DateLodged != appealSubmissionInternalDate
#######################
def test_appealSubmissionInternalDate(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("DateLodged").isNotNull()
        ).count() == 0:
        return TestResult("appealSubmissionInternalDate", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appealSubmissionDate = test_df.filter(
        col("DateLodged").cast("date") != col("appealSubmissionInternalDate").cast("date")
    )

    if ac_appealSubmissionDate.count() != 0:
        return TestResult("appealSubmissionInternalDate", "FAIL", f"appealSubmissionInternalDate acceptance criteria failed: {str(ac_appealSubmissionDate.count())} cases have been found where M1.DateLodged != appealInternalSubmissionDate" ,test_from_state)
    else:
        return TestResult("appealSubmissionDate", "PASS", f"appealSubmissionInternalDate acceptance criteria passed, all cases have M1.DateLodged = appealSubmissionInternalDate",test_from_state)
    
#######################
#tribunalReceivedDate - If M1.DateAppealReceived != tribunalReceivedDate
#######################
def test_tribunalReceivedDate(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("DateAppealReceived").isNotNull()
        ).count() == 0:
        return TestResult("appealSubmissionInternalDate", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_tribunalReceivedDate = test_df.filter(
        col("DateAppealReceived").cast("date") != (col("tribunalReceivedDate")).cast("date")
    )

    if ac_tribunalReceivedDate.count() != 0:
        return TestResult("tribunalReceivedDate", "FAIL", f"tribunalReceivedDate acceptance criteria failed: {str(ac_tribunalReceivedDate.count())} cases have been found where M1.DateAppealReceived != tribunalReceivedDate" ,test_from_state)
    else:
        return TestResult("tribunalReceivedDate", "PASS", f"tribunalReceivedDate acceptance criteria passed, all cases have M1.DateAppealReceived = tribunalReceivedDate",test_from_state)

#######################
#appellantsRepresentation - If Representation = LR and appellantsRepresentation != No
#######################
def test_appellantsRepresentation_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("dv_representation") == "LR"
        ).count() == 0:
        return TestResult("appellantsRepresentation", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantsRepresentation = test_df.filter(
        (col("dv_representation") == "LR") & (col("appellantsRepresentation") != "No")
    )

    if ac_appellantsRepresentation.count() != 0:
        return TestResult("appellantsRepresentation", "FAIL", f"appellantsRepresentation acceptance criteria failed: {str(ac_appellantsRepresentation.count())} cases have been found where dv_representation = LR and appellantsRepresentation != No" ,test_from_state)
    else:
        return TestResult("appellantsRepresentation", "PASS", f"appellantsRepresentation acceptance criteria passed, all cases where dv_representation = LR have appellantsRepresentation = No",test_from_state)

#######################
#appellantsRepresentation - If Representation = AIP and appellantsRepresentation != Yes
#######################
def test_appellantsRepresentation_ac2(test_df):
    #Check we have Records To test
    if test_df.filter(
        col("dv_representation") == "AIP"
        ).count() == 0:
        return TestResult("appellantsRepresentation", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac_appellantsRepresentation = test_df.filter(
        (col("dv_representation") == "AIP") & (col("appellantsRepresentation") != "Yes")
    )

    if ac_appellantsRepresentation.count() != 0:
        return TestResult("appellantsRepresentation", "FAIL", f"appellantsRepresentation acceptance criteria failed: {str(ac_appellantsRepresentation.count())} cases have been found where dv_representation = AIP and appellantsRepresentation != Yes" ,test_from_state)
    else:
        return TestResult("appellantsRepresentation", "PASS", f"appellantsRepresentation acceptance criteria passed, all cases where dv_representation = AIP have appellantsRepresentation = Yes",test_from_state)


############################################################################################

#######################
#isServiceRequestTabVisibleConsideringRemissions - PaymentRemissionRequested is null or 2 and isServiceRequestTabVisibleConsideringRemissions != Yes
#######################
def test_isServiceRequestTabVisibleConsideringRemissions_init(json, M1_bronze):
    try:
        json = json.select(
            "appealReferenceNumber",
            "isServiceRequestTabVisibleConsideringRemissions"
        )

        M1_bronze = M1_bronze.select(
            "CaseNo",
            "PaymentRemissionRequested"
        )

        test_df = json.join(
            M1_bronze,
            json["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        )

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("isServiceRequestTabVisibleConsideringRemissions", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state)

#######################
#isServiceRequestTabVisibleConsideringRemissions - PaymentRemissionRequested is null or 2 and isServiceRequestTabVisibleConsideringRemissions != Yes
#######################
def test_isServiceRequestTabVisibleConsideringRemissions_ac1(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("PaymentRemissionRequested").isNull())
        ).count() == 0:
        return TestResult("isServiceRequestTabVisibleConsideringRemissions", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac = test_df.filter(
    (col("PaymentRemissionRequested").isNull())
    & (col("isServiceRequestTabVisibleConsideringRemissions") != "Yes")        
    )

    if ac.count() != 0:
        return TestResult("isServiceRequestTabVisibleConsideringRemissions", "FAIL", f"isServiceRequestTabVisibleConsideringRemissions acceptance criteria failed: {str(ac.count())} cases have been found where PaymentRemissionRequested is null and isServiceRequestTabVisibleConsideringRemissions != Yes" ,test_from_state)
    else:
        return TestResult("isServiceRequestTabVisibleConsideringRemissions", "PASS", f"isServiceRequestTabVisibleConsideringRemissions acceptance criteria passed, all cases where PaymentRemissionRequested is null and isServiceRequestTabVisibleConsideringRemissions = Yes",test_from_state)
    
def test_isServiceRequestTabVisibleConsideringRemissions_ac2(test_df):
    #Check we have Records To test
    if test_df.filter( 
        (col("PaymentRemissionRequested") == 2)
        ).count() == 0:
        return TestResult("isServiceRequestTabVisibleConsideringRemissions", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac = test_df.filter(
    (col("PaymentRemissionRequested") == 2)
    & (col("isServiceRequestTabVisibleConsideringRemissions") != "Yes")        
    )

    if ac.count() != 0:
        return TestResult("isServiceRequestTabVisibleConsideringRemissions", "FAIL", f"isServiceRequestTabVisibleConsideringRemissions acceptance criteria failed: {str(ac.count())} cases have been found where PaymentRemissionRequested is 2 and isServiceRequestTabVisibleConsideringRemissions != Yes" ,test_from_state)
    else:
        return TestResult("isServiceRequestTabVisibleConsideringRemissions", "PASS", f"isServiceRequestTabVisibleConsideringRemissions acceptance criteria passed, all cases where PaymentRemissionRequested is 2 and isServiceRequestTabVisibleConsideringRemissions = Yes",test_from_state)

#######################
#isServiceRequestTabVisibleConsideringRemissions - PaymentRemissionRequested is not null or not 2 and isServiceRequestTabVisibleConsideringRemissions != No
#######################
def test_isServiceRequestTabVisibleConsideringRemissions_ac3(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("PaymentRemissionRequested").isNotNull())
        ).count() == 0:
        return TestResult("isServiceRequestTabVisibleConsideringRemissions", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac = test_df.filter(
        (col("PaymentRemissionRequested").isNotNull())
        & (col("isServiceRequestTabVisibleConsideringRemissions") != "No")        
    )

    if ac.count() != 0:
        return TestResult("isServiceRequestTabVisibleConsideringRemissions", "FAIL", f"isServiceRequestTabVisibleConsideringRemissions acceptance criteria failed: {str(ac.count())} cases have been found where PaymentRemissionRequested is not null and isServiceRequestTabVisibleConsideringRemissions != No" ,test_from_state)
    else:
        return TestResult("isServiceRequestTabVisibleConsideringRemissions", "PASS", f"isServiceRequestTabVisibleConsideringRemissions acceptance criteria passed, all cases where PaymentRemissionRequested is not null and isServiceRequestTabVisibleConsideringRemissions = No",test_from_state)
    
def test_isServiceRequestTabVisibleConsideringRemissions_ac4(test_df):
    #Check we have Records To test
    if test_df.filter(
        (col("PaymentRemissionRequested") != 2)
        ).count() == 0:
        return TestResult("isServiceRequestTabVisibleConsideringRemissions", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    ac = test_df.filter(
        (col("PaymentRemissionRequested") != 2)
        & (col("isServiceRequestTabVisibleConsideringRemissions") != "No")        
    )

    if ac.count() != 0:
        return TestResult("isServiceRequestTabVisibleConsideringRemissions", "FAIL", f"isServiceRequestTabVisibleConsideringRemissions acceptance criteria failed: {str(ac.count())} cases have been found where PaymentRemissionRequested is not 2 and isServiceRequestTabVisibleConsideringRemissions != No" ,test_from_state)
    else:
        return TestResult("isServiceRequestTabVisibleConsideringRemissions", "PASS", f"isServiceRequestTabVisibleConsideringRemissions acceptance criteria passed, all cases where PaymentRemissionRequested is not 2 and isServiceRequestTabVisibleConsideringRemissions = No",test_from_state)

################################################################
import pycountry

# Function to combine all appellant address fields into 1 string
def makeAppellantfullAddress(row):
    return ', '.join([
        str(row['AppellantAddress1']), 
        str(row['AppellantAddress2']), 
        str(row['AppellantAddress3']),
        str(row['AppellantAddress4']), 
        str(row['AppellantAddress5']), 
        str(row['AppellantPostcode'])
    ])

# Make a list of ISO Countries
all_countries = pycountry.countries
lower_countries = []

for country in all_countries:
    lower_countries.append(country.name.lower())

# Add additional countries list
additionalCountries = [
    'turkey', 'turkiye', 'russia', 'czech republic', 'usa', 'moldova', 
    'syria', 'ivory coast', "d'ivoire", 'iran', 'uae', 'irish republic', 
    'vietnam', 'kosovo', 'tanzania', 'united states of america', 'taiwan', 
    'bolivia', 'uk', 'swaziland', 'palestinian'
]

for country in additionalCountries:
    lower_countries.append(country)

# Function to get country from address
import string
import re

def getCountryFromAddress(address):
    if address is None:
        return None
    
    address = str(address)

    countries_matched = []
    low_address = address.lower()
    
    # Create a translator to remove punctuation
    translator = str.maketrans('', '', string.punctuation)
    
    # Attempt 1: Split by space and check words from the end
    split_address = low_address.split(' ')
    for word in reversed(split_address):
        if word.strip() in lower_countries:
            countries_matched.append(word.strip().capitalize())
            break
            
    if len(countries_matched) > 0:
        return ', '.join(countries_matched)
    
    # Attempt 2: Split by space, remove punctuation, and check from the end
    for word in reversed(split_address):
        clean_word = word.translate(translator)
        if clean_word.lower().strip() in lower_countries:
            countries_matched.append(clean_word.strip().capitalize())
            break
            
    if len(countries_matched) > 0:
        return ', '.join(countries_matched)
    
    # Attempt 3: Split by comma and check from the end
    split_address_comma = low_address.split(',')
    for word in reversed(split_address_comma):
        clean_word = word.translate(translator)
        if clean_word.strip() in lower_countries:
            countries_matched.append(clean_word.strip().capitalize())
            break
            
    if len(countries_matched) > 0:
        return ', '.join(countries_matched)
    
    # Attempt 4: Advanced regex split and sub-word checking
    split_address_regex = re.split(r"[,\s]+", low_address)
    for word in reversed(split_address_regex):
        # Direct check
        if word in lower_countries:
            countries_matched.append(word.strip().capitalize())
            break
        
        # Punctuation check
        clean_word = word.translate(translator)
        if clean_word.strip() in lower_countries:
            countries_matched.append(clean_word.strip().capitalize())
            break
            
        # Check for hyphenated words (e.g., South-Africa)
        split_word = word.split('-')
        for part in split_word:
            if part in lower_countries:
                countries_matched.append(part.strip().capitalize())
                break
        if len(countries_matched) > 0: break

        # Check for words split by dots (e.g., U.S.A)
        split_word2 = word.split('.')
        for part in split_word2:
            if part in lower_countries:
                countries_matched.append(part.strip().capitalize())
                break
        if len(countries_matched) > 0: break

    return ', '.join(countries_matched)

##CountryGovUkAdminJ
def test_countryGovUkOocAdminJ_ac2(test_df_sd,external_storage,spark):
    from pyspark.sql.functions import (
        col, when, lit, array, struct, collect_list, 
        max as spark_max, date_format, row_number, expr, 
        size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
        collect_set, current_timestamp,transform, first, array_contains
    )
    from pyspark.sql import functions as F

    getCountryFromAddress_udf = udf(getCountryFromAddress, StringType())

    test_df_sd = test_df_sd.withColumn(
        "countryresult",
        getCountryFromAddress_udf(col("appellantAddress"))
    )

    #Map Country to Code
    from pyspark.sql.types import (
        StructType,
        StructField,
    )
    schema = StructType([
            StructField("countryFromAddress", StringType(), True),
            StructField("oocLrCountryGovUkAdminJ", StringType(), True),
            StructField("countryGovUkOocAdminJ", StringType(), True)
        ])
    csv_file =  (
            spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "false")
                .option("encoding", "UTF-8")
                .schema(schema)
                .load(f"abfss://external-csv@{external_storage}.dfs.core.windows.net/ReferenceData/countries_countryFromAddress.csv")
                .select("countryFromAddress", "oocLrCountryGovUkAdminJ", "countryGovUkOocAdminJ")
        )

    #Join Country Code to Country    
    from pyspark.sql.functions import when, col, lit, length
    test_df_sd = test_df_sd.join(
        csv_file,
        test_df_sd.countryresult == csv_file.countryFromAddress,
        "left"
    ).withColumn("countrycode", when(length(csv_file.countryGovUkOocAdminJ) > 0, csv_file.countryGovUkOocAdminJ).otherwise(lit(None))
    ).select(
        test_df_sd["*"],
        "countrycode"
        # coalesce(csv_file.oocLrCountryGovUkAdminJ, lit("")).alias("countrycode")
    )
     
    test_df_sd = test_df_sd.filter(    
        (~col("countrycode").eqNullSafe(col("countryGovUkOocAdminJ")))                                  
        )

    if test_df_sd.count() != 0:
        return TestResult("CountryGovUkAdminJ", "FAIL", f"Failed CountryGovUkAdminJ Mismatch on expected Country Code - found :{str(test_df_sd.count())}",test_from_state),test_df_sd 
    else:
        return TestResult("CountryGovUkAdminJ", "PASS", f"All country codes in CountryGovUkAdminJ match expected",test_from_state),test_df_sd    

def test_legalRepDetails_init(json, M1_bronze,M1_silver):
    try:
        json_data = json.select(
                "AppealReferenceNumber",
                "legalRepEmail",
                "legalRepGivenName",
                "legalRepFamilyNamePaperJ",
                "legalRepCompanyPaperJ",
                "legalRepHasAddress",
                "legalRepAddressUK",
                "oocAddressLine1",
                "oocAddressLine2",
                "oocAddressLine3",
                "oocAddressLine4",
                "oocLrCountryGovUkAdminJ",
                "localAuthorityPolicy",        
                "countryGovUkOocAdminJ",
                "appellantAddress"
            )
        
        M1_silver = M1_silver.select(
            "CaseNo",
            "dv_representation"

        )

        M1_bronze = M1_bronze.select(
            "CaseNo",
            "CasePrefix",
            "Rep_Email",
            "CaseRep_Email",
            "CaseRep_FileSpecific_Email", 
            "Contact",
            "Rep_Name",
            "CaseRep_Name",
            "Rep_Postcode",
            "RepresentativeId",
            "Rep_Address1",
            "Rep_Address2",
            "Rep_Address3",
            "Rep_Address4",
            "Rep_Address5",
            "CaseRep_Address1",        
            "CaseRep_Address2",
            "CaseRep_Address3",
            "CaseRep_Address4",
            "CaseRep_Address5",
            "CaseRep_Postcode",
            
        )

        test_df =json_data.join(
            M1_bronze,
            json_data.AppealReferenceNumber == M1_bronze.CaseNo,
            "left"
        )
        
        test_df = test_df.join(
            M1_silver,
            test_df.CaseNo == M1_silver.CaseNo,
            "left"
        )

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("isServiceRequestTabVisibleConsideringRemissions", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state)

#Test1 - legalRepEmail - Representation = AIP = Ommited
def test_legalRepEmail_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(col("dv_representation") == "AIP").count() == 0:
        return TestResult("legalRepEmail", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    #Filter all records where rep=AIP and check legalRepEmail for any not null (fail)
    test_df = test_df_sd.filter((col("dv_representation") == "AIP") & (col("legalRepEmail").isNotNull()))
    
    if test_df.count() != 0:
        return TestResult("legalRepEmail", "FAIL", f"Failed as legalRepEmail is populated when dv_representation is AIP: found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepEmail", "PASS", f"legalRepEmail is null for all records where dv_representation is AIP",test_from_state)


#Test2 - RepEmail has data so expect legalRepEmail = RepEmail
def test_legalRepEmail_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(col("Rep_Email").isNotNull() & (col("dv_representation") == "LR")).count() == 0:
        return TestResult("legalRepEmail", "FAIL", "NO RECORDS TO TEST",test_from_state)
        
    #Filter RepEmail so has some data then check same value in legalRepEmail
    test_df = test_df_sd.filter(
        (col("dv_representation") == "LR") &
        (col("Rep_Email").isNotNull()) &             
        (~col("Rep_Email").eqNullSafe(col("legalRepEmail")))
        )    
    
    if test_df.count() != 0:
        return TestResult("legalRepEmail", "FAIL", f"Failed as legalRepEmail is populated but not with value from Rep_Email - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepEmail", "PASS", f"legalRepEmail is populated with value from Rep_Email",test_from_state)


#Test3 - RepEmail has NO data but CaseRepEmail Does so expect legalRepEmail = CaseRepEmail
def test_legalRepEmail_test3(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(col("Rep_Email").isNull() & col("CaseRep_Email").isNotNull() & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepEmail", "FAIL", "NO RECORDS TO TEST",test_from_state)
                
    #Filter where RepEmail is null and CaseRepEmail is not null - check legalRepEmail = CaseRepEmail
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("Rep_Email").isNull()) &
    (col("CaseRep_Email").isNotNull()) &    
    (~col("CaseRep_Email").eqNullSafe(col("legalRepEmail")))
    )    
    
    if test_df.count() != 0:
        return TestResult("legalRepEmail", "FAIL", f"Failed as legalRepEmail is populated but not with value from CaseRep_Email - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepEmail", "PASS", f"legalRepEmail is populated with value from CaseRep_Email",test_from_state)

#Test4 - RepEmail, CaseRepEmail have no data so expect legalRepEmail = FileSpecificEmail
def test_legalRepEmail_test4(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(col("Rep_Email").isNull() & col("CaseRep_Email").isNull() &
                        col("CaseRep_FileSpecific_Email").isNotNull() & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepEmail", "FAIL", "NO RECORDS TO TEST",test_from_state)
                    
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("Rep_Email").isNull()) &    
    (col("CaseRep_Email").isNull()) &
    (col("CaseRep_FileSpecific_Email").isNotNull()) &    
    (~col("CaseRep_FileSpecific_Email").eqNullSafe(col("legalRepEmail")))
    )    
    
    if test_df.count() != 0:
        return TestResult("legalRepEmail", "FAIL", f"Failed as legalRepEmail is populated but not with value from FileSpecificEmail - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepEmail", "PASS", f"legalRepEmail is populated with value from FileSpecificEmail",test_from_state)


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

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    ArrayType,
)

import re

#Test5 check post email has been cleaned
def test_legalRepEmail_test5(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(        
        (col("legalRepEmail").isNotNull())
        ).count() == 0:
        return TestResult("legalRepEmail", "FAIL", "NO RECORDS TO TEST",test_from_state)
    
    clean_email_udf = udf(cleanEmail, StringType())

    test_df_sd = test_df_sd.withColumn(
        "legalRepEmail_cleaned", 
        clean_email_udf(col("legalRepEmail"))
    )
    
    test_df = test_df_sd.filter(
    (~col("legalRepEmail").eqNullSafe(col("legalRepEmail_cleaned")))
    )

    if test_df.count() != 0:
        return TestResult("legalRepEmail", "FAIL", f"legalRepEmail failed to match expected cleaned up email. found : {str(test_df.count())}",test_from_state) 
    else:
        return TestResult("legalRepEmail", "PASS", f"legalRepEmail has passed the email cleaning checks",test_from_state)


#test1 - Contact is NOT NULL - legalRepGivenName = Contact
def test_legalRepGivenName_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(col("Contact").isNotNull() & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepGivenName", "FAIL", "NO RECORDS TO TEST",test_from_state)
                    
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("Contact").isNotNull()) &        
    (~col("Contact").eqNullSafe(col("legalRepGivenName")))
    )    
    
    if test_df.count() != 0:
        return TestResult("legalRepGivenName", "FAIL", f"Failed as legalRepGivenName is populated but not with value from Contact - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepGivenName", "PASS", f"legalRepGivenName is populated with value from Contact",test_from_state)


#test2 - Contact is NULL -RepName is not null --  legalRepGivenName = RepName
def test_legalRepGivenName_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(col("Contact").isNull() & col("Rep_Name").isNotNull() & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepGivenName", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                        
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("Contact").isNull()) &    
    (col("Rep_Name").isNotNull()) &   
    (~col("Rep_Name").eqNullSafe(col("legalRepGivenName")))
    )    
    
    if test_df.count() != 0:
        return TestResult("legalRepGivenName", "FAIL", f"Failed as legalRepGivenName is populated but not with value from Rep_Name - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepGivenName", "PASS", f"legalRepGivenName is populated with value from Rep_Name",test_from_state)

#test3 - Contact is NULL -RepName is null --  legalRepGivenName = CaseRep_Name
def test_legalRepGivenName_test3(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(col("Contact").isNull() & col("Rep_Name").isNull() & col("CaseRep_Name").isNotNull() & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepGivenName", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                        
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("Contact").isNull()) &    
    (col("Rep_Name").isNull()) &   
    (col("CaseRep_Name").isNotNull()) &       
    (~col("CaseRep_Name").eqNullSafe(col("legalRepGivenName")))
    )    
    
    if test_df.count() != 0:
        return TestResult("legalRepGivenName", "FAIL", f"Failed as legalRepGivenName is populated but not with value from CaseRep_Name - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepGivenName", "PASS", f"legalRepGivenName is populated with value from CaseRep_Name",test_from_state)
    

#Test1 - RepName is not null -- legalRepFamilyNamePaperJ = RepName
def test_legalRepFamilyNamePaperJ_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(col("Rep_Name").isNotNull() & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepFamilyNamePaperJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("Rep_Name").isNotNull()) &            
    (~col("Rep_Name").eqNullSafe(col("legalRepFamilyNamePaperJ")))
    )    
    
    if test_df.count() != 0:
        return TestResult("legalRepFamilyNamePaperJ", "FAIL", f"Failed as legalRepFamilyNamePaperJ is populated but not with value from Rep_Name - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepFamilyNamePaperJ", "PASS", f"legalRepFamilyNamePaperJ is populated with values from Rep_Name",test_from_state)

#Test2 - RepName is null -- legalRepFamilyNamePaperJ = CaseRep_Name
def test_legalRepFamilyNamePaperJ_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(col("Rep_Name").isNull() & col("CaseRep_Name").isNotNull() & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepFamilyNamePaperJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("Rep_Name").isNull()) &            
    (col("CaseRep_Name").isNotNull()) &            
    (~col("CaseRep_Name").eqNullSafe(col("legalRepFamilyNamePaperJ")))
    )    
    
    if test_df.count() != 0:
        return TestResult("legalRepFamilyNamePaperJ", "FAIL", f"Failed as legalRepFamilyNamePaperJ is populated but not with value from CaseRep_Name - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepFamilyNamePaperJ", "PASS", f"legalRepFamilyNamePaperJ is populated with values from CaseRep_Name",test_from_state)

#legalRepCompanyPaperJ - IF RepName IS NULL USE CaseRepName     
#Test1 - Rep_Name is not null -- legalRepCompanyPaperJ = RepName   
def test_legalRepCompanyPaperJ_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(col("Rep_Name").isNotNull() & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepCompanyPaperJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("Rep_Name").isNotNull()) &                
    (~col("Rep_Name").eqNullSafe(col("legalRepCompanyPaperJ")))
    )    
    
    if test_df.count() != 0:
        return TestResult("legalRepCompanyPaperJ", "FAIL", f"Failed as legalRepCompanyPaperJ is populated but not with value from Rep_name - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepCompanyPaperJ", "PASS", f"legalRepCompanyPaperJ is populated with values from Rep_name",test_from_state)
    

 #Test2 - Rep_Name is null -- legalRepCompanyPaperJ = CaseRep_Name     
def test_legalRepCompanyPaperJ_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter(col("Rep_Name").isNull() & (col("CaseRep_Name").isNotNull()) & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepCompanyPaperJ", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("Rep_Name").isNull()) &
    (col("CaseRep_Name").isNotNull()) &                                
    (~col("CaseRep_Name").eqNullSafe(col("legalRepCompanyPaperJ")))
    )    
    
    if test_df.count() != 0:
        return TestResult("legalRepCompanyPaperJ", "FAIL", f"Failed as legalRepCompanyPaperJ is populated but not with value from CaseRep_Name - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepCompanyPaperJ", "PASS", f"legalRepCompanyPaperJ is populated with values from CaseRep_Name",test_from_state)


#Test1 - RepId >0 -- legalRepHasAddress = Yes
def test_legalRepHasAddress_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("RepresentativeId") > 0) & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepHasAddress", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("RepresentativeId") > 0) &
    (col("legalRepHasAddress") != "Yes")                                    
    )    
    
    if test_df.count() != 0:
        return TestResult("legalRepHasAddress", "FAIL", f"Failed legalRepHasAddress values set to No when RepresentativeId > 0 found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepHasAddress", "PASS", f"legalRepHasAddress is populated with Yes when RepresentativeId >0",test_from_state)


#test2 -  RepId = 0 and Rep_Postcode is uk postcode legalRepHasAddress = yes
def test_legalRepHasAddress_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("RepresentativeId") == 0) & col("Rep_Postcode").isNotNull() & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepHasAddress", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    #Get Data that is LR rep ==0 and postcode is not null
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("RepresentativeId") == 0) &
    (col("Rep_Postcode").isNotNull())
    )    

    #Add column for is postcode valid
    UK_POSTCODE_REGEX = r"^(GIR 0AA|[A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})$"
    df_validated = test_df.withColumn(
        "is_valid_postcode",
        F.col("Rep_Postcode").rlike(UK_POSTCODE_REGEX)
    )

    #Check if any without a valid postcode have legalRepHasAddress = yes
    df_validated = df_validated.filter(
    (col("is_valid_postcode") == True) &    
    (col("legalRepHasAddress") == "No")
    )
            
    
    if df_validated.count() != 0:
        return TestResult("legalRepHasAddress", "FAIL", f"Failed legalRepHasAddress values set to No when Rep_Postcode is a valid uk postcode - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepHasAddress", "PASS", f"legalRepHasAddress is populated with Yes when Rep_Postcode = true",test_from_state)


#test3 -  RepId = 0 and Rep_Postcode is NOT uk postcode legalRepHasAddress = no
def test_legalRepHasAddress_test3(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("RepresentativeId") == 0) & col("Rep_Postcode").isNotNull() & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepHasAddress", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    #Get Data that is LR rep ==0 and postcode is not null
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("RepresentativeId") == 0) &
    (col("Rep_Postcode").isNotNull())
    )    

    #Add column for is postcode valid
    UK_POSTCODE_REGEX = r"^(GIR 0AA|[A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})$"
    df_validated = test_df.withColumn(
        "is_valid_postcode",
        F.col("Rep_Postcode").rlike(UK_POSTCODE_REGEX)
    )

    #Check if any without a valid postcode have legalRepHasAddress = no
    df_validated = df_validated.filter(
    (col("is_valid_postcode") == False) &    
    (col("legalRepHasAddress") == "Yes")
    )
            
    
    if df_validated.count() != 0:
        return TestResult("legalRepHasAddress", "FAIL", f"Failed legalRepHasAddress values set to Yes when Rep_Postcode is not a valid uk postcode - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepHasAddress", "PASS", f"legalRepHasAddress is populated with No when not a valid Rep_Postcode",test_from_state)


#legalRepAddressUK
def test_legalRepAddressUK_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("RepresentativeId") > 0) & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepAddressUK", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
        (col("dv_representation") == "LR") &
        (col("RepresentativeId") > 0)
    )
    
    test_df = test_df.withColumn(
    "full_address",
      F.concat_ws(" ", F.col("Rep_Address1"), F.col("Rep_Address2"), F.col("Rep_Address3"),
            F.col("Rep_Address4"), F.col("Rep_Address5"), F.col("Rep_Postcode"))
    )

    # NEW STEP: Convert the Struct into a matching String
    test_df = test_df.withColumn(
        "legalRepAddressUK_string",
        F.concat_ws(" ", 
            F.col("legalRepAddressUK.AddressLine1"), 
            F.col("legalRepAddressUK.AddressLine2"), 
            # Note: Your struct has 'PostTown' and 'PostCode', but your 'full_address' 
            # uses Rep_Address3/4/5. Ensure these align logically!
            F.col("legalRepAddressUK.PostTown"), 
            F.col("legalRepAddressUK.PostCode")
        )
    )

    # Now compare String vs String
    test_df = test_df.filter(
        (~col("full_address").eqNullSafe(col("legalRepAddressUK_string")))
    )
    
    # test_df = test_df.filter(   
    # (~col("full_address").eqNullSafe(col("legalRepAddressUK")))
    # )    
    
    if test_df.count() != 0:
        return TestResult("legalRepAddressUK", "FAIL", f"legalRepAddressUK did not match expected value for combined address fields(Rep fields) when RepresentativeId > 0 found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepAddressUK", "PASS", f"legalRepAddressUK matches combinations of addresss fields(Rep fields) when RepresentativeId >0",test_from_state)
    

def test_legalRepAddressUK_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("RepresentativeId") == 0) & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("legalRepAddressUK", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
        (col("dv_representation") == "LR") &
        (col("RepresentativeId") ==0)
    )
    
    test_df = test_df.withColumn(
    "full_address",
      F.concat_ws(" ", F.col("CaseRep_Address1"), F.col("CaseRep_Address2"), F.col("CaseRep_Address3"),
            F.col("CaseRep_Address4"), F.col("CaseRep_Address5"), F.col("CaseRep_Postcode"))
    )

    # NEW STEP: Convert the Struct into a matching String
    test_df = test_df.withColumn(
        "legalRepAddressUK_string",
        F.concat_ws(" ", 
            F.col("legalRepAddressUK.AddressLine1"), 
            F.col("legalRepAddressUK.AddressLine2"), 
            # Note: Your struct has 'PostTown' and 'PostCode', but your 'full_address' 
            # uses Rep_Address3/4/5. Ensure these align logically!
            F.col("legalRepAddressUK.PostTown"), 
            F.col("legalRepAddressUK.PostCode")
        )
    )

    # Now compare String vs String
    test_df = test_df.filter(
        (~col("full_address").eqNullSafe(col("legalRepAddressUK_string")))
    )
    
    # test_df = test_df.filter(   
    # (~col("full_address").eqNullSafe(col("legalRepAddressUK")))
    # )    
    
    if test_df.count() != 0:
        return TestResult("legalRepAddressUK", "FAIL", f"legalRepAddressUK did not match expected value for combined address fields(CaseRep fields) when RepresentativeId = 0 found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("legalRepAddressUK", "PASS", f"legalRepAddressUK matches combinations of addresss fields(CaseRep fields) when RepresentativeId =0",test_from_state)
    
#oocAddressLine1
#Test1 -  #If legalRepHasAddress = Yes and oocAddressLine1 is not null
def test_oocAddressLine1_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("legalRepHasAddress") == "Yes") & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("oocAddressLine1", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("legalRepHasAddress") == "Yes") &
    (col("oocAddressLine1").isNotNull())                                    
    )    
    
    if test_df.count() != 0:
        return TestResult("oocAddressLine1", "FAIL", f"Failed oocAddressLine1 is populated when legalRepHasAddress = Yes - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("oocAddressLine1", "PASS", f"oocAddressLine1 is NOT populated when when legalRepHasAddress = Yes",test_from_state)

 #Test2 - legalRepHasAddress =No Then oocaddress1 = CaseRepAddress1
def test_oocAddressLine1_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("legalRepHasAddress") == "No") & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("oocAddressLine1", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("legalRepHasAddress") == "No") &
    (~col("oocAddressLine1").eqNullSafe(col("CaseRep_Address1")))
    )    
    
    if test_df.count() != 0:
        return TestResult("oocAddressLine1", "FAIL", f"Failed oocAddressLine1 is NOT populated when legalRepHasAddress = No - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("oocAddressLine1", "PASS", f"oocAddressLine1 is populated when when legalRepHasAddress = No",test_from_state)


#oocAddressLine2
#Test1 -  #If legalRepHasAddress = Yes and oocAddressLine2 is not null
def test_oocAddressLine2_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("legalRepHasAddress") == "Yes") & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("oocAddressLine2", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("legalRepHasAddress") == "Yes") &
    (col("oocAddressLine2").isNotNull())                                    
    )    
    
    if test_df.count() != 0:
        return TestResult("oocAddressLine2", "FAIL", f"Failed oocAddressLine2 is populated when legalRepHasAddress = Yes - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("oocAddressLine2", "PASS", f"oocAddressLine2 is NOT populated when when legalRepHasAddress = Yes",test_from_state)

 #Test2 - legalRepHasAddress =No Then oocaddress2 = CaseRepAddress2
def test_oocAddressLine2_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("legalRepHasAddress") == "No") & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("oocAddressLine2", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("legalRepHasAddress") == "No") &
    (~col("oocAddressLine2").eqNullSafe(col("CaseRep_Address2")))
    )    
    
    if test_df.count() != 0:
        return TestResult("oocAddressLine2", "FAIL", f"Failed oocAddressLine2 is NOT populated when legalRepHasAddress = No - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("oocAddressLine2", "PASS", f"oocAddressLine2 is populated when when legalRepHasAddress = No",test_from_state)


#oocAddressLine3
#Test1 -  #If legalRepHasAddress = Yes and oocAddressLine3 is not null
def test_oocAddressLine3_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("legalRepHasAddress") == "Yes") & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("oocAddressLine3", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("legalRepHasAddress") == "Yes") &
    (col("oocAddressLine3").isNotNull())                                    
    )    
    
    if test_df.count() != 0:
        return TestResult("oocAddressLine3", "FAIL", f"Failed oocAddressLine3 is populated when legalRepHasAddress = Yes - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("oocAddressLine3", "PASS", f"oocAddressLine3 is NOT populated when when legalRepHasAddress = Yes",test_from_state)

 #Test2 - legalRepHasAddress =No Then oocaddress3 = oocAddressLine3
def test_oocAddressLine3_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("legalRepHasAddress") == "No") & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("oocAddressLine3", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("legalRepHasAddress") == "No") &    
    (col("oocAddressLine3") != (col("CaseRep_Address3") + ", " + col("CaseRep_Address4")))
    )    
    
    if test_df.count() != 0:
        return TestResult("oocAddressLine3", "FAIL", f"Failed oocAddressLine3 is NOT populated when legalRepHasAddress = No - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("oocAddressLine3", "PASS", f"oocAddressLine3 is populated when when legalRepHasAddress = No",test_from_state)



#oocAddressLine4
#Test1 -  #If legalRepHasAddress = Yes and oocAddressLine4 is not null
def test_oocAddressLine4_test1(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("legalRepHasAddress") == "Yes") & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("oocAddressLine4", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("legalRepHasAddress") == "Yes") &
    (col("oocAddressLine4").isNotNull())                                    
    )    
    
    if test_df.count() != 0:
        return TestResult("oocAddressLine4", "FAIL", f"Failed oocAddressLine4 is populated when legalRepHasAddress = Yes - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("oocAddressLine4", "PASS", f"oocAddressLine4 is NOT populated when when legalRepHasAddress = Yes",test_from_state)

 #Test2 - legalRepHasAddress =No Then oocaddress3 = oocAddressLine3
def test_oocAddressLine4_test2(test_df_sd):
    #Check we have Records To test
    if test_df_sd.filter((col("legalRepHasAddress") == "No") & (col("dv_representation") == "LR")).count() ==0:    
        return TestResult("oocAddressLine4", "FAIL", "NO RECORDS TO TEST",test_from_state)
                                            
    test_df = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("legalRepHasAddress") == "No") &    
    (col("oocAddressLine4") != (col("CaseRep_Address4") + ", " + col("CaseRep_Postcode")))
    )    
    
    if test_df.count() != 0:
        return TestResult("oocAddressLine4", "FAIL", f"Failed oocAddressLine4 is NOT populated when legalRepHasAddress = No - found :{str(test_df.count())}",test_from_state)
    else:
        return TestResult("oocAddressLine4", "PASS", f"oocAddressLine4 is populated when when legalRepHasAddress = No",test_from_state)

###################
#oocLrCountryGovUkAdminJ
###################
import string
import pycountry
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import coalesce, lit

# Function to combine all LR address fields into 1 string
def makeCaseRepFullAddress (row):
    return ', '.join([str(row['CaseRepAddress1']), str(row['CaseRepAddress2']), str(row['CaseRepAddress3']), str(row['CaseRepAddress4']), str(row['CaseRepAddress5']), str(row['CaseRepPostcode'])])


# Make a list of countries
all_countries = pycountry.countries
lower_countries = []
for country in all_countries:
    lower_countries.append(country.name.lower() + 'x')

 
def getCountryLR(row):
    print("into getCountryLR")
    # print("address: " + address)
    countries_matched = []
    address = row
    low_address = address.lower()
    
    translator = str.maketrans('', '', string.punctuation)
    
    split_address = low_address.split(' ')
    for word in reversed(split_address):
        if word.strip() in lower_countries:
            countries_matched.append(word.strip().capitalize())
            break
    
    if len(countries_matched) > 0:
        return ', '.join(countries_matched).removesuffix("x")
    
    for word in reversed(split_address):
        clean_word = word.translate(translator)
        if clean_word.lower().strip() in lower_countries:
            countries_matched.append(clean_word.strip().capitalize())
            break
    
    if len(countries_matched) > 0:
        return ', '.join(countries_matched).removesuffix("x")
    
    split_address = low_address.split(',')
    for word in reversed(split_address):
        clean_word = word.translate(translator)
        if clean_word.strip() in lower_countries:
            countries_matched.append(clean_word.strip().capitalize())
            break
            
    if len(countries_matched) > 0:
        return ', '.join(countries_matched).removesuffix("x")

    split_address = re.split(r"[,\s]+", low_address)
    for word in split_address:
        clean_word = word.translate(translator)
        if clean_word.strip() in lower_countries:
            countries_matched.append(clean_word.strip().capitalize())
            break
            
    return ', '.join(countries_matched).removesuffix("x")

##oocLrCountryGovUkAdminJ
def test_oocLrCountryGovUkAdminJ_test1(test_df_sd,external_storage,spark):
    from pyspark.sql.functions import (
        col, when, lit, array, struct, collect_list, 
        max as spark_max, date_format, row_number, expr, 
        size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
        collect_set, current_timestamp,transform, first, array_contains
    )
    from pyspark.sql import functions as F


    test_df_sd = test_df_sd.filter(
    (col("dv_representation") == "LR") &
    (col("legalRepHasAddress") == "No")        
    )    
    
    makeCaseRepFullAddress_udf = udf(
        lambda CaseRep_Address1, CaseRep_Address2, CaseRep_Address3, CaseRep_Address4, CaseRep_Address5, CaseRep_Postcode:
            ', '.join([str(x) for x in [CaseRep_Address1, CaseRep_Address2, CaseRep_Address3, CaseRep_Address4, CaseRep_Address5, CaseRep_Postcode] if x is not None]),
        StringType()
    )

    getCountryLR_udf = udf(getCountryLR, StringType())

    test_df_sd = test_df_sd.withColumn(
        "caseRepFullAddress",
        makeCaseRepFullAddress_udf(
            col("CaseRep_Address1"),
            col("CaseRep_Address2"),
            col("CaseRep_Address3"),
            col("CaseRep_Address4"),
            col("CaseRep_Address5"),
            col("CaseRep_Postcode")
        )
    )

    test_df_sd = test_df_sd.withColumn(
        "countryresult",
        getCountryLR_udf(col("caseRepFullAddress"))
    )
    
    #Map Country to Code
    from pyspark.sql.types import (
        StructType,
        StructField,
    )
    schema = StructType([
            StructField("countryFromAddress", StringType(), True),
            StructField("oocLrCountryGovUkAdminJ", StringType(), True),
            StructField("countryGovUkOocAdminJ", StringType(), True)
        ])
    csv_file =  (
            spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "false")
                .option("encoding", "UTF-8")
                .schema(schema)
                .load(f"abfss://external-csv@{external_storage}.dfs.core.windows.net/ReferenceData/countries_countryFromAddress.csv")
                .select("countryFromAddress", "oocLrCountryGovUkAdminJ", "countryGovUkOocAdminJ")
        )

    #Join Country Code to Country    
    from pyspark.sql.functions import when, col, lit, length
    test_df_sd = test_df_sd.join(
        csv_file,
        test_df_sd.countryresult == csv_file.countryFromAddress,
        "left"
    ).withColumn("countrycode", when(length(csv_file.oocLrCountryGovUkAdminJ) > 0, csv_file.oocLrCountryGovUkAdminJ).otherwise(lit(None))
    ).select(
        test_df_sd["*"],
        "countrycode"
        # coalesce(csv_file.oocLrCountryGovUkAdminJ, lit("")).alias("countrycode")
    )
     
    test_df_sd = test_df_sd.filter(    
        (~col("countrycode").eqNullSafe(col("oocLrCountryGovUkAdminJ")))                                  
        )

    if test_df_sd.count() != 0:
        return TestResult("oocLrCountryGovUkAdminJ", "FAIL", f"Failed oocLrCountryGovUkAdminJ Mismatch on expected Country Code - found :{str(test_df_sd.count())}",test_from_state),test_df_sd 
    else:
        return TestResult("oocLrCountryGovUkAdminJ", "PASS", f"All country codes in oocLrCountryGovUkAdminJ match expected",test_from_state),test_df_sd
    
############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "ccdReferenceNumberForDisplay",
            "hasOtherAppeals",
            "adminDeclaration1",
            "s94bStatus",
            "isAdmin",
            "isEjp",
            "isHomeOfficeIntegrationEnabled",
            "homeOfficeNotificationsEligible",
            "ariaDesiredState",
            "ariaMigrationTaskDueDays",
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
            "uploadAdditionalEvidenceHomeOfficeActionAvailable",
            "uploadTheAppealFormDocs",
            "tribunalDocuments",
            "legalRepresentativeDocuments"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("defaults", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state)

def test_defaultValues(test_df):
    expected_defaults = {
        "ccdReferenceNumberForDisplay": "",
        "hasOtherAppeals": "NotSure",
        "s94bStatus": "No",
        "isAdmin": "Yes",
        "isEjp": "No",
        "ariaDesiredState": "pendingPayment",
        "ariaMigrationTaskDueDays": 14,
        "submitNotificationStatus": "",
        "isFeePaymentEnabled": "Yes",
        "isRemissionsEnabled": "Yes",
        "isOutOfCountryEnabled": "Yes",
        "isIntegrated": "No",
        "isNabaEnabled": "No",
        "isNabaAdaEnabled": "Yes",
        "isNabaEnabledOoc": "No",
        "isCaseUsingLocationRefData": "Yes",
        "hasAddedLegalRepDetails": "Yes",
        "autoHearingRequestEnabled": "No",
        "isDlrmFeeRemissionEnabled": "Yes",
        "isDlrmFeeRefundEnabled": "Yes",
        "sendDirectionActionAvailable": "Yes",
        "changeDirectionDueDateActionAvailable": "No",
        "markEvidenceAsReviewedActionAvailable": "No",
        "uploadAddendumEvidenceActionAvailable": "No",
        "uploadAdditionalEvidenceActionAvailable": "No",
        "displayMarkAsPaidEventForPartialRemission": "No",
        "haveHearingAttendeesAndDurationBeenRecorded": "No",
        "markAddendumEvidenceAsReviewedActionAvailable": "No",
        "uploadAddendumEvidenceLegalRepActionAvailable": "No",
        "uploadAddendumEvidenceHomeOfficeActionAvailable": "No",
        "uploadAddendumEvidenceAdminOfficerActionAvailable": "No",
        "uploadAdditionalEvidenceHomeOfficeActionAvailable": "No"
    }

    expected_arrays = {
        "adminDeclaration1": "hasDeclared",
        "notificationsSent": None,
        "uploadTheAppealFormDocs": None,
        "tribunalDocuments": None,
        "legalRepresentativeDocuments": None
    }

    failed_field_names = []

    for field, expected in expected_defaults.items():
        condition = (col(field) != expected)
        if test_df.filter(condition).count() > 0:
            failed_field_names.append(field)

    for field, contains_val in expected_arrays.items():
        if contains_val:
            condition = (~array_contains(col(field), contains_val))
        else:
            condition = (size(col(field)) != 0)
            
        if test_df.filter(condition).count() > 0:
            failed_field_names.append(field)

    if len(failed_field_names) > 0:
        list_of_fail_field_names = ", ".join(failed_field_names)
        
        return TestResult(
            "defaultMappings", 
            "FAIL", 
            f"Failed default mapping test: {len(list_of_fail_field_names)} records failed. Fields: [{list_of_fail_field_names}]", 
            test_from_state
        )
    else:
        return TestResult("defaultMappings", "PASS", "All defaults match.", test_from_state)

############################################################################################
#######################
#hearingCentre Init code
#######################



