from Databricks.ACTIVE.APPEALS.shared_functions.caseUnderReview import hearingResponse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from unittest.mock import MagicMock, patch
import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("caseUnderReview_hearingResponse") \
        .getOrCreate()


class TestCaseUnderReviewHearingResponse:
    SILVER_M1_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("dv_representation", StringType()),
        StructField("lu_appealType", StringType())
    ])

    SILVER_M3_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("StatusId", IntegerType()),
        StructField("CaseStatus", IntegerType()),
        StructField("Outcome", IntegerType()),
        StructField("ListedCentre", StringType()),
        StructField("KeyDate", StringType()),
        StructField("HearingType", StringType()),
        StructField("CourtName", StringType()),
        StructField("ListType", StringType()),
        StructField("StartTime", StringType()),
        StructField("Judge1FT_Surname", StringType()),
        StructField("Judge1FT_Forenames", StringType()),
        StructField("Judge1FT_Title", StringType()),
        StructField("Judge2FT_Surname", StringType()),
        StructField("Judge2FT_Forenames", StringType()),
        StructField("Judge2FT_Title", StringType()),
        StructField("Judge3FT_Surname", StringType()),
        StructField("Judge3FT_Forenames", StringType()),
        StructField("Judge3FT_Title", StringType()),
        StructField("CourtClerk_Surname", StringType()),
        StructField("CourtClerk_Forenames", StringType()),
        StructField("CourtClerk_Title", StringType()),
        StructField("StartTime", StringType()),
        StructField("TimeEstimate", StringType()),
        StructField("Notes", StringType())
    ])

    SILVER_M6_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("Required", StringType()),
        StructField("Judge_Surname", StringType()),
        StructField("Judge_Forenames", StringType()),
        StructField("Judge_Title", StringType())
    ])

    def test_additionalInstructionsTribunalResponse(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.AwaitingEvidenceRespondant_a.PP') as PP:
            PP.appellantDetails.return_value = self.payment_pending_appellant_details_df(spark)

            silver_m1 = spark.createDataFrame([
                ("1", "rep", "appeal"),
                ("2", "rep", "appeal")
            ], self.SILVER_M1_SCHEMA)

            df, df_audit = appellantDetails(silver_m1, MagicMock(), MagicMock(), MagicMock(), MagicMock())

            resultList = df.orderBy(col("CaseNo").cast("int")).select("appellantFullName").collect()

            assert resultList[0][0] == "John Smith"
            assert resultList[1][0] == "Jane Doe"

            assert df.columns == ["CaseNo", "appellantFamilyName", "appellantGivenNames", "appellantFullName", "appellantNameForDisplay",
                 "appellantDateOfBirth", "isAppellantMinor", "caseNameHmctsInternal", "hmctsCaseNameInternal",
                 "internalAppellantEmail", "email", "internalAppellantMobileNumber", "mobileNumber",
                 "appellantInUk", "appealOutOfCountry", "oocAppealAdminJ", "appellantHasFixedAddress", "appellantHasFixedAddressAdminJ",
                 "appellantAddress", "addressLine1AdminJ", "addressLine2AdminJ", "addressLine3AdminJ", "addressLine4AdminJ", "countryGovUkOocAdminJ",
                 "appellantStateless", "appellantNationalities", "appellantNationalitiesDescription", "deportationOrderOptions"]
