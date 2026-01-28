from Databricks.ACTIVE.APPEALS.shared_functions.AwaitingEvidenceRespondant_a import appellantDetails

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from unittest.mock import MagicMock, patch
import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("awaitingRespondentEvidenceA_appellantDetails") \
        .getOrCreate()


class TestAwaitingRespondentEvidenceAAppellantDetails:
    SILVER_M1_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("dv_representation", StringType()),
        StructField("lu_appealType", StringType())
    ])

    def payment_pending_appellant_details_df(self, spark):
        # Alphabetical ordered columns for re-ordering by function.
        PP_COLUMNS = StructType([
            StructField("CaseNo", StringType()),
            StructField("appellantGivenNames", StringType()),
            StructField("appellantFamilyName", StringType()),
            StructField("addressLine1AdminJ", StringType()),
            StructField("addressLine2AdminJ", StringType()),
            StructField("addressLine3AdminJ", StringType()),
            StructField("addressLine4AdminJ", StringType()),
            StructField("appealOutOfCountry", StringType()),
            StructField("appellantAddress", StringType()),
            StructField("appellantDateOfBirth", StringType()),
            StructField("appellantHasFixedAddress", StringType()),
            StructField("appellantHasFixedAddressAdminJ", StringType()),
            StructField("appellantInUk", StringType()),
            StructField("appellantNameForDisplay", StringType()),
            StructField("appellantNationalities", StringType()),
            StructField("appellantNationalitiesDescription", StringType()),
            StructField("appellantStateless", StringType()),
            StructField("caseNameHmctsInternal", StringType()),
            StructField("countryGovUkOocAdminJ", StringType()),
            StructField("deportationOrderOptions", StringType()),
            StructField("email", StringType()),
            StructField("hmctsCaseNameInternal", StringType()),
            StructField("internalAppellantEmail", StringType()),
            StructField("internalAppellantMobileNumber", StringType()),
            StructField("isAppellantMinor", StringType()),
            StructField("mobileNumber", StringType()),
            StructField("oocAppealAdminJ", StringType())
        ])

        caseList = [
            ("1", "John", "Smith") + (None,) * 24,
            ("2", "Jane", "Doe") + (None,) * 24
        ]

        return spark.createDataFrame(caseList, PP_COLUMNS), spark.createDataFrame(caseList, PP_COLUMNS)

    def test_appellantFullName(self, spark):
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
