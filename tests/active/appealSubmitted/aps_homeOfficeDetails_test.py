from Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted import homeOfficeDetails

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from unittest.mock import MagicMock, patch
import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("appealSubmitted_homeOfficeDetails") \
        .getOrCreate()


SILVER_M1_SCHEMA = StructType([
    StructField("CaseNo", StringType()),
    StructField("dv_CCDAppealType", StringType()),
    StructField("dv_representation", StringType()),
])

CASE_NO_COLUMNS = StructType([
    StructField("CaseNo", StringType()),
])


class TestAppealSubmittedHomeOfficeDetails:

    def silver_m1(self, spark, case_nos, appeal_type="PA"):
        return spark.createDataFrame(
            [(cn, appeal_type, "LR") for cn in case_nos],
            SILVER_M1_SCHEMA,
        )

    def pp_df(self, spark, num_of_cases=0, appeal_type="PA"):
        case_no_df = spark.createDataFrame(
            [(str(n),) for n in range(1, num_of_cases + 1)],
            CASE_NO_COLUMNS,
        )
        return case_no_df, case_no_df

    def test_homeOfficeSearchStatus_pa(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PPD') as PPD:
            PPD.homeOfficeDetails.return_value = self.pp_df(spark, 2, appeal_type="PA")

            df, df_audit = homeOfficeDetails(
                self.silver_m1(spark, ["1", "2"], appeal_type="PA"),
                MagicMock(), MagicMock(), MagicMock()
            )

            resultList = df.orderBy(col("CaseNo").cast("int")).select("homeOfficeSearchStatus").collect()

            assert resultList[0][0] == "SUCCESS"
            assert resultList[1][0] == "SUCCESS"

    def test_homeOfficeSearchStatus_rp(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PPD') as PPD:
            PPD.homeOfficeDetails.return_value = self.pp_df(spark, 1, appeal_type="RP")

            df, df_audit = homeOfficeDetails(
                self.silver_m1(spark, ["1"], appeal_type="RP"),
                MagicMock(), MagicMock(), MagicMock()
            )

            assert df.select("homeOfficeSearchStatus").collect()[0][0] == "SUCCESS"

    def test_homeOfficeSearchStatus_non_pa_rp(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PPD') as PPD:
            PPD.homeOfficeDetails.return_value = self.pp_df(spark, 1, appeal_type="EA")

            df, df_audit = homeOfficeDetails(
                self.silver_m1(spark, ["1"], appeal_type="EA"),
                MagicMock(), MagicMock(), MagicMock()
            )

            assert df.select("homeOfficeSearchStatus").collect()[0][0] is None

    def test_homeOfficeSearchNoMatch_pa(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PPD') as PPD:
            PPD.homeOfficeDetails.return_value = self.pp_df(spark, 2, appeal_type="PA")

            df, df_audit = homeOfficeDetails(
                self.silver_m1(spark, ["1", "2"], appeal_type="PA"),
                MagicMock(), MagicMock(), MagicMock()
            )

            resultList = df.orderBy(col("CaseNo").cast("int")).select("homeOfficeSearchNoMatch").collect()

            assert resultList[0][0] == "NO_MATCH"
            assert resultList[1][0] == "NO_MATCH"

    def test_homeOfficeSearchNoMatch_non_pa_rp(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PPD') as PPD:
            PPD.homeOfficeDetails.return_value = self.pp_df(spark, 1, appeal_type="HU")

            df, df_audit = homeOfficeDetails(
                self.silver_m1(spark, ["1"], appeal_type="HU"),
                MagicMock(), MagicMock(), MagicMock()
            )

            assert df.select("homeOfficeSearchNoMatch").collect()[0][0] is None

    def test_matchingAppellantDetailsFound_pa(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PPD') as PPD:
            PPD.homeOfficeDetails.return_value = self.pp_df(spark, 2, appeal_type="PA")

            df, df_audit = homeOfficeDetails(
                self.silver_m1(spark, ["1", "2"], appeal_type="PA"),
                MagicMock(), MagicMock(), MagicMock()
            )

            resultList = df.orderBy(col("CaseNo").cast("int")).select("matchingAppellantDetailsFound").collect()

            assert resultList[0][0] == "No"
            assert resultList[1][0] == "No"

    def test_matchingAppellantDetailsFound_non_pa_rp(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PPD') as PPD:
            PPD.homeOfficeDetails.return_value = self.pp_df(spark, 1, appeal_type="DC")

            df, df_audit = homeOfficeDetails(
                self.silver_m1(spark, ["1"], appeal_type="DC"),
                MagicMock(), MagicMock(), MagicMock()
            )

            assert df.select("matchingAppellantDetailsFound").collect()[0][0] is None

    def test_homeOfficeAppellantsList_pa(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PPD') as PPD:
            PPD.homeOfficeDetails.return_value = self.pp_df(spark, 1, appeal_type="PA")

            df, df_audit = homeOfficeDetails(
                self.silver_m1(spark, ["1"], appeal_type="PA"),
                MagicMock(), MagicMock(), MagicMock()
            )

            result = df.select(
                col("homeOfficeAppellantsList.value.code").alias("value_code"),
                col("homeOfficeAppellantsList.value.label").alias("value_label"),
                col("homeOfficeAppellantsList.list_items")[0]["code"].alias("list_item_code"),
                col("homeOfficeAppellantsList.list_items")[0]["label"].alias("list_item_label"),
            ).collect()

            assert result[0]["value_code"] == "NoMatch"
            assert result[0]["value_label"] == "No Match"
            assert result[0]["list_item_code"] == "NoMatch"
            assert result[0]["list_item_label"] == "No Match"

    def test_homeOfficeAppellantsList_non_pa_rp(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PPD') as PPD:
            PPD.homeOfficeDetails.return_value = self.pp_df(spark, 1, appeal_type="EU")

            df, df_audit = homeOfficeDetails(
                self.silver_m1(spark, ["1"], appeal_type="EU"),
                MagicMock(), MagicMock(), MagicMock()
            )

            assert df.select("homeOfficeAppellantsList").collect()[0][0] is None

    def test_homeOfficeCaseStatusData_pa(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PPD') as PPD:
            PPD.homeOfficeDetails.return_value = self.pp_df(spark, 1, appeal_type="PA")

            df, df_audit = homeOfficeDetails(
                self.silver_m1(spark, ["1"], appeal_type="PA"),
                MagicMock(), MagicMock(), MagicMock()
            )

            result = df.select(
                col("homeOfficeCaseStatusData.displayDateOfBirth").alias("displayDateOfBirth"),
                col("homeOfficeCaseStatusData.displayAppellantDetailsTitle").alias("displayAppellantDetailsTitle"),
                col("homeOfficeCaseStatusData.displayApplicationDetailsTitle").alias("displayApplicationDetailsTitle"),
                col("homeOfficeCaseStatusData.person.familyName").alias("familyName"),
                col("homeOfficeCaseStatusData.person.fullName").alias("fullName"),
                col("homeOfficeCaseStatusData.person.givenName").alias("givenName"),
                col("homeOfficeCaseStatusData.person.dayOfBirth").alias("dayOfBirth"),
                col("homeOfficeCaseStatusData.person.monthOfBirth").alias("monthOfBirth"),
                col("homeOfficeCaseStatusData.person.yearOfBirth").alias("yearOfBirth"),
                col("homeOfficeCaseStatusData.person.gender.code").alias("gender_code"),
                col("homeOfficeCaseStatusData.person.nationality.code").alias("nationality_code"),
                col("homeOfficeCaseStatusData.applicationStatus.roleType.code").alias("roleType_code"),
                col("homeOfficeCaseStatusData.applicationStatus.roleSubType.code").alias("roleSubType_code"),
            ).collect()

            assert result[0]["displayDateOfBirth"] == "No match"
            assert result[0]["displayAppellantDetailsTitle"] == "<h2>Appellant details</h2>"
            assert result[0]["displayApplicationDetailsTitle"] == "<h2>Application details</h2>"
            assert result[0]["familyName"] == "No match"
            assert result[0]["fullName"] == "No match"
            assert result[0]["givenName"] == "No match"
            assert result[0]["dayOfBirth"] == 0
            assert result[0]["monthOfBirth"] == 0
            assert result[0]["yearOfBirth"] == 0
            assert result[0]["gender_code"] == "No match"
            assert result[0]["nationality_code"] == "No match"
            assert result[0]["roleType_code"] == "No match"
            assert result[0]["roleSubType_code"] == "No match"

    def test_homeOfficeCaseStatusData_non_pa_rp(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PPD') as PPD:
            PPD.homeOfficeDetails.return_value = self.pp_df(spark, 1, appeal_type="EA")

            df, df_audit = homeOfficeDetails(
                self.silver_m1(spark, ["1"], appeal_type="EA"),
                MagicMock(), MagicMock(), MagicMock()
            )

            assert df.select("homeOfficeCaseStatusData").collect()[0][0] is None
