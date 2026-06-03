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


class TestAppealSubmittedHomeOfficeDetails:
    CASE_NO_COLUMNS = StructType([
        StructField("CaseNo", StringType())
    ])

    def pp_df(self, spark, num_of_cases=0):
        case_no_df = spark.createDataFrame(
            ((str(n),) for n in range(1, num_of_cases + 1)),
            self.CASE_NO_COLUMNS
        )
        return case_no_df, case_no_df

    def test_homeOfficeSearchStatus(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.homeOfficeDetails.return_value = self.pp_df(spark, 2)

            df, df_audit = homeOfficeDetails(MagicMock(), MagicMock(), MagicMock(), MagicMock())

            resultList = df.orderBy(col("CaseNo").cast("int")).select("homeOfficeSearchStatus").collect()

            assert resultList[0][0] == "SUCCESS"
            assert resultList[1][0] == "SUCCESS"

    def test_homeOfficeSearchNoMatch(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.homeOfficeDetails.return_value = self.pp_df(spark, 2)

            df, df_audit = homeOfficeDetails(MagicMock(), MagicMock(), MagicMock(), MagicMock())

            resultList = df.orderBy(col("CaseNo").cast("int")).select("homeOfficeSearchNoMatch").collect()

            assert resultList[0][0] == "NO_MATCH"
            assert resultList[1][0] == "NO_MATCH"

    def test_matchingAppellantDetailsFound(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.homeOfficeDetails.return_value = self.pp_df(spark, 2)

            df, df_audit = homeOfficeDetails(MagicMock(), MagicMock(), MagicMock(), MagicMock())

            resultList = df.orderBy(col("CaseNo").cast("int")).select("matchingAppellantDetailsFound").collect()

            assert resultList[0][0] == "No"
            assert resultList[1][0] == "No"

    def test_homeOfficeAppellantsList(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.homeOfficeDetails.return_value = self.pp_df(spark, 1)

            df, df_audit = homeOfficeDetails(MagicMock(), MagicMock(), MagicMock(), MagicMock())

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

    def test_homeOfficeCaseStatusDate(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.homeOfficeDetails.return_value = self.pp_df(spark, 1)

            df, df_audit = homeOfficeDetails(MagicMock(), MagicMock(), MagicMock(), MagicMock())

            result = df.select(
                col("homeOfficeCaseStatusDate.displayDateOfBirth").alias("displayDateOfBirth"),
                col("homeOfficeCaseStatusDate.displayAppellantDetailsTitle").alias("displayAppellantDetailsTitle"),
                col("homeOfficeCaseStatusDate.displayApplicationDetailsTitle").alias("displayApplicationDetailsTitle"),
                col("homeOfficeCaseStatusDate.person.familyName").alias("familyName"),
                col("homeOfficeCaseStatusDate.person.fullName").alias("fullName"),
                col("homeOfficeCaseStatusDate.person.givenName").alias("givenName"),
                col("homeOfficeCaseStatusDate.person.dayOfBirth").alias("dayOfBirth"),
                col("homeOfficeCaseStatusDate.person.monthOfBirth").alias("monthOfBirth"),
                col("homeOfficeCaseStatusDate.person.yearOfBirth").alias("yearOfBirth"),
                col("homeOfficeCaseStatusDate.person.gender.code").alias("gender_code"),
                col("homeOfficeCaseStatusDate.person.nationality.code").alias("nationality_code"),
                col("homeOfficeCaseStatusDate.applicationStatus.roleType.code").alias("roleType_code"),
                col("homeOfficeCaseStatusDate.applicationStatus.roleSubType.code").alias("roleSubType_code"),
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
