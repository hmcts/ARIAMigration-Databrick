from Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted import appealSubmittedOnly

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
import pytest


SILVER_M1_SCHEMA = StructType([
    StructField("CaseNo", StringType()),
    StructField("dv_CCDAppealType", StringType()),
    StructField("dv_representation", StringType()),
    StructField("DateLodged", StringType()),
])


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("appealSubmitted_appealSubmittedOnly") \
        .getOrCreate()


class TestAppealSubmittedOnly:

    def silver_m1(self, spark, rows):
        return spark.createDataFrame(rows, SILVER_M1_SCHEMA)

    def test_completeCaseReviewDate_pa(self, spark):
        df, df_audit = appealSubmittedOnly(
            self.silver_m1(spark, [("1", "PA", "LR", "2024-01-15")])
        )

        assert df.select("completeCaseReviewDate").collect()[0][0] == "2024-01-15"

    def test_completeCaseReviewDate_rp(self, spark):
        df, df_audit = appealSubmittedOnly(
            self.silver_m1(spark, [("1", "RP", "AIP", "2024-02-20")])
        )

        assert df.select("completeCaseReviewDate").collect()[0][0] == "2024-02-20"

    def test_completeCaseReviewDate_non_pa_rp(self, spark):
        df, df_audit = appealSubmittedOnly(
            self.silver_m1(spark, [("1", "EA", "LR", "2024-01-15")])
        )

        assert df.select("completeCaseReviewDate").collect()[0][0] is None

    def test_completeCaseReviewDate_null_appeal_type(self, spark):
        df, df_audit = appealSubmittedOnly(
            self.silver_m1(spark, [("1", None, "LR", "2024-01-15")])
        )

        assert df.select("completeCaseReviewDate").collect()[0][0] is None

    def test_completeCaseReviewDate_multiple_cases(self, spark):
        df, df_audit = appealSubmittedOnly(
            self.silver_m1(spark, [
                ("1", "PA", "LR", "2024-01-15"),
                ("2", "RP", "AIP", "2024-02-20"),
                ("3", "EA", "LR", "2024-03-25"),
            ])
        )

        resultList = df.orderBy(col("CaseNo").cast("int")).select("completeCaseReviewDate").collect()

        assert resultList[0][0] == "2024-01-15"
        assert resultList[1][0] == "2024-02-20"
        assert resultList[2][0] is None

    def test_audit_inputFields_and_values(self, spark):
        df, df_audit = appealSubmittedOnly(
            self.silver_m1(spark, [("1", "PA", "LR", "2024-01-15")])
        )

        result = df_audit.select(
            col("completeCaseReviewDate_inputFields")[0]["col1"].alias("field_appealType"),
            col("completeCaseReviewDate_inputFields")[0]["col2"].alias("field_representation"),
            col("completeCaseReviewDate_inputValues")[0]["dv_CCDAppealType"].alias("value_appealType"),
            col("completeCaseReviewDate_inputValues")[0]["dv_representation"].alias("value_representation"),
            "completeCaseReviewDate",
            "completeCaseReviewDates_Transformed",
        ).collect()[0]

        assert result["field_appealType"] == "dv_CCDAppealType"
        assert result["field_representation"] == "dv_representation"
        assert result["value_appealType"] == "PA"
        assert result["value_representation"] == "LR"
        assert result["completeCaseReviewDate"] == "2024-01-15"
        assert result["completeCaseReviewDates_Transformed"] == "yes"

    def test_audit_transformed_flag_always_yes(self, spark):
        """The audit flag reflects that the field was evaluated, not that a value was produced."""
        df, df_audit = appealSubmittedOnly(
            self.silver_m1(spark, [("1", "EA", "LR", "2024-01-15")])
        )

        result = df_audit.collect()[0]

        assert result["completeCaseReviewDate"] is None
        assert result["completeCaseReviewDates_Transformed"] == "yes"
