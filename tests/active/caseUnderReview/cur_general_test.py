from Databricks.ACTIVE.APPEALS.shared_functions.caseUnderReview import generalDefault

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from unittest.mock import patch
import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("caseUnderReview_general") \
        .getOrCreate()


class TestCaseUnderReviewGeneral:
    SILVER_M1_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("dv_representation", StringType()),
        StructField("lu_appealType", StringType())
    ])

    def aerb_general_default_df(self, spark):
        # Alphabetical ordered columns for re-ordering by function.
        AERB_COLUMNS = StructType([
            StructField("CaseNo", StringType())
        ])

        caseList = [
            ("1",),
            ("2",),
            ("3",)
        ]

        return spark.createDataFrame(caseList, AERB_COLUMNS)

    def test_generalDefault(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.caseUnderReview.AERb') as AERb:
            AERb.generalDefault.return_value = self.aerb_general_default_df(spark)

            silver_m1 = spark.createDataFrame([
                ("1", "LR", "appeal"),
                ("2", "LR", "appeal"),
                ("3", "AIP", "appeal")
            ], self.SILVER_M1_SCHEMA)

            df = generalDefault(silver_m1)

            caseArgumentAvailableList = df.orderBy(col("CaseNo").cast("int")).select("caseArgumentAvailable").collect()

            assert caseArgumentAvailableList[0][0] == "Yes"
            assert caseArgumentAvailableList[1][0] == "Yes"
            assert len(caseArgumentAvailableList) == 2
