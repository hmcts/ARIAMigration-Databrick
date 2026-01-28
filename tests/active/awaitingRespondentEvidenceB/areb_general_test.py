from Databricks.ACTIVE.APPEALS.shared_functions.AwaitingEvidenceRespondant_b import generalDefault

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from unittest.mock import patch
import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("awaitingRespondentEvidenceB_general") \
        .getOrCreate()


class TestAwaitingRespondentEvidenceBGeneral:
    SILVER_M1_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("dv_representation", StringType()),
        StructField("lu_appealType", StringType())
    ])

    def aera_general_default_df(self, spark):
        AERA_COLUMNS = StructType([
            StructField("CaseNo", StringType())
        ])

        caseList = [
            ("1",),
            ("2",)
        ]

        return spark.createDataFrame(caseList, AERA_COLUMNS)

    def test_generalDefault(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.AwaitingEvidenceRespondant_b.AERa') as AERa:
            AERa.generalDefault.return_value = self.aera_general_default_df(spark)

            silver_m1 = spark.createDataFrame([
                ("1", "rep", "appeal"),
                ("2", "rep", "appeal")
            ], self.SILVER_M1_SCHEMA)

            df = generalDefault(silver_m1)

            resultsList = df.orderBy(col("CaseNo").cast("int")).select("uploadHomeOfficeBundleActionAvailable").collect()

            assert resultsList[0][0] == "No"
            assert resultsList[1][0] == "No"
