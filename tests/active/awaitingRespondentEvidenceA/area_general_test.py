from Databricks.ACTIVE.APPEALS.shared_functions.AwaitingEvidenceRespondant_a import generalDefault

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from unittest.mock import patch
import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("awaitingRespondentEvidenceA_general") \
        .getOrCreate()


class TestAwaitingRespondentEvidenceAGeneral:
    SILVER_M1_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("dv_representation", StringType()),
        StructField("lu_appealType", StringType())
    ])

    def payment_pending_general_default_df(self, spark):
        # Alphabetical ordered columns for re-ordering by function.
        PP_COLUMNS = StructType([
            StructField("CaseNo", StringType())
        ])

        caseList = [
            ("1",),
            ("2",)
        ]

        return spark.createDataFrame(caseList, PP_COLUMNS)

    def test_generalDefault(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.AwaitingEvidenceRespondant_a.PP') as PP:
            PP.generalDefault.return_value = self.payment_pending_general_default_df(spark)

            silver_m1 = spark.createDataFrame([
                ("1", "rep", "appeal"),
                ("2", "rep", "appeal")
            ], self.SILVER_M1_SCHEMA)

            df = generalDefault(silver_m1)

            directionsList = df.orderBy(col("CaseNo").cast("int")).select("directions").collect()
            uploadHomeOfficeBundleAvailableList = df.orderBy(col("CaseNo").cast("int")).select("uploadHomeOfficeBundleAvailable").collect()

            assert directionsList[0][0] == []
            assert directionsList[1][0] == []
            assert uploadHomeOfficeBundleAvailableList[0][0] == "Yes"
            assert uploadHomeOfficeBundleAvailableList[1][0] == "Yes"
