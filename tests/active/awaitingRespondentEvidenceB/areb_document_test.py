from Databricks.ACTIVE.APPEALS.shared_functions.AwaitingEvidenceRespondant_b import documents

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from unittest.mock import patch
import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("awaitingRespondentEvidenceB_document") \
        .getOrCreate()


class TestAwaitingRespondentEvidenceBDocument:
    SILVER_M1_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("dv_representation", StringType()),
        StructField("lu_appealType", StringType())
    ])

    def payment_pending_document_df(self, spark):
        PP_COLUMNS = StructType([
            StructField("CaseNo", StringType())
        ])

        caseList = [
            ("1",),
            ("2",)
        ]

        return spark.createDataFrame(caseList, PP_COLUMNS), spark.createDataFrame(caseList, PP_COLUMNS)

    def test_document(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.AwaitingEvidenceRespondant_b.PP') as PP:
            PP.documents.return_value = self.payment_pending_document_df(spark)

            silver_m1 = spark.createDataFrame([
                ("1", "rep", "appeal"),
                ("2", "rep", "appeal")
            ], self.SILVER_M1_SCHEMA)

            df, df_audit = documents(silver_m1)

            resultsList = df.orderBy(col("CaseNo").cast("int")).select("respondentDocuments").collect()

            assert resultsList[0][0] == []
            assert resultsList[1][0] == []
