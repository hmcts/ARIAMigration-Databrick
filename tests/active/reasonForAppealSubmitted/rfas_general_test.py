from Databricks.ACTIVE.APPEALS.shared_functions.reasonsForAppealSubmitted import generalDefault

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from unittest.mock import patch
import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("reasonsForAppealSubmitted_general") \
        .getOrCreate()


class TestReasonForAppealSubmittedGeneral:
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
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.reasonsForAppealSubmitted.AERb') as AERb:
            AERb.generalDefault.return_value = self.aerb_general_default_df(spark)

            silver_m1 = spark.createDataFrame([
                ("1", "AIP", "appeal"),
                ("2", "AIP", "appeal"),
                ("3", "LR", "appeal")
            ], self.SILVER_M1_SCHEMA)

            df = generalDefault(silver_m1)

            results = {row["CaseNo"]: row.asDict() for row in df.collect()}

            assert results["1"]["reasonsForAppealDecision"] == "This is a migrated ARIA case. Please see the documents provided as part of the notice of appeal."
            assert results["2"]["reasonsForAppealDecision"] == "This is a migrated ARIA case. Please see the documents provided as part of the notice of appeal."
            assert results["1"]["changeDirectionDueDateActionAvailable"] == "Yes"
            assert results["2"]["changeDirectionDueDateActionAvailable"] == "Yes"
            assert results["1"]["markEvidenceAsReviewedActionAvailable"] == "Yes"
            assert results["2"]["markEvidenceAsReviewedActionAvailable"] == "Yes"
            assert results["1"]["uploadAdditionalEvidenceActionAvailable"] == "Yes"
            assert results["2"]["uploadAdditionalEvidenceActionAvailable"] == "Yes"
            assert results["1"]["uploadAdditionalEvidenceHomeOfficeActionAvailable"] == "Yes"
            assert results["2"]["uploadAdditionalEvidenceHomeOfficeActionAvailable"] == "Yes"
            assert results["1"]["uploadHomeOfficeBundleAvailable"] == "Yes"
            assert results["2"]["uploadHomeOfficeBundleAvailable"] == "Yes"

            assert len(results) == 2
