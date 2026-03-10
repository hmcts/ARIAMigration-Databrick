from Databricks.ACTIVE.APPEALS.shared_functions.remitted import remittal
from pyspark.sql import SparkSession
import pytest
from pyspark.sql import types as T

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("remittedTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def remitted_outputs(spark):

    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("HearingDuration", T.IntegerType(), True),
        T.StructField("HearingCentre", T.StringType(), True),
        T.StructField("DecisionDate", T.StringType(), True),
        T.StructField("DateReceived", T.StringType(), True),
        T.StructField("Adj_Title", T.StringType(), True),
        T.StructField("Adj_Forenames", T.StringType(), True),
        T.StructField("Adj_Surname", T.StringType(), True),
        T.StructField("Party", T.IntegerType(), True),
        T.StructField("OutOfTime", T.IntegerType(), True),
        T.StructField("Outcome", T.IntegerType(), True),
    ])

    m3_data = [
        ("HU/01897/2024", 1, 42, 60, "LOC001", "2025-09-01T00:00:00.000+00:00", "2025-09-12T00:00:00.000+00:00", "Mr", "John", "Doe", 2, 0, 86),
        ("PA/01921/2025", 1, 43, 45, "LOC002", "2024-09-01T00:00:00.000+00:00", None, "Ms", "Jane", "Doe", 1, 0, 86),
        ("PA/03789/2024", 1, 44, 30, "LOC003", "2025-10-02T00:00:00.000+00:00", None, "Mr", "Guy", "Random", 1, 0, 86),
        ("PA/03885/2024", 1, 42, None, "LOC004","2025-11-02T00:00:00.000+00:00", None, "Sir", "Alex", "Smith", 1, 0, 86),
    ]

    silver_m3 = spark.createDataFrame(m3_data, m3_schema)

    remittal_content, _ = remittal(silver_m3)

    results = {row["CaseNo"]: row.asDict() for row in remittal_content.collect()}
    return results

def test_rehearingReason(remitted_outputs):
    r = remitted_outputs
    assert r["HU/01897/2024"]["rehearingReason"] == "Remitted"
    assert r["PA/01921/2025"]["rehearingReason"] == "Remitted"
    assert r["PA/03789/2024"]["rehearingReason"] == "Remitted"
    assert r["PA/03885/2024"]["rehearingReason"] == "Remitted"


def test_sourceOfRemittal(remitted_outputs):
    r = remitted_outputs
    assert r["HU/01897/2024"]["sourceOfRemittal"] == "Upper Tribunal"
    assert r["PA/01921/2025"]["sourceOfRemittal"] == "Upper Tribunal"
    assert r["PA/03789/2024"]["sourceOfRemittal"] == "Upper Tribunal"
    assert r["PA/03885/2024"]["sourceOfRemittal"] == "Upper Tribunal"


def test_appealRemittedDate(remitted_outputs):
    r = remitted_outputs
    assert r["HU/01897/2024"]["appealRemittedDate"] == "2025-09-01"
    assert r["PA/01921/2025"]["appealRemittedDate"] == "2024-09-01"
    assert r["PA/03789/2024"]["appealRemittedDate"] == "2025-10-02"
    assert r["PA/03885/2024"]["appealRemittedDate"] == "2025-11-02"


def test_courtReferenceNumber(remitted_outputs):
    r = remitted_outputs
    assert r["HU/01897/2024"]["courtReferenceNumber"] == "This is a migrated ARIA case. Please refer to the documents."
    assert r["PA/01921/2025"]["courtReferenceNumber"] == "This is a migrated ARIA case. Please refer to the documents."
    assert r["PA/03789/2024"]["courtReferenceNumber"] == "This is a migrated ARIA case. Please refer to the documents."
    assert r["PA/03885/2024"]["courtReferenceNumber"] == "This is a migrated ARIA case. Please refer to the documents."

