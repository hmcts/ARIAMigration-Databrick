import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import documents
import uuid

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("documentsTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def documents_outputs(spark):

    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
    ])

    m1_data = [
        ("EA/01001/2025", "LR", "EA"),
        ("HU/01004/2025", "LR", "HU"),
    ]

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)

    # Call the function under test
    documents_content, _ = documents(silver_m1)

    # Convert to dictionary keyed by CaseNo
    results = {row["CaseNo"]: row.asDict() for row in documents_content.collect()}
    return results

def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row.get(k) == v, f"{k} expected {v} but got {row.get(k)}"

def test_documents_static_fields(documents_outputs):
    """Check that documents() adds the correct empty array fields."""

    # Pick one case
    row = documents_outputs["EA/01001/2025"]

    # Assert the static fields exist and are empty arrays
    for field in [
        "uploadTheAppealFormDocs",
        "caseNotes",
        "tribunalDocuments",
        "legalRepresentativeDocuments"
    ]:
        assert field in row, f"{field} missing from row"
        assert row[field] == [], f"{field} expected empty list but got {row[field]}"

def test_documents_all_rows(documents_outputs):
    for case, row in documents_outputs.items():
        for field in [
            "uploadTheAppealFormDocs",
            "caseNotes",
            "tribunalDocuments",
            "legalRepresentativeDocuments"
        ]:
            assert field in row
            assert row[field] == []