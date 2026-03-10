from Databricks.ACTIVE.APPEALS.shared_functions.remitted import documents

from pyspark.sql import SparkSession
import pytest
from pyspark.sql import types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("remittedDocumentsTests")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def documents_outputs(spark):

    # ---- Minimal M1 schema ----
    # Add CaseStatus as nullable to avoid filters inside FSA.documents dropping rows
    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),  
    ])

    m1_data = [
        ("CASE005", "AIP", "FT", 39),
        ("CASE006", "AIP", "FT", 39),
        ("CASE007", "AIP", "FT", 39),
        ("CASE010", "AIP", "FT", 39),
        ("CASE011", "AIP", "FT", 39),
    ]

    # ---- Minimal M3 schema used by FSA.documents ----
    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("Party", T.IntegerType(), True),
    ])

    m3_data = [
        ("CASE005", 1, 39, 1),
        ("CASE006", 1, 39, 1),
        ("CASE007", 1, 39, 2),
        ("CASE010", 1, 39, 1),
        ("CASE011", 1, 39, 2),
    ]

    df_m1 = spark.createDataFrame(m1_data, m1_schema)
    df_m3 = spark.createDataFrame(m3_data, m3_schema)

    documents_content, _ = documents(df_m1, df_m3)

    # ✅ Guard so failures are obvious (instead of KeyError later)
    assert documents_content.count() > 0, "ftpa_decided.documents() returned 0 rows for unit test input"

    results = {row["CaseNo"]: row.asDict() for row in documents_content.collect()}
    return results


# ------------------------------------------------------------
# ftpa_decided-specific document tests (ONLY 4 new columns)
# ------------------------------------------------------------

def test_remittalDocuments(documents_outputs):
    r = documents_outputs

    assert r["CASE005"]["remittalDocuments"] == []
    assert r["CASE006"]["remittalDocuments"] == []
    assert r["CASE007"]["remittalDocuments"] == []
    assert r["CASE010"]["remittalDocuments"] == []
    assert r["CASE011"]["remittalDocuments"] == []


def test_uploadOtherRemittalDocs(documents_outputs):
    r = documents_outputs

    assert r["CASE005"]["uploadOtherRemittalDocs"] == []
    assert r["CASE006"]["uploadOtherRemittalDocs"] == []
    assert r["CASE007"]["uploadOtherRemittalDocs"] == []
    assert r["CASE010"]["uploadOtherRemittalDocs"] == []
    assert r["CASE011"]["uploadOtherRemittalDocs"] == []

