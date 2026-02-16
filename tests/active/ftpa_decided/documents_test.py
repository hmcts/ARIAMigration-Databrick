from Databricks.ACTIVE.APPEALS.shared_functions.ftpa_decided import documents

from pyspark.sql import SparkSession
import pytest
from pyspark.sql import types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("FtpaDecidedDocumentsTests")
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
        T.StructField("CaseStatus", T.IntegerType(), True),  # ✅ added (safe)
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

def test_allFtpaAppellantDecisionDocs_is_array_and_not_null(documents_outputs):
    r = documents_outputs

    assert r["CASE005"]["allFtpaAppellantDecisionDocs"] is not None
    assert r["CASE006"]["allFtpaAppellantDecisionDocs"] is not None
    assert r["CASE007"]["allFtpaAppellantDecisionDocs"] is not None
    assert r["CASE010"]["allFtpaAppellantDecisionDocs"] is not None
    assert r["CASE011"]["allFtpaAppellantDecisionDocs"] is not None

    assert r["CASE005"]["allFtpaAppellantDecisionDocs"] == []
    assert r["CASE006"]["allFtpaAppellantDecisionDocs"] == []
    assert r["CASE007"]["allFtpaAppellantDecisionDocs"] == []
    assert r["CASE010"]["allFtpaAppellantDecisionDocs"] == []
    assert r["CASE011"]["allFtpaAppellantDecisionDocs"] == []


def test_allFtpaRespondentDecisionDocs_is_array_and_not_null(documents_outputs):
    r = documents_outputs

    assert r["CASE005"]["allFtpaRespondentDecisionDocs"] is not None
    assert r["CASE006"]["allFtpaRespondentDecisionDocs"] is not None
    assert r["CASE007"]["allFtpaRespondentDecisionDocs"] is not None
    assert r["CASE010"]["allFtpaRespondentDecisionDocs"] is not None
    assert r["CASE011"]["allFtpaRespondentDecisionDocs"] is not None

    assert r["CASE005"]["allFtpaRespondentDecisionDocs"] == []
    assert r["CASE006"]["allFtpaRespondentDecisionDocs"] == []
    assert r["CASE007"]["allFtpaRespondentDecisionDocs"] == []
    assert r["CASE010"]["allFtpaRespondentDecisionDocs"] == []
    assert r["CASE011"]["allFtpaRespondentDecisionDocs"] == []


def test_ftpaAppellantNoticeDocument_is_array_and_not_null(documents_outputs):
    r = documents_outputs

    assert r["CASE005"]["ftpaAppellantNoticeDocument"] is not None
    assert r["CASE006"]["ftpaAppellantNoticeDocument"] is not None
    assert r["CASE007"]["ftpaAppellantNoticeDocument"] is not None
    assert r["CASE010"]["ftpaAppellantNoticeDocument"] is not None
    assert r["CASE011"]["ftpaAppellantNoticeDocument"] is not None

    assert r["CASE005"]["ftpaAppellantNoticeDocument"] == []
    assert r["CASE006"]["ftpaAppellantNoticeDocument"] == []
    assert r["CASE007"]["ftpaAppellantNoticeDocument"] == []
    assert r["CASE010"]["ftpaAppellantNoticeDocument"] == []
    assert r["CASE011"]["ftpaAppellantNoticeDocument"] == []


def test_ftpaRespondentNoticeDocument_is_array_and_not_null(documents_outputs):
    r = documents_outputs

    assert r["CASE005"]["ftpaRespondentNoticeDocument"] is not None
    assert r["CASE006"]["ftpaRespondentNoticeDocument"] is not None
    assert r["CASE007"]["ftpaRespondentNoticeDocument"] is not None
    assert r["CASE010"]["ftpaRespondentNoticeDocument"] is not None
    assert r["CASE011"]["ftpaRespondentNoticeDocument"] is not None

    assert r["CASE005"]["ftpaRespondentNoticeDocument"] == []
    assert r["CASE006"]["ftpaRespondentNoticeDocument"] == []
    assert r["CASE007"]["ftpaRespondentNoticeDocument"] == []
    assert r["CASE010"]["ftpaRespondentNoticeDocument"] == []
    assert r["CASE011"]["ftpaRespondentNoticeDocument"] == []
