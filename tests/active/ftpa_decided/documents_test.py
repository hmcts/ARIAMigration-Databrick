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

    # ---- Minimal M1 schema (not used by ftpa_decided.documents mapping directly,
    # but required by FSA.documents signature) ----
    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
    ])

    m1_data = [
        ("CASE005", "AIP", "FT"),
        ("CASE006", "AIP", "FT"),
        ("CASE007", "AIP", "FT"),
        ("CASE010", "AIP", "FT"),
        ("CASE011", "AIP", "FT"),
    ]

    # ---- M3 schema MUST include Party/StatusId/CaseStatus to allow FSA.documents
    # to build ftpaAppellantDocuments / ftpaRespondentDocuments etc. ----
    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("Party", T.IntegerType(), True),
    ])

    # Keep CaseStatus=39 and include both Party=1 and Party=2 cases
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
    results = {row["CaseNo"]: row.asDict() for row in documents_content.collect()}
    return results


# ------------------------------------------------------------
# ftpa_decided-specific document tests (ONLY 4 new columns)
# ------------------------------------------------------------

def test_allFtpaAppellantDecisionDocs_is_array_and_not_null(documents_outputs):
    r = documents_outputs

    # should exist and never be null due to coalesce(..., empty array)
    assert r["CASE005"]["allFtpaAppellantDecisionDocs"] is not None
    assert r["CASE006"]["allFtpaAppellantDecisionDocs"] is not None
    assert r["CASE007"]["allFtpaAppellantDecisionDocs"] is not None
    assert r["CASE010"]["allFtpaAppellantDecisionDocs"] is not None
    assert r["CASE011"]["allFtpaAppellantDecisionDocs"] is not None

    # in unit test data we did not attach any docs, so expect empty arrays
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
