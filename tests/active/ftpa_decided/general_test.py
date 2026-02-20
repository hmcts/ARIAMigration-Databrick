# tests/active/ftpa_decided/general_test.py

from Databricks.ACTIVE.APPEALS.shared_functions.ftpa_decided import general

from pyspark.sql import SparkSession
import pytest
from pyspark.sql import types as T

# PAWAN: required spark functions (avoid NameError if used in future edits/tests)
from pyspark.sql.functions import col, lit  # noqa: F401


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("FtpaDecidedGeneral_NewColumnsOnly_Tests")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def general_outputs(spark):
    # Simplified M3 schema for latest Party/Outcome
    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("Outcome", T.IntegerType(), True),
        T.StructField("Party", T.IntegerType(), True),
    ])

    m3_data = [
        ("CASE001", 10, 39, 31, 1),  # appellant decided
        ("CASE002", 20, 39, 30, 2),  # respondent decided
        ("CASE003", 30, 46, 31, 1),  # second Ftpa decision case
        ("CASE004", 40, 39, None, 1), # no Outcome yet
        ]
    
    silver_m3 = spark.createDataFrame(m3_data, m3_schema)

    empty_schema = T.StructType([T.StructField("CaseNo", T.StringType(), True)])
    silver_m1 = spark.createDataFrame([], empty_schema)
    silver_m2 = spark.createDataFrame([], empty_schema)
    silver_h = spark.createDataFrame([], empty_schema)
    bronze_hearing_centres = spark.createDataFrame([], empty_schema)
    bronze_derive_hearing_centres = spark.createDataFrame([], empty_schema)

    general_content, _ = general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres)

    results = {row["CaseNo"]: row.asDict() for row in general_content.collect()}
    return results


def test_party_visibility_flags(general_outputs):
    r = general_outputs

    # CASE001: appellant
    assert r["CASE001"]["isAppellantFtpaDecisionVisibleToAll"] == "Yes"
    assert r["CASE001"]["isRespondentFtpaDecisionVisibleToAll"] == "No"

    # CASE002: respondent
    assert r["CASE002"]["isAppellantFtpaDecisionVisibleToAll"] == "No"
    assert r["CASE002"]["isRespondentFtpaDecisionVisibleToAll"] == "Yes"

    # CASE004: appellant with no outcome
    assert r["CASE004"]["isAppellantFtpaDecisionVisibleToAll"] == "Yes"
    assert r["CASE004"]["isRespondentFtpaDecisionVisibleToAll"] == "No"


def test_constant_flags(general_outputs):
    r = general_outputs
    # Fields that should always be "Yes"
    for case in r.values():
        assert case["isDlrmSetAsideEnabled"] == "Yes"
        assert case["isFtpaAppellantDecided"] == "Yes"
        assert case["isFtpaRespondentDecided"] == "Yes"
        assert case["isReheardAppealEnabled"] == "Yes"


def test_second_ftpa_decision_flag(general_outputs):
    r = general_outputs
    # Only CASE003 has CaseStatus=46 & Outcome=31 -> Yes
    assert r["CASE001"]["secondFtpaDecisionExists"] == "No"
    assert r["CASE002"]["secondFtpaDecisionExists"] == "No"
    assert r["CASE003"]["secondFtpaDecisionExists"] == "Yes"
    assert r["CASE004"]["secondFtpaDecisionExists"] == "No"
    assert r["CASE005"]["secondFtpaDecisionExists"] == "No"