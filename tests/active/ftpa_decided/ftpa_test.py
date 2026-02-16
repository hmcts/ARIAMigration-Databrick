# tests/active/ftpa_decided/ftpa_test.py

from Databricks.ACTIVE.APPEALS.shared_functions.ftpa_decided import ftpa

from pyspark.sql import SparkSession
import pytest
from pyspark.sql import types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("ftpaDecidedTests")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def ftpa_outputs(spark):
    # Keep DateReceived + DecisionDate as STRING in this unit test.
    # ftpa_decided safely parses DecisionDate from string/timestamp.
    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("HearingDuration", T.IntegerType(), True),
        T.StructField("HearingCentre", T.StringType(), True),
        T.StructField("DateReceived", T.StringType(), True),
        T.StructField("DecisionDate", T.StringType(), True),
        T.StructField("Adj_Title", T.StringType(), True),
        T.StructField("Adj_Forenames", T.StringType(), True),
        T.StructField("Adj_Surname", T.StringType(), True),
        T.StructField("Party", T.IntegerType(), True),
        T.StructField("OutOfTime", T.IntegerType(), True),
        T.StructField("Outcome", T.IntegerType(), True),
    ])

    # Note:
    # - CaseStatus is INT (39) as per mapping
    # - Latest row per case is chosen by StatusId desc (and DecisionDate desc_nulls_last if you added it)
    # - Decision/outcome fields use rows where Outcome IN (30,31,14)
    m3_data = [
        # CASE005 has 2 rows: StatusId=2 should win (Outcome=30 => granted)
        ("CASE005", 1, 39, 180, "LOC001", "2024-10-02T00:00:00.000+00:00", "2025-10-01T00:00:00.000+00:00", "Mr", "John", "Doe", 1, 0, 31),
        ("CASE005", 2, 39, 60,  "LOC002", "2025-11-02T00:00:00.000+00:00", "2025-11-02T00:00:00.000+00:00", "Ms", "Jane", "Doe", 1, 0, 30),

        ("CASE006", 1, 39, 240, "LOC003", "2026-12-03T00:00:00.000+00:00", "2026-12-03T00:00:00.000+00:00", "Mr", "John", "xyz", 1, 1, 31),
        ("CASE007", 1, 39, 360, "LOC004", "2026-08-03T00:00:00.000+00:00", "2026-08-03T00:00:00.000+00:00", "Mr", "abc",  "Doe", 2, 0, 14),
        ("CASE008", 1, 39, None, "LOC005", "2024-10-02T00:00:00.000+00:00", "2024-10-02T00:00:00.000+00:00", "Sir", "Guy",  "Random", 1, 0, 30),

        # CASE010 has some null judge name fields; DecisionDate present
        ("CASE010", 1, 39, None, "LOC007", None, "2025-01-15T00:00:00.000+00:00", None, None, None, 1, None, 30),

        ("CASE011", 1, 39, 45,  "LOC008", "2025-11-02T00:00:00.000+00:00", "2025-11-02T00:00:00.000+00:00", "Mr", "World", "Hello", 2, 1, 30),
    ]

    c_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("CategoryId", T.IntegerType(), True),
    ])

    c_data = [
        ("CASE005", 37),
        ("CASE006", 37),
        ("CASE007", 37),
        ("CASE008", 37),
        ("CASE010", 37),
        ("CASE011", 37),
    ]

    df_m3 = spark.createDataFrame(m3_data, m3_schema)
    df_c = spark.createDataFrame(c_data, c_schema)

    ftpa_content, _ = ftpa(df_m3, df_c)

    assert ftpa_content.count() > 0, (
        "ftpa_decided.ftpa() returned 0 rows in unit test input "
        "(either FSB.ftpa returned 0 AND fallback did not trigger, or filters removed all rows)."
    )

    results = {row["CaseNo"]: row.asDict() for row in ftpa_content.collect()}
    return results


def test_ftpaApplicantType(ftpa_outputs):
    r = ftpa_outputs
    assert r["CASE005"]["ftpaApplicantType"] == "appellant"
    assert r["CASE006"]["ftpaApplicantType"] == "appellant"
    assert r["CASE007"]["ftpaApplicantType"] == "respondent"
    assert r["CASE010"]["ftpaApplicantType"] == "appellant"
    assert r["CASE011"]["ftpaApplicantType"] == "respondent"


def test_ftpaFirstDecision_and_FinalDecisionForDisplay(ftpa_outputs):
    r = ftpa_outputs

    assert r["CASE005"]["ftpaFirstDecision"] == "granted"
    assert r["CASE005"]["ftpaFinalDecisionForDisplay"] == "Granted"

    assert r["CASE006"]["ftpaFirstDecision"] == "refused"
    assert r["CASE006"]["ftpaFinalDecisionForDisplay"] == "Refused"

    assert r["CASE007"]["ftpaFirstDecision"] == "notAdmitted"
    assert r["CASE007"]["ftpaFinalDecisionForDisplay"] == "Not admitted"


def test_decision_dates_by_party_iso8601(ftpa_outputs):
    # Dates are ISO 8601 date: yyyy-MM-dd
    r = ftpa_outputs

    assert r["CASE005"]["ftpaAppellantDecisionDate"] == "2025-11-02"
    assert r["CASE006"]["ftpaAppellantDecisionDate"] == "2026-12-03"
    assert r["CASE007"]["ftpaAppellantDecisionDate"] is None
    assert r["CASE010"]["ftpaAppellantDecisionDate"] == "2025-01-15"

    assert r["CASE005"]["ftpaRespondentDecisionDate"] is None
    assert r["CASE007"]["ftpaRespondentDecisionDate"] == "2026-08-03"
    assert r["CASE011"]["ftpaRespondentDecisionDate"] == "2025-11-02"


def test_rj_outcome_types_by_party(ftpa_outputs):
    r = ftpa_outputs

    assert r["CASE005"]["ftpaAppellantRjDecisionOutcomeType"] == "granted"
    assert r["CASE005"]["ftpaRespondentRjDecisionOutcomeType"] is None

    assert r["CASE007"]["ftpaAppellantRjDecisionOutcomeType"] is None
    assert r["CASE007"]["ftpaRespondentRjDecisionOutcomeType"] == "notAdmitted"


def test_notice_of_decision_set_aside_flags(ftpa_outputs):
    r = ftpa_outputs

    assert r["CASE005"]["isFtpaAppellantNoticeOfDecisionSetAside"] == "No"
    assert r["CASE005"]["isFtpaRespondentNoticeOfDecisionSetAside"] is None

    assert r["CASE007"]["isFtpaAppellantNoticeOfDecisionSetAside"] is None
    assert r["CASE007"]["isFtpaRespondentNoticeOfDecisionSetAside"] == "No"
