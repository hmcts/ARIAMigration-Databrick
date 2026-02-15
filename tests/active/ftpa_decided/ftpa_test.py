from Databricks.ACTIVE.APPEALS.shared_functions.ftpa_decided import ftpa

from pyspark.sql import SparkSession
import pytest

from pyspark.sql import functions as F, types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("ftpaDecidedTests")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def ftpa_outputs(spark):

    # Schema must match what ftpa_decided.ftpa() expects
    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.StringType(), True),          # used for filter == 39
        T.StructField("HearingDuration", T.IntegerType(), True),
        T.StructField("HearingCentre", T.StringType(), True),
        T.StructField("DateReceived", T.TimestampType(), True),
        T.StructField("DecisionDate", T.TimestampType(), True),      # used in date_format()
        T.StructField("Adj_Title", T.StringType(), True),
        T.StructField("Adj_Forenames", T.StringType(), True),
        T.StructField("Adj_Surname", T.StringType(), True),
        T.StructField("Party", T.IntegerType(), True),
        T.StructField("OutOfTime", T.IntegerType(), True),
        T.StructField("Outcome", T.IntegerType(), True),
    ])

    # IMPORTANT: each row must have 13 values (same order as schema)
    m3_data = [
        # CASE005: two rows to test latest StatusId selection (latest = StatusId 2)
        ("CASE005", 1, "39", 180, "LOC001", datetime(2024, 10, 2), datetime(2025, 10, 1), "Mr", "John", "Doe", 1, 0, 31),
        ("CASE005", 2, "39", 60,  "LOC002", datetime(2025, 11, 2), datetime(2025, 11, 2), "Ms", "Jane", "Doe", 1, 0, 30),

        ("CASE006", 1, "39", 240, "LOC003", datetime(2026, 12, 3), datetime(2026, 12, 3), "Mr", "John", "xyz", 1, 1, 31),
        ("CASE007", 1, "39", 360, "LOC004", datetime(2026, 8, 3),  datetime(2026, 8, 3),  "Mr", "abc",  "Doe", 2, 0, 14),
        ("CASE008", 1, "39", None,"LOC005", datetime(2024, 10, 2), datetime(2024, 10, 2), "Sir","Guy",  "Random", 1, 0, 30),
        ("CASE010", 1, "39", None,"LOC007", None,                  datetime(2025, 1, 15),  None, None, None, 1, None, 30),
        ("CASE011", 1, "39", 45,  "LOC008", datetime(2025, 11, 2), datetime(2025, 11, 2), "Mr", "World","Hello", 2, 1, 30),
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

       # Guard to avoid KeyError and make failures obvious in CI
    assert ftpa_content.count() > 0, "ftpa_decided.ftpa() returned 0 rows in unit test input"

    # Useful debug if it fails again in pipeline:
    # print("ftpa_content rows:", ftpa_content.count())
    results = {row["CaseNo"]: row.asDict() for row in ftpa_content.collect()}

    return results


# ------------------------------------------------------------
# Tests
# ------------------------------------------------------------

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


def test_decision_dates_by_party(ftpa_outputs):
    r = ftpa_outputs
    assert r["CASE005"]["ftpaAppellantDecisionDate"] == "02/11/2025"
    assert r["CASE006"]["ftpaAppellantDecisionDate"] == "03/12/2026"
    assert r["CASE007"]["ftpaAppellantDecisionDate"] is None
    assert r["CASE010"]["ftpaAppellantDecisionDate"] == "15/01/2025"

    assert r["CASE005"]["ftpaRespondentDecisionDate"] is None
    assert r["CASE007"]["ftpaRespondentDecisionDate"] == "03/08/2026"
    assert r["CASE011"]["ftpaRespondentDecisionDate"] == "02/11/2025"


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
