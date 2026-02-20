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

    # ----------------------------
    # M1
    # ----------------------------
    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
        T.StructField("Sponsor_Name", T.StringType(), True),
        T.StructField("Interpreter", T.StringType(), True),
        T.StructField("CourtPreference", T.StringType(), True),
        T.StructField("InCamera", T.BooleanType(), True),
        T.StructField("VisitVisaType", T.IntegerType(), True),
        T.StructField("CentreId", T.IntegerType(), True),
        T.StructField("Rep_Postcode", T.StringType(), True),
        T.StructField("CaseRep_Postcode", T.StringType(), True),
        T.StructField("PaymentRemissionRequested", T.IntegerType(), True),
        T.StructField("lu_applicationChangeDesignatedHearingCentre", T.StringType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True), 
    ])

    m1_data = [
        ("CASE005", "AIP", "FT", None, 0, 0, True, 3, 5, "B12 0hf", "B12 0hf", 2, "Man", 39),
        ("CASE006", "AIP", "FT", None, 0, 0, True, 4, 6, "B12 0hf", "B12 0hf", 4, "Man", 39),
        ("CASE007", "AIP", "FT", None, 0, 0, False, None, 7, "B12 0hf", "B12 0hf", 0, "Man", 39),
        ("CASE011", "AIP", "FT", None, 0, 0, True, 61, 0, "B12 0hf", "B12 0hf", 1, "Man", 39),
    ]

    # ----------------------------
    # M2
    # ----------------------------
    m2_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("Appellant_Name", T.StringType(), True),
        T.StructField("Appellant_Postcode", T.StringType(), True),
        T.StructField("Relationship", T.StringType(), True),
    ])

    m2_data = [
        ("CASE005", "Green", "DD7 7PT", None),
        ("CASE006", "Blue", "DD7 7PT", None),
        ("CASE007", "Red", "DD7 7PT", None),
        ("CASE011", "Black", "DD7 7PT", None),
    ]

    # ----------------------------
    # M3
    # (general() picks latest Party by StatusId desc)
    # ----------------------------
    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("HearingDuration", T.IntegerType(), True),
        T.StructField("HearingCentre", T.StringType(), True),
        T.StructField("DateReceived", T.StringType(), True),
        T.StructField("StartTime", T.StringType(), True),
        T.StructField("Adj_Title", T.StringType(), True),
        T.StructField("Adj_Forenames", T.StringType(), True),
        T.StructField("Adj_Surname", T.StringType(), True),
        T.StructField("Party", T.IntegerType(), True),
        T.StructField("OutOfTime", T.IntegerType(), True),
    ])

    m3_data = [
        ("CASE005", 2, 39, 60,  "LOC002", "2025-11-02T00:00:00.000+00:00", "1899-12-30T12:00:00.000+00:00", "Ms", "Doe",   "Jane",  1, 0),
        ("CASE006", 1, 39, 240, "LOC003", "2026-12-03T00:00:00.000+00:00", "1899-12-30T13:00:00.000+00:00", "Mr", "xyz",   "John",  1, 1),
        ("CASE007", 1, 39, 360, "LOC004", "2026-08-03T00:00:00.000+00:00", "2000-12-30T07:10:58.000+00:00", "Mr", "Doe",   "abc",   2, 0),
        ("CASE011", 1, 39, 45,  "LOC008", "2025-11-02T00:00:00.000+00:00", "1899-12-30T12:00:00.999+00:00", "Mr", "Hello", "World", 2, 1),
    ]

    # ----------------------------
    # bronze_hearing_centres (bhc)
    # ----------------------------
    bhc_schema = T.StructType([
        T.StructField("CentreId", T.IntegerType(), True),
        T.StructField("locationCode", T.StringType(), True),
        T.StructField("locationLabel", T.StringType(), True),
        T.StructField("applicationChangeDesignatedHearingCentre", T.StringType(), True),
        T.StructField("prevFileLocation", T.StringType(), True),
        T.StructField("hearingCentre", T.StringType(), True),
        T.StructField("Conditions", T.StringType(), True),
    ])

    bhc_data = [
        (5, "123", "Court1", "Bham", "123 xyz", "Man",  "1st cond"),
        (6, "456", "Court2", "Man",  "123 abc", "Bham", "2nd cond"),
        (7, "789", "Court3", "Scot", "456 asd", "Cov",  "3rd cond"),
        (0, None,  "Court4", "Cov",  "7676 jgfd", "Scot", "4th cond"),
    ]

    # ----------------------------
    # silver_history_detail (mh)
    # ----------------------------
    mh_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("HistType", T.IntegerType(), True),
        T.StructField("Comment", T.StringType(), True),
        T.StructField("HistoryId", T.IntegerType(), True),
    ])

    mh_data = [
        ("CASE005", 15, "Comment5", 5),
        ("CASE006", 15, "Comment6", 6),
        ("CASE007", 15, "Comment7", 7),
        ("CASE011", 15, "Comment11", 11),
    ]

    # ----------------------------
    # bronze_derive_hearing_centres 
    # ----------------------------
    bdhc_schema = T.StructType([
        T.StructField("selectedHearingCentreRefData", T.StringType(), True),
        T.StructField("locationCode", T.StringType(), True),
        T.StructField("locationLabel", T.StringType(), True),
        T.StructField("applicationChangeDesignatedHearingCentre", T.StringType(), True),
        T.StructField("hearingCentre", T.StringType(), True),
    ])

    bdhc_data = [
        ("Birmingham Centre", "456", "Court2", "Man", "Bham"),
        ("London Centre", "xyz", "Court6", "ply", "Nor"),
    ]

    # Create DFs
    df_m1 = spark.createDataFrame(m1_data, m1_schema)
    df_m2 = spark.createDataFrame(m2_data, m2_schema)
    df_m3 = spark.createDataFrame(m3_data, m3_schema)
    df_mh = spark.createDataFrame(mh_data, mh_schema)
    df_bhc = spark.createDataFrame(bhc_data, bhc_schema)
    df_bdhc = spark.createDataFrame(bdhc_data, bdhc_schema)

    general_content, _ = general(df_m1, df_m2, df_m3, df_mh, df_bhc, df_bdhc)

    # Guard for CI clarity
    assert general_content.count() > 0, "ftpa_decided.general() returned 0 rows for unit test input"

    return {row["CaseNo"]: row.asDict() for row in general_content.collect()}


# =========================================================
# ONLY NEW 7 columns - expects "Yes"/"No"
# =========================================================

def test_isAppellantFtpaDecisionVisibleToAll(general_outputs):
    r = general_outputs
    assert r["CASE005"]["isAppellantFtpaDecisionVisibleToAll"] == "Yes"
    assert r["CASE006"]["isAppellantFtpaDecisionVisibleToAll"] == "Yes"
    assert r["CASE007"]["isAppellantFtpaDecisionVisibleToAll"] == "No"
    assert r["CASE011"]["isAppellantFtpaDecisionVisibleToAll"] == "No"


def test_isRespondentFtpaDecisionVisibleToAll(general_outputs):
    r = general_outputs
    assert r["CASE005"]["isRespondentFtpaDecisionVisibleToAll"] == "No"
    assert r["CASE006"]["isRespondentFtpaDecisionVisibleToAll"] == "No"
    assert r["CASE007"]["isRespondentFtpaDecisionVisibleToAll"] == "Yes"
    assert r["CASE011"]["isRespondentFtpaDecisionVisibleToAll"] == "Yes"


def test_isDlrnSetAsideEnabled(general_outputs):
    r = general_outputs
    assert r["CASE005"]["isDlrnSetAsideEnabled"] == "Yes"
    assert r["CASE006"]["isDlrnSetAsideEnabled"] == "Yes"
    assert r["CASE007"]["isDlrnSetAsideEnabled"] == "Yes"
    assert r["CASE011"]["isDlrnSetAsideEnabled"] == "Yes"


def test_isFtpaAppellantDecided(general_outputs):
    r = general_outputs
    assert r["CASE005"]["isFtpaAppellantDecided"] == "Yes"
    assert r["CASE006"]["isFtpaAppellantDecided"] == "Yes"
    assert r["CASE007"]["isFtpaAppellantDecided"] == "No"
    assert r["CASE011"]["isFtpaAppellantDecided"] == "No"


def test_isFtpaRespondentDecided(general_outputs):
    r = general_outputs
    assert r["CASE005"]["isFtpaRespondentDecided"] == "No"
    assert r["CASE006"]["isFtpaRespondentDecided"] == "No"
    assert r["CASE007"]["isFtpaRespondentDecided"] == "Yes"
    assert r["CASE011"]["isFtpaRespondentDecided"] == "Yes"


def test_isReheardAppealEnabled(general_outputs):
    r = general_outputs
    assert r["CASE005"]["isReheardAppealEnabled"] == "Yes"
    assert r["CASE006"]["isReheardAppealEnabled"] == "Yes"
    assert r["CASE007"]["isReheardAppealEnabled"] == "Yes"
    assert r["CASE011"]["isReheardAppealEnabled"] == "Yes"


def test_secondFtpaDecisionExists(general_outputs):
    r = general_outputs
    assert r["CASE005"]["secondFtpaDecisionExists"] == "No"
    assert r["CASE006"]["secondFtpaDecisionExists"] == "No"
    assert r["CASE007"]["secondFtpaDecisionExists"] == "No"
    assert r["CASE011"]["secondFtpaDecisionExists"] == "No"
