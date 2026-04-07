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
        T.StructField("DetentionCentreId", T.IntegerType(), True),
    ])

    m2_data = [
        ("CASE005", "Green", "DD7 7PT", None,2),
        ("CASE006", "Blue", "DD7 7PT", None,5),
        ("CASE007", "Red", "DD7 7PT", None,8),
        ("CASE011", "Black", "DD7 7PT", None,9),
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
        T.StructField("Outcome", T.IntegerType(), True),
    ])

    m3_data = [
        ("CASE005", 2, 39, 60,  "LOC002", "2025-11-02T00:00:00.000+00:00", "1899-12-30T12:00:00.000+00:00", "Ms", "Doe",   "Jane",  1, 0, 39),
        ("CASE006", 1, 39, 240, "LOC003", "2026-12-03T00:00:00.000+00:00", "1899-12-30T13:00:00.000+00:00", "Mr", "xyz",   "John",  1, 1, 46),
        ("CASE007", 1, 39, 360, "LOC004", "2026-08-03T00:00:00.000+00:00", "2000-12-30T07:10:58.000+00:00", "Mr", "Doe",   "abc",   2, 0, 0),
        ("CASE011", 1, 39, 45,  "LOC008", "2025-11-02T00:00:00.000+00:00", "1899-12-30T12:00:00.999+00:00", "Mr", "Hello", "World", 2, 1, 31),
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
    dcs_schema = T.StructType([
        T.StructField("Detained", T.IntegerType(), True),
        T.StructField("DetentionCentreId", T.IntegerType(), True),
        T.StructField("DetentionCentre", T.StringType(), True),
        T.StructField("prisonName", T.StringType(), True),
        T.StructField("ircName", T.StringType(), True),
        T.StructField("detentionBuilding", T.StringType(), True),
        T.StructField("detentionAddressLines", T.StringType(), True),
        T.StructField("detentionPostcode", T.StringType(), True),
        T.StructField("hearingCentre", T.StringType(), True),
        T.StructField("staffLocation", T.StringType(), True),

        # caseManagementLocation as STRUCT
        T.StructField(
            "caseManagementLocation",
            T.StructType([
                T.StructField("region", T.StringType(), True),
                T.StructField("baseLocation", T.StringType(), True),
            ]),
            True
        ),

        T.StructField("locationCode", T.StringType(), True),
        T.StructField("locationLabel", T.StringType(), True),
        T.StructField("selectedHearingCentreRefData", T.StringType(), True),
        T.StructField("applicationChangeDesignatedHearingCentre", T.StringType(), True),
    ])

    dcs_data = [
    (
        1, 2, "Rochester", "Rochester", None,
        "HMP/YOI Rochester",
        "1 Fort Road, Rochester, Kent",
        "ME1 3QS",
        "taylorHouse",
        "Taylor House",
        {"region": "1", "baseLocation": "765324"},
        "765324",
        "Taylor House Tribunal Hearing Centre",
        "Taylor House Tribunal Hearing Centre",
        "taylorHouse"
    ),
    (
        1, 5, "Wormwood Scrubs", "Wormwood Scrubs", None,
        "HMP Wormwood Scrubs",
        "PO Box 757, Du Cane Road, London",
        "W12 0AE",
        "hattonCross",
        "Hatton Cross",
        {"region": "1", "baseLocation": "386417"},
        "386417",
        "Hatton Cross Tribunal Hearing Centre",
        "Hatton Cross Tribunal Hearing Centre",
        "hattonCross"
    ),
    (
        1, 8, "Belmarsh", "Belmarsh", None,
        "HMP Belmarsh",
        "Western Way, Thamesmead, London",
        "SE28 0EB",
        "taylorHouse",
        "Taylor House",
        {"region": "1", "baseLocation": "765324"},
        "765324",
        "Taylor House Tribunal Hearing Centre",
        "Taylor House Tribunal Hearing Centre",
        "taylorHouse"
    ),
    (
        1, 9, "Birmingham", "Birmingham", None,
        "HMP Birmingham",
        "Winson Green Road, Birmingham",
        "B18 4AS",
        "birmingham",
        "Birmingham",
        {"region": "1", "baseLocation": "231596"},
        "231596",
        "Birmingham Civil And Family Justice Centre",
        "Birmingham Civil And Family Justice Centre",
        "birmingham"
    ),
    (
        1, 10, "High Down", "High Down", None,
        "HMP/YOI High Down",
        "High Down Lane, Sutton, Surrey",
        "SM2 5PJ",
        "hattonCross",
        "Hatton Cross",
        {"region": "1", "baseLocation": "386417"},
        "386417",
        "Hatton Cross Tribunal Hearing Centre",
        "Hatton Cross Tribunal Hearing Centre",
        "hattonCross"
    ),
    (
        1, 13, "Pentonville", "Pentonville", None,
        "HMP/YOI Pentonville",
        "Caledonian Road, London",
        "N7 8TT",
        "taylorHouse",
        "Taylor House",
        {"region": "1", "baseLocation": "765324"},
        "765324",
        "Taylor House Tribunal Hearing Centre",
        "Taylor House Tribunal Hearing Centre",
        "taylorHouse"
    ),
    (
        1, 14, "Wandsworth", "Wandsworth", None,
        "HMP Wandsworth",
        "PO Box 757, Heathfield Road, Wandsworth, London",
        "SW18 3HS",
        "hattonCross",
        "Hatton Cross",
        {"region": "1", "baseLocation": "386417"},
        "386417",
        "Hatton Cross Tribunal Hearing Centre",
        "Hatton Cross Tribunal Hearing Centre",
        "hattonCross"
    ),
    (
        1, 18, "Elmley", "Elmley", None,
        "HMP/YOI Elmley",
        "Church Road, Eastchurch, Sheerness, Kent",
        "ME12 4DZ",
        "taylorHouse",
        "Taylor House",
        {"region": "1", "baseLocation": "765324"},
        "765324",
        "Taylor House Tribunal Hearing Centre",
        "Taylor House Tribunal Hearing Centre",
        "taylorHouse"
    ),
    (
        1, 23, "Magilligan", "Magilligan", None,
        "HMP Magilligan",
        "Point Road, Londonderry, Limavady",
        "BT49 0LR",
        "glasgow",
        "Glasgow",
        {"region": "1", "baseLocation": "366559"},
        "366559",
        "Atlantic Quay - Glasgow",
        "Atlantic Quay - Glasgow",
        "glasgow"
    ),
    ]

    # Create DFs
    df_m1 = spark.createDataFrame(m1_data, m1_schema)
    df_m2 = spark.createDataFrame(m2_data, m2_schema)
    df_m3 = spark.createDataFrame(m3_data, m3_schema)
    df_mh = spark.createDataFrame(mh_data, mh_schema)
    df_bhc = spark.createDataFrame(bhc_data, bhc_schema)
    df_bdhc = spark.createDataFrame(bdhc_data, bdhc_schema)
    df_dcs = spark.createDataFrame(dcs_data, dcs_schema)

    general_content, _ = general(df_m1, df_m2, df_m3, df_mh, df_bhc, df_bdhc,df_dcs)

    # Guard for CI clarity
    assert general_content.count() > 0, "ftpa_decided.general() returned 0 rows for unit test input"

    return {row["CaseNo"]: row.asDict() for row in general_content.collect()}

def test_party_visibility_flags(general_outputs):
    r = general_outputs

    # CASE005: Party=1 → appellant
    assert r["CASE005"]["isAppellantFtpaDecisionVisibleToAll"] == "Yes"
    assert r["CASE005"]["isRespondentFtpaDecisionVisibleToAll"] is None

    # CASE007: Party=2 → respondent
    assert r["CASE007"]["isAppellantFtpaDecisionVisibleToAll"] is None
    assert r["CASE007"]["isRespondentFtpaDecisionVisibleToAll"] == "Yes"


def test_constant_flags(general_outputs):
    r = general_outputs
    for case in r.values():
        assert case["isDlrmSetAsideEnabled"] == "Yes"
        assert case["isFtpaAppellantDecided"] == "Yes"
        assert case["isFtpaRespondentDecided"] == "Yes"
        assert case["isReheardAppealEnabled"] == "Yes"


def test_second_ftpa_decision_flag(general_outputs):
    r = general_outputs
    # All current cases are "No"
    for case_no in ["CASE005", "CASE006", "CASE007", "CASE011"]:
        assert r[case_no]["secondFtpaDecisionExists"] == "No"
