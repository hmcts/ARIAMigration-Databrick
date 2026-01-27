import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import general
import uuid

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("generalTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def general_outputs(spark):

    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
        T.StructField("CentreId", T.IntegerType(), True),
        T.StructField("Rep_Postcode", T.StringType(), True),
        T.StructField("CaseRep_Postcode", T.StringType(), True),
        T.StructField("PaymentRemissionRequested", T.StringType(), True),
        T.StructField("lu_applicationChangeDesignatedHearingCentre", T.StringType(), True),
    ])

    m1_data = [
        ("EA/02375/2024", "AIP", "euSettlementScheme", 37, None, None, "0", None),
        ("HU/00496/2025", "AIP", "refusalOfHumanRights", 2, None, None, "0", None),
        ("HU/00510/2025", "AIP", "refusalOfHumanRights", 2, None, None, "0", None),
        ("EA/01319/2023", "LR", "euSettlementScheme", 1, "W6F 0ZD", None, "0", None),
        ("EA/01698/2024", "LR", "euSettlementScheme", 2, "TR52 9HX", None, "0", None),
        ("HU/00560/2025", "LR", "refusalOfHumanRights", 78, "G3 2PS", None, "0", None),
    ]

    m2_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("Relationship", T.StringType(), True),
        T.StructField("Appellant_Postcode", T.StringType(), True),
    ])

    m2_data = [
        ("EA/02375/2024", None, "LD2R 5HB"),
        ("HU/00496/2025", None, None),
        ("HU/00510/2025", None, None),
        ("EA/01319/2023", None, "EN4 2YU"),
        ("EA/01698/2024", None, "B37 5LW"),
        ("HU/00560/2025", None, None),
    ]

    m3_schema = T.StructType([])
    m3_data = []


    m3_data = []

    silver_h_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("HistoryId", T.IntegerType(), True),
        T.StructField("HistType", T.IntegerType(), True),
        T.StructField("Comment", T.StringType(), True),
        T.StructField("dv_targetState", T.StringType(), True),
    ])

    silver_h_data = [
        ("HU/00001/2025", 118053722, 49, "XXXXXXXXXXXXXXXX", "paymentPending"),
        ("HU/00002/2023", 117439400, 10, "XXXXXXXXXXXXXXXXXXXXX", "ftpaSubmitted(b)"),
        ("HU/00002/2023", 117439492, 18, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "ftpaSubmitted(b)"),
    ]

    bronze_hearing_centres_schema = T.StructType([
        T.StructField("CentreId", T.IntegerType(), True),
        T.StructField("prevFileLocation", T.StringType(), True),
        T.StructField("Conditions", T.StringType(), True),
        T.StructField("hearingCentre", T.StringType(), True),
        T.StructField("staffLocation", T.StringType(), True),
        T.StructField("locationCode", T.StringType(), True),
        T.StructField("locationLabel", T.StringType(), True),
        T.StructField("selectedHearingCentreRefData", T.StringType(), True),
        T.StructField("listCaseHearingCentre", T.StringType(), True),
        T.StructField("applicationChangeDesignatedHearingCentre", T.StringType(), True),
        T.StructField("caseManagementLocation",
                T.StructType([
                    T.StructField("region", T.StringType(), True),
                    T.StructField("baseLocation", T.StringType(), True)
                    ]), 
                True
        ),
    ])


    bronze_hearing_centres_data = [
        (
            540,"Alloa Sheriff Court",None,"glasgow","Glasgow","366559","Atlantic Quay - Glasgow","Atlantic Quay - Glasgow","alloaSherrif","glasgow",{"region":"1","baseLocation":"366559"},
        ),
        (
            7, "Belfast", None, "glasgow", "Glasgow", "366559", "Atlantic Quay - Glasgow", "Atlantic Quay - Glasgow", "belfast", "glasgow", {"region":"1","baseLocation":"366559"},
        ),
        (
            421,"Belfast - Laganside", None, "glasgow", "Glasgow", "366559", "Atlantic Quay - Glasgow", "Atlantic Quay - Glasgow", "belfast", "glasgow", {"region":"1","baseLocation":"366559"},
        ),
        (
            520, "Birmingham IAC (Priory Courts)", None, "birmingham", "Birmingham", "231596", "Birmingham Civil And Family Justice Centre", "Birmingham Civil And Family Justice Centre", "birmingham", "birmingham", {"region":"1","baseLocation":"231596"},
        ),
        (
            444, "Birmingham Magistrates Court (VLC)", None, "birmingham", "Birmingham", "231596", "Birmingham Civil And Family Justice Centre", "Birmingham Civil And Family Justice Centre", "birmingham", "birmingham", {"region":"1","baseLocation":"231596"},
        ),
        (
            86, "Bradford", None, "bradford", "Bradford","698118","Bradford Tribunal Hearing Centre", "Bradford Tribunal Hearing Centre", "bradford", "bradford", {"region":"1","baseLocation":"698118"},
        ),
    ]

    bronze_derive_hearing_centres_schema = T.StructType([
        T.StructField("hearingCentre", T.StringType(), True),
        T.StructField("staffLocation", T.StringType(), True),
        T.StructField(
            "caseManagementLocation",
            T.StructType([
                T.StructField("region", T.StringType(), True),
                T.StructField("baseLocation", T.StringType(), True)
            ]),
            True
        ),
        T.StructField("locationCode", T.IntegerType(), True),
        T.StructField("locationLabel", T.StringType(), True),
        T.StructField("selectedHearingCentreRefData", T.StringType(), True),
        T.StructField("applicationChangeDesignatedHearingCentre", T.StringType(), True),
    ])

    bronze_derive_hearing_centres_data = [
        ("bradford", "Bradford", {"region":"1","baseLocation":"698118"}, 698118, "Bradford Tribunal Hearing Centre", "Bradford Tribunal Hearing Centre", "bradford"),
        ("manchester", "Manchester", {"region":"1","baseLocation":"512401"}, 512401, "Manchester Tribunal Hearing Centre - Piccadilly Exchange", "Manchester Tribunal Hearing Centre - Piccadilly Exchange", "manchester"),
        ("newport", "Newport", {"region":"1","baseLocation":"227101"}, 227101, "Newport Tribunal Centre - Columbus House", "Newport Tribunal Centre - Columbus House", "newport"),
        ("taylorHouse", "Taylor House", {"region":"1","baseLocation":"765324"}, 765324, "Taylor House Tribunal Hearing Centre", "Taylor House Tribunal Hearing Centre", "taylorHouse"),
        ("newcastle", "Newcastle", {"region":"1","baseLocation":"366796"}, 366796, "Newcastle Civil And Family Courts And Tribunals Centre", "Newcastle Civil And Family Courts And Tribunals Centre", "newcastle"),
        ("birmingham", "Birmingham", {"region":"1","baseLocation":"231596"}, 231596, "Birmingham Civil And Family Justice Centre", "Birmingham Civil And Family Justice Centre", "birmingham"),
        ("hattonCross", "Hatton Cross", {"region":"1","baseLocation":"386417"}, 386417, "Hatton Cross Tribunal Hearing Centre", "Hatton Cross Tribunal Hearing Centre", "hattonCross"),
        ("glasgow", "Glasgow", {"region":"1","baseLocation":"366559"}, 366559, "Atlantic Quay - Glasgow", "Atlantic Quay - Glasgow", "glasgow"),
    ]

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)
    silver_m2 = spark.createDataFrame(m2_data, m2_schema)
    silver_m3 = spark.createDataFrame(m3_data, m3_schema)
    silver_h = spark.createDataFrame(silver_h_data, silver_h_schema)
    bronze_hearing_centres = spark.createDataFrame(bronze_hearing_centres_data, bronze_hearing_centres_schema)
    bronze_derive_hearing_centres = spark.createDataFrame(bronze_derive_hearing_centres_data, bronze_derive_hearing_centres_schema)


    general_content = general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres)

    results = {row["CaseNo"]: row.asDict() for row in general_content.collect()}
    return results

def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row.get(k) == v, f"{k} expected {v} but got {row.get(k)}"

def test_hearing_centre_lookup(general_outputs):
    assert general_outputs["EA/02375/2024"]["applicationChangeDesignatedHearingCentre"] == "glasgow"
    assert general_outputs["EA/01698/2024"]["applicationChangeDesignatedHearingCentre"] == "hattonCross"

def test_appellant_postcode_join(general_outputs):
    assert general_outputs["EA/02375/2024"]["Appellant_Postcode"] == "LD2R 5HB"
    assert general_outputs["EA/01698/2024"]["Appellant_Postcode"] == "B37 5LW"
    assert general_outputs["HU/00496/2025"]["Appellant_Postcode"] is None

def test_payment_remission(general_outputs):
    assert general_outputs["EA/01319/2023"]["PaymentRemissionRequested"] == "0"
    assert general_outputs["HU/00496/2025"]["PaymentRemissionRequested"] == "0"

def test_case_management_location(general_outputs):
    expected = {"region": "1", "baseLocation": "366559"}
    assert general_outputs["EA/02375/2024"]["caseManagementLocation"] == expected

def test_missing_centreId_handling(general_outputs):
    assert general_outputs["HU/00560/2025"]["applicationChangeDesignatedHearingCentre"] == "newport"