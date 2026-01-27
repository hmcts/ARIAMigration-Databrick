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
        ("EA/10544/2022", "AIP", "euSettlementScheme", 37, None, None, "0", None),
        ("HU/00516/2025", "LR", "refusalOfHumanRights", 86, "S06 7UR", None, "0", None),
        ("EA/04437/2020", "LR", "refusalOfEu", 77, None, "WN4R 8ET", "0", None),
        ("HU/00140/2024", "LR", "refusalOfHumanRights", 2, "NE45 8RJ", None, "0", None),
        ("EA/03592/2023", "LR", "euSettlementScheme", 77, None, "SE86 9UW", "0", None),
        ("EA/02375/2024", "AIP", "euSettlementScheme", 37, None, None, "0", None),
    ]

    m2_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("Relationship", T.StringType(), True),
        T.StructField("Appellant_Postcode", T.StringType(), True),
    ])

    m2_data = [
        ("EA/10544/2022", None, "NN33 8XZ"),
        ("HU/00516/2025", None, "N1W 0LE"),
        ("EA/04437/2020", None, "S5 8NH"),
        ("HU/00140/2024", None, "PE6 4RH"),
        ("EA/03592/2023", None, "W95 3UX"),
        ("EA/02375/2024", None, "LD2R 5HB"),
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
        ("HU/00140/2024", 117070534, 49, "XXXXXXXXXXXXXXXX", "paymentPending"),
        ("HU/00516/2025", 118350122, 18, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "paymentPending"),
        ("EA/02375/2024", 117941932, 20, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "paymentPending"),
        ("EA/04437/2020", 111949213, 18, "XXXXXXXXXX", "paymentPending"),
        ("EA/10544/2022", 115591725, 16, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "paymentPending"),
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
            540,"Alloa Sheriff Court",None,"glasgow","Glasgow","366559","Atlantic Quay - Glasgow","Atlantic Quay - Glasgow","alloaSherrif","glasgow",{"region":"1","baseLocation":"366559"}
        ),
        (
            7, "Belfast", None, "glasgow", "Glasgow", "366559", "Atlantic Quay - Glasgow", "Atlantic Quay - Glasgow", "belfast", "glasgow", {"region":"1","baseLocation":"366559"}
        ),
        (
            421,"Belfast - Laganside", None, "glasgow", "Glasgow", "366559", "Atlantic Quay - Glasgow", "Atlantic Quay - Glasgow", "belfast", "glasgow", {"region":"1","baseLocation":"366559"}
        ),
        (
            520, "Birmingham IAC (Priory Courts)", None, "birmingham", "Birmingham", "231596", "Birmingham Civil And Family Justice Centre", "Birmingham Civil And Family Justice Centre", "birmingham", "birmingham", {"region":"1","baseLocation":"231596"}
        ),
        (
            444, "Birmingham Magistrates Court (VLC)", None, "birmingham", "Birmingham", "231596", "Birmingham Civil And Family Justice Centre", "Birmingham Civil And Family Justice Centre", "birmingham", "birmingham", {"region":"1","baseLocation":"231596"}
        ),
        (
            86, "Bradford", None, "bradford", "Bradford","698118","Bradford Tribunal Hearing Centre", "Bradford Tribunal Hearing Centre", "bradford", "bradford", {"region":"1","baseLocation":"698118"}
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


    general_content, _ = general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres)

    results = {row["CaseNo"]: row.asDict() for row in general_content.collect()}
    return results

def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row.get(k) == v, f"{k} expected {v} but got {row.get(k)}"

def test_isServiceRequestTabVisibleConsideringRemissions(general_outputs):
    """
    Test 'isServiceRequestTabVisibleConsideringRemissions' with varied examples, including 'No'.
    """
    results = general_outputs

    # Expected values based on your example data
    expected_values = {
        "EA/10544/2022": "No",
        "HU/00516/2025": "No",
        "EA/04437/2020": "No",
        "HU/00140/2024": "No",
        "EA/03592/2023": "No",
        "EA/02375/2024": "No",
    }

    for case_no, expected in expected_values.items():
        assert results[case_no]["isServiceRequestTabVisibleConsideringRemissions"] == expected, \
            f"Case {case_no}: expected {expected}, got {results[case_no]['isServiceRequestTabVisibleConsideringRemissions']}"

def test_applicationChangeDesignatedHearingCentre(general_outputs):
    """
    Test 'applicationChangeDesignatedHearingCentre' with multiple examples, including nulls and different hearing centres.
    """
    results = general_outputs

    # Expected values based on postcode / CentreId logic
    expected_values = {
        "EA/10544/2022": "birmingham",
        "HU/00516/2025": "bradford",
        "EA/04437/2020": "manchester",
        "HU/00140/2024": "hattonCross",
        "EA/03592/2023": "taylorHouse",
        "EA/02375/2024": None,
    }

    for case_no, expected in expected_values.items():
        actual = results[case_no]["applicationChangeDesignatedHearingCentre"]
        assert actual == expected, f"Case {case_no}: expected {expected}, got {actual}"