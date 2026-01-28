import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import caseData
from datetime import date, datetime

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("caseDataTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def caseData_outputs(spark):

    # -------------------------------
    # m1 data (cases)
    # -------------------------------
    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
        T.StructField("CentreId", T.IntegerType(), True),
        T.StructField("Rep_Postcode", T.StringType(), True),
        T.StructField("CaseRep_Postcode", T.StringType(), True),
        T.StructField("PaymentRemissionRequested", T.StringType(), True),
        T.StructField("lu_applicationChangeDesignatedHearingCentre", T.StringType(), True),
        T.StructField("OutOfTimeIssue", T.BooleanType(), True),
        T.StructField("DateLodged", T.TimestampType(), True),
        T.StructField("DateAppealReceived", T.TimestampType(), True),
        T.StructField("lu_hearingCentre", T.StringType(), True),
        T.StructField("lu_staffLocation", T.StringType(), True), 
        T.StructField("lu_caseManagementLocation",                      
                        T.StructType([
                            T.StructField("region", T.StringType(), True),
                            T.StructField("baseLocation", T.StringType(), True)
                     ]), True),
        T.StructField(
        "dv_hearingCentreDynamicList",
        T.StructType([
            T.StructField("value", T.StructType([
                T.StructField("code", T.StringType(), True),
                T.StructField("label", T.StringType(), True),
            ]), True),
            T.StructField("list_items", T.ArrayType(
                T.StructType([
                    T.StructField("code", T.StringType(), True),
                    T.StructField("label", T.StringType(), True),
                ])
            ), True)
        ]),
        True
    ),
    T.StructField(
    "dv_caseManagementLocationRefData",
    T.StructType([
        T.StructField("region", T.StringType(), True),
        T.StructField("baseLocation", T.StructType([
            T.StructField("value", T.StructType([
                T.StructField("code", T.StringType(), True),
                T.StructField("label", T.StringType(), True)
            ]), True),
            T.StructField("list_items", T.ArrayType(
                T.StructType([
                    T.StructField("code", T.StringType(), True),
                    T.StructField("label", T.StringType(), True)
                ])
            ), True)
        ]), True)
    ]),
    True
    )
    ])

    dv_hearingCentreDynamicList = {
    "value": {"code": "231596", "label": "Birmingham Civil And Family Justice Centre"},
    "list_items": [
        {"code": "227101", "label": "Newport Tribunal Centre - Columbus House"},
        {"code": "231596", "label": "Birmingham Civil And Family Justice Centre"},
        {"code": "28837", "label": "Harmondsworth Tribunal Hearing Centre"},
        {"code": "366559", "label": "Atlantic Quay - Glasgow"},
        {"code": "366796", "label": "Newcastle Civil And Family Courts And Tribunals Centre"},
        {"code": "386417", "label": "Hatton Cross Tribunal Hearing Centre"},
        {"code": "512401", "label": "Manchester Tribunal Hearing Centre - Piccadilly Exchange"},
        {"code": "649000", "label": "Yarls Wood Immigration And Asylum Hearing Centre"},
        {"code": "698118", "label": "Bradford Tribunal Hearing Centre"},
        {"code": "765324", "label": "Taylor House Tribunal Hearing Centre"}
        ]
    }

    dv_caseManagementLocationRefData = {
    "region": "1",
    "baseLocation": {
        "value": {"code": "231596", "label": "Birmingham Civil And Family Justice Centre"},
        "list_items": [
            {"code":"227101","label":"Newport Tribunal Centre - Columbus House"},
            {"code":"231596","label":"Birmingham Civil And Family Justice Centre"},
            {"code":"28837","label":"Harmondsworth Tribunal Hearing Centre"},
            {"code":"366559","label":"Atlantic Quay - Glasgow"},
            {"code":"366796","label":"Newcastle Civil And Family Courts And Tribunals Centre"},
            {"code":"386417","label":"Hatton Cross Tribunal Hearing Centre"},
            {"code":"512401","label":"Manchester Tribunal Hearing Centre - Piccadilly Exchange"},
            {"code":"649000","label":"Yarls Wood Immigration And Asylum Hearing Centre"},
            {"code":"698118","label":"Bradford Tribunal Hearing Centre"},
            {"code":"765324","label":"Taylor House Tribunal Hearing Centre"}
            ]
        }
    }

    # Assign CentreId to match bronze_hearing_centres
    m1_data = [
        # EA/10544/2022 – Birmingham
        ("EA/10544/2022", "AIP", "euSettlementScheme", 520, None, None, "0", None, False,
        datetime(2022,10,20), datetime(2022,10,20),
        "birmingham", "Birmingham", {"region":"1","baseLocation":"231596"}, dv_hearingCentreDynamicList, dv_caseManagementLocationRefData),

        # HU/00516/2025 – Bradford
        ("HU/00516/2025", "LR", "refusalOfHumanRights", 86, "S06 7UR", None, "0", None, False,
        datetime(2025,2,28), datetime(2025,2,28),
        "bradford", "Bradford", {"region":None,"baseLocation":None}, dv_hearingCentreDynamicList, dv_caseManagementLocationRefData),

        # EA/01698/2024 – Hatton Cross (migrated ARIA)
        ("EA/01698/2024", "LR", "euSettlementScheme", 386417, None, None, "0",
        "This is an ARIA Migrated Case.", True,
        datetime(2024,7,31), datetime(2024,7,31),
        "hattonCross", "Hatton Cross", {"region":None,"baseLocation":None}, dv_hearingCentreDynamicList, dv_caseManagementLocationRefData),

        # HU/00240/2022 – Manchester
        ("HU/00240/2022", "LR", "refusalOfHumanRights", 512401, None, "WN4R 8ET", "0", None, False,
        datetime(2021,11,4), datetime(2021,11,4),
        "manchester", "Manchester", {"region":None,"baseLocation":None}, dv_hearingCentreDynamicList, dv_caseManagementLocationRefData),

        # HU/00576/2025 – Taylor House
        ("HU/00576/2025", "AIP", "refusalOfHumanRights", 765324, None, None, "0",
        "This is a migrated ARIA case. Please refer to the documents.", True,
        datetime(2025,4,1), datetime(2025,4,1),
        "taylorHouse", "Taylor House", {"region":"1","baseLocation":"765324"}, dv_hearingCentreDynamicList, dv_caseManagementLocationRefData),

        # HU/00366/2025 – Fully missing
        ("HU/00366/2025", "AIP", "refusalOfHumanRights", None, None, None, "0",
        None, False,
        datetime(2024,11,6), datetime(2024,11,6),
        None, None, {"region":None,"baseLocation":None}, dv_hearingCentreDynamicList, dv_caseManagementLocationRefData),
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
        ("HU/00366/2025", None, "AB1 2CD")
    ]

    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("Outcome", T.StringType(), True),
    ])

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
                     ]), True),
    ])

    bronze_hearing_centres_data = [
        (
        520, "Birmingham IAC (Priory Courts)", None, "birmingham", "Birmingham",
        231596, "Birmingham Civil And Family Justice Centre",
        "Birmingham Civil And Family Justice Centre", "birmingham", "birmingham",
        {"region":"1","baseLocation":"231596"}
    ),
    (
        444, "Birmingham Magistrates Court (VLC)", None, "birmingham", "Birmingham",
        231596, "Birmingham Civil And Family Justice Centre",
        "Birmingham Civil And Family Justice Centre", "birmingham", "birmingham",
        {"region":"1","baseLocation":"231596"}
    ),
    (
        86, "Bradford", None, "bradford", "Bradford",
        698118, "Bradford Tribunal Hearing Centre",
        "Bradford Tribunal Hearing Centre", "bradford", "bradford",
        {"region":"1","baseLocation":"698118"}
    ),
    (
        512401, "Manchester Tribunal", None, "manchester", "Manchester",
        512401, "Manchester Tribunal Hearing Centre - Piccadilly Exchange",
        "Manchester Tribunal Hearing Centre - Piccadilly Exchange", "manchester", "manchester",
        {"region":"1","baseLocation":"512401"}
    ),
    (
        765324, "Taylor House", None, "taylorHouse", "Taylor House",
        765324, "Taylor House Tribunal Hearing Centre",
        "Taylor House Tribunal Hearing Centre", "taylorHouse", "taylorHouse",
        {"region":"1","baseLocation":"765324"}
    ),
    (
        386417, "Hatton Cross", None, "hattonCross", "Hatton Cross",
        386417, "Hatton Cross Tribunal Hearing Centre",
        "Hatton Cross Tribunal Hearing Centre", "hattonCross", "hattonCross",
        {"region":"1","baseLocation":"386417"}
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
            ]), True
        ),
        T.StructField("locationCode", T.IntegerType(), True),
        T.StructField("locationLabel", T.StringType(), True),
        T.StructField("selectedHearingCentreRefData", T.StringType(), True),
        T.StructField("applicationChangeDesignatedHearingCentre", T.StringType(), True),
    ])

    bronze_derive_hearing_centres_data = [
        ("birmingham", "Birmingham", {"region":"1","baseLocation":"231596"}, 231596, "Birmingham Civil And Family Justice Centre", "Birmingham Civil And Family Justice Centre", "birmingham"),
        ("bradford", "Bradford", {"region":"1","baseLocation":"698118"}, 698118, "Bradford Tribunal Hearing Centre", "Bradford Tribunal Hearing Centre", "bradford"),
        ("manchester", "Manchester", {"region":"1","baseLocation":"512401"}, 512401, "Manchester Tribunal Hearing Centre - Piccadilly Exchange", "Manchester Tribunal Hearing Centre - Piccadilly Exchange", "manchester"),
        ("taylorHouse", "Taylor House", {"region":"1","baseLocation":"765324"}, 765324, "Taylor House Tribunal Hearing Centre", "Taylor House Tribunal Hearing Centre", "taylorHouse"),
        ("hattonCross", "Hatton Cross", {"region":"1","baseLocation":"386417"}, 386417, "Hatton Cross Tribunal Hearing Centre", "Hatton Cross Tribunal Hearing Centre", "hattonCross"),
    ]

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)
    silver_m2 = spark.createDataFrame(m2_data, m2_schema)
    silver_m3 = spark.createDataFrame(m3_data, m3_schema)
    silver_h = spark.createDataFrame(silver_h_data, silver_h_schema)
    bronze_hearing_centres = spark.createDataFrame(bronze_hearing_centres_data, bronze_hearing_centres_schema)
    bronze_derive_hearing_centres = spark.createDataFrame(bronze_derive_hearing_centres_data, bronze_derive_hearing_centres_schema)

    caseData_content, _ = caseData(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres)
    results = {row["CaseNo"]: row.asDict() for row in caseData_content.collect()}
    return results

def test_submission_out_of_time(caseData_outputs):
    expected = {
        "EA/10544/2022": "No",
        "HU/00516/2025": "No",
        "EA/01698/2024": "Yes",
        "HU/00240/2022": "No",
        "HU/00576/2025": "Yes",
        "HU/00366/2025": "No",
    }
    for case, value in expected.items():
        assert caseData_outputs[case]["submissionOutOfTime"] == value

def test_recorded_out_of_time_decision_omitted(caseData_outputs):
    # All should be omitted with current empty m3_data
    for case in caseData_outputs.keys():
        assert "recordedOutOfTimeDecision" not in caseData_outputs[case]

def test_application_out_of_time_explanation(caseData_outputs):
    expected_cases = ["EA/01698/2024", "HU/00576/2025"]
    for case, data in caseData_outputs.items():
        if case in expected_cases:
            assert data["applicationOutOfTimeExplanation"] == "This is a migrated ARIA case. Please refer to the documents."
        else:
            assert "applicationOutOfTimeExplanation" not in data

def test_appeal_dates(caseData_outputs):
    for case, data in caseData_outputs.items():
        assert data["appealSubmissionDate"] is not None
        assert data["appealSubmissionInternalDate"] is not None
        assert data["tribunalReceivedDate"] is not None
        # Optionally check type
        from datetime import datetime
        assert isinstance(data["appealSubmissionDate"], datetime)

def test_admin_declaration(caseData_outputs):
    for data in caseData_outputs.values():
        assert data["adminDeclaration1"] == ["hasDeclared"]

def test_hearing_centres(caseData_outputs):
    expected = {
        "EA/10544/2022": ("birmingham", "Birmingham", {"region":"1","baseLocation":"231596"}),
        "HU/00516/2025": ("bradford", "Bradford", {"region":"1","baseLocation":"698118"}),
        "HU/00366/2025": (None, None, None),
    }
    for case, values in expected.items():
        hc, sl, cm = values
        data = caseData_outputs[case]
        assert data.get("hearingCentre") == hc
        assert data.get("staffLocation") == sl
        assert data.get("caseManagementLocation") == cm

def test_appeal_was_not_submitted_reason(caseData_outputs):
    expected = {
        "EA/01698/2024": "This is an ARIA Migrated Case.",
        "HU/00576/2025": None,
    }
    for case, value in expected.items():
        assert caseData_outputs[case].get("appealWasNotSubmittedReason") == value

def test_appellants_representation(caseData_outputs):
    expected = {
        "EA/10544/2022": "Yes",
        "HU/00516/2025": "No",
        "EA/01698/2024": "No",
        "HU/00240/2022": "No",
        "HU/00576/2025": "Yes",
        "HU/00366/2025": "Yes",
    }
    for case, value in expected.items():
        assert caseData_outputs[case]["appellantsRepresentation"] == value

def test_has_other_appeals(caseData_outputs):
    for data in caseData_outputs.values():
        assert data["hasOtherAppeals"] == "NotSure"

