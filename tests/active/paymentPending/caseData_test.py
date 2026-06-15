import pytest
from pyspark.sql import SparkSession, types as T, Row
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPendingDetained import caseData
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
        T.StructField("dv_TargetState", T.StringType(), True),
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
    ),
    T.StructField(
    "lu_selectedHearingCentreRefData",
    T.StructType([
        T.StructField("code", T.StringType(), True),
        T.StructField("label", T.StringType(), True)
    ]),
    True
    )
    ])

    lu_selectedHearingCentre_birmingham = {
        "code": "231596",
        "label": "Birmingham Civil And Family Justice Centre"
    }
    lu_selectedHearingCentre_taylor = {
        "code": "765324",
        "label": "Taylor House Tribunal Hearing Centre"
    }
    lu_selectedHearingCentre_manchester = {
        "code": "512401",
        "label": "Manchester Tribunal Hearing Centre - Piccadilly Exchange"
    }
    lu_selectedHearingCentre_bradford = {
        "code": "698118",
        "label": "Bradford Tribunal Hearing Centre"
    }
    lu_selectedHearingCentre_newport = {
        "code": "227101",
        "label": "Newport Tribunal Centre - Columbus House"
    }
    lu_selectedHearingCentre_hatton = {
        "code": "386417",
        "label": "Hatton Cross Tribunal Hearing Centre"
    }

    # Placeholder / unresolved
    lu_selectedHearingCentre_placeholder = {
        "code": "map with fileLocation, else assignHearingCentre",
        "label": "map with fileLocation, else assignHearingCentre"
    }

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
    ("EA/10544/2022", "listing", "AIP", "euSettlementScheme", 520, None, None, "0", None, False,
     datetime(2022,10,20), datetime(2022,10,20),
     "birmingham", "Birmingham", {"region":"1","baseLocation":"231596"}, dv_hearingCentreDynamicList, dv_caseManagementLocationRefData, lu_selectedHearingCentre_birmingham),

    # HU/00516/2025 – Bradford
    ("HU/00516/2025", "appealSubmitted", "LR", "refusalOfHumanRights", 86, "S06 7UR", None, "0", None, False,
     datetime(2025,2,28), datetime(2025,2,28),
     "bradford", "Bradford", {"region":None,"baseLocation":None}, dv_hearingCentreDynamicList, dv_caseManagementLocationRefData, lu_selectedHearingCentre_bradford),

    # EA/01698/2024 – Hatton Cross (migrated ARIA)
    ("EA/01698/2024", "appealSubmitted", "LR", "euSettlementScheme", 386417, None, None, "0",
     "This is an ARIA Migrated Case.", True,
     datetime(2024,7,31), datetime(2024,7,31),
     "hattonCross", "Hatton Cross", {"region":None,"baseLocation":None}, dv_hearingCentreDynamicList, dv_caseManagementLocationRefData, lu_selectedHearingCentre_hatton),

    # HU/00240/2022 – Manchester
    ("HU/00240/2022", "appealSubmitted", "LR", "refusalOfHumanRights", 512401, None, "WN4R 8ET", "0", None, False,
     datetime(2021,11,4), datetime(2021,11,4),
     "manchester", "Manchester", {"region":None,"baseLocation":None}, dv_hearingCentreDynamicList, dv_caseManagementLocationRefData, lu_selectedHearingCentre_manchester),

    # HU/00576/2025 – Taylor House
    ("HU/00576/2025", "appealSubmitted", "AIP", "refusalOfHumanRights", 765324, None, None, "0",
     "This is a migrated ARIA case. Please refer to the documents.", True,
     datetime(2025,4,1), datetime(2025,4,1),
     "taylorHouse", "Taylor House", {"region":"1","baseLocation":"765324"}, dv_hearingCentreDynamicList, dv_caseManagementLocationRefData, lu_selectedHearingCentre_taylor),

    # HU/00366/2025 – Fully missing / unresolved
    ("HU/00366/2025", "appealSubmitted", "AIP", "refusalOfHumanRights", None, None, None, "0",
     None, False,
     datetime(2024,11,6), datetime(2024,11,6),
     None, None, {"region":None,"baseLocation":None}, dv_hearingCentreDynamicList, dv_caseManagementLocationRefData, lu_selectedHearingCentre_placeholder),
    ]

    m2_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("Relationship", T.StringType(), True),
        T.StructField("Appellant_Postcode", T.StringType(), True),
        T.StructField("DetentionCentreId", T.IntegerType(), True),
        T.StructField("Detained", T.IntegerType(), True),
        # T.StructField("CentreId", T.IntegerType(), True),
    ])

    m2_data = [
        ("EA/10544/2022", None, "NN33 8XZ",None,None),
        ("HU/00516/2025", None, "N1W 0LE",None,None),
        ("EA/04437/2020", None, "S5 8NH",None,None),
        ("HU/00140/2024", None, "PE6 4RH",None,None),
        ("EA/03592/2023", None, "W95 3UX",None,None),
        ("EA/02375/2024", None, "LD2R 5HB",None,None),
        ("HU/00366/2025", None, "AB1 2CD",None,None)
    ]

    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("Outcome", T.StringType(), True),
        T.StructField("OutOfTime", T.BooleanType(), True),
    ])

    m3_data = [
        ("EA/01698/2024", 1, "0", True),   # OutOfTimeIssue=True, appealSubmitted, Outcome=0 → recordedOutOfTimeDecision None
        ("HU/00576/2025", 1, "1", True),   # OutOfTimeIssue=True, appealSubmitted, Outcome=1 → recordedOutOfTimeDecision Yes
    ]

    silver_h_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("HistoryId", T.IntegerType(), True),
        T.StructField("HistType", T.IntegerType(), True),
        T.StructField("Comment", T.StringType(), True),
        T.StructField("dv_targetState", T.StringType(), True),
        
    ])

    silver_h_data = [
        ("HU/00140/2024", 117070534, 49, "XXXXXXXXXXXXXXXX", "appealSubmitted"),
        ("HU/00516/2025", 118350122, 18, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "appealSubmitted"),
        ("EA/02375/2024", 117941932, 20, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "appealSubmitted"),
        ("EA/04437/2020", 111949213, 18, "XXXXXXXXXX", "appealSubmitted"),
        ("EA/10544/2022", 115591725, 16, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "listing"),
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
        T.StructField("caseManagementLocation", T.StringType(), True),

        # caseManagementLocation as STRUCT
        # T.StructField(
        #     "caseManagementLocation",
        #     T.StructType([
        #         T.StructField("region", T.StringType(), True),
        #         T.StructField("baseLocation", T.StringType(), True),
        #     ]),
        #     True
        # ),

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

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)
    silver_m2 = spark.createDataFrame(m2_data, m2_schema)
    silver_m3 = spark.createDataFrame(m3_data, m3_schema)
    silver_h = spark.createDataFrame(silver_h_data, silver_h_schema)
    bronze_hearing_centres = spark.createDataFrame(bronze_hearing_centres_data, bronze_hearing_centres_schema)
    bronze_derive_hearing_centres = spark.createDataFrame(bronze_derive_hearing_centres_data, bronze_derive_hearing_centres_schema)
    df_dcs = spark.createDataFrame(dcs_data, dcs_schema)

    caseData_content, _ = caseData(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres,df_dcs)

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

def test_recorded_out_of_time_decision(caseData_outputs):
    expected = {
        "EA/01698/2024": None,   # OutOfTimeIssue=True, appealSubmitted, Outcome="0" → None (0 excluded)
        "HU/00576/2025": "Yes",  # OutOfTimeIssue=True, appealSubmitted, Outcome="1" → Yes
        "EA/10544/2022": None,   # OutOfTimeIssue=False
        "HU/00516/2025": None,   # OutOfTimeIssue=False
        "HU/00240/2022": None,   # OutOfTimeIssue=False
        "HU/00366/2025": None,   # OutOfTimeIssue=False
    }
    for case, value in expected.items():
        assert caseData_outputs[case].get("recordedOutOfTimeDecision") == value

def test_application_out_of_time_explanation(caseData_outputs):
    expected_cases = ["EA/01698/2024", "HU/00576/2025"]

    for case, data in caseData_outputs.items():
        if case in expected_cases:
            assert (
                data["applicationOutOfTimeExplanation"]
                == "This is a migrated ARIA case. Please refer to the documents."
            )
        else:
            assert data.get("applicationOutOfTimeExplanation") in (None, "")

def test_appeal_dates(caseData_outputs):
    for data in caseData_outputs.values():
        assert data["appealSubmissionDate"] is not None
        assert data["appealSubmissionInternalDate"] is not None
        assert data["tribunalReceivedDate"] is not None

        # validate ISO date string
        datetime.strptime(data["appealSubmissionDate"], "%Y-%m-%d")

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
        cm_actual = data.get("caseManagementLocation")
        # Convert PySpark Row to dict if needed
        if isinstance(cm_actual, Row):
            cm_actual = cm_actual.asDict()
        assert cm_actual == cm

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
