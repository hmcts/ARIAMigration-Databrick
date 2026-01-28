import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import homeOfficeDetails
import uuid

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("homeOfficeDetailsTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def homeOfficeDetails_outputs(spark):

    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
        T.StructField("DateOfApplicationDecision", T.DateType(), True),
        T.StructField("HORef", T.StringType(), True),
    ])

    m1_data = [    
        ("EA/06826/2022", "LR",  "A", date(2022, 6, 30), None),
        ("EA/09676/2022", "LR",  "A", date(2020, 2, 15), None),
        ("EA/00591/2025", "LR",  "A", date(2022, 9, 5),  None),
        ("EA/00441/2025", "AIP", "B", date(2025, 2, 12), None),
        ("HU/00512/2025", "LR",  "A", date(2024, 7, 11), "R1277473"),
        ("HU/02151/2024", "LR",  "A", None, None),
    ]

    m2_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), False),
        T.StructField("FCONumber", T.StringType(), True),
        T.StructField("Relationship", T.StringType(), True),
    ])

    m2_data = [
        ("EA/06826/2022", "XXXXXX", None),
        ("EA/09676/2022", "XXXXXX", None),
        ("EA/00591/2025", "XXXXXX", None),
        ("EA/00441/2025", None,      None),
        ("HU/00512/2025", None,      None),
        ("HU/02151/2024", "XXXXXXXXXXXX", None),
    ]

    silver_c_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), False),
        T.StructField("CategoryId", T.IntegerType(), True),
        T.StructField("dv_targetState", T.StringType(), True),
    ])

    silver_c_data = [    ("EA/00441/2025", 3,  "paymentPending"),
    ("EA/00441/2025", 11, "paymentPending"),
    ("EA/00441/2025", 37, "paymentPending"),
    ("EA/00441/2025", 47, "paymentPending"),

    ("EA/00591/2025", 11, "paymentPending"),
    ("EA/00591/2025", 38, "paymentPending"),
    ("EA/00591/2025", 47, "paymentPending"),

    ("EA/06826/2022", 11, "paymentPending"),
    ("EA/06826/2022", 38, "paymentPending"),
    ("EA/06826/2022", 47, "paymentPending"),

    ("EA/09676/2022", 11, "paymentPending"),
    ("EA/09676/2022", 38, "paymentPending"),
    ("EA/09676/2022", 47, "paymentPending"),

    ("HU/00512/2025", 3,  "paymentPending"),
    ("HU/00512/2025", 10, "paymentPending"),
    ("HU/00512/2025", 31, "paymentPending"),
    ("HU/00512/2025", 37, "paymentPending"),
    ("HU/00512/2025", 39, "paymentPending"),
    ]

    bronze_HORef_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), False),
        T.StructField("HORef", T.StringType(), True),
        T.StructField("FCONumber", T.StringType(), True),
    ])

    bronze_HORef_data = [("EA/09676/2022", "GWF063622668", "338224"), ("EA/06826/2022", "GWF061121374", "253601"),]

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)
    silver_m2 = spark.createDataFrame(m2_data, m2_schema)
    silver_c = spark.createDataFrame(silver_c_data, silver_c_schema)
    bronze_HORef = spark.createDataFrame(bronze_HORef_data, bronze_HORef_schema)

    # Call the function under test
    homeOfficeDetails_content, _ = homeOfficeDetails(silver_m1, silver_m2, silver_c, bronze_HORef)

    # Convert to dictionary keyed by CaseNo
    results = {row["CaseNo"]: row.asDict() for row in homeOfficeDetails_content.collect()}
    return results

def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row.get(k) == v, f"{k} expected {v} but got {row.get(k)}"

def test_category_37_sets_home_office_decision_date(homeOfficeDetails_outputs):
    row = homeOfficeDetails_outputs["EA/00441/2025"]

    assert_equals(
        row,
        homeOfficeDecisionDate="2025-02-12",
        decisionLetterReceivedDate=None,
        dateEntryClearanceDecision=None,
        homeOfficeReferenceNumber=None,
        gwfReferenceNumber=None,
    )

def test_category_38_with_gwf_reference(homeOfficeDetails_outputs):
    row = homeOfficeDetails_outputs["EA/06826/2022"]

    assert_equals(
        row,
        homeOfficeDecisionDate=None,
        decisionLetterReceivedDate=None,
        dateEntryClearanceDecision="2022-06-30",
        homeOfficeReferenceNumber=None,
        gwfReferenceNumber="GWF061121374",
    )

def test_category_38_with_gwf_reference_second_case(homeOfficeDetails_outputs):
    row = homeOfficeDetails_outputs["EA/09676/2022"]

    assert_equals(
        row,
        decisionLetterReceivedDate=None,
        dateEntryClearanceDecision="2020-02-15",
        homeOfficeReferenceNumber=None,
        gwfReferenceNumber="GWF063622668",
    )

def test_category_38_without_gwf_reference(homeOfficeDetails_outputs):
    row = homeOfficeDetails_outputs["EA/00591/2025"]

    assert_equals(
        row,
        decisionLetterReceivedDate="2022-09-05",
        dateEntryClearanceDecision=None,
        homeOfficeReferenceNumber=None,
        gwfReferenceNumber=None,
    )

def test_no_relevant_category_ids(homeOfficeDetails_outputs):
    row = homeOfficeDetails_outputs["HU/02151/2024"]

    assert_equals(
        row,
        homeOfficeDecisionDate=None,
        decisionLetterReceivedDate=None,
        dateEntryClearanceDecision=None,
        homeOfficeReferenceNumber=None,
        gwfReferenceNumber=None,
    )

def test_static_home_office_flags(homeOfficeDetails_outputs):
    for case_no, row in homeOfficeDetails_outputs.items():
        assert row["isHomeOfficeIntegrationEnabled"] == "Yes"
        assert row["homeOfficeNotificationsEligible"] == "Yes"

