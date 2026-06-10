import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPendingDetained import homeOfficeDetails
from datetime import date


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("paymentPendingDetainedHomeOfficeTests")
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
        T.StructField("dv_CCDAppealType", T.StringType(), True),
    ])

    # Test cases:
    #   HU/DET/0001 — Detained=1, OOC (no UK postcode), no GWF → homeOfficeDecisionDate populated (detained)
    #   HU/DET/0002 — Detained=2, same
    #   HU/DET/0003 — Detained=4, same
    #   HU/DET/0004 — Detained=1, OOC, GWF ref in bronze → homeOfficeDecisionDate populated; gwfReferenceNumber=None
    #   HU/INK/0001 — Not detained, UK postcode → dv_appellantIsInUk=True → homeOfficeDecisionDate populated
    #   HU/CAT/0037 — Not detained, CategoryId=37 → dv_appellantIsInUk=True → homeOfficeDecisionDate populated
    #   HU/OOC/0001 — Not detained, OOC, NOT GWF → decisionLetterReceivedDate populated
    #   HU/OOC/0002 — Not detained, OOC, GWF → gwfReferenceNumber + dateEntryClearanceDecision populated
    #   HU/NUL/0001 — No DateOfApplicationDecision → all date fields null
    m1_data = [
        ("HU/DET/0001", "LR", "A", date(2024, 3, 15), None,  "HU"),
        ("HU/DET/0002", "LR", "A", date(2024, 3, 15), None,  "HU"),
        ("HU/DET/0003", "LR", "A", date(2024, 3, 15), None,  "HU"),
        ("HU/DET/0004", "LR", "A", date(2024, 3, 15), None,  "HU"),
        ("HU/INK/0001", "LR", "A", date(2024, 7, 22), None,  "HU"),
        ("HU/CAT/0037", "LR", "A", date(2024, 8, 10), None,  "HU"),
        ("HU/OOC/0001", "LR", "A", date(2023, 11, 5), None,  "HU"),
        ("HU/OOC/0002", "LR", "A", date(2022, 6, 30), None,  "HU"),
        ("HU/NUL/0001", "LR", "A", None,              None,  "HU"),
    ]

    m2_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("FCONumber", T.StringType(), True),
        T.StructField("Relationship", T.StringType(), True),
        T.StructField("AppellantCountryId", T.IntegerType(), True),
        T.StructField("lu_countryGovUkOocAdminJ", T.StringType(), True),
        T.StructField("Appellant_Address1", T.StringType(), True),
        T.StructField("Appellant_Address2", T.StringType(), True),
        T.StructField("Appellant_Address3", T.StringType(), True),
        T.StructField("Appellant_Address4", T.StringType(), True),
        T.StructField("Appellant_Address5", T.StringType(), True),
        T.StructField("Appellant_Postcode", T.StringType(), True),
        T.StructField("Detained", T.IntegerType(), True),
    ])

    m2_data = [
        # Detained cases: no UK postcode → dv_appellantIsInUk=False, but detained flag drives decision date
        ("HU/DET/0001", None,        None, None, None, None, None, None, None, None, None, 1),
        ("HU/DET/0002", None,        None, None, None, None, None, None, None, None, None, 2),
        ("HU/DET/0003", None,        None, None, None, None, None, None, None, None, None, 4),
        # Detained=1 with GWF ref: is_detained_or_in_uk=True → still uses decision date path; gwfReferenceNumber omitted
        ("HU/DET/0004", None,        None, None, None, None, None, None, None, None, None, 1),
        # Not detained, valid UK postcode → dv_appellantIsInUk=True
        ("HU/INK/0001", None,        None, None, None, None, None, None, None, None, "SW1A 1AA", 0),
        # Not detained, CategoryId=37 → dv_appellantIsInUk=True
        ("HU/CAT/0037", None,        None, None, None, None, None, None, None, None, None, 0),
        # Not detained, OOC, no GWF → decisionLetterReceivedDate path
        ("HU/OOC/0001", "FCOXXXXXX", None, None, None, None, None, None, None, None, None, 0),
        # Not detained, OOC, GWF ref via HORef → dateEntryClearanceDecision + gwfReferenceNumber
        ("HU/OOC/0002", None,        None, None, None, None, None, None, None, None, None, 0),
        # No decision date
        ("HU/NUL/0001", None,        None, None, None, None, None, None, None, None, None, 0),
    ]

    silver_c_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("CategoryId", T.IntegerType(), True),
        T.StructField("dv_targetState", T.StringType(), True),
    ])

    silver_c_data = [
        ("HU/CAT/0037", 37, "paymentPendingDetained"),
    ]

    bronze_HORef_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("HORef", T.StringType(), True),
        T.StructField("FCONumber", T.StringType(), True),
    ])

    # DET/0004: GWF reference in bronze — detained should still use decision date path, not GWF
    # OOC/0002: GWF reference in bronze → dateEntryClearanceDecision + gwfReferenceNumber populated
    bronze_HORef_data = [
        ("HU/DET/0004", "GWF061000001", None),
        ("HU/OOC/0002", "GWF063622999", None),
    ]

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)
    silver_m2 = spark.createDataFrame(m2_data, m2_schema)
    silver_c = spark.createDataFrame(silver_c_data, silver_c_schema)
    bronze_HORef = spark.createDataFrame(bronze_HORef_data, bronze_HORef_schema)

    homeOfficeDetails_content, _ = homeOfficeDetails(silver_m1, silver_m2, silver_c, bronze_HORef)

    return {row["CaseNo"]: row.asDict() for row in homeOfficeDetails_content.collect()}


def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row.get(k) == v, f"{k}: expected {v!r} but got {row.get(k)!r}"


# ──────────────────────────────────────────────────────────────────────────────
# Detained cases: homeOfficeDecisionDate must be populated regardless of dv_appellantIsInUk
# ──────────────────────────────────────────────────────────────────────────────

def test_detained_1_home_office_decision_date(homeOfficeDetails_outputs):
    """Detained=1: homeOfficeDecisionDate populated; decisionLetterReceivedDate and gwfReferenceNumber null."""
    row = homeOfficeDetails_outputs["HU/DET/0001"]
    assert_equals(
        row,
        homeOfficeDecisionDate="2024-03-15",
        decisionLetterReceivedDate=None,
        dateEntryClearanceDecision=None,
        gwfReferenceNumber=None,
    )
    assert row["homeOfficeReferenceNumber"] is not None


def test_detained_2_home_office_decision_date(homeOfficeDetails_outputs):
    """Detained=2 (IRC): same date logic as Detained=1."""
    row = homeOfficeDetails_outputs["HU/DET/0002"]
    assert_equals(
        row,
        homeOfficeDecisionDate="2024-03-15",
        decisionLetterReceivedDate=None,
        dateEntryClearanceDecision=None,
        gwfReferenceNumber=None,
    )


def test_detained_4_home_office_decision_date(homeOfficeDetails_outputs):
    """Detained=4 (other): same date logic as Detained=1."""
    row = homeOfficeDetails_outputs["HU/DET/0003"]
    assert_equals(
        row,
        homeOfficeDecisionDate="2024-03-15",
        decisionLetterReceivedDate=None,
        dateEntryClearanceDecision=None,
        gwfReferenceNumber=None,
    )


def test_detained_with_gwf_reference_uses_decision_date_not_gwf(homeOfficeDetails_outputs):
    """Detained=1 with a GWF bronze reference still uses the decision date path (not OOC path)."""
    row = homeOfficeDetails_outputs["HU/DET/0004"]
    assert_equals(
        row,
        homeOfficeDecisionDate="2024-03-15",
        decisionLetterReceivedDate=None,
        dateEntryClearanceDecision=None,
        gwfReferenceNumber=None,
    )
    assert row["homeOfficeReferenceNumber"] is not None


# ──────────────────────────────────────────────────────────────────────────────
# In-UK cases (not detained, dv_appellantIsInUk=True)
# ──────────────────────────────────────────────────────────────────────────────

def test_in_uk_home_office_decision_date(homeOfficeDetails_outputs):
    """Not detained but UK postcode → dv_appellantIsInUk=True → homeOfficeDecisionDate populated."""
    row = homeOfficeDetails_outputs["HU/INK/0001"]
    assert_equals(
        row,
        homeOfficeDecisionDate="2024-07-22",
        decisionLetterReceivedDate=None,
        dateEntryClearanceDecision=None,
        gwfReferenceNumber=None,
    )
    assert row["homeOfficeReferenceNumber"] is not None


def test_category_37_sets_home_office_decision_date(homeOfficeDetails_outputs):
    """CategoryId=37 → dv_appellantIsInUk=True → homeOfficeDecisionDate populated even without UK postcode."""
    row = homeOfficeDetails_outputs["HU/CAT/0037"]
    assert_equals(
        row,
        homeOfficeDecisionDate="2024-08-10",
        decisionLetterReceivedDate=None,
        dateEntryClearanceDecision=None,
        gwfReferenceNumber=None,
    )
    assert row["homeOfficeReferenceNumber"] is not None


# ──────────────────────────────────────────────────────────────────────────────
# OOC cases (not detained, dv_appellantIsInUk=False)
# ──────────────────────────────────────────────────────────────────────────────

def test_ooc_non_gwf_decision_letter_received_date(homeOfficeDetails_outputs):
    """OOC, not detained, no GWF ref → decisionLetterReceivedDate populated; homeOfficeDecisionDate null."""
    row = homeOfficeDetails_outputs["HU/OOC/0001"]
    assert_equals(
        row,
        homeOfficeDecisionDate=None,
        decisionLetterReceivedDate="2023-11-05",
        dateEntryClearanceDecision=None,
        gwfReferenceNumber=None,
    )
    assert row["homeOfficeReferenceNumber"] is not None


def test_ooc_gwf_date_entry_clearance_and_gwf_number(homeOfficeDetails_outputs):
    """OOC, not detained, GWF ref → dateEntryClearanceDecision + gwfReferenceNumber populated."""
    row = homeOfficeDetails_outputs["HU/OOC/0002"]
    assert_equals(
        row,
        homeOfficeDecisionDate=None,
        decisionLetterReceivedDate=None,
        dateEntryClearanceDecision="2022-06-30",
        homeOfficeReferenceNumber="999999999",
    )
    assert row["gwfReferenceNumber"] is not None
    assert "GWF" not in row["gwfReferenceNumber"]  # cleaned — digits only


# ──────────────────────────────────────────────────────────────────────────────
# No DateOfApplicationDecision → all date fields null
# ──────────────────────────────────────────────────────────────────────────────

def test_no_decision_date_all_date_fields_null(homeOfficeDetails_outputs):
    """No DateOfApplicationDecision → every date field is null regardless of detained/in-uk status."""
    row = homeOfficeDetails_outputs["HU/NUL/0001"]
    assert_equals(
        row,
        homeOfficeDecisionDate=None,
        decisionLetterReceivedDate=None,
        dateEntryClearanceDecision=None,
        gwfReferenceNumber=None,
    )
    assert row["homeOfficeReferenceNumber"] is not None


# ──────────────────────────────────────────────────────────────────────────────
# Static flags
# ──────────────────────────────────────────────────────────────────────────────

def test_static_home_office_flags(homeOfficeDetails_outputs):
    """homeOfficeNotificationsEligible is always 'Yes'."""
    for case_no, row in homeOfficeDetails_outputs.items():
        assert row["homeOfficeNotificationsEligible"] == "Yes", (
            f"{case_no}: homeOfficeNotificationsEligible expected 'Yes' but got {row['homeOfficeNotificationsEligible']!r}"
        )
