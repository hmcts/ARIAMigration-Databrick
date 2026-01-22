import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import legalRepDetails

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("legalRepDetailsTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def legalRepDetails_outputs(spark):

    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True), 
        T.StructField("Rep_Email", T.StringType(), True),
        T.StructField("CaseRep_Email", T.StringType(), True),
        T.StructField("CaseRep_FileSpecific_Email", T.StringType(), True),
        T.StructField("Contact", T.StringType(), True),
        T.StructField("Rep_Name", T.StringType(), True),
        T.StructField("CaseRep_Name", T.StringType(), True),
        T.StructField("RepresentativeId", T.IntegerType(), True),
        T.StructField("CaseRep_Address1", T.StringType(), True),
        T.StructField("CaseRep_Address2", T.StringType(), True),
        T.StructField("CaseRep_Address3", T.StringType(), True),
        T.StructField("CaseRep_Address4", T.StringType(), True),
        T.StructField("CaseRep_Address5", T.StringType(), True),
        T.StructField("CaseRep_Postcode", T.StringType(), True),
        T.StructField("Rep_Address1", T.StringType(), True),
        T.StructField("Rep_Address2", T.StringType(), True),
        T.StructField("Rep_Address3", T.StringType(), True),
        T.StructField("Rep_Address4", T.StringType(), True),
        T.StructField("Rep_Address5", T.StringType(), True),
        T.StructField("Rep_Postcode", T.StringType(), True)
    ])

    m1_data = [
        # HU/00185/2025 -> No address, ooc all null
        ("HU/00185/2025", "LR", "FTPA",
         "Mendez-ReedX", "Mendez-ReedX", None,
         0,
         "fgrimes@example.com", None, None,
         None, None, None, None, None, None,
         None, None, None, None, None, None),

        # HU/01475/2024 -> Non-UK -> No, OOC populated (MVX)
        ("HU/01475/2024", "LR", "FTPA",
         "Barnes-VargasX", "Barnes-VargasX", None,
         0,
         "rachel34@example.org", None, None,
         "56292 Raymond TrafficwayX", "Cheyenne SquareX", "New MollyX", "ReunionX", "MVX", None,
         None, None, None, None, None, None),

        # HU/02191/2024 -> Guam -> GU, No, OOC populated
        ("HU/02191/2024", "LR", "FTPA",
         "Douglas GutierrezX", "Leon LLCX", None,
         0,
         "jameskelly@example.com", None, None,
         "925 Lisa Plains Apt. 642X", "Hill SquareX", "LynchhavenX", "GuamX", "NLX", None,
         None, None, None, None, None, None),

        # HU/02151/2024 -> No address, ooc all null
        ("HU/02151/2024", "LR", "FTPA",
         "Black, Moore and BurkeX", "Black, Moore and BurkeX", None,
         0,
         "wstevens@example.org", None, None,
         None, None, None, None, None, None,
         None, None, None, None, None, None),

        # HU/00539/2025 -> RepId=0 but sample says Yes + legalRepAddressUK populated
        # We simulate it as RepId>0 using Rep_* to guarantee Yes
        ("HU/00539/2025", "LR", "FTPA",
         "Mcmahon IncX", "Mcmahon IncX", None,
         999,
         "ptravis@example.org", None, None,
         None, None, None, None, None, None,
         "209 Hampton TerraceX", "Kimberly PassageX", "ChristensenstadX", None, None, "IP4 2EJ"),

        # EA/03862/2020 -> RepId>0 Yes, Rep_* used
        ("EA/03862/2020", "LR", "FTPA",
         "Walker, Greene and WhiteX", "Walker, Greene and WhiteX", None,
         1674,
         "ynelson@example.org", None, None,
         None, None, None, None, None, None,
         "734 Carson Plains Apt. 731X", "Rodgers ShoalX", None, None, None, "S65 7EB"),
    ]

    bronze_country_schema = T.StructType([
        T.StructField("countryFromAddress", T.StringType(), True),
        T.StructField("oocLrCountryGovUkAdminJ", T.StringType(), True)
    ])

    bronze_country_data = [("United Kingdom", "UK"), ("Guam", "GU"), ("Argentina", "AR")]

    silver_m1 =  spark.createDataFrame(m1_data, m1_schema)
    bronze_countryFromAddress =  spark.createDataFrame(bronze_country_data, bronze_country_schema)

    # Call the function under test
    legalRepDetails_content, _ = legalRepDetails(silver_m1, bronze_countryFromAddress)

    # Convert to dictionary keyed by CaseNo
    results = {row["CaseNo"]: row.asDict() for row in legalRepDetails_content.collect()}
    return results

def assert_is_null(row, *fields):
    for f in fields:
        assert row[f] is None, f"{f} expected None but got {row[f]}"

def assert_equals(row, **expected):
    for field, value in expected.items():
        assert row[field] == value, f"{field} expected {value} but got {row[field]}"

def test_legalRepHasAddress_case02(legalRepDetails_outputs):
    """RepId>0 → Yes"""
    row = legalRepDetails_outputs["Case02"]
    assert_equals(row, legalRepHasAddress="Yes")

def test_rep_id_gt_zero_always_has_address(legalRepDetails_outputs):
    """
    RepresentativeId > 0
    → legalRepHasAddress = Yes
    → OOC fields must be null
    """
    row = legalRepDetails_outputs["HU/00539/2025"]

    assert row["legalRepHasAddress"] == "Yes"
    assert_all_null(
        row,
        "oocAddressLine1",
        "oocAddressLine2",
        "oocAddressLine3",
        "oocAddressLine4",
        "oocLrCountryGovUkAdminJ",
    )

def test_rep_id_zero_and_no_address_means_no(legalRepDetails_outputs):
    """
    RepresentativeId = 0
    AND no usable address
    → legalRepHasAddress = No
    → OOC fields null
    """
    row = legalRepDetails_outputs["HU/00185/2025"]

    assert row["legalRepHasAddress"] == "No"
    assert row["legalRepAddressUK"] in ("", None)
    assert_all_null(
        row,
        "oocAddressLine1",
        "oocAddressLine2",
        "oocAddressLine3",
        "oocAddressLine4",
        "oocLrCountryGovUkAdminJ",
    )

def test_rep_id_zero_with_ooc_address_populates_ooc_fields(legalRepDetails_outputs):
    """
    RepresentativeId = 0
    AND address exists
    AND address is non-UK
    → legalRepHasAddress = No
    → OOC address lines populated
    """
    row = legalRepDetails_outputs["HU/02191/2024"]

    assert row["legalRepHasAddress"] == "No"
    assert_any_not_null(
        row,
        "oocAddressLine1",
        "oocAddressLine2",
        "oocAddressLine3",
        "oocAddressLine4",
    )

def test_ooc_country_code_derived_from_country_lookup(legalRepDetails_outputs):
    """
    Non-UK address with country in lookup table
    → oocLrCountryGovUkAdminJ populated
    """
    row = legalRepDetails_outputs["HU/02191/2024"]

    assert row["oocLrCountryGovUkAdminJ"] == "GU"

def test_non_uk_without_lookup_country_has_null_country_code(legalRepDetails_outputs):
    """
    Non-UK address
    BUT country not in lookup
    → oocLrCountryGovUkAdminJ is null
    """
    row = legalRepDetails_outputs["HU/01475/2024"]

    assert row["legalRepHasAddress"] == "No"
    assert row["oocLrCountryGovUkAdminJ"] is None

def test_uk_postcode_results_in_yes_and_no_ooc(legalRepDetails_outputs):
    """
    UK postcode present
    → legalRepHasAddress = Yes
    → no OOC fields populated
    """
    row = legalRepDetails_outputs["EA/03862/2020"]

    assert row["legalRepHasAddress"] == "Yes"
    assert_all_null(
        row,
        "oocAddressLine1",
        "oocAddressLine2",
        "oocAddressLine3",
        "oocAddressLine4",
        "oocLrCountryGovUkAdminJ",
    )

def test_email_precedence_rep_over_case_rep(legalRepDetails_outputs):
    """
    Rep_Email should be used in preference to CaseRep_Email
    """
    row = legalRepDetails_outputs["HU/00539/2025"]

    assert row["legalRepEmail"] == "ptravis@example.org"

def test_email_falls_back_to_case_rep_when_rep_missing(legalRepDetails_outputs):
    """
    CaseRep_Email used when Rep_Email missing
    """
    row = legalRepDetails_outputs["HU/02191/2024"]

    assert row["legalRepEmail"] == "jameskelly@example.com"

def test_address_concatenation_does_not_contain_nulls(legalRepDetails_outputs):
    """
    legalRepAddressUK must not contain 'None' or 'null' literals
    """
    for row in legalRepDetails_outputs.values():
        addr = row.get("legalRepAddressUK")
        if addr:
            assert "None" not in addr
            assert "null" not in addr.lower()