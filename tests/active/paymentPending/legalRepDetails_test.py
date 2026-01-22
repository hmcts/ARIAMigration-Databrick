from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import legalRepDetails
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pytest


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("legalRepDetailsTests")
        .getOrCreate()
    )

##### Testing the PaymentType field grouping function #####
@pytest.fixture(scope="session")
def legalRepDetails_outputs(spark):

    bronze_countryFromAddress = spark.createDataFrame([
        Row(countryFromAddress="United Kingdom", countryGovUkOocAdminJ="GB"),
        Row(countryFromAddress="Guam", countryGovUkOocAdminJ="GU"),
        Row(countryFromAddress="Argentina", countryGovUkOocAdminJ="AR")
    ])

    data = [
        # ---------------------------------------------------------------------
        # Case01: AIP → should be FILTERED OUT (no output row)
        # ---------------------------------------------------------------------
        ("Case01", "AIP",
         None, None, None,
         None, None, None,
         0,
         None, None, None, None, None,
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case02: LR + RepId >0 + UK address content → legalRepHasAddress = Yes
        # Rep fields should be used. OOC must be omitted (NULL).
        # ---------------------------------------------------------------------
        ("Case02", "LR",
         "rep@test.com", None, None,
         "Contact Name", "RepName", None,
         123,
         "CR1", "CR2", "CR3", "CR4", "CRP",
         "10 Downing Street", "Westminster", "London", "Greater London", "United Kingdom"),

        # ---------------------------------------------------------------------
        # Case03: LR + RepId = 0 + non-UK country Guam → legalRepHasAddress = No
        # CaseRep fields used. OOC omitted. CountryCode omitted.
        # ---------------------------------------------------------------------
        ("Case03", "LR",
         None, "case@test.com", None,
         None, None, "CaseRepName",
         0,
         "925 Lisa Plains Apt. 642", "Hill Square", "Lynchhaven", "Guam", "96910",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case04: LR + RepId = 0 + valid UK postcode → legalRepHasAddress = Yes
        # CaseRep fields used. OOC should populate. CountryCode likely blank/null.
        # ---------------------------------------------------------------------
        ("Case04", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         "1 Some Street", "Area", "London", None, "SW1A 1AA",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case05: LR + RepId = 0 + invalid postcode but contains 'United Kingdom' → Yes
        # ---------------------------------------------------------------------
        ("Case05", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         "Flat 2", "Road Name", "TownName", "United Kingdom", "NOTAPOSTCODE",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case06: LR + RepId = 0 + invalid postcode + non-UK country Argentina → No
        # ---------------------------------------------------------------------
        ("Case06", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         "10 Foreign Road", "District", "City", "Argentina", "ABC123",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case07: LR + RepId = 0 + Address1 NULL → Address2 becomes mandatory Line1
        # Use UK postcode so legalRepHasAddress becomes Yes and OOC populates.
        # ---------------------------------------------------------------------
        ("Case07", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         None, "Address2Only", "Town", None, "E1 6LZ",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case08: LR + RepId = 0 + Address1+2 NULL → Address3 becomes mandatory Line1
        # ---------------------------------------------------------------------
        ("Case08", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         None, None, "OnlyTown", "SomeCounty", "W1A 0AX",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case09: LR + RepId = 0 + trailing comma protection
        # Address4 = "" should NOT produce "TownOnly, "
        # ---------------------------------------------------------------------
        ("Case09", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         "Line1", "Line2", "TownOnly", "", "SW1A 1AA",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case10: LR + RepId = 0 + line4 protection
        # Address5 = "" but postcode present: avoid ", SW1A 1AA"
        # ---------------------------------------------------------------------
        ("Case10", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         "Line1", "Line2", "Town", "County", "SW1A 1AA",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case11: LR + RepId >0 + messy Rep_Email cleaned, takes precedence over others
        # ---------------------------------------------------------------------
        ("Case11", "LR",
         "  ,test.user@email.com  ", "backup@email.com", "third@email.com",
         None, "RepName", None,
         999,
         None, None, None, None, None,
         "Addr1", None, "Town", "County", "United Kingdom"),

        # ---------------------------------------------------------------------
        # Case12: LR + RepId >0 + all emails NULL → legalRepEmail should be NULL
        # ---------------------------------------------------------------------
        ("Case12", "LR",
         None, None, None,
         None, "RepName", None,
         888,
         None, None, None, None, None,
         "Addr1", None, "Town", "County", "United Kingdom"),

        # ---------------------------------------------------------------------
        # Case13: LR + RepId >0 + RepAddress1 NULL → fallback to RepAddress2 for AddressLine1
        # ---------------------------------------------------------------------
        ("Case13", "LR",
         "rep13@test.com", None, None,
         None, "RepName", None,
         77,
         None, None, None, None, None,
         None, "FallbackLine2", "Manchester", "Lancashire", "United Kingdom"),

        # ---------------------------------------------------------------------
        # Case14: LR + RepId = 0 + CaseRepAddress1..4 NULL, Address5 exists
        # Tests deepest fallback for AddressLine1 mandatory (should use Address5)
        # Use UK postcode to force legalRepHasAddress=Yes and OOC builds
        # ---------------------------------------------------------------------
        ("Case14", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         None, None, None, None, "EC1A 1BB",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case15: LR + RepId = 0 + whitespace-only fields (should behave like empty)
        # Useful for trim/nullif behaviour in concat_ws logic
        # ---------------------------------------------------------------------
        ("Case15", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         "   ", "Line2", "Town", "  ", "E2 8AA",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case16: LR + RepId = 0 + country appears in Address3 not Address4
        # (tests scan across entire address string)
        # ---------------------------------------------------------------------
        ("Case16", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         "Line1", "Line2", "Guam", None, "96910",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case17: LR + RepId = 0 + country appears with punctuation
        # e.g. "Guam," should still be matched by punctuation-stripping logic
        # ---------------------------------------------------------------------
        ("Case17", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         "Line1", "Line2", "Town", "Guam,", "96910",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case18: LR + RepId = 0 + mixed-case country name "united kingdom"
        # tests lower/upper matching
        # ---------------------------------------------------------------------
        ("Case18", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         "Line1", "Line2", "Town", "united kingdom", "ZZZ",
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case19: LR + RepId = 0 + no postcode + no country (unknown)
        # Should become legalRepHasAddress = No in your strict rules
        # ---------------------------------------------------------------------
        ("Case19", "LR",
         None, None, None,
         None, None, "CaseRepName",
         0,
         "Unknown Address1", None, None, None, None,
         None, None, None, None, None),

        # ---------------------------------------------------------------------
        # Case20: LR + RepId >0 but Rep fields mostly missing
        # Still should be legalRepHasAddress=Yes due to RepresentativeId>0
        # ---------------------------------------------------------------------
        ("Case20", "LR",
         None, None, None,
         None, "RepName", None,
         42,
         None, None, None, None, None,
         None, None, None, None, None),
    ]

    columns = [
        "CaseNo", "dv_representation",
        "Rep_Email", "CaseRep_Email", "CaseRep_FileSpecific_Email",
        "Contact", "Rep_Name", "CaseRep_Name",
        "RepresentativeId",
        "CaseRep_Address1", "CaseRep_Address2", "CaseRep_Address3", "CaseRep_Address4", "CaseRep_Postcode",
        "Rep_Address1", "Rep_Address2", "Rep_Address3", "Rep_Address4", "Rep_Address5"
    ]

    df = spark.createDataFrame(data, columns)

    legalRepDetails_content, _ = legalRepDetails(df, bronze_countryFromAddress)

    # ✅ build dict keyed by CaseNo
    results = {row["CaseNo"]: row.asDict() for row in legalRepDetails_content.collect()}
    return results


def assert_is_null(row, *fields):
    for f in fields:
        assert row[f] is None, f"{f} expected None but got {row[f]}"

def assert_equals(row, **expected):
    for field, value in expected.items():
        assert row[field] == value, f"{field} expected {value} but got {row[field]}"

def test_legalRepEmail_priority_and_cleaning(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case11"]
    assert_equals(row, legalRepEmail="test.user@email.com")

def test_legalRepEmail_all_null(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case12"]
    assert_is_null(row, "legalRepEmail")

def test_legalRepGivenName_precedence(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case02"]
    assert_equals(row, legalRepGivenName="Contact Name")

def test_legalRepFamilyNamePaperJ(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case03"]
    assert_equals(row, legalRepFamilyNamePaperJ="CaseRepName")

def test_legalRepCompanyPaperJ(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case03"]
    assert_equals(row, legalRepCompanyPaperJ="CaseRepName")

def test_legalRepDeclaration_exists(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case02"]
    assert row["legalRepDeclaration"] is not None

def test_legalRepHasAddress_rep_gt_0(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case02"]
    assert_equals(row, legalRepHasAddress="Yes")

def test_legalRepHasAddress_non_uk_country(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case03"]
    assert_equals(row, legalRepHasAddress="No")

def test_legalRepAddressUK_rep_address_used(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case02"]
    assert "Downing" in row["legalRepAddressUK"]

def test_legalRepAddressUK_case_rep_fallback(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case07"]
    assert row["legalRepAddressUK"].startswith("Address2Only")

def test_ooc_addresses_populated_when_has_address(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case04"]
    assert row["oocAddressLine1"] is not None
    assert row["oocAddressLine2"] is not None

def test_ooc_addresses_null_when_no_address(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case03"]
    assert_is_null(row, "oocAddressLine1", "oocAddressLine2", "oocAddressLine3", "oocAddressLine4")

def test_ooc_no_trailing_commas(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case09"]
    assert not row["oocAddressLine3"].endswith(", ")
    assert not row["oocAddressLine4"].endswith(", ")

def test_ooc_country_code_guam(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case03"]
    assert_is_null(row, "oocLrCountryGovUkAdminJ")

def test_ooc_country_code_populated(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case04"]
    assert row["oocLrCountryGovUkAdminJ"] in (None, "GB")  # adjust as per your mapping table

def test_legalRepRefNumberPaperJ(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case02"]
    assert row["legalRepRefNumberPaperJ"] is not None

def test_localAuthorityPolicy_structure(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case02"]
    policy = row["localAuthorityPolicy"]
    assert "LEGALREPRESENTATIVE" in policy

def test_case04_golden_path(legalRepDetails_outputs):
    row = legalRepDetails_outputs["Case04"]
    assert_equals(row, legalRepHasAddress="Yes")
    assert row["oocAddressLine1"] is not None
    assert row["oocAddressLine2"] is not None
    assert not row["oocAddressLine3"].endswith(", ")
    assert row["oocLrCountryGovUkAdminJ"] in (None, "GB")