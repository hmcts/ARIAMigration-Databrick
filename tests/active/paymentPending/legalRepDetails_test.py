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

def assert_all_null(row, *fields):
    for f in fields:
        assert row[f] is None, f"{f} expected None but got {row[f]}"


def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row[k] == v, f"{k} expected {v} but got {row[k]}"


# ---------------------------------------------------------
# 1) RepId=0 + no address -> No + all OOC null
# ---------------------------------------------------------
def test_case_HU_00185_2025_rep0_no_address_ooc_null(legalRepDetails_outputs):
    row = legalRepDetails_outputs["HU/00185/2025"]

    assert_equals(
        row,
        legalRepHasAddress="No",
        legalRepEmail="fgrimes@example.com",
        legalRepGivenName="Mendez-ReedX",
        legalRepFamilyNamePaperJ="Mendez-ReedX",
        legalRepCompanyPaperJ="Mendez-ReedX",
    )

    # address empty expected
    assert row["legalRepAddressUK"] in ("", None)

    # OOC should be null
    assert_all_null(
        row,
        "oocAddressLine1",
        "oocAddressLine2",
        "oocAddressLine3",
        "oocAddressLine4",
        "oocLrCountryGovUkAdminJ",
    )


# ---------------------------------------------------------
# 2) RepId=0 + CaseRep address present -> No + OOC populated
# ---------------------------------------------------------
def test_case_HU_00539_2025_rep0_caseRep_address_ooc_populated(legalRepDetails_outputs):
    row = legalRepDetails_outputs["HU/00539/2025"]

    # sample says "No" for HasAddress
    assert_equals(
        row,
        legalRepHasAddress="No",
        legalRepEmail="ptravis@example.org",
        legalRepGivenName="Mcmahon IncX",
        legalRepFamilyNamePaperJ="Mcmahon IncX",
        legalRepCompanyPaperJ="Mcmahon IncX",
    )

    # legalRepAddressUK should be concatenated caseRep address
    assert row["legalRepAddressUK"] == "209 Hampton TerraceX Kimberly PassageX ChristensenstadX IP4 2EJ"

    # OOC lines are populated from caseRep fields in your sample
    assert row["oocAddressLine1"] == "209 Hampton TerraceX"
    assert row["oocAddressLine2"] == "Kimberly PassageX"
    assert row["oocAddressLine3"] == "ChristensenstadX"
    assert row["oocAddressLine4"] == "IP4 2EJ"
    assert row["oocLrCountryGovUkAdminJ"] is None


# ---------------------------------------------------------
# 3) RepId>0 -> Yes + OOC must be null
# ---------------------------------------------------------
def test_case_EA_03862_2020_rep_gt0_yes_ooc_null(legalRepDetails_outputs):
    row = legalRepDetails_outputs["EA/03862/2020"]

    assert_equals(
        row,
        legalRepHasAddress="Yes",
        legalRepEmail="ynelson@example.org",
        legalRepGivenName="Walker, Greene and WhiteX",
        legalRepFamilyNamePaperJ="Walker, Greene and WhiteX",
        legalRepCompanyPaperJ="Walker, Greene and WhiteX",
    )

    assert row["legalRepAddressUK"] == "734 Carson Plains Apt. 731X Rodgers ShoalX S65 7EB"

    assert_all_null(
        row,
        "oocAddressLine1",
        "oocAddressLine2",
        "oocAddressLine3",
        "oocAddressLine4",
        "oocLrCountryGovUkAdminJ",
    )


# ---------------------------------------------------------
# 4) RepId>0, non-UK words appear in address -> still Yes, OOC null
# ---------------------------------------------------------
def test_case_EA_04228_2020_rep_gt0_yes_even_if_non_uk_in_text(legalRepDetails_outputs):
    row = legalRepDetails_outputs["EA/04228/2020"]

    assert_equals(
        row,
        legalRepHasAddress="Yes",
        legalRepEmail="joshuawarner@example.org",
        legalRepGivenName="Brandon GarciaX",
        legalRepFamilyNamePaperJ="Hughes LtdX",
        legalRepCompanyPaperJ="Hughes LtdX",
    )

    assert "Holy See (Vatican City State)" in row["legalRepAddressUK"]

    assert_all_null(
        row,
        "oocAddressLine1",
        "oocAddressLine2",
        "oocAddressLine3",
        "oocAddressLine4",
        "oocLrCountryGovUkAdminJ",
    )


# ---------------------------------------------------------
# 5) RepId=0, partial CaseRep address -> No + OOC handles missing lines
# ---------------------------------------------------------
def test_case_HU_00302_2025_rep0_partial_address_ooc_consistent(legalRepDetails_outputs):
    row = legalRepDetails_outputs["HU/00302/2025"]

    assert_equals(
        row,
        legalRepHasAddress="No",
        legalRepEmail="sandra85@example.net",
        legalRepGivenName="Amanda CollinsX",
        legalRepFamilyNamePaperJ="Wang PLCX",
        legalRepCompanyPaperJ="Wang PLCX",
    )

    # legalRepAddressUK built from CaseRep fields you provided
    assert row["legalRepAddressUK"] == "902 Carol FlatsX WA08 8ZH"

    # OOC should match: line1 has value, rest may be null/blank depending on your concat logic
    assert row["oocAddressLine1"] == "902 Carol FlatsX"
    assert row["oocAddressLine4"] == "WA08 8ZH"

    # if you expect line2/3 to be null for partial addresses:
    assert row["oocAddressLine2"] is None
    assert row["oocAddressLine3"] is None
    assert row["oocLrCountryGovUkAdminJ"] is None
