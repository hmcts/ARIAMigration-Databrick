import pytest
from pyspark.sql import SparkSession, types as T, Row
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
        # HU/00185/2025
        (
            "HU/00185/2025",
            "LR",          # dv_representation
            "HU",          # lu_appealType
            None,          # Rep_Email
            "fgrimes@example.com",     # CaseRep_Email
            None,          # CaseRep_FileSpecific_Email
            None,          # Contact
            None,          # Rep_Name
            "Mendez-ReedX",# CaseRep_Name
            0,             # RepresentativeId
            None, None, None, None, None, None,   # CaseRep address fields
            None, None, None, None, None, None    # Rep address fields
        ),

        # HU/01475/2024
        (
            "HU/01475/2024",
            "LR",
            "HU",
            None,
            "rachel34@example.org",
            "acarter@example.org",
            None,
            None,
            "Barnes-VargasX",
            0,
            "56292 Raymond TrafficwayX",
            "Cheyenne SquareX",
            "New MollyX",
            "ReunionX",
            "MVX",
            None,          # CaseRep_Postcode
            None, None, None, None, None, None
        ),

        # HU/02191/2024
        (
            "HU/02191/2024",
            "LR",
            "HU",
            None,
            "jameskelly@example.com",
            "angelabryant@example.net",
            "Douglas GutierrezX",   # Contact
            None,
            "Leon LLCX",            # CaseRep_Name
            0,
            "925 Lisa Plains Apt. 642X",
            "Hill SquareX",
            "LynchhavenX",
            "GuamX",
            "NLX",
            None,                   # CaseRep_Postcode
            None, None, None, None, None, None
        ),

        # HU/02151/2024
        (
            "HU/02151/2024",
            "LR",
            "HU",
            None,
            "wstevens@example.org",
            None,
            None,
            None,
            "Black, Moore and BurkeX",
            0,
            None, None, None, None, None, None,
            None, None, None, None, None, None
        ),

        # HU/00539/2025
        (
            "HU/00539/2025",
            "LR",
            "HU",
            None,
            "ptravis@example.org",
            "greg20@example.com",
            None,
            None,
            "Mcmahon IncX",
            0,
            "209 Hampton TerraceX",
            "Kimberly PassageX",
            "ChristensenstadX",
            None,
            None,
            "IP4 2EJ",   # CaseRep_Postcode
            None, None, None, None, None, None
        ),

        # EA/03862/2020
        (
            "EA/03862/2020",
            "LR",
            "EA",
            "ynelson@example.org",
            None,
            "jacobsgregory@example.com",
            None,
            "Walker, Greene and WhiteX",   # Rep_Name
            None,
            1674,
            None, None, None, None, None, None,
            "734 Carson Plains Apt. 731X",
            "Rodgers ShoalX",
            None,
            None,
            None,
            "S65 7EB"
        ),
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
        assert row.get(f) is None, f"{f} expected None but got {row.get(f)}"


def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row.get(k) == v, f"{k} expected {v} but got {row.get(k)}"

def test_HU_00185_2025_no_address_null(legalRepDetails_outputs):
    row = legalRepDetails_outputs["HU/00185/2025"]

    assert_equals(
        row,
        legalRepGivenName="Mendez-ReedX",
        legalRepFamilyNamePaperJ="Mendez-ReedX",
        legalRepCompanyPaperJ="Mendez-ReedX",
        legalRepEmail="fgrimes@example.com",
        legalRepHasAddress="No",
    )

    assert row["legalRepAddressUK"]["AddressLine1"] in ("", None)
    assert row["legalRepAddressUK"]["AddressLine2"] in ("", None)
    assert row["legalRepAddressUK"]["PostTown"] in ("", None)
    assert row["legalRepAddressUK"]["County"] in ("", None)
    assert row["legalRepAddressUK"]["Country"] in ("", None)
    assert row["legalRepAddressUK"]["PostCode"] in ("", None)

    assert_all_null(
        row,
        "oocAddressLine1",
        "oocAddressLine2",
        "oocAddressLine3",
        "oocAddressLine4",
        "oocLrCountryGovUkAdminJ",
    )

def test_HU_01475_2024_caseRep_address_ooc(legalRepDetails_outputs):
    row = legalRepDetails_outputs["HU/01475/2024"]
    
    expectedLegalRepAddressUkRow = Row(
        AddressLine1='56292 Raymond TrafficwayX',
        AddressLine2='Cheyenne SquareX',
        PostTown='New MollyX',
        County='ReunionX',
        Country='MVX',
        PostCode=None
    )

    assert_equals(
        row,
        legalRepGivenName="Barnes-VargasX",
        legalRepFamilyNamePaperJ="Barnes-VargasX",
        legalRepCompanyPaperJ="Barnes-VargasX",
        legalRepEmail="rachel34@example.org",
        legalRepHasAddress="No",
        legalRepAddressUK=expectedLegalRepAddressUkRow,
    )

    assert_equals(
        row,
        oocAddressLine1="56292 Raymond TrafficwayX",
        oocAddressLine2="Cheyenne SquareX",
        oocAddressLine3="New MollyX, ReunionX",
        oocAddressLine4="MVX",
    )

    assert row["oocLrCountryGovUkAdminJ"] is None


def test_HU_02191_2024_caseRep_nonUK_country(legalRepDetails_outputs):
    row = legalRepDetails_outputs["HU/02191/2024"]

    expectedLegalRepAddressUkRow = Row(
        AddressLine1='925 Lisa Plains Apt. 642X',
        AddressLine2='Hill SquareX',
        PostTown='LynchhavenX',
        County='GuamX',
        Country='NLX',
        PostCode=None
    )

    assert_equals(
        row,
        legalRepGivenName="Douglas GutierrezX",
        legalRepFamilyNamePaperJ="Leon LLCX",
        legalRepCompanyPaperJ="Leon LLCX",
        legalRepEmail="jameskelly@example.com",
        legalRepHasAddress="No",
        legalRepAddressUK=expectedLegalRepAddressUkRow,
    )

    assert_equals(
        row,
        oocAddressLine1="925 Lisa Plains Apt. 642X",
        oocAddressLine2="Hill SquareX",
        oocAddressLine3="LynchhavenX, GuamX",
        oocAddressLine4="NLX",
        oocLrCountryGovUkAdminJ="GU",
    )


def test_HU_02151_2024_no_address(legalRepDetails_outputs):
    row = legalRepDetails_outputs["HU/02151/2024"]

    assert_equals(
        row,
        legalRepGivenName="Black, Moore and BurkeX",
        legalRepFamilyNamePaperJ="Black, Moore and BurkeX",
        legalRepCompanyPaperJ="Black, Moore and BurkeX",
        legalRepEmail="wstevens@example.org",
        legalRepHasAddress="No",
    )

    assert row["legalRepAddressUK"]["AddressLine1"] in ("", None)
    assert row["legalRepAddressUK"]["AddressLine2"] in ("", None)
    assert row["legalRepAddressUK"]["PostTown"] in ("", None)
    assert row["legalRepAddressUK"]["County"] in ("", None)
    assert row["legalRepAddressUK"]["Country"] in ("", None)
    assert row["legalRepAddressUK"]["PostCode"] in ("", None)

    assert_all_null(
        row,
        "oocAddressLine1",
        "oocAddressLine2",
        "oocAddressLine3",
        "oocAddressLine4",
        "oocLrCountryGovUkAdminJ",
    )


def test_HU_00539_2025_yes_even_repId_0(legalRepDetails_outputs):
    row = legalRepDetails_outputs["HU/00539/2025"]

    expectedLegalRepAddressUkRow = Row(
        AddressLine1='209 Hampton TerraceX',
        AddressLine2='Kimberly PassageX',
        PostTown='ChristensenstadX',
        County=None,
        Country=None,
        PostCode='IP4 2EJ'
    )

    assert_equals(
        row,
        legalRepGivenName="Mcmahon IncX",
        legalRepFamilyNamePaperJ="Mcmahon IncX",
        legalRepCompanyPaperJ="Mcmahon IncX",
        legalRepEmail="ptravis@example.org",
        legalRepHasAddress="Yes",
        legalRepAddressUK=expectedLegalRepAddressUkRow,
    )

    assert_all_null(
        row,
        "oocAddressLine1",
        "oocAddressLine2",
        "oocAddressLine3",
        "oocAddressLine4",
        "oocLrCountryGovUkAdminJ",
    )


def test_EA_03862_2020_rep_address_yes(legalRepDetails_outputs):
    row = legalRepDetails_outputs["EA/03862/2020"]

    expectedLegalRepAddressUkRow = Row(
        AddressLine1='734 Carson Plains Apt. 731X',
        AddressLine2='Rodgers ShoalX',
        PostTown=None,
        County=None,
        Country=None,
        PostCode='S65 7EB'
    )

    assert_equals(
        row,
        legalRepGivenName="Walker, Greene and WhiteX",
        legalRepFamilyNamePaperJ="Walker, Greene and WhiteX",
        legalRepCompanyPaperJ="Walker, Greene and WhiteX",
        legalRepEmail="ynelson@example.org",
        legalRepHasAddress="Yes",
        legalRepAddressUK=expectedLegalRepAddressUkRow,
    )

    assert_all_null(
        row,
        "oocAddressLine1",
        "oocAddressLine2",
        "oocAddressLine3",
        "oocAddressLine4",
        "oocLrCountryGovUkAdminJ",
    )
