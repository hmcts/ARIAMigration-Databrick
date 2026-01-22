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
        # Case01: AIP → filtered out
        ("Case01", "AIP", None, None, None, None, None, None, None, 0,
        None, None, None, None, None, None, None, None, None, None, None, None),

        # Case02: LR + RepId>0 → legalRepHasAddress=Yes, Rep fields used (UK)
        ("Case02", "LR", "FTPA", "rep@test.com", None, None, "Contact Name", "RepName", None, 123,
        "CR1", "CR2", "CR3", "CR4", "CRP", "SW1A 1AA",
        "10 Downing Street", "Westminster", "London", "Greater London", "United Kingdom", "SW1A 1AA"),

        # Case03: LR + RepId=0 + non-UK → legalRepHasAddress=No
        ("Case03", "LR", "FTPA", None, "case@test.com", None, None, None, "CaseRepName", 0,
        "925 Lisa Plains Apt. 642", "Hill Square", "Lynchhaven", "Guam", "96910",
        None, None, None, None, None, None, None),  

        # Case04: LR + RepId=0 + valid UK postcode → legalRepHasAddress=Yes
        ("Case04", "LR", "FTPA", None, None, None, None, None, "CaseRepName", 0,
        "1 Some Street", "Area", "London", None, "SW1A 1AA",
        None, None, None, None, None, None, None),  

        # Case05: LR + RepId=0 + invalid postcode but country in address → legalRepHasAddress=Yes
        ("Case05", "LR", "FTPA", None, None, None, None, None, "CaseRepName", 0,
        "Flat 2", "Road Name", "TownName", "United Kingdom", "NOTAPOSTCODE",
        None, None, None, None, None, None, None),

        # Case06: LR + RepId=0 + UK country, missing postcode → should still be Yes
        ("Case06", "LR", "FTPA", None, None, None, None, None, "CaseRepName", 0,
        "10 Baker Street", None, None, None, None, None,
        None, None, None, None, None, None),

        # Case07: LR + RepId>0 + non-UK Rep → legalRepHasAddress=Yes, Rep fields used
        ("Case07", "LR", "FTPA", "nonuk@test.com", None, None, None, "RepNonUK", None, 5,
        None, None, None, None, None, None,
        "123 Elm St", "Toronto", "Ontario", "Canada", None, "M1A 2B3"),

        # Case08: LR + RepId=0 + all addresses missing → legalRepHasAddress=No
        ("Case08", "LR", "FTPA", None, None, None, None, None, None, 0,
        None, None, None, None, None, None,
        None, None, None, None, None, None),
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
    """Case02: LR + RepId>0 → legalRepHasAddress should be Yes"""
    row = legalRepDetails_outputs["Case02"]
    assert_equals(row, legalRepHasAddress="Yes")
    assert_equals(row, legalRepEmail="rep@test.com")
    assert_equals(row, legalRepAddressUK="10 Downing Street Westminster London Greater London United Kingdom")

def test_legalRepHasAddress_case03(legalRepDetails_outputs):
    """Case03: LR + RepId=0 + non-UK → legalRepHasAddress should be No, OOC fields None"""
    row = legalRepDetails_outputs["Case03"]
    assert_equals(row, legalRepHasAddress="No")
    assert_is_null(row, "oocAddressLine1", "oocAddressLine2", "oocAddressLine3", "oocAddressLine4", "oocLrCountryGovUkAdminJ")

def test_legalRepHasAddress_case04(legalRepDetails_outputs):
    """Case04: LR + RepId=0 + UK postcode → legalRepHasAddress should be Yes"""
    row = legalRepDetails_outputs["Case04"]
    assert_equals(row, legalRepHasAddress="Yes")
    assert_equals(row, legalRepCompanyPaperJ="CaseRepName")

def test_legalRepHasAddress_case05(legalRepDetails_outputs):
    """Case05: LR + RepId=0 + invalid postcode but UK country → legalRepHasAddress should be Yes"""
    row = legalRepDetails_outputs["Case05"]
    assert_equals(row, legalRepHasAddress="Yes")
    assert_equals(row, legalRepCompanyPaperJ="CaseRepName")

def test_legalRepHasAddress_case06(legalRepDetails_outputs):
    """Case06: LR + RepId=0 + UK country, missing postcode → legalRepHasAddress should be Yes"""
    row = legalRepDetails_outputs["Case06"]
    assert_equals(row, legalRepHasAddress="Yes")
    assert_equals(row, legalRepCompanyPaperJ="CaseRepName")

def test_legalRepHasAddress_case07(legalRepDetails_outputs):
    """Case07: LR + RepId>0 + non-UK Rep → legalRepHasAddress should be Yes, use Rep fields"""
    row = legalRepDetails_outputs["Case07"]
    assert_equals(row, legalRepHasAddress="Yes")
    assert_equals(row, legalRepEmail="nonuk@test.com")
    assert_equals(row, legalRepAddressUK="123 Elm St Toronto Ontario Canada M1A 2B3")

def test_legalRepHasAddress_case08(legalRepDetails_outputs):
    """Case08: LR + RepId=0 + all addresses missing → legalRepHasAddress should be No"""
    row = legalRepDetails_outputs["Case08"]
    assert_equals(row, legalRepHasAddress="No")
    assert_is_null(row, "oocAddressLine1", "oocAddressLine2", "oocAddressLine3", "oocAddressLine4", "oocLrCountryGovUkAdminJ")

