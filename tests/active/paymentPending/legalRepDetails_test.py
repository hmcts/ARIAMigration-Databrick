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
        # --------------------------------------------------
        # RepId = 0, no CaseRep address → No, everything null
        # --------------------------------------------------
        ("HU/00185/2025", "LR", "FTPA",
        "Mendez-ReedX", "Mendez-ReedX", None,
        0,
        "fgrimes@example.com", None, None,
        None, None, None, None, None, None,
        None, None, None, None, None, None),

        # --------------------------------------------------
        # RepId = 0, CaseRep address → No, OOC populated
        # --------------------------------------------------
        ("HU/00539/2025", "LR", "FTPA",
        "Mcmahon IncX", "Mcmahon IncX", None,
        0,
        "ptravis@example.org", None, None,
        "209 Hampton TerraceX", "Kimberly PassageX", "ChristensenstadX", None, None, "IP4 2EJ",
        None, None, None, None, None, None),

        # --------------------------------------------------
        # RepId > 0, Rep address → Yes, OOC null
        # --------------------------------------------------
        ("EA/03862/2020", "LR", "FTPA",
        "Walker, Greene and WhiteX", "Walker, Greene and WhiteX", None,
        1674,
        "ynelson@example.org", None, None,
        None, None, None, None, None, None,
        "734 Carson Plains Apt. 731X", "Rodgers ShoalX", None, None, None, "S65 7EB"),

        # --------------------------------------------------
        # RepId > 0, non-UK text inside Rep address → still Yes
        # --------------------------------------------------
        ("EA/04228/2020", "LR", "FTPA",
        "Brandon GarciaX", "Hughes LtdX", None,
        2068,
        "joshuawarner@example.org", None, None,
        None, None, None, None, None, None,
        "040 Teresa Estates Suite 621X", "Vargas ManorsX", "Port GeorgeX",
        "Holy See (Vatican City State)X", "PKX", "LS16 7JS"),

        # --------------------------------------------------
        # RepId = 0, partial CaseRep address → No, partial OOC
        # --------------------------------------------------
        ("HU/00302/2025", "LR", "FTPA",
        "Amanda CollinsX", "Wang PLCX", None,
        0,
        "sandra85@example.net", None, None,
        "902 Carol FlatsX", None, None, None, None, "WA08 8ZH",
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

def assert_all_null(row, *fields):
    for f in fields:
        assert row[f] is None, f"{f} expected None but got {row[f]}"


def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row[k] == v, f"{k} expected {v} but got {row[k]}"


# ---------------------------------------------------------
# 1) RepId=0 + no address -> No + all OOC null
# ---------------------------------------------------------
def test_HU_00185_rep0_no_address(legalRepDetails_outputs):
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

# ---------------------------------------------------------
# 2) RepId=0 + CaseRep address present -> No + OOC populated
# ---------------------------------------------------------
def test_HU_00539_rep0_caseRep_address(legalRepDetails_outputs):
    row = legalRepDetails_outputs["HU/00539/2025"]

    assert row["legalRepHasAddress"] == "No"
    assert row["legalRepAddressUK"] == (
        "209 Hampton TerraceX Kimberly PassageX ChristensenstadX IP4 2EJ"
    )

    assert row["oocAddressLine1"] == "209 Hampton TerraceX"
    assert row["oocAddressLine2"] == "Kimberly PassageX"
    assert row["oocAddressLine3"] == "ChristensenstadX"
    assert row["oocAddressLine4"] == "IP4 2EJ"

# ---------------------------------------------------------
# 3) RepId>0 -> Yes + OOC must be null
# ---------------------------------------------------------
def test_EA_03862_rep_gt0_yes(legalRepDetails_outputs):
    row = legalRepDetails_outputs["EA/03862/2020"]

    assert row["legalRepHasAddress"] == "Yes"
    assert row["legalRepAddressUK"] == (
        "734 Carson Plains Apt. 731X Rodgers ShoalX S65 7EB"
    )

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
def test_EA_04228_rep_gt0_non_uk_text(legalRepDetails_outputs):
    row = legalRepDetails_outputs["EA/04228/2020"]

    assert row["legalRepHasAddress"] == "Yes"
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
def test_HU_00302_rep0_partial_caseRep_address(legalRepDetails_outputs):
    row = legalRepDetails_outputs["HU/00302/2025"]

    assert row["legalRepHasAddress"] == "No"
    assert row["legalRepAddressUK"] == "902 Carol FlatsX WA08 8ZH"

    assert row["oocAddressLine1"] == "902 Carol FlatsX"
    assert row["oocAddressLine4"] == "WA08 8ZH"
    assert row["oocAddressLine2"] is None
    assert row["oocAddressLine3"] is None
