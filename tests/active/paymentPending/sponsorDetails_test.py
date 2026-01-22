import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import sponsorDetails

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("sponsorDetailsTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def sponsorDetails_outputs(spark):

    # -------------------------------
    # 1. Define schema for silver_m1 (sponsor table)
    # -------------------------------
    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("hasSponsor", T.StringType(), True),
        T.StructField("sponsorGivenNames", T.StringType(), True),
        T.StructField("sponsorFamilyName", T.StringType(), True),
        T.StructField("sponsorAddress", T.StringType(), True),
        T.StructField("categoryIdList", T.ArrayType(T.IntegerType()), True),
        T.StructField("Sponsor_Name", T.StringType(), True),
        T.StructField("Sponsor_Forenames", T.StringType(), True),
        T.StructField("Sponsor_Email", T.StringType(), True),
        T.StructField("Sponsor_Telephone", T.StringType(), True),
        T.StructField("Sponsor_Authorisation", T.StringType(), True),
        T.StructField("sponsorEmailAdminJ", T.StringType(), True),
        T.StructField("sponsorMobileNumberAdminJ", T.StringType(), True),
        T.StructField("sponsorAuthorisation", T.StringType(), True),
        T.StructField("sponsor_Address1", T.StringType(), True),
        T.StructField("sponsor_Address2", T.StringType(), True),
        T.StructField("sponsor_Address3", T.StringType(), True),
        T.StructField("sponsor_Address4", T.StringType(), True),
        T.StructField("sponsor_Address5", T.StringType(), True),
        T.StructField("sponsor_Postcode", T.StringType(), True),
    ])

    # -------------------------------
    # 2. Populate m1_data based on your CSV
    # -------------------------------
    m1_data = [
    # --------------------------------------------------
    # EA/00490/2025 → No sponsor (category 38 missing)
    # --------------------------------------------------
    ("EA/00490/2025", None, None, None, None, [47,38,11,3],
     None, None, None, None, None, None, None, None,
     None, None, None, None, None, None),

    # --------------------------------------------------
    # EA/00072/2025 → Has sponsor, full address, authorisation Yes
    # --------------------------------------------------
    ("EA/00072/2025", None, None, None, None, [47,38,11],
     "OwenX", "HarryX", "matthewthompson@example.org", "01314960380", True,
     None, None, None,
     "873 Hurst Parkways Suite 226X", "Owens StravenueX", None, None, None, "L7T 6AE"),

    # --------------------------------------------------
    # HU/00447/2025 → Has sponsor, no telephone, authorisation True
    # --------------------------------------------------
    ("HU/00447/2025", None, None, None, None, [38,10],
     "FisherX", "TashaX", "meghanmitchell@example.org", None, True,
     None, None, None,
     "64644 Michael Junction Suite 48X", "Kathryn MeadowX", None, None, None, "G30 7NJ"),

    # --------------------------------------------------
    # EA/00061/2025 → Missing categoryId 38 → sponsor omitted
    # --------------------------------------------------
    ("EA/00061/2025", None, None, None, None, [47,37,13,11,3],
     "UnknownX", "UnknownX", "unknown@example.org", None, None,
     None, None, None,
     None, None, None, None, None, None),

    # --------------------------------------------------
    # HU/00574/2023 → Has sponsor, multiple addresses, partial telephone
    # --------------------------------------------------
    ("HU/00574/2023", None, None, None, None, [38,10],
     "LopezX", "BruceX", None, "0141 496 0600", True,
     None, None, None,
     "358 Carter Corners Suite 044X", "Wells FordX", None, None, None, "UB4B 0LY"),
    ]

    # -------------------------------
    # 3. Define schema and data for silver_c (category table)
    # -------------------------------
    silver_c_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("CategoryId", T.IntegerType(), True)
    ])

    silver_c_data = [
        ("EA/00490/2025", 3), ("EA/00490/2025", 11), ("EA/00490/2025", 47),
        ("EA/00072/2025", 11), ("EA/00072/2025", 38), ("EA/00072/2025", 47),
        ("EA/00061/2025", 3), ("EA/00061/2025", 11), ("EA/00061/2025", 13),
        ("EA/00061/2025", 37), ("EA/00061/2025", 47),
        ("HU/00447/2025", 38), ("HU/00447/2025", 10),
        ("HU/00574/2023", 10), ("HU/00574/2023", 38),
    ]

    # -------------------------------
    # 4. Create DataFrames
    # -------------------------------
    silver_m1_df = spark.createDataFrame(m1_data, m1_schema)
    silver_c_df = spark.createDataFrame(silver_c_data, silver_c_schema)

    # -------------------------------
    # 5. Call the function under test
    # -------------------------------
    sponsorDetails_content, _ = sponsorDetails(silver_m1_df, silver_c_df)

    # -------------------------------
    # 6. Convert to dict keyed by CaseNo
    # -------------------------------
    results = {row["CaseNo"]: row.asDict() for row in sponsorDetails_content.collect()}
    return results


def assert_all_null(row, *fields):
    for f in fields:
        assert row.get(f) is None, f"{f} expected None but got {row.get(f)}"


def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row.get(k) == v, f"{k} expected {v} but got {row.get(k)}"

def test_EA_00490_2025_no_sponsor_paymentPending(sponsorDetails_outputs):
    """Case EA/00490/2025 has hasSponsor='No' → all sponsor fields should be null"""
    res = sponsorDetails_outputs["EA/00490/2025"]
    assert res["Sponsor_Name"] is None
    assert res["Sponsor_Forenames"] is None
    assert res["Sponsor_Email"] is None
    assert res["Sponsor_Telephone"] is None
    assert res["Sponsor_Authorisation"] is None

def test_EA_00072_2025_has_sponsor_paymentPending(sponsorDetails_outputs):
    """Case EA/00072/2025 has hasSponsor='Yes' and matching CategoryIds → sponsor fields populated"""
    res = sponsorDetails_outputs["EA/00072/2025"]
    assert res["Sponsor_Name"] == "OwenX"
    assert res["Sponsor_Forenames"] == "HarryX"
    assert res["Sponsor_Email"] == "matthewthompson@example.org"
    assert res["Sponsor_Telephone"] == "01314960380"
    assert res["Sponsor_Authorisation"] == "Yes"

def test_HU_00447_2025_has_sponsor_paymentPending(sponsorDetails_outputs):
    """Case HU/00447/2025 has hasSponsor='Yes' → sponsor fields populated"""
    res = sponsorDetails_outputs["HU/00447/2025"]
    assert res["Sponsor_Name"] == "FisherX"
    assert res["Sponsor_Forenames"] == "TashaX"
    assert res["Sponsor_Email"] == "meghanmitchell@example.org"
    assert res["Sponsor_Telephone"] is None
    assert res["Sponsor_Authorisation"] == "Yes"

def test_EA_00061_2025_missing_categoryIds(sponsorDetails_outputs):
    """Case EA/00061/2025 has no matching CategoryId in silver_c → all sponsor fields should be null"""
    res = sponsorDetails_outputs["EA/00061/2025"]
    assert res["Sponsor_Name"] is None
    assert res["Sponsor_Forenames"] is None
    assert res["Sponsor_Email"] is None
    assert res["Sponsor_Telephone"] is None
    assert res["Sponsor_Authorisation"] is None

def test_HU_00574_2023_has_sponsor_paymentPending(sponsorDetails_outputs):
    """Case HU/00574/2023 has hasSponsor='Yes' and matching CategoryIds → sponsor fields populated"""
    res = sponsorDetails_outputs["HU/00574/2023"]
    assert res["Sponsor_Name"] == "LopezX"
    assert res["Sponsor_Forenames"] == "BruceX"
    assert res["Sponsor_Email"] is None
    assert res["Sponsor_Telephone"] == "0141 496 0600"
    assert res["Sponsor_Authorisation"] == "Yes"

def test_sponsorAddress_present_when_category38(sponsorDetails_outputs):
    # EA/00072/2025 has category 38 → sponsorAddress should be populated
    res = sponsorDetails_outputs["EA/00072/2025"]
    assert res["sponsorAddress"] is not None
    assert "873 Hurst Parkways" in res["sponsorAddress"]
    assert "Owens StravenueX" in res["sponsorAddress"]

    # HU/00447/2025 has category 38 → sponsorAddress should be populated
    res = sponsorDetails_outputs["HU/00447/2025"]
    assert res["sponsorAddress"] is not None
    assert "64644 Michael Junction" in res["sponsorAddress"]

def test_sponsorAddress_none_when_no_category38(sponsorDetails_outputs):
    # EA/00490/2025 missing category 38 → sponsorAddress should be None
    res = sponsorDetails_outputs["EA/00490/2025"]
    assert res["sponsorAddress"] is None

    # EA/00061/2025 missing category 38 → sponsorAddress should be None
    res = sponsorDetails_outputs["EA/00061/2025"]
    assert res["sponsorAddress"] is None

def test_sponsorAddress_multiple_lines(sponsorDetails_outputs):
    # HU/00574/2023 has category 38 → multiple address parts
    res = sponsorDetails_outputs["HU/00574/2023"]
    address = res["sponsorAddress"]
    assert address is not None
    # Check that the first part and postcode are included
    assert "358 Carter Corners" in address
    assert "UB4B 0LY" in address
