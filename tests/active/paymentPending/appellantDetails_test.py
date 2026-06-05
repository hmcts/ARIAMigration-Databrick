import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import appellantDetails

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("appellantDetaildTests")
        .getOrCreate()
    )

def normalise_rows(row_list):
    """Convert list of PySpark Row objects to list of dicts recursively"""
    def row_to_dict(r):
        if isinstance(r, list):
            return [row_to_dict(x) for x in r]
        elif isinstance(r, tuple) or isinstance(r, T.Row):
            return {k: row_to_dict(v) for k, v in r.asDict().items()}
        else:
            return r
    return row_to_dict(row_list)

@pytest.fixture(scope="session")
def appellantDetails_outputs(spark):

    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_CCDAppealType", T.StringType(), True),
        T.StructField("DateLodged", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("BirthDate", T.StringType(), True),
        T.StructField("NationalityId", T.StringType(), True),
        T.StructField("lu_countryCode", T.StringType(), True),
        T.StructField("lu_appellantNationalitiesDescription", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
        T.StructField("DeportationDate", T.StringType(), True),
        T.StructField("RemovalDate", T.StringType(), True),
        T.StructField("HORef", T.StringType(), True),
        T.StructField("CasePrefix", T.StringType(), True),
    ])

    m1_data = [
        ("HU/00487/2025", "HU", "2025-03-07", "LR", "1961-05-18", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/00365/2025", "HU", "2024-11-06", "LR", "2017-05-06", "27", "BI", "Burundi", "euSettlementScheme", None, None, None, "HU"),
        ("EA/03208/2023", "EA", "2023-09-15", "AIP", "1995-07-23", "63", "GR", "Greece", "refusalOfEu", None, None, None, "EA"),
        ("EA/01698/2024", "EA", "2024-07-31", "AIP", "2000-04-28", "94", "LR", "Liberia", "euSettlementScheme", None, None, "T1113940", "EA"),
        ("HU/00560/2025", "HU", "2025-03-31", "LR", "1950-11-06", "201", "NO MAPPING REQUIRED", "NO MAPPING REQUIRED", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/00532/2025", "HU", "2025-03-24", "LR", "1983-08-08", "169", "TR", "Turkey", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/00423/2025", "HU", "2025-02-21", "LR", "1936-05-07", "179", "VE", "Venezuela (Bolivarian Republic of)", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/00001/2025", "HU", "2025-02-21", "LR", "1936-05-07", "179", "UK", "British", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/00002/2025", "HU", "2025-02-21", "LR", "1936-05-07", "179", "UK", "British", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/00003/2025", "HU", "2025-02-21", "LR", "1936-05-07", "179", "UK", "British", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/00004/2025", "HU", "2025-02-21", "LR", "1936-05-07", "179", "UK", "British", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/00005/2025", "HU", "2025-02-21", "LR", "1936-05-07", "211", "UK", "British", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/00006/2025", "HU", "2025-04-01", "LR", "1975-03-15", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/00007/2025", "HU", "2025-04-01", "LR", "1975-03-15", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/00008/2025", "HU", "2025-04-01", "LR", "1975-03-15", "211", "ZZ", "Stateless", "refusalOfHumanRights", None, None, None, "HU"),
    ]

    m2_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("Appellant_Name", T.StringType(), True),
        T.StructField("Appellant_Forenames", T.StringType(), True),
        T.StructField("Appellant_Email", T.StringType(), True),
        T.StructField("Appellant_Telephone", T.StringType(), True),
        T.StructField("Appellant_Address1", T.StringType(), True),
        T.StructField("Appellant_Address2", T.StringType(), True),
        T.StructField("Appellant_Address3", T.StringType(), True),
        T.StructField("Appellant_Address4", T.StringType(), True),
        T.StructField("Appellant_Address5", T.StringType(), True),
        T.StructField("Appellant_Postcode", T.StringType(), True),
        T.StructField("AppellantCountryId", T.IntegerType(), True),
        T.StructField("Relationship", T.StringType(), True),
        T.StructField("lu_countryGovUkOocAdminJ", T.StringType(), True),
        T.StructField("FCONumber", T.StringType(), True),
        T.StructField("Detained", T.StringType(), True)
    ])

    m2_data = [
        ("HU/00487/2025", "RobinsonX", "AdamX", None, None,
        "7759 Rios SquareX", "Paul WalksX", "KristinfurtX", "Trinidad and TobagoX", None, "W3 8PF",
        None, None, None, None, 0),

        ("HU/00365/2025", "SandersX", "AmandaX", "smithjohn@example.net", None,
        "4280 Michael Highway Suite 815X", "Stephanie AlleyX", "Port DanielX", "GibraltarX", None, "DD3 1HW",
        None, None, "GI", "XXXXXXX", 0),

        ("EA/03208/2023", "PachecoX", "KiaraX", "chelsea42@example.net", None,
        "7706 Barbara Gateway Apt. 725X", "Daniel BurgsX", "North JillportX", None, None, "LS3M 4BX",
        None, None, None, None, 0),

        ("EA/01698/2024", "ColemanX", "AlyssaX", "betty23@example.net", None,
        "06382 Bryan MountX", "Kimberly ThroughwayX", "ZacharyburghX", None, None, "B37 5LW",
        None, None, None, "T1113940", 0),

        ("HU/00560/2025", "MccallX", "ThomasX", None, None,
        None, None, None, None, None, None,
        None, None, None, None, 0),

        ("HU/00532/2025", "AlvarezX", "JasmineX", None, None,
        None, None, None, None, None, None,
        None, None, None, None, 0),

        ("HU/00423/2025", "WilliamsX", "SarahX", None, None,
        None, None, None, None, None, None,
        None, None, None, None, 0),

        ("HU/00001/2025", "WilliamsX", "SarahX", None, None,
        None, None, None, None, None, None,
        None, None, None, None, 1),

        ("HU/00002/2025", "WilliamsX", "SarahX", None, None,
        None, None, None, None, None, None,
        None, None, None, None, 2),

        ("HU/00003/2025", "WilliamsX", "SarahX", None, None,
        None, None, None, None, None, None,
        None, None, None, None, 4),

        ("HU/00004/2025", "WilliamsX", "SarahX", None, None,
        None, None, None, None, "UK", None,
        None, None, None, None, 0),

        ("HU/00005/2025", "WilliamsX", "SarahX", None, None,
        None, None, None, None, "NotUK", None,
        None, None, None, None, 0),

        # Marshall Islands (MH) — was previously mapped to NO MAPPING REQUIRED → NULL, now passes through as "MH"
        ("HU/00006/2025", "TestNameX", "TestGivenX", None, None,
        None, None, None, None, None, None,
        None, None, "MH", None, 0),

        # Monaco (MC) — was missing from allowed list, now valid
        ("HU/00007/2025", "TestNameX", "TestGivenX", None, None,
        None, None, None, None, None, None,
        None, None, "MC", None, 0),

        # Stateless (ZZ) — was missing from allowed list, now valid
        ("HU/00008/2025", "TestNameX", "TestGivenX", None, None,
        None, None, None, None, None, None,
        None, None, "ZZ", None, 0),
    ]

    silver_c_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("CategoryId", T.IntegerType(), True)
    ])

    silver_c_data = [
        ("HU/00487/2025", 3), ("HU/00487/2025", 10), ("HU/00487/2025", 37),
        ("HU/00365/2025", 10), ("HU/00365/2025", 38),
        ("EA/03208/2023", 3), ("EA/03208/2023", 11), ("EA/03208/2023", 37), ("EA/03208/2023", 47),
        ("EA/01698/2024", 3), ("EA/01698/2024", 11), ("EA/01698/2024", 37), ("EA/01698/2024", 38), ("EA/01698/2024", 48),
        ("HU/00560/2025", 3), ("HU/00560/2025", 10), ("HU/00560/2025", 31), ("HU/00560/2025", 37), ("HU/00560/2025", 39),
        ("HU/00532/2025", 3), ("HU/00532/2025", 10), ("HU/00532/2025", 32), ("HU/00532/2025", 37), ("HU/00532/2025", 39),
        ("HU/00423/2025", 3), ("HU/00423/2025", 10), ("HU/00423/2025", 31), ("HU/00423/2025", 37), ("HU/00423/2025", 39),
        ("HU/00006/2025", 38),
        ("HU/00007/2025", 38),
        ("HU/00008/2025", 38),
    ]

    bronze_countryFromAddress_schema = T.StructType([
        T.StructField("countryFromAddress", T.StringType(), True),
        T.StructField("countryGovUkOocAdminJ", T.StringType(), True)
    ])

    bronze_countryFromAddress_data = [
        ("Poland", "PL"),
        ("Gibraltar", "GI"),
        ("South Africa", "ZA"),
        ("Zimbabwe", "WZ")
    ]

    bronze_nationalities_schema = T.StructType([
        T.StructField("NationalityId", T.StringType(), True),
        T.StructField("Description", T.StringType(), True),
        T.StructField("countryCode", T.StringType(), True),
        T.StructField("appellantNationalitiesDescription", T.StringType(), True),
    ])

    bronze_nationalities_data = [
        (0, "No Nationality", "NO MAPPING REQUIRED", "NO MAPPING REQUIRED"),
        (1, "Afghan", "AF", "Afghanistan"),
        (2, "Albanian", "AL", "Albania"),
        (3, "Algerian", "DZ", "Algeria"),
        (211, "Stateless", "ZZ", "Stateless"),
    ]

    bronze_HORef_cleansing_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("HORef", T.StringType(), True),
        T.StructField("FCONumber", T.StringType(), True)
    ])

    bronze_HORef_cleansing_data = [
        ("HU/00365/2025", "R1286425", None),
        ("EA/01698/2024", "T1113940", None)
    ]

    silver_m1 =  spark.createDataFrame(m1_data, m1_schema)
    silver_m2 =  spark.createDataFrame(m2_data, m2_schema)
    silver_c =  spark.createDataFrame(silver_c_data, silver_c_schema)
    bronze_nationalities = spark.createDataFrame(bronze_nationalities_data, bronze_nationalities_schema)
    bronze_countryFromAddress =  spark.createDataFrame(bronze_countryFromAddress_data, bronze_countryFromAddress_schema)
    bronze_HORef_cleansing =  spark.createDataFrame(bronze_HORef_cleansing_data, bronze_HORef_cleansing_schema)

    appellantDetails_content, _ = appellantDetails(
        silver_m1, silver_m2, silver_c, bronze_countryFromAddress, bronze_HORef_cleansing, bronze_nationalities
    )

    results = {row["CaseNo"]: row.asDict() for row in appellantDetails_content.collect()}
    return results

def assert_all_null(row, *fields):
    for f in fields:
        assert row.get(f) is None, f"{f} expected None but got {row.get(f)}"

def normalise_null(v):
    if isinstance(v, str) and v.strip().lower() in {"none", "null", ""}:
        return None
    return v

def assert_equals(row, **expected):
    for k, v in expected.items():
        actual = normalise_null(row.get(k))
        expected_v = normalise_null(v)
        assert actual == expected_v, f"{k} expected {expected_v} but got {actual}"

def test_appellant_basic_names(appellantDetails_outputs):
    """Check that names are mapped correctly."""
    row = appellantDetails_outputs["HU/00487/2025"]
    assert_equals(row,
        appellantFamilyName="RobinsonX",
        appellantGivenNames="AdamX",
        appellantNameForDisplay="AdamX RobinsonX",
        caseNameHmctsInternal="AdamX RobinsonX",
        hmctsCaseNameInternal="AdamX RobinsonX"
    )

def test_is_appellant_minor(appellantDetails_outputs):
    """Check minor vs adult based on BirthDate."""
    # Minor
    assert_equals(appellantDetails_outputs["HU/00365/2025"], isAppellantMinor="Yes")
    # Adult
    assert_equals(appellantDetails_outputs["HU/00487/2025"], isAppellantMinor="No")

def test_appellant_contact_info(appellantDetails_outputs):
    """Check email and mobile presence."""
    # Has email
    assert_equals(appellantDetails_outputs["HU/00365/2025"],
                  internalAppellantEmail="smithjohn@example.net",
                  email="smithjohn@example.net",
                  internalAppellantMobileNumber=None,
                  mobileNumber=None)
    # No email/mobile
    assert_all_null(appellantDetails_outputs["HU/00487/2025"],
                    "internalAppellantEmail", "email",
                    "internalAppellantMobileNumber", "mobileNumber")

def test_appellant_in_uk_and_ooc(appellantDetails_outputs):
    """Check appellant location flags and ooc admin."""
    # In UK
    assert_equals(appellantDetails_outputs["HU/00487/2025"], appellantInUk="Yes")
    assert_equals(appellantDetails_outputs["HU/00487/2025"], appealOutOfCountry="No")
    # Out of country
    assert_equals(appellantDetails_outputs["HU/00365/2025"], appealOutOfCountry="Yes")
    # Out-of-country with HORef triggers adminJ
    assert_equals(appellantDetails_outputs["EA/01698/2024"], oocAppealAdminJ=None)

def test_appellant_address_fields(appellantDetails_outputs):
    """Check formatted addresses for both in-UK and out-of-country appeals."""

    # In UK address (CategoryId 37) => STRUCT
    row = appellantDetails_outputs["HU/00487/2025"]

    assert_equals(row, appellantHasFixedAddress="Yes")

    addr = row["appellantAddress"]
    assert addr is not None

    assert addr["AddressLine1"] == "7759 Rios SquareX"
    assert addr["AddressLine2"] == "Paul WalksX"
    assert addr["AddressLine3"] == ""
    assert addr["PostTown"] == "KristinfurtX"
    assert addr["County"] == "Trinidad and TobagoX"
    assert addr["Country"] == ""
    assert addr["PostCode"] == "W3 8PF"

    # Out-of-country adminJ address (CategoryId 38)
    row = appellantDetails_outputs["HU/00365/2025"]
    print(f"[DEBUG] countryGovUkOocAdminJ={row.get('countryGovUkOocAdminJ')}")
    assert_equals(row,
              addressLine1AdminJ="4280 Michael Highway Suite 815X",
              addressLine2AdminJ="Stephanie AlleyX",
              addressLine3AdminJ="Port DanielX, GibraltarX",
              addressLine4AdminJ="DD3 1HW",
              countryGovUkOocAdminJ="GI",
              appellantHasFixedAddressAdminJ="Yes"
             )
    
def test_appellant_stateless(appellantDetails_outputs):
    row = appellantDetails_outputs["HU/00005/2025"]

    assert_equals(row,
                  appellantStateless="isStateless",
                  appellantNationalitiesDescription="Stateless")

    normalized_nationalities = normalise_rows(row["appellantNationalities"])
    assert normalized_nationalities == [{'id': normalized_nationalities[0]['id'], 'value': {'code': 'ZZ'}}]

def test_appellant_nationalities(appellantDetails_outputs):
    row = appellantDetails_outputs["HU/00487/2025"]

    assert_equals(row,
                  appellantStateless="hasNationality",
                  appellantNationalitiesDescription="Afghanistan")

    normalized_nationalities = normalise_rows(row["appellantNationalities"])
    assert normalized_nationalities == [{'id': normalized_nationalities[0]['id'], 'value': {'code': 'AF'}}]

def test_deportation_order_options(appellantDetails_outputs):
    """Check deportation order options for CategoryId 48."""
    row = appellantDetails_outputs["EA/01698/2024"]
    assert_equals(row, deportationOrderOptions="Yes")

def test_missing_fields_are_none(appellantDetails_outputs):
    """Ensure optional fields remain None if not present."""
    row = appellantDetails_outputs["HU/00560/2025"]
    assert_all_null(row,
                    "Appellant_Email", "internalAppellantEmail",
                    "Appellant_Telephone", "internalAppellantMobileNumber",
                    "appellantAddress", "addressLine1AdminJ", "addressLine2AdminJ",
                    "addressLine3AdminJ", "addressLine4AdminJ", "countryGovUkOocAdminJ"
                   )


def test_appellant_in_uk_field_conditions(appellantDetails_outputs):
    # No category 37 or 38; detained cases are handled by paymentPendingDetained, not here
    assert appellantDetails_outputs["HU/00001/2025"]["appellantInUk"] == "No"   # Detained 1 — no longer sets Yes in paymentPending
    assert appellantDetails_outputs["HU/00002/2025"]["appellantInUk"] == "No"   # Detained 2 — no longer sets Yes in paymentPending
    assert appellantDetails_outputs["HU/00003/2025"]["appellantInUk"] == "No"   # Detained 4 — no longer sets Yes in paymentPending
    assert appellantDetails_outputs["HU/00004/2025"]["appellantInUk"] == "Yes"  # Appellant_Address5 in UK
    assert appellantDetails_outputs["HU/00005/2025"]["appellantInUk"] == "No"   # Appellant_Address5 not in UK


def test_country_gov_uk_ooc_adminj_new_codes(appellantDetails_outputs):
    """MH, MC and ZZ are now valid countryGovUkOocAdminJ codes with category 38.

    MH was previously mapped to NO MAPPING REQUIRED (→ NULL); it now passes through as 'MH'.
    MC and ZZ were missing from the DQ allowed list and are now included.
    """
    assert_equals(appellantDetails_outputs["HU/00006/2025"], countryGovUkOocAdminJ="MH")
    assert_equals(appellantDetails_outputs["HU/00007/2025"], countryGovUkOocAdminJ="MC")
    assert_equals(appellantDetails_outputs["HU/00008/2025"], countryGovUkOocAdminJ="ZZ")
