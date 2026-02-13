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
        elif isinstance(r, Row):
            return {k: row_to_dict(v) for k, v in r.asDict().items()}
        else:
            return r
    return row_to_dict(row_list)

@pytest.fixture(scope="session")
def appellantDetails_outputs(spark):

    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("DateLodged", T.StringType(), True),
        T.StructField("dv_CCDAppealType", T.StringType(), True),
        T.StructField("BirthDate", T.StringType(), True),
        T.StructField("NationalityId", T.StringType(), True),
        T.StructField("lu_countryCode", T.StringType(), True),
        T.StructField("lu_appellantNationalitiesDescription", T.StringType(), True)
    ])

    m1_data = [
        ("HU/00487/2025", "2025-03-07", "HU", "1961-05-18", "1", "AF", "Afghanistan"),
        ("HU/00365/2025", "2024-11-06", "HU", "2017-05-06", "27", "BI", "Burundi"),
        ("EA/03208/2023", "2023-09-15", "EA", "1995-07-23", "63", "GR", "Greece"),
        ("EA/01698/2024", "2024-07-31", "EA", "2000-04-28", "94", "LR", "Liberia"),
        ("HU/00560/2025", "2025-03-31", "HU", "1950-11-06", "201", "NO MAPPING REQUIRED", "NO MAPPING REQUIRED"),
        ("HU/00532/2025", "2025-03-24", "HU", "1983-08-08", "169", "TR", "Turkey"),
        ("HU/00423/2025", "2025-02-21", "HU", "1936-05-07", "179", "VE", "Venezuela (Bolivarian Republic of)"),
        ("HU/00001/2025", "2025-02-21", "HU", "1936-05-07", "179", "UK", "British"),
        ("HU/00002/2025", "2025-02-21", "HU", "1936-05-07", "179", "UK", "British"),
        ("HU/00003/2025", "2025-02-21", "HU", "1936-05-07", "179", "UK", "British"),
        ("HU/00004/2025", "2025-02-21", "HU", "1936-05-07", "179", "UK", "British"),
        ("HU/00005/2025", "2025-02-21", "HU", "1936-05-07", "179", "UK", "British")
    ]

    m2_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
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
        T.StructField("DeportationDate", T.StringType(), True),
        T.StructField("RemovalDate", T.StringType(), True),
        T.StructField("HORef", T.StringType(), True),
        T.StructField("CasePrefix", T.StringType(), True),
        T.StructField("AppellantCountryId", T.IntegerType(), True),
        T.StructField("Relationship", T.StringType(), True),
        T.StructField("dv_countryGovUkOocAdminJ", T.StringType(), True),
        T.StructField("FCONumber", T.StringType(), True),
        T.StructField("Detained", T.StringType(), True)
    ])

    m2_data = [
        ("HU/00487/2025", "LR", "refusalOfHumanRights", "RobinsonX", "AdamX", None, None,
         "7759 Rios SquareX", "Paul WalksX", "KristinfurtX", "Trinidad and TobagoX", None, "W3 8PF",
         None, None, None, "HU", 133, None, None, None, 0),
        ("HU/00365/2025", "AIP", "euSettlementScheme", "SandersX", "AmandaX", "smithjohn@example.net", None,
         "4280 Michael Highway Suite 815X", "Stephanie AlleyX", "Port DanielX", "GibraltarX", None, "DD3 1HW",
         None, None, None, "HU", 128, None, "Gibraltar", "XXXXXXX", 0),
        ("EA/03208/2023", "LR", "refusalOfEu", "PachecoX", "KiaraX", "chelsea42@example.net", None,
         "7706 Barbara Gateway Apt. 725X", "Daniel BurgsX", "North JillportX", None, None, "LS3M 4BX",
         None, None, None, "EA", 155, None, None, None, 0),
        ("EA/01698/2024", "AIP", "euSettlementScheme", "ColemanX", "AlyssaX", "betty23@example.net", None,
         "06382 Bryan MountX", "Kimberly ThroughwayX", "ZacharyburghX", None, None, "B37 5LW",
         None, None, "T1113940", "EA", 124, None, None, None, 0),
        ("HU/00560/2025", "LR", "refusalOfHumanRights", "MccallX", "ThomasX", None, None,
         None, None, None, None, None, None,
         None, None, None, "HU", 86, None, None, None, 0),
        ("HU/00532/2025", "LR", "refusalOfHumanRights", "AlvarezX", "JasmineX", None, None,
         None, None, None, None, None, None,
         None, None, None, "HU", 152, None, None, None, 0),
        ("HU/00423/2025", "LR", "refusalOfHumanRights", "WilliamsX", "SarahX", None, None,
         None, None, None, None, None, None,
         None, None, None, "HU", 191, None, None, None, 0),
        ("HU/00001/2025", "LR", "refusalOfHumanRights", "WilliamsX", "SarahX", None, None,
         None, None, None, None, None, None,
         None, None, None, "HU", 191, None, None, None, 1),
        ("HU/00002/2025", "LR", "refusalOfHumanRights", "WilliamsX", "SarahX", None, None,
         None, None, None, None, None, None,
         None, None, None, "HU", 191, None, None, None, 2),
        ("HU/00003/2025", "LR", "refusalOfHumanRights", "WilliamsX", "SarahX", None, None,
         None, None, None, None, None, None,
         None, None, None, "HU", 191, None, None, None, 4),
        ("HU/00004/2025", "LR", "refusalOfHumanRights", "WilliamsX", "SarahX", None, None,
         None, None, None, None, "UK", None,
         None, None, None, "HU", 191, None, None, None, 0),
        ("HU/00005/2025", "LR", "refusalOfHumanRights", "WilliamsX", "SarahX", None, None,
         None, None, None, None, "NotUK", None,
         None, None, None, "HU", 191, None, None, None, 0)
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
        ("HU/00423/2025", 3), ("HU/00423/2025", 10), ("HU/00423/2025", 31), ("HU/00423/2025", 37), ("HU/00423/2025", 39)
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
    bronze_countryFromAddress =  spark.createDataFrame(bronze_countryFromAddress_data, bronze_countryFromAddress_schema)
    bronze_HORef_cleansing =  spark.createDataFrame(bronze_HORef_cleansing_data, bronze_HORef_cleansing_schema)

    appellantDetails_content, _ = appellantDetails(
        silver_m1, silver_m2, silver_c, bronze_countryFromAddress, bronze_HORef_cleansing
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
    assert addr["Country"] is None
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
    
def test_appellant_stateless_and_nationalities(appellantDetails_outputs):
    row = appellantDetails_outputs["HU/00487/2025"]
    normalized_nationalities = normalise_rows(row["appellantNationalities"])
    assert normalized_nationalities == [{'id': '4f7b9a0a-90fa-4258-a530-395aedebfc02',
                                         'value': {'code': 'AF'}}]

def test_appellant_stateless_and_nationalities(appellantDetails_outputs):
    # No mapped nationality
    row_stateless = appellantDetails_outputs["HU/00560/2025"]
    assert_equals(row_stateless,
                  appellantStateless="hasNationality",
                  appellantNationalities=None,
                  appellantNationalitiesDescription=None
                 )

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
    # No category 37 or 38
    assert appellantDetails_outputs["HU/00001/2025"]["appellantInUk"] == "Yes"  # Detained 1
    assert appellantDetails_outputs["HU/00002/2025"]["appellantInUk"] == "Yes"  # Detained 2
    assert appellantDetails_outputs["HU/00003/2025"]["appellantInUk"] == "Yes"  # Detained 4
    assert appellantDetails_outputs["HU/00004/2025"]["appellantInUk"] == "Yes"  # Appellant_Address5 in UK
    assert appellantDetails_outputs["HU/00005/2025"]["appellantInUk"] == "No"   # Appellant_Address5 not in UK
