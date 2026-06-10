import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPendingDetained import appellantDetails


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("paymentPendingDetainedAppellantDetailsTests")
        .getOrCreate()
    )


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

    # Six cases covering every branch of appellantInUk in paymentPendingDetained:
    #   DET/0001–0003: detained (1, 2, 4), no categories → appellantInUk must be "Yes"
    #   DET/0004:      detained (1) + category 38 (OOC) → detained wins, still "Yes"
    #   DET/0005:      not detained, category 37 (in-UK) → "Yes"
    #   DET/0006:      not detained, category 38 (OOC)  → "No"
    # Three cases for derive_country_silver_m2 / getCountryApp paths (no category 37/38, not detained):
    #   DET/0007:      Appellant_Address5='UK', no postcode → dv_addressInUk=True → appellantInUk="Yes"
    #   DET/0008:      valid UK postcode, no Address5     → dv_addressInUk=True → appellantInUk="Yes"
    #   DET/0009:      address text 'Poland', no postcode → bronze lookup → dv_addressInUk=False → appellantInUk="No"
    #   DET/0010:      AppellantCountryId=188 (UK)        → dv_addressInUk=True immediately → appellantInUk="Yes"
    m1_data = [
        ("HU/DET/0001", "HU", "2025-03-07", "LR", "1980-01-01", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/DET/0002", "HU", "2025-03-07", "LR", "1980-01-01", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/DET/0003", "HU", "2025-03-07", "LR", "1980-01-01", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/DET/0004", "HU", "2025-03-07", "LR", "1980-01-01", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/DET/0005", "HU", "2025-03-07", "LR", "1980-01-01", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/DET/0006", "HU", "2025-03-07", "LR", "1980-01-01", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/DET/0007", "HU", "2025-03-07", "LR", "1980-01-01", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/DET/0008", "HU", "2025-03-07", "LR", "1980-01-01", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/DET/0009", "HU", "2025-03-07", "LR", "1980-01-01", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
        ("HU/DET/0010", "HU", "2025-03-07", "LR", "1980-01-01", "1", "AF", "Afghanistan", "refusalOfHumanRights", None, None, None, "HU"),
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
        T.StructField("Detained", T.IntegerType(), True),
    ])

    m2_data = [
        ("HU/DET/0001", "SmithX", "JohnX", None, None, None, None, None, None, None, None, None, None, None, None, 1),
        ("HU/DET/0002", "SmithX", "JohnX", None, None, None, None, None, None, None, None, None, None, None, None, 2),
        ("HU/DET/0003", "SmithX", "JohnX", None, None, None, None, None, None, None, None, None, None, None, None, 4),
        ("HU/DET/0004", "SmithX", "JohnX", None, None, None, None, None, None, None, None, None, None, None, None, 1),
        ("HU/DET/0005", "SmithX", "JohnX", None, None, "1 High StreetX", None, None, None, None, "SW1A 1AAX", None, None, None, None, 0),
        ("HU/DET/0006", "SmithX", "JohnX", None, None, None, None, None, None, None, None, None, None, None, None, 0),
        # derive_country_silver_m2 path tests (not detained, no categories)
        ("HU/DET/0007", "SmithX", "JohnX", None, None, None, None, None, None, "UK", None, 0, None, None, None, 0),
        ("HU/DET/0008", "SmithX", "JohnX", None, None, None, None, None, None, None, "W3 8PF", 0, None, None, None, 0),
        ("HU/DET/0009", "SmithX", "JohnX", None, None, "123 StreetX", None, "Poland", None, None, None, 0, None, None, None, 0),
        # getIsInUk: AppellantCountryId=188 (UK) → True immediately, no address/postcode needed
        ("HU/DET/0010", "SmithX", "JohnX", None, None, None, None, None, None, None, None, 188, None, None, None, 0),
    ]

    silver_c_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("CategoryId", T.IntegerType(), True),
    ])

    silver_c_data = [
        # DET/0001–0003: no categories (detained only)
        # DET/0004: detained + OOC category
        ("HU/DET/0004", 38),
        # DET/0005: in-UK category
        ("HU/DET/0005", 37),
        # DET/0006: OOC category
        ("HU/DET/0006", 38),
    ]

    bronze_countryFromAddress_schema = T.StructType([
        T.StructField("countryFromAddress", T.StringType(), True),
        T.StructField("countryGovUkOocAdminJ", T.StringType(), True),
    ])

    bronze_nationalities_schema = T.StructType([
        T.StructField("NationalityId", T.StringType(), True),
        T.StructField("Description", T.StringType(), True),
        T.StructField("countryCode", T.StringType(), True),
        T.StructField("appellantNationalitiesDescription", T.StringType(), True),
    ])

    bronze_nationalities_data = [
        (1, "Afghan", "AF", "Afghanistan"),
    ]

    bronze_HORef_cleansing_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("HORef", T.StringType(), True),
        T.StructField("FCONumber", T.StringType(), True),
    ])

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)
    silver_m2 = spark.createDataFrame(m2_data, m2_schema)
    silver_c = spark.createDataFrame(silver_c_data, silver_c_schema)
    bronze_countryFromAddress_data = [
        ("Poland", "PL"),
    ]
    bronze_countryFromAddress = spark.createDataFrame(bronze_countryFromAddress_data, bronze_countryFromAddress_schema)
    bronze_nationalities = spark.createDataFrame(bronze_nationalities_data, bronze_nationalities_schema)
    bronze_HORef_cleansing = spark.createDataFrame([], bronze_HORef_cleansing_schema)

    appellantDetails_df, _ = appellantDetails(
        silver_m1, silver_m2, silver_c, bronze_countryFromAddress, bronze_HORef_cleansing, bronze_nationalities
    )

    return {row["CaseNo"]: row.asDict() for row in appellantDetails_df.collect()}


def test_detained_1_appellant_in_uk(appellantDetails_outputs):
    """Detained=1 (prison) must produce appellantInUk='Yes' in the detained path."""
    assert appellantDetails_outputs["HU/DET/0001"]["appellantInUk"] == "Yes"


def test_detained_2_appellant_in_uk(appellantDetails_outputs):
    """Detained=2 (IRC) must produce appellantInUk='Yes' in the detained path."""
    assert appellantDetails_outputs["HU/DET/0002"]["appellantInUk"] == "Yes"


def test_detained_4_appellant_in_uk(appellantDetails_outputs):
    """Detained=4 (other) must produce appellantInUk='Yes' in the detained path."""
    assert appellantDetails_outputs["HU/DET/0003"]["appellantInUk"] == "Yes"


def test_detained_appeal_out_of_country(appellantDetails_outputs):
    """Detained cases have appellantInUk='Yes', so appealOutOfCountry must be the inverse: 'No'."""
    assert appellantDetails_outputs["HU/DET/0001"]["appealOutOfCountry"] == "No"
    assert appellantDetails_outputs["HU/DET/0002"]["appealOutOfCountry"] == "No"
    assert appellantDetails_outputs["HU/DET/0003"]["appealOutOfCountry"] == "No"


def test_detained_overrides_ooc_category(appellantDetails_outputs):
    """Detained=1 takes precedence over category 38 (OOC): appellantInUk must still be 'Yes'."""
    assert appellantDetails_outputs["HU/DET/0004"]["appellantInUk"] == "Yes"
    assert appellantDetails_outputs["HU/DET/0004"]["appealOutOfCountry"] == "No"


def test_non_detained_category_37_in_uk(appellantDetails_outputs):
    """Non-detained with category 37 still gets appellantInUk='Yes'."""
    assert appellantDetails_outputs["HU/DET/0005"]["appellantInUk"] == "Yes"
    assert appellantDetails_outputs["HU/DET/0005"]["appealOutOfCountry"] == "No"


def test_non_detained_category_38_out_of_country(appellantDetails_outputs):
    """Non-detained with category 38 gets appellantInUk='No' and appealOutOfCountry='Yes'."""
    assert appellantDetails_outputs["HU/DET/0006"]["appellantInUk"] == "No"
    assert appellantDetails_outputs["HU/DET/0006"]["appealOutOfCountry"] == "Yes"


def test_appellant_address5_uk_sets_gb(appellantDetails_outputs):
    """Appellant_Address5='UK' → searchCountry returns 'GB' → dv_addressInUk=True → appellantInUk='Yes'."""
    assert appellantDetails_outputs["HU/DET/0007"]["appellantInUk"] == "Yes"
    assert appellantDetails_outputs["HU/DET/0007"]["appealOutOfCountry"] == "No"


def test_uk_postcode_sets_gb(appellantDetails_outputs):
    """Valid UK postcode → searchCountry returns 'GB' → dv_addressInUk=True → appellantInUk='Yes'."""
    assert appellantDetails_outputs["HU/DET/0008"]["appellantInUk"] == "Yes"
    assert appellantDetails_outputs["HU/DET/0008"]["appealOutOfCountry"] == "No"


def test_address_lookup_non_uk_country(appellantDetails_outputs):
    """Address containing 'Poland' with no postcode → bronze lookup → non-GB → appellantInUk='No'."""
    assert appellantDetails_outputs["HU/DET/0009"]["appellantInUk"] == "No"
    assert appellantDetails_outputs["HU/DET/0009"]["appealOutOfCountry"] == "Yes"


def test_appellant_in_uk_from_country_id_188(appellantDetails_outputs):
    """AppellantCountryId=188 (UK) → getIsInUk returns True immediately → appellantInUk='Yes'."""
    assert appellantDetails_outputs["HU/DET/0010"]["appellantInUk"] == "Yes"
    assert appellantDetails_outputs["HU/DET/0010"]["appealOutOfCountry"] == "No"
