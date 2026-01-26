import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import PartyID
import uuid

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("PartyIDTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def PartyID_outputs(spark):

    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True), 
        T.StructField("lu_appealType", T.StringType(), True),
        T.StructField("Sponsor_Name", T.StringType(), True),
    ])

    m1_data = [
        # LR + sponsor + category 38
        ("HU/00638/2024", "LR", "HU", "John Sponsor"),

        # AIP + sponsor + category 38
        ("EA/00556/2025", "AIP", "EA", "Jane Sponsor"),

        # LR but no sponsor
        ("HU/01871/2024", "LR", "HU", None),

        # AIP no sponsor
        ("HU/00588/2025", "AIP", "HU", None),

        # Appeal type null → everything null
        ("HU/00592/2025", "LR", None, "Ghost Sponsor"),
    ]

    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True), 
        T.StructField("Outcome", T.IntegerType(), True),
    ])

    m3_data = [
    ("HU/00638/2024", 1, None),
    ("HU/00638/2024", 2, 1),
    ("EA/00556/2025", 1, 1),
    ("HU/01871/2024", 1, 1),
    ("HU/00588/2025", 1, 1),
    ("HU/00592/2025", 1, 1)]

    silver_c_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("CategoryId", T.IntegerType(), True)
    ])

    silver_c_data = [
    ("HU/00638/2024", 38),
    ("EA/00556/2025", 38),
    ("HU/01871/2024", 10),
    ]
        
    silver_m1 =  spark.createDataFrame(m1_data, m1_schema)
    silver_m3 =  spark.createDataFrame(m3_data, m3_schema)
    silver_c =  spark.createDataFrame(silver_c_data, silver_c_schema)

    # Call the function under test
    PartyID_content, _ = PartyID(silver_m1, silver_m3, silver_c)

    # Convert to dictionary keyed by CaseNo
    results = {row["CaseNo"]: row.asDict() for row in PartyID_content.collect()}
    return results

def is_uuid(v):
    try:
        uuid.UUID(v)
        return True
    except Exception:
        return False

def test_appellant_party_id_created(PartyID_outputs):
    row = PartyID_outputs["HU/00638/2024"]
    assert is_uuid(row["appellantPartyId"])

def test_legal_rep_created_for_LR(PartyID_outputs):
    row = PartyID_outputs["HU/00638/2024"]
    assert is_uuid(row["legalRepIndividualPartyId"])
    assert is_uuid(row["legalRepOrganisationPartyId"])

def test_legal_rep_null_for_AIP(PartyID_outputs):
    row = PartyID_outputs["EA/00556/2025"]
    assert row["legalRepIndividualPartyId"] is None
    assert row["legalRepOrganisationPartyId"] is None

def test_sponsor_created_when_category_38_and_name(PartyID_outputs):
    row = PartyID_outputs["HU/00638/2024"]
    assert is_uuid(row["sponsorPartyId"])

def test_sponsor_null_without_name(PartyID_outputs):
    row = PartyID_outputs["HU/01871/2024"]
    assert row["sponsorPartyId"] is None

def test_no_ids_when_appeal_type_null(PartyID_outputs):
    row = PartyID_outputs["HU/00592/2025"]
    assert row["appellantPartyId"] is None
    assert row["legalRepIndividualPartyId"] is None
    assert row["legalRepOrganisationPartyId"] is None
    assert row["sponsorPartyId"] is None
