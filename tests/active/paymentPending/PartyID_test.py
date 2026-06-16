import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import partyID
import uuid

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("PartyIDTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def partyID_outputs(spark):

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

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)

    # Call the function under test
    partyID_content, _ = partyID(silver_m1)

    # Convert to dictionary keyed by CaseNo
    results = {row["CaseNo"]: row.asDict() for row in partyID_content.collect()}
    return results


def is_uuid(v):
    try:
        uuid.UUID(v)
        return True
    except Exception:
        return False


def test_appellant_party_id_created(partyID_outputs):
    row = partyID_outputs["HU/00638/2024"]
    assert is_uuid(row["appellantPartyId"])


def test_legal_rep_created_for_LR(partyID_outputs):
    row = partyID_outputs["HU/00638/2024"]
    assert is_uuid(row["legalRepIndividualPartyId"])
    assert is_uuid(row["legalRepOrganisationPartyId"])


def test_legal_rep_null_for_AIP(partyID_outputs):
    row = partyID_outputs["EA/00556/2025"]
    assert row["legalRepIndividualPartyId"] is None
    assert row["legalRepOrganisationPartyId"] is None


def test_sponsor_created_when_name_not_null(partyID_outputs):
    row = partyID_outputs["HU/00638/2024"]
    assert is_uuid(row["sponsorPartyId"])


def test_sponsor_null_without_name(partyID_outputs):
    row = partyID_outputs["HU/01871/2024"]
    assert row["sponsorPartyId"] is None


def test_no_ids_when_appeal_type_null(partyID_outputs):
    row = partyID_outputs["HU/00592/2025"]
    assert is_uuid(row["appellantPartyId"])  # each case as an appealType. this should not be null
    assert row["legalRepIndividualPartyId"] is None
    assert row["legalRepOrganisationPartyId"] is None
    assert row["sponsorPartyId"] is None
