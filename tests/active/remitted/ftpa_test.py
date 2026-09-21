from Databricks.ACTIVE.APPEALS.shared_functions.remitted import ftpa
from pyspark.sql import SparkSession
import pytest
from pyspark.sql import types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("remittedFtpaTests")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def ftpa_outputs(spark):

    # ---- Minimal M1 schema ----
    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
        T.StructField("Sponsor_Name", T.StringType(), True),
        T.StructField("Interpreter", T.StringType(), True),
        T.StructField("CourtPreference", T.StringType(), True),
        T.StructField("InCamera", T.BooleanType(), True),
        T.StructField("VisitVisaType", T.IntegerType(), True),
        T.StructField("CentreId", T.IntegerType(), True),
        T.StructField("Rep_Postcode", T.StringType(), True),
        T.StructField("CaseRep_Postcode", T.StringType(), True),
        T.StructField("PaymentRemissionRequested", T.IntegerType(), True),
        T.StructField("lu_applicationChangeDesignatedHearingCentre", T.StringType(), True),
    ])

    m1_data = [
        ("CASE001", "AIP", "FTPA", None, 0, 0, True, 1, 1, "B12 0hf", "B12 0hf", 1, "Man"),
        ("CASE002", "AIP", "FTPA", None, 0, 0, False, 2, 2, "B12 0hf", "B12 0hf", 2, "Man"),
        ("CASE003", "AIP", "FTPA", None, 0, 0, True, 2, 3, "B12 0hf", "B12 0hf", 3, "Man"),
    ]

    # ---- Minimal M2 schema ----
    m2_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("Detained", T.IntegerType(), True),
        T.StructField("AppellantCountryId", T.IntegerType(), True),
        T.StructField("Appellant_Postcode", T.StringType(), True),
        T.StructField("Appellant_Address1", T.StringType(), True),
        T.StructField("Appellant_Address2", T.StringType(), True),
        T.StructField("Appellant_Address3", T.StringType(), True),
        T.StructField("Appellant_Address4", T.StringType(), True),
        T.StructField("Appellant_Address5", T.StringType(), True),
        T.StructField("lu_countryGovUkOocAdminJ", T.StringType(), True),
    ])

    m2_data = [
        ("CASE001", None, None, None, None, None, None, None, None, None),
        ("CASE002", None, None, None, None, None, None, None, None, None),
        ("CASE003", None, None, None, None, None, None, None, None, None),
    ]

    # ---- Minimal M3 schema with various decision outcomes ----
    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("HearingDuration", T.IntegerType(), True),
        T.StructField("HearingCentre", T.StringType(), True),
        T.StructField("DateReceived", T.StringType(), True),
        T.StructField("DecisionDate", T.StringType(), True),
        T.StructField("Adj_Title", T.StringType(), True),
        T.StructField("Adj_Forenames", T.StringType(), True),
        T.StructField("Adj_Surname", T.StringType(), True),
        T.StructField("Party", T.IntegerType(), True),
        T.StructField("OutOfTime", T.IntegerType(), True),
        T.StructField("Outcome", T.IntegerType(), True),
    ])

    # M3 data with different outcomes:
    # - CASE001: Outcome 30 (granted)
    # - CASE002: Outcome 31 (refused)
    # - CASE003: Outcome 14 (notAdmitted)
    m3_data = [
        ("CASE001", 1, 39, 60, "LOC001", "2025-09-01T00:00:00.000+00:00", "2025-09-12T00:00:00.000+00:00", "Mr", "John", "Doe", 2, 0, 30),
        ("CASE002", 1, 39, 45, "LOC002", "2025-09-01T00:00:00.000+00:00", "2025-09-12T00:00:00.000+00:00", "Ms", "Jane", "Smith", 1, 0, 31),
        ("CASE003", 1, 39, 30, "LOC003", "2025-10-02T00:00:00.000+00:00", "2025-10-10T00:00:00.000+00:00", "Mr", "Guy", "Random", 1, 0, 14),
    ]

    # ---- Minimal C schema ----
    c_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("CategoryId", T.IntegerType(), True),
    ])

    c_data = [
        ("CASE001", 37),
        ("CASE002", 37),
        ("CASE003", 37),
    ]

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)
    silver_m2 = spark.createDataFrame(m2_data, m2_schema)
    silver_m3 = spark.createDataFrame(m3_data, m3_schema)
    silver_c = spark.createDataFrame(c_data, c_schema)

    ftpa_content, _ = ftpa(silver_m1, silver_m2, silver_m3, silver_c)

    results = {row["CaseNo"]: row.asDict() for row in ftpa_content.collect()}
    return results


def test_ftpaFinalDecisionForDisplay_is_undecided(ftpa_outputs):
    """
    Test that ftpaFinalDecisionForDisplay is always set to 'undecided'
    regardless of the original outcome value (granted, refused, notAdmitted).
    """
    r = ftpa_outputs

    # All cases should have "undecided" regardless of their original outcomes
    assert r["CASE001"]["ftpaFinalDecisionForDisplay"] == "undecided"
    assert r["CASE002"]["ftpaFinalDecisionForDisplay"] == "undecided"
    assert r["CASE003"]["ftpaFinalDecisionForDisplay"] == "undecided"


def test_other_fields_still_populated(ftpa_outputs):
    """
    Test that other ftpa fields are still properly populated after
    the ftpaFinalDecisionForDisplay override.
    """
    r = ftpa_outputs

    # CASE001: Party 2 (respondent), Outcome 30 (granted) -> first decision should be granted
    assert r["CASE001"]["ftpaApplicantType"] == "respondent"
    assert r["CASE001"]["ftpaFirstDecision"] == "granted"
    assert r["CASE001"]["ftpaRespondentDecisionDate"] == "2025-09-12"
    assert r["CASE001"]["ftpaAppellantDecisionDate"] is None

    # CASE002: Party 1 (appellant), Outcome 31 (refused) -> first decision should be refused
    assert r["CASE002"]["ftpaApplicantType"] == "appellant"
    assert r["CASE002"]["ftpaFirstDecision"] == "refused"
    assert r["CASE002"]["ftpaAppellantDecisionDate"] == "2025-09-12"
    assert r["CASE002"]["ftpaRespondentDecisionDate"] is None

    # CASE003: Party 1 (appellant), Outcome 14 (notAdmitted) -> first decision should be notAdmitted
    assert r["CASE003"]["ftpaApplicantType"] == "appellant"
    assert r["CASE003"]["ftpaFirstDecision"] == "notAdmitted"
    assert r["CASE003"]["ftpaAppellantDecisionDate"] == "2025-10-10"
    assert r["CASE003"]["ftpaRespondentDecisionDate"] is None
