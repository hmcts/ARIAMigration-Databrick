from Databricks.ACTIVE.APPEALS.shared_functions.ftpa_decided import ftpa
from pyspark.sql import SparkSession
import pytest
from pyspark.sql import types as T

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("ftpaDecidedTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def ftpa_outputs(spark):

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
        ("CASE001", "AIP", "FTPA", None, 0, 0, True, 1,1, "B12 0hf", "B12 0hf",1,"Man"),  
        ("CASE002", "AIP", "FTPA", None, 0, 0, False, 2,2, "B12 0hf", "B12 0hf",2,"Man"),  
        ("CASE003", "AIP", "FTPA", None, 0, 0, True, 2,3, "B12 0hf", "B12 0hf",3,"Man"),  
        ("CASE004", "AIP", "FTPA", None, 0, 0, False, 2,4, "B12 0hf", "B12 0hf",1,"Man"),  
        ("CASE005", "AIP", "FT", None, 0, 0, True, 3,5, "B12 0hf", "B12 0hf",2,"Man"), 
        ("CASE006", "AIP", "FT", None, 0, 0, True, 4,6, "B12 0hf", "B12 0hf",4,"Man"),    
        ("CASE007", "AIP", "FT", None, 0, 0, False, None,7, "B12 0hf", "B12 0hf",0,"Man"),    
        ("CASE008", "AIP", "FT", None, 0, 0, False, None,8, "B12 0hf", "B12 0hf",None,"Man"),    
        ("CASE009", "AIP", "FT", None, 0, 0, None, 2,9, "B12 0hf", "B12 0hf",7,"Man"),    
        ("CASE010", "AIP", "FT", None, 0, 0, None, 2,None, "B12 0hf", "B12 0hf",1,"Man"),  
        ("CASE011", "AIP", "FT", None, 0, 0, True, 61,0, "B12 0hf", "B12 0hf",1,"Man")
        ]

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
    ])

    m2_data = [
        # stage_detained: Detained==3 → OOC (short-circuits everything)
        ("CASE005", 3,    None, None,      None,           None, None, None, None),
        # stage_category: CategoryId==37 → IN
        ("CASE006", None, None, None,      None,           None, None, None, None),
        # stage_category: CategoryId==38 → OUT
        ("CASE007", None, None, None,      None,           None, None, None, None),
        # stage_country: AppellantCountryId==188 → IN
        ("CASE008", None, 188,  None,      None,           None, None, None, None),
        # stage_postcode: valid UK postcode → IN
        ("CASE009", None, None, "B12 0HF", None,           None, None, None, None),
        # stage_address: address contains "united kingdom" → IN
        ("CASE010", None, None, None,      "123 Some Road","Birmingham","United Kingdom", None, None),
        # falls through all stages → OOC
        ("CASE011", None, None, None,      "123 Rue de la Paix", "Paris", "France", None, None),
    ]

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

    m3_data = [
        ("HU/01897/2024", 1, 39, 60, "LOC001", "2025-09-01T00:00:00.000+00:00", "2025-09-12T00:00:00.000+00:00", "Mr", "John", "Doe", 2, 0, 31),
        ("PA/01921/2025", 1, 39, 45, "LOC002", "2025-09-01T00:00:00.000+00:00", None, "Ms", "Jane", "Doe", 1, 0, None),
        ("PA/03789/2024", 1, 39, 30, "LOC003", "2025-10-02T00:00:00.000+00:00", None, "Mr", "Guy", "Random", 1, 0, None),
        ("PA/03885/2024", 1, 39, None, "LOC004", "2025-11-02T00:00:00.000+00:00", None, "Sir", "Alex", "Smith", 1, 0, None),
    ]

    c_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("CategoryId", T.IntegerType(), True),
    ])

    c_data = [
        ("HU/01897/2024", 37),
        ("PA/01921/2025", 37),
        ("PA/03789/2024", 37),
        ("PA/03885/2024", 37),
    ]

    silver_m1 =  spark.createDataFrame(m1_data, m1_schema)
    silver_m2 =  spark.createDataFrame(m2_data, m2_schema)
    silver_m3 = spark.createDataFrame(m3_data, m3_schema)
    silver_c = spark.createDataFrame(c_data, c_schema)

    ftpa_content, _ = ftpa(silver_m1, silver_m2, silver_m3, silver_c)

    results = {row["CaseNo"]: row.asDict() for row in ftpa_content.collect()}
    return results

def test_ftpaApplicantType(ftpa_outputs):
    r = ftpa_outputs
    assert r["HU/01897/2024"]["ftpaApplicantType"] == "respondent"
    assert r["PA/01921/2025"]["ftpaApplicantType"] is None
    assert r["PA/03789/2024"]["ftpaApplicantType"] is None
    assert r["PA/03885/2024"]["ftpaApplicantType"] is None

def test_ftpaFirstDecision_and_FinalDecisionForDisplay(ftpa_outputs):
    r = ftpa_outputs
    assert r["HU/01897/2024"]["ftpaFirstDecision"] == "refused"
    assert r["HU/01897/2024"]["ftpaFinalDecisionForDisplay"] == "refused"

    # Cases with no decision should be None
    for case in ["PA/01921/2025", "PA/03789/2024", "PA/03885/2024"]:
        assert r[case]["ftpaFirstDecision"] is None
        assert r[case]["ftpaFinalDecisionForDisplay"] is None

def test_decision_dates_by_party_iso8601(ftpa_outputs):
    r = ftpa_outputs
    # Respondent decision
    assert r["HU/01897/2024"]["ftpaRespondentDecisionDate"] == "2025-09-12"
    assert r["HU/01897/2024"]["ftpaAppellantDecisionDate"] is None

    # No decisions
    for case in ["PA/01921/2025", "PA/03789/2024", "PA/03885/2024"]:
        assert r[case]["ftpaAppellantDecisionDate"] is None
        assert r[case]["ftpaRespondentDecisionDate"] is None

def test_rj_outcome_types(ftpa_outputs):
    r = ftpa_outputs
    assert r["HU/01897/2024"]["ftpaRespondentRjDecisionOutcomeType"] == "refused"
    assert r["HU/01897/2024"]["ftpaAppellantRjDecisionOutcomeType"] == "refused"
    assert r["PA/01921/2025"]["ftpaAppellantRjDecisionOutcomeType"] is None
    assert r["PA/03789/2024"]["ftpaAppellantRjDecisionOutcomeType"] is None

def test_notice_of_decision_set_aside_flags(ftpa_outputs):
    r = ftpa_outputs
    # Party 2 => respondent flag set to "No"
    assert r["HU/01897/2024"]["isFtpaRespondentNoticeOfDecisionSetAside"] == "No"
    assert r["HU/01897/2024"]["isFtpaAppellantNoticeOfDecisionSetAside"] is None

    # Party 1 => appellant flag set to "No"
    for case in ["PA/01921/2025", "PA/03789/2024", "PA/03885/2024"]:
        assert r[case]["isFtpaAppellantNoticeOfDecisionSetAside"] == "No"
        assert r[case]["isFtpaRespondentNoticeOfDecisionSetAside"] is None
