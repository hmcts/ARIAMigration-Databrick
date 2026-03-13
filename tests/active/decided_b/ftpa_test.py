from Databricks.ACTIVE.APPEALS.shared_functions.decided_b import ftpa
from pyspark.sql import SparkSession
import pytest
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("generalTests")
        .getOrCreate()
    )

##### Testing the hearingDetails field grouping function #####
@pytest.fixture(scope="session")
def ftpa_outputs(spark):


    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("HearingDuration", T.IntegerType(), True),
        T.StructField("HearingCentre", T.StringType(), True),
        T.StructField("DecisionDate", T.StringType(), True),
        T.StructField("DateReceived", T.StringType(), True),
        T.StructField("Adj_Title", T.StringType(), True),
        T.StructField("Adj_Forenames", T.StringType(), True),
        T.StructField("Adj_Surname", T.StringType(), True),
        T.StructField("Party", T.IntegerType(), True),
        T.StructField("OutOfTime", T.IntegerType(), True),
        T.StructField("Outcome", T.IntegerType(), True),
    ])

    m3_data = [
        ("CASE005", 1, 37, 180, "LOC001","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Mr","Doe","John",1,1,1),
        ("CASE005", 2, 38, 60, "LOC002","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.000+00:00","Ms","Doe","Jane",1,0,1),   
        ("CASE006", 1, 26, 240, "LOC003","2026-12-03T00:00:00.000+00:00","1899-12-30T13:00:00.000+00:00","Mr","xyz","John",1,1,1),   
        ("CASE007", 1, 37, 360, "LOC004","2026-08-03T00:00:00.000+00:00","2000-12-30T07:10:58.000+00:00","Mr","Doe","abc",2,0,1),  
        ("CASE008", 1, 38, None, "LOC005","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Sir","Random","Guy",1,1,1),  
        ("CASE009", 1, 26, 30, "LOC006","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Mr","John","Snow",2,0,1),  
        ("CASE010", 1, 37, None, "LOC007",None,"1899-12-30T10:00:00.000+00:00",None,None,None,1,None,1),  
        ("CASE011", 1, 38, 45, "LOC008","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.999+00:00","Mr","Hello","World",2,1,1),   
        ("CASE005", 1, 39, 180, "LOC001","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Mr","Doe","John",1,1,1),
        ("CASE005", 2, 39, 60, "LOC002","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.000+00:00","Ms","Doe","Jane",1,0,1),   
        ("CASE006", 1, 39, 240, "LOC003","2026-12-03T00:00:00.000+00:00","1899-12-30T13:00:00.000+00:00","Mr","xyz","John",1,1,1),   
        ("CASE007", 1, 39, 360, "LOC004","2026-08-03T00:00:00.000+00:00","2000-12-30T07:10:58.000+00:00","Mr","Doe","abc",2,0,1),  
        ("CASE008", 1, 39, None, "LOC005","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Sir","Random","Guy",1,1,1),  
        ("CASE009", 1, 39, 30, "LOC006","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Mr","John","Snow",2,0,1),  
        ("CASE010", 1, 39, None, "LOC007",None,"1899-12-30T10:00:00.000+00:00",None,None,None,1,None,1),  
        ("CASE011", 1, 39, 45, "LOC008","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.999+00:00","Mr","Hello","World",2,1,1)
        ]     

    c_schema = T.StructType([
    T.StructField("CaseNo", T.StringType(), True),
    T.StructField("CategoryId", T.IntegerType(), True),
    ])
    
    c_data = [
        ("CASE005", 37),  
        ("CASE006", 37),  
        ("CASE007", 37),  
        ("CASE008", 37),  
        ("CASE009", 37), 
        ("CASE010", 37),
        ("CASE011", 37),
        ]
    
    df_m3 =  spark.createDataFrame(m3_data, m3_schema)
    df_c =  spark.createDataFrame(c_data, c_schema)


    ftpa_content,_ = ftpa(df_m3, df_c)
    results = {row["CaseNo"]: row.asDict() for row in ftpa_content.collect()}
    
    return results


def test_ftpaList(spark, ftpa_outputs):

    results = ftpa_outputs

    # CASE005 – appellant, no explanation
    assert results["CASE005"]["ftpaList"] == [
        Row(id='1', value=Row(ftpaApplicant='appellant', ftpaDecisionDate='2025-11-02', ftpaApplicationDate='2025-11-02', ftpaGroundsDocuments=[], ftpaEvidenceDocuments=[], ftpaDecisionOutcomeType='remadeRule32', ftpaAppellantGroundsText='', ftpaDecisionRemadeRule32Text='This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.', isFtpaNoticeOfDecisionSetAside='No'))
    ]

    # CASE006 – appellant, explanation provided
    assert results["CASE006"]["ftpaList"] == [
        Row(id='1', value=Row(ftpaApplicant='appellant', ftpaDecisionDate='2026-12-03', ftpaApplicationDate='2026-12-03', ftpaGroundsDocuments=[], ftpaEvidenceDocuments=[], ftpaDecisionOutcomeType='remadeRule32', ftpaAppellantGroundsText='', ftpaDecisionRemadeRule32Text='This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.', isFtpaNoticeOfDecisionSetAside='No'))
    ]

    # CASE007 – respondent, no explanation
    assert results["CASE007"]["ftpaList"] == [
        Row(id='1', value=Row(ftpaApplicant='respondent', ftpaDecisionDate='2026-08-03', ftpaApplicationDate='2026-08-03', ftpaGroundsDocuments=[], ftpaEvidenceDocuments=[], ftpaDecisionOutcomeType='remadeRule32', ftpaAppellantGroundsText='', ftpaDecisionRemadeRule32Text='This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.', isFtpaNoticeOfDecisionSetAside='No'))
    ]

    # CASE010 – respondent, explanation provided
    assert results["CASE010"]["ftpaList"] == [
        Row(id='1', value=Row(ftpaApplicant='appellant', ftpaDecisionDate=None, ftpaApplicationDate=None, ftpaGroundsDocuments=[], ftpaEvidenceDocuments=[], ftpaDecisionOutcomeType='remadeRule32', ftpaAppellantGroundsText='', ftpaDecisionRemadeRule32Text='This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.', isFtpaNoticeOfDecisionSetAside='No'))
    ]

    # CASE011 – respondent, explanation provided
    assert results["CASE011"]["ftpaList"] == [
        Row(id='1', value=Row(ftpaApplicant='respondent', ftpaDecisionDate='2025-11-02', ftpaApplicationDate='2025-11-02', ftpaGroundsDocuments=[], ftpaEvidenceDocuments=[], ftpaDecisionOutcomeType='remadeRule32', ftpaAppellantGroundsText='', ftpaDecisionRemadeRule32Text='This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.', isFtpaNoticeOfDecisionSetAside='No'))
    ]

def test_ftpaFirstDecision(spark,ftpa_outputs):

    results = ftpa_outputs

    assert results["CASE005"]["ftpaFirstDecision"] == "remadeRule32"
    assert results["CASE006"]["ftpaFirstDecision"] == "remadeRule32"
    assert results["CASE007"]["ftpaFirstDecision"] == 'remadeRule32'
    assert results["CASE010"]["ftpaFirstDecision"] == 'remadeRule32'
    assert results["CASE011"]["ftpaFirstDecision"] == 'remadeRule32'

def test_ftpaFinalDecisionForDisplay(spark,ftpa_outputs):

    results = ftpa_outputs

    assert results["CASE005"]["ftpaFinalDecisionForDisplay"] == 'undecided'
    assert results["CASE006"]["ftpaFinalDecisionForDisplay"] == 'undecided'
    assert results["CASE007"]["ftpaFinalDecisionForDisplay"] == 'undecided'
    assert results["CASE010"]["ftpaFinalDecisionForDisplay"] == 'undecided'
    assert results["CASE011"]["ftpaFinalDecisionForDisplay"] == 'undecided'

def test_ftpaApplicantType(spark,ftpa_outputs):

    results = ftpa_outputs

    assert results["CASE005"]["ftpaApplicantType"] == 'appellant'
    assert results["CASE006"]["ftpaApplicantType"] == 'appellant'
    assert results["CASE007"]["ftpaApplicantType"] == 'respondent'
    assert results["CASE010"]["ftpaApplicantType"] == 'appellant'
    assert results["CASE011"]["ftpaApplicantType"] == 'respondent'


def test_ftpaAppellantDecisionDate(spark,ftpa_outputs):

    results = ftpa_outputs

    assert results["CASE005"]["ftpaAppellantDecisionDate"] == '2025-11-02'
    assert results["CASE006"]["ftpaAppellantDecisionDate"] == '2026-12-03'
    assert results["CASE007"]["ftpaAppellantDecisionDate"] == None
    assert results["CASE010"]["ftpaAppellantDecisionDate"] == None
    assert results["CASE011"]["ftpaAppellantDecisionDate"] == None


def test_ftpaRespondentDecisionDate(spark,ftpa_outputs):

    results = ftpa_outputs

    assert results["CASE005"]["ftpaRespondentDecisionDate"] == None
    assert results["CASE006"]["ftpaRespondentDecisionDate"] == None
    assert results["CASE007"]["ftpaRespondentDecisionDate"] == '2026-08-03'
    assert results["CASE010"]["ftpaRespondentDecisionDate"] == None
    assert results["CASE011"]["ftpaRespondentDecisionDate"] == '2025-11-02'

def test_ftpaAppellantRjDecisionOutcomeType(spark,ftpa_outputs):

    results = ftpa_outputs

    assert results["CASE005"]["ftpaAppellantRjDecisionOutcomeType"] == 'remadeRule32'
    assert results["CASE006"]["ftpaAppellantRjDecisionOutcomeType"] == 'remadeRule32'
    assert results["CASE007"]["ftpaAppellantRjDecisionOutcomeType"] == None
    assert results["CASE010"]["ftpaAppellantRjDecisionOutcomeType"] == 'remadeRule32'
    assert results["CASE011"]["ftpaAppellantRjDecisionOutcomeType"] == None

def test_ftpaRespondentRjDecisionOutcomeType(spark,ftpa_outputs):

    results = ftpa_outputs

    assert results["CASE005"]["ftpaRespondentRjDecisionOutcomeType"] == None
    assert results["CASE006"]["ftpaRespondentRjDecisionOutcomeType"] == None
    assert results["CASE007"]["ftpaRespondentRjDecisionOutcomeType"] == 'remadeRule32'
    assert results["CASE010"]["ftpaRespondentRjDecisionOutcomeType"] == None
    assert results["CASE011"]["ftpaRespondentRjDecisionOutcomeType"] == 'remadeRule32'

