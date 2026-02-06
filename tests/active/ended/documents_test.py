from Databricks.ACTIVE.APPEALS.shared_functions.ftpa_submitted_a import documents
from pyspark.sql import SparkSession
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("DocumentsTests")
        .getOrCreate()
    )

##### Testing the documents field grouping function #####
@pytest.fixture(scope="session")
def documents_outputs(spark):

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
    

    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("HearingDuration", T.IntegerType(), True),
        T.StructField("HearingCentre", T.StringType(), True),
        T.StructField("DateReceived", T.StringType(), True),
        T.StructField("StartTime", T.StringType(), True),
        T.StructField("Adj_Title", T.StringType(), True),
        T.StructField("Adj_Forenames", T.StringType(), True),
        T.StructField("Adj_Surname", T.StringType(), True),
        T.StructField("Party", T.IntegerType(), True),
        T.StructField("OutOfTime", T.BooleanType(), True)
    ])

    m3_data = [
        ("CASE005", 1, 39, 180, "LOC001","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Mr","Doe","John",1,True),
        ("CASE005", 2, 39, 60, "LOC002","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.000+00:00","Ms","Doe","Jane",1,False),   
        ("CASE006", 1, 39, 240, "LOC003","2026-12-03T00:00:00.000+00:00","1899-12-30T13:00:00.000+00:00","Mr","xyz","John",1,True),   
        ("CASE007", 1, 39, 360, "LOC004","2026-08-03T00:00:00.000+00:00","2000-12-30T07:10:58.000+00:00","Mr","Doe","abc",2,False),  
        ("CASE008", 1, 39, None, "LOC005","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Sir","Random","Guy",None,True),  
        ("CASE009", 1, 39, 30, "LOC006","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Mr","John","Snow",2,False),  
        ("CASE010", 1, 39, None, "LOC007",None,"1899-12-30T10:00:00.000+00:00",None,None,None,1,None),  
        ("CASE011", 1, 39, 45, "LOC008","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.999+00:00","Mr","Hello","World",2,True)   
        ]   
    
    df_m1 =  spark.createDataFrame(m1_data, m1_schema)
    df_m3 =  spark.createDataFrame(m3_data, m3_schema)

    documents_content,_ = documents(df_m1,df_m3)

    results = {row["CaseNo"]: row.asDict() for row in documents_content.collect()}
    return results


def test_ftpaAppellantDocuments(spark,documents_outputs):

    results = documents_outputs

    assert results["CASE005"]["ftpaAppellantDocuments"] == []
    assert results["CASE006"]["ftpaAppellantDocuments"] == []
    assert results["CASE007"]["ftpaAppellantDocuments"] == None
    assert results["CASE010"]["ftpaAppellantDocuments"] == []


def test_ftpaRespondentDocuments(spark,documents_outputs):

    results = documents_outputs

    assert results["CASE005"]["ftpaRespondentDocuments"] == None
    assert results["CASE006"]["ftpaRespondentDocuments"] == None
    assert results["CASE007"]["ftpaRespondentDocuments"] == []
    assert results["CASE011"]["ftpaRespondentDocuments"] == []


def test_ftpaAppellantGroundsDocuments(spark,documents_outputs):

    results = documents_outputs

    assert results["CASE005"]["ftpaAppellantGroundsDocuments"] == []
    assert results["CASE006"]["ftpaAppellantGroundsDocuments"] == []
    assert results["CASE007"]["ftpaAppellantGroundsDocuments"] == None
    assert results["CASE010"]["ftpaAppellantGroundsDocuments"] == []

def test_ftpaRespondentGroundsDocuments(spark,documents_outputs):

    results = documents_outputs

    assert results["CASE005"]["ftpaRespondentGroundsDocuments"] == None
    assert results["CASE006"]["ftpaRespondentGroundsDocuments"] == None
    assert results["CASE007"]["ftpaRespondentGroundsDocuments"] == []
    assert results["CASE011"]["ftpaRespondentGroundsDocuments"] == []


def test_ftpaAppellantEvidenceDocuments(spark,documents_outputs):

    results = documents_outputs

    assert results["CASE005"]["ftpaAppellantEvidenceDocuments"] == []
    assert results["CASE006"]["ftpaAppellantEvidenceDocuments"] == []
    assert results["CASE007"]["ftpaAppellantEvidenceDocuments"] == None
    assert results["CASE010"]["ftpaAppellantEvidenceDocuments"] == []

def test_ftpaRespondentEvidenceDocuments(spark,documents_outputs):

    results = documents_outputs

    assert results["CASE005"]["ftpaRespondentEvidenceDocuments"] == None
    assert results["CASE006"]["ftpaRespondentEvidenceDocuments"] == None
    assert results["CASE007"]["ftpaRespondentEvidenceDocuments"] == []
    assert results["CASE011"]["ftpaRespondentEvidenceDocuments"] == []


def test_ftpaAppellantOutOfTimeDocuments(spark,documents_outputs):

    results = documents_outputs

    assert results["CASE005"]["ftpaAppellantOutOfTimeDocuments"] == []
    assert results["CASE006"]["ftpaAppellantOutOfTimeDocuments"] == []
    assert results["CASE007"]["ftpaAppellantOutOfTimeDocuments"] == None
    assert results["CASE010"]["ftpaAppellantOutOfTimeDocuments"] == []

def test_ftpaRespondentEvidenceDocuments(spark,documents_outputs):

    results = documents_outputs

    assert results["CASE005"]["ftpaRespondentOutOfTimeDocuments"] == None
    assert results["CASE006"]["ftpaRespondentOutOfTimeDocuments"] == None
    assert results["CASE007"]["ftpaRespondentOutOfTimeDocuments"] == []
    assert results["CASE011"]["ftpaRespondentOutOfTimeDocuments"] == []

