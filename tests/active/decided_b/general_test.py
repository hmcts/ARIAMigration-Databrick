from Databricks.ACTIVE.APPEALS.shared_functions.ftpa_submitted_a import general
from pyspark.sql import SparkSession
import pytest

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
def general_outputs(spark):

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
    T.StructField("Appellant_Name", T.StringType(), True),
    T.StructField("Appellant_Postcode", T.StringType(), True),
    T.StructField("Relationship", T.StringType(), True)])
    
    m2_data = [
        ("CASE001", "Gold", "B12 0hf","Relationship1"),  
        ("CASE002", "Smith", "M8 1XY","Relationship2"),  
        ("CASE003", "Johns", "DE4 9HN","Relationship3"),  
        ("CASE004", "Black", "BN6 0PA","Relationship4"),  
        ("CASE005", "Green", "DD7 7PT",None), 
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
        T.StructField("OutOfTime", T.IntegerType(), True)
    ])

    m3_data = [
        ("CASE005", 1, 39, 180, "LOC001","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Mr","Doe","John",1,1),
        ("CASE005", 2, 39, 60, "LOC002","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.000+00:00","Ms","Doe","Jane",1,0),   
        ("CASE006", 1, 39, 240, "LOC003","2026-12-03T00:00:00.000+00:00","1899-12-30T13:00:00.000+00:00","Mr","xyz","John",1,1),   
        ("CASE007", 1, 39, 360, "LOC004","2026-08-03T00:00:00.000+00:00","2000-12-30T07:10:58.000+00:00","Mr","Doe","abc",2,0),  
        ("CASE008", 1, 39, None, "LOC005","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Sir","Random","Guy",1,1),  
        ("CASE009", 1, 39, 30, "LOC006","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Mr","John","Snow",2,0),  
        ("CASE010", 1, 39, None, "LOC007",None,"1899-12-30T10:00:00.000+00:00",None,None,None,1,None),  
        ("CASE011", 1, 39, 45, "LOC008","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.999+00:00","Mr","Hello","World",2,1),   
        ]  

    bhc_schema = T.StructType([
        T.StructField("CentreId", T.IntegerType(), True),
        T.StructField("locationCode", T.StringType(), True),
        T.StructField("locationLabel", T.StringType(), True),
        T.StructField("applicationChangeDesignatedHearingCentre", T.StringType(), True),
        T.StructField("prevFileLocation", T.StringType(), True),
        T.StructField("hearingCentre", T.StringType(), True),
        T.StructField("Conditions", T.StringType(), True),
        ])

    bhc_data = [
        (1, "123", "Court1","Bham","123 xyz","Man","1st cond"),   
        (2, "456", "Court2","Man","123 abc","Bham","2nd cond"),   
        (3, "789", "Court3","Scot","456 asd","Cov","3rd cond"),   
        (4, None, "Court4","Cov","7676 jgfd","Scot","4th cond"),  
        (5, None, "Court5","Nor","954 bbb","ply","5th cond"),   
        (6, "xyz", "Court6","ply","456 mmm","Nor","6th cond"),  
        ]
    
    mh_schema = T.StructType([
    T.StructField("CaseNo", T.StringType(), True),
    T.StructField("HistType", T.IntegerType(), True),
    T.StructField("Comment", T.StringType(), True),
    T.StructField("HistoryId", T.IntegerType(), True)])
    
    mh_data = [
        ("CASE001", 6,"Comment1",1),  
        ("CASE002",6,"Comment2",2),  
        ("CASE003", 6,"Comment3",3),  
        ("CASE004",12,"Comment4",4),  
        ("CASE005", 15,"Comment5",5),  
        ("CASE006", 15,"Comment6",6),  
        ("CASE007", 15,"Comment7",7), 
        ]


    bdhc_schema = T.StructType([
        T.StructField("selectedHearingCentreRefData", T.StringType(), True),
        T.StructField("locationCode", T.StringType(), True),
        T.StructField("locationLabel", T.StringType(), True),
        T.StructField("applicationChangeDesignatedHearingCentre", T.StringType(), True),
        T.StructField("hearingCentre", T.StringType(), True),
        ])
    
    bdhc_data = [
        ("Bradford Centre", "123", "Court1","Bham","Man"),   
        ("Birmingham Centre", "456", "Court2","Man","Bham"),   
        ("Manchester Centre", "789", "Court3","Scot","Cov"),   
        ("Random Centre", None, "Court4","Cov","Scot"),  
        ("Coventry Centre", None, "Court5","Nor","ply"),   
        ("London Centre", "xyz", "Court6","ply","Nor"),  
        ]
    

    df_m1 =  spark.createDataFrame(m1_data, m1_schema)
    df_m2 =  spark.createDataFrame(m2_data, m2_schema)
    df_m3 =  spark.createDataFrame(m3_data, m3_schema)
    df_mh =  spark.createDataFrame(mh_data, mh_schema)
    df_bhc =  spark.createDataFrame(bhc_data, bhc_schema)
    df_bdhc =  spark.createDataFrame(bdhc_data, bdhc_schema)

    general_content,_ = general(df_m1, df_m2, df_m3, df_mh, df_bhc, df_bdhc)
    results = {row["CaseNo"]: row.asDict() for row in general_content.collect()}
    
    return results

def test_ftpaAppellantSubmitted(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["ftpaAppellantSubmitted"] == "Yes"
    assert results["CASE006"]["ftpaAppellantSubmitted"] == "Yes"
    assert results["CASE007"]["ftpaAppellantSubmitted"] == None
    assert results["CASE010"]["ftpaAppellantSubmitted"] == "Yes"

def test_isFtpaAppellantDocsVisibleInDecided(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaAppellantDocsVisibleInDecided"] == "No"
    assert results["CASE006"]["isFtpaAppellantDocsVisibleInDecided"] == "No"
    assert results["CASE007"]["isFtpaAppellantDocsVisibleInDecided"] == None
    assert results["CASE010"]["isFtpaAppellantDocsVisibleInDecided"] == "No"

def test_isFtpaAppellantDocsVisibleInSubmitted(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaAppellantDocsVisibleInSubmitted"] == "Yes"
    assert results["CASE006"]["isFtpaAppellantDocsVisibleInSubmitted"] == "Yes"
    assert results["CASE007"]["isFtpaAppellantDocsVisibleInSubmitted"] == None
    assert results["CASE010"]["isFtpaAppellantDocsVisibleInSubmitted"] == "Yes"


def test_isFtpaAppellantOotDocsVisibleInDecided(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaAppellantOotDocsVisibleInDecided"] == None
    assert results["CASE006"]["isFtpaAppellantOotDocsVisibleInDecided"] == "No"
    assert results["CASE007"]["isFtpaAppellantOotDocsVisibleInDecided"] == None
    assert results["CASE008"]["isFtpaAppellantOotDocsVisibleInDecided"] == "No"


def test_isFtpaAppellantOotDocsVisibleInSubmitted(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaAppellantOotDocsVisibleInSubmitted"] == None
    assert results["CASE006"]["isFtpaAppellantOotDocsVisibleInSubmitted"] == "Yes"
    assert results["CASE007"]["isFtpaAppellantOotDocsVisibleInSubmitted"] == None
    assert results["CASE008"]["isFtpaAppellantOotDocsVisibleInSubmitted"] == "Yes"

def test_isFtpaAppellantGroundsDocsVisibleInDecided(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaAppellantGroundsDocsVisibleInDecided"] == "No"
    assert results["CASE006"]["isFtpaAppellantGroundsDocsVisibleInDecided"] == "No"
    assert results["CASE007"]["isFtpaAppellantGroundsDocsVisibleInDecided"] == None
    assert results["CASE008"]["isFtpaAppellantGroundsDocsVisibleInDecided"] == "No"

def test_isFtpaAppellantEvidenceDocsVisibleInDecided(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaAppellantEvidenceDocsVisibleInDecided"] == "No"
    assert results["CASE006"]["isFtpaAppellantEvidenceDocsVisibleInDecided"] == "No"
    assert results["CASE007"]["isFtpaAppellantEvidenceDocsVisibleInDecided"] == None
    assert results["CASE008"]["isFtpaAppellantEvidenceDocsVisibleInDecided"] == "No"

def test_isFtpaAppellantGroundsDocsVisibleInSubmitted(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaAppellantGroundsDocsVisibleInSubmitted"] == "Yes"
    assert results["CASE006"]["isFtpaAppellantGroundsDocsVisibleInSubmitted"] == "Yes"
    assert results["CASE007"]["isFtpaAppellantGroundsDocsVisibleInSubmitted"] == None
    assert results["CASE008"]["isFtpaAppellantGroundsDocsVisibleInSubmitted"] == "Yes"

def test_isFtpaAppellantEvidenceDocsVisibleInSubmitted(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaAppellantEvidenceDocsVisibleInSubmitted"] == "Yes"
    assert results["CASE006"]["isFtpaAppellantEvidenceDocsVisibleInSubmitted"] == "Yes"
    assert results["CASE007"]["isFtpaAppellantEvidenceDocsVisibleInSubmitted"] == None
    assert results["CASE008"]["isFtpaAppellantEvidenceDocsVisibleInSubmitted"] == "Yes"

def test_isFtpaAppellantOotExplanationVisibleInDecided(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaAppellantOotExplanationVisibleInDecided"] == None
    assert results["CASE006"]["isFtpaAppellantOotExplanationVisibleInDecided"] == "No"
    assert results["CASE007"]["isFtpaAppellantOotExplanationVisibleInDecided"] == None
    assert results["CASE008"]["isFtpaAppellantOotExplanationVisibleInDecided"] == "No"

def test_isFtpaAppellantOotExplanationVisibleInSubmitted(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaAppellantOotExplanationVisibleInSubmitted"] == None
    assert results["CASE006"]["isFtpaAppellantOotExplanationVisibleInSubmitted"] == "Yes"
    assert results["CASE007"]["isFtpaAppellantOotExplanationVisibleInSubmitted"] == None
    assert results["CASE008"]["isFtpaAppellantOotExplanationVisibleInSubmitted"] == "Yes"

###############################################################################################################


def test_ftpaRespondentSubmitted(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["ftpaRespondentSubmitted"] == None
    assert results["CASE006"]["ftpaRespondentSubmitted"] == None
    assert results["CASE007"]["ftpaRespondentSubmitted"] == "Yes"
    assert results["CASE011"]["ftpaRespondentSubmitted"] == "Yes"

def test_isFtpaRespondentDocsVisibleInDecided(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaRespondentDocsVisibleInDecided"] == None
    assert results["CASE006"]["isFtpaRespondentDocsVisibleInDecided"] == None
    assert results["CASE007"]["isFtpaRespondentDocsVisibleInDecided"] == "No"
    assert results["CASE011"]["isFtpaRespondentDocsVisibleInDecided"] == "No"

def test_isFtpaRespondentDocsVisibleInSubmitted(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaRespondentDocsVisibleInSubmitted"] == None
    assert results["CASE006"]["isFtpaRespondentDocsVisibleInSubmitted"] == None
    assert results["CASE007"]["isFtpaRespondentDocsVisibleInSubmitted"] == "Yes"
    assert results["CASE011"]["isFtpaRespondentDocsVisibleInSubmitted"] == "Yes"


def test_isFtpaRespondentOotDocsVisibleInDecided(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaRespondentOotDocsVisibleInDecided"] == None
    assert results["CASE006"]["isFtpaRespondentOotDocsVisibleInDecided"] == None
    assert results["CASE007"]["isFtpaRespondentOotDocsVisibleInDecided"] == None
    assert results["CASE011"]["isFtpaRespondentOotDocsVisibleInDecided"] == "No"


def test_isFtpaRespondentOotDocsVisibleInSubmitted(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaRespondentOotDocsVisibleInSubmitted"] == None
    assert results["CASE006"]["isFtpaRespondentOotDocsVisibleInSubmitted"] == None
    assert results["CASE007"]["isFtpaRespondentOotDocsVisibleInSubmitted"] == None
    assert results["CASE011"]["isFtpaRespondentOotDocsVisibleInSubmitted"] == "Yes"

def test_isFtpaRespondentGroundsDocsVisibleInDecided(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaRespondentGroundsDocsVisibleInDecided"] == None
    assert results["CASE006"]["isFtpaRespondentGroundsDocsVisibleInDecided"] == None
    assert results["CASE007"]["isFtpaRespondentGroundsDocsVisibleInDecided"] == "No"
    assert results["CASE011"]["isFtpaRespondentGroundsDocsVisibleInDecided"] == "No"

def test_isFtpaRespondentEvidenceDocsVisibleInDecided(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaRespondentEvidenceDocsVisibleInDecided"] == None
    assert results["CASE006"]["isFtpaRespondentEvidenceDocsVisibleInDecided"] == None
    assert results["CASE007"]["isFtpaRespondentEvidenceDocsVisibleInDecided"] == "No"
    assert results["CASE011"]["isFtpaRespondentEvidenceDocsVisibleInDecided"] == "No"

def test_isFtpaRespondentGroundsDocsVisibleInSubmitted(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaRespondentGroundsDocsVisibleInSubmitted"] == None
    assert results["CASE006"]["isFtpaRespondentGroundsDocsVisibleInSubmitted"] == None
    assert results["CASE007"]["isFtpaRespondentGroundsDocsVisibleInSubmitted"] == "Yes"
    assert results["CASE011"]["isFtpaRespondentGroundsDocsVisibleInSubmitted"] == "Yes"

def test_isFtpaRespondentEvidenceDocsVisibleInSubmitted(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaRespondentEvidenceDocsVisibleInSubmitted"] == None
    assert results["CASE006"]["isFtpaRespondentEvidenceDocsVisibleInSubmitted"] == None
    assert results["CASE007"]["isFtpaRespondentEvidenceDocsVisibleInSubmitted"] == "Yes"
    assert results["CASE011"]["isFtpaRespondentEvidenceDocsVisibleInSubmitted"] == "Yes"

def test_isFtpaRespondentOotExplanationVisibleInDecided(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaRespondentOotExplanationVisibleInDecided"] == None
    assert results["CASE006"]["isFtpaRespondentOotExplanationVisibleInDecided"] == None
    assert results["CASE007"]["isFtpaRespondentOotExplanationVisibleInDecided"] == None
    assert results["CASE011"]["isFtpaRespondentOotExplanationVisibleInDecided"] == "No"

def test_isFtpaRespondentOotExplanationVisibleInSubmitted(spark,general_outputs):

    results = general_outputs

    assert results["CASE005"]["isFtpaRespondentOotExplanationVisibleInSubmitted"] == None
    assert results["CASE006"]["isFtpaRespondentOotExplanationVisibleInSubmitted"] == None
    assert results["CASE007"]["isFtpaRespondentOotExplanationVisibleInSubmitted"] == None
    assert results["CASE011"]["isFtpaRespondentOotExplanationVisibleInSubmitted"] == "Yes"




