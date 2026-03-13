from Databricks.ACTIVE.APPEALS.shared_functions.decided_b import setAside
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
def setAside_outputs(spark):

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
    
    m6_schema = T.StructType([
    T.StructField("CaseNo", T.StringType(), True),
    T.StructField("Required", T.BooleanType(), True),
    T.StructField("Judge_Surname", T.StringType(), True),
    T.StructField("Judge_Title", T.StringType(), True),
    T.StructField("Judge_Forenames", T.StringType(), True)])
    
    m6_data = [
        ("CASE001", True ,"Gold", "Mr","James"),  
        ("CASE002", True, "Smith", "Ms","Maria"),  
        ("CASE003", True, "Johns", "Dr","Peter"),  
        ("CASE004", False, "Green", None,None),  
        ("CASE005", False, "Black", "Sir","Tim"),  
        ("CASE006", True ,"Gold", "Mr","James"),  
        ("CASE007", True, "Smith", "Ms","Maria"),  
        ("CASE008", True, "Johns", "Dr","Peter"),  
        ("CASE009", False, "Green", None,None),  
        ("CASE010", False, "pearn", "Mr","Leo"),  
        ("CASE011", False, "Finney", "Prof","Alex"),  
        ]


    df_m1 =  spark.createDataFrame(m1_data, m1_schema)  
    df_m3 =  spark.createDataFrame(m3_data, m3_schema)
    df_m6 =  spark.createDataFrame(m6_data, m6_schema)


    setAside_content,_ = setAside(df_m1, df_m3, df_m6)
    results = {row["CaseNo"]: row.asDict() for row in setAside_content.collect()}
    
    return results
def test_judgesNamesToExclude(spark,setAside_outputs):

    results = setAside_outputs

    assert results["CASE005"]["judgesNamesToExclude"] == 'Black, Tim (Sir)'
    assert results["CASE006"]["judgesNamesToExclude"] == ''
    assert results["CASE007"]["judgesNamesToExclude"] == ''
    assert results["CASE010"]["judgesNamesToExclude"] == 'pearn, Leo (Mr)'
    assert results["CASE011"]["judgesNamesToExclude"] == 'Finney, Alex (Prof)'

def test_reasonRehearingRule32(spark,setAside_outputs):

    results = setAside_outputs

    assert results["CASE005"]["reasonRehearingRule32"] == 'Set aside and to be reheard under rule 32'
    assert results["CASE006"]["reasonRehearingRule32"] == 'Set aside and to be reheard under rule 32'
    assert results["CASE007"]["reasonRehearingRule32"] == 'Set aside and to be reheard under rule 32'
    assert results["CASE010"]["reasonRehearingRule32"] == 'Set aside and to be reheard under rule 32'
    assert results["CASE011"]["reasonRehearingRule32"] == 'Set aside and to be reheard under rule 32'

def test_rule32ListingAdditionalIns(spark,setAside_outputs):

    results = setAside_outputs

    assert results["CASE005"]["rule32ListingAdditionalIns"] == 'This is an ARIA Migrated case. Please refer to the documents for any additional listing instructions.'
    assert results["CASE006"]["rule32ListingAdditionalIns"] == 'This is an ARIA Migrated case. Please refer to the documents for any additional listing instructions.'
    assert results["CASE007"]["rule32ListingAdditionalIns"] == 'This is an ARIA Migrated case. Please refer to the documents for any additional listing instructions.'
    assert results["CASE010"]["rule32ListingAdditionalIns"] == 'This is an ARIA Migrated case. Please refer to the documents for any additional listing instructions.'
    assert results["CASE011"]["rule32ListingAdditionalIns"] == 'This is an ARIA Migrated case. Please refer to the documents for any additional listing instructions.'

def test_updateTribunalDecisionList(spark,setAside_outputs):

    results = setAside_outputs

    assert results["CASE005"]["updateTribunalDecisionList"] == 'underRule32'
    assert results["CASE006"]["updateTribunalDecisionList"] == 'underRule32'
    assert results["CASE007"]["updateTribunalDecisionList"] == 'underRule32'
    assert results["CASE010"]["updateTribunalDecisionList"] == 'underRule32'
    assert results["CASE011"]["updateTribunalDecisionList"] == 'underRule32'

def test_ftpaFinalDecisionRemadeRule32(spark,setAside_outputs):

    results = setAside_outputs

    assert results["CASE005"]["ftpaFinalDecisionRemadeRule32"] == ''
    assert results["CASE006"]["ftpaFinalDecisionRemadeRule32"] == ''
    assert results["CASE007"]["ftpaFinalDecisionRemadeRule32"] == ''
    assert results["CASE010"]["ftpaFinalDecisionRemadeRule32"] == ''
    assert results["CASE011"]["ftpaFinalDecisionRemadeRule32"] == ''


def test_updateTribunalDecisionDateRule32(spark,setAside_outputs):

    results = setAside_outputs

    assert results["CASE005"]["updateTribunalDecisionDateRule32"] == '2025-11-02'
    assert results["CASE006"]["updateTribunalDecisionDateRule32"] == '2026-12-03'
    assert results["CASE007"]["updateTribunalDecisionDateRule32"] == '2026-08-03'
    assert results["CASE010"]["updateTribunalDecisionDateRule32"] == None
    assert results["CASE011"]["updateTribunalDecisionDateRule32"] == '2025-11-02'

def test_ftpaAppellantDecisionRemadeRule32Text(spark,setAside_outputs):

    results = setAside_outputs

    assert results["CASE005"]["ftpaAppellantDecisionRemadeRule32Text"] == 'This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.'
    assert results["CASE006"]["ftpaAppellantDecisionRemadeRule32Text"] == 'This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.'
    assert results["CASE007"]["ftpaAppellantDecisionRemadeRule32Text"] == None
    assert results["CASE010"]["ftpaAppellantDecisionRemadeRule32Text"] == 'This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.'
    assert results["CASE011"]["ftpaAppellantDecisionRemadeRule32Text"] == None

def test_ftpaRespondentDecisionRemadeRule32Text(spark,setAside_outputs):

    results = setAside_outputs

    assert results["CASE005"]["ftpaRespondentDecisionRemadeRule32Text"] == None
    assert results["CASE006"]["ftpaRespondentDecisionRemadeRule32Text"] == None
    assert results["CASE007"]["ftpaRespondentDecisionRemadeRule32Text"] == 'This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.'
    assert results["CASE010"]["ftpaRespondentDecisionRemadeRule32Text"] == None
    assert results["CASE011"]["ftpaRespondentDecisionRemadeRule32Text"] == 'This is an ARIA Migrated case. Please refer to the documents for the notice to set aside.'


