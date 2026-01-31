from Databricks.ACTIVE.APPEALS.shared_functions.decided_a import ftpa
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
        T.StructField("DecisionDate", T.StringType(), True),
        T.StructField("StartTime", T.StringType(), True),
        T.StructField("Adj_Determination_Title", T.StringType(), True),
        T.StructField("Adj_Determination_Forenames", T.StringType(), True),
        T.StructField("Adj_Determination_Surname", T.StringType(), True),
        T.StructField("Outcome", T.IntegerType(), True),
    ])

    m3_data = [
        ("CASE005", 1, 37, 180, "LOC001","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Mr","Doe","John",1),
        ("CASE005", 2, 37, 60, "LOC002","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.000+00:00","Ms","Doe","Jane",1),   
        ("CASE006", 1, 38, 240, "LOC003","2026-12-03T00:00:00.000+00:00","1899-12-30T13:00:00.000+00:00","Mr","xyz","John",2),   
        ("CASE007", 1, 38, 360, "LOC004","2026-08-03T00:00:00.000+00:00","2000-12-30T07:10:58.000+00:00","Mr","Doe","abc",2),  
        ("CASE008", 1, 37, None, "LOC005","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Sir","Random","Guy",None),  
        ("CASE009", 1, 37, 30, "LOC006","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00","Mr","John","Snow",3),  
        ("CASE010", 1, 38, None, "LOC007",None,"1899-12-30T10:00:00.000+00:00",None,None,None,1),  
        ("CASE011", 1, 38, 45, "LOC008","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.999+00:00","Mr","Hello","World",2)   
        ]      

    

    c_schema = T.StructType([
    T.StructField("CaseNo", T.StringType(), True),
    T.StructField("CategoryId", T.IntegerType(), True),
    ])
    
    c_data = [
        ("CASE005", 37),  
        ("CASE006", 38),  
        ("CASE007", None),  
        ("CASE008", None),  
        ("CASE009", None), 
        ("CASE010", None),
        ]
    
    df_m1 =  spark.createDataFrame(m1_data, m1_schema)
    df_m2 =  spark.createDataFrame(m2_data, m2_schema)
    df_m3 =  spark.createDataFrame(m3_data, m3_schema)
    df_c =  spark.createDataFrame(c_data, c_schema)


    ftpa_content,_ = ftpa(df_m3, df_c)
    results = {row["CaseNo"]: row.asDict() for row in ftpa_content.collect()}
    
    return results

def test_ftpaApplicationDeadline(spark,ftpa_outputs):

    results = ftpa_outputs

    assert results["CASE005"]["ftpaApplicationDeadline"] == "16/11/2025"
    assert results["CASE006"]["ftpaApplicationDeadline"] == "31/12/2026"
    assert results["CASE007"]["ftpaApplicationDeadline"] == '03/08/2026'
    assert results["CASE010"]["ftpaApplicationDeadline"] == None


