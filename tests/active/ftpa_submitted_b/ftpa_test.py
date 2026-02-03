from Databricks.ACTIVE.APPEALS.shared_functions.ftpa_submitted_b import ftpa
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

def test_judgeAllocationExists(spark,ftpa_outputs):

    results = ftpa_outputs

    assert results["CASE005"]["judgeAllocationExists"] == "Yes"
    assert results["CASE006"]["judgeAllocationExists"] == "Yes"
    assert results["CASE007"]["judgeAllocationExists"] == "Yes"
    assert results["CASE010"]["judgeAllocationExists"] == "Yes"

def test_allocatedJudge(spark,ftpa_outputs):

    results = ftpa_outputs

    assert results["CASE005"]["allocatedJudge"] == "No"
    assert results["CASE006"]["allocatedJudge"] == "Yes"
    assert results["CASE007"]["allocatedJudge"] == None
    assert results["CASE010"]["allocatedJudge"] == None

def test_allocatedJudgeEdit(spark,ftpa_outputs):

    results = ftpa_outputs

    assert results["CASE005"]["allocatedJudgeEdit"] == None
    assert results["CASE006"]["allocatedJudgeEdit"] == "This is a migrated ARIA case. Please refer to the documents."
    assert results["CASE007"]["allocatedJudgeEdit"] == None
    assert results["CASE010"]["allocatedJudgeEdit"] == None



