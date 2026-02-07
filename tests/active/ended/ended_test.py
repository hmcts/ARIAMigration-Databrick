from Databricks.ACTIVE.APPEALS.shared_functions.ended import ended
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

##### Testing the ended field grouping function #####
@pytest.fixture(scope="session")
def ended_outputs(spark):


    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("HearingDuration", T.IntegerType(), True),
        T.StructField("HearingCentre", T.StringType(), True),
        T.StructField("DateReceived", T.StringType(), True),
        T.StructField("DecisionDate", T.StringType(), True),
        T.StructField("Adj_Determination_Title", T.StringType(), True),
        T.StructField("Adj_Determination_Forenames", T.StringType(), True),
        T.StructField("Adj_Determination_Surname", T.StringType(), True),
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

    es_schema = T.StructType([
    T.StructField("CaseNo", T.StringType(), True),
    T.StructField("CategoryId", T.IntegerType(), True),
    ])
    
    es_data = [
        ("CASE005", 37),  
        ("CASE006", 37),  
        ("CASE007", 37),  
        ("CASE008", 37),  
        ("CASE009", 37), 
        ("CASE010", 37),
        ("CASE011", 37),
        ]
    
    df_m3 =  spark.createDataFrame(m3_data, m3_schema)
    df_es =  spark.createDataFrame(es_data, es_schema)


    ended_content,_ = ended(df_m3, df_es)
    results = {row["CaseNo"]: row.asDict() for row in ended_content.collect()}
    
    return results

def test_endAppealOutcome(spark,ended_outputs):

    results = ended_outputs

    assert results["CASE005"]["endAppealOutcome"] == "02/11/2025"
    assert results["CASE006"]["endAppealOutcome"] == "03/12/2026"
    assert results["CASE007"]["endAppealOutcome"] == None
    assert results["CASE010"]["endAppealOutcome"] == None

def test_endAppealOutcomeReason(spark,ended_outputs):

    results = ended_outputs

    assert results["CASE005"]["endAppealOutcomeReason"] == "No"
    assert results["CASE006"]["endAppealOutcomeReason"] == "Yes"
    assert results["CASE007"]["endAppealOutcomeReason"] == None
    assert results["CASE010"]["endAppealOutcomeReason"] == None

def test_endAppealApproverType(spark,ended_outputs):

    results = ended_outputs

    assert results["CASE005"]["endAppealApproverType"] == None
    assert results["CASE006"]["endAppealApproverType"] == "This is a migrated ARIA case. Please refer to the documents."
    assert results["CASE007"]["endAppealApproverType"] == None
    assert results["CASE010"]["endAppealApproverType"] == None


def test_endAppealApproverName(spark,ended_outputs):

    results = ended_outputs

    assert results["CASE005"]["endAppealApproverName"] == None
    assert results["CASE006"]["endAppealApproverName"] == None
    assert results["CASE007"]["endAppealApproverName"] == "03/08/2026"
    assert results["CASE011"]["endAppealApproverName"] == "02/11/2025"


def test_endAppealDate(spark,ended_outputs):

    results = ended_outputs

    assert results["CASE005"]["endAppealDate"] == None
    assert results["CASE006"]["endAppealDate"] == None
    assert results["CASE007"]["endAppealDate"] == "No"
    assert results["CASE011"]["endAppealDate"] == "Yes"

def test_stateBeforeEndAppeal(spark,ended_outputs):

    results = ended_outputs

    assert results["CASE005"]["stateBeforeEndAppeal"] == None
    assert results["CASE006"]["stateBeforeEndAppeal"] == None
    assert results["CASE007"]["stateBeforeEndAppeal"] == None
    assert results["CASE011"]["stateBeforeEndAppeal"] == "This is a migrated ARIA case. Please refer to the documents."

