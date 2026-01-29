from Databricks.ACTIVE.APPEALS.shared_functions.decided_a import substantiveDecision
from pyspark.sql import SparkSession
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("substantiveDecisionTests")
        .getOrCreate()
    )

##### Testing the substantiveDecision field grouping function #####
@pytest.fixture(scope="session")
def substantiveDecision_outputs(spark):
    m1_data = [
        ("CASE001", "AIP", "FT"),
        ("CASE002", "LR", "FT"),
        ("CASE003", None, None)
        ("CASE004", "AIP", "FT"),
        ("CASE005", "LR", "FT"),
        ("CASE006", "AIP", "FT")
        ("CASE007", None, None)
        ("CASE008", "AIP", "FT")
    ]
    m1_schema = ["CaseNo", "dv_representation", "lu_appealType"]

    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("HearingDuration", T.IntegerType(), True),
        T.StructField("HearingCentre", T.StringType(), True),
        T.StructField("DecisionDate", T.StringType(), True),
        T.StructField("StartTime", T.StringType(), True),
        T.StructField("Outcome", T.IntegerType(), True),
    ])

    m3_data = [
        ("CASE001", 1, 37, 180, "LOC001","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00",1),
        ("CASE002", 2, 37, 60, "LOC002","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.000+00:00",2),   
        ("CASE003", 1, 38, 240, "LOC003","2026-12-03T00:00:00.000+00:00","1899-12-30T13:00:00.000+00:00",3),   
        ("CASE004", 1, 26, 360, "LOC004","2026-08-03T00:00:00.000+00:00","2000-12-30T07:10:58.000+00:00"1),  
        ("CASE005", 1, 37, None, "LOC005","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00",2),  
        ("CASE006", 1, 37, 30, "LOC006","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00",3),  
        ("CASE007", 1, 38, None, "LOC007","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00",None),  
        ("CASE008", 1, 39, 45, "LOC008","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.999+00:00"1)   
        ]   
    
    df_m1 =  spark.createDataFrame(m1_data, m1_schema)
    df_m3 = spark.createDataFrame(m3_data, m3_schema)

    substantiveDecision_content,_ = substantiveDecision(df_m1,df_m3)

    results = {row["CaseNo"]: row.asDict() for row in substantiveDecision_content.collect()}
    return results


def test_anonymityOrder(spark,substantiveDecision_outputs):

    results = substantiveDecision_outputs

    assert results["CASE001"]["anonymityOrder"] == "No"
    assert results["CASE002"]["anonymityOrder"] == "No"
    assert results["CASE003"]["anonymityOrder"] == "No"

def test_sendDecisionsAndReasonsDate(spark,substantiveDecision_outputs):

    results = substantiveDecision_outputs

    assert results["CASE001"]["sendDecisionsAndReasonsDate"] == "This is a migrated ARIA case. Please see the documents for information on the schedule of issues."
    assert results["CASE002"]["sendDecisionsAndReasonsDate"] == "This is a migrated ARIA case. Please see the documents for information on the schedule of issues."
    assert results["CASE003"]["sendDecisionsAndReasonsDate"] == "This is a migrated ARIA case. Please see the documents for information on the schedule of issues."

def test_appealDate(spark,substantiveDecision_outputs):

    results = substantiveDecision_outputs

    assert results["CASE001"]["appealDate"] == "No"
    assert results["CASE002"]["appealDate"] == "No"
    assert results["CASE003"]["appealDate"] == "No"

def test_appealDecision(spark,substantiveDecision_outputs):

    results = substantiveDecision_outputs

    assert results["CASE001"]["appealDecision"] == "This is a migrated ARIA case. Please see the documents for information on the immigration history."
    assert results["CASE002"]["appealDecision"] == "This is a migrated ARIA case. Please see the documents for information on the immigration history."
    assert results["CASE003"]["appealDecision"] == "This is a migrated ARIA case. Please see the documents for information on the immigration history."

def test_isDecisionAllowed(spark,substantiveDecision_outputs):

    results = substantiveDecision_outputs

    assert results["CASE001"]["isDecisionAllowed"] == "This is a migrated ARIA case. Please see the documents for information on the immigration history."
    assert results["CASE002"]["isDecisionAllowed"] == "This is a migrated ARIA case. Please see the documents for information on the immigration history."
    assert results["CASE003"]["isDecisionAllowed"] == "This is a migrated ARIA case. Please see the documents for information on the immigration history."

