from Databricks.ACTIVE.APPEALS.shared_functions.decision import hearingDetails
from pyspark.sql import SparkSession
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("hearingDetailsTests")
        .getOrCreate()
    )

##### Testing the documents field grouping function #####
@pytest.fixture(scope="session")
def hearingDetails_outputs(spark):

    m1_schema = T.StructType([
    T.StructField("CaseNo", T.StringType(), True),
    T.StructField("dv_representation", T.StringType(), True),
    T.StructField("lu_appealType", T.StringType(), True),
    T.StructField("Sponsor_Name", T.StringType(), True),
    T.StructField("Interpreter", T.StringType(), True),
    T.StructField("CourtPreference", T.StringType(), True),
    T.StructField("InCamera", T.BooleanType(), True),
    T.StructField("VisitVisaType", T.IntegerType(), True),
    ])

    m1_data = [
        ("CASE001", "AIP", "FTPA", None, 0, 0, True, 1),  # LanguageCode 1 - Spoken Language
        ("CASE002", "AIP", "FTPA", None, 0, 0, False, 2),  # LanguageCode 5 - Spoken Language Manual Entry
        ("CASE003", "AIP", "FTPA", None, 0, 0, True, 2),  # LanguageCode 6 - Sign Language
        ("CASE004", "AIP", "FTPA", None, 0, 0, False, 2),  # LanguageCode 7 - Sign Language Manual Entry
        ("CASE005", "AIP", "FT", None, 0, 0, True, 3), 
        ("CASE006", "AIP", "FT", None, 0, 0, True, 4),    # For m3 conditional tests - Additional Language Spoken + Spoken Manual
        ("CASE007", "AIP", "FT", None, 0, 0, False, None),    # For m3 conditional tests - Additional Language Spoken + Sign
        ("CASE008", "AIP", "FT", None, 0, 0, False, None),    # For m3 conditional tests - Additional Language Spoken + Sign Manual
        ("CASE009", "AIP", "FT", None, 0, 0, None, 2),    # For m3 conditional tests - Additional Language Sign + Sign
        ("CASE010", "AIP", "FT", None, 0, 0, None, 2),   # For m3 conditional tests - Additional Language Sign + Spoken Manual
        ("CASE011", "AIP", "FT", None, 0, 0, True, 61)    # For m3 conditional tests - Additional Language Sign + Sign Manual
        ]

    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("TimeEstimate", T.IntegerType(), True),
        T.StructField("HearingCentre", T.StringType(), True),
        T.StructField("HearingDate", T.StringType(), True),
        T.StructField("StartTime", T.StringType(), True),
    ])

    m3_data = [
        ("CASE005", 1, 37, 180, "LOC001","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00"),
        ("CASE005", 2, 37, 60, "LOC002","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.000+00:00"),   
        ("CASE006", 1, 38, 240, "LOC003","2026-12-03T00:00:00.000+00:00","1899-12-30T13:00:00.000+00:00"),   
        ("CASE007", 1, 38, 360, "LOC004","2026-08-03T00:00:00.000+00:00","2000-12-30T07:10:58.000+00:00"),  
        ("CASE008", 1, 37, None, "LOC005","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00"),  
        ("CASE009", 1, 37, 30, "LOC006","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00"),  
        ("CASE010", 1, 38, None, "LOC007","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00"),  
        ("CASE011", 1, 38, 45, "LOC008","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.999+00:00")   
        ]   

    loc_schema = T.StructType([
        T.StructField("ListedCentre", T.StringType(), True),
        T.StructField("locationCode", T.StringType(), True),
        T.StructField("locationLabel", T.StringType(), True)])

    loc_data = [
        ("LOC001", "123", "Court1"),   # StatusId 1 Unused Additional Spoken Language Only
        ("LOC002", "456", "Court2"),   # StatusId 2 First Additional Spoken Language Only (to Spoken + Spoken)
        ("LOC003", "789", "Court3"),   # Additional Manual Language Entry (to Spoken + Spoken Manual)
        ("LOC004", None, "Court4"),   # Additional Sign Language (to Spoken + Sign)
        ("LOC005", None, "Court5"),   # Additional Sign Manual Language (to Spoken + Sign Manual)
        ("LOC006", "xyz", "Court6"),   # Additional Sign Language (to Sign + Sign Manual)
        ]
    

    df_m1 =  spark.createDataFrame(m1_data, m1_schema)
    df_m3 =  spark.createDataFrame(m3_data, m3_schema)
    df_loc =  spark.createDataFrame(loc_data, loc_schema)

    hearingDetails_content,_ = hearingDetails(df_m1,df_m3,df_loc)
    results = {row["CaseNo"]: row.asDict() for row in hearingDetails_content.collect()}
    return results

def test_listCaseHearingDate(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["listCaseHearingDate"] == None
    assert results["CASE006"]["listCaseHearingDate"] == "2026-12-03T13:00:00.000"
    assert results["CASE008"]["listCaseHearingDate"] == "2024-10-02TT10:00:00.000"
    assert results["CASE011"]["listCaseHearingDate"] == "2025-11-02T12:00:00.999"
