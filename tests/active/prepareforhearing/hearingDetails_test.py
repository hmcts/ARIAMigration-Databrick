from Databricks.ACTIVE.APPEALS.shared_functions.prepareForHearing import hearingDetails
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
        T.StructField("locationLabel", T.StringType(), True),
        T.StructField("listCaseHearingCentre", T.StringType(), True),
        T.StructField("listCaseHearingCentreAddress", T.StringType(), True)
        ])

    loc_data = [
        ("LOC001", "123", "Court1","Bham","123 xyz"),   
        ("LOC002", "456", "Court2","Man","123 abc"),   
        ("LOC003", "789", "Court3","Scot","456 asd"),   
        ("LOC004", None, "Court4","Cov","7676 jgfd"),  
        ("LOC005", None, "Court5","Nor","954 bbb"),   
        ("LOC006", "xyz", "Court6","ply","456 mmm"),  
    ]
    

    df_m1 =  spark.createDataFrame(m1_data, m1_schema)
    df_m3 =  spark.createDataFrame(m3_data, m3_schema)
    df_loc =  spark.createDataFrame(loc_data, loc_schema)

    hearingDetails_content,_ = hearingDetails(df_m1,df_m3,df_loc)
    results = {row["CaseNo"]: row.asDict(recursive=True) for row in hearingDetails_content.collect()}
    return results

def test_listingLength(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["listingLength"] == None
    assert results["CASE006"]["listingLength"] == {'hours': 4, 'minutes': 0}
    assert results["CASE008"]["listingLength"] == {'hours': None, 'minutes': None}
    assert results["CASE011"]["listingLength"] == {'hours': 1, 'minutes': 0}


def test_hearingChannel(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    hearing_channel_list_items = [
        {'code': 'INTER', 'label': 'In Person'},
        {'code': 'NA', 'label': 'Not in Attendance'},
        {'code': 'ONPPRS', 'label': 'On The Papers'},
        {'code': 'TEL', 'label': 'Telephone'},
        {'code': 'VID', 'label': 'Video'}
    ]

    assert results["CASE001"]["hearingChannel"] == {'value': {'code': 'ONPPRS', 'label': 'On The Papers'}, 'list_items': hearing_channel_list_items}
    assert results["CASE002"]["hearingChannel"] == {'value': {'code': 'INTER', 'label': 'In Person'}, 'list_items': hearing_channel_list_items}
    assert results["CASE006"]["hearingChannel"] == {'value': {'code': None, 'label': None}, 'list_items': hearing_channel_list_items}

def test_witnessDetails(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witnessDetails"] == []
    assert results["CASE002"]["witnessDetails"] == []
    assert results["CASE006"]["witnessDetails"] == []


def test_listingLocation(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    # Formatted version of loc_data
    list_items = [
        {"code": "123", "label": "Court1"},
        {"code": "456", "label": "Court2"},
        {"code": "789", "label": "Court3"},
        {"code": None, "label": "Court4"},
        {"code": None, "label": "Court5"},
        {"code": "xyz", "label": "Court6"}
    ]

    assert results["CASE001"]["listingLocation"] == None
    assert results["CASE006"]["listingLocation"] == {'value': {'code': '789', 'label': 'Court3'}, 'list_items': list_items}
    assert results["CASE008"]["listingLocation"] == {'value': {'code': None, 'label': 'Court5'}, 'list_items': list_items}
    assert results["CASE011"]["listingLocation"] == {'value': {'code': None, 'label': None}, 'list_items': list_items}

def test_witness1InterpreterSignLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness1InterpreterSignLanguage"] == {}
    assert results["CASE002"]["witness1InterpreterSignLanguage"] == {}
    assert results["CASE006"]["witness1InterpreterSignLanguage"] == {}

def test_witness2InterpreterSignLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness2InterpreterSignLanguage"] == {}
    assert results["CASE002"]["witness2InterpreterSignLanguage"] == {}
    assert results["CASE006"]["witness2InterpreterSignLanguage"] == {}

def test_witness3InterpreterSignLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness3InterpreterSignLanguage"] == {}
    assert results["CASE002"]["witness3InterpreterSignLanguage"] == {}
    assert results["CASE006"]["witness3InterpreterSignLanguage"] == {}

def test_witness4InterpreterSignLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness4InterpreterSignLanguage"] == {}
    assert results["CASE002"]["witness4InterpreterSignLanguage"] == {}
    assert results["CASE006"]["witness4InterpreterSignLanguage"] == {}

def test_witness5InterpreterSignLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness5InterpreterSignLanguage"] == {}
    assert results["CASE002"]["witness5InterpreterSignLanguage"] == {}
    assert results["CASE006"]["witness5InterpreterSignLanguage"] == {}

def test_witness6InterpreterSignLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness6InterpreterSignLanguage"] == {}
    assert results["CASE002"]["witness6InterpreterSignLanguage"] == {}
    assert results["CASE006"]["witness6InterpreterSignLanguage"] == {}

def test_witness7InterpreterSignLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness7InterpreterSignLanguage"] == {}
    assert results["CASE002"]["witness7InterpreterSignLanguage"] == {}
    assert results["CASE006"]["witness7InterpreterSignLanguage"] == {}

def test_witness8InterpreterSignLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness8InterpreterSignLanguage"] == {}
    assert results["CASE002"]["witness8InterpreterSignLanguage"] == {}
    assert results["CASE006"]["witness8InterpreterSignLanguage"] == {}

def test_witness9InterpreterSignLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness9InterpreterSignLanguage"] == {}
    assert results["CASE002"]["witness9InterpreterSignLanguage"] == {}
    assert results["CASE006"]["witness9InterpreterSignLanguage"] == {}

def test_witness10InterpreterSignLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness10InterpreterSignLanguage"] == {}
    assert results["CASE002"]["witness10InterpreterSignLanguage"] == {}
    assert results["CASE006"]["witness10InterpreterSignLanguage"] == {}


def test_witness1InterpreterSpokenLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness1InterpreterSpokenLanguage"] == {}
    assert results["CASE002"]["witness1InterpreterSpokenLanguage"] == {}
    assert results["CASE006"]["witness1InterpreterSpokenLanguage"] == {}

def test_witness2InterpreterSpokenLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness2InterpreterSpokenLanguage"] == {}
    assert results["CASE002"]["witness2InterpreterSpokenLanguage"] == {}
    assert results["CASE006"]["witness2InterpreterSpokenLanguage"] == {}

def test_witness3InterpreterSpokenLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness3InterpreterSpokenLanguage"] == {}
    assert results["CASE002"]["witness3InterpreterSpokenLanguage"] == {}
    assert results["CASE006"]["witness3InterpreterSpokenLanguage"] == {}

def test_witness4InterpreterSpokenLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness4InterpreterSpokenLanguage"] == {}
    assert results["CASE002"]["witness4InterpreterSpokenLanguage"] == {}
    assert results["CASE006"]["witness4InterpreterSpokenLanguage"] == {}

def test_witness5InterpreterSpokenLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness5InterpreterSpokenLanguage"] == {}
    assert results["CASE002"]["witness5InterpreterSpokenLanguage"] == {}
    assert results["CASE006"]["witness5InterpreterSpokenLanguage"] == {}

def test_witness6InterpreterSpokenLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness6InterpreterSpokenLanguage"] == {}
    assert results["CASE002"]["witness6InterpreterSpokenLanguage"] == {}
    assert results["CASE006"]["witness6InterpreterSpokenLanguage"] == {}

def test_witness7InterpreterSpokenLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness7InterpreterSpokenLanguage"] == {}
    assert results["CASE002"]["witness7InterpreterSpokenLanguage"] == {}
    assert results["CASE006"]["witness7InterpreterSpokenLanguage"] == {}

def test_witness8InterpreterSpokenLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness8InterpreterSpokenLanguage"] == {}
    assert results["CASE002"]["witness8InterpreterSpokenLanguage"] == {}
    assert results["CASE006"]["witness8InterpreterSpokenLanguage"] == {}

def test_witness9InterpreterSpokenLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness9InterpreterSpokenLanguage"] == {}
    assert results["CASE002"]["witness9InterpreterSpokenLanguage"] == {}
    assert results["CASE006"]["witness9InterpreterSpokenLanguage"] == {}

def test_witness10InterpreterSpokenLanguage(spark,hearingDetails_outputs):

    results = hearingDetails_outputs

    assert results["CASE001"]["witness10InterpreterSpokenLanguage"] == {}
    assert results["CASE002"]["witness10InterpreterSpokenLanguage"] == {}
    assert results["CASE006"]["witness10InterpreterSpokenLanguage"] == {}