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
        ("CASE011", "AIP", "FT", None, 0, 0, True, 61),    # For m3 conditional tests - Additional Language Sign + Sign Manual
        ("CASE012", "AIP", "FT", None, 0, 0, True, 61)
        ]

    
    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("StatusId", T.IntegerType(), True),
        T.StructField("CaseStatus", T.IntegerType(), True),
        T.StructField("Outcome", T.IntegerType(), True),
        T.StructField("TimeEstimate", T.IntegerType(), True),
        T.StructField("HearingCentre", T.StringType(), True),
        T.StructField("HearingDate", T.StringType(), True),
        T.StructField("StartTime", T.StringType(), True),
        T.StructField("DecisionDate", T.StringType(), True),
    ])

    m3_data = [
        # Existing test cases - CaseStatus IN (37, 38) with Outcome IS NULL
        ("CASE005", 1, 37, None, 180, "LOC001","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00"),
        ("CASE005", 2, 37, None, 60, "LOC002","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.000+00:00"),   
        ("CASE006", 1, 38, None, 240, "LOC003","2026-12-03T00:00:00.000+00:00","1899-12-30T13:00:00.000+00:00"),   
        ("CASE007", 1, 38, None, 360, "LOC004","2026-08-03T00:00:00.000+00:00","2000-12-30T07:10:58.000+00:00"),  
        ("CASE008", 1, 37, None, None, "LOC005","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00"),  
        ("CASE009", 1, 37, None, 30, "LOC006","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00"),  
        ("CASE010", 1, 38, None, None, "LOC007","2024-10-02T00:00:00.000+00:00","1899-12-30T10:00:00.000+00:00"),  
        ("CASE011", 1, 38, None, 45, "LOC008","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.999+00:00"),
        ("CASE012", 1, 38, None, 0, "LOC008","2025-11-02T00:00:00.000+00:00","1899-12-30T12:00:00.999+00:00"),
        
        # New test cases for listCaseHearing fields with CaseStatus = 26
        # CASE013: CaseStatus 26, Outcome != 38 (should be INCLUDED in listCaseHearing fields)
        ("CASE013", 1, 26, 40, 120, "LOC001","2025-06-15T00:00:00.000+00:00","1899-12-30T14:00:00.000+00:00"),
        
        # CASE014: CaseStatus 26, Outcome = 38 (should be EXCLUDED from listCaseHearing fields)
        ("CASE014", 1, 26, 38, 150, "LOC002","2025-07-20T00:00:00.000+00:00","1899-12-30T15:00:00.000+00:00"),
        
        # CASE015: Test MAX(StatusId) with multiple rows - highest StatusId has CaseStatus 26, Outcome != 38
        ("CASE015", 1, 37, None, 90, "LOC003","2025-08-10T00:00:00.000+00:00","1899-12-30T16:00:00.000+00:00"),
        ("CASE015", 2, 26, 50, 90, "LOC003","2025-09-10T00:00:00.000+00:00","1899-12-30T17:00:00.000+00:00"),
        
        # CASE016: Test MAX(StatusId) - highest StatusId has CaseStatus 26, Outcome = 38 (should be excluded)
        ("CASE016", 1, 37, None, 180, "LOC004","2025-10-10T00:00:00.000+00:00","1899-12-30T18:00:00.000+00:00"),
        ("CASE016", 2, 26, 38, 180, "LOC004","2025-11-10T00:00:00.000+00:00","1899-12-30T19:00:00.000+00:00"),
        
        # CASE017: Test DecisionDate fallback - CaseStatus 38 with NULL HearingDate
        ("CASE017", 1, 38, None, 120, "LOC005", None, None, "2024-06-15"),
        
        # CASE018: Test DecisionDate fallback - CaseStatus 38 with NULL HearingDate but valid StartTime
        ("CASE018", 1, 38, None, 90, "LOC006", None, "2024-09-15T10:30:00.000+00:00", "2024-05-20"),
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

    # assert results["CASE001"]["listingLength"] == {'hours': 0, 'minutes': 30}
    assert results["CASE006"]["listingLength"] == {'hours': 4, 'minutes': 0}
    assert results["CASE008"]["listingLength"] == {'hours': 0, 'minutes': 30}
    assert results["CASE011"]["listingLength"] == {'hours': 1, 'minutes': 0}
    assert results["CASE012"]["listingLength"] == {'hours': 0, 'minutes': 30}


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


def test_listCaseHearingLength(spark, hearingDetails_outputs):
    """Test listCaseHearingLength field with the new filter condition:
    CaseStatus IN (37,38,26) AND Outcome != 38
    """
    results = hearingDetails_outputs
    
    # CASE013: CaseStatus 26, Outcome=40 (not 38) - should be INCLUDED
    assert results["CASE013"]["listCaseHearingLength"] == "120", "CASE013 should have listCaseHearingLength=120 (rounded from 120)"
    
    # CASE014: CaseStatus 26, Outcome=38 - should be EXCLUDED (NULL)
    assert results["CASE014"]["listCaseHearingLength"] is None, "CASE014 should have NULL listCaseHearingLength (Outcome=38 excluded)"
    
    # CASE015: MAX(StatusId)=2 has CaseStatus 26, Outcome=50 - should use that row
    assert results["CASE015"]["listCaseHearingLength"] == "90", "CASE015 should use MAX(StatusId) row with CaseStatus 26, Outcome 50"
    
    # CASE016: MAX(StatusId)=2 has CaseStatus 26, Outcome=38 - excluded, fallback to StatusId=1
    assert results["CASE016"]["listCaseHearingLength"] == "180", "CASE016 should use StatusId=1 row (StatusId=2 excluded due to Outcome=38)"


def test_listCaseHearingDate(spark, hearingDetails_outputs):
    """Test listCaseHearingDate field with the new filter condition:
    CaseStatus IN (37,38,26) AND Outcome != 38
    """
    results = hearingDetails_outputs
    
    # CASE013: CaseStatus 26, Outcome=40 - should be INCLUDED
    assert results["CASE013"]["listCaseHearingDate"] == "2025-06-15T14:00:00.000", "CASE013 should have listCaseHearingDate from StatusId=1 row"
    
    # CASE014: CaseStatus 26, Outcome=38 - should be EXCLUDED (NULL)
    assert results["CASE014"]["listCaseHearingDate"] is None, "CASE014 should have NULL listCaseHearingDate (Outcome=38 excluded)"
    
    # CASE015: MAX(StatusId)=2 has CaseStatus 26, Outcome=50 - should use that row
    assert results["CASE015"]["listCaseHearingDate"] == "2025-09-10T17:00:00.000", "CASE015 should use MAX(StatusId)=2 row"
    
    # CASE016: MAX(StatusId)=2 excluded, fallback to StatusId=1
    assert results["CASE016"]["listCaseHearingDate"] == "2025-10-10T18:00:00.000", "CASE016 should use StatusId=1 row"


def test_listCaseHearingCentre(spark, hearingDetails_outputs):
    """Test listCaseHearingCentre field with the new filter condition:
    CaseStatus IN (37,38,26) AND Outcome != 38
    """
    results = hearingDetails_outputs
    
    # CASE013: CaseStatus 26, Outcome=40 - should be INCLUDED
    assert results["CASE013"]["listCaseHearingCentre"] == "Bham", "CASE013 should have listCaseHearingCentre from bronze data"
    
    # CASE014: CaseStatus 26, Outcome=38 - should be EXCLUDED (NULL)
    assert results["CASE014"]["listCaseHearingCentre"] is None, "CASE014 should have NULL listCaseHearingCentre (Outcome=38 excluded)"
    
    # CASE015: MAX(StatusId)=2 has CaseStatus 26, Outcome=50 - should use that row
    assert results["CASE015"]["listCaseHearingCentre"] == "Scot", "CASE015 should use MAX(StatusId)=2 row (LOC003)"
    
    # CASE016: MAX(StatusId)=2 excluded, fallback to StatusId=1
    assert results["CASE016"]["listCaseHearingCentre"] == "Court4", "CASE016 should use StatusId=1 row (LOC004)"


def test_listCaseHearingCentreAddress(spark, hearingDetails_outputs):
    """Test listCaseHearingCentreAddress field with the new filter condition:
    CaseStatus IN (37,38,26) AND Outcome != 38
    """
    results = hearingDetails_outputs
    
    # CASE013: CaseStatus 26, Outcome=40 - should be INCLUDED
    assert results["CASE013"]["listCaseHearingCentreAddress"] == "123 xyz", "CASE013 should have listCaseHearingCentreAddress from bronze data"
    
    # CASE014: CaseStatus 26, Outcome=38 - should be EXCLUDED (NULL)
    assert results["CASE014"]["listCaseHearingCentreAddress"] is None, "CASE014 should have NULL listCaseHearingCentreAddress (Outcome=38 excluded)"
    
    # CASE015: MAX(StatusId)=2 has CaseStatus 26, Outcome=50 - should use that row
    assert results["CASE015"]["listCaseHearingCentreAddress"] == "456 asd", "CASE015 should use MAX(StatusId)=2 row (LOC003)"
    
    # CASE016: MAX(StatusId)=2 excluded, fallback to StatusId=1
    assert results["CASE016"]["listCaseHearingCentreAddress"] == "7676 jgfd", "CASE016 should use StatusId=1 row (LOC004)"


def test_listCaseHearingDate_DecisionDate_Fallback(spark, hearingDetails_outputs):
    """Test listCaseHearingDate uses DecisionDate when CaseStatus == 38 AND HearingDate is NULL
    """
    results = hearingDetails_outputs
    
    # CASE017: CaseStatus 38, HearingDate=NULL, DecisionDate='2024-06-15'
    # Should use DecisionDate with time set to 00:00:00.000
    assert results["CASE017"]["listCaseHearingDate"] == "2024-06-15T00:00:00.000", \
        f"CASE017 should use DecisionDate (2024-06-15) when HearingDate is NULL, got {results['CASE017']['listCaseHearingDate']}"
    
    # CASE018: CaseStatus 38, HearingDate=NULL, StartTime='2024-09-15T10:30:00.000+00:00', DecisionDate='2024-05-20'
    # Should use DecisionDate for date (2024-05-20) with StartTime preserved
    assert results["CASE018"]["listCaseHearingDate"] == "2024-05-20T10:30:00.000", \
        f"CASE018 should use DecisionDate (2024-05-20) with StartTime, got {results['CASE018']['listCaseHearingDate']}"
    
    # CASE006: CaseStatus 38 with valid HearingDate - should NOT use DecisionDate fallback
    assert results["CASE006"]["listCaseHearingDate"] == "2026-12-03T00:00:00.000", \
        f"CASE006 should use HearingDate since it's not NULL, got {results['CASE006']['listCaseHearingDate']}"