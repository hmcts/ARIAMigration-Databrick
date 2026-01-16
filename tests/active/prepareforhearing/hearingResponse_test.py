from Databricks.ACTIVE.APPEALS.shared_functions.prepareForHearing import hearingResponse
from pyspark.sql import SparkSession
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("HearingResponseTests")
        .getOrCreate()
    )


##### Testing the documents field grouping function #####
@pytest.fixture(scope="session")
def hearingResponse_outputs(spark):

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
        ("CASE001", "AIP", "FTPA", None, 0, 1, True, 1),  # LanguageCode 1 - Spoken Language
        ("CASE002", "AIP", "FTPA", None, 0, 2, False, 2),  # LanguageCode 5 - Spoken Language Manual Entry
        ("CASE003", "AIP", "FTPA", None, 0, 0, True, 2),  # LanguageCode 6 - Sign Language
        ("CASE004", "AIP", "FTPA", None, 0, None, False, 2),  # LanguageCode 7 - Sign Language Manual Entry
        ("CASE005", "AIP", "FT", None, 0, 33, True, 3), 
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
    T.StructField("CourtClerk_Surname", T.StringType(), True),
    T.StructField("CourtClerk_Forenames", T.StringType(), True),
    T.StructField("CourtClerk_Title", T.StringType(), True),
    T.StructField("ListTypeId", T.IntegerType(), True),
    T.StructField("ListType", T.StringType(), True),
    T.StructField("HearingDate", T.StringType(), True),  # <-- keep this
    T.StructField("HearingType", T.StringType(), True),
    T.StructField("CourtName", T.StringType(), True),
    T.StructField("StartTime", T.StringType(), True),
    T.StructField("Judge1FT_Surname", T.StringType(), True),
    T.StructField("Judge1FT_Forenames", T.StringType(), True),
    T.StructField("Judge1FT_Title", T.StringType(), True),
    T.StructField("Judge2FT_Surname", T.StringType(), True),
    T.StructField("Judge2FT_Forenames", T.StringType(), True),
    T.StructField("Judge2FT_Title", T.StringType(), True),
    T.StructField("Judge3FT_Surname", T.StringType(), True),
    T.StructField("Judge3FT_Forenames", T.StringType(), True),
    T.StructField("Judge3FT_Title", T.StringType(), True),
    T.StructField("Notes", T.StringType(), True)
    ])


    m3_data = [
        ("CASE001", 1, 37, 180, "LOC001","CC_Sur", "CC_Fore", "CC_T",5,"Standard","2023-10-01","HearingType","CourtName","2023-10-01T10:00:00","Jud_S1_Name","Jud_F1_Name","Judge1_T","Jud_S2_Name","Jud_F2_Name","Judge2_T","Jud_S3_Name","Jud_F3_Name","Judge3_T","xxxx"),  
        ("CASE002", 2, 37, 60, "LOC002", None, "CC_Fore", "CC_T",5,"Urgent","2023-10-01",None,"CourtName","2023-10-01T10:00:00","Jud_S1_Name","Jud_F1_Name","Judge1_T","Jud_S2_Name","Jud_F2_Name","Judge2_T","Jud_S3_Name","Jud_F3_Name","Judge3_T","Noxxxxtes"),  
        ("CASE003", 1, 38, 240, "LOC003", "CC_Sur", None, "CC_T",5,"Special","2023-10-01","HearingType","CourtName","2023-10-01T10:00:00","Jud_S1_Name","Jud_F1_Name","Judge1_T","Jud_S2_Name","Jud_F2_Name","Judge2_T","Jud_S3_Name","Jud_F3_Name","Judge3_T","Notes"),  
        ("CASE004", 1, 38, 360, "LOC004", "CC_Sur", "CC_Fore", None,5,"Standard","2023-10-01",None,"CourtName","2023-10-01T10:00:00","Jud_S1_Name",None,"Judge1_T","Jud_S2_Name","Jud_F2_Name","Judge2_T","Jud_S3_Name","Jud_F3_Name","Judge3_T","Notes"),   
        ("CASE005", 1, 37, None, "LOC005", None, None, "CC_T",5,"Urgent","2023-10-01","HearingType",None,"2023-10-01T10:00:00","Jud_S1_Name","Jud_F1_Name","Judge1_T","Jud_S2_Name","Jud_F2_Name","Judge2_T","Jud_S3_Name","Jud_F3_Name","Judge3_T",None),   
        ("CASE006", 1, 37, 30, "LOC006",None, None, None,None,"Special","2023-10-01","HearingType","CourtName","2023-10-01T10:00:00","Jud_S1_Name","Jud_F1_Name","Judge1_T","Jud_S2_Name","Jud_F2_Name","Judge2_T","Jud_S3_Name","Jud_F3_Name","Judge3_T","Notes"),   
        ("CASE007", 1, 38, None, "LOC007", "CC_Sur", "CC_Fore", "CC_T",2,"Standard","2023-10-01","HearingType","CourtName","2023-10-01T10:00:00","Jud_S1_Name","Jud_F1_Name","Judge1_T","Jud_S2_Name","Jud_F2_Name","Judge2_T","Jud_S3_Name","Jud_F3_Name","Judge3_T","Notes"),
        ("CASE008", 1, 38, 45, "LOC008", "CC_Sur", "CC_Fore", "CC_T",1,"Urgent","2023-10-01","HearingType","CourtName","2023-10-01T10:00:00","Jud_S1_Name","Jud_F1_Name","Judge1_T","Jud_S2_Name","Jud_F2_Name","Judge2_T","Jud_S3_Name","Jud_F3_Name","Judge3_T","Notes")
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
    
    m6_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("Required", T.IntegerType(), True),
        T.StructField("Judge_Surname", T.StringType(), True),
        T.StructField("Judge_Forenames", T.StringType(), True),
        T.StructField("Judge_Title", T.StringType(), True),
        ])
    
    m6_data = [
        ("CASE001", 1,  "Jud_S_Name","Jud_F_Name","Judge_T"),   # StatusId 1 Unused Additional Spoken Language Only
        ("CASE002", 0,  None,"Jud_F_Name","Judge_T"),   # StatusId 2 First Additional Spoken Language Only (to Spoken + Spoken)
        ("CASE003", 1, "Jud_S_Name",None,"Judge_T"),   # Additional Manual Language Entry (to Spoken + Spoken Manual)
        ("CASE004", 1,  "Jud_S_Name","Jud_F_Name",None),   # Additional Sign Language (to Spoken + Sign)
        ("CASE005", 0, None,None,"Judge_T"),   # Additional Sign Manual Language (to Spoken + Sign Manual)
        ("CASE006", 0, None,None,None),   # Additional Sign Language (to Sign + Sign Manual)
        ("CASE007", None,  "Jud_S_Name","Jud_F_Name","Judge_T"),  # Additional Manual Language Entry (to Sign + Spoken Manual)
        ("CASE008", None, "Jud_S_Name","Jud_F_Name","Judge_T")   # Additional Sign Manual Language (to Sign + Sign Manual)
        ] 
    

    df_m1 =  spark.createDataFrame(m1_data, m1_schema)
    df_m3 =  spark.createDataFrame(m3_data, m3_schema)
    df_loc =  spark.createDataFrame(loc_data, loc_schema)
    df_m6 =  spark.createDataFrame(m6_data, m6_schema)

    hearingResponse_content,_ = hearingResponse(df_m1,df_m3,df_m6)
    results = {row["CaseNo"]: row.asDict() for row in hearingResponse_content.collect()}
    return results


def test_isRemoteHearing(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["isRemoteHearing"] == 'No'
    assert results["CASE002"]["isRemoteHearing"] == 'No'
    assert results["CASE006"]["isRemoteHearing"] == 'No'

def test_isAppealSuitableToFloat(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["isAppealSuitableToFloat"] == 'Yes'
    assert results["CASE006"]["isAppealSuitableToFloat"] == 'No'
    assert results["CASE007"]["isAppealSuitableToFloat"] == 'No'
    assert results["CASE008"]["isAppealSuitableToFloat"] == 'No'

def test_isMultimediaAllowed(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["isMultimediaAllowed"] == 'Granted'
    assert results["CASE002"]["isMultimediaAllowed"] == 'Granted'
    assert results["CASE006"]["isMultimediaAllowed"] == 'Granted'

def test_multimediaTribunalResponse(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["multimediaTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE002"]["multimediaTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE006"]["multimediaTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'

def test_multimediaDecisionForDisplay(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["multimediaDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE002"]["multimediaDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE006"]["multimediaDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'

def test_isInCameraCourtAllowed(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["isInCameraCourtAllowed"] == 'Granted'
    assert results["CASE002"]["isInCameraCourtAllowed"] == None
    assert results["CASE009"]["isInCameraCourtAllowed"] == None

def test_inCameraCourtTribunalResponse(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["inCameraCourtTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE002"]["inCameraCourtTribunalResponse"] == None
    assert results["CASE009"]["inCameraCourtTribunalResponse"] == None

def test_inCameraCourtDecisionForDisplay(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["inCameraCourtDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE002"]["inCameraCourtDecisionForDisplay"] == None
    assert results["CASE009"]["inCameraCourtDecisionForDisplay"] == None

def test_isSingleSexCourtAllowed(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["isSingleSexCourtAllowed"] == 'Granted'
    assert results["CASE002"]["isSingleSexCourtAllowed"] == 'Granted'
    assert results["CASE003"]["isSingleSexCourtAllowed"] == None
    assert results["CASE004"]["isSingleSexCourtAllowed"] == None

def test_singleSexCourtTribunalResponse(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["singleSexCourtTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE002"]["singleSexCourtTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE003"]["singleSexCourtTribunalResponse"] == None
    assert results["CASE004"]["singleSexCourtTribunalResponse"] == None

def test_singleSexCourtDecisionForDisplay(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["singleSexCourtDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE002"]["singleSexCourtDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE003"]["singleSexCourtDecisionForDisplay"] == None
    assert results["CASE004"]["singleSexCourtDecisionForDisplay"] == None




def test_isVulnerabilitiesAllowed(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["isVulnerabilitiesAllowed"] == 'Granted'
    assert results["CASE002"]["isVulnerabilitiesAllowed"] == 'Granted'
    assert results["CASE008"]["isVulnerabilitiesAllowed"] == 'Granted'

def test_vulnerabilitiesTribunalResponse(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["vulnerabilitiesTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE002"]["vulnerabilitiesTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE008"]["vulnerabilitiesTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'

def test_vulnerabilitiesDecisionForDisplay(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["vulnerabilitiesDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE002"]["vulnerabilitiesDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE008"]["vulnerabilitiesDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'


def test_isRemoteHearingAllowed(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["isRemoteHearingAllowed"] == 'Granted'
    assert results["CASE002"]["isRemoteHearingAllowed"] == 'Granted'
    assert results["CASE008"]["isRemoteHearingAllowed"] == 'Granted'

def test_remoteVideoCallTribunalResponse(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["remoteVideoCallTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE002"]["remoteVideoCallTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE008"]["remoteVideoCallTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'

def test_remoteHearingDecisionForDisplay(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["remoteHearingDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE002"]["remoteHearingDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE008"]["remoteHearingDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'

def test_isAdditionalAdjustmentsAllowed(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["isAdditionalAdjustmentsAllowed"] == 'Granted'
    assert results["CASE002"]["isAdditionalAdjustmentsAllowed"] == 'Granted'
    assert results["CASE008"]["isAdditionalAdjustmentsAllowed"] == 'Granted'

def test_additionalTribunalResponse(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["additionalTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE002"]["additionalTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE008"]["additionalTribunalResponse"] == 'This is a migrated ARIA case. Please refer to the documents.'

def test_otherDecisionForDisplay(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["otherDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE002"]["otherDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'
    assert results["CASE008"]["otherDecisionForDisplay"] == 'Granted - This is a migrated ARIA case. Please refer to the documents.'

def test_isAdditionalInstructionAllowed(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["isAdditionalInstructionAllowed"] == 'Yes'
    assert results["CASE002"]["isAdditionalInstructionAllowed"] == 'Yes'
    assert results["CASE008"]["isAdditionalInstructionAllowed"] == 'Yes'

def test_additionalInstructionsTribunalResponse(spark,hearingResponse_outputs):

    results = hearingResponse_outputs

    assert results["CASE001"]["additionalInstructionsTribunalResponse"] == 'Listed details from ARIA: \n Hearing Centre: LOC001\n Hearing Date: 2023-10-01\n Hearing Type: HearingType\n Court: CourtName\n List Type: Standard\n List Start Time: 2023-10-01T10:00:00\n Judge First Tier: Jud_S1_Name Jud_F1_Name (Judge1_T) Jud_S2_Name Jud_F2_Name (Judge2_T) Jud_S3_Name Jud_F3_Name (Judge3_T)\n Court Clerk / Usher: CC_Sur CC_Fore (CC_T)\n Start Time: 2023-10-01T10:00:00\n Estimated Duration: 180\n Required/Incompatible Judicial Officers: Jud_S_Name Jud_F_Name ( Judge_T ) : Required\n Notes: xxxx'
    assert results["CASE002"]["additionalInstructionsTribunalResponse"] == 'Listed details from ARIA: \n Hearing Centre: LOC002\n Hearing Date: 2023-10-01\n Hearing Type: N/A\n Court: CourtName\n List Type: Urgent\n List Start Time: 2023-10-01T10:00:00\n Judge First Tier: Jud_S1_Name Jud_F1_Name (Judge1_T) Jud_S2_Name Jud_F2_Name (Judge2_T) Jud_S3_Name Jud_F3_Name (Judge3_T)\n Court Clerk / Usher: N/A\n Start Time: 2023-10-01T10:00:00\n Estimated Duration: 60\n Required/Incompatible Judicial Officers: Jud_F_Name ( Judge_T ) : Not Required\n Notes: Noxxxxtes'
    assert results["CASE003"]["additionalInstructionsTribunalResponse"] == 'Listed details from ARIA: \n Hearing Centre: LOC003\n Hearing Date: 2023-10-01\n Hearing Type: HearingType\n Court: CourtName\n List Type: Special\n List Start Time: 2023-10-01T10:00:00\n Judge First Tier: Jud_S1_Name Jud_F1_Name (Judge1_T) Jud_S2_Name Jud_F2_Name (Judge2_T) Jud_S3_Name Jud_F3_Name (Judge3_T)\n Court Clerk / Usher: CC_Sur (CC_T)\n Start Time: 2023-10-01T10:00:00\n Estimated Duration: 240\n Required/Incompatible Judicial Officers: Jud_S_Name ( Judge_T ) : Required\n Notes: Notes'
    assert results["CASE004"]["additionalInstructionsTribunalResponse"] == 'Listed details from ARIA: \n Hearing Centre: LOC004\n Hearing Date: 2023-10-01\n Hearing Type: N/A\n Court: CourtName\n List Type: Standard\n List Start Time: 2023-10-01T10:00:00\n Judge First Tier: Jud_S1_Name (Judge1_T) Jud_S2_Name Jud_F2_Name (Judge2_T) Jud_S3_Name Jud_F3_Name (Judge3_T)\n Court Clerk / Usher: CC_Sur CC_Fore\n Start Time: 2023-10-01T10:00:00\n Estimated Duration: 360\n Required/Incompatible Judicial Officers: Jud_S_Name Jud_F_Name : Required\n Notes: Notes'
    assert results["CASE005"]["additionalInstructionsTribunalResponse"] == 'Listed details from ARIA: \n Hearing Centre: LOC005\n Hearing Date: 2023-10-01\n Hearing Type: HearingType\n Court: N/A\n List Type: Urgent\n List Start Time: 2023-10-01T10:00:00\n Judge First Tier: Jud_S1_Name Jud_F1_Name (Judge1_T) Jud_S2_Name Jud_F2_Name (Judge2_T) Jud_S3_Name Jud_F3_Name (Judge3_T)\n Court Clerk / Usher: N/A\n Start Time: 2023-10-01T10:00:00\n Estimated Duration: N/A\n Required/Incompatible Judicial Officers: ( Judge_T ) : Not Required\n Notes: N/A'
    assert results["CASE006"]["additionalInstructionsTribunalResponse"] == 'Listed details from ARIA: \n Hearing Centre: LOC006\n Hearing Date: 2023-10-01\n Hearing Type: HearingType\n Court: CourtName\n List Type: Special\n List Start Time: 2023-10-01T10:00:00\n Judge First Tier: Jud_S1_Name Jud_F1_Name (Judge1_T) Jud_S2_Name Jud_F2_Name (Judge2_T) Jud_S3_Name Jud_F3_Name (Judge3_T)\n Court Clerk / Usher: N/A\n Start Time: 2023-10-01T10:00:00\n Estimated Duration: 30\n Required/Incompatible Judicial Officers: : Not Required\n Notes: Notes'
    assert results["CASE007"]["additionalInstructionsTribunalResponse"] == 'Listed details from ARIA: \n Hearing Centre: LOC007\n Hearing Date: 2023-10-01\n Hearing Type: HearingType\n Court: CourtName\n List Type: Standard\n List Start Time: 2023-10-01T10:00:00\n Judge First Tier: Jud_S1_Name Jud_F1_Name (Judge1_T) Jud_S2_Name Jud_F2_Name (Judge2_T) Jud_S3_Name Jud_F3_Name (Judge3_T)\n Court Clerk / Usher: CC_Sur CC_Fore (CC_T)\n Start Time: 2023-10-01T10:00:00\n Estimated Duration: N/A\n Required/Incompatible Judicial Officers: Jud_S_Name Jud_F_Name ( Judge_T )\n Notes: Notes'
    assert results["CASE008"]["additionalInstructionsTribunalResponse"] == 'Listed details from ARIA: \n Hearing Centre: LOC008\n Hearing Date: 2023-10-01\n Hearing Type: HearingType\n Court: CourtName\n List Type: Urgent\n List Start Time: 2023-10-01T10:00:00\n Judge First Tier: Jud_S1_Name Jud_F1_Name (Judge1_T) Jud_S2_Name Jud_F2_Name (Judge2_T) Jud_S3_Name Jud_F3_Name (Judge3_T)\n Court Clerk / Usher: CC_Sur CC_Fore (CC_T)\n Start Time: 2023-10-01T10:00:00\n Estimated Duration: 45\n Required/Incompatible Judicial Officers: Jud_S_Name Jud_F_Name ( Judge_T )\n Notes: Notes'


