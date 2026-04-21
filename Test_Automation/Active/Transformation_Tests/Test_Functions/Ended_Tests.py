from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
import inspect

#Import Test Results class
from models.test_result import TestResult

#Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "ended"


import inspect
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

############################################################################################
#Default Mapping
############################################################################################

def get_ended_group_id(df):
    # Sort history to use lag correctly
    history_window = Window.partitionBy("CaseNo").orderBy("StatusId")
    
    # Lag allows us to see the CaseStatus from the PREVIOUS StatusId row
    df_with_prev = df.withColumn("PrevCaseStatusId", F.lag("CaseStatus").over(history_window))

    return df_with_prev.withColumn("EndedGroup",
        F.when((F.col("CaseStatus") == 37) & (F.col("Outcome") == 80), 3)
        .when((F.col("CaseStatus") == 38) & (F.col("Outcome") == 80), 3)
        .when((F.col("CaseStatus") == 10) & (F.col("Outcome") == 80), 1)
        .when((F.col("CaseStatus") == 10) & (F.col("Outcome") == 122), 1) 
        .when((F.col("CaseStatus") == 26) & (F.col("Outcome") == 80), 2)
        .when((F.col("CaseStatus") == 51) & (F.col("Outcome") == 94), 1) 
        .when((F.col("CaseStatus") == 37) & (F.col("Outcome") == 13), 3)
        .when((F.col("CaseStatus") == 38) & (F.col("Outcome") == 13), 3)
        .when((F.col("CaseStatus") == 26) & (F.col("Outcome") == 13), 2)
        .when((F.col("CaseStatus") == 37) & (F.col("Outcome") == 25), 3)
        .when((F.col("CaseStatus") == 38) & (F.col("Outcome") == 25), 3)
        .when((F.col("CaseStatus") == 39) & (F.col("Outcome") == 25), 4)
        .when((F.col("CaseStatus") == 10) & (F.col("Outcome") == 25), 1)
        .when((F.col("CaseStatus") == 26) & (F.col("Outcome") == 25), 2)
        .when((F.col("CaseStatus") == 52) & (F.col("Outcome") == 91), 1)
        .when((F.col("CaseStatus") == 52) & (F.col("Outcome") == 95), 1)
        .when((F.col("CaseStatus") == 51) & (F.col("Outcome") == 93), 1) 
        .when((F.col("CaseStatus") == 38) & (F.col("Outcome") == 72), 3)
        .when((F.col("CaseStatus") == 10) & (F.col("Outcome") == 120), 1)
        .when((F.col("CaseStatus") == 10) & (F.col("Outcome") == 2), 1)
        .when((F.col("CaseStatus") == 10) & (F.col("Outcome") == 105), 1)
        # Fixed Set Aside logic (MAX-1 was Status 10)
        .when((F.col("CaseStatus") == 46) & (F.col("Outcome") == 31) & (F.col("PrevCaseStatusId") == 10), 1)
        .otherwise(0)
    )


def test_default_mapping_init(json_data, M1_silver, M3_bronze):
    try:
# 1. Select ALL JSON fields to avoid Unresolved Column errors
        test_df = json_data.select(
            "appealReferenceNumber", "outOfTimeDecisionType", "uploadHomeOfficeBundleAvailable",
            "caseArgumentAvailable", "reasonsForAppealDecision", "reviewedHearingRequirements",
            "isAppellantAttendingTheHearing", "isAppellantGivingOralEvidence", "isWitnessesAttending",
            "isHearingRoomNeeded", "isHearingLoopNeeded", "remoteVideoCall", "remoteVideoCallDescription",
            "physicalOrMentalHealthIssues", "physicalOrMentalHealthIssuesDescription", "pastExperiences",
            "pastExperiencesDescription", "multimediaEvidence", "multimediaEvidenceDescription",
            "additionalRequests", "additionalRequestsDescription", "datesToAvoidYesNo", "isRemoteHearing",
            "isMultimediaAllowed", "multimediaTribunalResponse", "multimediaDecisionForDisplay",
            "isVulnerabilitiesAllowed", "vulnerabilitiesTribunalResponse", "vulnerabilitiesDecisionForDisplay",
            "isRemoteHearingAllowed", "remoteVideoCallTribunalResponse", "remoteHearingDecisionForDisplay",
            "isAdditionalAdjustmentsAllowed", "additionalTribunalResponse", "otherDecisionForDisplay",
            "isAdditionalInstructionAllowed", "scheduleOfIssuesAgreement", "scheduleOfIssuesDisagreementDescription",
            "immigrationHistoryAgreement", "immigrationHistoryDisagreementDescription", "anonymityOrder",
            "uploadHomeOfficeBundleActionAvailable", "appealReviewOutcome", "appealResponseAvailable",
            "amendResponseActionAvailable", "currentHearingDetailsVisible", "reviewResponseActionAvailable",
            "reviewHomeOfficeResponseByLegalRep", "submitHearingRequirementsAvailable", 
            "uploadHomeOfficeAppealResponseActionAvailable", "stitchingStatus", "bundleConfiguration",
            "appealDecisionAvailable", 
            "isFtpaListVisible", 
            "hmcts",             
            "witnessDetails", "directions", 
            "respondentDocuments", "hearingRequirements", "hearingDocuments", "letterBundleDocuments", 
            "caseBundles", "finalDecisionAndReasonsDocuments", "ftpaAppellantDocuments", 
            "ftpaAppellantGroundsDocuments", "ftpaAppellantEvidenceDocuments", "ftpaAppellantOutOfTimeDocuments",
            "witness1InterpreterSignLanguage", "witness2InterpreterSignLanguage", "witness3InterpreterSignLanguage", 
            "witness4InterpreterSignLanguage", "witness5InterpreterSignLanguage", "witness6InterpreterSignLanguage",
            "witness7InterpreterSignLanguage", "witness8InterpreterSignLanguage", "witness9InterpreterSignLanguage",
            "witness10InterpreterSignLanguage", "witness1InterpreterSpokenLanguage", "witness2InterpreterSpokenLanguage",
            "witness3InterpreterSpokenLanguage", "witness4InterpreterSpokenLanguage", "witness5InterpreterSpokenLanguage",
            "witness6InterpreterSpokenLanguage", "witness7InterpreterSpokenLanguage", "witness8InterpreterSpokenLanguage",
            "witness9InterpreterSpokenLanguage", "witness10InterpreterSpokenLanguage"
        )

        # 2. Process M3 for Group Logic
        full_status_with_groups = get_ended_group_id(M3_bronze)
        
        # 3. Get LATEST status (rn=1)
        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        latest_status = full_status_with_groups.withColumn("rn", F.row_number().over(window_spec)) \
                                               .filter("rn = 1")

        # 4. Master Join: JSON + M3 (Groups) + M1 (Representation)
        test_df = test_df.join(
            latest_status.select("CaseNo", "EndedGroup", "CaseStatus", "StatusId", "Outcome"),
            test_df.appealReferenceNumber == latest_status.CaseNo,
            "left"
        ).join(
            M1_silver.select(F.col("CaseNo").alias("M1_CaseNo"), "Dv_Representation"),
            test_df.appealReferenceNumber == F.col("M1_CaseNo"),
            "left"
        ).drop("CaseNo", "M1_CaseNo")

        return test_df, True
    except Exception as e:
        return None, TestResult("Init", "FAIL", f"Error: {str(e)[:200]}", "ended")


def test_ended_defaultValues(test_df, fields_to_exclude):
    results_list = []
    
    # Define mapping between fields and required EndedGroups
    group_requirements = {
        "isAppellantAttendingTheHearing": [3, 4], "isAppellantGivingOralEvidence": [3, 4],
        "isWitnessesAttending": [3, 4], "isHearingRoomNeeded": [3, 4], "isHearingLoopNeeded": [3, 4],
        "remoteVideoCall": [3, 4], "remoteVideoCallDescription": [3, 4], "physicalOrMentalHealthIssues": [3, 4],
        "physicalOrMentalHealthIssuesDescription": [3, 4], "pastExperiences": [3, 4], "pastExperiencesDescription": [3, 4],
        "multimediaEvidence": [3, 4], "multimediaEvidenceDescription": [3, 4], "additionalRequests": [3, 4],
        "additionalRequestsDescription": [3, 4], "datesToAvoidYesNo": [3, 4], "appealReviewOutcome": [3, 4],
        "appealResponseAvailable": [3, 4], "reviewedHearingRequirements": [3, 4], "amendResponseActionAvailable": [3, 4],
        "currentHearingDetailsVisible": [3, 4], "reviewResponseActionAvailable": [3, 4], "reviewHomeOfficeResponseByLegalRep": [3, 4],
        "submitHearingRequirementsAvailable": [3, 4], "uploadHomeOfficeAppealResponseActionAvailable": [3, 4], "hearingRequirements": [3, 4],
        "isRemoteHearing": [4], "isMultimediaAllowed": [4], "multimediaTribunalResponse": [4], "multimediaDecisionForDisplay": [4],
        "isVulnerabilitiesAllowed": [4], "vulnerabilitiesTribunalResponse": [4], "vulnerabilitiesDecisionForDisplay": [4],
        "isRemoteHearingAllowed": [4], "remoteVideoCallTribunalResponse": [4], "remoteHearingDecisionForDisplay": [4],
        "isAdditionalAdjustmentsAllowed": [4], "additionalTribunalResponse": [4], "otherDecisionForDisplay": [4],
        "isAdditionalInstructionAllowed": [4], "witnessDetails": [4], "witness1InterpreterSignLanguage": [4],
        "witness2InterpreterSignLanguage": [4], "witness3InterpreterSignLanguage": [4], "witness4InterpreterSignLanguage": [4],
        "witness5InterpreterSignLanguage": [4], "witness6InterpreterSignLanguage": [4], "witness7InterpreterSignLanguage": [4],
        "witness8InterpreterSignLanguage": [4], "witness9InterpreterSignLanguage": [4], "witness10InterpreterSignLanguage": [4],
        "witness1InterpreterSpokenLanguage": [4], "witness2InterpreterSpokenLanguage": [4], "witness3InterpreterSpokenLanguage": [4],
        "witness4InterpreterSpokenLanguage": [4], "witness5InterpreterSpokenLanguage": [4], "witness6InterpreterSpokenLanguage": [4],
        "witness7InterpreterSpokenLanguage": [4], "witness8InterpreterSpokenLanguage": [4], "witness9InterpreterSpokenLanguage": [4],
        "witness10InterpreterSpokenLanguage": [4], "scheduleOfIssuesAgreement": [4], "scheduleOfIssuesDisagreementDescription": [4],
        "immigrationHistoryAgreement": [4], "immigrationHistoryDisagreementDescription": [4], "hmcts": [4], "stitchingStatus": [4],
        "bundleConfiguration": [4], "appealDecisionAvailable": [4], "isFtpaListVisible": [4], "hearingDocuments": [4],
        "letterBundleDocuments": [4], "caseBundles": [4], "finalDecisionAndReasonsDocuments": [4], "ftpaAppellantDocuments": [4], 
        "ftpaAppellantGroundsDocuments": [4], "ftpaAppellantEvidenceDocuments": [4], "ftpaAppellantOutOfTimeDocuments": [4], "anonymityOrder": [4],
        "directions": [2, 3, 4], "uploadHomeOfficeBundleAvailable": [2, 3, 4], "uploadHomeOfficeBundleActionAvailable": [2, 3, 4],
        "caseArgumentAvailable": [2, 3, 4], "reasonsForAppealDecision": [2, 3, 4], "respondentDocuments": [2, 3, 4]
    }

    expected_defaults = {
        "isAppellantAttendingTheHearing": "Yes", "isAppellantGivingOralEvidence": "Yes", "isWitnessesAttending": "No",
        "isHearingRoomNeeded": "Yes", "isHearingLoopNeeded": "Yes", "remoteVideoCall": "Yes",
        "remoteVideoCallDescription": "This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form.",
        "physicalOrMentalHealthIssues": "Yes", "physicalOrMentalHealthIssuesDescription": "This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form.",
        "pastExperiences": "Yes", "pastExperiencesDescription": "This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form.",
        "multimediaEvidence": "Yes", "multimediaEvidenceDescription": "This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form.",
        "additionalRequests": "Yes", "additionalRequestsDescription": "This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form.",
        "datesToAvoidYesNo": "No", "isRemoteHearing": "No", "isMultimediaAllowed": "Granted",
        "multimediaTribunalResponse": "This is a migrated ARIA case. Please refer to the documents.",
        "multimediaDecisionForDisplay": "Granted - This is a migrated ARIA case. Please refer to the documents.",
        "isVulnerabilitiesAllowed": "Granted", "vulnerabilitiesTribunalResponse": "This is a migrated ARIA case. Please refer to the documents.",
        "vulnerabilitiesDecisionForDisplay": "Granted - This is a migrated ARIA case. Please refer to the documents.",
        "isRemoteHearingAllowed": "Granted", "remoteVideoCallTribunalResponse": "This is a migrated ARIA case. Please refer to the documents.",
        "remoteHearingDecisionForDisplay": "Granted - This is a migrated ARIA case. Please refer to the documents.",
        "isAdditionalAdjustmentsAllowed": "Granted", "additionalTribunalResponse": "This is a migrated ARIA case. Please refer to the documents.",
        "otherDecisionForDisplay": "Granted - This is a migrated ARIA case. Please refer to the documents.",
        "isAdditionalInstructionAllowed": "Yes", "witness1InterpreterSignLanguage": "{}", "witness2InterpreterSignLanguage": "{}",
        "witness3InterpreterSignLanguage": "{}", "witness4InterpreterSignLanguage": "{}", "witness5InterpreterSignLanguage": "{}",
        "witness6InterpreterSignLanguage": "{}", "witness7InterpreterSignLanguage": "{}", "witness8InterpreterSignLanguage": "{}",
        "witness9InterpreterSignLanguage": "{}", "witness10InterpreterSignLanguage": "{}", "witness1InterpreterSpokenLanguage": "{}",
        "witness2InterpreterSpokenLanguage": "{}", "witness3InterpreterSpokenLanguage": "{}", "witness4InterpreterSpokenLanguage": "{}",
        "witness5InterpreterSpokenLanguage": "{}", "witness6InterpreterSpokenLanguage": "{}", "witness7InterpreterSpokenLanguage": "{}",
        "witness8InterpreterSpokenLanguage": "{}", "witness9InterpreterSpokenLanguage": "{}", "witness10InterpreterSpokenLanguage": "{}",
        "scheduleOfIssuesAgreement": "No", "scheduleOfIssuesDisagreementDescription": "This is a migrated ARIA case. Please see the documents for information on the schedule of issues.",
        "immigrationHistoryAgreement": "No", "immigrationHistoryDisagreementDescription": "This is a migrated ARIA case. Please see the documents for information on the immigration history.",
        "anonymityOrder": "No", "uploadHomeOfficeBundleAvailable": "Yes", "uploadHomeOfficeBundleActionAvailable": "No",
        "caseArgumentAvailable": "Yes", "reasonsForAppealDecision": "This is a migrated ARIA case. Please see the documents provided as part of the notice of appeal.",
        "appealReviewOutcome": "decisionMaintained", "appealResponseAvailable": "Yes", "reviewedHearingRequirements": "Yes",
        "amendResponseActionAvailable": "Yes", "currentHearingDetailsVisible": "Yes", "reviewResponseActionAvailable": "No",
        "reviewHomeOfficeResponseByLegalRep": "Yes", "submitHearingRequirementsAvailable": "Yes",
        "uploadHomeOfficeAppealResponseActionAvailable": "No", "hmcts": "[userImage:hmcts.png]", "stitchingStatus": "DONE",
        "bundleConfiguration": "iac-hearing-bundle-config.yaml", "appealDecisionAvailable": "Yes", "isFtpaListVisible": "Yes"
    }

    expected_arrays = {
        "witnessDetails": None, "directions": None, "respondentDocuments": None, "hearingRequirements": None,
        "hearingDocuments": None, "letterBundleDocuments": None, "caseBundles": None, "finalDecisionAndReasonsDocuments": None,
        "ftpaAppellantDocuments": None, "ftpaAppellantGroundsDocuments": None, "ftpaAppellantEvidenceDocuments": None,
        "ftpaAppellantOutOfTimeDocuments": None
    }

    try:
        # Loop for Simple Defaults
        for field, expected in expected_defaults.items():
            if field in fields_to_exclude: continue
            valid_groups = group_requirements.get(field, [1, 2, 3, 4])
            
            # Base Filter
            subset = test_df.filter(F.col("EndedGroup").isin(valid_groups))
            
            # --- REPRESENTATION FILTERING ---
            if field == "caseArgumentAvailable":
                subset = subset.filter(F.col("Dv_Representation") == "LR")
            elif field == "reasonsForAppealDecision":
                subset = subset.filter(F.col("Dv_Representation") == "AIP")

            if subset.count() == 0:
                continue

            condition = (F.col(field) != expected) | (F.col(field).isNull())
            fail_count = subset.filter(condition).count()
            
            if fail_count > 0:
                results_list.append(TestResult(field, "FAIL", f"Mismatches in Groups {valid_groups}: {fail_count}", "ended", inspect.stack()[0].function))
            else:
                results_list.append(TestResult(field, "PASS", f"Valid for Groups {valid_groups}", "ended", inspect.stack()[0].function))

        # Loop for Arrays
        for field, contains_val in expected_arrays.items():
            if field in fields_to_exclude: continue
            valid_groups = group_requirements.get(field, [1, 2, 3, 4])
            subset = test_df.filter(F.col("EndedGroup").isin(valid_groups))
            
            if contains_val:
                condition = (~F.array_contains(F.col(field), contains_val))
            else:
                condition = (F.size(F.col(field)) != 0)
                
            fail_count = subset.filter(condition).count()
            if fail_count > 0:
                results_list.append(TestResult(field, "FAIL", f"Array mismatch in Groups {valid_groups}: {fail_count}", "ended", inspect.stack()[0].function))
            else:
                results_list.append(TestResult(field, "PASS", f"Array valid for Groups {valid_groups}", "ended", inspect.stack()[0].function))

        return results_list
    except Exception as e:
        return [TestResult("DefaultMapping", "FAIL", f"Error: {str(e)[:300]}", "ended", inspect.stack()[0].function)]

def test_caseData_init(json, M1_bronze, M3_bronze):
    try:
        # 1. Start with JSON
        test_df = json.select(
            "appealReferenceNumber",
            "outOfTimeDecisionType"
        )

        # 2. Process M3 to get the LATEST status AND the EndedGroup
        # Use the function you wrote earlier to define the EndedGroup column
        history_with_groups = get_ended_group_id(M3_bronze) 

        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        
        # We must select EndedGroup here so it exists in the final joined table
        latest_status = history_with_groups.withColumn("rn", F.row_number().over(window_spec)) \
                                           .filter("rn = 1") \
                                           .select("CaseNo", "CaseStatus", "StatusId", "Outcome", "EndedGroup")

        # 3. Join
        test_df = test_df.join(
            M1_bronze.select("CaseNo"),
            test_df["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        ).join(
            latest_status,
            test_df["appealReferenceNumber"] == latest_status["CaseNo"],
            "inner"
        ).drop("CaseNo")

        return test_df, True
    except Exception as e:
        return None, TestResult("caseData_init", "FAIL", f"Error: {str(e)[:200]}", "ended", "init")

############################################################################################
# outOfTimeDecisionType - Scenario 1
# IF M3.CaseStatus = 10 and M3.Outcome IN (120, 2, 105) Expected: 'rejected'
############################################################################################
def test_outOfTimeDecisionType_test1(test_df):
    try:
        # 1. Filter for the specific rejection criteria
        target_records = test_df.filter((col("CaseStatus") == 10) & (col("Outcome").isin(120, 2, 105)))
        
        if target_records.count() == 0:
            return TestResult("outOfTime_Scenario1", "PASS", "No records matching Status 10 with rejection outcomes.", test_from_state, inspect.stack()[0].function)

        # 2. Check for mismatches (Value must be 'rejected')
        failures = target_records.filter((col("outOfTimeDecisionType") != "rejected") | (col("outOfTimeDecisionType").isNull()))

        if failures.count() != 0:
            return TestResult("outOfTime_Scenario1", "FAIL", f"Found {failures.count()} rows where Status 10 rejection outcomes were not 'rejected'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("outOfTime_Scenario1", "PASS", "Status 10 rejection outcomes correctly mapped to 'rejected'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("outOfTime_Scenario1", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


############################################################################################
# outOfTimeDecisionType - Scenario 2
# Check where M3.CaseStatus != 10 and M3.Outcome is in (120, 2, 105) Expected: 'approved'
############################################################################################
def test_outOfTimeDecisionType_test2(test_df):
    try:
        # 1. Filter for outcomes in list but Status is NOT 10
        target_records = test_df.filter((col("CaseStatus") != 10) & (col("Outcome").isin(120, 2, 105)))
        
        if target_records.count() == 0:
            return TestResult("outOfTime_Scenario2", "PASS", "No records found where Status != 10 but Outcome was in rejection list.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be 'approved'
        failures = target_records.filter((col("outOfTimeDecisionType") != "approved") | (col("outOfTimeDecisionType").isNull()))

        if failures.count() != 0:
            return TestResult("outOfTime_Scenario2", "FAIL", f"Found {failures.count()} rows where Status != 10 was not mapped to 'approved'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("outOfTime_Scenario2", "PASS", "Status != 10 correctly forced to 'approved' regardless of outcome", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("outOfTime_Scenario2", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


############################################################################################
# outOfTimeDecisionType - Scenario 3
# Check where M3.CaseStatus != 10 and M3.Outcome is NOT in (120, 2, 105) Expected: 'approved'
############################################################################################
def test_outOfTimeDecisionType_test3(test_df):
    try:
        # 1. Filter for Status != 10 and Outcome NOT in the rejection list
        target_records = test_df.filter((col("CaseStatus") != 10) & (~col("Outcome").isin(120, 2, 105)))
        
        if target_records.count() == 0:
            return TestResult("outOfTime_Scenario3", "PASS", "No records matching standard approval criteria.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be 'approved'
        failures = target_records.filter((col("outOfTimeDecisionType") != "approved") | (col("outOfTimeDecisionType").isNull()))

        if failures.count() != 0:
            return TestResult("outOfTime_Scenario3", "FAIL", f"Found {failures.count()} rows where standard approval cases were not 'approved'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("outOfTime_Scenario3", "PASS", "Standard approval outcomes correctly mapped to 'approved'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("outOfTime_Scenario3", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    
############################################################################################
# hearingRequirements init code
############################################################################################
def test_hearingRequirements_init(json, M1_bronze, M3_bronze, bac):
    try:
        # 1. Select JSON fields
        test_df = json.select(
            "appealReferenceNumber",
            "isEvidenceFromOutsideUkOoc",
            "isEvidenceFromOutsideUkInCountry",
            "isInterpreterServicesNeeded",
            "singleSexCourt",
            "inCameraCourt"
        )

        # 2. Prepare M1_bronze with metadata
        m1_clean = M1_bronze.select(
            col("CaseNo").alias("m1_CaseNo"),
            "Sponsor_Name", 
            "Interpreter", 
            "CourtPreference", 
            "InCamera"
        )

        # 3. Prepare BAC table for CategoryId
        bac_clean = bac.select(col("CaseNo").alias("bac_CaseNo"), "CategoryId")

        # 4. Process M3 for Latest Status and EndedGroup
        # Note: If get_ended_group_id needs CategoryId, we join bac to M3 first
        m3_with_cat = M3_bronze.join(bac_clean, M3_bronze["CaseNo"] == bac_clean["bac_CaseNo"], "left")
        
        history_with_groups = get_ended_group_id(m3_with_cat)

        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        latest_status = history_with_groups.withColumn("rn", F.row_number().over(window_spec)) \
                                           .filter("rn = 1") \
                                           .select(
                                               "CaseNo", 
                                               "CaseStatus", 
                                               "StatusId", 
                                               "Party", 
                                               "CategoryId",
                                               "EndedGroup"
                                           )

        # 5. Master Join
        # Start with JSON -> Add M1 Metadata -> Add Latest M3 Status/Group
        test_df = test_df.join(
            m1_clean,
            test_df["appealReferenceNumber"] == m1_clean["m1_CaseNo"],
            "inner"
        ).join(
            latest_status,
            test_df["appealReferenceNumber"] == latest_status["CaseNo"],
            "inner"
        ).drop("m1_CaseNo", "CaseNo", "bac_CaseNo")

        return test_df, True

    except Exception as e:
        error_message = str(e)        
        return None, TestResult("HearingReq_Init", "FAIL", f"Failed to Setup Data: {error_message[:300]}", "ended", inspect.stack()[0].function)
#######################
# isEvidenceFromOutsideUkOoc - Scenario 1
# Check if CategoryId = 38 (Group 3/4)
# Expected: Value = "Yes" (assuming Sponsor logic is met)
#######################
def test_isEvidenceFromOutsideUkOoc_test1(test_df):
    try:
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CategoryId") == 38)
        )
        
        if target_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "No Category 38 records found.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("isEvidenceFromOutsideUkOoc") != "Yes")

        if failures.count() != 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"Found {failures.count()} Category 38 rows that were not 'Yes'", "ended", inspect.stack()[0].function)
        
        return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "CategoryId 38 correctly mapped to 'Yes'", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)


#######################
# isEvidenceFromOutsideUkOoc - Scenario 2
# Check if CategoryId != 38 (Group 3/4)
# Expected: Field should be OMITTED (Null)
#######################
def test_isEvidenceFromOutsideUkOoc_test2(test_df):
    try:
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CategoryId") != 38)
        )
        
        if target_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "No non-Category 38 records found.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("isEvidenceFromOutsideUkOoc").isNotNull())

        if failures.count() != 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"Found {failures.count()} rows where Category != 38 was incorrectly included", "ended", inspect.stack()[0].function)
        
        return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "Field correctly omitted when CategoryId is not 38", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)


#######################
# isEvidenceFromOutsideUkOoc - Scenario 3
# Check if SponsorName is NOT NULL (Group 3/4 and Category 38)
# Expected: Value = "Yes"
#######################
def test_isEvidenceFromOutsideUkOoc_test3(test_df):
    try:
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CategoryId") == 38) & 
            (col("Sponsor_Name").isNotNull())
        )
        
        if target_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "No records with SponsorName found.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("isEvidenceFromOutsideUkOoc") != "Yes")

        if failures.count() != 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"Found {failures.count()} rows with SponsorName not set to 'Yes'", "ended", inspect.stack()[0].function)
        
        return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "SponsorName correctly mapped to 'Yes'", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)


#######################
# isEvidenceFromOutsideUkOoc - Scenario 4
# Check if SponsorName IS NULL (Group 3/4 and Category 38)
# Expected: Value = "No"
#######################
def test_isEvidenceFromOutsideUkOoc_test4(test_df):
    try:
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CategoryId") == 38) & 
            (col("Sponsor_Name").isNull())
        )
        
        if target_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "No records with NULL SponsorName found.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("isEvidenceFromOutsideUkOoc") != "No")

        if failures.count() != 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"Found {failures.count()} rows with Null SponsorName not set to 'No'", "ended", inspect.stack()[0].function)
        
        return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "Null SponsorName correctly mapped to 'No'", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)




#######################
# isEvidenceFromOutsideUkInCountry - Scenario 1
# Check if CategoryId = 37 (Group 3/4)
# Expected: Value = "Yes"
#######################
def test_isEvidenceFromOutsideUkInCountry_test1(test_df):
    try:
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CategoryId") == 37)
        )
        
        if target_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkInCountry", "PASS", "No Category 37 records found.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("isEvidenceFromOutsideUkInCountry") != "Yes")

        if failures.count() != 0:
            return TestResult("isEvidenceFromOutsideUkInCountry", "FAIL", f"Found {failures.count()} Category 37 rows that were not 'Yes'", "ended", inspect.stack()[0].function)
        
        return TestResult("isEvidenceFromOutsideUkInCountry", "PASS", "CategoryId 37 correctly mapped to 'Yes'", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkInCountry", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)


#######################
# isEvidenceFromOutsideUkInCountry - Scenario 2
# Check where CategoryId != 37 (Group 3/4)
# Expected: isEvidenceFromOutsideUkInCountry is OMITTED (Null)
#######################
def test_isEvidenceFromOutsideUkInCountry_test2(test_df):
    try:
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CategoryId") != 37)
        )
        
        if target_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkInCountry", "PASS", "No non-Category 37 records found.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("isEvidenceFromOutsideUkInCountry").isNotNull())

        if failures.count() != 0:
            return TestResult("isEvidenceFromOutsideUkInCountry", "FAIL", f"Found {failures.count()} rows where Category != 37 was incorrectly included", "ended", inspect.stack()[0].function)
        
        return TestResult("isEvidenceFromOutsideUkInCountry", "PASS", "Field correctly omitted when CategoryId is not 37", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkInCountry", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)


#######################
# isEvidenceFromOutsideUkInCountry - Scenario 3
# Check if SponsorName is NOT NULL (Group 3/4 and Category 37)
# Expected: Value = "Yes"
#######################
def test_isEvidenceFromOutsideUkInCountry_test3(test_df):
    try:
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CategoryId") == 37) & 
            (col("Sponsor_Name").isNotNull())
        )
        
        if target_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkInCountry", "PASS", "No records with SponsorName found.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("isEvidenceFromOutsideUkInCountry") != "Yes")

        if failures.count() != 0:
            return TestResult("isEvidenceFromOutsideUkInCountry", "FAIL", f"Found {failures.count()} rows where populated Sponsor did not result in 'Yes'", "ended", inspect.stack()[0].function)
        
        return TestResult("isEvidenceFromOutsideUkInCountry", "PASS", "SponsorName correctly mapped to 'Yes'", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkInCountry", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)


#######################
# isEvidenceFromOutsideUkInCountry - Scenario 4
# Check if SponsorName IS NULL (Group 3/4 and Category 37)
# Expected: Value = "No"
#######################
def test_isEvidenceFromOutsideUkInCountry_test4(test_df):
    try:
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CategoryId") == 37) & 
            (col("Sponsor_Name").isNull())
        )
        
        if target_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkInCountry", "PASS", "No records with NULL SponsorName found.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("isEvidenceFromOutsideUkInCountry") != "No")

        if failures.count() != 0:
            return TestResult("isEvidenceFromOutsideUkInCountry", "FAIL", f"Found {failures.count()} rows where Null Sponsor did not result in 'No'", "ended", inspect.stack()[0].function)
        
        return TestResult("isEvidenceFromOutsideUkInCountry", "PASS", "Null SponsorName correctly mapped to 'No'", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkInCountry", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


#######################
def test_isInterpreterServicesNeeded_test1(test_df):
    try:
        # 1. Filter for Group 3/4 and Interpreter = 1
        target_records = test_df.filter(
        (col("EndedGroup").isin(3, 4)) &
        (col("Interpreter") == 1)
        )

        if target_records.count() == 0:
            return TestResult("isInterpreterServicesNeeded", "PASS", "No records with Interpreter = 1 found.", "ended", inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "Yes"
        failures = target_records.filter(col("isInterpreterServicesNeeded") != "Yes")

        if failures.count() != 0:
            return TestResult("isInterpreterServicesNeeded", "FAIL", f"Found {failures.count()} rows where Interpreter 1 was not mapped to 'Yes'", "ended", inspect.stack()[0].function)
        
        return TestResult("isInterpreterServicesNeeded", "PASS", "Interpreter 1 correctly mapped to 'Yes'", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isInterpreterServicesNeeded", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    

#######################
def test_isInterpreterServicesNeeded_test2(test_df):
    try:
# 1. Filter for Group 3/4 and Interpreter != 1 (usually 0)
        target_records = test_df.filter(
        (col("EndedGroup").isin(3, 4)) &
        (col("Interpreter") != 1)
        )

        if target_records.count() == 0:
            return TestResult("isInterpreterServicesNeeded", "PASS", "No records with Interpreter != 1 found.", "ended", inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "No"
        failures = target_records.filter(col("isInterpreterServicesNeeded") != "No")

        if failures.count() != 0:
            return TestResult("isInterpreterServicesNeeded", "FAIL", f"Found {failures.count()} rows where Interpreter != 1 was not mapped to 'No'", "ended", inspect.stack()[0].function)
        
        return TestResult("isInterpreterServicesNeeded", "PASS", "Interpreter != 1 correctly mapped to 'No'", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isInterpreterServicesNeeded", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    



#######################
def test_singleSexCourt_test1(test_df):
    try:
        # 1. Filter for Group 3/4 and CourtPreference 0
        target_records = test_df.filter(
        (col("EndedGroup").isin(3, 4)) &
        (col("CourtPreference") == 0)
        )

        if target_records.count() == 0:
            return TestResult("singleSexCourt", "PASS", "No records found in Groups 3/4 with CourtPreference 0.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "No"
        failures = target_records.filter(col("singleSexCourt") != "No")

        if failures.count() != 0:
            return TestResult("singleSexCourt", "FAIL", f"Found {failures.count()} rows (Pref 0) not set to 'No'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("singleSexCourt", "PASS", "CourtPreference 0 correctly mapped to 'No'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("singleSexCourt", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
def test_singleSexCourt_test2(test_df):
    try:
        # 1. Filter for Group 3/4 and CourtPreference NOT equal to 0
        target_records = test_df.filter(
        (col("EndedGroup").isin(3, 4)) &
        (col("CourtPreference") != 0)
        )

        if target_records.count() == 0:
            return TestResult("singleSexCourt", "PASS", "No records found in Groups 3/4 with CourtPreference != 0.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "Yes"
        failures = target_records.filter(col("singleSexCourt") != "Yes")

        if failures.count() != 0:
            return TestResult("singleSexCourt", "FAIL", f"Found {failures.count()} rows (Pref != 0) not set to 'Yes'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("singleSexCourt", "PASS", "CourtPreference != 0 correctly mapped to 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("singleSexCourt", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)



#######################
# singleSexCourtType - Scenario 1
# IF dbo.CourtPreference IS 1 = "All male"
# (MAX StatusID where EndedGroup = 3 or 4)
#######################
def test_singleSexCourtType_test1(test_df):
    try:
        # 1. Filter for the target state (Group 3 or 4 and CourtPreference 1)
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CourtPreference") == 1)
        )
        
        if target_records.count() == 0:
            return TestResult("singleSexCourtType", "PASS", "No records found with Group 3/4 and CourtPreference 1 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "All male"
        failures = target_records.filter(col("singleSexCourtType") != "All male")

        if failures.count() != 0:
            return TestResult("singleSexCourtType", "FAIL", f"Found {failures.count()} rows where CourtPreference 1 was not mapped to 'All male'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("singleSexCourtType", "PASS", "CourtPreference 1 correctly mapped to 'All male'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("singleSexCourtType", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


#######################
# singleSexCourtType - Scenario 2
# IF dbo.CourtPreference IS 2 = "All female"
# (MAX StatusID where EndedGroup = 3 or 4)
#######################
def test_singleSexCourtType_test2(test_df):
    try:
        # 1. Filter for the target state (Group 3 or 4 and CourtPreference 2)
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CourtPreference") == 2)
        )
        
        if target_records.count() == 0:
            return TestResult("singleSexCourtType", "PASS", "No records found with Group 3/4 and CourtPreference 2 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "All female"
        failures = target_records.filter(col("singleSexCourtType") != "All female")

        if failures.count() != 0:
            return TestResult("singleSexCourtType", "FAIL", f"Found {failures.count()} rows where CourtPreference 2 was not mapped to 'All female'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("singleSexCourtType", "PASS", "CourtPreference 2 correctly mapped to 'All female'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("singleSexCourtType", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


#######################
# singleSexCourtType - Scenario 3
# IF CourtPreference IS NOT 1 or 2 = OMIT
# (MAX StatusID where EndedGroup = 3 or 4)
#######################
def test_singleSexCourtType_test3(test_df):
    try:
        # 1. Filter for Group 3/4 where CourtPreference is NOT 1 or 2
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (~col("CourtPreference").isin(1, 2))
        )
        
        if target_records.count() == 0:
            return TestResult("singleSexCourtType", "PASS", "No records found with Group 3/4 and CourtPreference != 1 or 2 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Field must be Null (Omitted)
        failures = target_records.filter(col("singleSexCourtType").isNotNull())

        if failures.count() != 0:
            return TestResult("singleSexCourtType", "FAIL", f"Found {failures.count()} rows where singleSexCourtType was incorrectly included", test_from_state, inspect.stack()[0].function)
        
        return TestResult("singleSexCourtType", "PASS", "Field correctly omitted when CourtPreference is not 1 or 2", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("singleSexCourtType", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    


############################################################################################
# Field: singleSexCourtTypeDescription
############################################################################################

#######################
# singleSexCourtTypeDescription - Scenario 1
# IF dbo.CourtPreference IS 1 = Include ARIA Migrated String
# (MAX StatusID where EndedGroup = 3 or 4)
#######################
def test_singleSexCourtTypeDescription_test1(test_df):
    try:
        expected_string = "This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court."
        
        # 1. Filter for Group 3/4 and CourtPreference 1
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CourtPreference") == 1)
        )
        
        if target_records.count() == 0:
            return TestResult("singleSexCourtTypeDescription", "PASS", "No records found with Group 3/4 and CourtPreference 1 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must match the specific ARIA string
        failures = target_records.filter(
            (col("singleSexCourtTypeDescription") != expected_string) | 
            (col("singleSexCourtTypeDescription").isNull())
        )

        if failures.count() != 0:
            return TestResult("singleSexCourtTypeDescription", "FAIL", f"Found {failures.count()} rows (Pref 1) where description was incorrect or missing", test_from_state, inspect.stack()[0].function)
        
        return TestResult("singleSexCourtTypeDescription", "PASS", "CourtPreference 1 correctly mapped to ARIA migrated string", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("singleSexCourtTypeDescription", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


#######################
# singleSexCourtTypeDescription - Scenario 2
# IF dbo.CourtPreference IS 2 = Include ARIA Migrated String
# (MAX StatusID where EndedGroup = 3 or 4)
#######################
def test_singleSexCourtTypeDescription_test2(test_df):
    try:
        expected_string = "This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court."
        
        # 1. Filter for Group 3/4 and CourtPreference 2
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CourtPreference") == 2)
        )
        
        if target_records.count() == 0:
            return TestResult("singleSexCourtTypeDescription", "PASS", "No records found with Group 3/4 and CourtPreference 2 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must match the specific ARIA string
        failures = target_records.filter(
            (col("singleSexCourtTypeDescription") != expected_string) | 
            (col("singleSexCourtTypeDescription").isNull())
        )

        if failures.count() != 0:
            return TestResult("singleSexCourtTypeDescription", "FAIL", f"Found {failures.count()} rows (Pref 2) where description was incorrect or missing", test_from_state, inspect.stack()[0].function)
        
        return TestResult("singleSexCourtTypeDescription", "PASS", "CourtPreference 2 correctly mapped to ARIA migrated string", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("singleSexCourtTypeDescription", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


#######################
# singleSexCourtTypeDescription - Scenario 3
# IF CourtPreference IS NOT 1 or 2 = OMIT
# (MAX StatusID where EndedGroup = 3 or 4)
#######################
def test_singleSexCourtTypeDescription_test3(test_df):
    try:
        # 1. Filter for Group 3/4 where CourtPreference is NOT 1 or 2
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (~col("CourtPreference").isin(1, 2))
        )
        
        if target_records.count() == 0:
            return TestResult("singleSexCourtTypeDescription", "PASS", "No records found with Group 3/4 and CourtPreference != 1 or 2 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Field must be Null (Omitted)
        failures = target_records.filter(col("singleSexCourtTypeDescription").isNotNull())

        if failures.count() != 0:
            return TestResult("singleSexCourtTypeDescription", "FAIL", f"Found {failures.count()} rows where singleSexCourtTypeDescription was incorrectly included", test_from_state, inspect.stack()[0].function)
        
        return TestResult("singleSexCourtTypeDescription", "PASS", "Field correctly omitted when CourtPreference is not 1 or 2", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("singleSexCourtTypeDescription", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    




############################################################################################
# Field: inCameraCourt
############################################################################################

#######################
# inCameraCourt - Scenario 1
# IF dbo.InCamera IS 1 = "Yes"
# (MAX StatusID where EndedGroup = 3 or 4)
#######################
def test_inCameraCourt_test1(test_df):
    try:
        # 1. Filter for Group 3/4 and InCamera 1
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("InCamera") == 1)
        )
        
        if target_records.count() == 0:
            return TestResult("inCameraCourt", "PASS", "No records found in Groups 3/4 with InCamera 1.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "Yes"
        failures = target_records.filter(col("inCameraCourt") != "Yes")

        if failures.count() != 0:
            return TestResult("inCameraCourt", "FAIL", f"Found {failures.count()} rows (InCamera 1) not set to 'Yes'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("inCameraCourt", "PASS", "InCamera 1 correctly mapped to 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("inCameraCourt", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


#######################
# inCameraCourt - Scenario 2
# IF dbo.InCamera IS 0 = "No"
# (MAX StatusID where EndedGroup = 3 or 4)
#######################
def test_inCameraCourt_test2(test_df):
    try:
        # 1. Filter for Group 3/4 and InCamera 0
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("InCamera") == 0)
        )
        
        if target_records.count() == 0:
            return TestResult("inCameraCourt", "PASS", "No records found in Groups 3/4 with InCamera 0.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "No"
        failures = target_records.filter(col("inCameraCourt") != "No")

        if failures.count() != 0:
            return TestResult("inCameraCourt", "FAIL", f"Found {failures.count()} rows (InCamera 0) not set to 'No'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("inCameraCourt", "PASS", "InCamera 0 correctly mapped to 'No'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("inCameraCourt", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)



def test_hearingResponse_init(json_data, M1_bronze, M3_bronze, bac, M6_bronze):
    try:
        # Using a very unique name 'json_df_input' to avoid any conflict with 'json' module
        test_df = json_data.select(
            "appealReferenceNumber",
            "isAppealSuitableToFloat",
            "listingLength"
            # "isInCameraCourtAllowed",
            # "inCameraCourtTribunalResponse",
            # "inCameraCourtDecisionForDisplay",
            # "isSingleSexCourtAllowed",
            # "singleSexCourtTribunalResponse",
            # "singleSexCourtDecisionForDisplay"
        )

        # 2. Prepare M1_bronze with metadata (Verify if Sponsor_Name or SponsorName)
        m1_clean = M1_bronze.select(
            col("CaseNo").alias("m1_CaseNo"),
            "Sponsor_Name", 
            "Interpreter", 
            "CourtPreference", 
            "InCamera"
        )

        # 3. Prepare BAC table for CategoryId
        bac_clean = bac.select(col("CaseNo").alias("bac_CaseNo"), "CategoryId")

        # 4. Prepare M6_bronze
        m6_clean = M6_bronze.select(
            col("CaseNo").alias("m6_CaseNo")
        #     F.col("Judge_Forenames").alias("M6_Judge_Forenames"),
        #     F.col("Judge_Surname").alias("M6_Judge_Surname"),
        #     F.col("Judge_Title").alias("M6_Judge_Title"),
        #     F.col("Required").alias("M6_Required")
        )

        # 5. Process M3 history
        m3_history = M3_bronze.join(bac_clean, M3_bronze["CaseNo"] == bac_clean["bac_CaseNo"], "left")
        history_with_groups = get_ended_group_id(m3_history)

        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        
        history_with_max_group = history_with_groups.withColumn(
            "FinalEndedGroup", 
            F.max("EndedGroup").over(Window.partitionBy("CaseNo"))
        )

        # STEP B: Filter for rows that are 37 or 38 (as per Mapping Doc)
        # But keep the Group 4 designation we found in Step A
        latest_status = history_with_max_group \
            .filter(F.col("CaseStatus").isin(37, 38)) \
            .withColumn("rn", F.row_number().over(window_spec)) \
            .filter("rn = 1") \
            .select(
                "CaseNo", 
                "CaseStatus", 
                "StatusId", 
                "Party", 
                "CategoryId", 
                F.col("FinalEndedGroup").alias("EndedGroup"), # Use the spread Group ID
                "ListTypeId", 
                "TimeEstimate"
            )

        # 6. Master Join
        test_df = test_df.join(
            m1_clean,
            test_df["appealReferenceNumber"] == m1_clean["m1_CaseNo"],
            "inner"
        ).join(
            latest_status,
            test_df["appealReferenceNumber"] == latest_status["CaseNo"],
            "inner"
        ).join(
            m6_clean,
            test_df["appealReferenceNumber"] == m6_clean["m6_CaseNo"],
            "left"
        ).drop("m1_CaseNo", "CaseNo", "bac_CaseNo", "m6_CaseNo")

        return test_df, True

    except Exception as e:
        return None, TestResult("HearingResponse_Init", "FAIL", f"Error: {str(e)[:300]}", "ended")

#######################
# isAppealSuitableToFloat - Scenario 1
# IF dbo.ListTypeId IS 5 = "Yes"
# (MAX StatusID WHERE CaseStatus IN (37,38) AND EndedGroup = 4)
#######################
def test_isAppealSuitableToFloat_test1(test_df):
    try:
        # 1. Filter for Group 4 and ListTypeId 5
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("ListTypeId") == 5)
        )
        
        if target_records.count() == 0:
            return TestResult("isAppealSuitableToFloat", "PASS", "No Group 4 records found with ListTypeId 5.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "Yes"
        failures = target_records.filter(col("isAppealSuitableToFloat") != "Yes")

        if failures.count() != 0:
            return TestResult("isAppealSuitableToFloat", "FAIL", f"Found {failures.count()} rows (ListTypeId 5) not set to 'Yes'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("isAppealSuitableToFloat", "PASS", "ListTypeId 5 correctly mapped to 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isAppealSuitableToFloat", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


#######################
# isAppealSuitableToFloat - Scenario 2
# IF dbo.ListTypeId IS NOT 5 = "No"
# (MAX StatusID WHERE CaseStatus IN (37,38) AND EndedGroup = 4)
#######################
def test_isAppealSuitableToFloat_test2(test_df):
    try:
        # 1. Filter for Group 4 and ListTypeId NOT equal to 5
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("ListTypeId") != 5)
        )
        
        if target_records.count() == 0:
            return TestResult("isAppealSuitableToFloat", "PASS", "No Group 4 records found with ListTypeId != 5.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "No"
        failures = target_records.filter(col("isAppealSuitableToFloat") != "No")

        if failures.count() != 0:
            return TestResult("isAppealSuitableToFloat", "FAIL", f"Found {failures.count()} rows (ListTypeId != 5) not set to 'No'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("isAppealSuitableToFloat", "PASS", "ListTypeId != 5 correctly mapped to 'No'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isAppealSuitableToFloat", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)



#######################
# listingLength.hours - Scenario 1 (Updated to handle nested struct)
#######################
def test_listingLength_test1(test_df):
    try:
        # Filter for Group 4
        target_records = test_df.filter(col("EndedGroup") == 4)
        
        if target_records.count() == 0:
            return TestResult("listingLength.hours", "PASS", "No Group 4 records found.", "ended", inspect.stack()[0].function)

        # Note: listingLength is a struct, so we use dot notation to access hours
        # We need to make sure TimeEstimate is actually in the test_df
        if "TimeEstimate" not in test_df.columns:
             return TestResult("listingLength.hours", "FAIL", "Column 'TimeEstimate' missing from test_df", "ended", inspect.stack()[0].function)

        failures = target_records.filter(
            (col("listingLength.hours") != F.floor(col("TimeEstimate") / 60))
        )

        if failures.count() != 0:
            return TestResult("listingLength.hours", "FAIL", f"Found {failures.count()} mismatches", "ended", inspect.stack()[0].function)
        
        return TestResult("listingLength.hours", "PASS", "Hours correctly mapped", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("listingLength.hours", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)


#######################
# listingLength.minutes - Scenario 2
#######################
def test_listingLength_test2(test_df):
    try:
        target_records = test_df.filter(col("EndedGroup") == 4)
        
        if target_records.count() == 0:
            return TestResult("listingLength.minutes", "PASS", "No Group 4 records found.", "ended", inspect.stack()[0].function)

        if "TimeEstimate" not in test_df.columns:
             return TestResult("listingLength.minutes", "FAIL", "Column 'TimeEstimate' missing from test_df", "ended", inspect.stack()[0].function)

        failures = target_records.filter(
            (col("listingLength.minutes") != (col("TimeEstimate") % 60))
        )

        if failures.count() != 0:
            return TestResult("listingLength.minutes", "FAIL", f"Found {failures.count()} mismatches", "ended", inspect.stack()[0].function)
        
        return TestResult("listingLength.minutes", "PASS", "Minutes correctly mapped", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("listingLength.minutes", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
