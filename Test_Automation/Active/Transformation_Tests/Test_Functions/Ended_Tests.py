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


from pyspark.sql import functions as F
from pyspark.sql.window import Window
import inspect

def get_ended_group_id(df):
    history_window = Window.partitionBy("CaseNo").orderBy("StatusId")
    df_with_prev = df.withColumn("PrevCaseStatusID", F.lag("CaseStatus").over(history_window))

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
        .when((F.col("CaseStatus") == 46) & (F.col("Outcome") == 31) & (F.col("PrevCaseStatusId") == 10), 1)
        .otherwise(0)
    )

def test_default_mapping_init(json_data, M3_bronze):
    try:
        # Select EVERY field needed for all tests combined
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
            "appealDecisionAvailable", "isFtpaListVisible", "witnessDetails", "directions", 
            "respondentDocuments", "hearingRequirements", "hearingDocuments", "letterBundleDocuments", 
            "caseBundles", "finalDecisionAndReasonsDocuments", "ftpaAppellantDocuments", 
            "ftpaAppellantGroundsDocuments", "ftpaAppellantEvidenceDocuments", "ftpaAppellantOutOfTimeDocuments"
        )

        full_status_with_groups = get_ended_group_id(M3_bronze)
        
        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        
        latest_status = full_status_with_groups.withColumn("rn", F.row_number().over(window_spec)) \
                                               .filter("rn = 1")

        # 2. Join the group info to the JSON fields selected above
        test_df = test_df.join(
            latest_status.select("CaseNo", "EndedGroup", "CaseStatus", "StatusId", "Outcome"),
            test_df.appealReferenceNumber == latest_status.CaseNo,
            "left"
        ).drop("CaseNo")

        return test_df, True
    except Exception as e:
        return None, TestResult("Init", "FAIL", f"Error: {str(e)[:200]}", "ended")

def test_ended_defaultValues(test_df, fields_to_exclude):
    results_list = []
    
    # GROUP REQUIREMENTS 
    group_requirements = {
        # Group 3, 4
        "isAppellantAttendingTheHearing": [3, 4], "isAppellantGivingOralEvidence": [3, 4],
        "isWitnessesAttending": [3, 4], "isHearingRoomNeeded": [3, 4], "isHearingLoopNeeded": [3, 4],
        "remoteVideoCall": [3, 4], "remoteVideoCallDescription": [3, 4], "physicalOrMentalHealthIssues": [3, 4],
        "physicalOrMentalHealthIssuesDescription": [3, 4], "pastExperiences": [3, 4], "pastExperiencesDescription": [3, 4],
        "multimediaEvidence": [3, 4], "multimediaEvidenceDescription": [3, 4], "additionalRequests": [3, 4],
        "additionalRequestsDescription": [3, 4], "datesToAvoidYesNo": [3, 4], "appealReviewOutcome": [3, 4],
        "appealResponseAvailable": [3, 4], "reviewedHearingRequirements": [3, 4], "amendResponseActionAvailable": [3, 4],
        "currentHearingDetailsVisible": [3, 4], "reviewResponseActionAvailable": [3, 4], "reviewHomeOfficeResponseByLegalRep": [3, 4],
        "submitHearingRequirementsAvailable": [3, 4], "uploadHomeOfficeAppealResponseActionAvailable": [3, 4], "hearingRequirements": [3, 4],
        
        # Group 4
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
        "letterBundleDocuments": [4], "caseBundles": [4], "finalDecisionAndReasonsDocuments": [4], "ftpaAppellantDocuments": [4], "ftpaAppellantGroundsDocuments": [4],
        "ftpaAppellantEvidenceDocuments": [4], "ftpaAppellantOutOfTimeDocuments": [4], "anonymityOrder": [4],
        
        # Group 2, 3, 4
        "directions": [2, 3, 4], "uploadHomeOfficeBundleAvailable": [2, 3, 4], "uploadHomeOfficeBundleActionAvailable": [2, 3, 4],
        "caseArgumentAvailable": [2, 3, 4], "reasonsForAppealDecision": [2, 3, 4], "respondentDocuments": [2, 3, 4]
    }

    # EXPECTED DEFAULTS
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

    # EXPECTED ARRAYS
    expected_arrays = {
        "witnessDetails": None, "directions": None, "respondentDocuments": None, "hearingRequirements": None,
        "hearingDocuments": None, "letterBundleDocuments": None, "caseBundles": None, "finalDecisionAndReasonsDocuments": None,
        "ftpaAppellantDocuments": None, "ftpaAppellantGroundsDocuments": None, "ftpaAppellantEvidenceDocuments": None,
        "ftpaAppellantOutOfTimeDocuments": None
    }

    try:
        # Loop Defaults
        for field, expected in expected_defaults.items():
            if field in fields_to_exclude: continue
            valid_groups = group_requirements.get(field, [1, 2, 3, 4])
            subset = test_df.filter(F.col("EndedGroup").isin(valid_groups))
            condition = (F.col(field) != expected) | (F.col(field).isNull())
            
            fail_count = subset.filter(condition).count()
            if fail_count > 0:
                results_list.append(TestResult(field, "FAIL", f"Mismatches in Groups {valid_groups}: {fail_count}", "ended", inspect.stack()[0].function))
            else:
                results_list.append(TestResult(field, "PASS", f"Valid for Groups {valid_groups}", "ended", inspect.stack()[0].function))

        # Loop Arrays
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

        # 2. Prepare BAC table (selecting only necessary columns)
        # We assume BAC joins on CaseNo or AppealReference
        bac_clean = bac.select(col("CaseNo").alias("bac_CaseNo"), "CategoryId")

        # 3. Join BAC to M3_bronze before calculating Groups
        # This ensures CategoryId is available for the EndedGroup logic
        m3_with_category = M3_bronze.join(
            bac_clean, 
            M3_bronze["CaseNo"] == bac_clean["bac_CaseNo"], 
            "left"
        ).drop("bac_CaseNo")

        # 4. Get Group Logic (Now has access to CategoryId if needed in get_ended_group_id)
        history_with_groups = get_ended_group_id(m3_with_category) 

        # 5. Isolate the LATEST status (MAX StatusID)
        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        latest_status = history_with_groups.withColumn("rn", F.row_number().over(window_spec)) \
                                           .filter("rn = 1") \
                                           .select(
                                               "CaseNo", "CaseStatus", "StatusId", "Party", 
                                               "CategoryId", "SponsorName", "Interpreter", 
                                               "CourtPreference", "InCamera", "EndedGroup"
                                           )

        # 6. Final Join
        # Join M1 first, then join the LATEST status with Category and Group info
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
        error_message = str(e)        
        return None, TestResult("HearingReq_Init", "FAIL", f"Failed to Setup Data: {error_message[:300]}", "ended", inspect.stack()[0].function)
#######################
# isEvidenceFromOutsideUkOoc - Scenario 1
# Check Where CaseNo is NOT in ended groups 3 or 4
# Expected: Field should be OMITTED (Null)
#######################
def test_isEvidenceFromOutsideUkOoc_test1(test_df):
    try:
        # 1. Filter for records NOT in Group 3 or 4
        target_records = test_df.filter(~col("EndedGroup").isin(3, 4))
        
        if target_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "No records found outside of Groups 3/4 to test omission.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Field must be Null
        failures = target_records.filter(col("isEvidenceFromOutsideUkOoc").isNotNull())

        if failures.count() != 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"Found {failures.count()} rows not in Group 3/4 where field was incorrectly populated", test_from_state, inspect.stack()[0].function)
        
        return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "Field correctly omitted for non-target groups", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


#######################
# isEvidenceFromOutsideUkOoc - Scenario 2
# Check if CategoryId = 38 (Group 3/4) and SponsorName IS NOT NULL
# Expected: "Yes"
#######################
def test_isEvidenceFromOutsideUkOoc_test2(test_df):
    try:
        # 1. Filter for Group 3/4, Category 38, and existing Sponsor
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CategoryId") == 38) & 
            (col("SponsorName").isNotNull())
        )
        
        if target_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "No Category 38 records with Sponsor found.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "Yes"
        failures = target_records.filter(col("isEvidenceFromOutsideUkOoc") != "Yes")

        if failures.count() != 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"Found {failures.count()} rows (Cat 38 + Sponsor) not set to 'Yes'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "Category 38 with Sponsor correctly mapped to 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


#######################
# isEvidenceFromOutsideUkOoc - Scenario 3
# Check if CategoryId != 38 (Group 3/4)
# Expected: Field should be OMITTED (Null)
#######################
def test_isEvidenceFromOutsideUkOoc_test3(test_df):
    try:
        # 1. Filter for Group 3/4 where Category is NOT 38
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CategoryId") != 38)
        )
        
        if target_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "No records found in Groups 3/4 with Category != 38.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Field must be Null
        failures = target_records.filter(col("isEvidenceFromOutsideUkOoc").isNotNull())

        if failures.count() != 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"Found {failures.count()} rows where Category != 38 was incorrectly included", test_from_state, inspect.stack()[0].function)
        
        return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "Field correctly omitted for Category != 38", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)


#######################
# isEvidenceFromOutsideUkOoc - Scenario 4
# Check if SponsorName IS NULL (Group 3/4 and Category 38)
# Expected: "No"
#######################
def test_isEvidenceFromOutsideUkOoc_test4(test_df):
    try:
        # 1. Filter for Group 3/4, Category 38, and NULL Sponsor
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CategoryId") == 38) & 
            (col("SponsorName").isNull())
        )
        
        if target_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "No Category 38 records with NULL Sponsor found.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "No"
        failures = target_records.filter(col("isEvidenceFromOutsideUkOoc") != "No")

        if failures.count() != 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"Found {failures.count()} rows (Cat 38 + Null Sponsor) not set to 'No'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("isEvidenceFromOutsideUkOoc", "PASS", "Category 38 with Null Sponsor correctly mapped to 'No'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)




