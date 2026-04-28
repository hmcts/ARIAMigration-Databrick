from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
from pyspark.sql.functions import (
    col, lit, when, array, array_union, array_sort, 
    create_map, date_add, date_format, max as spark_max
)
from itertools import chain
import inspect
import re
import inspect

#Import Test Results class
from models.test_result import TestResult

#Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "ended"


import inspect
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import inspect

############################################################################################
# Default Mapping
############################################################################################

def get_ended_group_id(df):

    history_window = Window.partitionBy("CaseNo").orderBy("StatusId")
    

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
        .when((F.col("CaseStatus") == 46) & (F.col("Outcome") == 31) & (F.col("PrevCaseStatusId") == 10), 1)
        .otherwise(0)
    )


def test_default_mapping_init(json_data, M1_silver, M3_bronze):
    try:

        manual_fields = [
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
            "appealDecisionAvailable", "isFtpaListVisible", "hmcts", "witnessDetails", "directions", 
            "respondentDocuments", "hearingRequirements", "hearingDocuments", "letterBundleDocuments", 
            "caseBundles", "finalDecisionAndReasonsDocuments",
            "witness1InterpreterSignLanguage", "witness2InterpreterSignLanguage", "witness3InterpreterSignLanguage", 
            "witness4InterpreterSignLanguage", "witness5InterpreterSignLanguage", "witness6InterpreterSignLanguage",
            "witness7InterpreterSignLanguage", "witness8InterpreterSignLanguage", "witness9InterpreterSignLanguage",
            "witness10InterpreterSignLanguage", "witness1InterpreterSpokenLanguage", "witness2InterpreterSpokenLanguage",
            "witness3InterpreterSpokenLanguage", "witness4InterpreterSpokenLanguage", "witness5InterpreterSpokenLanguage",
            "witness6InterpreterSpokenLanguage", "witness7InterpreterSpokenLanguage", "witness8InterpreterSpokenLanguage",
            "witness9InterpreterSpokenLanguage", "witness10InterpreterSpokenLanguage"
        ]

 
        available_fields = [f for f in manual_fields if f in json_data.columns]
        test_df = json_data.select(*available_fields)

  
        full_status_with_groups = get_ended_group_id(M3_bronze)
        
      
        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        latest_status = full_status_with_groups.withColumn("rn", F.row_number().over(window_spec)) \
                                               .filter(F.col("rn") == 1) \
                                               .select(F.col("CaseNo").alias("M3_CaseNo"), "EndedGroup", "CaseStatus", "StatusId", "Outcome")

        test_df = test_df.join(
            latest_status, test_df.appealReferenceNumber == latest_status.M3_CaseNo, "left"
        ).join(
            M1_silver.select(F.col("CaseNo").alias("M1_CaseNo"), "Dv_Representation"),
            test_df.appealReferenceNumber == F.col("M1_CaseNo"), "left"
        ).drop("M3_CaseNo", "M1_CaseNo")

        return test_df, True
    except Exception as e:
        return None, TestResult("Init", "FAIL", f"Error: {str(e)[:500]}", "ended", "init")

############################################################################################
# 3. Test: Default Values for Ended State
############################################################################################

def test_ended_defaultValues(test_df, fields_to_exclude):
    results_list = []
    
    # Omitted FTPA fields that should fail as "No data to test"
    omitted_fields = [
        "ftpaAppellantDocuments", "ftpaAppellantGroundsDocuments", 
        "ftpaAppellantEvidenceDocuments", "ftpaAppellantOutOfTimeDocuments"
    ]

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
        "letterBundleDocuments": [4], "caseBundles": [4], "finalDecisionAndReasonsDocuments": [4],
        "anonymityOrder": [4],
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

    try:
        # Step 1: Explicitly FAIL omitted fields
        for field in omitted_fields:
            if field in fields_to_exclude: continue
            results_list.append(TestResult(field, "FAIL", "No data to test: Field omitted from ended state", "ended", "omitted_check"))

        # Step 2: Loop for Simple Defaults
        for field, expected in expected_defaults.items():
            if field in fields_to_exclude or field in omitted_fields: continue
            
            # Override for fields confirmed as "Yes" in ended Groups
            current_expected = "Yes" if field in ["uploadHomeOfficeBundleAvailable", "reviewedHearingRequirements"] else expected

            valid_groups = group_requirements.get(field, [1, 2, 3, 4])
            subset = test_df.filter(F.col("EndedGroup").isin(valid_groups))
            
            # Representation filtering
            if field == "caseArgumentAvailable":
                subset = subset.filter(F.col("Dv_Representation") == "LR")
            elif field == "reasonsForAppealDecision":
                subset = subset.filter(F.col("Dv_Representation") == "AIP")

            if subset.count() == 0: continue

            condition = (F.col(field) != current_expected) | (F.col(field).isNull())
            fail_count = subset.filter(condition).count()
            
            status = "PASS" if fail_count == 0 else "FAIL"
            msg = f"Valid for Groups {valid_groups}" if fail_count == 0 else f"Mismatches in Groups {valid_groups}: {fail_count}"
            results_list.append(TestResult(field, status, msg, "ended", inspect.stack()[0].function))

        # Step 3: Loop for Arrays
        expected_arrays = {"witnessDetails": None, "directions": None, "respondentDocuments": None, "hearingRequirements": None, "hearingDocuments": None, "letterBundleDocuments": None, "caseBundles": None, "finalDecisionAndReasonsDocuments": None}
        
        for field, contains_val in expected_arrays.items():
            if field in fields_to_exclude or field in omitted_fields: continue
            valid_groups = group_requirements.get(field, [1, 2, 3, 4])
            subset = test_df.filter(F.col("EndedGroup").isin(valid_groups))
            
            condition = (~F.array_contains(F.col(field), contains_val)) if contains_val else (F.size(F.col(field)) != 0)
                
            fail_count = subset.filter(condition).count()
            status = "PASS" if fail_count == 0 else "FAIL"
            msg = f"Array valid for Groups {valid_groups}" if fail_count == 0 else f"Array mismatch in Groups {valid_groups}: {fail_count}"
            results_list.append(TestResult(field, status, msg, "ended", inspect.stack()[0].function))

        return results_list
    except Exception as e:
        return [TestResult("DefaultMapping", "FAIL", f"Error: {str(e)[:300]}", "ended", "test")]

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
def test_hearingRequirements_init(json_data, M1_bronze, M3_bronze, bac):
    try:
        # 1. Select JSON fields
        test_df = json_data.select(
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
                                               "EndedGroup",
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



def test_hearingResponse_init(json_data, M1_bronze, M3_bronze, bac, M6_bronze, M1_silver, M2_bronze):
    try:
        # Using a very unique name 'json_df_input' to avoid any conflict with 'json' module
        test_df = json_data.select(
            "appealReferenceNumber",
            "isAppealSuitableToFloat",
            "listingLength",
            "hearingChannel",
            "listingLocation",
            "actualCaseHearingLength",
            "listCaseHearingLength",
            "listCaseHearingDate",
            "listCaseHearingCentre",
            "listCaseHearingCentreAddress",
            "sendDecisionsAndReasonsDate",
            "appealDate",
            "appealDecision",
            "isDecisionAllowed",
            "attendingJudge",
            "actualCaseHearingLength",
            "ftpaApplicationDeadline",
            "ftpaList",
            "ftpaAppellantApplicationDate",
            "ftpaAppellantSubmissionOutOfTime",
            "ftpaAppellantOutOfTimeExplanation",
            "endAppealOutcome",
            "endAppealOutcomeReason",
            "endAppealApproverType",
            "endAppealApproverName",
            "endAppealDate",
            "stateBeforeEndAppeal",
            "bundleFileNamePrefix",
            "ftpaAppellantSubmitted",
            "isFtpaAppellantDocsVisibleInDecided",
            "isFtpaAppellantDocsVisibleInSubmitted",
            "isFtpaAppellantOotDocsVisibleInDecided",
            "isFtpaAppellantOotDocsVisibleInSubmitted",
            "isFtpaAppellantGroundsDocsVisibleInDecided",
            "isFtpaAppellantEvidenceDocsVisibleInDecided",
            "isFtpaAppellantGroundsDocsVisibleInSubmitted",
            "isFtpaAppellantEvidenceDocsVisibleInSubmitted",
            "isFtpaAppellantOotExplanationVisibleInDecided",
            "isFtpaAppellantOotExplanationVisibleInSubmitted",
            "ftpaRespondentSubmitted",
            "isFtpaRespondentDocsVisibleInDecided",
            "isFtpaRespondentDocsVisibleInSubmitted",
            "isFtpaRespondentOotDocsVisibleInDecided",
            "isFtpaRespondentOotDocsVisibleInSubmitted",
            "isFtpaRespondentGroundsDocsVisibleInDecided",
            "isFtpaRespondentEvidenceDocsVisibleInDecided",
            "isFtpaRespondentGroundsDocsVisibleInSubmitted",
            "isFtpaRespondentEvidenceDocsVisibleInSubmitted",
            "isFtpaRespondentOotExplanationVisibleInDecided",
            "isFtpaRespondentOotExplanationVisibleInSubmitted"
            # "ftpaRespondentApplicationDate",
            # "ftpaRespondentSubmissionOutOfTime",
            # "ftpaRespondentOutOfTimeExplanation"
            # "isInCameraCourtAllowed",
            # "inCameraCourtTribunalResponse",
            # "inCameraCourtDecisionForDisplay",
            # "isSingleSexCourtAllowed",
            # "singleSexCourtTribunalResponse",
            # "singleSexCourtDecisionForDisplay"
        )

        m1_clean = M1_bronze.select(
            col("CaseNo").alias("m1_CaseNo"),
            "Sponsor_Name", 
            "Interpreter", 
            "CourtPreference", 
            "InCamera",
            "VisitVisaType"
        )

        M2_bronze = M2_bronze.select(
            col("CaseNo").alias("M2_CaseNo"),
            "Appellant_Name"
        )

        m1_silver_clean = M1_silver.select(
            col("CaseNo").alias("m1_silver_CaseNo"),
            "dv_representation"
        )
        bac_clean = bac.select(col("CaseNo").alias("bac_CaseNo"), "CategoryId")

        m6_clean = M6_bronze.select(
            col("CaseNo").alias("m6_CaseNo")
        ).groupBy("m6_CaseNo").count().select("m6_CaseNo")


        m3_history = M3_bronze.join(bac_clean, M3_bronze["CaseNo"] == bac_clean["bac_CaseNo"], "left")
        
        history_with_groups = get_ended_group_id(m3_history)

        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        
        history_with_max_group = history_with_groups.withColumn(
            "FinalEndedGroup", 
            F.max("EndedGroup").over(Window.partitionBy("CaseNo"))
        )

        # Filter for rows that are 37 or 38 (as per Mapping Doc)
    
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
                F.col("FinalEndedGroup").alias("EndedGroup"), 
                "ListTypeId", 
                "TimeEstimate",
                "HearingDate",
                "StartTime",
                "HearingCentre",
                "Outcome",
                "DecisionDate",
                "DateReceived",
                "Adj_Determination_Title",
                "Adj_Determination_Forenames",
                "Adj_Determination_Surname",
                "HearingDuration",
                "DecisionDate",
                "OutOfTime"
            )

        # 6. Master Join
        test_df = test_df.join(
            m1_clean,
            test_df["appealReferenceNumber"] == m1_clean["m1_CaseNo"],
            "inner"
        ).join(
            m1_silver_clean,
            test_df["appealReferenceNumber"] == m1_silver_clean["m1_silver_CaseNo"],
            "inner"
        ).join(
            M2_bronze,
            test_df["appealReferenceNumber"] == M2_bronze["M2_CaseNo"],
            "inner"
        ).join(
            latest_status,
            test_df["appealReferenceNumber"] == latest_status["CaseNo"],
            "left"
        ).join(
            m6_clean,
            test_df["appealReferenceNumber"] == m6_clean["m6_CaseNo"],
            "left"
        ).drop("m1_CaseNo", "m1_silver_CaseNo", "CaseNo", "bac_CaseNo", "m6_CaseNo")

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

#######################
# hearingChannel
#######################
def test_hearingChannel_test1(test_df):
    try:
        # Filter for Group 4 and VisitVisaType 1
        target_df = test_df.filter((F.col("EndedGroup") == 4) & (F.col("VisitVisaType").cast("string") == "1"))
        
        if target_df.count() == 0:
            return TestResult("hearingChannel", "PASS", "No records found where VisitVisaType is 1.", "ended", "test_hearingChannel_test1")

        # VALIDATION: Matching "On The Papers" exactly as seen in your data
        # Using upper() for the code and a case-insensitive match for the label is safest
        failures = target_df.filter(
            (F.upper(F.col("hearingChannel.code")) != "ONPPRS") | 
            (F.lower(F.col("hearingChannel.label")) != "on the papers")
        )

        if failures.count() > 0:
            mismatch = failures.select("hearingChannel.code", "hearingChannel.label").first()
            return TestResult(
                "hearingChannel", 
                "FAIL", 
                f"Mismatch found. Actual: '{mismatch[0]}' / '{mismatch[1]}'. Expected: 'ONPPRS' / 'On The Papers'", 
                "ended", 
                "test_hearingChannel_test1"
            )
        
        return TestResult("hearingChannel", "PASS", "VisitVisaType 1 correctly mapped to 'On The Papers'.", "ended", "test_hearingChannel_test1")
    except Exception as e:
        return TestResult("hearingChannel", "FAIL", f"EXCEPTION: {str(e)}", "ended", "test_hearingChannel_test1")
    


def test_hearingChannel_test2(test_df):
    try:
        # Filter for Group 4 and VisitVisaType 2
        target_df = test_df.filter((F.col("EndedGroup") == 4) & (F.col("VisitVisaType") == 2))
        
        if target_df.count() == 0:
            return TestResult("hearingChannel", "PASS", "No records found where VisitVisaType is 2.", "ended", "test_hearingChannel_test2")

        # VALIDATION: Only check fields that exist in the struct (code, label)
        failures = target_df.filter(
            (F.col("hearingChannel.code") != "INTER") | 
            (F.col("hearingChannel.label") != "In Person")
        )

        if failures.count() > 0:
            sample_cases = failures.select("appealReferenceNumber").limit(5).toPandas()["appealReferenceNumber"].tolist()
            return TestResult("hearingChannel", "FAIL", f"Mismatches found for VisitVisaType 2. Examples: {sample_cases}", "ended", "test_hearingChannel_test2")
        
        return TestResult("hearingChannel", "PASS", "VisitVisaType 2 correctly mapped to 'In Person'.", "ended", "test_hearingChannel_test2")
    except Exception as e:
        return TestResult("hearingChannel", "FAIL", f"EXCEPTION: {str(e)}", "ended", "test_hearingChannel_test2")
    

#######################
# listingLocation
#######################
from itertools import chain
def test_listingLocation_test1(test_df):
    try:
        # 1. Scenario Filter: EndedGroup 4 and CaseStatus in (37, 38)
        # Note:init function usually handles MAX(StatusId) selection
        target_df = test_df.filter(
            (F.col("EndedGroup") == 4) & 
            (F.col("CaseStatus").isin(37, 38))
        )
        
        if target_df.count() == 0:
            return TestResult("listingLocation", "PASS", "No records found for the specified statuses in Group 4.", "ended", "test_listingLocation_test1")

        # 2. Define valid mapping dictionary (Code -> Label)
        location_map = {
            "227101": "Newport Tribunal Centre - Columbus House",
            "231596": "Birmingham Civil And Family Justice Centre",
            "366559": "Atlantic Quay - Glasgow",
            "366796": "Newcastle Civil And Family Courts And Tribunals Centre",
            "386417": "Hatton Cross Tribunal Hearing Centre",
            "443257": "North Tyneside Magistrates Court",
            "580554": "Bradford and Keighley Magistrates Court and Family Court",
            "618632": "Nottingham Magistrates Court",
            "649000": "Yarls Wood Immigration And Asylum Hearing Centre",
            "698118": "Bradford Tribunal Hearing Centre",
            "745389": "Hendon Magistrates Court",
            "765324": "Taylor House Tribunal Hearing Centre",
            "326944": "Manchester Crown Court (Minshull st)",
            "144641": "Manchester Crown Court (Crown Square)",
            "787030": "Coventry Magistrates Court",
            "783803": "Manchester Magistrates Court",
            "569737": "Leeds Magistrates Court and Family Court",
            "28837": "Harmondsworth Tribunal Hearing Centre",
            "999971": "Alloa Sheriff Court",
            "999973": "Belfast Laganside Court",
            "512401": "Manchester Tribunal Hearing Centre - Piccadilly Exchange"
        }

        # 3. Create conditional expression to validate label matches the code
        # We handle both nested (value.code) and flat (.code) just in case
        is_nested = "value" in target_df.schema["listingLocation"].dataType.names
        code_col = "listingLocation.value.code" if is_nested else "listingLocation.code"
        label_col = "listingLocation.value.label" if is_nested else "listingLocation.label"

        # Check for invalid mappings
        failures = target_df.withColumn("expected_label", 
            F.create_map([F.lit(x) for x in chain(*location_map.items())])[F.col(code_col)]
        ).filter(
            (F.col(label_col) != F.col("expected_label")) | (F.col(code_col).isNull())
        )

        if failures.count() > 0:
            sample = failures.select("appealReferenceNumber", code_col, label_col).limit(5).toPandas()
            return TestResult(
                "listingLocation", 
                "FAIL", 
                f"Invalid location mapping in {failures.count()} records. Example: {sample.values.tolist()}", 
                "ended", 
                "test_listingLocation_test1"
            )
        
        return TestResult("listingLocation", "PASS", "listingLocation values correctly mapped for Group 4.", "ended", "test_listingLocation_test1")

    except Exception as e:
        return TestResult("listingLocation", "FAIL", f"EXCEPTION: {str(e)}", "ended", "test_listingLocation_test1")

#######################
# listCaseHearingLength
#######################
def test_listCaseHearingLength_test1(test_df):
    """
    Scenario: listCaseHearingLength Rounding logic.
    BA NOTE: Passing all records regardless of rounding consistency.
    Reasoning: Ended cases (Group 4) are closed and will not be re-listed for hearings.
    """
    try:
        # Filter for the correct scenario group
        target_df = test_df.filter(
            (F.col("EndedGroup") == 4) & 
            (F.col("CaseStatus").isin(37, 38))
        )
        
        if target_df.count() == 0:
            return TestResult("listCaseHearingLength", "PASS", "No records found for Group 4.", "ended", "test_listCaseHearingLength_test1")

        # We verify the field is at least populated to ensure data exists
        null_count = target_df.filter(F.col("actualCaseHearingLength").isNull()).count()
        
        if null_count > 0:
            return TestResult(
                "listCaseHearingLength", 
                "FAIL", 
                f"Found {null_count} records with NULL hearing length.", 
                "ended", 
                "test_listCaseHearingLength_test1"
            )

        # Return PASS for all other cases per BA instruction
        return TestResult(
            "listCaseHearingLength", 
            "PASS", 
            "Passed with noted inconsistencies; Ended cases will not be listed for a hearing.", 
            "ended", 
            "test_listCaseHearingLength_test1"
        )

    except Exception as e:
        return TestResult("listCaseHearingLength", "FAIL", f"EXCEPTION: {str(e)}", "ended", "test_listCaseHearingLength_test1")
#######################
# listCaseHearingDate
#######################   
def test_listCaseHearingDate_test1(test_df):
    """Scenario: Combine HearingDate and StartTime into ISO format"""
    try:
        target_df = test_df.filter((F.col("EndedGroup") == 4) & (F.col("CaseStatus").isin(37, 38)))
        if target_df.count() == 0:
            return TestResult("listCaseHearingDate", "PASS", "No records found.", "ended", "test_listCaseHearingDate_test1")

        # Format expected string using HearingDate and extraction from StartTime Timestamp
        target_df = target_df.withColumn("expected_date_str", 
            F.concat(
                F.date_format(F.col("HearingDate"), "yyyy-MM-dd"),
                F.lit("T"),
                F.date_format(F.col("StartTime"), "HH:mm:ss"),
                F.lit(".000")
            )
        )

        failures = target_df.filter(F.col("listCaseHearingDate") != F.col("expected_date_str"))

        if failures.count() > 0:
            sample = failures.select("listCaseHearingDate", "expected_date_str").first()
            return TestResult("listCaseHearingDate", "FAIL", f"Mismatch. Actual: {sample[0]}, Expected: {sample[1]}", "ended", "test_listCaseHearingDate_test1")
        
        return TestResult("listCaseHearingDate", "PASS", "DateTime mapping correct.", "ended", "test_listCaseHearingDate_test1")
    except Exception as e:
        return TestResult("listCaseHearingDate", "FAIL", f"EXCEPTION: {str(e)}", "ended", "test_listCaseHearingDate_test1")

#######################
# listCaseHearingCentre
#######################   

def test_listCaseHearingCentre_test1(test_df):
    try:
        # 1. Filter: EndedGroup 4 and CaseStatus 37/38
        target_df = test_df.filter(
            (F.col("EndedGroup") == 4) & 
            (F.col("CaseStatus").isin(37, 38))
        )
        
        if target_df.count() == 0:
            return TestResult("listCaseHearingCentre", "PASS", "No records found.", "ended", "test_listCaseHearingCentre_test1")

        # 2. Define the Mapping Table (ListedCentre -> [Code, Address])
        # Note: I've grouped these by the source ListedCentre string provided in your table
        centre_mapping = {
            "Alloa Sheriff Court": ["alloaSherrif", "Alloa Sheriff Court, 47 Drysdale Street, Alloa, FK10 1JA"],
            "Belfast - Laganside": ["belfast", "Belfast Laganside Court, 45 Donegall Quay, BT1 3LL"],
            "Birmingham IAC (Priory Courts)": ["birmingham", "Birmingham Civil And Family Justice Centre, Priory Courts, 33 Bull Street, B4 6DS"],
            "Birmingham IAC Sheldon Court": ["birmingham", "Birmingham Civil And Family Justice Centre, Priory Courts, 33 Bull Street, B4 6DS"],
            "Bradford": ["bradford", "Bradford Tribunal Hearing Centre, Rushton Avenue, BD3 7BH"],
            "Bradford Crown Court": ["bradford", "Bradford Tribunal Hearing Centre, Rushton Avenue, BD3 7BH"],
            "Bradford Magistrates Court": ["bradfordKeighley", "Bradford and Keighley Magistrates Court and Family Court, The Tyrls, PO Box 187, BD1 1JL"],
            "Coventry Magistrates' Court IAC": ["coventry", "Coventry Magistrates Court, Little Park Street, CV1 2SQ"],
            "Field House (TH)": ["taylorHouse", "Taylor House Tribunal Hearing Centre, Rosebery Avenue, EC1R 4QU"],
            "Glasgow (Eagle Building)": ["glasgowTribunalsCentre", "Atlantic Quay - Glasgow, 20 York Street, Glasgow, G2  8GT"],
            "Glasgow (Tribunals Centre)": ["glasgowTribunalsCentre", "Atlantic Quay - Glasgow, 20 York Street, Glasgow, G2  8GT"],
            "Harmondsworth": ["harmondsworth", "Harmondsworth Tribunal Hearing Centre, Colnbrook Bypass, UB7 0HB"],
            "Harmondsworth (HX)": ["harmondsworth", "Harmondsworth Tribunal Hearing Centre, Colnbrook Bypass, UB7 0HB"],
            "Hatton Cross": ["hattonCross", "Hatton Cross Tribunal Hearing Centre, York House And Wellington House, 2-3 Dukes Green, Feltham, Middlesex, TW14 0LS"],
            "Hendon Magistrates Court (HX)": ["hendon", "Hendon Magistrates Court, The Court House, The Hyde, NW9 7BY"],
            "Hendon Magistrates Court (TH)": ["hendon", "Hendon Magistrates Court, The Court House, The Hyde, NW9 7BY"],
            "Manchester (Piccadilly)": ["manchester", "Manchester Tribunal Hearing Centre - Piccadilly Exchange, Piccadilly Plaza, M1 4AH"],
            "Newcastle CFCTC": ["newcastle", "Newcastle Civil And Family Courts And Tribunals Centre, Barras Bridge, Newcastle-Upon-Tyne, NE1 8QF"],
            "Newcastle Law Courts": ["newcastle", "Newcastle Civil And Family Courts And Tribunals Centre, Barras Bridge, Newcastle-Upon-Tyne, NE1 8QF"],
            "Newport (Columbus House)": ["newport", "Newport Tribunal Centre - Columbus House, Langstone Business Park, Newport, NP18 2LX"],
            "North Shields (Kings Court)": ["newcastle", "Newcastle Civil And Family Courts And Tribunals Centre, Barras Bridge, Newcastle-Upon-Tyne, NE1 8QF"],
            "North Tyneside Magistrates Court": ["nthTyneMags", "North Tyneside Magistrates Court, Tynemouth Road, The Court House, NE30 1AG"],
            "Nottingham Justice Centre": ["nottingham", "Nottingham Magistrates Court, Carrington Street, NG2 1EE"],
            "Stockport Magistrates' Court": ["manchester", "Manchester Tribunal Hearing Centre - Piccadilly Exchange, Piccadilly Plaza, M1 4AH"],
            "Taylor House": ["taylorHouse", "Taylor House Tribunal Hearing Centre, Rosebery Avenue, EC1R 4QU"],
            "Wigan and Leigh Magistrates' Court": ["manchester", "Manchester Tribunal Hearing Centre - Piccadilly Exchange, Piccadilly Plaza, M1 4AH"],
            "Yarl's Wood": ["yarlsWord", "Yarls Wood Immigration And Asylum Hearing Centre, Twinwood Road, MK44 1FD"]
        }

        # 3. Apply Mapping and Compare
        # Create separate maps for code and address
        code_map = F.create_map([F.lit(x) for x in chain(*[(k, v[0]) for k, v in centre_mapping.items()])])
        addr_map = F.create_map([F.lit(x) for x in chain(*[(k, v[1]) for k, v in centre_mapping.items()])])

        target_df = target_df.withColumn("expected_code", code_map[F.col("HearingCentre")]) \
                             .withColumn("expected_addr", addr_map[F.col("HearingCentre")])

        # Validation Logic: Check both JSON fields against expected values
        failures = target_df.filter(
            (F.col("listCaseHearingCentre") != F.col("expected_code")) | 
            (F.col("listCaseHearingCentreAddress") != F.col("expected_addr"))
        )

        if failures.count() > 0:
            sample = failures.select("appealReferenceNumber", "HearingCentre", "listCaseHearingCentre", "expected_code").limit(5).toPandas()
            return TestResult(
                "listCaseHearingCentre", 
                "FAIL", 
                f"Mapping mismatch in {failures.count()} records. Examples: {sample.values.tolist()}", 
                "ended", 
                "test_listCaseHearingCentre_test1"
            )
        
        return TestResult("listCaseHearingCentre", "PASS", "listCaseHearingCentre and Address correctly mapped.", "ended", "test_listCaseHearingCentre_test1")

    except Exception as e:
        return TestResult("listCaseHearingCentre", "FAIL", f"EXCEPTION: {str(e)}", "ended", "test_listCaseHearingCentre_test1")
    
#######################
# listCaseHearingCentreAddress
#######################   

def test_listCaseHearingCentreAddress_test1(test_df):
    try:
        # 1. Filter: EndedGroup 4 and CaseStatus 37/38
        target_df = test_df.filter(
            (F.col("EndedGroup") == 4) & 
            (F.col("CaseStatus").isin(37, 38))
        )
        
        if target_df.count() == 0:
            return TestResult("listCaseHearingCentreAddress", "PASS", "No records found.", "ended", "test_listCaseHearingCentreAddress_test1")

        # 2. Define the Mapping Table (HearingCentre -> Address)
        # Note: Ensure the keys match exactly what is in your M3.HearingCentre column
        address_mapping = {
            "Alloa Sheriff Court": "Alloa Sheriff Court, 47 Drysdale Street, Alloa, FK10 1JA",
            "Belfast - Laganside": "Belfast Laganside Court, 45 Donegall Quay, BT1 3LL",
            "Birmingham IAC (Priory Courts)": "Birmingham Civil And Family Justice Centre, Priory Courts, 33 Bull Street, B4 6DS",
            "Birmingham IAC Sheldon Court": "Birmingham Civil And Family Justice Centre, Priory Courts, 33 Bull Street, B4 6DS",
            "Bradford": "Bradford Tribunal Hearing Centre, Rushton Avenue, BD3 7BH",
            "Bradford Crown Court": "Bradford Tribunal Hearing Centre, Rushton Avenue, BD3 7BH",
            "Bradford Magistrates Court": "Bradford and Keighley Magistrates Court and Family Court, The Tyrls, PO Box 187, BD1 1JL",
            "Coventry Magistrates' Court IAC": "Coventry Magistrates Court, Little Park Street, CV1 2SQ",
            "Field House (TH)": "Taylor House Tribunal Hearing Centre, Rosebery Avenue, EC1R 4QU",
            "Glasgow (Eagle Building)": "Atlantic Quay - Glasgow, 20 York Street, Glasgow, G2  8GT",
            "Glasgow (Tribunals Centre)": "Atlantic Quay - Glasgow, 20 York Street, Glasgow, G2  8GT",
            "Harmondsworth": "Harmondsworth Tribunal Hearing Centre, Colnbrook Bypass, UB7 0HB",
            "Harmondsworth (HX)": "Harmondsworth Tribunal Hearing Centre, Colnbrook Bypass, UB7 0HB",
            "Hatton Cross": "Hatton Cross Tribunal Hearing Centre, York House And Wellington House, 2-3 Dukes Green, Feltham, Middlesex, TW14 0LS",
            "Hendon Magistrates Court (HX)": "Hendon Magistrates Court, The Court House, The Hyde, NW9 7BY",
            "Hendon Magistrates Court (TH)": "Hendon Magistrates Court, The Court House, The Hyde, NW9 7BY",
            "Manchester (Piccadilly)": "Manchester Tribunal Hearing Centre - Piccadilly Exchange, Piccadilly Plaza, M1 4AH",
            "Newcastle CFCTC": "Newcastle Civil And Family Courts And Tribunals Centre, Barras Bridge, Newcastle-Upon-Tyne, NE1 8QF",
            "Newcastle Law Courts": "Newcastle Civil And Family Courts And Tribunals Centre, Barras Bridge, Newcastle-Upon-Tyne, NE1 8QF",
            "Newport (Columbus House)": "Newport Tribunal Centre - Columbus House, Langstone Business Park, Newport, NP18 2LX",
            "North Shields (Kings Court)": "Newcastle Civil And Family Courts And Tribunals Centre, Barras Bridge, Newcastle-Upon-Tyne, NE1 8QF",
            "North Tyneside Magistrates Court": "North Tyneside Magistrates Court, Tynemouth Road, The Court House, NE30 1AG",
            "Nottingham Justice Centre": "Nottingham Magistrates Court, Carrington Street, NG2 1EE",
            "Stockport Magistrates' Court": "Manchester Tribunal Hearing Centre - Piccadilly Exchange, Piccadilly Plaza, M1 4AH",
            "Taylor House": "Taylor House Tribunal Hearing Centre, Rosebery Avenue, EC1R 4QU",
            "Wigan and Leigh Magistrates' Court": "Manchester Tribunal Hearing Centre - Piccadilly Exchange, Piccadilly Plaza, M1 4AH",
            "Yarl's Wood": "Yarls Wood Immigration And Asylum Hearing Centre, Twinwood Road, MK44 1FD"
        }

        # 3. Create Spark Map
        spark_map = F.create_map([F.lit(x) for x in chain(*address_mapping.items())])

        # 4. Compare Actual vs Expected
        target_df = target_df.withColumn("expected_address", spark_map[F.col("HearingCentre")])

        failures = target_df.filter(
            (F.col("listCaseHearingCentreAddress") != F.col("expected_address")) |
            (F.col("expected_address").isNull())
        )

        if failures.count() > 0:
            sample = failures.select("appealReferenceNumber", "HearingCentre", "listCaseHearingCentreAddress", "expected_address").limit(5).toPandas()
            return TestResult(
                "listCaseHearingCentreAddress", 
                "FAIL", 
                f"Address mismatch in {failures.count()} records. Example: {sample.values.tolist()}", 
                "ended", 
                "test_listCaseHearingCentreAddress_test1"
            )
        
        return TestResult("listCaseHearingCentreAddress", "PASS", "listCaseHearingCentreAddress correctly mapped.", "ended", "test_listCaseHearingCentreAddress_test1")

    except Exception as e:
        return TestResult("listCaseHearingCentreAddress", "FAIL", f"EXCEPTION: {str(e)}", "ended", "test_listCaseHearingCentreAddress_test1")
    
#######################  
#sendDecisionsAndReasonsDate
#######################  
def test_sendDecisionsAndReasonsDate_test1(test_df):
    try:
        # 1. Filter: EndedGroup 4, Statuses (37, 38, 26), and Outcome (1, 2)
        target_df = test_df.filter(
            (F.col("EndedGroup") == 4) & 
            (F.col("CaseStatus").isin(37, 38, 26)) &
            (F.col("Outcome").isin(1, 2))
        )
        
        if target_df.count() == 0:
            return TestResult("sendDecisionsAndReasonsDate", "PASS", "No records found matching Group 4 status/outcome criteria.", "ended", "test_sendDecisionsAndReasonsDate_test1")

        # 2. Construct Expected ISO 8601 String from ARIA Data (DecisionDate)
        target_df = target_df.withColumn("expected_iso_date", F.date_format(F.col("DecisionDate"), "yyyy-MM-dd"))

        # 3. Validation Logic
        # - Check 1: Value matches ARIA
        # - Check 2: Format matches ISO 8601 Regex (YYYY-MM-DD)
        failures = target_df.filter(
            (F.col("sendDecisionsAndReasonsDate") != F.col("expected_iso_date")) |
            (~F.col("sendDecisionsAndReasonsDate").rlike(r"^\d{4}-\d{2}-\d{2}$"))
        )

        if failures.count() > 0:
            sample = failures.select("appealReferenceNumber", "DecisionDate", "sendDecisionsAndReasonsDate").limit(5).toPandas()
            return TestResult(
                "sendDecisionsAndReasonsDate", 
                "FAIL", 
                f"ISO 8601 mismatch or format error in {failures.count()} records. Example: {sample.values.tolist()}", 
                "ended", 
                "test_sendDecisionsAndReasonsDate_test1"
            )
        
        return TestResult("sendDecisionsAndReasonsDate", "PASS", "sendDecisionsAndReasonsDate correctly mapped to ISO 8601 format.", "ended", "test_sendDecisionsAndReasonsDate_test1")

    except Exception as e:
        return TestResult("sendDecisionsAndReasonsDate", "FAIL", f"EXCEPTION: {str(e)}", "ended", "test_sendDecisionsAndReasonsDate_test1")
#######################
# appealDate
#######################  
def test_appealDate_test1(test_df):
    try:
        # 1. Filter: EndedGroup 4, Statuses (37, 38, 26), and Outcome (1, 2)
        target_df = test_df.filter(
            (F.col("EndedGroup") == 4) & 
            (F.col("CaseStatus").isin(37, 38, 26)) &
            (F.col("Outcome").isin(1, 2))
        )
        
        if target_df.count() == 0:
            return TestResult("appealDate", "PASS", "No records found matching criteria.", "ended", "test_appealDate_test1")

        # 2. Construct Expected ISO 8601 String (YYYY-MM-DD)
        target_df = target_df.withColumn("expected_iso_date", F.date_format(F.col("DateReceived"), "yyyy-MM-dd"))

        # 3. Validation Logic
        # - Check 1: Value matches ARIA source
        # - Check 2: Format strictly follows YYYY-MM-DD
        failures = target_df.filter(
            (F.col("appealDate") != F.col("expected_iso_date")) |
            (~F.col("appealDate").rlike(r"^\d{4}-\d{2}-\d{2}$"))
        )

        if failures.count() > 0:
            sample = failures.select("appealReferenceNumber", "DateReceived", "appealDate").limit(5).toPandas()
            return TestResult(
                "appealDate", 
                "FAIL", 
                f"ISO 8601 mismatch. Examples: {sample.values.tolist()}", 
                "ended", 
                "test_appealDate_test1"
            )
        
        return TestResult("appealDate", "PASS", "appealDate correctly mapped to ISO 8601 from ARIA.", "ended", "test_appealDate_test1")

    except Exception as e:
        return TestResult("appealDate", "FAIL", f"EXCEPTION: {str(e)}", "ended", "test_appealDate_test1")
    
#######################
# appealDecision (Allowed) - Scenario 1
# Check if Outcome = 1 (Group 4, Status 26/37/38)
# Expected: Value = "Allowed"
#######################

def test_appealDecision_test1(test_df):
    """
    Scenario: Verify Outcome 1 maps to 'Allowed'.
    Criteria: EndedGroup 4, Status (26, 37, 38).
    """
    try:
        # Filter strictly for Outcome 1
        target_df = test_df.filter(
            (F.col("EndedGroup") == 4) & 
            (F.col("CaseStatus").isin(26, 37, 38)) &
            (F.col("Outcome") == 1)
        )
        
        if target_df.count() == 0:
            return TestResult("appealDecision_Allowed", "PASS", "No records found for Outcome 1.", "ended", "test_appealDecision_allowed_test1")

        # Validation logic
        failures = target_df.filter(F.col("appealDecision") != "Allowed")

        if failures.count() > 0:
            sample = failures.select("appealReferenceNumber", "appealDecision").limit(5).toPandas()
            return TestResult(
                "appealDecision_Allowed", 
                "FAIL", 
                f"Outcome 1 should be 'Allowed' but found: {sample.values.tolist()}", 
                "ended", 
                "test_appealDecision_allowed_test1"
            )
        
        return TestResult("appealDecision_Allowed", "PASS", "Outcome 1 correctly mapped to 'Allowed'.", "ended", "test_appealDecision_allowed_test1")

    except Exception as e:
        return TestResult("appealDecision_Allowed", "FAIL", f"EXCEPTION: {str(e)}", "ended", "test_appealDecision_allowed_test1")
#######################
# appealDecision (Dismissed) - Scenario 2
# Check if Outcome = 2 (Group 4, Status 26/37/38)
# Expected: Value = "Dismissed"
#######################    

def test_appealDecision_test2(test_df):
    """
    Scenario: Verify Outcome 2 maps to 'Dismissed'.
    Criteria: EndedGroup 4, Status (26, 37, 38).
    """
    try:
        # Filter strictly for Outcome 2
        target_df = test_df.filter(
            (F.col("EndedGroup") == 4) & 
            (F.col("CaseStatus").isin(26, 37, 38)) &
            (F.col("Outcome") == 2)
        )
        
        if target_df.count() == 0:
            return TestResult("appealDecision_Dismissed", "PASS", "No records found for Outcome 2.", "ended", "test_appealDecision_dismissed_test1")

        # Validation logic
        failures = target_df.filter(F.col("appealDecision") != "Dismissed")

        if failures.count() > 0:
            sample = failures.select("appealReferenceNumber", "appealDecision").limit(5).toPandas()
            return TestResult(
                "appealDecision_Dismissed", 
                "FAIL", 
                f"Outcome 2 should be 'Dismissed' but found: {sample.values.tolist()}", 
                "ended", 
                "test_appealDecision_dismissed_test1"
            )
        
        return TestResult("appealDecision_Dismissed", "PASS", "Outcome 2 correctly mapped to 'Dismissed'.", "ended", "test_appealDecision_dismissed_test1")

    except Exception as e:
        return TestResult("appealDecision_Dismissed", "FAIL", f"EXCEPTION: {str(e)}", "ended", "test_appealDecision_dismissed_test1")
    
#######################
# isDecisionAllowed (Allowed) - Scenario 1
# Updated to be case-insensitive
#######################
from pyspark.sql.functions import col, lit, lower, date_format, concat
def test_isDecisionAllowed_test1(test_df):
    try:
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus").isin(26, 37, 38)) &
            (col("Outcome") == 1)
        )
        
        if target_records.count() == 0:
            return TestResult("isDecisionAllowed", "PASS", "No Outcome 1 records found.", "ended", inspect.stack()[0].function)

        # Use lower() to ignore casing differences
        failures = target_records.filter(lower(col("isDecisionAllowed")) != "allowed")

        if failures.count() != 0:
            return TestResult("isDecisionAllowed", "FAIL", f"Found {failures.count()} Outcome 1 rows that were not 'allowed'", "ended", inspect.stack()[0].function)
        
        return TestResult("isDecisionAllowed", "PASS", "Outcome 1 correctly mapped (case-insensitive check)", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isDecisionAllowed", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)


#######################
# isDecisionAllowed (Dismissed) - Scenario 2
# Updated to be case-insensitive
#######################
def test_isDecisionAllowed_test2(test_df):
    try:
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus").isin(26, 37, 38)) &
            (col("Outcome") == 2)
        )
        
        if target_records.count() == 0:
            return TestResult("isDecisionAllowed", "PASS", "No Outcome 2 records found.", "ended", inspect.stack()[0].function)

        # Use lower() to ignore casing differences
        failures = target_records.filter(lower(col("isDecisionAllowed")) != "dismissed")

        if failures.count() != 0:
            return TestResult("isDecisionAllowed", "FAIL", f"Found {failures.count()} Outcome 2 rows that were not 'dismissed'", "ended", inspect.stack()[0].function)
        
        return TestResult("isDecisionAllowed", "PASS", "Outcome 2 correctly mapped (case-insensitive check)", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isDecisionAllowed", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


    


#######################
# attendingJudge - Scenario 1
# Check concatenation of Title, Forenames, and Surname (Group 4)
# Expected: "Title Forenames Surname" (with N/A for nulls)
#######################
def test_attendingJudge_test1(test_df):
    try:
        # Filter: EndedGroup 4 (Scenario requires largest StatusID, which is handled in Init)
        target_records = test_df.filter(col("EndedGroup") == 4)
        
        if target_records.count() == 0:
            return TestResult("attendingJudge", "PASS", "No EndedGroup 4 records found.", "ended", inspect.stack()[0].function)

        # Helper to handle 'N/A' for name parts to ensure concatenation doesn't break
        def name_part(col_name):
            return coalesce(col(col_name).cast("string"), lit("N/A"))

        # Construct the expected string: "Title Forenames Surname"
        expected_df = target_records.withColumn("expected_judge", 
            concat(
                name_part("Adj_Determination_Title"), lit(" "),
                name_part("Adj_Determination_Forenames"), lit(" "),
                name_part("Adj_Determination_Surname")
            )
        )

        failures = expected_df.filter(col("attendingJudge") != col("expected_judge"))

        if failures.count() != 0:
            sample = failures.select("attendingJudge", "expected_judge").limit(1).collect()
            return TestResult("attendingJudge", "FAIL", f"Found {failures.count()} rows with name mismatch. Example: Actual '{sample[0][0]}' vs Expected '{sample[0][1]}'", "ended", inspect.stack()[0].function)
        
        return TestResult("attendingJudge", "PASS", "attendingJudge correctly concatenated from Title, Forenames, and Surname", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("attendingJudge", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


#######################
# actualCaseHearingLength - Scenario 1
# Check conversion of total minutes to Hours and Minutes struct
# Criteria: EndedGroup 4, Status (26, 37, 38), Outcome (1, 2)
#######################
def test_actualCaseHearingLength_test1(test_df):
    try:
        # 1. Filter: EndedGroup 4, valid statuses and outcomes
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus").isin(26, 37, 38)) &
            (col("Outcome").isin(1, 2))
        )
        
        if target_records.count() == 0:
            return TestResult("actualCaseHearingLength", "PASS", "No records found matching criteria.", "ended", inspect.stack()[0].function)

        # 2. Define Expected Logic (Spark equivalent of the pandas logic provided)
        # hours = HearingDuration // 60
        # minutes = HearingDuration % 60
        expected_df = target_records.withColumn("exp_hours", (col("HearingDuration") / 60).cast("int")) \
                                   .withColumn("exp_minutes", (col("HearingDuration") % 60).cast("int"))

        # 3. Validation Logic
        # Checking the nested JSON fields against our calculated expected values
        failures = expected_df.filter(
            (col("actualCaseHearingLength.hours") != col("exp_hours")) | 
            (col("actualCaseHearingLength.minutes") != col("exp_minutes"))
        )

        if failures.count() != 0:
            sample = failures.select(
                "appealReferenceNumber", 
                "HearingDuration", 
                "actualCaseHearingLength.hours", 
                "actualCaseHearingLength.minutes",
                "exp_hours",
                "exp_minutes"
            ).limit(1).collect()
            
            return TestResult(
                "actualCaseHearingLength", 
                "FAIL", 
                f"Mismatch for Duration {sample[0][1]}. Actual: {sample[0][2]}h:{sample[0][3]}m | Expected: {sample[0][4]}h:{sample[0][5]}m", 
                "ended", 
                inspect.stack()[0].function
            )
        
        return TestResult("actualCaseHearingLength", "PASS", "Minutes correctly converted to hours and minutes struct.", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("actualCaseHearingLength", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


############################################################################################
#######################
#language tests Init code
#######################
def test_languages_init(json_data, M1_bronze, M3_bronze):
    try:
        # 1. First, generate the EndedGroup column using your helper function
        m3_with_groups = get_ended_group_id(M3_bronze)

        # 2. Now that EndedGroup exists, define the window and filter
        window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())
        
        m3_latest = m3_with_groups.withColumn("rn", row_number().over(window_spec)) \
            .filter(
                (col("rn") == 1) & 
                (col("EndedGroup").isin(3, 4)) & 
                (col("CaseStatus").isin(40, 52)) # Row selection 40, 52
            ) \
            .select("CaseNo", "CaseStatus", "EndedGroup")

        # 3. Join with M1 for LanguageId
        m1_data = M1_bronze.select(col("CaseNo").alias("M1_CaseNo"), col("LanguageId"))

        # 4. Final Join
        test_df = json_data.join(
            m3_latest, 
            json_data.appealReferenceNumber == m3_latest.CaseNo, 
            "inner"
        ).join(
            m1_data, 
            json_data.appealReferenceNumber == m1_data.M1_CaseNo, 
            "left"
        ).select(
            "appealReferenceNumber",
            "appellantInterpreterLanguageCategory",
            "appellantInterpreterSpokenLanguage",
            # "appellantInterpreterSignLanguage",
            "LanguageId",
            "EndedGroup"
        )

        return test_df, True

    except Exception as e:
        return None, TestResult("Language_Init", "FAIL", f"Init Error: {str(e)}", "ended", "init")
    
def test_languageInterpreterMapping(test_df):
    language_requirements = {
    1: ("spokenLanguageInterpreter", "fra", "French", [], None),
    2: ("spokenLanguageInterpreter", "deu", "German", [], None),
    3: ("spokenLanguageInterpreter", "ach", "Acholi", [], None),
    4: ("spokenLanguageInterpreter", "aka", "Akan", [], None),
    5: ("spokenLanguageInterpreter", "afr", "Afrikaans", [], None),
    6: ("spokenLanguageInterpreter", "sqi", "Albanian", [], None),
    7: ("spokenLanguageInterpreter", "amh", "Amharic", [], None),
    8: ("spokenLanguageInterpreter", None, None, ["Yes"], "Bajuni"),
    9: ("spokenLanguageInterpreter", "ara", "Arabic", [], None),
    10: ("spokenLanguageInterpreter", "ara-ana", "Arabic North African", [], None),
    11: ("spokenLanguageInterpreter", "ara-ame", "Arabic Middle Eastern", [], None),
    12: ("spokenLanguageInterpreter", None, None, ["Yes"], "Ashanti"),
    13: ("spokenLanguageInterpreter", "aii", "Assyrian", [], None),
    14: ("spokenLanguageInterpreter", "teo", "Ateso", [], None),
    15: ("spokenLanguageInterpreter", "bjs", "Bajan (West Indian)", [], None),
    16: ("spokenLanguageInterpreter", "bal", "Baluchi", [], None),
    17: ("spokenLanguageInterpreter", "bam", "Bambara", [], None),
    18: ("spokenLanguageInterpreter", "bel", "Belorussian", [], None),
    19: ("spokenLanguageInterpreter", "ben", "Bengali", [], None),
    20: ("spokenLanguageInterpreter", "bin", "Benin/Edo", [], None),
    21: ("spokenLanguageInterpreter", "ber", "Berber", [], None),
    22: ("spokenLanguageInterpreter", "abr", "Brong", [], None),
    23: ("spokenLanguageInterpreter", "bul", "Bulgarian", [], None),
    24: ("spokenLanguageInterpreter", "yue", "Cantonese", [], None),
    25: ("spokenLanguageInterpreter", "ceb", "Cebuano", [], None),
    26: ("spokenLanguageInterpreter", "ces", "Czech", [], None),
    27: ("spokenLanguageInterpreter", "prs", "Dari", [], None),
    28: ("spokenLanguageInterpreter", "din", "Dinka", [], None),
    29: ("spokenLanguageInterpreter", "dyu", "Dioula", [], None),
    30: ("spokenLanguageInterpreter", "bin", "Benin/Edo", [], None),
    31: ("spokenLanguageInterpreter", "ewe", "Ewe", [], None),
    32: ("spokenLanguageInterpreter", "fat", "Fanti", [], None),
    33: ("spokenLanguageInterpreter", "fas", "Farsi", [], None),
    34: ("spokenLanguageInterpreter", "fra-faf", "French African", [], None),
    35: ("spokenLanguageInterpreter", "fra-far", "French Arabic", [], None),
    36: ("spokenLanguageInterpreter", "gaa", "Ga", [], None),
    37: ("spokenLanguageInterpreter", "ell", "Greek", [], None),
    38: ("spokenLanguageInterpreter", "guj", "Gujarati", [], None),
    39: ("spokenLanguageInterpreter", "sgw", "Gurage", [], None),
    40: ("spokenLanguageInterpreter", "hak", "Hakka", [], None),
    41: ("spokenLanguageInterpreter", "hau", "Hausa", [], None),
    42: ("spokenLanguageInterpreter", "heb", "Hebrew", [], None),
    43: ("spokenLanguageInterpreter", "hin", "Hindi", [], None),
    44: ("spokenLanguageInterpreter", "hnd", "Hindko", [], None),
    45: ("spokenLanguageInterpreter", "ibo", "Igbo (Also Known As Ibo)", [], None),
    46: ("spokenLanguageInterpreter", "ilo", "Ilocano", [], None),
    47: ("spokenLanguageInterpreter", None, None, ["Yes"], "Ishan"),
    48: ("spokenLanguageInterpreter", "ita", "Italian", [], None),
    49: ("spokenLanguageInterpreter", "jam", "Jamaican", [], None),
    50: ("spokenLanguageInterpreter", "jpn", "Japanese", [], None),
    51: ("spokenLanguageInterpreter", None, None, ["Yes"], "Karaninka"),
    52: ("spokenLanguageInterpreter", "kas", "Kashmiri", [], None),
    53: ("spokenLanguageInterpreter", "kck", "Khalanga", [], None),
    54: ("spokenLanguageInterpreter", "kon", "Kikongo", [], None),
    55: ("spokenLanguageInterpreter", "kik", "Kikuyu", [], None),
    56: ("spokenLanguageInterpreter", "kin", "Kinyarwandan", [], None),
    57: ("spokenLanguageInterpreter", None, None, ["Yes"], "Kisakata"),
    58: ("spokenLanguageInterpreter", "knn", "Konkani", [], None),
    59: ("spokenLanguageInterpreter", "kri", "Krio (Sierra Leone)", [], None),
    60: ("spokenLanguageInterpreter", "kru", "Kru", [], None),
    61: ("spokenLanguageInterpreter", "kur-kkr", "Kurdish kurmanji", [], None),
    62: ("spokenLanguageInterpreter", "kur-ksr", "Kurdish Sorani", [], None),
    63: ("spokenLanguageInterpreter", "kfr", "Kutchi", [], None),
    64: ("spokenLanguageInterpreter", "laj", "Lango", [], None),
    65: ("spokenLanguageInterpreter", "lin", "Lingala", [], None),
    66: ("spokenLanguageInterpreter", "lit", "Lithuanian", [], None),
    67: ("spokenLanguageInterpreter", "lug", "Lugandan", [], None),
    68: ("spokenLanguageInterpreter", "luo", "Luo", [], None),
    69: ("spokenLanguageInterpreter", None, None, ["Yes"], "Lunyankole"),
    70: ("spokenLanguageInterpreter", None, None, ["Yes"], "Lutoro"),
    71: ("spokenLanguageInterpreter", "mku", "Malinke", [], None),
    72: ("spokenLanguageInterpreter", "cmn", "Mandarin", [], None),
    73: ("spokenLanguageInterpreter", "mnk", "Mandinka", [], None),
    74: ("spokenLanguageInterpreter", "mar", "Marathi", [], None),
    75: ("spokenLanguageInterpreter", "men", "Mende", [], None),
    76: ("spokenLanguageInterpreter", None, None, ["Yes"], "Mirpuri"),
    77: ("spokenLanguageInterpreter", "ron-fmo", "Moldovan", [], None),
    78: ("spokenLanguageInterpreter", "mon", "Mongolian", [], None),
    79: ("spokenLanguageInterpreter", "nde", "Ndebele", [], None),
    80: ("spokenLanguageInterpreter", "nep", "Nepali", [], None),
    81: ("spokenLanguageInterpreter", None, None, ["Yes"], "Ngwa"),
    82: ("spokenLanguageInterpreter", "nzi", "Nzima", [], None),
    83: ("spokenLanguageInterpreter", "orm", "Oromo", [], None),
    84: ("spokenLanguageInterpreter", "pat", "Patois", [], None),
    85: ("spokenLanguageInterpreter", None, None, ["Yes"], "Pidgin English"),
    86: ("spokenLanguageInterpreter", "pol", "Polish", [], None),
    87: ("spokenLanguageInterpreter", "por", "Portuguese", [], None),
    88: ("spokenLanguageInterpreter", "pan-pji", "Punjabi Indian", [], None),
    89: ("spokenLanguageInterpreter", "pus", "Pushtu (Also Known As Pashto)", [], None),
    90: ("spokenLanguageInterpreter", "ron", "Romanian", [], None),
    91: ("spokenLanguageInterpreter", "rus", "Russian", [], None),
    92: ("spokenLanguageInterpreter", "krn", "Sarpo", [], None),
    93: ("spokenLanguageInterpreter", "hbs", "Serbo-Croatian", [], None),
    94: ("spokenLanguageInterpreter", "sna", "Shona", [], None),
    95: ("spokenLanguageInterpreter", "snd", "Sindhi", [], None),
    96: ("spokenLanguageInterpreter", "sin", "Sinhalese", [], None),
    97: ("spokenLanguageInterpreter", "slk", "Slovak", [], None),
    98: ("spokenLanguageInterpreter", "som", "Somali", [], None),
    99: ("spokenLanguageInterpreter", "spa", "Spanish", [], None),
    100: ("spokenLanguageInterpreter", "sus", "Susu", [], None),
    101: ("spokenLanguageInterpreter", "swa", "Swahili", [], None),
    102: ("spokenLanguageInterpreter", "syl", "Sylheti", [], None),
    103: ("spokenLanguageInterpreter", "tgl", "Tagalog", [], None),
    104: ("spokenLanguageInterpreter", "tai", "Taiwanese", [], None),
    105: ("spokenLanguageInterpreter", "tam", "Tamil", [], None),
    106: ("spokenLanguageInterpreter", "tem", "Temne", [], None),
    107: ("spokenLanguageInterpreter", "tha", "Thai", [], None),
    108: ("spokenLanguageInterpreter", "tig", "Tigre", [], None),
    109: ("spokenLanguageInterpreter", "tir", "Tigrinya", [], None),
    110: ("spokenLanguageInterpreter", "tur", "Turkish", [], None),
    111: ("spokenLanguageInterpreter", "twi", "Twi", [], None),
    112: ("spokenLanguageInterpreter", "ukr", "Ukrainian", [], None),
    113: ("spokenLanguageInterpreter", "urd", "Urdu", [], None),
    114: ("spokenLanguageInterpreter", "urh", "Urohobo", [], None),
    115: ("spokenLanguageInterpreter", "vie", "Vietnamese", [], None),
    116: ("spokenLanguageInterpreter", "wol", "Wolof", [], None),
    117: ("spokenLanguageInterpreter", "xho", "Xhosa", [], None),
    118: ("spokenLanguageInterpreter", "yor", "Yoruba", [], None),
    119: ("spokenLanguageInterpreter", "zul", "Zulu", [], None),
    120: ("spokenLanguageInterpreter", "hye", "Armenian", [], None),
    121: ("spokenLanguageInterpreter", "swa-sbv", "Swahili Bravanese", [], None),
    122: ("spokenLanguageInterpreter", "zho-hok", "Hokkein", [], None),
    123: ("spokenLanguageInterpreter", "cpf", "Creole (French)", [], None),
    124: ("spokenLanguageInterpreter", "efi", "Efik", [], None),
    125: ("spokenLanguageInterpreter", "ibb", "Ibibio", [], None),
    126: ("spokenLanguageInterpreter", "est", "Estonian", [], None),
    127: ("spokenLanguageInterpreter", "kur-fey", "Feyli", [], None),
    128: ("spokenLanguageInterpreter", "ind", "Indonesian", [], None),
    129: ("spokenLanguageInterpreter", "jav", "Javanese", [], None),
    130: ("spokenLanguageInterpreter", "kor", "Korean", [], None),
    131: ("signLanguageInterpreter", "sign-lps", "Lipspeaker", [], None),
    132: ("spokenLanguageInterpreter", "mkd", "Macedonian", [], None),
    133: ("spokenLanguageInterpreter", "fij", "Fijian", [], None),
    134: ("spokenLanguageInterpreter", "bfz", "Pahari", [], None),
    135: ("spokenLanguageInterpreter", None, None, ["Yes"], "Hendko"),
    136: ("signLanguageInterpreter", "bfi", "British Sign Language (BSL)", [], None),
    137: ("spokenLanguageInterpreter", "bnt-kic", "Kichagga", [], None),
    138: ("spokenLanguageInterpreter", "pag", "Pangasinan", [], None),
    139: ("spokenLanguageInterpreter", "ful", "Fula", [], None),
    140: ("spokenLanguageInterpreter", None, None, ["Yes"], "Sarahuleh"),
    141: ("spokenLanguageInterpreter", None, None, ["Yes"], "Putonghue"),
    143: ("spokenLanguageInterpreter", "wol", "Wolof", [], None),
    144: ("spokenLanguageInterpreter", "tel", "Telugu", [], None),
    145: ("spokenLanguageInterpreter", "crp", "Creole (Spanish)", [], None),
    146: ("spokenLanguageInterpreter", "cpp", "Creole (Portuguese)", [], None),
    147: ("spokenLanguageInterpreter", "pan-pjp", "Punjabi Pakistani", [], None),
    148: ("signLanguageInterpreter", "sign-sse", "Speech Supported English (SSE)", [], None),
    149: ("signLanguageInterpreter", None, None, ["Yes"], "Sign Language (Others)"),
    150: ("spokenLanguageInterpreter", "arq", "Algerian", [], None),
    151: ("spokenLanguageInterpreter", "mya", "Burmese", [], None),
    152: ("spokenLanguageInterpreter", "hun", "Hungarian", [], None),
    153: ("spokenLanguageInterpreter", "xog", "Lusoga", [], None),
    154: ("spokenLanguageInterpreter", "msa", "Malay", [], None),
    155: ("spokenLanguageInterpreter", "mal", "Malayalam", [], None),
    156: ("spokenLanguageInterpreter", None, None, ["Yes"], "NavsarispokenLanguageInterpreter"),
    157: ("spokenLanguageInterpreter", "pam", "Pampangan", [], None),
    158: ("spokenLanguageInterpreter", "rom", "Romany", [], None),
    159: ("spokenLanguageInterpreter", "swe", "Swedish", [], None),
    160: ("spokenLanguageInterpreter", "don", "Toura", [], None),
    161: ("spokenLanguageInterpreter", "cym", "Welsh", [], None),
    162: ("spokenLanguageInterpreter", None, None, ["Yes"], "Senegal (French) Olof Dialect"),
    163: ("spokenLanguageInterpreter", "swa-skb", "Swahili Kibajuni", [], None),
    164: ("spokenLanguageInterpreter", "swh", "Kiswahili", [], None),
    165: ("spokenLanguageInterpreter", None, None, ["Yes"], "Banjuni"),
    166: ("spokenLanguageInterpreter", "vsa", "Visayan", [], None),
    167: ("spokenLanguageInterpreter", "rmm", "Roma", [], None),
    168: ("spokenLanguageInterpreter", "lav", "Latvian", [], None),
    169: ("spokenLanguageInterpreter", "kat", "Georgian", [], None),
    170: ("spokenLanguageInterpreter", "ben-bsy", "Bengali Sylheti", [], None),
    171: ("spokenLanguageInterpreter", "pan", "Punjabi", [], None),
    172: ("spokenLanguageInterpreter", None, None, ["Yes"], "Lugisa"),
    173: ("spokenLanguageInterpreter", "cgg", "Rukiga", [], None),
    174: ("spokenLanguageInterpreter", "luo-lky", "Luo Kenyan", [], None),
    175: ("spokenLanguageInterpreter", "luo-llg", "Luo Lango", [], None),
    176: ("spokenLanguageInterpreter", "luo-lah", "Luo Acholi", [], None),
    177: ("spokenLanguageInterpreter", "aze", "Azerbajani (aka Nth Azari)", [], None),
    178: ("spokenLanguageInterpreter", "ctg", "Chittagonain", [], None),
    179: ("spokenLanguageInterpreter", None, None, ["Yes"], "Cambellpuri"),
    180: ("spokenLanguageInterpreter", "kur-kbr", "Kurdish kurmanji", [], None),
    181: ("spokenLanguageInterpreter", "gjk", "Kachi", [], None),
    182: ("spokenLanguageInterpreter", None, None, ["Yes"], "Bharuchi"),
    183: ("spokenLanguageInterpreter", None, None, ["Yes"], "Emakhuna"),
    184: ("spokenLanguageInterpreter", "glg", "Galician", [], None),
    185: ("spokenLanguageInterpreter", "cpe", "Creole (English)", [], None),
    186: ("spokenLanguageInterpreter", None, None, ["Yes"], "Azari"),
    187: ("spokenLanguageInterpreter", "nyo", "Runyoro", [], None),
    188: ("spokenLanguageInterpreter", None, None, ["Yes"], "Guran"),
    189: ("spokenLanguageInterpreter", "ara-mag", "Maghreb", [], None),
    190: ("spokenLanguageInterpreter", None, None, ["Yes"], "Training"),
    191: ("spokenLanguageInterpreter", "nor", "Norwegian", [], None),
    192: ("spokenLanguageInterpreter", "ttj", "Rutoro", [], None),
    193: ("spokenLanguageInterpreter", None, None, ["Yes"], "Kurundi"),
    194: ("spokenLanguageInterpreter", "nld-nfl", "Flemish", [], None),
    195: ("spokenLanguageInterpreter", "uzb", "Uzbek", [], None),
    196: ("spokenLanguageInterpreter", "btn", "Bhutanese", [], None),
    197: ("spokenLanguageInterpreter", "nya", "Chichewa", [], None),
    198: ("spokenLanguageInterpreter", "run", "Kirundi", [], None),
    199: ("spokenLanguageInterpreter", "bem", "Benba (Bemba)", [], None),
    200: ("spokenLanguageInterpreter", "swa-skb", "Swahili Kibajuni", [], None),
    201: ("spokenLanguageInterpreter", "min", "Mina", [], None),
    202: ("spokenLanguageInterpreter", "khm", "Khmer", [], None),
    203: ("spokenLanguageInterpreter", "bih", "Bihari", [], None),
    204: ("spokenLanguageInterpreter", "dua", "Douala", [], None),
    205: ("spokenLanguageInterpreter", "ewo", "Ewondo", [], None),
    206: ("spokenLanguageInterpreter", "bas", "Bassa", [], None),
    207: ("spokenLanguageInterpreter", "bod", "Tibetan", [], None),
    208: ("spokenLanguageInterpreter", "scl", "Shina", [], None),
    209: ("spokenLanguageInterpreter", None, None, ["Yes"], "Pothohari"),
    210: ("spokenLanguageInterpreter", "slv", "Slovenian", [], None),
    211: ("spokenLanguageInterpreter", "hac", "Gorani", [], None),
    212: ("spokenLanguageInterpreter", "lub", "Luba (Tshiluba)", [], None),
    213: ("spokenLanguageInterpreter", "kur-kbr", "Kurdish kurmanji", [], None),
    214: ("spokenLanguageInterpreter", "tuk", "Turkmen", [], None),
    215: ("spokenLanguageInterpreter", "kir", "Kyrgyz", [], None),
    216: ("spokenLanguageInterpreter", "mkw", "Monokutuba", [], None),
    217: ("spokenLanguageInterpreter", "byn", "Bilin", [], None),
    218: ("spokenLanguageInterpreter", "tsn", "Setswana", [], None),
    219: ("spokenLanguageInterpreter", "bas", "Bassa", [], None),
    220: ("spokenLanguageInterpreter", "uig", "Uighur", [], None),
    221: ("spokenLanguageInterpreter", None, None, ["Yes"], "Pathwari"),
    222: ("spokenLanguageInterpreter", None, None, ["Yes"], "Fur (Sudanese)"),
    223: ("spokenLanguageInterpreter", "nld", "Dutch", [], None),
    224: ("spokenLanguageInterpreter", None, None, ["Yes"], "Kosovan"),
    225: ("spokenLanguageInterpreter", None, None, ["Yes"], "Afreerhamar"),
    226: ("spokenLanguageInterpreter", "che", "Chechen", [], None),
    227: ("spokenLanguageInterpreter", None, None, ["Yes"], "Khymer Khymer"),
    228: ("spokenLanguageInterpreter", "zza", "Zaza", [], None),
    229: ("spokenLanguageInterpreter", "dan", "Danish", [], None),
    230: ("spokenLanguageInterpreter", "zag", "Zaghawa", [], None),
    231: ("spokenLanguageInterpreter", "div", "Maldivian", [], None),
    232: ("signLanguageInterpreter", "sign-pst", "Palantypist / Speech to text", [], None),
    233: ("signLanguageInterpreter", "sign-dfr", "Deaf Relay", [], None),
    234: ("signLanguageInterpreter", "ase", "American Sign Language (ASL)", [], None),
    235: ("signLanguageInterpreter", "sign-hos", "Hands on signing", [], None),
    236: ("signLanguageInterpreter", "sign-lps", "Lipspeaker", [], None),
    237: ("signLanguageInterpreter", "sign-mkn", "Makaton", [], None),
    238: ("signLanguageInterpreter", "sign-dma", "Deafblind manual alphabet", [], None),
    239: ("signLanguageInterpreter", "sign-ntr", "Notetaker", [], None),
    240: ("signLanguageInterpreter", "sign-vfs", "Visual frame signing", [], None),
    241: ("signLanguageInterpreter", "ils", "International Sign (IS)", [], None),
    242: ("spokenLanguageInterpreter", "iso", "Isoko", [], None),
    243: ("spokenLanguageInterpreter", "her", "Herero", [], None),
    244: ("spokenLanguageInterpreter", "mlt", "Maltese", [], None),
    245: ("spokenLanguageInterpreter", "skr", "Saraiki (Seraiki)", [], None),
    246: ("spokenLanguageInterpreter", None, None, ["Yes"], "Kalabari"),
    247: ("spokenLanguageInterpreter", None, None, ["Yes"], "Kinyamulenge"),
    249: ("spokenLanguageInterpreter", None, None, ["Yes"], "Wobe"),
    250: ("spokenLanguageInterpreter", None, None, ["Yes"], "Mauritian"),
    251: ("spokenLanguageInterpreter", "mnk", "Mandinka", [], None),
    252: ("spokenLanguageInterpreter", None, None, ["Yes"], "Gaelic"),
    253: ("spokenLanguageInterpreter", None, None, ["Yes"], "Bosnian"),
    254: ("spokenLanguageInterpreter", None, None, ["Yes"], "Filipino"),
    255: ("spokenLanguageInterpreter", None, None, ["Yes"], "Mauritian Creole"),
    256: ("spokenLanguageInterpreter", None, None, ["Yes"], "Yiddish"),
    257: ("spokenLanguageInterpreter", None, None, ["Yes"], "Pular"),
    258: ("spokenLanguageInterpreter", None, None, ["Yes"], "Runyankole"),
    259: ("spokenLanguageInterpreter", None, None, ["Yes"], "Gurung"),
    260: ("spokenLanguageInterpreter", None, None, ["Yes"], "Karen"),
    261: ("spokenLanguageInterpreter", "tam", "Tamil", [], None),
    262: ("spokenLanguageInterpreter", None, None, ["Yes"], "Rotana"),
    263: ("spokenLanguageInterpreter", None, None, ["Yes"], "Spanish Hispanic"),
    264: ("spokenLanguageInterpreter", None, None, ["Yes"], "Spanish Latin"),
    265: ("spokenLanguageInterpreter", None, None, ["Yes"], "Tetum")
    }
    
    
    try:
        results_list = []
        rows = test_df.collect()

        for row in rows:
            case_no = row['appealReferenceNumber']
            lang_id = row['LanguageId']
            actual_categories = row['appellantInterpreterLanguageCategory'] or []

            # Scenario: ID 0 (No Interpreter)
            if lang_id is None or lang_id == 0:
                if len(actual_categories) > 0:
                    results_list.append(f"FAIL - {case_no}: ID 0/Null but Category list is not empty: {actual_categories}")
                continue

            req = language_requirements.get(lang_id)
            if not req:
                results_list.append(f"FAIL - {case_no}: ID {lang_id} not found in mapping doc")
                continue
            
            req_category, req_code, req_label, req_manual, req_desc = req
            
            # 1. Validate Category List
            # Check if the required category (spoken or sign) is present in the JSON list
            if req_category not in actual_categories:
                results_list.append(f"FAIL - {case_no}: Missing category '{req_category}' in {actual_categories}")

            # 2. Validate Language Data
            field_name = "appellantInterpreterSpokenLanguage" if req_category == 'spokenLanguageInterpreter' else "appellantInterpreterSignLanguage"
            target_data = row[field_name]

            if target_data is None:
                results_list.append(f"FAIL - {case_no}: {field_name} is null for ID {lang_id}")
                continue

            # Unpack Spark Row/Struct to Dict
            d = target_data.asDict(recursive=True)
            actual_ref = d.get('languageRefData') or {}
            actual_val = actual_ref.get('value') or {}
            
            # Handle Manual Entry vs RefData
            errors = []
            if req_code is None: # It's a manual entry requirement
                actual_manual = d.get('languageManualEntry') or []
                actual_desc = d.get('languageManualEntryDescription')
                if req_desc and req_desc not in (actual_desc or ""):
                    errors.append(f"ManualDesc: Expected '{req_desc}', Found '{actual_desc}'")
            else:
                # Standard RefData Comparison
                if actual_val.get('code') != req_code:
                    errors.append(f"Code: Expected '{req_code}', Found '{actual_val.get('code')}'")
                if actual_val.get('label') != req_label:
                    errors.append(f"Label: Expected '{req_label}', Found '{actual_val.get('label')}'")

            if errors:
                results_list.append(f"FAIL - {case_no} (ID {lang_id}): " + " | ".join(errors))

        # Result Reporting
        if results_list:
            message = f"Found {len(results_list)} failures: " + " || ".join(results_list)
            return TestResult("InterpreterMapping", "FAIL", message, "ended", "test")
        
        return TestResult("InterpreterMapping", "PASS", "All rows mapped correctly", "ended", "test")

    except Exception as e:
        return TestResult("InterpreterMapping", "FAIL", f"Crash: {str(e)}", "ended", "test")
############################################################################################
#######################
#additionalInstructionsTribunalResponse Init code
#######################
def test_additionalInstructionsTribunalResponse_init(json, M3_silver, M6_bronze):
    try:
        window_spec = Window.partitionBy("CaseNo")

        filtered_df = M3_silver.filter(
            (col("CaseStatus").isin(37, 38)) | 
            ((col("CaseStatus") == 26) & (col("Outcome") == 0))
        )

        df_with_max = filtered_df.withColumn("max_status_id", spark_max("StatusId").over(window_spec))
        final_m3_df = df_with_max.filter(col("StatusId") == col("max_status_id")).drop("max_status_id")

        json = json.select(
            col("appealReferenceNumber"),
            col("additionalInstructionsTribunalResponse")
        )

        M3_silver = final_m3_df.select(
            col("CaseNo"),
            col("CaseStatus"),
            col("HearingCentre"), 
            col("HearingDate"), 
            col("HearingType"), col("CourtName"), 
            col("ListType"), col("StartTime"), col("Judge1FT_Surname"), col("Judge1FT_Forenames"), 
            col("Judge1FT_Title"), col("Judge2FT_Surname"), col("Judge2FT_Forenames"), 
            col("Judge2FT_Title"), col("Judge3FT_Surname"), col("Judge3FT_Forenames"), 
            col("Judge3FT_Title"), col("CourtClerk_Surname"), col("CourtClerk_Forenames"), 
            col("CourtClerk_Title"), col("TimeEstimate"), col("Notes")
        )

        M6_bronze = M6_bronze.select(
            col("CaseNo"),
            col("Judge_Forenames"), col("Judge_Surname"), col("Judge_Title"), col("Required")
        )

        test_df = json.join(
            M3_silver, 
            json["appealReferenceNumber"] == M3_silver["CaseNo"], 
            "inner"
        ).join(
            M6_bronze, 
            json["appealReferenceNumber"] == M6_bronze["CaseNo"], 
            "left" # <--- This was 'inner', change it to 'left'
        ).filter(col("additionalInstructionsTribunalResponse").isNotNull()).drop("CaseNo")

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("additionalInstructionsTribunalResponse", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state,inspect.stack()[0].function)
    

def test_additionalInstructionsTribunalResponse(test_df):
    try:
        import re
        from pyspark.sql.functions import first, collect_list, struct

        def normalize(text):
            if not text: return ""
            t = " ".join(text.split())
            t = re.sub(r'\s*\(\s*', ' (', t) 
            t = re.sub(r'\s*\)\s*', ')', t)   
            return t.strip()

        # Group by Case to handle multiple M6 officers
        grouped_df = test_df.groupBy("appealReferenceNumber").agg(
            first("additionalInstructionsTribunalResponse").alias("actual"),
            first("HearingCentre").alias("HC"),
            first("HearingDate").alias("HD"),
            first("HearingType").alias("HT"),
            first("CourtName").alias("CN"),
            first("ListType").alias("LT"),
            first("TimeEstimate").alias("TE"),
            collect_list(struct("Judge_Surname", "Judge_Forenames", "Judge_Title", "Required")).alias("M6_Officers")
        ).collect()

        results_list = []

        for row in grouped_df:
            actual_norm = normalize(row['actual'] or "")
            case_no = row['appealReferenceNumber']
            errors = []

            # 1. Corrected Date Formatting
            if row.HD:
                raw_date = str(row.HD).replace('T', ' ')
                aria_date = re.split(r'[\.\+]', raw_date)[0]
            else:
                aria_date = "N/A"

            # 2. Expected field mapping
            expected_fields = {
                "Hearing Centre": str(row.HC or "N/A"),
                "Hearing Date": aria_date, # This will now be '2025-08-19 00:00:00'
                "Hearing Type": str(row.HT or "N/A"),
                "Court": str(row.CN or "N/A"),
                "List Type": str(row.LT or "N/A"),
                "Estimated Duration": str(row.TE or "N/A")
            }

            for key, val in expected_fields.items():
                snippet = normalize(f"{key}: {val}")
                if snippet not in actual_norm:
                    errors.append(f"{key} Mismatch | Expected: '{snippet}'")

            # 3. Handle M6 Officers (Only check if data exists)
            for officer in row['M6_Officers']:
                if officer.Judge_Surname is not None:
                    req_status = "Required" if officer.Required == 1 else "Not Required"
                    snippet = normalize(f"{officer.Judge_Surname} {officer.Judge_Forenames} ({officer.Judge_Title}) : {req_status}")
                    
                    if snippet not in actual_norm:
                        errors.append(f"Judicial Officer Mismatch | Expected: '{snippet}'")

            if errors:
                results_list.append(f"FAIL - {case_no}: " + " | ".join(errors))

        if results_list:
            return TestResult("additionalInstructionsTribunalResponse", "FAIL", f"Found {len(results_list)} failures: " + "|||".join(results_list), "ended", "test_additionalInstructionsTribunalResponse")
        
        return TestResult("additionalInstructionsTribunalResponse", "PASS", "All instructions match ARIA source.", "ended", "test_additionalInstructionsTribunalResponse")

    except Exception as e:
        return TestResult("additionalInstructionsTribunalResponse", "FAIL", f"Crash: {str(e)}", "ended", "test_additionalInstructionsTribunalResponse")
    

#######################
# ftpaApplicationDeadline - Scenario 1 & 2
# Check DecisionDate + 14 or 28 days based on CategoryId
# Criteria: EndedGroup 4, Status (26, 37, 38), Outcome (1, 2)
#######################
def test_ftpaApplicationDeadline_test1(test_df):
    try:
        # 1. Filter: EndedGroup 4, valid statuses and outcomes
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus").isin(26, 37, 38)) &
            (col("Outcome").isin(1, 2))
        )
        
        if target_records.count() == 0:
            return TestResult("ftpaApplicationDeadline", "PASS", "No records found matching criteria.", "ended", inspect.stack()[0].function)

        # 2. Define Expected Logic
        # Category 37 -> +14 days, Category 38 -> +28 days
        # date_format ensures ISO 8601 (YYYY-MM-DD)
        expected_df = target_records.withColumn("exp_deadline", 
            when(col("CategoryId") == 37, date_add(col("DecisionDate"), 14))
            .when(col("CategoryId") == 38, date_add(col("DecisionDate"), 28))
            .otherwise(lit(None))
        ).withColumn("exp_deadline_iso", date_format(col("exp_deadline"), "yyyy-MM-dd"))

        # 3. Validation Logic
        # Note: If DecisionDate is null in source, deadline should be null in JSON
        failures = expected_df.filter(
            (col("ftpaApplicationDeadline").isNotNull()) & 
            (col("ftpaApplicationDeadline") != col("exp_deadline_iso"))
        )

        if failures.count() != 0:
            sample = failures.select(
                "appealReferenceNumber", 
                "CategoryId", 
                "DecisionDate", 
                "ftpaApplicationDeadline", 
                "exp_deadline_iso"
            ).limit(1).collect()
            
            return TestResult(
                "ftpaApplicationDeadline", 
                "FAIL", 
                f"Date Mismatch for Case {sample[0][0]} (Cat {sample[0][1]}). ARIA DecisionDate: {sample[0][2]} | JSON Actual: '{sample[0][3]}' | Expected: '{sample[0][4]}'", 
                "ended", 
                inspect.stack()[0].function
            )
        
        return TestResult("ftpaApplicationDeadline", "PASS", "FTPA Deadline correctly calculated (+14/+28 days) in ISO format.", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaApplicationDeadline", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    

#######################
# ftpaList - Scenario 1 & 2
# Check FTPA Applicant mapping based on M3 Party (1=Appellant, 2=Respondent)
# Criteria: EndedGroup 4, CaseStatus 39
#######################
def test_ftpaList_test1(test_df):
    try:
        # 1. Filter for Status 39 and EndedGroup 4
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("ftpaList", "PASS", "No CaseStatus 39 records found.", "ended", inspect.stack()[0].function)

        # 2. Define Expected Value Logic
        # Party 1 -> appellant, Party 2 -> respondent
        expected_df = target_records.withColumn("expected_applicant", 
            when(col("Party") == 1, lit("appellant"))
            .when(col("Party") == 2, lit("respondent"))
            .otherwise(lit(None))
        )

        # 3. Validation Logic
        # We check the first item in the ftpaList array (index 0)
        failures = expected_df.filter(
            (col("ftpaList").getItem(0)["value"]["ftpaApplicant"] != col("expected_applicant")) |
            (col("ftpaList").getItem(0)["id"] != lit("1"))
        )

        if failures.count() != 0:
            sample = failures.select(
                "appealReferenceNumber", 
                "Party", 
                col("ftpaList").getItem(0)["value"]["ftpaApplicant"].alias("actual_applicant")
            ).limit(1).collect()
            
            return TestResult(
                "ftpaList", 
                "FAIL", 
                f"Applicant mismatch for Case {sample[0][0]}. M3 Party: {sample[0][1]} | JSON Applicant: {sample[0][2]}", 
                "ended", 
                inspect.stack()[0].function
            )
        
        return TestResult("ftpaList", "PASS", "ftpaList correctly mapped based on Party type.", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaList", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)



#######################
# ftpaAppellantApplicationDate - Scenario 1
# Check: M3.Party = 1 -> ftpaAppellantApplicationDate MUST be included
#######################
def test_ftpaAppellantApplicationDate_test1(test_df):
    try:
        # Filter for Status 39, EndedGroup 4, and Party 1
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39) &
            (col("Party") == 1)
        )
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantApplicationDate", "PASS", "No Party 1 records found for Status 39.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = []

        for row in rows:
            ftpa_list = row['ftpaList']
            # Access first item in array
            if not ftpa_list or len(ftpa_list) == 0:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: ftpaList array is empty")
                continue
            
            first_item_val = ftpa_list[0]["value"].asDict()
            
            # Check for inclusion
            if "ftpaAppellantApplicationDate" not in first_item_val:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Key missing from JSON")
            elif first_item_val["ftpaAppellantApplicationDate"] is None:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Key present but value is null")

        if results_list:
            return TestResult("ftpaAppellantApplicationDate", "FAIL", "Found inclusion failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        
        return TestResult("ftpaAppellantApplicationDate", "PASS", "Field correctly included for Party 1.", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantApplicationDate", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


#######################
# ftpaAppellantApplicationDate - Scenario 2
# Check: M3.Party = 2 -> ftpaAppellantApplicationDate MUST be omitted
#######################
def test_ftpaAppellantApplicationDate_test2(test_df):
    try:
        # Filter for Status 39, EndedGroup 4, and Party 2
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39) &
            (col("Party") == 2)
        )
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantApplicationDate", "PASS", "No Party 2 records found for Status 39.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = []

        for row in rows:
            ftpa_list = row['ftpaList']
            if not ftpa_list or len(ftpa_list) == 0:
                continue # If list is empty, field is technically omitted
            
            first_item_val = ftpa_list[0]["value"].asDict()
            
            # Check for omission (Key should not exist in the dictionary)
            if "ftpaAppellantApplicationDate" in first_item_val:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Key found in JSON for Party 2 (Should be omitted)")

        if results_list:
            return TestResult("ftpaAppellantApplicationDate", "FAIL", "Found omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        
        return TestResult("ftpaAppellantApplicationDate", "PASS", "Field correctly omitted for Party 2.", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantApplicationDate", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    

#######################
# ftpaAppellantSubmissionOutOfTime - Scenario 1
# Check: M3.Party = 1 -> Must be 'Yes' if OutOfTime=1, else 'No'
#######################
def test_ftpaAppellantSubmissionOutOfTime_test1(test_df):
    try:
        # Filter for Status 39, EndedGroup 4, and Party 1
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39) &
            (col("Party") == 1)
        )
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "PASS", "No Party 1 records found for Status 39.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "OutOfTime", "ftpaList").collect()
        results_list = []

        for row in rows:
            case_no = row['appealReferenceNumber']
            # Determine expected value: 1 -> Yes, anything else -> No
            expected_val = "Yes" if row['OutOfTime'] == 1 else "No"
            
            ftpa_list = row['ftpaList']
            if not ftpa_list or len(ftpa_list) == 0:
                results_list.append(f"FAIL - {case_no}: ftpaList array is empty")
                continue
            
            first_item_val = ftpa_list[0]["value"].asDict()
            
            # Validation
            if "ftpaAppellantSubmissionOutOfTime" not in first_item_val:
                results_list.append(f"FAIL - {case_no}: Key missing from JSON for Party 1")
            else:
                actual_val = first_item_val["ftpaAppellantSubmissionOutOfTime"]
                if actual_val != expected_val:
                    results_list.append(f"FAIL - {case_no}: ARIA OutOfTime={row['OutOfTime']} | Expected JSON='{expected_val}' | Found='{actual_val}'")

        if results_list:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", "Found Party 1 mapping failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        
        return TestResult("ftpaAppellantSubmissionOutOfTime", "PASS", "Field correctly mapped to Yes/No for Party 1.", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


#######################
# ftpaAppellantSubmissionOutOfTime - Scenario 2
# Check: M3.Party != 1 -> ftpaAppellantSubmissionOutOfTime MUST be omitted
#######################
def test_ftpaAppellantSubmissionOutOfTime_test2(test_df):
    try:
        # Filter for Status 39, EndedGroup 4, and Party NOT 1
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39) &
            (col("Party") != 1)
        )
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "PASS", "No non-Party 1 records found for Status 39.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = []

        for row in rows:
            ftpa_list = row['ftpaList']
            if not ftpa_list or len(ftpa_list) == 0:
                continue # Omitted by virtue of empty list
            
            first_item_val = ftpa_list[0]["value"].asDict()
            
            # Check for omission
            if "ftpaAppellantSubmissionOutOfTime" in first_item_val:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Key found in JSON for Party {row['Party']} (Should be omitted)")

        if results_list:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", "Found omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        
        return TestResult("ftpaAppellantSubmissionOutOfTime", "PASS", "Field correctly omitted for non-Party 1.", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    

#######################
# ftpaAppellantOutOfTimeExplanation - Scenario 1
# Check: M3.OutOfTime=1 AND M3.Party=1 -> MUST include hardcoded string
#######################
def test_ftpaAppellantOutOfTimeExplanation_test1(test_df):
    try:
        expected_str = "This is a migrated ARIA case. Please refer to the documents."
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & 
                                        (col("OutOfTime") == 1) & (col("Party") == 1))
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "No records found for OutOfTime=1, Party=1.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = []
        for row in rows:
            ftpa_val = row['ftpaList'][0]["value"].asDict() if row['ftpaList'] else {}
            if "ftpaAppellantOutOfTimeExplanation" not in ftpa_val:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Field missing")
            elif ftpa_val["ftpaAppellantOutOfTimeExplanation"] != expected_str:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Found '{ftpa_val['ftpaAppellantOutOfTimeExplanation']}'")

        if results_list:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", "Found mapping failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "Scenario 1: Correctly included hardcoded string.", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


#######################
# ftpaAppellantOutOfTimeExplanation - Scenario 2
# Check: M3.OutOfTime != 1 AND M3.Party != 1 -> MUST be omitted
#######################
def test_ftpaAppellantOutOfTimeExplanation_test2(test_df):
    try:
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & 
                                        (col("OutOfTime") != 1) & (col("Party") != 1))
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "No records for Scenario 2.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Key found" for r in rows if r['ftpaList'] and "ftpaAppellantOutOfTimeExplanation" in r['ftpaList'][0]["value"].asDict()]

        if results_list:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "Scenario 2: Correctly omitted.", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)



    #######################
# ftpaAppellantOutOfTimeExplanation - Scenario 3
# Check: M3.OutOfTime = 1 AND M3.Party != 1 -> MUST be omitted
#######################
def test_ftpaAppellantOutOfTimeExplanation_test3(test_df):
    try:
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & 
                                        (col("OutOfTime") == 1) & (col("Party") != 1))
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "No records for Scenario 3.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Key found" for r in rows if r['ftpaList'] and "ftpaAppellantOutOfTimeExplanation" in r['ftpaList'][0]["value"].asDict()]

        if results_list:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "Scenario 3: Correctly omitted.", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    



#######################
# ftpaAppellantOutOfTimeExplanation - Scenario 4
# Check: M3.OutOfTime != 1 AND M3.Party = 1 -> MUST be omitted
#######################
def test_ftpaAppellantOutOfTimeExplanation_test4(test_df):
    try:
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & 
                                        (col("OutOfTime") != 1) & (col("Party") == 1))
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "No records for Scenario 4.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Key found" for r in rows if r['ftpaList'] and "ftpaAppellantOutOfTimeExplanation" in r['ftpaList'][0]["value"].asDict()]

        if results_list:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "Scenario 4: Correctly omitted.", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)



#######################
# ftpaRespondentApplicationDate - Scenario 1
# Check: M3.Party = 2 -> ftpaRespondentApplicationDate MUST be included
#######################
def test_ftpaRespondentApplicationDate_test1(test_df):
    try:
        # Filter for Status 39, EndedGroup 4, and Party 2
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39) &
            (col("Party") == 2)
        )
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentApplicationDate", "PASS", "No Party 2 records found for Status 39.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = []

        for row in rows:
            ftpa_list = row['ftpaList']
            if not ftpa_list or len(ftpa_list) == 0:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: ftpaList array is empty")
                continue
            
            first_item_val = ftpa_list[0]["value"].asDict()
            
            # Check for inclusion
            if "ftpaRespondentApplicationDate" not in first_item_val:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Key missing from JSON for Party 2")
            elif first_item_val["ftpaRespondentApplicationDate"] is None:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Key present but value is null")

        if results_list:
            return TestResult("ftpaRespondentApplicationDate", "FAIL", "Found inclusion failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        
        return TestResult("ftpaRespondentApplicationDate", "PASS", "Field correctly included for Party 2.", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentApplicationDate", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    



#######################
# ftpaRespondentApplicationDate - Scenario 2
# Check: M3.Party = 1 -> ftpaRespondentApplicationDate MUST be omitted
#######################
def test_ftpaRespondentApplicationDate_test2(test_df):
    try:
        # Filter for Status 39, EndedGroup 4, and Party 1
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39) &
            (col("Party") == 1)
        )
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentApplicationDate", "PASS", "No Party 1 records found for Status 39.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = []

        for row in rows:
            ftpa_list = row['ftpaList']
            if not ftpa_list or len(ftpa_list) == 0:
                continue 
            
            first_item_val = ftpa_list[0]["value"].asDict()
            
            # Check for omission
            if "ftpaRespondentApplicationDate" in first_item_val:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Key found in JSON for Party 1 (Should be omitted)")

        if results_list:
            return TestResult("ftpaRespondentApplicationDate", "FAIL", "Found omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        
        return TestResult("ftpaRespondentApplicationDate", "PASS", "Field correctly omitted for Party 1.", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentApplicationDate", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


#######################
# ftpaRespondentSubmissionOutOfTime - Scenario 1
# Check: M3.Party = 2 -> Must be 'Yes' if OutOfTime=1, else 'No'
#######################
def test_ftpaRespondentSubmissionOutOfTime_test1(test_df):
    try:
        # Filter for Status 39, EndedGroup 4, and Party 2
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39) &
            (col("Party") == 2)
        )
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "PASS", "No Party 2 records found for Status 39.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "OutOfTime", "ftpaList").collect()
        results_list = []

        for row in rows:
            case_no = row['appealReferenceNumber']
            expected_val = "Yes" if row['OutOfTime'] == 1 else "No"
            
            ftpa_list = row['ftpaList']
            if not ftpa_list or len(ftpa_list) == 0:
                results_list.append(f"FAIL - {case_no}: ftpaList array is empty")
                continue
            
            first_item_val = ftpa_list[0]["value"].asDict()
            
            if "ftpaRespondentSubmissionOutOfTime" not in first_item_val:
                results_list.append(f"FAIL - {case_no}: Key missing from JSON for Party 2")
            else:
                actual_val = first_item_val["ftpaRespondentSubmissionOutOfTime"]
                if actual_val != expected_val:
                    results_list.append(f"FAIL - {case_no}: ARIA OutOfTime={row['OutOfTime']} | Expected JSON='{expected_val}' | Found='{actual_val}'")

        if results_list:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", "Found Party 2 mapping failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        
        return TestResult("ftpaRespondentSubmissionOutOfTime", "PASS", "Field correctly mapped to Yes/No for Party 2.", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


#######################
# ftpaRespondentSubmissionOutOfTime - Scenario 2
# Check: M3.Party != 2 -> ftpaRespondentSubmissionOutOfTime MUST be omitted
#######################
def test_ftpaRespondentSubmissionOutOfTime_test2(test_df):
    try:
        # Filter for Status 39, EndedGroup 4, and Party NOT 2
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39) &
            (col("Party") != 2)
        )
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "PASS", "No non-Party 2 records found for Status 39.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = []

        for row in rows:
            ftpa_list = row['ftpaList']
            if not ftpa_list or len(ftpa_list) == 0:
                continue 
            
            first_item_val = ftpa_list[0]["value"].asDict()
            
            if "ftpaRespondentSubmissionOutOfTime" in first_item_val:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Key found in JSON for Party {row['Party']} (Should be omitted)")

        if results_list:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", "Found omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        
        return TestResult("ftpaRespondentSubmissionOutOfTime", "PASS", "Field correctly omitted for non-Party 2.", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


#######################
# ftpaRespondentOutOfTimeExplanation - Scenario 1
# Check: M3.OutOfTime=1 AND M3.Party=2 -> MUST include hardcoded string
#######################
def test_ftpaRespondentOutOfTimeExplanation_test1(test_df):
    try:
        expected_str = "This is a migrated ARIA case. Please refer to the documents."
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & 
                                        (col("OutOfTime") == 1) & (col("Party") == 2))
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "No records found for Scenario 1.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = []
        for row in rows:
            ftpa_val = row['ftpaList'][0]["value"].asDict() if row['ftpaList'] else {}
            if "ftpaRespondentOutOfTimeExplanation" not in ftpa_val:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Key missing")
            elif ftpa_val["ftpaRespondentOutOfTimeExplanation"] != expected_str:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Found '{ftpa_val['ftpaRespondentOutOfTimeExplanation']}'")

        if results_list:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "Mapping failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "Scenario 1: Correctly included hardcoded string.", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    

#######################
# ftpaRespondentOutOfTimeExplanation - Scenario 2
# Check: M3.OutOfTime = 1 AND M3.Party != 2 -> MUST be omitted
#######################
def test_ftpaRespondentOutOfTimeExplanation_test2(test_df):
    try:
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & 
                                        (col("OutOfTime") == 1) & (col("Party") != 2))
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "No records for Scenario 2.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Key found" for r in rows if r['ftpaList'] and "ftpaRespondentOutOfTimeExplanation" in r['ftpaList'][0]["value"].asDict()]

        if results_list:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "Scenario 2: Correctly omitted.", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


#######################
# ftpaRespondentOutOfTimeExplanation - Scenario 3
# Check: M3.OutOfTime != 1 AND M3.Party = 2 -> MUST be omitted
#######################
def test_ftpaRespondentOutOfTimeExplanation_test3(test_df):
    try:
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & 
                                        (col("OutOfTime") != 1) & (col("Party") == 2))
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "No records for Scenario 3.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Key found" for r in rows if r['ftpaList'] and "ftpaRespondentOutOfTimeExplanation" in r['ftpaList'][0]["value"].asDict()]

        if results_list:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "Scenario 3: Correctly omitted.", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    



#######################
# ftpaRespondentOutOfTimeExplanation - Scenario 4
# Check: M3.OutOfTime != 1 AND M3.Party != 2 -> MUST be omitted
#######################
def test_ftpaRespondentOutOfTimeExplanation_test4(test_df):
    try:
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & 
                                        (col("OutOfTime") != 1) & (col("Party") != 2))
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "No records for Scenario 4.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Key found" for r in rows if r['ftpaList'] and "ftpaRespondentOutOfTimeExplanation" in r['ftpaList'][0]["value"].asDict()]

        if results_list:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "Scenario 4: Correctly omitted.", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


####################################################   


from pyspark.sql import functions as F
from pyspark.sql.window import Window

def test_general_init(json_data, M1_bronze, M2_bronze, M3_bronze, M1_silver):
    try:
        # List of ALL fields for Appellant, Respondent, and Bundle
        target_fields = [
            "appealReferenceNumber",
            "ftpaAppellantSubmitted",
            "isFtpaAppellantDocsVisibleInDecided",
            "isFtpaAppellantDocsVisibleInSubmitted",
            "isFtpaAppellantOotDocsVisibleInDecided",
            "isFtpaAppellantOotDocsVisibleInSubmitted",
            "isFtpaAppellantGroundsDocsVisibleInDecided",
            "isFtpaAppellantEvidenceDocsVisibleInDecided",
            "isFtpaAppellantGroundsDocsVisibleInSubmitted",
            "isFtpaAppellantEvidenceDocsVisibleInSubmitted",
            "isFtpaAppellantOotExplanationVisibleInDecided",
            "isFtpaAppellantOotExplanationVisibleInSubmitted",
            "ftpaRespondentSubmitted",
            "isFtpaRespondentDocsVisibleInDecided",
            "isFtpaRespondentDocsVisibleInSubmitted",
            "isFtpaRespondentOotDocsVisibleInDecided",
            "isFtpaRespondentOotDocsVisibleInSubmitted",
            "isFtpaRespondentGroundsDocsVisibleInDecided",
            "isFtpaRespondentEvidenceDocsVisibleInDecided",
            "isFtpaRespondentGroundsDocsVisibleInSubmitted",
            "isFtpaRespondentEvidenceDocsVisibleInSubmitted",
            "isFtpaRespondentOotExplanationVisibleInDecided",
            "isFtpaRespondentOotExplanationVisibleInSubmitted",
            "bundleFileNamePrefix"
        ]

        # 1. Dynamic Select from JSON
        available_fields = [col for col in target_fields if col in json_data.columns]
        if "appealReferenceNumber" not in available_fields:
             return None, TestResult("general_init", "FAIL", "Critical Error: appealReferenceNumber missing", "ended", "init")

        test_df = json_data.select(*available_fields)

        # 2. Window logic with Priority for M3
        m3_priority = M3_bronze.withColumn(
            "priority", 
            F.when(F.col("CaseStatus").isin(37, 38), 1)
             .when(F.col("CaseStatus") == 39, 2)
             .otherwise(3)
        )
        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("priority").asc(), F.col("StatusId").desc())
        m3_latest = m3_priority.withColumn("rn", F.row_number().over(window_spec)) \
            .filter(F.col("rn") == 1) \
            .select("CaseNo", "CaseStatus", "StatusId", "Party", "OutOfTime")

        # 3. Prepare M1 (Formatted CaseNo) and M2 (Appellant Name) for Bundle Prefix
        m1_prep = M1_bronze.select(
            F.col("CaseNo").alias("M1_Join_Key"),
            F.regexp_replace(F.col("CaseNo"), "/", " ").alias("Formatted_CaseNo")
        )
        
        m2_prep = M2_bronze.select(
            F.col("CaseNo").alias("M2_Join_Key"), 
            "Appellant_Name"
        )

        # 4. Prepare M1_silver for Representation
        m1_silver_prep = M1_silver.select(
            F.col("CaseNo").alias("Silver_Join_Key"), 
            F.col("dv_representation").alias("Representation")
        )

        # 5. Master Join
        test_df = test_df.join(
            m3_latest, test_df["appealReferenceNumber"] == m3_latest["CaseNo"], "inner"
        ).join(
            m1_prep, test_df["appealReferenceNumber"] == m1_prep["M1_Join_Key"], "left"
        ).join(
            m2_prep, test_df["appealReferenceNumber"] == m2_prep["M2_Join_Key"], "left"
        ).join(
            m1_silver_prep, test_df["appealReferenceNumber"] == m1_silver_prep["Silver_Join_Key"], "left"
        )

        # 6. Calculate Expected bundleFileNamePrefix ('CaseNo-Appellant_Name')
        test_df = test_df.withColumn(
            "expected_bundle_prefix",
            F.concat(F.col("Formatted_CaseNo"), F.lit("-"), F.col("Appellant_Name"))
        )

        # Drop redundant join keys
        test_df = test_df.drop("CaseNo", "M1_Join_Key", "M2_Join_Key", "Silver_Join_Key")

        return test_df, True

    except Exception as e:
        return None, TestResult("general_init", "FAIL", f"Failed to Setup Data: {str(e)[:300]}", "ended", "init")
    

def test_bundleFileNamePrefix_test1(test_df):
    try:
        # Collect relevant columns for validation
        # expected_bundle_prefix was created in the Init via: CaseNo (slashes to spaces) + "-" + Appellant_Name
        rows = test_df.select(
            "appealReferenceNumber", 
            "bundleFileNamePrefix", 
            "expected_bundle_prefix"
        ).collect()
        
        results = []
        
        for row in rows:
            actual = row['bundleFileNamePrefix']
            expected = row['expected_bundle_prefix']
            
            # Handle potential nulls in source data (e.g., if Appellant_Name was missing in M2)
            if expected is None or "-" not in expected:
                # If we couldn't build a valid expected string, we skip or flag as data error
                continue 

            if actual != expected:
                results.append(f"FAIL - {row['appealReferenceNumber']}: Expected '{expected}' | Found '{actual}'")

        count = len(rows)
        if results:
            return TestResult(
                "bundleFileNamePrefix", 
                "FAIL", 
                f"Found {len(results)} mismatches. Sample: " + " || ".join(results[:3]), 
                "ended", 
                "test1"
            )
        
        return TestResult(
            "bundleFileNamePrefix", 
            "PASS", 
            f"Verified {count} records; all bundle prefixes match the 'CaseNo-Appellant_Name' format with correct space-formatting.", 
            "ended", 
            "test1"
        )

    except Exception as e:
        return TestResult("bundleFileNamePrefix", "FAIL", f"EXCEPTION: {str(e)[:200]}", "ended", "test1")

#################################################################################
# ftpaAppellantSubmitted
# Logic: IF M3.Party IS 1 = "Yes"; ELSE IF M3.Party IS 2 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#################################################################################

def test_ftpaAppellantSubmitted_test1(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['ftpaAppellantSubmitted'] != "Yes":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'Yes' for Party 1")
    
    count = len(rows)
    if results:
        return TestResult("ftpaAppellantSubmitted", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("ftpaAppellantSubmitted", "PASS", f"Verified {count} Party 1 records; all correctly contain 'Yes'.", "ended", "test1")

def test_ftpaAppellantSubmitted_test2(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['ftpaAppellantSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for Party 2")
    
    count = len(rows)
    if results:
        return TestResult("ftpaAppellantSubmitted", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("ftpaAppellantSubmitted", "PASS", f"Verified {count} Party 2 records; all correctly omit the field.", "ended", "test2")


#######################
# isFtpaAppellantDocsVisibleInDecided
# Logic: IF M3.Party IS 1 = "No"; ELSE IF M3.Party IS 2 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaAppellantDocsVisibleInDecided_test1(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaAppellantDocsVisibleInDecided'] != "No":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'No' for Party 1")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaAppellantDocsVisibleInDecided", "PASS", f"Verified {count} Party 1 records; visibility correctly set to 'No'.", "ended", "test1")

def test_isFtpaAppellantDocsVisibleInDecided_test2(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaAppellantDocsVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for Party 2")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaAppellantDocsVisibleInDecided", "PASS", f"Verified {count} Party 2 records; correctly omitted.", "ended", "test2")





#######################
# isFtpaAppellantDocsVisibleInSubmitted
# Logic: IF M3.Party IS 1 = "Yes"; ELSE IF M3.Party IS 2 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaAppellantDocsVisibleInSubmitted_test1(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaAppellantDocsVisibleInSubmitted'] != "Yes":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'Yes' for Party 1")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 1 records; visibility set to 'Yes'.", "ended", "test1")

def test_isFtpaAppellantDocsVisibleInSubmitted_test2(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaAppellantDocsVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for Party 2")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 2 records; correctly omitted.", "ended", "test2")






#######################
# isFtpaAppellantOotDocsVisibleInDecided
# Logic: IF M3.Party IS 1 AND M3.OutOfTime IS 1 = "No"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaAppellantOotDocsVisibleInDecided_test1(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1) & (F.col("OutOfTime") == 1)).collect()
    for row in rows:
        if row['isFtpaAppellantOotDocsVisibleInDecided'] != "No":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'No' (P1-OOT)")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", "OOT Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "PASS", f"Verified {count} P1 OOT records; visibility set to 'No'.", "ended", "test1")

def test_isFtpaAppellantOotDocsVisibleInDecided_test2(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1) & (F.col("OutOfTime") != 1)).collect()
    for row in rows:
        if row['isFtpaAppellantOotDocsVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit (P1-InTime)")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", "Omit Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "PASS", f"Verified {count} P1 In-Time records; correctly omitted.", "ended", "test2")

def test_isFtpaAppellantOotDocsVisibleInDecided_test3(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaAppellantOotDocsVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit (P2)")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test3")
    return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "PASS", f"Verified {count} Party 2 records; correctly omitted.", "ended", "test3")







#######################
# isFtpaAppellantOotDocsVisibleInSubmitted
# Logic: IF M3.Party IS 1 AND M3.OutOfTime IS 1 = "Yes"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaAppellantOotDocsVisibleInSubmitted_test1(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1) & (F.col("OutOfTime") == 1)).collect()
    for row in rows:
        if row['isFtpaAppellantOotDocsVisibleInSubmitted'] != "Yes":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'Yes' (P1-OOT)")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", "OOT Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "PASS", f"Verified {count} P1 OOT records; visibility set to 'Yes'.", "ended", "test1")

def test_isFtpaAppellantOotDocsVisibleInSubmitted_test2(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1) & (F.col("OutOfTime") != 1)).collect()
    for row in rows:
        if row['isFtpaAppellantOotDocsVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit (P1-InTime)")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", "Omit Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "PASS", f"Verified {count} P1 In-Time records; correctly omitted.", "ended", "test2")

def test_isFtpaAppellantOotDocsVisibleInSubmitted_test3(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaAppellantOotDocsVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit (P2)")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test3")
    return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 2 records; correctly omitted.", "ended", "test3")





#######################
# isFtpaAppellantGroundsDocsVisibleInDecided
# Logic: IF M3.Party IS 1 = "No"; ELSE IF M3.Party IS 2 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaAppellantGroundsDocsVisibleInDecided_test1(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaAppellantGroundsDocsVisibleInDecided'] != "No":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'No' for P1")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "PASS", f"Verified {count} Party 1 records; visibility correctly set to 'No'.", "ended", "test1")

def test_isFtpaAppellantGroundsDocsVisibleInDecided_test2(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaAppellantGroundsDocsVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for P2")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "PASS", f"Verified {count} Party 2 records; correctly omitted.", "ended", "test2")






#######################
# isFtpaAppellantEvidenceDocsVisibleInDecided
# Logic: IF M3.Party IS 1 = "No"; ELSE IF M3.Party IS 2 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaAppellantEvidenceDocsVisibleInDecided_test1(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaAppellantEvidenceDocsVisibleInDecided'] != "No":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'No' for P1")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "PASS", f"Verified {count} Party 1 records; visibility set to 'No'.", "ended", "test1")

def test_isFtpaAppellantEvidenceDocsVisibleInDecided_test2(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaAppellantEvidenceDocsVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for P2")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "PASS", f"Verified {count} Party 2 records; correctly omitted.", "ended", "test2")





#######################
# isFtpaAppellantGroundsDocsVisibleInSubmitted
# Logic: IF M3.Party IS 1 = "Yes"; ELSE IF M3.Party IS 2 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaAppellantGroundsDocsVisibleInSubmitted_test1(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaAppellantGroundsDocsVisibleInSubmitted'] != "Yes":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'Yes' for P1")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 1 records; visibility set to 'Yes'.", "ended", "test1")

def test_isFtpaAppellantGroundsDocsVisibleInSubmitted_test2(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaAppellantGroundsDocsVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for P2")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 2 records; correctly omitted.", "ended", "test2")





#######################
# isFtpaAppellantEvidenceDocsVisibleInSubmitted
# Logic: IF M3.Party IS 1 = "Yes"; ELSE IF M3.Party IS 2 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaAppellantEvidenceDocsVisibleInSubmitted_test1(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaAppellantEvidenceDocsVisibleInSubmitted'] != "Yes":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'Yes' for P1")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 1 records; visibility set to 'Yes'.", "ended", "test1")

def test_isFtpaAppellantEvidenceDocsVisibleInSubmitted_test2(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaAppellantEvidenceDocsVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for P2")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 2 records; correctly omitted.", "ended", "test2")





#######################
# isFtpaAppellantOotExplanationVisibleInDecided
# Logic: IF M3.Party IS 1 AND M3.OutOfTime IS 1 = "No"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaAppellantOotExplanationVisibleInDecided_test1(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1) & (F.col("OutOfTime") == 1)).collect()
    for row in rows:
        if row['isFtpaAppellantOotExplanationVisibleInDecided'] != "No":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'No' (P1-OOT)")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", "OOT Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "PASS", f"Verified {count} P1 OOT records; visibility set to 'No'.", "ended", "test1")

def test_isFtpaAppellantOotExplanationVisibleInDecided_test2(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1) & (F.col("OutOfTime") != 1)).collect()
    for row in rows:
        if row['isFtpaAppellantOotExplanationVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit (P1-InTime)")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", "Omit Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "PASS", f"Verified {count} P1 In-Time records; correctly omitted.", "ended", "test2")

def test_isFtpaAppellantOotExplanationVisibleInDecided_test3(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaAppellantOotExplanationVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit (P2)")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test3")
    return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "PASS", f"Verified {count} Party 2 records; correctly omitted.", "ended", "test3")




#######################
# isFtpaAppellantOotExplanationVisibleInSubmitted
# Logic: IF M3.Party IS 1 AND M3.OutOfTime IS 1 = "Yes"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaAppellantOotExplanationVisibleInSubmitted_test1(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1) & (F.col("OutOfTime") == 1)).collect()
    for row in rows:
        if row['isFtpaAppellantOotExplanationVisibleInSubmitted'] != "Yes":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'Yes' (P1-OOT)")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", "OOT Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "PASS", f"Verified {count} P1 OOT records; visibility set to 'Yes'.", "ended", "test1")

def test_isFtpaAppellantOotExplanationVisibleInSubmitted_test2(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1) & (F.col("OutOfTime") != 1)).collect()
    for row in rows:
        if row['isFtpaAppellantOotExplanationVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit (P1-InTime)")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", "Omit Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "PASS", f"Verified {count} P1 In-Time records; correctly omitted.", "ended", "test2")

def test_isFtpaAppellantOotExplanationVisibleInSubmitted_test3(test_df):
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaAppellantOotExplanationVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit (P2)")
    count = len(rows)
    if results:
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test3")
    return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "PASS", f"Verified {count} Party 2 records; correctly omitted.", "ended", "test3")


#######################
# ftpaRespondentSubmitted
# Logic: IF M3.Party IS 2 = "Yes"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_ftpaRespondentSubmitted_test1(test_df):
    if "ftpaRespondentSubmitted" not in test_df.columns:
        return TestResult("ftpaRespondentSubmitted", "FAIL", "no data to test", "ended", "test1")
    
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['ftpaRespondentSubmitted'] != "Yes":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'Yes' for Party 2")
    count = len(rows)
    if results:
        return TestResult("ftpaRespondentSubmitted", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("ftpaRespondentSubmitted", "PASS", f"Verified {count} Party 2 records; all correctly contain 'Yes'.", "ended", "test1")

def test_ftpaRespondentSubmitted_test2(test_df):
    if "ftpaRespondentSubmitted" not in test_df.columns:
        return TestResult("ftpaRespondentSubmitted", "FAIL", "no data to test", "ended", "test2")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['ftpaRespondentSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for Party 1")
    count = len(rows)
    if results:
        return TestResult("ftpaRespondentSubmitted", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("ftpaRespondentSubmitted", "PASS", f"Verified {count} Party 1 records; fields correctly omitted.", "ended", "test2")



#######################
# isFtpaRespondentDocsVisibleInDecided
# Logic: IF M3.Party IS 2 = "No"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentDocsVisibleInDecided_test1(test_df):
    if "isFtpaRespondentDocsVisibleInDecided" not in test_df.columns:
        return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", "no data to test", "ended", "test1")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaRespondentDocsVisibleInDecided'] != "No":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'No' for Party 2")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaRespondentDocsVisibleInDecided", "PASS", f"Verified {count} Party 2 records; visibility correctly set to 'No'.", "ended", "test1")

def test_isFtpaRespondentDocsVisibleInDecided_test2(test_df):
    if "isFtpaRespondentDocsVisibleInDecided" not in test_df.columns:
        return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", "no data to test", "ended", "test2")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentDocsVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for Party 1")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaRespondentDocsVisibleInDecided", "PASS", f"Verified {count} Party 1 records; correctly omitted.", "ended", "test2")




#######################
# isFtpaRespondentDocsVisibleInSubmitted
# Logic: IF M3.Party IS 2 = "Yes"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentDocsVisibleInSubmitted_test1(test_df):
    if "isFtpaRespondentDocsVisibleInSubmitted" not in test_df.columns:
        return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", "no data to test", "ended", "test1")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaRespondentDocsVisibleInSubmitted'] != "Yes":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'Yes' for Party 2")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 2 records; visibility set to 'Yes'.", "ended", "test1")

def test_isFtpaRespondentDocsVisibleInSubmitted_test2(test_df):
    if "isFtpaRespondentDocsVisibleInSubmitted" not in test_df.columns:
        return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", "no data to test", "ended", "test2")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentDocsVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for Party 1")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", "Logic Mismatch: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 1 records; correctly omitted.", "ended", "test2")




#######################
# isFtpaRespondentOotDocsVisibleInDecided
# Logic: IF M3.Party IS 2 AND M3.OutOfTime IS 1 = "No"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentOotDocsVisibleInDecided_test1(test_df):
    if "isFtpaRespondentOotDocsVisibleInDecided" not in test_df.columns:
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "no data to test", "ended", "test1")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2) & (F.col("OutOfTime") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentOotDocsVisibleInDecided'] != "No":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'No' (P2-OOT)")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "OOT Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "PASS", f"Verified {count} Respondent OOT records; set to 'No'.", "ended", "test1")

def test_isFtpaRespondentOotDocsVisibleInDecided_test2(test_df):
    if "isFtpaRespondentOotDocsVisibleInDecided" not in test_df.columns:
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "no data to test", "ended", "test2")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2) & (F.col("OutOfTime") != 1)).collect()
    for row in rows:
        if row['isFtpaRespondentOotDocsVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit (P2-InTime)")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "PASS", f"Verified {count} Respondent In-Time records; correctly omitted.", "ended", "test2")

def test_isFtpaRespondentOotDocsVisibleInDecided_test3(test_df):
    if "isFtpaRespondentOotDocsVisibleInDecided" not in test_df.columns:
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "no data to test", "ended", "test3")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentOotDocsVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for Party 1")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test3")
    return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "PASS", f"Verified {count} Party 1 records; correctly omitted.", "ended", "test3")




#######################
# isFtpaRespondentOotDocsVisibleInSubmitted
# Logic: IF M3.Party IS 2 AND M3.OutOfTime IS 1 = "Yes"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentOotDocsVisibleInSubmitted_test1(test_df):
    if "isFtpaRespondentOotDocsVisibleInSubmitted" not in test_df.columns:
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "no data to test", "ended", "test1")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2) & (F.col("OutOfTime") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentOotDocsVisibleInSubmitted'] != "Yes":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'Yes' (P2-OOT)")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "OOT Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "PASS", f"Verified {count} Respondent OOT records; set to 'Yes'.", "ended", "test1")

def test_isFtpaRespondentOotDocsVisibleInSubmitted_test2(test_df):
    if "isFtpaRespondentOotDocsVisibleInSubmitted" not in test_df.columns:
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "no data to test", "ended", "test2")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2) & (F.col("OutOfTime") != 1)).collect()
    for row in rows:
        if row['isFtpaRespondentOotDocsVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit (P2-InTime)")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "PASS", f"Verified {count} Respondent In-Time records; correctly omitted.", "ended", "test2")

def test_isFtpaRespondentOotDocsVisibleInSubmitted_test3(test_df):
    if "isFtpaRespondentOotDocsVisibleInSubmitted" not in test_df.columns:
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "no data to test", "ended", "test3")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentOotDocsVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for Party 1")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test3")
    return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 1 records; correctly omitted.", "ended", "test3")




#######################
# isFtpaRespondentGroundsDocsVisibleInDecided
# Logic: IF M3.Party IS 2 = "No"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentGroundsDocsVisibleInDecided_test1(test_df):
    if "isFtpaRespondentGroundsDocsVisibleInDecided" not in test_df.columns:
        return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", "no data to test", "ended", "test1")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaRespondentGroundsDocsVisibleInDecided'] != "No":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'No' for P2")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "PASS", f"Verified {count} Party 2 records; grounds set to 'No'.", "ended", "test1")

def test_isFtpaRespondentGroundsDocsVisibleInDecided_test2(test_df):
    if "isFtpaRespondentGroundsDocsVisibleInDecided" not in test_df.columns:
        return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", "no data to test", "ended", "test2")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentGroundsDocsVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for P1")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "PASS", f"Verified {count} Party 1 records; correctly omitted.", "ended", "test2")




#######################
# isFtpaRespondentEvidenceDocsVisibleInDecided
# Logic: IF M3.Party IS 2 = "No"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentEvidenceDocsVisibleInDecided_test1(test_df):
    if "isFtpaRespondentEvidenceDocsVisibleInDecided" not in test_df.columns:
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", "no data to test", "ended", "test1")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaRespondentEvidenceDocsVisibleInDecided'] != "No":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'No' for P2")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "PASS", f"Verified {count} Party 2 records; evidence set to 'No'.", "ended", "test1")

def test_isFtpaRespondentEvidenceDocsVisibleInDecided_test2(test_df):
    if "isFtpaRespondentEvidenceDocsVisibleInDecided" not in test_df.columns:
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", "no data to test", "ended", "test2")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentEvidenceDocsVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for P1")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "PASS", f"Verified {count} Party 1 records; correctly omitted.", "ended", "test2")



#######################
# isFtpaRespondentGroundsDocsVisibleInSubmitted
# Logic: IF M3.Party IS 2 = "Yes"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test1(test_df):
    if "isFtpaRespondentGroundsDocsVisibleInSubmitted" not in test_df.columns:
        return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", "no data to test", "ended", "test1")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaRespondentGroundsDocsVisibleInSubmitted'] != "Yes":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'Yes' for P2")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 2 records; grounds set to 'Yes'.", "ended", "test1")

def test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test2(test_df):
    if "isFtpaRespondentGroundsDocsVisibleInSubmitted" not in test_df.columns:
        return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", "no data to test", "ended", "test2")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentGroundsDocsVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for P1")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 1 records; correctly omitted.", "ended", "test2")




#######################
# isFtpaRespondentEvidenceDocsVisibleInSubmitted
# Logic: IF M3.Party IS 2 = "Yes"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test1(test_df):
    if "isFtpaRespondentEvidenceDocsVisibleInSubmitted" not in test_df.columns:
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", "no data to test", "ended", "test1")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2)).collect()
    for row in rows:
        if row['isFtpaRespondentEvidenceDocsVisibleInSubmitted'] != "Yes":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'Yes' for P2")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 2 records; evidence set to 'Yes'.", "ended", "test1")

def test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test2(test_df):
    if "isFtpaRespondentEvidenceDocsVisibleInSubmitted" not in test_df.columns:
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", "no data to test", "ended", "test2")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentEvidenceDocsVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for P1")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "PASS", f"Verified {count} Party 1 records; correctly omitted.", "ended", "test2")



#######################
# isFtpaRespondentOotExplanationVisibleInDecided
# Logic: IF M3.Party IS 2 AND M3.OutOfTime IS 1 = "No"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentOotExplanationVisibleInDecided_test1(test_df):
    if "isFtpaRespondentOotExplanationVisibleInDecided" not in test_df.columns:
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "no data to test", "ended", "test1")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2) & (F.col("OutOfTime") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentOotExplanationVisibleInDecided'] != "No":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'No' (P2-OOT)")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "OOT Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "PASS", f"Verified {count} Respondent OOT records; set to 'No'.", "ended", "test1")

def test_isFtpaRespondentOotExplanationVisibleInDecided_test2(test_df):
    if "isFtpaRespondentOotExplanationVisibleInDecided" not in test_df.columns:
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "no data to test", "ended", "test2")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2) & (F.col("OutOfTime") != 1)).collect()
    for row in rows:
        if row['isFtpaRespondentOotExplanationVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit (P2-InTime)")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "PASS", f"Verified {count} Respondent In-Time records; correctly omitted.", "ended", "test2")

def test_isFtpaRespondentOotExplanationVisibleInDecided_test3(test_df):
    if "isFtpaRespondentOotExplanationVisibleInDecided" not in test_df.columns:
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "no data to test", "ended", "test3")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentOotExplanationVisibleInDecided'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for Party 1")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test3")
    return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "PASS", f"Verified {count} Party 1 records; correctly omitted.", "ended", "test3")



#######################
# isFtpaRespondentOotExplanationVisibleInSubmitted
# Logic: IF M3.Party IS 2 AND M3.OutOfTime IS 1 = "Yes"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentOotExplanationVisibleInSubmitted_test1(test_df):
    if "isFtpaRespondentOotExplanationVisibleInSubmitted" not in test_df.columns:
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "no data to test", "ended", "test1")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2) & (F.col("OutOfTime") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentOotExplanationVisibleInSubmitted'] != "Yes":
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected 'Yes' (P2-OOT)")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "OOT Error: " + "|||".join(results[:5]), "ended", "test1")
    return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "PASS", f"Verified {count} Respondent OOT records; set to 'Yes'.", "ended", "test1")

def test_isFtpaRespondentOotExplanationVisibleInSubmitted_test2(test_df):
    if "isFtpaRespondentOotExplanationVisibleInSubmitted" not in test_df.columns:
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "no data to test", "ended", "test2")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 2) & (F.col("OutOfTime") != 1)).collect()
    for row in rows:
        if row['isFtpaRespondentOotExplanationVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit (P2-InTime)")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test2")
    return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "PASS", f"Verified {count} Respondent In-Time records; correctly omitted.", "ended", "test2")

def test_isFtpaRespondentOotExplanationVisibleInSubmitted_test3(test_df):
    if "isFtpaRespondentOotExplanationVisibleInSubmitted" not in test_df.columns:
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "no data to test", "ended", "test3")
        
    results = []
    rows = test_df.filter((F.col("CaseStatus") == 39) & (F.col("Party") == 1)).collect()
    for row in rows:
        if row['isFtpaRespondentOotExplanationVisibleInSubmitted'] is not None:
            results.append(f"FAIL - {row['appealReferenceNumber']}: Expected Omit for Party 1")
    count = len(rows)
    if results:
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "Logic Error: " + "|||".join(results[:5]), "ended", "test3")
    return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "PASS", f"Verified {count} Party 1 records; correctly omitted.", "ended", "test3")

############################################################################################
#######################
#ended Init code
#######################
def test_ended_init(json_data, M1_bronze, M3_bronze, M1_silver):
    try:
        # 1. Define Ended Filter
        ended_filter = (
            ((F.col("CaseStatus") == '10') & (F.col("Outcome").isin(80, 122, 25, 120, 2, 105, 13))) |
            ((F.col("CaseStatus") == '46') & (F.col("Outcome") == 31)) |
            ((F.col("CaseStatus") == '26') & (F.col("Outcome").isin(80, 13, 25))) |
            ((F.col("CaseStatus").isin('37', '38')) & (F.col("Outcome").isin(80, 13, 25, 72, 125))) |
            ((F.col("CaseStatus") == '39') & (F.col("Outcome") == 25)) |
            ((F.col("CaseStatus") == '51') & (F.col("Outcome").isin(0, 94, 93))) |
            ((F.col("CaseStatus") == '52') & (F.col("Outcome").isin(91, 95))) |
            ((F.col("CaseStatus") == '36') & (F.col("Outcome").isin(1, 2, 25)))
        )

        # 2. Select JSON fields
        json_df = json_data.select(
            "appealReferenceNumber", "endAppealOutcome", "endAppealOutcomeReason",
            "endAppealApproverType", "endAppealApproverName", "endAppealDate", "stateBeforeEndAppeal"
        )

        # 3. Get M1_silver Representation (dv_representation)
        # Assuming CaseNo is the join key in M1_silver
        m1_rep = M1_silver.select("CaseNo", F.col("dv_representation").alias("Representation"))

        # 4. Process M3 Ended
        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        m3_ended = M3_bronze.filter(ended_filter) \
            .withColumn("rn", F.row_number().over(window_spec)) \
            .filter("rn = 1") \
            .select(
                "CaseNo", "CaseStatus", "Outcome", "StatusId", "DecisionDate",
                "Adj_Determination_Title", "Adj_Determination_Forenames", "Adj_Determination_Surname"
            )

        # 5. Master Join (JSON + M3 + M1_silver)
        test_df = json_df.join(
            m3_ended, json_df["appealReferenceNumber"] == m3_ended["CaseNo"], "inner"
        ).join(
            m1_rep, json_df["appealReferenceNumber"] == m1_rep["CaseNo"], "left" # Left join in case M1_silver is missing rows
        ).drop(m3_ended["CaseNo"], m1_rep["CaseNo"])

        if test_df.isEmpty():
            return None, TestResult("ended_init", "FAIL", "No records matched criteria.", "ended")

        return test_df, True

    except Exception as e:
        return None, TestResult("ended_init", "FAIL", f"Setup Error: {str(e)[:300]}", "ended")
########################
def test_endAppealOutcome_test1(test_df):
    try:
        # Define mapping: (CaseStatus, Outcome) -> Expected endAppealOutcome
        mapping = {
            ('37', 80): "Abandoned",
            ('38', 80): "Abandoned",
            ('10', 80): "Abandoned",
            ('10', 122): "Abandoned",
            ('26', 80): "Abandoned",
            ('51', 94): "Struck out",
            ('37', 13): "No valid appeal",
            ('38', 13): "No valid appeal",
            ('26', 13): "No valid appeal",
            ('37', 25): "Withdrawn",
            ('38', 25): "Withdrawn",
            ('39', 25): "Withdrawn",
            ('10', 25): "Withdrawn",
            ('26', 25): "Withdrawn",
            ('52', 91): "Struck out",
            ('52', 95): "Struck out",
            ('51', 93): "Struck out",
            ('38', 72): "Abandoned",
            ('10', 120): "Struck out",
            ('10', 2): "Struck out",
            ('46', 31): "Struck out"
        }

        rows = test_df.select("appealReferenceNumber", "CaseStatus", "Outcome", "endAppealOutcome").collect()
        results = []
        
        for row in rows:
            status = str(row['CaseStatus'])
            outcome = int(row['Outcome'])
            actual = row['endAppealOutcome']
            
            expected = mapping.get((status, outcome))
            
            if expected is None:
                continue # Record doesn't match our specific test list
                
            if actual != expected:
                results.append(f"FAIL - {row['appealReferenceNumber']}: Source({status}/{outcome}) | Expected '{expected}' | Found '{actual}'")

        if results:
            return TestResult("endAppealOutcome", "FAIL", "Logic Mismatch: " + " || ".join(results[:5]), "ended", "test1")
        
        return TestResult("endAppealOutcome", "PASS", f"Verified {len(rows)} records; all outcomes match mapping table.", "ended", "test1")

    except Exception as e:
        return TestResult("endAppealOutcome", "FAIL", f"EXCEPTION: {str(e)[:200]}", "ended", "test1")
    

def test_endAppealOutcomeReason_test1(test_df):
    try:
        # Define mapping: (CaseStatus, Outcome) -> Expected Reason Snippet
        # This follows your requirement: "This is a migrated case. The final outcome was {Snippet}."
        reason_mapping = {
            ('37', 80): "First Tier - Hearing | Abandoned",
            ('38', 80): "First Tier - Paper | Abandoned",
            ('10', 80): "Preliminary Issue | Abandoned",
            ('10', 122): "Preliminary Issue | Abandoned (non-CCD)",
            ('26', 80): "Case Management Review | Abandoned",
            ('51', 94): "Closed - Fee Not Paid | Struck Out",
            ('37', 13): "First Tier - Hearing | No Valid Appeal",
            ('38', 13): "First Tier - Paper | No Valid Appeal",
            ('26', 13): "Case Management Review | No Valid Appeal",
            ('37', 25): "First Tier - Hearing | Withdrawn",
            ('38', 25): "First Tier - Paper | Withdrawn",
            ('39', 25): "First Tier Permission Application | Withdrawn",
            ('10', 25): "Preliminary Issue | Withdrawn",
            ('26', 25): "Case Management Review | Withdrawn",
            ('52', 91): "Case closed fee outstanding | Fee Paid/Exempt",
            ('52', 95): "Case closed fee outstanding | Write Off",
            ('51', 93): "Closed - Fee Not Paid | Admin Closure",
            ('38', 72): "First Tier - Paper | Certified under Rule 16",
            ('10', 120): "Preliminary Issue | Admin Rejected (Non-CCD)",
            ('10', 2): "Preliminary Issue | Dismissed",
            ('46', 31): "Set Aside Application | Refused"
        }

        rows = test_df.select("appealReferenceNumber", "CaseStatus", "Outcome", "endAppealOutcomeReason").collect()
        results = []
        prefix = "This is a migrated case. The final outcome was "

        for row in rows:
            status = str(row['CaseStatus'])
            outcome = int(row['Outcome'])
            actual_reason = row['endAppealOutcomeReason'] or ""
            
            snippet = reason_mapping.get((status, outcome))
            if not snippet:
                continue
                
            expected_full_reason = f"{prefix}{snippet}."
            
            # We use a clean comparison (removing trailing spaces if any)
            if actual_reason.strip() != expected_full_reason.strip():
                results.append(f"FAIL - {row['appealReferenceNumber']}: Expected '{expected_full_reason}' | Found '{actual_reason}'")

        if results:
            return TestResult("endAppealOutcomeReason", "FAIL", f"Found {len(results)} reason mismatches. Sample: " + " || ".join(results[:3]), "ended", "test1")
        
        return TestResult("endAppealOutcomeReason", "PASS", f"Verified {len(rows)} records; all reason strings match the migrated ARIA format.", "ended", "test1")

    except Exception as e:
        return TestResult("endAppealOutcomeReason", "FAIL", f"EXCEPTION: {str(e)[:200]}", "ended", "test1")





def test_endAppealApproverType_test1(test_df):
    try:
        # Collect relevant columns
        rows = test_df.select("appealReferenceNumber", "CaseStatus", "endAppealApproverType").collect()
        results = []
        
        for row in rows:
            # Ensure CaseStatus is treated as a string for comparison
            status = str(row['CaseStatus'])
            actual_type = row['endAppealApproverType']
            
            # Logic: IF CaseStatus IS 46 = "Judge"; ELSE "Case Worker"
            expected_type = "Judge" if status == '46' else "Case Worker"
            
            if actual_type != expected_type:
                results.append(f"FAIL - {row['appealReferenceNumber']}: Status {status} | Expected '{expected_type}' | Found '{actual_type}'")
        
        count = len(rows)
        if results:
            return TestResult("endAppealApproverType", "FAIL", f"Logic Mismatch found in {len(results)} records. Sample: " + " || ".join(results[:3]), "ended", "test1")
        
        return TestResult("endAppealApproverType", "PASS", f"Verified {count} records; all ApproverTypes correctly assigned (Status 46 as Judge, others as Case Worker).", "ended", "test1")

    except Exception as e:
        return TestResult("endAppealApproverType", "FAIL", f"EXCEPTION: {str(e)[:200]}", "ended", "test1")
    

def test_endAppealApproverName_test1(test_df):
    try:
        rows = test_df.select("appealReferenceNumber", "CaseStatus", "endAppealApproverName",
                               "Adj_Determination_Surname", "Adj_Determination_Forenames", 
                               "Adj_Determination_Title").collect()
        results = []
        for row in rows:
            status = str(row['CaseStatus'])
            actual = row['endAppealApproverName']
            if status == '46':
                s = (row['Adj_Determination_Surname'] or "").strip()
                f = (row['Adj_Determination_Forenames'] or "").strip()
                t = (row['Adj_Determination_Title'] or "").strip()
                # Migration format: "Surname Forenames (Title)"
                expected = f"{s} {f} ({t})".replace("  ", " ").strip()
            else:
                expected = "This is a migrated ARIA case"
            
            if actual != expected:
                results.append(f"FAIL - {row['appealReferenceNumber']}: Expected '{expected}' | Found '{actual}'")
        
        if results:
            return TestResult("endAppealApproverName", "FAIL", f"Mismatches: " + " || ".join(results[:3]), "ended", "test1")
        return TestResult("endAppealApproverName", "PASS", f"Verified {len(rows)} records.", "ended", "test1")
    except Exception as e:
        return TestResult("endAppealApproverName", "FAIL", str(e), "ended", "test1")




def test_endAppealDate_test1(test_df):
    try:
        # Collect columns: appealReferenceNumber, the actual JSON date, and the source ARIA date
        rows = test_df.select(
            "appealReferenceNumber", 
            "endAppealDate", 
            "DecisionDate",
            "CaseStatus",
            "Outcome"
        ).collect()
        
        results = []
        
        for row in rows:
            actual_json_date = row['endAppealDate']
            aria_source_date = row['DecisionDate']
            
            # If the source date is missing in ARIA, we should flag it as an issue 
            # or skip if the business logic allows nulls for certain outcomes
            if aria_source_date is None:
                if actual_json_date is not None:
                    results.append(f"FAIL - {row['appealReferenceNumber']}: ARIA date is NULL but JSON found '{actual_json_date}'")
                continue

            # Convert ARIA Timestamp to ISO 8601 String (YYYY-MM-DD)
            expected_date_str = aria_source_date.strftime('%Y-%m-%d')
            
            # Compare. We check if the expected date string is contained within the actual 
            # (handles cases where JSON might include a timestamp T00:00:00Z)
            if actual_json_date is None or expected_date_str not in actual_json_date:
                results.append(f"FAIL - {row['appealReferenceNumber']}: Source Date '{expected_date_str}' | Found in JSON '{actual_json_date}'")

        count = len(rows)
        if results:
            return TestResult("endAppealDate", "FAIL", f"Date Mismatch in {len(results)} records. Sample: " + " || ".join(results[:3]), "ended", "test1")
        
        return TestResult("endAppealDate", "PASS", f"Verified {count} records; all dates match ARIA DecisionDate in ISO 8601 format.", "ended", "test1")

    except Exception as e:
        return TestResult("endAppealDate", "FAIL", f"EXCEPTION: {str(e)[:200]}", "ended", "test1")
    
def test_stateBeforeEndAppeal_test1(test_df):
    try:
        state_mapping = {
            ('37', 80): "listing", ('38', 80): "listing", ('10', 80): "appealSubmitted",
            ('10', 122): "appealSubmitted", ('51', 94): "pendingPayment", ('37', 13): "listing",
            ('38', 13): "listing", ('37', 25): "listing", ('38', 25): "listing",
            ('39', 25): "ftpaSubmitted", ('10', 25): "appealSubmitted", ('52', 91): "pendingPayment",
            ('52', 95): "pendingPayment", ('51', 93): "pendingPayment", ('38', 72): "listing",
            ('10', 120): "appealSubmitted", ('10', 2): "appealSubmitted", ('46', 31): "appealSubmitted"
        }

        rows = test_df.collect()
        results = []
        for row in rows:
            status, outcome = str(row['CaseStatus']), int(row['Outcome'])
            actual = row['stateBeforeEndAppeal']
            
            if status == '26':
                # DEFENSIVE CHECK: 
                # Handles 'Legal Representative', 'LR', or 'Yes' 
                # Migration logic often looks for any string containing 'Legal'
                rep_value = str(row['Representation'] or "").upper()
                
                if "LEGAL" in rep_value or rep_value == "LR":
                    expected = "caseUnderReview"
                else:
                    expected = "reasonsForAppealSubmitted"
            else:
                expected = state_mapping.get((status, outcome))

            if expected and actual != expected:
                results.append(f"FAIL - {row['appealReferenceNumber']} ({status}/{outcome}): Expected '{expected}' | Found '{actual}' (Rep: {row['Representation']})")

        if results:
            return TestResult("stateBeforeEndAppeal", "FAIL", f"{len(results)} mismatches. Sample: " + " || ".join(results[:3]), "ended", "test1")
        
        return TestResult("stateBeforeEndAppeal", "PASS", f"Verified {len(rows)} records; all states match.", "ended", "test1")
    except Exception as e:
        return TestResult("stateBeforeEndAppeal", "FAIL", str(e), "ended", "test1")
    
