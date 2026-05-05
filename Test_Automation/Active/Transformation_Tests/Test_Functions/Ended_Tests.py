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
        # FAIL omitted fields
        for field in omitted_fields:
            if field in fields_to_exclude: continue
            results_list.append(TestResult(field, "FAIL", "No data to test: Field omitted from ended state", "ended", "DefaultMapping"))


        for field, expected in expected_defaults.items():
            if field in fields_to_exclude or field in omitted_fields: continue
            
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
        # 1. Prep the JSON data
        json_prep = json.select(
            "appealReferenceNumber",
            "outOfTimeDecisionType"
        )

        # 2. Process M3 to get the LATEST status AND the EndedGroup
        history_with_groups = get_ended_group_id(M3_bronze) 

        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        
        # We select EndedGroup and filter for non-null to get ALL categorized cases
        latest_status = history_with_groups.withColumn("rn", F.row_number().over(window_spec)) \
                                           .filter("rn = 1") \
                                           .filter(F.col("EndedGroup").isNotNull()) \
                                           .select("CaseNo", "CaseStatus", "StatusId", "Outcome", "EndedGroup")

        # 3. Master Join: Start with M3 (Source) and Left Join the JSON (Target)
        # This ensures we see cases that SHOULD be there even if they are missing from JSON
        test_df = latest_status.join(
            json_prep,
            latest_status["CaseNo"] == json_prep["appealReferenceNumber"],
            "left"
        ).join(
            M1_bronze.select(F.col("CaseNo").alias("M1_CaseNo")),
            latest_status["CaseNo"] == F.col("M1_CaseNo"),
            "left"
        )

        # Standardize the name for your test functions
        test_df = test_df.withColumnRenamed("CaseNo", "appealReferenceNumber")

        return test_df, True
    except Exception as e:
        return None, TestResult("caseData_init", "FAIL", f"Error: {str(e)[:200]}", "ended", "init")

############################################################################################
# outOfTimeDecisionType - Scenario 1
# IF M3.CaseStatus = 10 and M3.Outcome IN (120, 2, 105) Expected: 'rejected'
############################################################################################
def test_outOfTimeDecisionType_test1(test_df):
    try:
        test_from_state = "ended"
        # Filter for ANY valid Ended Group and the specific 'Rejected' conditions
        target_records = test_df.filter(
            (col("EndedGroup").isNotNull()) & 
            (col("CaseStatus") == 10) &
            (col("Outcome").isin(120, 2, 105))
        )

        if target_records.count() == 0:
            return TestResult("outOfTimeDecisionType", "FAIL", "NO RECORDS TO TEST: No cases across all groups met Rejected criteria", test_from_state, inspect.stack()[0].function)

        # Acceptance Criteria: Must be 'rejected'
        failures = target_records.filter(col("outOfTimeDecisionType") != "rejected")

        if failures.count() > 0:
            return TestResult("outOfTimeDecisionType", "FAIL", f"Scenario 1 FAIL: Found {failures.count()} rows that should be 'rejected' but aren't", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("outOfTimeDecisionType", "PASS", "Scenario 1 PASS: All applicable rows correctly marked as 'rejected'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("outOfTimeDecisionType", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)


############################################################################################
# outOfTimeDecisionType - Scenario 2
# Check where M3.CaseStatus != 10 and M3.Outcome is in (120, 2, 105) Expected: 'approved'
############################################################################################
def test_outOfTimeDecisionType_test2(test_df):
    try:
        test_from_state = "ended"
        # Filter for ANY valid Ended Group
        # Criteria for 'approved': Decision made but NOT meeting the rejected criteria
        target_records = test_df.filter(
            (col("EndedGroup").isNotNull()) &
            ~( (col("CaseStatus") == 10) & (col("Outcome").isin(120, 2, 105)) ) &
            (col("Outcome").isNotNull()) 
        )

        if target_records.count() == 0:
            return TestResult("outOfTimeDecisionType", "FAIL", "NO RECORDS TO TEST: No cases across all groups met Approved criteria", test_from_state, inspect.stack()[0].function)

        # Acceptance Criteria: Must be 'approved'
        failures = target_records.filter(col("outOfTimeDecisionType") != "approved")

        if failures.count() > 0:
            return TestResult("outOfTimeDecisionType", "FAIL", f"Scenario 2 FAIL: Found {failures.count()} rows that should be 'approved' but aren't", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("outOfTimeDecisionType", "PASS", "Scenario 2 PASS: All applicable rows correctly marked as 'approved'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("outOfTimeDecisionType", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)


############################################################################################
# outOfTimeDecisionType - Scenario 3
# Omission Check: Field should be omitted if NO decision exists
############################################################################################
def test_outOfTimeDecisionType_test3(test_df):
    try:
        test_from_state = "ended"
        # Filter for ANY valid Ended Group where no decision was made
        target_records = test_df.filter(
            (col("EndedGroup").isNotNull()) &
            (col("Outcome").isNull())
        )

        if target_records.count() == 0:
            return TestResult("outOfTimeDecisionType", "FAIL", "NO RECORDS TO TEST: All categorised cases have decisions", test_from_state, inspect.stack()[0].function)

        # Acceptance Criteria: Field MUST be NULL (omitted)
        failures = target_records.filter(col("outOfTimeDecisionType").isNotNull())

        if failures.count() > 0:
            return TestResult("outOfTimeDecisionType", "FAIL", f"Scenario 3 FAIL: Found {failures.count()} rows where field is populated but no decision exists", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("outOfTimeDecisionType", "PASS", "Scenario 3 PASS: Field correctly omitted where no decision exists", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("outOfTimeDecisionType", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    
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



def test_isEvidenceFromOutsideUkOoc_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Ended Groups 3 and 4
        # We target these groups specifically as they represent the finalized migration states
        group_records = test_df.filter(col("EndedGroup").isin(3, 4))
        
        if group_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", "NO RECORDS TO TEST: No cases found in EndedGroup 3 or 4", test_from_state, inspect.stack()[0].function)

        # 2. Perform the Conditional OOC Check
        # Criteria: If CategoryId is 38 and Sponsor_Name exists, field must be 'Yes'
        ooc_fail_count = group_records.filter(
            (col("CategoryId") == 38) & 
            (col("Sponsor_Name").isNotNull()) & 
            (col("isEvidenceFromOutsideUkOoc") != "Yes")
        ).count()

        # 3. Generate Result
        if ooc_fail_count == 0:
            return TestResult(
                "isEvidenceFromOutsideUkOoc", 
                "PASS", 
                "Conditional OOC Check passed for Ended Groups 3 & 4", 
                test_from_state, 
                inspect.stack()[0].function
            )
        else:
            return TestResult(
                "isEvidenceFromOutsideUkOoc", 
                "FAIL", 
                f"Conditional OOC Check failed: Found {ooc_fail_count} rows in Group 3/4 where Cat 38 + Sponsor did not result in 'Yes'", 
                test_from_state, 
                inspect.stack()[0].function
            )

    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkOoc", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

def test_isEvidenceFromOutsideUkInCountry_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Ended Groups 3 and 4
        group_records = test_df.filter(col("EndedGroup").isin(3, 4))
        
        if group_records.count() == 0:
            return TestResult("isEvidenceFromOutsideUkInCountry", "FAIL", "NO RECORDS TO TEST: No cases found in EndedGroup 3 or 4", test_from_state, inspect.stack()[0].function)

        # 2. Perform the Conditional InCountry Check
        # Criteria: IF CategoryId is 37 AND Sponsor_Name exists, field must be 'Yes'
        ic_fail_count = group_records.filter(
            (col("CategoryId") == 37) & 
            (col("Sponsor_Name").isNotNull()) & 
            (col("isEvidenceFromOutsideUkInCountry") != "Yes")
        ).count()

        # 3. Generate Result
        if ic_fail_count == 0:
            return TestResult(
                "isEvidenceFromOutsideUkInCountry", 
                "PASS", 
                "Conditional InCountry Check passed for Ended Groups 3 & 4", 
                test_from_state, 
                inspect.stack()[0].function
            )
        else:
            return TestResult(
                "isEvidenceFromOutsideUkInCountry", 
                "FAIL", 
                f"Conditional InCountry Check failed: Found {ic_fail_count} rows in Group 3/4 where Cat 37 + Sponsor did not result in 'Yes'", 
                test_from_state, 
                inspect.stack()[0].function
            )

    except Exception as e:
        return TestResult("isEvidenceFromOutsideUkInCountry", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
def test_isInterpreterServicesNeeded_test1(test_df):
    try:
        # 1. Filter for Group 3/4 and Interpreter = 1
        target_records = test_df.filter(
        (col("EndedGroup").isin(3, 4)) &
        (col("Interpreter") == 1)
        )

        if target_records.count() == 0:
            return TestResult("isInterpreterServicesNeeded", "FAIL", "No records with Interpreter = 1 found.", "ended", inspect.stack()[0].function)

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
            return TestResult("isInterpreterServicesNeeded", "FAIL", "No records with Interpreter != 1 found.", "ended", inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "No"
        failures = target_records.filter(col("isInterpreterServicesNeeded") != "No")

        if failures.count() != 0:
            return TestResult("isInterpreterServicesNeeded", "FAIL", f"Found {failures.count()} rows where Interpreter != 1 was not mapped to 'No'", "ended", inspect.stack()[0].function)
        
        return TestResult("isInterpreterServicesNeeded", "PASS", "Interpreter != 1 correctly mapped to 'No'", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isInterpreterServicesNeeded", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    



from pyspark.sql.functions import col

#######################
# singleSexCourt - Scenario 1
# Check where M1.CourtPreference = 0 and singleSexCourt = No
# Context: EndedGroup 3, 4
#######################
def test_singleSexCourt_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Group 3/4 and target Preference
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) &
            (col("CourtPreference") == 0)
        )

        if target_records.count() == 0:
            return TestResult("singleSexCourt", "FAIL", "DATA DEFICIENCY: No records found with CourtPreference = 0 in EndedGroup 3 or 4", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "No"
        failures = target_records.filter(col("singleSexCourt") != "No")

        if failures.count() != 0:
            return TestResult("singleSexCourt", "FAIL", f"singleSexCourt FAIL: Found {failures.count()} rows where Preference = 0 but singleSexCourt != No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourt", "PASS", f"singleSexCourt PASS: Verified {target_records.count()} records as 'No'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("singleSexCourt", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# singleSexCourt - Scenario 2
# Check where M1.CourtPreference = 1 and singleSexCourt = Yes
# Context: EndedGroup 3, 4
#######################
def test_singleSexCourt_test2(test_df):
    try:
        test_from_state = "ended"
        
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) &
            (col("CourtPreference") == 1)
        )

        if target_records.count() == 0:
            return TestResult("singleSexCourt", "FAIL", "DATA DEFICIENCY: No records found with CourtPreference = 1 in EndedGroup 3 or 4", test_from_state, inspect.stack()[0].function)

        failures = target_records.filter(col("singleSexCourt") != "Yes")

        if failures.count() != 0:
            return TestResult("singleSexCourt", "FAIL", f"singleSexCourt FAIL: Found {failures.count()} rows where Preference = 1 but singleSexCourt != Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourt", "PASS", f"singleSexCourt PASS: Verified {target_records.count()} records as 'Yes'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("singleSexCourt", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# singleSexCourt - Scenario 3
# Check where M1.CourtPreference = 2 and singleSexCourt = Yes
# Context: EndedGroup 3, 4
#######################
def test_singleSexCourt_test3(test_df):
    try:
        test_from_state = "ended"
        
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) &
            (col("CourtPreference") == 2)
        )

        if target_records.count() == 0:
            return TestResult("singleSexCourt", "FAIL", "DATA DEFICIENCY: No records found with CourtPreference = 2 in EndedGroup 3 or 4", test_from_state, inspect.stack()[0].function)

        failures = target_records.filter(col("singleSexCourt") != "Yes")

        if failures.count() != 0:
            return TestResult("singleSexCourt", "FAIL", f"singleSexCourt FAIL: Found {failures.count()} rows where Preference = 2 but singleSexCourt != Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourt", "PASS", f"singleSexCourt PASS: Verified {target_records.count()} records as 'Yes'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("singleSexCourt", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# singleSexCourt - Scenario 4
# Check singleSexCourt is not null
# Context: EndedGroup 3, 4
#######################
def test_singleSexCourt_test4(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for the groups
        target_records = test_df.filter(col("EndedGroup").isin(3, 4))

        if target_records.count() == 0:
            return TestResult("singleSexCourt", "FAIL", "DATA DEFICIENCY: No records found in EndedGroup 3 or 4", test_from_state, inspect.stack()[0].function)

        # 2. Identify any NULL values
        failures = target_records.filter(col("singleSexCourt").isNull())

        if failures.count() != 0:
            return TestResult("singleSexCourt", "FAIL", f"singleSexCourt FAIL: Found {failures.count()} records where singleSexCourt is NULL in Group 3/4", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourt", "PASS", f"singleSexCourt PASS: All {target_records.count()} records in Group 3/4 have a value", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("singleSexCourt", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)



def test_singleSexCourtType_test1(json_data, M1_bronze, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "singleSexCourtType")
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "CourtPreference")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("singleSexCourtType", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup").isin(3, 4)) & (col("CourtPreference") == 1))
        if target_records.count() == 0:
            return TestResult("singleSexCourtType", "FAIL", "No Group 3/4 records with Preference 1.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("singleSexCourtType") != "All male")
        if failures.count() != 0:
            return TestResult("singleSexCourtType", "FAIL", f"Found {failures.count()} rows not mapped to 'All male'", "ended", inspect.stack()[0].function)
        return TestResult("singleSexCourtType", "PASS", "Preference 1 correctly mapped to 'All male'", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("singleSexCourtType", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

def test_singleSexCourtType_test2(json_data, M1_bronze, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "singleSexCourtType")
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "CourtPreference")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("singleSexCourtType", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup").isin(3, 4)) & (col("CourtPreference") == 2))
        if target_records.count() == 0:
            return TestResult("singleSexCourtType", "FAIL", "No Group 3/4 records with Preference 2.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("singleSexCourtType") != "All female")
        if failures.count() != 0:
            return TestResult("singleSexCourtType", "FAIL", f"Found {failures.count()} rows not mapped to 'All female'", "ended", inspect.stack()[0].function)
        return TestResult("singleSexCourtType", "PASS", "Preference 2 correctly mapped to 'All female'", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("singleSexCourtType", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

def test_singleSexCourtType_test3(json_data, M1_bronze, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "singleSexCourtType")
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "CourtPreference")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("singleSexCourtType", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup").isin(3, 4)) & (~col("CourtPreference").isin(1, 2)))
        if target_records.count() == 0:
            return TestResult("singleSexCourtType", "FAIL", "No records found for Omission test (Pref != 1/2).", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("singleSexCourtType").isNotNull())
        if failures.count() != 0:
            return TestResult("singleSexCourtType", "FAIL", f"Found {failures.count()} rows incorrectly included", "ended", inspect.stack()[0].function)
        return TestResult("singleSexCourtType", "PASS", "Field correctly omitted when Preference is not 1 or 2", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("singleSexCourtType", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

#######################
# singleSexCourtTypeDescription - Scenario 1
# IF dbo.CourtPreference IS 1 = Include ARIA Migrated String
# (MAX StatusID where EndedGroup = 3 or 4)
#######################
def test_singleSexCourtTypeDescription_test1(json_data, M1_bronze, M3_bronze):
    try:
        try:
            # CHECK IF singleSexCourtTypeDescription IS IN OUR PAYLOAD 
            # 1. Select JSON fields
            json = json_data.select(
                "appealReferenceNumber",
                "singleSexCourtTypeDescription"
            )

            # 2. Prepare M1_bronze with metadata
            m1_clean = M1_bronze.select(
                col("CaseNo").alias("m1_CaseNo"),
                "CourtPreference"
            )

            # 3. Prepare M3_bronze
            M3_bronze = M3_bronze.select(
                col("CaseNo").alias("m3_CaseNo"),
                "CaseStatus",
                "Outcome"
            )
            
            # 4. Join M3 to test_df to get ended group M3 fields
            unended_test_df = json.join(
                m1_clean,
                json["appealReferenceNumber"] == m1_clean["m1_CaseNo"],
                "inner"
            )

            unended_test_df = unended_test_df.join(
                M3_bronze,
                unended_test_df["appealReferenceNumber"] == M3_bronze["m3_CaseNo"],
                "inner"
            )

            # 5. Make ended group
            test_df = get_ended_group_id(unended_test_df)

            expected_string = "This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court."
        except Exception as e:
            return TestResult("singleSexCourtTypeDescription", "FAIL", f"No data to test: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
        
        # 1. Filter for Group 3/4 and CourtPreference 1
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) & 
            (col("CourtPreference") == 1)
        )
        
        if target_records.count() == 0:
            return TestResult("singleSexCourtTypeDescription", "FAIL", "No records found with Group 3/4 and CourtPreference 1 to test.", test_from_state, inspect.stack()[0].function)

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


def test_singleSexCourtTypeDescription_test2(json_data, M1_bronze, M3_bronze):
    try:
        try:
            # 1. Select JSON fields
            json = json_data.select(
                "appealReferenceNumber",
                "singleSexCourtTypeDescription"
            )

            # 2. Prepare M1_bronze
            m1_clean = M1_bronze.select(
                col("CaseNo").alias("m1_CaseNo"),
                "CourtPreference"
            )

            # 3. Prepare M3_bronze
            m3_clean = M3_bronze.select(
                col("CaseNo").alias("m3_CaseNo"),
                "CaseStatus",
                "Outcome"
            )

            # 4. Join and setup test_df
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            
            test_df = get_ended_group_id(unended_test_df)

            expected_string = "This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court."
        except Exception as e:
            return TestResult("singleSexCourtTypeDescription", "FAIL", f"No data to test: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
        
        # 1. Filter for Group 3/4 and CourtPreference 2
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) &
            (col("CourtPreference") == 2)
        )
        
        if target_records.count() == 0:
            return TestResult("singleSexCourtTypeDescription", "FAIL", "No records found with Group 3/4 and CourtPreference 2 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must match string
        failures = target_records.filter(
            (col("singleSexCourtTypeDescription") != expected_string) |
            (col("singleSexCourtTypeDescription").isNull())
        )

        if failures.count() != 0:
            return TestResult("singleSexCourtTypeDescription", "FAIL", f"Found {failures.count()} rows (Pref 2) where description was incorrect or missing", test_from_state, inspect.stack()[0].function)
        
        return TestResult("singleSexCourtTypeDescription", "PASS", "CourtPreference 2 correctly mapped to ARIA migrated string", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("singleSexCourtTypeDescription", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

def test_singleSexCourtTypeDescription_test3(json_data, M1_bronze, M3_bronze):
    try:
        try:
            # 1. Select JSON fields
            json = json_data.select(
                "appealReferenceNumber",
                "singleSexCourtTypeDescription"
            )

            # 2. Prepare metadata
            m1_clean = M1_bronze.select(
                col("CaseNo").alias("m1_CaseNo"),
                "CourtPreference"
            )

            m3_clean = M3_bronze.select(
                col("CaseNo").alias("m3_CaseNo"),
                "CaseStatus",
                "Outcome"
            )

            # 3. Join and setup test_df
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("singleSexCourtTypeDescription", "FAIL", f"No data to test: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
        
        # 1. Filter for Group 3/4 and CourtPreference NOT 1 or 2
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) &
            (~col("CourtPreference").isin(1, 2))
        )
        
        if target_records.count() == 0:
            return TestResult("singleSexCourtTypeDescription", "FAIL", "No records found with Group 3/4 and CourtPreference != 1 or 2 to test.", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Field must be Null (OMITTED)
        failures = target_records.filter(col("singleSexCourtTypeDescription").isNotNull())

        if failures.count() != 0:
            return TestResult("singleSexCourtTypeDescription", "FAIL", f"Found {failures.count()} rows where description was incorrectly included", test_from_state, inspect.stack()[0].function)
        
        return TestResult("singleSexCourtTypeDescription", "PASS", "Field correctly omitted when CourtPreference is not 1 or 2", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("singleSexCourtTypeDescription", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)



from pyspark.sql.functions import col

#######################
# inCameraCourt - Scenario 1
# Logic: M1.inCamera = 1 -> inCameraCourt = 'Yes'
# Context: EndedGroup 3, 4
#######################
def test_inCameraCourt_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Group 3/4 and target inCamera value
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) &
            (col("inCamera") == 1)
        )

        if target_records.count() == 0:
            return TestResult("inCameraCourt", "FAIL", "DATA DEFICIENCY: No records found with inCamera = 1 in EndedGroup 3 or 4", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "Yes"
        failures = target_records.filter(col("inCameraCourt") != "Yes")

        if failures.count() != 0:
            return TestResult("inCameraCourt", "FAIL", f"inCameraCourt FAIL: Found {failures.count()} rows where inCamera = 1 but inCameraCourt != Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("inCameraCourt", "PASS", f"inCameraCourt PASS: Verified {target_records.count()} records as 'Yes'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("inCameraCourt", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# inCameraCourt - Scenario 2
# Logic: M1.inCamera = 0 -> inCameraCourt = 'No'
# Context: EndedGroup 3, 4
#######################
def test_inCameraCourt_test2(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Group 3/4 and target inCamera value
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) &
            (col("inCamera") == 0)
        )

        if target_records.count() == 0:
            return TestResult("inCameraCourt", "FAIL", "DATA DEFICIENCY: No records found with inCamera = 0 in EndedGroup 3 or 4", test_from_state, inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must be "No"
        failures = target_records.filter(col("inCameraCourt") != "No")

        if failures.count() != 0:
            return TestResult("inCameraCourt", "FAIL", f"inCameraCourt FAIL: Found {failures.count()} rows where inCamera = 0 but inCameraCourt != No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("inCameraCourt", "PASS", f"inCameraCourt PASS: Verified {target_records.count()} records as 'No'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("inCameraCourt", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# inCameraCourt - Scenario 3
# Check inCameraCourt is not null
# Context: EndedGroup 3, 4
#######################
def test_inCameraCourt_test3(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for the groups
        target_records = test_df.filter(col("EndedGroup").isin(3, 4))

        if target_records.count() == 0:
            return TestResult("inCameraCourt", "FAIL", "DATA DEFICIENCY: No records found in EndedGroup 3 or 4", test_from_state, inspect.stack()[0].function)

        # 2. Identify any NULL values
        failures = target_records.filter(col("inCameraCourt").isNull())

        if failures.count() != 0:
            return TestResult("inCameraCourt", "FAIL", f"inCameraCourt FAIL: Found {failures.count()} records where inCameraCourt is NULL in Group 3/4", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("inCameraCourt", "PASS", f"inCameraCourt PASS: All {target_records.count()} records in Group 3/4 have a value", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("inCameraCourt", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

def test_hearingResponse_init(json_data, M1_bronze, M3_bronze, bac, M6_bronze, M1_silver, M2_bronze):
    try:
        # 1. Selection & Lifting: Extract nested FTPA data to root-level columns
        # This ensures 'ftpaAppellantApplicationDate' actually contains the data from the array
        test_df = json_data.withColumn(
            "ftpaAppellantApplicationDate", 
            F.col("ftpaList").getItem(0).getItem("value").getItem("ftpaApplicationDate")
        ).withColumn(
            "ftpaAppellantOutOfTimeExplanation", 
            F.col("ftpaList").getItem(0).getItem("value").getItem("ftpaOutOfTimeExplanation")
        ).select(
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
            "ftpaApplicationDeadline",
            "ftpaList",
            "ftpaAppellantApplicationDate", # Now contains lifted data
            "ftpaAppellantSubmissionOutOfTime",
            "ftpaAppellantOutOfTimeExplanation" # Now contains lifted data
        )

        # 2. Setup Bronze dependencies
        m1_clean = M1_bronze.select(
            F.col("CaseNo").alias("m1_CaseNo"),
            "Sponsor_Name", "Interpreter", "CourtPreference", "InCamera", "VisitVisaType"
        )

        m2_clean = M2_bronze.select(
            F.col("CaseNo").alias("M2_CaseNo"),
            "Appellant_Name"
        )

        m1_silver_clean = M1_silver.select(
            F.col("CaseNo").alias("m1_silver_CaseNo"),
            "dv_representation"
        )

        bac_clean = bac.select(F.col("CaseNo").alias("bac_CaseNo"), "CategoryId")

        m6_clean = M6_bronze.select(F.col("CaseNo").alias("m6_CaseNo")).groupBy("m6_CaseNo").count().select("m6_CaseNo")

        # 3. Process M3 History & Ended Groups
        m3_history = M3_bronze.join(bac_clean, M3_bronze["CaseNo"] == bac_clean["bac_CaseNo"], "left")
        history_with_groups = get_ended_group_id(m3_history)

        history_with_max_group = history_with_groups.withColumn(
            "FinalEndedGroup", 
            F.max("EndedGroup").over(Window.partitionBy("CaseNo"))
        )

        # 4. Judge Name Priority Logic
        history_ranked_names = history_with_max_group.withColumn(
            "name_priority",
            F.when(F.col("CaseStatus").isin(37, 38), 1)
             .when(F.col("CaseStatus") == 39, 2)
             .otherwise(3)
        )

        judge_name_window = Window.partitionBy("CaseNo").orderBy(F.col("name_priority").asc(), F.col("StatusId").desc())

        history_with_names = history_ranked_names.withColumn(
            "Adj_Determination_Title", F.first("Adj_Determination_Title", True).over(judge_name_window)
        ).withColumn(
            "Adj_Determination_Forenames", F.first("Adj_Determination_Forenames", True).over(judge_name_window)
        ).withColumn(
            "Adj_Determination_Surname", F.first("Adj_Determination_Surname", True).over(judge_name_window)
        )

        # 5. Filter for the latest relevant "Ended" status row
        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        
        latest_status = history_with_names \
            .filter(F.col("CaseStatus").isin(37, 38, 39)) \
            .withColumn("rn", F.row_number().over(window_spec)) \
            .filter("rn = 1") \
            .select(
                "CaseNo", "CaseStatus", "StatusId", "Party", "CategoryId", 
                F.col("FinalEndedGroup").alias("EndedGroup"), 
                "ListTypeId", "TimeEstimate", "HearingDate", "StartTime", "HearingCentre",
                "Outcome", "DecisionDate", "DateReceived",
                "Adj_Determination_Title", "Adj_Determination_Forenames", "Adj_Determination_Surname",
                "HearingDuration", 
                F.col("OutOfTime").cast("string").alias("OutOfTime") # Cast to string for easier comparison
            )

        # 6. Master Join (Switched metadata joins to LEFT to prevent data loss)
        final_test_df = test_df.join(
            m1_clean,
            test_df["appealReferenceNumber"] == m1_clean["m1_CaseNo"],
            "inner"
        ).join(
            m1_silver_clean,
            test_df["appealReferenceNumber"] == m1_silver_clean["m1_silver_CaseNo"],
            "left" # Changed from inner
        ).join(
            m2_clean,
            test_df["appealReferenceNumber"] == m2_clean["M2_CaseNo"],
            "left" # Changed from inner
        ).join(
            latest_status,
            test_df["appealReferenceNumber"] == latest_status["CaseNo"],
            "left"
        ).join(
            m6_clean,
            test_df["appealReferenceNumber"] == m6_clean["m6_CaseNo"],
            "left"
        ).drop("m1_CaseNo", "m1_silver_CaseNo", "CaseNo", "bac_CaseNo", "m6_CaseNo")

        return final_test_df, True

    except Exception as e:
        return None, TestResult("HearingResponse_Init", "FAIL", f"Setup Error: {str(e)[:400]}", "ended", "init")

#######################
# isAppealSuitableToFloat - Scenario 1
# If M3.ListTypeId is 5, value should be ‘Yes’ (MAX StatusId in EndedGroup 4)
#######################
def test_isAppealSuitableToFloat_test1(test_df):
    try:
        test_from_state = "ended"
        # 1. Filter for EndedGroup 4 and relevant statuses
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus").isin(37, 38, 39))
        )
        
        if target_records.count() == 0:
            return TestResult("isAppealSuitableToFloat", "FAIL", "NO RECORDS TO TEST (No Group 4 with Status 37/38/39 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        # 3. Acceptance Criteria
        acceptance_criteria = winning_records.filter(
            (col("ListTypeId") == 5) &
            (col("isAppealSuitableToFloat") != "Yes")
        )

        if acceptance_criteria.count() != 0:
            return TestResult("isAppealSuitableToFloat","FAIL", f"Scenario 1 FAIL: found {acceptance_criteria.count()} rows where ListTypeId is 5 but value is not 'Yes'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isAppealSuitableToFloat","PASS", "Scenario 1 PASS: All ListTypeId 5 records are 'Yes'", test_from_state, inspect.stack()[0].function)
    
    except Exception as e:
        return TestResult("isAppealSuitableToFloat", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# isAppealSuitableToFloat - Scenario 2
# If M3.ListTypeId is not 5, value should be 'No' (MAX StatusId in EndedGroup 4)
#######################
def test_isAppealSuitableToFloat_test2(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus").isin(37, 38, 39))
        )
        
        if target_records.count() == 0:
            return TestResult("isAppealSuitableToFloat", "FAIL", "NO RECORDS TO TEST (No Group 4 with Status 37/38/39 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        # 3. Acceptance Criteria
        acceptance_criteria = winning_records.filter(
            (col("ListTypeId") != 5) &
            (col("isAppealSuitableToFloat") != "No")
        )

        if acceptance_criteria.count() != 0:
            return TestResult("isAppealSuitableToFloat","FAIL", f"Scenario 2 FAIL: found {acceptance_criteria.count()} rows where ListTypeId is not 5 but value is not 'No'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isAppealSuitableToFloat","PASS", "Scenario 2 PASS: All non-ListTypeId 5 records are 'No'", test_from_state, inspect.stack()[0].function)
            
    except Exception as e:
        return TestResult("isAppealSuitableToFloat", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# isAppealSuitableToFloat - Scenario 3
# Field must only contain ‘Yes’ or ‘No’ (No Nulls or unexpected strings in EndedGroup 4)
#######################
def test_isAppealSuitableToFloat_test3(test_df):
    try:
        test_from_state = "ended"
        # Filter for Group 4
        group_df = test_df.filter(col("EndedGroup") == 4)

        if group_df.count() == 0:
            return TestResult("isAppealSuitableToFloat", "FAIL", "NO RECORDS TO TEST in Group 4", test_from_state, inspect.stack()[0].function)

        # Identify rows that are NOT "Yes" or "No"
        invalid_rows = group_df.filter(
            ~col("isAppealSuitableToFloat").isin("Yes", "No")
        )

        if invalid_rows.count() != 0:
            return TestResult("isAppealSuitableToFloat","FAIL", f"Scenario 3 FAIL: found {invalid_rows.count()} rows that are not 'Yes' or 'No' (includes Nulls)", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isAppealSuitableToFloat","PASS", "Scenario 3 PASS: All Group 4 rows are correctly 'Yes' or 'No'", test_from_state, inspect.stack()[0].function)
            
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
            return TestResult("listingLength.hours", "FAIL", "No Group 4 records found.", "ended", inspect.stack()[0].function)

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
            return TestResult("listingLength.minutes", "FAIL", "No Group 4 records found.", "ended", inspect.stack()[0].function)

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
            return TestResult("hearingChannel", "FAIL", "No records found where VisitVisaType is 1.", "ended", "test_hearingChannel_test1")

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
            return TestResult("hearingChannel", "FAIL", "No records found where VisitVisaType is 2.", "ended", "test_hearingChannel_test2")

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
    

from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

############################################################################################
# listingLocation
# Logic: Verify that when ListedCentre exists, the listingLocation object matches
# Context: EndedGroup 4 AND MAX StatusID WHERE CaseStatus is 37 or 38
############################################################################################
def test_listingLocation_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Group 4 and relevant Statuses (37, 38)
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus").isin(37, 38))
        )

        if target_records.count() == 0:
            return TestResult("listingLocation", "FAIL", "DATA DEFICIENCY: No Status 37/38 records found in EndedGroup 4", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        # Included TimeEstimate in order as per your original logic for tie-breaking
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(
            col("StatusId").desc(), 
            col("TimeEstimate").desc()
        )

        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Collapse/Aggregate to prepare for comparison
        collapsed_df = winning_records.groupBy("appealReferenceNumber").agg(
            F.max("listingLocation.code").alias("actual_code"),
            F.max("listingLocation.label").alias("actual_label"),
            F.max("locationCode").alias("expected_code"),
            F.max("locationLabel").alias("expected_label"),
            F.max("ListedCentre").alias("ListedCentre")
        )

        # 4. Acceptance Criteria: Check for mismatches only where ListedCentre is populated
        # This filters for Party-specific records that should have a location
        failures = collapsed_df.filter(
            (col("ListedCentre").isNotNull()) & 
            (
                (col("actual_code") != col("expected_code")) | 
                (col("actual_label") != col("expected_label"))
            )
        )

        if failures.count() > 0:
            return TestResult("listingLocation", "FAIL", f"listingLocation FAIL: Found {failures.count()} records where actual listingLocation does not match expected source location", test_from_state, inspect.stack()[0].function)
        else:
            # Check if we actually tested any populated ListedCentres
            tested_count = collapsed_df.filter(col("ListedCentre").isNotNull()).count()
            if tested_count == 0:
                return TestResult("listingLocation", "FAIL", "DATA DEFICIENCY: Records found, but none had a populated ListedCentre to verify", test_from_state, inspect.stack()[0].function)
            
            return TestResult("listingLocation", "PASS", f"listingLocation PASS: Verified {tested_count} records have matching location codes and labels", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("listingLocation", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

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
            return TestResult("listCaseHearingLength", "FAIL", "No records found for Group 4.", "ended", "test_listCaseHearingLength_test1")

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
            return TestResult("listCaseHearingDate", "FAIL", "No records found.", "ended", "test_listCaseHearingDate_test1")

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
            return TestResult("listCaseHearingCentre", "FAIL", "No records found.", "ended", "test_listCaseHearingCentre_test1")

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
            "Yarl's Wood": ["yarlsWood", "Yarls Wood Immigration And Asylum Hearing Centre, Twinwood Road, MK44 1FD"]
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
            return TestResult("listCaseHearingCentreAddress", "FAIL", "No records found.", "ended", "test_listCaseHearingCentreAddress_test1")

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
    
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

#######################
# sendDecisionsAndReasonsDate
# Logic: Ensure M3.DecisionDate and sendDecisionsAndReasonsDate are equal
# Context: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus IN (37, 38, 26) AND Outcome IN (1, 2)
#######################
def test_sendDecisionsAndReasonsDate_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Group 4, relevant CaseStatuses, and specific Outcomes
        target_records = test_df.filter(
            (col("EndedGroup") == 4) &
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1, 2))
        )

        if target_records.count() == 0:
            return TestResult("sendDecisionsAndReasonsDate", "FAIL", "DATA DEFICIENCY: No records found with Status 37/38/26 and Outcome 1/2 in EndedGroup 4", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusID").desc())

        # Get the latest Record per Case
        winning_records = target_records.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)

        # 3. Acceptance Criteria: sendDecisionsAndReasonsDate must match DecisionDate
        # Note: We filter for rows where they are NOT equal to find failures
        failures = winning_records.filter(
            col("sendDecisionsAndReasonsDate") != col("DecisionDate")
        )

        if failures.count() > 0:
            return TestResult("sendDecisionsAndReasonsDate", "FAIL", f"sendDecisionsAndReasonsDate FAIL: Found {failures.count()} mismatches between DecisionDate and sendDecisionsAndReasonsDate", test_from_state, inspect.stack()[0].function)
        else:
            success_count = winning_records.count()
            return TestResult("sendDecisionsAndReasonsDate", "PASS", f"sendDecisionsAndReasonsDate PASS: Verified {success_count} records where dates correctly match", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("sendDecisionsAndReasonsDate", "FAIL", f"EXCEPTION: Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

#######################
# appealDate
# Logic: Ensure M3.DecisionDate and appealDate are equal
# Context: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus IN (37, 38, 26) AND Outcome IN (1, 2)
#######################
def test_appealDate_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Group 4, relevant CaseStatuses, and specific Outcomes
        target_records = test_df.filter(
            (col("EndedGroup") == 4) &
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1, 2))
        )

        if target_records.count() == 0:
            return TestResult("appealDate", "FAIL", "DATA DEFICIENCY: No records found with Status 37/38/26 and Outcome 1/2 in EndedGroup 4", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusID").desc())

        # Get the latest Record per Case
        winning_records = target_records.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)

        # 3. Acceptance Criteria: appealDate must match DecisionDate
        failures = winning_records.filter(
            col("appealDate") != col("DecisionDate")
        )

        if failures.count() > 0:
            return TestResult("appealDate", "FAIL", f"appealDate FAIL: Found {failures.count()} mismatches between DecisionDate and appealDate", test_from_state, inspect.stack()[0].function)
        else:
            success_count = winning_records.count()
            return TestResult("appealDate", "PASS", f"appealDate PASS: Verified {success_count} records where appealDate correctly matches DecisionDate", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("appealDate", "FAIL", f"EXCEPTION: Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

############################################################################################
# appealDecision - Scenario 1
# Logic: M3.Outcome = 1 -> appealDecision = 'Allowed'
# Context: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus IN (37,38,26)
############################################################################################
def test_appealDecision_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Group 4 and relevant decision statuses
        target_records = test_df.filter(
            (col("EndedGroup") == 4) &
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1, 2))
        )

        if target_records.count() == 0:
            return TestResult("appealDecision", "FAIL", "DATA DEFICIENCY: No records found with Status 37/38/26 in EndedGroup 4", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusID").desc())
        winning_records = target_records.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)

        # 3. Acceptance Criteria: Outcome 1 must be 'Allowed'
        outcome_1_records = winning_records.filter(col("Outcome") == 1)
        
        if outcome_1_records.count() == 0:
            return TestResult("appealDecision", "FAIL", "DATA DEFICIENCY: No Outcome 1 records found to verify 'Allowed' mapping", test_from_state, inspect.stack()[0].function)

        failures = outcome_1_records.filter(col("appealDecision") != "Allowed")

        if failures.count() > 0:
            return TestResult("appealDecision", "FAIL", f"appealDecision FAIL: Found {failures.count()} cases where Outcome = 1 but decision != 'Allowed'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("appealDecision", "PASS", f"appealDecision PASS: Verified {outcome_1_records.count()} 'Allowed' mappings", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("appealDecision", "FAIL", f"EXCEPTION: Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

############################################################################################
# appealDecision - Scenario 2
# Logic: M3.Outcome = 2 -> appealDecision = 'Dismissed'
# Context: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus IN (37,38,26)
############################################################################################
def test_appealDecision_test2(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Group 4 and relevant decision statuses
        target_records = test_df.filter(
            (col("EndedGroup") == 4) &
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1, 2))
        )

        if target_records.count() == 0:
            return TestResult("appealDecision", "FAIL", "DATA DEFICIENCY: No records found with Status 37/38/26 in EndedGroup 4", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusID").desc())
        winning_records = target_records.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)

        # 3. Acceptance Criteria: Outcome 2 must be 'Dismissed'
        outcome_2_records = winning_records.filter(col("Outcome") == 2)
        
        if outcome_2_records.count() == 0:
            return TestResult("appealDecision", "FAIL", "DATA DEFICIENCY: No Outcome 2 records found to verify 'Dismissed' mapping", test_from_state, inspect.stack()[0].function)

        failures = outcome_2_records.filter(col("appealDecision") != "Dismissed")

        if failures.count() > 0:
            return TestResult("appealDecision", "FAIL", f"appealDecision FAIL: Found {failures.count()} cases where Outcome = 2 but decision != 'Dismissed'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("appealDecision", "PASS", f"appealDecision PASS: Verified {outcome_2_records.count()} 'Dismissed' mappings", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("appealDecision", "FAIL", f"EXCEPTION: Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

############################################################################################
# isDecisionAllowed - Scenario 1
# Logic: M3.Outcome = 1 -> isDecisionAllowed = 'allowed'
# Context: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus IN (37,38,26)
############################################################################################
def test_isDecisionAllowed_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Group 4 and relevant decision statuses
        target_records = test_df.filter(
            (col("EndedGroup") == 4) &
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1, 2))
        )

        if target_records.count() == 0:
            return TestResult("isDecisionAllowed", "FAIL", "DATA DEFICIENCY: No records found with Status 37/38/26 in EndedGroup 4", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusID").desc())
        winning_records = target_records.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)

        # 3. Acceptance Criteria: Outcome 1 must be 'allowed'
        outcome_1_records = winning_records.filter(col("Outcome") == 1)
        
        if outcome_1_records.count() == 0:
            return TestResult("isDecisionAllowed", "FAIL", "DATA DEFICIENCY: No Outcome 1 records found in Group 4", test_from_state, inspect.stack()[0].function)

        failures = outcome_1_records.filter(col("isDecisionAllowed") != "allowed")

        if failures.count() > 0:
            return TestResult("isDecisionAllowed", "FAIL", f"isDecisionAllowed FAIL: Found {failures.count()} cases where Outcome = 1 but field != 'allowed'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isDecisionAllowed", "PASS", f"isDecisionAllowed PASS: Verified {outcome_1_records.count()} 'allowed' mappings", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isDecisionAllowed", "FAIL", f"EXCEPTION: Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

############################################################################################
# isDecisionAllowed - Scenario 2
# Logic: M3.Outcome = 2 -> isDecisionAllowed = 'dismissed'
# Context: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus IN (37,38,26)
############################################################################################
def test_isDecisionAllowed_test2(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Group 4 and relevant decision statuses
        target_records = test_df.filter(
            (col("EndedGroup") == 4) &
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1, 2))
        )

        if target_records.count() == 0:
            return TestResult("isDecisionAllowed", "FAIL", "DATA DEFICIENCY: No records found with Status 37/38/26 in EndedGroup 4", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusID").desc())
        winning_records = target_records.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)

        # 3. Acceptance Criteria: Outcome 2 must be 'dismissed'
        outcome_2_records = winning_records.filter(col("Outcome") == 2)
        
        if outcome_2_records.count() == 0:
            return TestResult("isDecisionAllowed", "FAIL", "DATA DEFICIENCY: No Outcome 2 records found in Group 4", test_from_state, inspect.stack()[0].function)

        failures = outcome_2_records.filter(col("isDecisionAllowed") != "dismissed")

        if failures.count() > 0:
            return TestResult("isDecisionAllowed", "FAIL", f"isDecisionAllowed FAIL: Found {failures.count()} cases where Outcome = 2 but field != 'dismissed'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isDecisionAllowed", "PASS", f"isDecisionAllowed PASS: Verified {outcome_2_records.count()} 'dismissed' mappings", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isDecisionAllowed", "FAIL", f"EXCEPTION: Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
#######################
# attendingJudge - Scenario 1
# Check concatenation of Title, Forenames, and Surname (Group 4)
# Expected format: "Title Forenames Surname"
#######################
def test_attendingJudge_test1(test_df):
    try:
        # Filter: EndedGroup 4 (Scenario requires largest StatusID, which is handled in Init)
        target_records = test_df.filter(col("EndedGroup") == 4)
        
        if target_records.count() == 0:
            return TestResult("attendingJudge", "FAIL", "No EndedGroup 4 records found.", "ended", inspect.stack()[0].function)

        # 1. Use coalesce to turn nulls into empty strings so concatenation doesn't result in NULL
        # 2. Use concat_ws to handle spaces between parts automatically
        # 3. Use trim to clean up any leading/trailing spaces if one of the parts is missing
        expected_df = target_records.withColumn("expected_judge", 
            F.trim(
                F.concat_ws(" ", 
                    F.coalesce(col("Adj_Determination_Title").cast("string"), F.lit("")),
                    F.coalesce(col("Adj_Determination_Forenames").cast("string"), F.lit("")),
                    F.coalesce(col("Adj_Determination_Surname").cast("string"), F.lit(""))
                )
            )
        )

        # Compare Actual vs Expected
        failures = expected_df.filter(col("attendingJudge") != col("expected_judge"))

        if failures.count() != 0:
            sample = failures.select("appealReferenceNumber", "attendingJudge", "expected_judge").limit(1).collect()
            return TestResult(
                "attendingJudge", 
                "FAIL", 
                f"Found {failures.count()} rows with mismatch. Case {sample[0][0]} Actual: '{sample[0][1]}' vs Expected: '{sample[0][2]}'", 
                "ended", 
                inspect.stack()[0].function
            )
        
        return TestResult("attendingJudge", "PASS", "attendingJudge correctly concatenated from Title, Forenames, and Surname", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("attendingJudge", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

############################################################################################
# actualCaseHearingLength
# Logic: Convert M3.HearingDuration (Total Minutes) to { hours, minutes } struct
# Context: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus IN (37,38,26) AND Outcome IN (1,2)
############################################################################################
def test_actualCaseHearingLength_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Group 4, relevant CaseStatuses, and specific Outcomes
        target_records = test_df.filter(
            (col("EndedGroup") == 4) &
            (col("CaseStatus").isin(37, 38, 26)) & 
            (col("Outcome").isin(1, 2))
        )

        if target_records.count() == 0:
            return TestResult("actualCaseHearingLength", "FAIL", "DATA DEFICIENCY: No records found with Status 37/38/26 in EndedGroup 4", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusID").desc())
        winning_records = target_records.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)

        # 3. Process actual and expected values
        # Extract from destination struct
        winning_records = winning_records.withColumn(
            "actual_hours", col("actualCaseHearingLength").getField("hours").cast("int")
        ).withColumn(
            "actual_minutes", col("actualCaseHearingLength").getField("minutes").cast("int")
        )

        # Calculate expected values from source total minutes
        # floor(65 / 60) = 1 hour; 65 % 60 = 5 minutes
        winning_records = winning_records.withColumn(
            "expected_hours", F.floor(col("HearingDuration") / 60).cast("int")
        ).withColumn(
            "expected_minutes", (col("HearingDuration") % 60).cast("int")
        )

        # 4. Validation - Check for mismatches
        # We only validate where HearingDuration is not null to avoid false failures on missing data
        failures = winning_records.filter(
            (col("HearingDuration").isNotNull()) & 
            (
                (col("actual_hours") != col("expected_hours")) | 
                (col("actual_minutes") != col("expected_minutes"))
            )
        )

        if failures.count() > 0:
            return TestResult("actualCaseHearingLength", "FAIL", f"actualCaseHearingLength FAIL: Found {failures.count()} cases where struct hours/minutes do not match M3.HearingDuration", test_from_state, inspect.stack()[0].function)
        else:
            success_count = winning_records.filter(col("HearingDuration").isNotNull()).count()
            if success_count == 0:
                 return TestResult("actualCaseHearingLength", "FAIL", "DATA DEFICIENCY: records found but none have HearingDuration populated", test_from_state, inspect.stack()[0].function)
            
            return TestResult("actualCaseHearingLength", "PASS", f"actualCaseHearingLength PASS: Verified {success_count} records correctly match M3.HearingDuration", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("actualCaseHearingLength", "FAIL", f"EXCEPTION: Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    
#######################
# isInCameraCourtAllowed
#######################

def test_isInCameraCourtAllowed_test1(json_data, M1_bronze, M3_bronze):
    try:
        try:
            # 1. Select JSON fields
            json = json_data.select("appealReferenceNumber", "isInCameraCourtAllowed")
            # 2. Prepare Metadata (InCamera is in M1)
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "InCamera")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            # 3. Joins
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            # 4. Process Ended Group
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isInCameraCourtAllowed", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        # Scenario 1: Include (Granted) if InCamera IS 1 and Group 4
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("InCamera") == 1))
        
        if target_records.count() == 0:
            return TestResult("isInCameraCourtAllowed", "FAIL", "No records found with Group 4 and InCamera=1.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "isInCameraCourtAllowed").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Found '{r['isInCameraCourtAllowed']}'" for r in rows if r['isInCameraCourtAllowed'] != "Granted"]

        if results_list:
            return TestResult("isInCameraCourtAllowed", "FAIL", "Inclusion failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("isInCameraCourtAllowed", "PASS", "Correctly included 'Granted' for InCamera=1", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isInCameraCourtAllowed", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

def test_isInCameraCourtAllowed_test2(json_data, M1_bronze, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isInCameraCourtAllowed")
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "InCamera")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isInCameraCourtAllowed", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        # Scenario 2: Omit if InCamera IS NOT 1
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("InCamera") != 1))
        
        if target_records.count() == 0:
            return TestResult("isInCameraCourtAllowed", "FAIL", "No records found for Omission test (InCamera != 1).", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "isInCameraCourtAllowed").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Field found" for r in rows if r['isInCameraCourtAllowed'] is not None]

        if results_list:
            return TestResult("isInCameraCourtAllowed", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("isInCameraCourtAllowed", "PASS", "Correctly omitted for InCamera!=1", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isInCameraCourtAllowed", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
#######################
# inCameraCourtTribunalResponse
#######################

def test_inCameraCourtTribunalResponse_test1(json_data, M1_bronze, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "inCameraCourtTribunalResponse")
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "InCamera")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
            expected = "This is a migrated ARIA case. Please refer to the documents."
        except Exception as e:
            return TestResult("inCameraCourtTribunalResponse", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("InCamera") == 1))
        
        if target_records.count() == 0:
            return TestResult("inCameraCourtTribunalResponse", "FAIL", "No Group 4 records with InCamera=1.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "inCameraCourtTribunalResponse").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Found '{r['inCameraCourtTribunalResponse']}'" for r in rows if r['inCameraCourtTribunalResponse'] != expected]

        if results_list:
            return TestResult("inCameraCourtTribunalResponse", "FAIL", "Mapping failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("inCameraCourtTribunalResponse", "PASS", "Correct string included for InCamera=1", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("inCameraCourtTribunalResponse", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

def test_inCameraCourtTribunalResponse_test2(json_data, M1_bronze, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "inCameraCourtTribunalResponse")
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "InCamera")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("inCameraCourtTribunalResponse", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("InCamera") != 1))
        
        if target_records.count() == 0:
            return TestResult("inCameraCourtTribunalResponse", "FAIL", "No records found for Omission test.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "inCameraCourtTribunalResponse").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Field found" for r in rows if r['inCameraCourtTribunalResponse'] is not None]

        if results_list:
            return TestResult("inCameraCourtTribunalResponse", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("inCameraCourtTribunalResponse", "PASS", "Correctly omitted for InCamera!=1", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("inCameraCourtTribunalResponse", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
#######################
# inCameraCourtDecisionForDisplay
#######################

def test_inCameraCourtDecisionForDisplay_test1(json_data, M1_bronze, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "inCameraCourtDecisionForDisplay")
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "InCamera")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
            expected = "Granted - This is a migrated ARIA case. Please refer to the documents."
        except Exception as e:
            return TestResult("inCameraCourtDecisionForDisplay", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("InCamera") == 1))
        
        if target_records.count() == 0:
            return TestResult("inCameraCourtDecisionForDisplay", "FAIL", "No Group 4 records with InCamera=1.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "inCameraCourtDecisionForDisplay").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Found '{r['inCameraCourtDecisionForDisplay']}'" for r in rows if r['inCameraCourtDecisionForDisplay'] != expected]

        if results_list:
            return TestResult("inCameraCourtDecisionForDisplay", "FAIL", "Mapping failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("inCameraCourtDecisionForDisplay", "PASS", "Correct string included for InCamera=1", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("inCameraCourtDecisionForDisplay", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

def test_inCameraCourtDecisionForDisplay_test2(json_data, M1_bronze, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "inCameraCourtDecisionForDisplay")
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "InCamera")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("inCameraCourtDecisionForDisplay", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("InCamera") != 1))
        
        if target_records.count() == 0:
            return TestResult("inCameraCourtDecisionForDisplay", "FAIL", "No records found for Omission test.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "inCameraCourtDecisionForDisplay").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Field found" for r in rows if r['inCameraCourtDecisionForDisplay'] is not None]

        if results_list:
            return TestResult("inCameraCourtDecisionForDisplay", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("inCameraCourtDecisionForDisplay", "PASS", "Correctly omitted for InCamera!=1", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("inCameraCourtDecisionForDisplay", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)



def test_inCameraCourtDescription_test1(json_data, M1_bronze, M3_bronze):
    try:
        try:
            # 1. Select JSON fields
            json = json_data.select(
                "appealReferenceNumber",
                "inCameraCourtDescription"
            )

            # 2. Prepare M1_bronze (Source of InCamera flag)
            m1_clean = M1_bronze.select(
                col("CaseNo").alias("m1_CaseNo"),
                "InCamera"
            )

            # 3. Prepare M3_bronze
            m3_clean = M3_bronze.select(
                col("CaseNo").alias("m3_CaseNo"),
                "CaseStatus"
            )

            # 4. Join and setup test_df
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            
            test_df = get_ended_group_id(unended_test_df)

            expected_string = "This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the appellants need for an in camera court."
        except Exception as e:
            return TestResult("inCameraCourtDescription", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)
        
        # 1. Filter for Ended Group 3/4 and InCamera = 1
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) &
            (col("inCameraCourtDescription") == 1)
        )
        
        if target_records.count() == 0:
            return TestResult("inCameraCourtDescription", "FAIL", "No records found with Group 3/4 and InCamera=1 to test.", "ended", inspect.stack()[0].function)

        # 2. Acceptance Criteria: Must match the hardcoded ARIA string
        rows = target_records.select("appealReferenceNumber", "inCameraCourtDescription").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: String mismatch or Null" for r in rows if r['inCameraCourtDescription'] != expected_string]

        if results_list:
            return TestResult("inCameraCourtDescription", "FAIL", "Mapping failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        
        return TestResult("inCameraCourtDescription", "PASS", "InCamera=1 correctly mapped to ARIA migrated description", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("inCameraCourtDescription", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


def test_inCameraCourtDescription_test2(json_data, M1_bronze, M3_bronze):
    try:
        try:
            # 1. Select JSON fields
            json = json_data.select(
                "appealReferenceNumber",
                "inCameraCourtDescription"
            )

            # 2. Prepare Metadata
            m1_clean = M1_bronze.select(
                col("CaseNo").alias("m1_CaseNo"),
                "InCamera"
            )

            m3_clean = M3_bronze.select(
                col("CaseNo").alias("m3_CaseNo"),
                "CaseStatus"
            )

            # 3. Join and setup test_df
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("inCameraCourtDescription", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)
        
        # 1. Filter for Ended Group 3/4 and InCamera != 1
        target_records = test_df.filter(
            (col("EndedGroup").isin(3, 4)) &
            (col("inCameraCourtDescription") != 1)
        )
        
        if target_records.count() == 0:
            return TestResult("inCameraCourtDescription", "FAIL", "No records found for Omission test (InCamera != 1).", "ended", inspect.stack()[0].function)

        # 2. Acceptance Criteria: Field must be Null
        rows = target_records.select("appealReferenceNumber", "inCameraCourtDescription").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Key found unexpectedly" for r in rows if r['inCameraCourtDescription'] is not None]

        if results_list:
            return TestResult("inCameraCourtDescription", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        
        return TestResult("inCameraCourtDescription", "PASS", "Field correctly omitted when InCamera is not 1", "ended", inspect.stack()[0].function)

    except Exception as e:
        return TestResult("inCameraCourtDescription", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)




def test_isSingleSexCourtAllowed_test1(json_data, M1_bronze, M3_bronze):
    test_from_state = "ended"
    try:
        try:
            # 1. Select JSON fields
            json_df = json_data.select("appealReferenceNumber", "isSingleSexCourtAllowed")

            # 2. Prepare M1_bronze
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "CourtPreference")

            # 3. Prepare M3_bronze
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Outcome")
            
            # 4. Join
            unended_test_df = json_df.join(m1_clean, json_df["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")

            # 5. Make ended group
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isSingleSexCourtAllowed", "FAIL", f"No data to test: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
        
        # Filter for Group 4 and Preference 1 or 2
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CourtPreference").isin(1, 2)))
        
        if target_records.count() == 0:
            return TestResult("isSingleSexCourtAllowed", "FAIL", "No records found with Group 4 and Preference 1/2 to test.", test_from_state, inspect.stack()[0].function)

        # Acceptance Criteria: Must match "Granted"
        failures = target_records.filter(col("isSingleSexCourtAllowed") != "Granted")

        if failures.count() != 0:
            return TestResult("isSingleSexCourtAllowed", "FAIL", f"Inclusion failures: Found {failures.count()} rows where value was not 'Granted'", test_from_state, inspect.stack()[0].function)
        
        return TestResult("isSingleSexCourtAllowed", "PASS", "Correctly included 'Granted' for Preference 1/2", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isSingleSexCourtAllowed", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

def test_isSingleSexCourtAllowed_test2(json_data, M1_bronze, M3_bronze):
    test_from_state = "ended"
    try:
        try:
            # 1. Select JSON fields
            json_df = json_data.select("appealReferenceNumber", "isSingleSexCourtAllowed")

            # 2. Prepare M1_bronze
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "CourtPreference")

            # 3. Prepare M3_bronze
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Outcome")
            
            # 4. Join
            unended_test_df = json_df.join(m1_clean, json_df["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")

            # 5. Make ended group
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isSingleSexCourtAllowed", "FAIL", f"No data to test: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
        
        # Filter for Group 4 and Preference NOT 1 or 2
        target_records = test_df.filter((col("EndedGroup") == 4) & (~col("CourtPreference").isin(1, 2)))
        
        if target_records.count() == 0:
            return TestResult("isSingleSexCourtAllowed", "FAIL", "No records found with Group 4 and Preference != 1/2 to test.", test_from_state, inspect.stack()[0].function)

        # Acceptance Criteria: Must be Omitted (Null)
        failures = target_records.filter(col("isSingleSexCourtAllowed").isNotNull())

        if failures.count() != 0:
            return TestResult("isSingleSexCourtAllowed", "FAIL", f"Omission failures: Found {failures.count()} rows where field should be omitted", test_from_state, inspect.stack()[0].function)
        
        return TestResult("isSingleSexCourtAllowed", "PASS", "Correctly omitted for Preference != 1/2", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isSingleSexCourtAllowed", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# singleSexCourtTribunalResponse
#######################

def test_singleSexCourtTribunalResponse_test1(json_data, M1_bronze, M3_bronze):
    try:
        try:
            # 1. Select JSON fields
            json = json_data.select("appealReferenceNumber", "singleSexCourtTribunalResponse")
            # 2. Prepare Metadata
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "CourtPreference")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            # 3. Join and setup test_df
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
            expected = "This is a migrated ARIA case. Please refer to the documents."
        except Exception as e:
            return TestResult("singleSexCourtTribunalResponse", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CourtPreference").isin(1, 2)))
        if target_records.count() == 0:
            return TestResult("singleSexCourtTribunalResponse", "FAIL", "No records found with Group 4 and CourtPreference 1/2.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "singleSexCourtTribunalResponse").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Found '{r['singleSexCourtTribunalResponse']}'" for r in rows if r['singleSexCourtTribunalResponse'] != expected]

        if results_list:
            return TestResult("singleSexCourtTribunalResponse", "FAIL", "Mapping failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("singleSexCourtTribunalResponse", "PASS", "Correct string included for Preference 1/2", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("singleSexCourtTribunalResponse", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

def test_singleSexCourtTribunalResponse_test2(json_data, M1_bronze, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "singleSexCourtTribunalResponse")
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "CourtPreference")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("singleSexCourtTribunalResponse", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (~col("CourtPreference").isin(1, 2)))
        if target_records.count() == 0:
            return TestResult("singleSexCourtTribunalResponse", "FAIL", "No records found for Omission test (Pref != 1/2).", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "singleSexCourtTribunalResponse").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Field found" for r in rows if r['singleSexCourtTribunalResponse'] is not None]

        if results_list:
            return TestResult("singleSexCourtTribunalResponse", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("singleSexCourtTribunalResponse", "PASS", "Correctly omitted for Preference != 1/2", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("singleSexCourtTribunalResponse", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
#######################
# singleSexCourtDecisionForDisplay
#######################

def test_singleSexCourtDecisionForDisplay_test1(json_data, M1_bronze, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "singleSexCourtDecisionForDisplay")
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "CourtPreference")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
            expected = "Granted - This is a migrated ARIA case. Please refer to the documents."
        except Exception as e:
            return TestResult("singleSexCourtDecisionForDisplay", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CourtPreference").isin(1, 2)))
        if target_records.count() == 0:
            return TestResult("singleSexCourtDecisionForDisplay", "FAIL", "No Group 4 records with Preference 1/2.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "singleSexCourtDecisionForDisplay").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Found '{r['singleSexCourtDecisionForDisplay']}'" for r in rows if r['singleSexCourtDecisionForDisplay'] != expected]

        if results_list:
            return TestResult("singleSexCourtDecisionForDisplay", "FAIL", "Mapping failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("singleSexCourtDecisionForDisplay", "PASS", "Correct string included for Preference 1/2", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("singleSexCourtDecisionForDisplay", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

def test_singleSexCourtDecisionForDisplay_test2(json_data, M1_bronze, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "singleSexCourtDecisionForDisplay")
            m1_clean = M1_bronze.select(col("CaseNo").alias("m1_CaseNo"), "CourtPreference")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus")
            unended_test_df = json.join(m1_clean, json["appealReferenceNumber"] == m1_clean["m1_CaseNo"], "inner")
            unended_test_df = unended_test_df.join(m3_clean, unended_test_df["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("singleSexCourtDecisionForDisplay", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (~col("CourtPreference").isin(1, 2)))
        if target_records.count() == 0:
            return TestResult("singleSexCourtDecisionForDisplay", "FAIL", "No records found for Omission test.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "singleSexCourtDecisionForDisplay").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Field found" for r in rows if r['singleSexCourtDecisionForDisplay'] is not None]

        if results_list:
            return TestResult("singleSexCourtDecisionForDisplay", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("singleSexCourtDecisionForDisplay", "PASS", "Correctly omitted for Preference != 1/2", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("singleSexCourtDecisionForDisplay", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

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
# ftpaApplicationDeadline - Combined Ended State
# Where CategoryId in 37, ftpaApplicationDeadline = M3.DecisionDate + 14 days
# Where CategoryId in 38, ftpaApplicationDeadline = M3.DecisionDate + 28 days
# Filtered for EndedGroup 4 and relevant Status/Outcome
#######################
def test_ftpaApplicationDeadline_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Ended Group 4 and business conditions
        # We include Status 39 as it represents the 'Ended' row in your current data
        target_records = test_df.filter(
            (col("EndedGroup") == 4) &
            (col("CaseStatus").isin(37, 38, 26, 39)) & 
            (col("Outcome").isin(1, 2)) &
            (col("CategoryId").isin(37, 38))
        )

        # Check we have records to test
        if target_records.count() == 0:
            return TestResult("ftpaApplicationDeadline", "FAIL", "NO RECORDS TO TEST (No Decision/FTPA in EndedGroup 4)", test_from_state, inspect.stack()[0].function)

        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_df = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        # 3. Calculate expected date based on CategoryId
        # This simplifies the logic by creating a single 'expected' column
        winning_df = winning_df.withColumn(
            "expected_date", 
            F.when(col("CategoryId") == 37, F.date_add(F.to_date("DecisionDate"), 14))
             .when(col("CategoryId") == 38, F.date_add(F.to_date("DecisionDate"), 28))
        )

        # 4. Acceptance Criteria
        # Fail if the field is NULL or the dates don't match exactly
        acceptance_criteria = winning_df.filter(
            (col("ftpaApplicationDeadline").isNull()) | 
            (F.to_date("ftpaApplicationDeadline") != col("expected_date"))
        )

        if acceptance_criteria.count() > 0:
            return TestResult("ftpaApplicationDeadline", "FAIL", f"ftpaApplicationDeadline failed: found {acceptance_criteria.count()} rows with missing or incorrect deadline math", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaApplicationDeadline", "PASS", "ftpaApplicationDeadline passed: all deadlines match M3.DecisionDate + required days", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaApplicationDeadline", "FAIL", f"TEST FAILED WITH EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

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
            return TestResult("ftpaList", "FAIL", "No CaseStatus 39 records found.", "ended", inspect.stack()[0].function)

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



    
def test_ftpaRespondentApplicationDate_test1(json_data, M3_bronze):
    try:
        try:
            # 1. Select JSON fields
            json = json_data.select("appealReferenceNumber", "ftpaList")
            # 2. Prepare Metadata (Filter for Status 39 / Party 2)
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            # 3. Joins
            unended_test_df = json.join(m3_clean, json["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            # 4. Process Ended Group
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("ftpaRespondentApplicationDate", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        # Scenario 1: Include if Party=2 and Status=39
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2))
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentApplicationDate", "FAIL", "No records found for Group 4 / Status 39 / Party 2.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = []
        for row in rows:
            ftpa_val = row['ftpaList'][0]["value"].asDict() if row['ftpaList'] else {}
            if "ftpaRespondentApplicationDate" not in ftpa_val:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Key missing for Party 2")
            elif ftpa_val["ftpaRespondentApplicationDate"] is None:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Value is null")

        if results_list:
            return TestResult("ftpaRespondentApplicationDate", "FAIL", "Inclusion failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentApplicationDate", "PASS", "Field correctly included for Party 2", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentApplicationDate", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

def test_ftpaRespondentApplicationDate_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "ftpaList")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3_clean, json["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("ftpaRespondentApplicationDate", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        # Scenario 2: Omit if Party=1
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 1))
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentApplicationDate", "FAIL", "No records found for Party 1 omission test.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Key found for Party 1" for r in rows if r['ftpaList'] and "ftpaRespondentApplicationDate" in r['ftpaList'][0]["value"].asDict()]

        if results_list:
            return TestResult("ftpaRespondentApplicationDate", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentApplicationDate", "PASS", "Field correctly omitted for Party 1", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentApplicationDate", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

def test_ftpaRespondentSubmissionOutOfTime_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "ftpaList")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3_clean, json["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2))
        if target_records.count() == 0:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", "No records found for Party 2 mapping.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "OutOfTime", "ftpaList").collect()
        results_list = []
        for row in rows:
            expected = "Yes" if row['OutOfTime'] == 1 else "No"
            ftpa_val = row['ftpaList'][0]["value"].asDict() if row['ftpaList'] else {}
            if "ftpaRespondentSubmissionOutOfTime" not in ftpa_val:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Key missing")
            elif ftpa_val["ftpaRespondentSubmissionOutOfTime"] != expected:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Expected {expected}, found {ftpa_val['ftpaRespondentSubmissionOutOfTime']}")

        if results_list:
            return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", "Mapping failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentSubmissionOutOfTime", "PASS", "Field correctly mapped for Party 2", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentSubmissionOutOfTime", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)


def test_ftpaRespondentOutOfTimeExplanation_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "ftpaList")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3_clean, json["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
            expected_str = "This is a migrated ARIA case. Please refer to the documents."
        except Exception as e:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        # Target: Party 2 (Respondent) and OutOfTime (1)
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("OutOfTime") == 1) & (col("Party") == 2))
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "No records found for Scenario 1 (Party 2 & OutOfTime 1).", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = []
        for row in rows:
            ftpa_val = row['ftpaList'][0]["value"].asDict() if row['ftpaList'] else {}
            if "ftpaRespondentOutOfTimeExplanation" not in ftpa_val:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: Key missing")
            elif ftpa_val["ftpaRespondentOutOfTimeExplanation"] != expected_str:
                results_list.append(f"FAIL - {row['appealReferenceNumber']}: String mismatch")

        if results_list:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "Inclusion failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "Scenario 1: Correct string included for Party 2/OutOfTime 1", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    

#######################
# ftpaRespondentOutOfTimeExplanation - Scenario 2
# Check: M3.OutOfTime = 1 AND M3.Party != 2 -> MUST be omitted
#######################
def test_ftpaRespondentOutOfTimeExplanation_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "ftpaList")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3_clean, json["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        # Target: Not Party 2 (e.g. Appellant) but is OutOfTime
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("OutOfTime") == 1) & (col("Party") != 2))
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "No records found for Scenario 2 (Party != 2).", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Key found for Party {r['Party']}" for r in rows if r['ftpaList'] and "ftpaRespondentOutOfTimeExplanation" in r['ftpaList'][0]["value"].asDict()]

        if results_list:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "Scenario 2: Correctly omitted for Party != 2", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


#######################
# ftpaRespondentOutOfTimeExplanation - Scenario 3
# Check: M3.OutOfTime != 1 AND M3.Party = 2 -> MUST be omitted
#######################
def test_ftpaRespondentOutOfTimeExplanation_test3(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "ftpaList")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3_clean, json["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        # Target: Party 2 but was In-Time
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("OutOfTime") != 1) & (col("Party") == 2))
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "No records found for Scenario 3 (In-Time).", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Key found for In-Time case" for r in rows if r['ftpaList'] and "ftpaRespondentOutOfTimeExplanation" in r['ftpaList'][0]["value"].asDict()]

        if results_list:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "Scenario 3: Correctly omitted for In-Time case", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)



#######################
# ftpaRespondentOutOfTimeExplanation - Scenario 4
# Check: M3.OutOfTime != 1 AND M3.Party != 2 -> MUST be omitted
#######################
def test_ftpaRespondentOutOfTimeExplanation_test4(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "ftpaList")
            m3_clean = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3_clean, json["appealReferenceNumber"] == m3_clean["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        # Target: Everything else (Party 1 and In-Time)
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("OutOfTime") != 1) & (col("Party") != 2))
        
        if target_records.count() == 0:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "No records found for Scenario 4.", "ended", inspect.stack()[0].function)

        rows = target_records.select("appealReferenceNumber", "ftpaList").collect()
        results_list = [f"FAIL - {r['appealReferenceNumber']}: Key found" for r in rows if r['ftpaList'] and "ftpaRespondentOutOfTimeExplanation" in r['ftpaList'][0]["value"].asDict()]

        if results_list:
            return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", "Omission failures: " + "|||".join(results_list), "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentOutOfTimeExplanation", "PASS", "Scenario 4: Correctly omitted", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)
    


####################################################   


from pyspark.sql import functions as F
from pyspark.sql.window import Window
def test_general_init(json_data, M1_bronze, M2_bronze, M3_bronze, M1_silver, bac):
    try:
        # 1. Selection from JSON
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
            "bundleFileNamePrefix"
        ]

        available_fields = [col for col in target_fields if col in json_data.columns]
        test_df = json_data.select(*available_fields)

        # 2. Process M3 History with EndedGroup Logic
        # We need the bac table to identify Categories for EndedGroups
        bac_clean = bac.select(F.col("CaseNo").alias("bac_CaseNo"), "CategoryId")
        
        m3_history = M3_bronze.join(bac_clean, M3_bronze["CaseNo"] == bac_clean["bac_CaseNo"], "left")
        
        # Apply the grouping logic (ensure this function is available in your notebook)
        history_with_groups = get_ended_group_id(m3_history)

        # Calculate the Max Group and Priority
        window_max = Window.partitionBy("CaseNo")
        history_with_max_group = history_with_groups.withColumn(
            "EndedGroup", F.max("EndedGroup").over(window_max)
        )

        m3_priority = history_with_max_group.withColumn(
            "priority", 
            F.when(F.col("CaseStatus").isin(37, 38), 1)
             .when(F.col("CaseStatus") == 39, 2)
             .otherwise(3)
        )

        window_latest = Window.partitionBy("CaseNo").orderBy(F.col("priority").asc(), F.col("StatusId").desc())
        
        m3_latest = m3_priority.withColumn("rn", F.row_number().over(window_latest)) \
            .filter(F.col("rn") == 1) \
            .select("CaseNo", "CaseStatus", "StatusId", "Party", "OutOfTime", "EndedGroup")

        # 3. Bronze Preps
        m1_prep = M1_bronze.select(
            F.col("CaseNo").alias("M1_Join_Key"),
            F.regexp_replace(F.col("CaseNo"), "/", " ").alias("Formatted_CaseNo")
        )
        m2_prep = M2_bronze.select(F.col("CaseNo").alias("M2_Join_Key"), "Appellant_Name")
        m1_silver_prep = M1_silver.select(F.col("CaseNo").alias("Silver_Join_Key"), F.col("dv_representation").alias("Representation"))

        # 4. Master Join (Left joins for metadata to prevent data loss)
        test_df = test_df.join(m3_latest, test_df["appealReferenceNumber"] == m3_latest["CaseNo"], "inner") \
                         .join(m1_prep, test_df["appealReferenceNumber"] == m1_prep["M1_Join_Key"], "left") \
                         .join(m2_prep, test_df["appealReferenceNumber"] == m2_prep["M2_Join_Key"], "left") \
                         .join(m1_silver_prep, test_df["appealReferenceNumber"] == m1_silver_prep["Silver_Join_Key"], "left")

        # 5. Expected Bundle Prefix Calculation
        test_df = test_df.withColumn(
            "expected_bundle_prefix", 
            F.concat(F.col("Formatted_CaseNo"), F.lit("-"), F.col("Appellant_Name"))
        )

        return test_df.drop("CaseNo", "M1_Join_Key", "M2_Join_Key", "Silver_Join_Key", "bac_CaseNo"), True

    except Exception as e:
        return None, TestResult("general_init", "FAIL", f"Failed: {str(e)[:200]}", "ended", "init")

#######################
# bundleFileNamePrefix - Concatenate format 'CaseNo-Appellant_Name'
# Format: Replace '/' in CaseNo with ' ' (space)
# State: Ended (Group 4)
#######################
def test_bundleFileNamePrefix_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for the specific Ended Group
        # We also filter for where CaseStatus is 39 (or 37/38) as these are the typical "Ended" rows
        target_records = test_df.filter(col("EndedGroup") == 4)

        # 2. Check we have Records to test in this group
        if target_records.count() == 0:
            return TestResult("bundleFileNamePrefix", "FAIL", "NO RECORDS TO TEST: No cases found in EndedGroup 4", test_from_state, inspect.stack()[0].function)
        
        # 3. Calculate the expected format
        # regexp_replace turns "EA/02495/2024" into "EA 02495 2024"
        final_df = target_records.withColumn(
            "expected_prefix",
            F.concat(
                F.regexp_replace(F.col("appealReferenceNumber"), "/", " "),
                F.lit("-"),
                F.col("Appellant_Name")
            )
        )

        # 4. Acceptance Criteria: Find any mismatch
        # Note: If Appellant_Name is NULL, the concat result will be NULL, which is a failure
        acceptance_criteria = final_df.filter(
            (col("bundleFileNamePrefix") != col("expected_prefix")) |
            (col("bundleFileNamePrefix").isNull())
        )

        if acceptance_criteria.count() != 0:
            return TestResult("bundleFileNamePrefix", "FAIL", f"bundleFileNamePrefix failed: found {acceptance_criteria.count()} rows where prefix does not match 'CaseNo-Appellant_Name' format", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("bundleFileNamePrefix", "PASS", "bundleFileNamePrefix passed: all Group 4 rows match the required naming convention", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("bundleFileNamePrefix", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# ftpaAppellantSubmitted - Scenario 1
# IF M3.Party IS 1 = Include 'Yes'
# MAX(StatusId) WHERE CaseStatus = 39 in EndedGroup 4
#######################
def test_ftpaAppellantSubmitted_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Ended Group 4 and CaseStatus 39
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmitted", "FAIL", "DATA DEFICIENCY: No Status 39 records found in EndedGroup 4", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", F.row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 1 must have 'Yes'
        # We filter for Party 1 first to check if any exist
        party1_records = winning_records.filter(col("Party") == 1)
        p1_count = party1_records.count()
        
        if p1_count == 0:
            return TestResult("ftpaAppellantSubmitted", "FAIL", "DATA DEFICIENCY: Found Status 39 in Group 4, but 0 records are Party 1", test_from_state, inspect.stack()[0].function)

        failures = party1_records.filter(col("ftpaAppellantSubmitted") != "Yes")

        if failures.count() != 0:
            return TestResult("ftpaAppellantSubmitted", "FAIL", f"ftpaAppellantSubmitted FAIL: Found {failures.count()} Party 1 cases where value is not 'Yes'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantSubmitted", "PASS", f"ftpaAppellantSubmitted PASS: Verified {p1_count} Party 1 records as 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantSubmitted - Scenario 2
# IF M3.Party IS 2 = OMIT (NULL)
# MAX(StatusId) WHERE CaseStatus = 39 in EndedGroup 4
#######################
def test_ftpaAppellantSubmitted_test2(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Ended Group 4 and CaseStatus 39
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmitted", "FAIL", "DATA DEFICIENCY: No Status 39 records found in EndedGroup 4", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", F.row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 2 must be OMITTED (NULL)
        party2_records = winning_records.filter(col("Party") == 2)
        p2_count = party2_records.count()

        if p2_count == 0:
            return TestResult("ftpaAppellantSubmitted", "FAIL", "DATA DEFICIENCY: Found Status 39 in Group 4, but 0 records are Party 2", test_from_state, inspect.stack()[0].function)

        failures = party2_records.filter(col("ftpaAppellantSubmitted").isNotNull())

        if failures.count() != 0:
            return TestResult("ftpaAppellantSubmitted", "FAIL", f"ftpaAppellantSubmitted FAIL: Found {failures.count()} Party 2 cases where field was not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantSubmitted", "PASS", f"ftpaAppellantSubmitted PASS: Verified {p2_count} Party 2 records are correctly omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# isFtpaAppellantDocsVisibleInDecided - Scenario 1
# IF M3.Party IS 1 = Include 'No'
# MAX(StatusId) WHERE CaseStatus = 39 in EndedGroup 4
#######################
def test_isFtpaAppellantDocsVisibleInDecided_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Ended Group 4 and CaseStatus 39
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Status 39 records found in EndedGroup 4", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", F.row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 1 must have 'No'
        # Filtering strictly for Party 1 as requested
        party1_records = winning_records.filter(col("Party") == 1)
        p1_count = party1_records.count()
        
        if p1_count == 0:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: Found Status 39 in Group 4, but 0 records are Party 1", test_from_state, inspect.stack()[0].function)

        failures = party1_records.filter(col("isFtpaAppellantDocsVisibleInDecided") != "No")

        if failures.count() != 0:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", f"Scenario 1 FAIL: Found {failures.count()} Party 1 cases where value is not 'No'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "PASS", f"Scenario 1 PASS: Verified {p1_count} Party 1 records as 'No'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# isFtpaAppellantDocsVisibleInDecided - Scenario 2
# IF M3.Party IS 2 = OMIT (NULL)
# MAX(StatusId) WHERE CaseStatus = 39 in EndedGroup 4
#######################
def test_isFtpaAppellantDocsVisibleInDecided_test2(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Ended Group 4 and CaseStatus 39
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Status 39 records found in EndedGroup 4", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", F.row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 2 must be OMITTED (NULL)
        party2_records = winning_records.filter(col("Party") == 2)
        p2_count = party2_records.count()

        if p2_count == 0:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Party 2 records found in Group 4/Status 39", test_from_state, inspect.stack()[0].function)

        # Fail if the field IS NOT NULL for Party 2
        failures = party2_records.filter(col("isFtpaAppellantDocsVisibleInDecided").isNotNull())

        if failures.count() != 0:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", f"Scenario 2 FAIL: Found {failures.count()} Party 2 cases where field was not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantDocsVisibleInDecided", "PASS", f"Scenario 2 PASS: Verified {p2_count} Party 2 records are correctly omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

############################################################################################
# isFtpaAppellantDocsVisibleInSubmitted - Scenario 1
# IF M3.Party IS 1 = Include 'Yes'
# Must be MAX(StatusId) WHERE CaseStatus = 39 AND EndedGroup = 4
############################################################################################
def test_isFtpaAppellantDocsVisibleInSubmitted_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Ended Group 4 and CaseStatus 39
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No Status 39 records found in EndedGroup 4", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 1 must have 'Yes'
        party1_records = winning_records.filter(col("Party") == 1)
        p1_count = party1_records.count()
        
        if p1_count == 0:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: Found Status 39/Group 4, but 0 records are Party 1", test_from_state, inspect.stack()[0].function)

        # Fail if value is NOT 'Yes'
        failures = party1_records.filter(col("isFtpaAppellantDocsVisibleInSubmitted") != "Yes")

        if failures.count() != 0:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", f"Scenario 1 FAIL: Found {failures.count()} Party 1 rows where value is not 'Yes'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "PASS", f"Scenario 1 PASS: Verified {p1_count} Party 1 records as 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

############################################################################################
# isFtpaAppellantDocsVisibleInSubmitted - Scenario 2
# IF M3.Party IS 2 = OMIT (NULL)
# Must be MAX(StatusId) WHERE CaseStatus = 39 AND EndedGroup = 4
############################################################################################
def test_isFtpaAppellantDocsVisibleInSubmitted_test2(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for Ended Group 4 and CaseStatus 39
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No Status 39 records found in EndedGroup 4", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 2 must be OMITTED (NULL)
        party2_records = winning_records.filter(col("Party") == 2)
        p2_count = party2_records.count()

        if p2_count == 0:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: Found Status 39/Group 4, but 0 records are Party 2", test_from_state, inspect.stack()[0].function)

        # Fail if the field IS NOT NULL for Party 2
        failures = party2_records.filter(col("isFtpaAppellantDocsVisibleInSubmitted").isNotNull())

        if failures.count() != 0:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", f"Scenario 2 FAIL: Found {failures.count()} Party 2 rows where field was not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "PASS", f"Scenario 2 PASS: Verified {p2_count} Party 2 records are correctly omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)






#######################
# isFtpaAppellantOotDocsVisibleInDecided
# Logic: IF M3.Party IS 1 AND M3.OutOfTime IS 1 = "No"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

############################################################################################
# Scenario 1: P1 & OOT == 1 -> 'No'
############################################################################################
def test_isFtpaAppellantOotDocsVisibleInDecided_test1(test_df):
    try:
        test_from_state = "ended"
        # 1. Filter for Ended Group 4 and CaseStatus 39
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Status 39 in Group 4", test_from_state, inspect.stack()[0].function)
        
        # 2. Get Max Status record
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 1 and OutOfTime == 1 must be 'No'
        scenario_df = winning_records.filter((col("Party") == 1) & (col("OutOfTime") == 1))
        
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Party 1 OOT records found", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotDocsVisibleInDecided") != "No")
        
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"FAIL: {failures.count()} OOT rows not 'No'", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "PASS", f"PASS: Verified {scenario_df.count()} OOT records as 'No'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
# Scenario 2: P1 & OOT != 1 -> OMIT (NULL)
############################################################################################
def test_isFtpaAppellantOotDocsVisibleInDecided_test2(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # Check for Party 1 and NOT OutOfTime
        scenario_df = winning_records.filter((col("Party") == 1) & (col("OutOfTime") != 1))
        
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Party 1 In-Time records found", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotDocsVisibleInDecided").isNotNull())
        
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"FAIL: {failures.count()} In-Time rows not omitted", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "PASS", "PASS: All In-Time records correctly omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
# Scenario 3: P2 & OOT == 1 -> OMIT (NULL)
############################################################################################
def test_isFtpaAppellantOotDocsVisibleInDecided_test3(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # Check for Party 2 and OutOfTime == 1
        scenario_df = winning_records.filter((col("Party") == 2) & (col("OutOfTime") == 1))
        
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Party 2 OOT records found", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotDocsVisibleInDecided").isNotNull())
        
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"FAIL: {failures.count()} Party 2 OOT rows not omitted", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "PASS", "PASS: Party 2 OOT correctly omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
# Scenario 4: P2 & OOT != 1 -> OMIT (NULL)
############################################################################################
def test_isFtpaAppellantOotDocsVisibleInDecided_test4(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # Check for Party 2 and NOT OutOfTime
        scenario_df = winning_records.filter((col("Party") == 2) & (col("OutOfTime") != 1))
        
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Party 2 In-Time records found", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotDocsVisibleInDecided").isNotNull())
        
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"FAIL: {failures.count()} Party 2 In-Time rows not omitted", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "PASS", "PASS: Party 2 In-Time correctly omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)



from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

############################################################################################
# isFtpaAppellantOotDocsVisibleInSubmitted 1: P1 & OOT == 1 -> 'Yes'
############################################################################################
def test_isFtpaAppellantOotDocsVisibleInSubmitted_test1(test_df):
    try:
        test_from_state = "ended"
        # 1. Filter for Group 4 and Status 39
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No Status 39 in Group 4", test_from_state, inspect.stack()[0].function)
        
        # 2. Get Max Status record
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Check for P1 and OutOfTime == 1
        scenario_df = winning_records.filter((col("Party") == 1) & (col("OutOfTime") == 1))
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No Party 1 OOT records", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotDocsVisibleInSubmitted") != "Yes")
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"FAIL: {failures.count()} OOT rows not 'Yes'", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "PASS", f"PASS: Verified {scenario_df.count()} OOT records as 'Yes'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
# Scenario 2: P1 & OOT != 1 -> OMIT (NULL)
############################################################################################
def test_isFtpaAppellantOotDocsVisibleInSubmitted_test2(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # Check for P1 and NOT OutOfTime
        scenario_df = winning_records.filter((col("Party") == 1) & (col("OutOfTime") != 1))
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No Party 1 In-Time records", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotDocsVisibleInSubmitted").isNotNull())
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"FAIL: {failures.count()} In-Time rows not omitted", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "PASS", "PASS: All In-Time records correctly omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
# Scenario 3: P2 & OOT == 1 -> OMIT (NULL)
############################################################################################
def test_isFtpaAppellantOotDocsVisibleInSubmitted_test3(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # Check for P2 and OutOfTime == 1
        scenario_df = winning_records.filter((col("Party") == 2) & (col("OutOfTime") == 1))
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No Party 2 OOT records", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotDocsVisibleInSubmitted").isNotNull())
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"FAIL: {failures.count()} Party 2 OOT rows not omitted", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "PASS", "PASS: Party 2 OOT correctly omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
# Scenario 4: P2 & OOT != 1 -> OMIT (NULL)
############################################################################################
def test_isFtpaAppellantOotDocsVisibleInSubmitted_test4(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # Check for P2 and NOT OutOfTime
        scenario_df = winning_records.filter((col("Party") == 2) & (col("OutOfTime") != 1))
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No Party 2 In-Time records", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotDocsVisibleInSubmitted").isNotNull())
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"FAIL: {failures.count()} Party 2 In-Time rows not omitted", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "PASS", "PASS: Party 2 In-Time correctly omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# isFtpaAppellantGroundsDocsVisibleInDecided
# Logic: IF M3.Party IS 1 = "No"; ELSE IF M3.Party IS 2 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

############################################################################################
# Scenario 1: Party 1 -> 'No'
# Logic: MAX(StatusId) WHERE CaseStatus = 39 AND EndedGroup = 4
############################################################################################
def test_isFtpaAppellantGroundsDocsVisibleInDecided_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for the specific group and status
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Status 39 in Group 4 found", test_from_state, inspect.stack()[0].function)
        
        # 2. Get latest record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 1 must be 'No'
        p1_records = winning_records.filter(col("Party") == 1)
        p1_count = p1_records.count()
        
        if p1_count == 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Party 1 records found in Group 4/Status 39", test_from_state, inspect.stack()[0].function)

        failures = p1_records.filter(col("isFtpaAppellantGroundsDocsVisibleInDecided") != "No")

        if failures.count() > 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", f"FAIL: Found {failures.count()} Party 1 rows where value is not 'No'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "PASS", f"PASS: Verified {p1_count} Party 1 records as 'No'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

############################################################################################
# Scenario 2: Party 2 -> OMIT (NULL)
# Logic: MAX(StatusId) WHERE CaseStatus = 39 AND EndedGroup = 4
############################################################################################
def test_isFtpaAppellantGroundsDocsVisibleInDecided_test2(test_df):
    try:
        test_from_state = "ended"
        
        # 1. Filter for the specific group and status
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Status 39 in Group 4 found", test_from_state, inspect.stack()[0].function)
        
        # 2. Get latest record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 2 must be OMITTED
        p2_records = winning_records.filter(col("Party") == 2)
        p2_count = p2_records.count()

        if p2_count == 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Party 2 records found in Group 4/Status 39", test_from_state, inspect.stack()[0].function)

        # Fail if the field IS NOT NULL for Party 2
        failures = p2_records.filter(col("isFtpaAppellantGroundsDocsVisibleInDecided").isNotNull())

        if failures.count() > 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", f"FAIL: Found {failures.count()} Party 2 rows that were not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "PASS", f"PASS: Verified {p2_count} Party 2 records correctly omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantGroundsDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)




#######################
# isFtpaAppellantEvidenceDocsVisibleInDecided
# Logic: IF M3.Party IS 1 = "No"; ELSE IF M3.Party IS 2 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

############################################################################################
# Scenario 1: Party 1 -> 'No'
# Filter: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus 39
############################################################################################
def test_isFtpaAppellantEvidenceDocsVisibleInDecided_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. universe: Group 4 and Status 39
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Status 39 in Group 4 found", test_from_state, inspect.stack()[0].function)
        
        # 2. Get latest record
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: P1 must be 'No'
        p1_records = winning_records.filter(col("Party") == 1)
        p1_count = p1_records.count()
        
        if p1_count == 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: Found Group 4/Status 39, but 0 records are Party 1", test_from_state, inspect.stack()[0].function)

        failures = p1_records.filter(col("isFtpaAppellantEvidenceDocsVisibleInDecided") != "No")

        if failures.count() > 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", f"FAIL: Found {failures.count()} Party 1 rows not marked 'No'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "PASS", f"PASS: Verified {p1_count} Party 1 records as 'No'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

############################################################################################
# Scenario 2: Party 2 -> OMIT (NULL)
# Filter: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus 39
############################################################################################
def test_isFtpaAppellantEvidenceDocsVisibleInDecided_test2(test_df):
    try:
        test_from_state = "ended"
        
        # 1. universe: Group 4 and Status 39
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Status 39 in Group 4 found", test_from_state, inspect.stack()[0].function)
        
        # 2. Get latest record
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: P2 must be OMITTED
        p2_records = winning_records.filter(col("Party") == 2)
        p2_count = p2_records.count()

        if p2_count == 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", "DATA DEFICIENCY: Found Group 4/Status 39, but 0 records are Party 2", test_from_state, inspect.stack()[0].function)

        # Fail if field is populated (isNotNull)
        failures = p2_records.filter(col("isFtpaAppellantEvidenceDocsVisibleInDecided").isNotNull())

        if failures.count() > 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", f"FAIL: Found {failures.count()} Party 2 rows not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "PASS", f"PASS: Verified {p2_count} Party 2 records correctly omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantEvidenceDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)



#######################
# isFtpaAppellantGroundsDocsVisibleInSubmitted
# Logic: IF M3.Party IS 1 = "Yes"; ELSE IF M3.Party IS 2 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

############################################################################################
# Scenario 1: Party 1 -> 'Yes'
# Filter: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus 39
############################################################################################
def test_isFtpaAppellantGroundsDocsVisibleInSubmitted_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. universe: Group 4 and Status 39
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No Status 39 in Group 4 found", test_from_state, inspect.stack()[0].function)
        
        # 2. Get latest record
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: P1 must be 'Yes'
        p1_records = winning_records.filter(col("Party") == 1)
        p1_count = p1_records.count()
        
        if p1_count == 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: Found Group 4/Status 39, but 0 records are Party 1", test_from_state, inspect.stack()[0].function)

        failures = p1_records.filter(col("isFtpaAppellantGroundsDocsVisibleInSubmitted") != "Yes")

        if failures.count() > 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", f"FAIL: Found {failures.count()} Party 1 rows not marked 'Yes'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "PASS", f"PASS: Verified {p1_count} Party 1 records as 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

############################################################################################
# Scenario 2: Party 2 -> OMIT (NULL)
# Filter: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus 39
############################################################################################
def test_isFtpaAppellantGroundsDocsVisibleInSubmitted_test2(test_df):
    try:
        test_from_state = "ended"
        
        # 1. universe: Group 4 and Status 39
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No Status 39 in Group 4 found", test_from_state, inspect.stack()[0].function)
        
        # 2. Get latest record
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: P2 must be OMITTED
        p2_records = winning_records.filter(col("Party") == 2)
        p2_count = p2_records.count()

        if p2_count == 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: Found Group 4/Status 39, but 0 records are Party 2", test_from_state, inspect.stack()[0].function)

        # Fail if field is populated (isNotNull)
        failures = p2_records.filter(col("isFtpaAppellantGroundsDocsVisibleInSubmitted").isNotNull())

        if failures.count() > 0:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", f"FAIL: Found {failures.count()} Party 2 rows not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "PASS", f"PASS: Verified {p2_count} Party 2 records correctly omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantGroundsDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)




#######################
# isFtpaAppellantEvidenceDocsVisibleInSubmitted
# Logic: IF M3.Party IS 1 = "Yes"; ELSE IF M3.Party IS 2 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

############################################################################################
# Scenario 1: Party 1 -> 'Yes'
# Logic: MAX(StatusId) WHERE CaseStatus = 39 AND EndedGroup = 4
############################################################################################
def test_isFtpaAppellantEvidenceDocsVisibleInSubmitted_test1(test_df):
    try:
        test_from_state = "ended"
        
        # 1. universe: Group 4 and Status 39
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No Status 39 in Group 4 found", test_from_state, inspect.stack()[0].function)
        
        # 2. Get latest record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 1 must be 'Yes'
        p1_records = winning_records.filter(col("Party") == 1)
        p1_count = p1_records.count()
        
        if p1_count == 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: Found Group 4/Status 39, but 0 records are Party 1", test_from_state, inspect.stack()[0].function)

        failures = p1_records.filter(col("isFtpaAppellantEvidenceDocsVisibleInSubmitted") != "Yes")

        if failures.count() > 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", f"FAIL: Found {failures.count()} Party 1 rows not marked 'Yes'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "PASS", f"PASS: Verified {p1_count} Party 1 records as 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

############################################################################################
# Scenario 2: Party 2 -> OMIT (NULL)
# Logic: MAX(StatusId) WHERE CaseStatus = 39 AND EndedGroup = 4
############################################################################################
def test_isFtpaAppellantEvidenceDocsVisibleInSubmitted_test2(test_df):
    try:
        test_from_state = "ended"
        
        # 1. universe: Group 4 and Status 39
        target_records = test_df.filter(
            (col("EndedGroup") == 4) & 
            (col("CaseStatus") == 39)
        )
        
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No Status 39 in Group 4 found", test_from_state, inspect.stack()[0].function)
        
        # 2. Get latest record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Party 2 must be OMITTED
        p2_records = winning_records.filter(col("Party") == 2)
        p2_count = p2_records.count()

        if p2_count == 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: Found Group 4/Status 39, but 0 records are Party 2", test_from_state, inspect.stack()[0].function)

        # Fail if field is populated (isNotNull)
        failures = p2_records.filter(col("isFtpaAppellantEvidenceDocsVisibleInSubmitted").isNotNull())

        if failures.count() > 0:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", f"FAIL: Found {failures.count()} Party 2 rows not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "PASS", f"PASS: Verified {p2_count} Party 2 records correctly omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("isFtpaAppellantEvidenceDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)





#######################
# isFtpaAppellantOotExplanationVisibleInDecided
# Logic: IF M3.Party IS 1 AND M3.OutOfTime IS 1 = "No"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

############################################################################################
# Scenario 1: P1 & OOT == 1 -> 'No'
# Logic: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus 39
############################################################################################
def test_isFtpaAppellantOotExplanationVisibleInDecided_test1(test_df):
    try:
        test_from_state = "ended"
        # 1. Filter for Group 4 and Status 39
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", "DATA DEFICIENCY: No Status 39 in Group 4 found", test_from_state, inspect.stack()[0].function)
        
        # 2. Get Max Status record
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: P1 OOT must be 'No'
        scenario_df = winning_records.filter((col("Party") == 1) & (col("OutOfTime") == 1))
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", "DATA DEFICIENCY: No P1 OOT records", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotExplanationVisibleInDecided") != "No")
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"FAIL: {failures.count()} rows not marked 'No'", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "PASS", f"PASS: Verified {scenario_df.count()} records", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
# Scenario 2: P1 & OOT != 1 -> OMIT (NULL)
############################################################################################
def test_isFtpaAppellantOotExplanationVisibleInDecided_test2(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        scenario_df = winning_records.filter((col("Party") == 1) & (col("OutOfTime") != 1))
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", "DATA DEFICIENCY: No P1 In-Time records", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotExplanationVisibleInDecided").isNotNull())
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"FAIL: {failures.count()} rows not omitted", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "PASS", "PASS: Correctly omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
# Scenario 3: P2 & OOT == 1 -> OMIT (NULL)
############################################################################################
def test_isFtpaAppellantOotExplanationVisibleInDecided_test3(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        scenario_df = winning_records.filter((col("Party") == 2) & (col("OutOfTime") == 1))
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", "DATA DEFICIENCY: No P2 OOT records", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotExplanationVisibleInDecided").isNotNull())
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"FAIL: {failures.count()} rows not omitted", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "PASS", "PASS: Correctly omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
# Scenario 4: P2 & OOT != 1 -> OMIT (NULL)
############################################################################################
def test_isFtpaAppellantOotExplanationVisibleInDecided_test4(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        scenario_df = winning_records.filter((col("Party") == 2) & (col("OutOfTime") != 1))
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", "DATA DEFICIENCY: No P2 In-Time records", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotExplanationVisibleInDecided").isNotNull())
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"FAIL: {failures.count()} rows not omitted", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "PASS", "PASS: Correctly omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)



#######################
# isFtpaAppellantOotExplanationVisibleInSubmitted
# Logic: IF M3.Party IS 1 AND M3.OutOfTime IS 1 = "Yes"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number

############################################################################################
# Scenario 1: P1 & OOT == 1 -> 'Yes'
# Logic: EndedGroup 4 AND MAX(StatusId) WHERE CaseStatus 39
############################################################################################
def test_isFtpaAppellantOotExplanationVisibleInSubmitted_test1(test_df):
    try:
        test_from_state = "ended"
        # 1. Filter for Group 4 and Status 39
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        if target_records.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No Status 39 in Group 4 found", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify latest record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: P1 OOT must be 'Yes'
        scenario_df = winning_records.filter((col("Party") == 1) & (col("OutOfTime") == 1))
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No P1 OOT records", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotExplanationVisibleInSubmitted") != "Yes")
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"FAIL: {failures.count()} rows not marked 'Yes'", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "PASS", f"PASS: Verified {scenario_df.count()} records", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
# Scenario 2: P1 & OOT != 1 -> OMIT (NULL)
############################################################################################
def test_isFtpaAppellantOotExplanationVisibleInSubmitted_test2(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        scenario_df = winning_records.filter((col("Party") == 1) & (col("OutOfTime") != 1))
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No P1 In-Time records", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotExplanationVisibleInSubmitted").isNotNull())
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"FAIL: {failures.count()} rows not omitted", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "PASS", "PASS: Correctly omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
# Scenario 3: P2 & OOT == 1 -> OMIT (NULL)
############################################################################################
def test_isFtpaAppellantOotExplanationVisibleInSubmitted_test3(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        scenario_df = winning_records.filter((col("Party") == 2) & (col("OutOfTime") == 1))
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No P2 OOT records", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotExplanationVisibleInSubmitted").isNotNull())
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"FAIL: {failures.count()} rows not omitted", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "PASS", "PASS: Correctly omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
# Scenario 4: P2 & OOT != 1 -> OMIT (NULL)
############################################################################################
def test_isFtpaAppellantOotExplanationVisibleInSubmitted_test4(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39))
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        winning_records = target_records.withColumn("row_rank", row_number().over(window_spec)).filter(col("row_rank") == 1)

        scenario_df = winning_records.filter((col("Party") == 2) & (col("OutOfTime") != 1))
        if scenario_df.count() == 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", "DATA DEFICIENCY: No P2 In-Time records", test_from_state, inspect.stack()[0].function)

        failures = scenario_df.filter(col("isFtpaAppellantOotExplanationVisibleInSubmitted").isNotNull())
        if failures.count() > 0:
            return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"FAIL: {failures.count()} rows not omitted", test_from_state, inspect.stack()[0].function)
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "PASS", "PASS: Correctly omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaAppellantOotExplanationVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
#######################
# ftpaRespondentSubmitted
# Logic: IF M3.Party IS 2 = "Yes"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################


def test_ftpaRespondentSubmitted_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "ftpaRespondentSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("ftpaRespondentSubmitted", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2))
        if target_records.count() == 0:
            return TestResult("ftpaRespondentSubmitted", "FAIL", "No Party 2 records found for Status 39.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("ftpaRespondentSubmitted") != "Yes")
        if failures.count() != 0:
            return TestResult("ftpaRespondentSubmitted", "FAIL", f"Found {failures.count()} rows where Party 2 was not 'Yes'", "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentSubmitted", "PASS", "Correctly included 'Yes' for Party 2", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

def test_ftpaRespondentSubmitted_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "ftpaRespondentSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("ftpaRespondentSubmitted", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 1))
        if target_records.count() == 0:
            return TestResult("ftpaRespondentSubmitted", "FAIL", "No Party 1 records found for Status 39.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("ftpaRespondentSubmitted").isNotNull())
        if failures.count() != 0:
            return TestResult("ftpaRespondentSubmitted", "FAIL", f"Found {failures.count()} rows where Party 1 was not omitted", "ended", inspect.stack()[0].function)
        return TestResult("ftpaRespondentSubmitted", "PASS", "Correctly omitted for Party 1", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("ftpaRespondentSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)



#######################
# isFtpaRespondentDocsVisibleInDecided
# Logic: IF M3.Party IS 2 = "No"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentDocsVisibleInDecided_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentDocsVisibleInDecided")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2))
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", "No Party 2 records found for Status 39.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("isFtpaRespondentDocsVisibleInDecided") != "No")
        if failures.count() != 0:
            return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", f"Found {failures.count()} rows where Party 2 was not 'No'", "ended", inspect.stack()[0].function)
        return TestResult("isFtpaRespondentDocsVisibleInDecided", "PASS", "Correctly included 'No' for Party 2", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)

def test_isFtpaRespondentDocsVisibleInDecided_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentDocsVisibleInDecided")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", f"No data to test: {str(e)[:300]}", "ended", inspect.stack()[0].function)

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 1))
        if target_records.count() == 0:
            return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", "No Party 1 records found for Status 39.", "ended", inspect.stack()[0].function)

        failures = target_records.filter(col("isFtpaRespondentDocsVisibleInDecided").isNotNull())
        if failures.count() != 0:
            return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", f"Found {failures.count()} rows where Party 1 was not omitted", "ended", inspect.stack()[0].function)
        return TestResult("isFtpaRespondentDocsVisibleInDecided", "PASS", "Correctly omitted for Party 1", "ended", inspect.stack()[0].function)
    except Exception as e:
        return TestResult("isFtpaRespondentDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", inspect.stack()[0].function)




#######################
# isFtpaRespondentDocsVisibleInSubmitted
# Logic: IF M3.Party IS 2 = "Yes"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentDocsVisibleInSubmitted_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentDocsVisibleInSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", f"No data to test: {str(e)[:300]}", "ended", "test_isFtpaRespondentDocsVisibleInSubmitted_test1")

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2))
        failures = target_records.filter(col("isFtpaRespondentDocsVisibleInSubmitted") != "Yes")
        if failures.count() != 0:
            return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", "Logic Mismatch", "ended", "test_isFtpaRespondentDocsVisibleInSubmitted_test1")
        return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "PASS", "Verified 'Yes' for Party 2", "ended", "test_isFtpaRespondentDocsVisibleInSubmitted_test1")
    except Exception as e:
        return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentDocsVisibleInSubmitted_test1")

def test_isFtpaRespondentDocsVisibleInSubmitted_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentDocsVisibleInSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", f"No data to test: {str(e)[:300]}", "ended", "test_isFtpaRespondentDocsVisibleInSubmitted_test2")

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 1))
        failures = target_records.filter(col("isFtpaRespondentDocsVisibleInSubmitted").isNotNull())
        if failures.count() != 0:
            return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", "Omit Mismatch", "ended", "test_isFtpaRespondentDocsVisibleInSubmitted_test2")
        return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "PASS", "Correctly omitted for Party 1", "ended", "test_isFtpaRespondentDocsVisibleInSubmitted_test2")
    except Exception as e:
        return TestResult("isFtpaRespondentDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentDocsVisibleInSubmitted_test2")




#######################
# isFtpaRespondentOotDocsVisibleInDecided
# Logic: IF M3.Party IS 2 AND M3.OutOfTime IS 1 = "No"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################
def test_isFtpaRespondentOotDocsVisibleInDecided_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentOotDocsVisibleInDecided")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"No data: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test1")

        target = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2) & (col("OutOfTime") == 1))
        if target.count() == 0: return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "No P2-OOT records", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test1")
        
        failures = target.filter(col("isFtpaRespondentOotDocsVisibleInDecided") != "No")
        if failures.count() != 0: return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "Logic Error", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test1")
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "PASS", "Verified 'No' for P2-OOT", "ended", "test1")
    except Exception as e: return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test1")

def test_isFtpaRespondentOotDocsVisibleInDecided_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentOotDocsVisibleInDecided")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"No data: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test2")

        target = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2) & (col("OutOfTime") != 1))
        if target.count() == 0: return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "No P2 In-Time records", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test2")
        
        failures = target.filter(col("isFtpaRespondentOotDocsVisibleInDecided").isNotNull())
        if failures.count() != 0: return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "Omission Error", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test2")
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "PASS", "Correctly omitted (P2 In-Time)", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test2")
    except Exception as e: return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test2")

def test_isFtpaRespondentOotDocsVisibleInDecided_test3(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentOotDocsVisibleInDecided")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"No data: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test3")

        target = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 1))
        if target.count() == 0: return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "No P1 records", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test3")
        
        failures = target.filter(col("isFtpaRespondentOotDocsVisibleInDecided").isNotNull())
        if failures.count() != 0: return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", "Omission Error", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test3")
        return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "PASS", "Correctly omitted (Party 1)", "ended", "test3")
    except Exception as e: return TestResult("isFtpaRespondentOotDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotDocsVisibleInDecided_test3")




#######################
# isFtpaRespondentOotDocsVisibleInSubmitted
# Logic: IF M3.Party IS 2 AND M3.OutOfTime IS 1 = "Yes"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentOotDocsVisibleInSubmitted_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentOotDocsVisibleInSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"No data: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotDocsVisibleInSubmitted_test1")

        target = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2) & (col("OutOfTime") == 1))
        failures = target.filter(col("isFtpaRespondentOotDocsVisibleInSubmitted") != "Yes")
        if failures.count() != 0: return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "Logic Error", "ended", "test_isFtpaRespondentOotDocsVisibleInSubmitted_test1")
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "PASS", "Verified 'Yes' for P2-OOT", "ended", "test1")
    except Exception as e: return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotDocsVisibleInSubmitted_test1")

def test_isFtpaRespondentOotDocsVisibleInSubmitted_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentOotDocsVisibleInSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"No data: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotDocsVisibleInSubmitted_test2")

        target = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2) & (col("OutOfTime") != 1))
        failures = target.filter(col("isFtpaRespondentOotDocsVisibleInSubmitted").isNotNull())
        if failures.count() != 0: return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "Omission Error", "ended", "test_isFtpaRespondentOotDocsVisibleInSubmitted_test2")
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "PASS", "Correctly omitted (P2 In-Time)", "ended", "test_isFtpaRespondentOotDocsVisibleInSubmitted_test2")
    except Exception as e: return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotDocsVisibleInSubmitted_test2")

def test_isFtpaRespondentOotDocsVisibleInSubmitted_test3(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentOotDocsVisibleInSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"No data: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotDocsVisibleInSubmitted_test3")

        target = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 1))
        failures = target.filter(col("isFtpaRespondentOotDocsVisibleInSubmitted").isNotNull())
        if failures.count() != 0: return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", "Omission Error", "ended", "test_isFtpaRespondentOotDocsVisibleInSubmitted_test3")
        return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "PASS", "Correctly omitted (Party 1)", "ended", "test_isFtpaRespondentOotDocsVisibleInSubmitted_test3")
    except Exception as e: return TestResult("isFtpaRespondentOotDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotDocsVisibleInSubmitted_test3")




#######################
# isFtpaRespondentGroundsDocsVisibleInDecided
# Logic: IF M3.Party IS 2 = "No"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentGroundsDocsVisibleInDecided_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentGroundsDocsVisibleInDecided")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", f"No data to test: {str(e)[:300]}", "ended", "test_isFtpaRespondentGroundsDocsVisibleInDecided_test1")

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2))
        failures = target_records.filter(col("isFtpaRespondentGroundsDocsVisibleInDecided") != "No")
        if failures.count() != 0:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", "Logic Error", "ended", "test_isFtpaRespondentGroundsDocsVisibleInDecided_test1")
        return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "PASS", "Verified 'No' for P2", "ended", "test_isFtpaRespondentGroundsDocsVisibleInDecided_test1")
    except Exception as e:
        return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentGroundsDocsVisibleInDecided_test1")

def test_isFtpaRespondentGroundsDocsVisibleInDecided_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentGroundsDocsVisibleInDecided")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", f"No data to test: {str(e)[:300]}", "ended", "test_isFtpaRespondentGroundsDocsVisibleInDecided_test2")

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 1))
        failures = target_records.filter(col("isFtpaRespondentGroundsDocsVisibleInDecided").isNotNull())
        if failures.count() != 0:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", "Failed Omission", "ended", "test_isFtpaRespondentGroundsDocsVisibleInDecided_test2")
        return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "PASS", "Correctly omitted for Party 1", "ended", "test_isFtpaRespondentGroundsDocsVisibleInDecided_test2")
    except Exception as e:
        return TestResult("isFtpaRespondentGroundsDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentGroundsDocsVisibleInDecided_test2")




#######################
# isFtpaRespondentEvidenceDocsVisibleInDecided
# Logic: IF M3.Party IS 2 = "No"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentEvidenceDocsVisibleInDecided_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentEvidenceDocsVisibleInDecided")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", f"No data to test: {str(e)[:300]}", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInDecided_test1")

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2))
        failures = target_records.filter(col("isFtpaRespondentEvidenceDocsVisibleInDecided") != "No")
        if failures.count() != 0:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", "Logic Error", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInDecided_test1")
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "PASS", "Verified 'No' for P2", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInDecided_test1")
    except Exception as e:
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInDecided_test1")

def test_isFtpaRespondentEvidenceDocsVisibleInDecided_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentEvidenceDocsVisibleInDecided")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", f"No data to test: {str(e)[:300]}", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInDecided_test2")

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 1))
        failures = target_records.filter(col("isFtpaRespondentEvidenceDocsVisibleInDecided").isNotNull())
        if failures.count() != 0:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", "Failed Omission", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInDecided_test2")
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "PASS", "Correctly omitted for Party 1", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInDecided_test2")
    except Exception as e:
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInDecided_test2")



#######################
# isFtpaRespondentGroundsDocsVisibleInSubmitted
# Logic: IF M3.Party IS 2 = "Yes"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentGroundsDocsVisibleInSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", f"No data to test: {str(e)[:300]}", "ended", "test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test1")

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2))
        failures = target_records.filter(col("isFtpaRespondentGroundsDocsVisibleInSubmitted") != "Yes")
        if failures.count() != 0:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", "Logic Error", "ended", "test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test1")
        return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "PASS", "Verified 'Yes' for P2", "ended", "test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test1")
    except Exception as e:
        return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test1")

def test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentGroundsDocsVisibleInSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", f"No data to test: {str(e)[:300]}", "ended", "test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test2")

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 1))
        failures = target_records.filter(col("isFtpaRespondentGroundsDocsVisibleInSubmitted").isNotNull())
        if failures.count() != 0:
            return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", "Failed Omission", "ended", "test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test2")
        return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "PASS", "Correctly omitted for Party 1", "ended", "test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test2")
    except Exception as e:
        return TestResult("isFtpaRespondentGroundsDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentGroundsDocsVisibleInSubmitted_test2")




#######################
# isFtpaRespondentEvidenceDocsVisibleInSubmitted
# Logic: IF M3.Party IS 2 = "Yes"; ELSE IF M3.Party IS 1 = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentEvidenceDocsVisibleInSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", f"No data to test: {str(e)[:300]}", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test1")

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2))
        failures = target_records.filter(col("isFtpaRespondentEvidenceDocsVisibleInSubmitted") != "Yes")
        if failures.count() != 0:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", "Logic Error", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test1")
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "PASS", "Verified 'Yes' for P2", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test1")
    except Exception as e:
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test1")

def test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentEvidenceDocsVisibleInSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", f"No data to test: {str(e)[:300]}", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test2")

        target_records = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 1))
        failures = target_records.filter(col("isFtpaRespondentEvidenceDocsVisibleInSubmitted").isNotNull())
        if failures.count() != 0:
            return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", "Failed Omission", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test2")
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "PASS", "Correctly omitted for Party 1", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test2")
    except Exception as e:
        return TestResult("isFtpaRespondentEvidenceDocsVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentEvidenceDocsVisibleInSubmitted_test2")



#######################
# isFtpaRespondentOotExplanationVisibleInDecided
# Logic: IF M3.Party IS 2 AND M3.OutOfTime IS 1 = "No"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentOotExplanationVisibleInDecided_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentOotExplanationVisibleInDecided")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"No data: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotExplanationVisibleInDecided_test1")

        target = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2) & (col("OutOfTime") == 1))
        failures = target.filter(col("isFtpaRespondentOotExplanationVisibleInDecided") != "No")
        if failures.count() != 0: return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "Logic Error", "ended", "test_isFtpaRespondentOotExplanationVisibleInDecided_test1")
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "PASS", "Verified 'No' for P2-OOT", "ended", "test_isFtpaRespondentOotExplanationVisibleInDecided_test1")
    except Exception as e: return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotExplanationVisibleInDecided_test1")

def test_isFtpaRespondentOotExplanationVisibleInDecided_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentOotExplanationVisibleInDecided")
            m3 = F.col("m3_CaseNo") if "m3_CaseNo" in locals() else col("CaseNo").alias("m3_CaseNo") # Safety
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"No data: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotExplanationVisibleInDecided_test2")

        target = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2) & (col("OutOfTime") != 1))
        failures = target.filter(col("isFtpaRespondentOotExplanationVisibleInDecided").isNotNull())
        if failures.count() != 0: return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "Omission Error", "ended", "test_isFtpaRespondentOotExplanationVisibleInDecided_test2")
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "PASS", "Correctly omitted (P2 In-Time)", "ended", "test_isFtpaRespondentOotExplanationVisibleInDecided_test2")
    except Exception as e: return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotExplanationVisibleInDecided_test2")

def test_isFtpaRespondentOotExplanationVisibleInDecided_test3(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentOotExplanationVisibleInDecided")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"No data: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotExplanationVisibleInDecided_test3")

        target = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 1))
        failures = target.filter(col("isFtpaRespondentOotExplanationVisibleInDecided").isNotNull())
        if failures.count() != 0: return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", "Omission Error", "ended", "test_isFtpaRespondentOotExplanationVisibleInDecided_test3")
        return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "PASS", "Correctly omitted (Party 1)", "ended", "test_isFtpaRespondentOotExplanationVisibleInDecided_test3")
    except Exception as e: return TestResult("isFtpaRespondentOotExplanationVisibleInDecided", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotExplanationVisibleInDecided_test3")


#######################
# isFtpaRespondentOotExplanationVisibleInSubmitted
# Logic: IF M3.Party IS 2 AND M3.OutOfTime IS 1 = "Yes"; ELSE = OMIT
# Group: EndedGroup 4 (MAX StatusId WHERE CaseStatus = 39)
#######################

def test_isFtpaRespondentOotExplanationVisibleInSubmitted_test1(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentOotExplanationVisibleInSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"No data: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotExplanationVisibleInSubmitted_test1")

        target = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2) & (col("OutOfTime") == 1))
        failures = target.filter(col("isFtpaRespondentOotExplanationVisibleInSubmitted") != "Yes")
        if failures.count() != 0: return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "Logic Error", "ended", "test_isFtpaRespondentOotExplanationVisibleInSubmitted_test1")
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "PASS", "Verified 'Yes' for P2-OOT", "ended", "test_isFtpaRespondentOotExplanationVisibleInSubmitted_test1")
    except Exception as e: return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotExplanationVisibleInSubmitted_test1")

def test_isFtpaRespondentOotExplanationVisibleInSubmitted_test2(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentOotExplanationVisibleInSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party", "OutOfTime")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"No data: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotExplanationVisibleInSubmitted_test2")

        target = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 2) & (col("OutOfTime") != 1))
        failures = target.filter(col("isFtpaRespondentOotExplanationVisibleInSubmitted").isNotNull())
        if failures.count() != 0: return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "Omission Error", "ended", "test_isFtpaRespondentOotExplanationVisibleInSubmitted_test2")
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "PASS", "Correctly omitted (P2 In-Time)", "ended", "test_isFtpaRespondentOotExplanationVisibleInSubmitted_test2")
    except Exception as e: return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotExplanationVisibleInSubmitted_test2")

def test_isFtpaRespondentOotExplanationVisibleInSubmitted_test3(json_data, M3_bronze):
    try:
        try:
            json = json_data.select("appealReferenceNumber", "isFtpaRespondentOotExplanationVisibleInSubmitted")
            m3 = M3_bronze.select(col("CaseNo").alias("m3_CaseNo"), "CaseStatus", "Party")
            unended_test_df = json.join(m3, json["appealReferenceNumber"] == m3["m3_CaseNo"], "inner")
            test_df = get_ended_group_id(unended_test_df)
        except Exception as e:
            return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"No data: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotExplanationVisibleInSubmitted_test3")

        target = test_df.filter((col("EndedGroup") == 4) & (col("CaseStatus") == 39) & (col("Party") == 1))
        failures = target.filter(col("isFtpaRespondentOotExplanationVisibleInSubmitted").isNotNull())
        if failures.count() != 0: return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", "Omission Error", "ended", "test_isFtpaRespondentOotExplanationVisibleInSubmitted_test3")
        return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "PASS", "Correctly omitted (Party 1)", "ended", "test_isFtpaRespondentOotExplanationVisibleInSubmitted_test3")
    except Exception as e: return TestResult("isFtpaRespondentOotExplanationVisibleInSubmitted", "FAIL", f"EXCEPTION: {str(e)[:300]}", "ended", "test_isFtpaRespondentOotExplanationVisibleInSubmitted_test3")

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


        m1_rep = M1_silver.select("CaseNo", F.col("dv_representation").alias("Representation"))

        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        m3_ended = M3_bronze.filter(ended_filter) \
            .withColumn("rn", F.row_number().over(window_spec)) \
            .filter("rn = 1") \
            .select(
                "CaseNo", "CaseStatus", "Outcome", "StatusId", "DecisionDate",
                "Adj_Determination_Title", "Adj_Determination_Forenames", "Adj_Determination_Surname"
            )

    
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

            if aria_source_date is None:
                if actual_json_date is not None:
                    results.append(f"FAIL - {row['appealReferenceNumber']}: ARIA date is NULL but JSON found '{actual_json_date}'")
                continue

            # Convert ARIA Timestamp to ISO 8601 String (YYYY-MM-DD)
            expected_date_str = aria_source_date.strftime('%Y-%m-%d')
            
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
    









    #######################
# ftpaAppellantApplicationDate - Scenario 1
# IF M3.Party IS 1 = Include (MAX StatusID WHERE CaseStatus = 39 AND EndedGroup = 4)
#######################
def test_ftpaAppellantApplicationDate_test1(test_df):
    try:
        test_from_state = "ended"
        # 1. Filter for the specific CaseStatus, Party required, AND EndedGroup 4
        target_records = test_df.filter((col("CaseStatus") == 39) & (col("EndedGroup") == 4))

        if target_records.count() == 0:
            return TestResult("ftpaAppellantApplicationDate", "FAIL", "NO RECORDS TO TEST (No M3.Party 1 CaseStatus 39 EndedGroup 4 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where M3.Party 1 date is missing (NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 1) &
            (col("ftpaAppellantApplicationDate").isNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantApplicationDate", "FAIL", f"ftpaAppellantApplicationDate acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party is 1 & ftpaAppellantApplicationDate is not included", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantApplicationDate", "PASS", "ftpaAppellantApplicationDate acceptance criteria pass: all rows where M3.Party is 1 have ftpaAppellantApplicationDate included", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("ftpaAppellantApplicationDate", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    
#######################
# ftpaAppellantApplicationDate - Scenario 2
# IF M3.Party IS 2 = OMIT (MAX StatusID WHERE CaseStatus = 39 AND EndedGroup = 4)
#######################
def test_ftpaAppellantApplicationDate_test2(test_df):
    try:
        test_from_state = "ended"
        # 1. Filter for the specific CaseStatus, Party required, AND EndedGroup 4
        target_records = test_df.filter((col("CaseStatus") == 39) & (col("EndedGroup") == 4))

        if target_records.count() == 0:
            return TestResult("ftpaAppellantApplicationDate", "FAIL", "NO RECORDS TO TEST (No M3.Party 2 CaseStatus 39 EndedGroup 4 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Find failures where Party 2 date is NOT missing (NOT NULL)
        acceptance_critera = winning_records.filter(
            (col("Party") == 2) &
            (col("ftpaAppellantApplicationDate").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantApplicationDate", "FAIL", f"ftpaAppellantApplicationDate acceptance criteria failed: found {acceptance_critera.count()} rows where M3.Party = 2 & ftpaAppellantApplicationDate is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantApplicationDate", "PASS", "ftpaAppellantApplicationDate acceptance criteria pass: all rows where M3.Party = 2 have ftpaAppellantApplicationDate omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("ftpaAppellantApplicationDate", "FAIL", f"TEST FAILED WITH EXCEPTION : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    


#######################
# ftpaAppellantOutOfTimeExplanation - Scenario 1
# IF M3.OutOfTime IS 1 AND M3.Party IS 1 = Include (MAX StatusID WHERE CaseStatus = 39 AND EndedGroup = 4)
#######################
def test_ftpaAppellantOutOfTimeExplanation_test1(test_df):
    try:
        test_from_state = "ended"
        expected_str = "This is a migrated ARIA case. Please refer to the documents."
        
        # 1. Filter for Appeal State and Ended Group 4
        target_records = test_df.filter((col("CaseStatus") == 39) & (col("EndedGroup") == 4)) 
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", "NO RECORDS TO TEST (No Status 39, Group 4 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: M3.OutOfTime is '1' (or true) and Party is '1' -> Must match expected_str
        acceptance_critera = winning_records.filter(
            (col("OutOfTime").cast("string").isin("1", "true")) &
            (col("Party") == 1) &
            (col("ftpaAppellantOutOfTimeExplanation") != expected_str)
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"Scenario 1 FAIL: found {acceptance_critera.count()} rows where explanation is missing or incorrect for OOT Appellant", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "Scenario 1 PASS: All OOT Appellant FTPA records have correct migrated string", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# ftpaAppellantOutOfTimeExplanation - Scenario 2
# IF M3.OutOfTime IS 1 AND M3.Party IS NOT 1 = OMIT
#######################
def test_ftpaAppellantOutOfTimeExplanation_test2(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("CaseStatus") == 39) & (col("EndedGroup") == 4)) 
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", "NO RECORDS TO TEST (No Status 39, Group 4 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: If Party is not 1 (Respondent), the field must be OMITTED (Null)
        acceptance_critera = winning_records.filter(
            (col("OutOfTime").cast("string").isin("1", "true")) &
            (col("Party") != 1) &
            (col("ftpaAppellantOutOfTimeExplanation").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"Scenario 2 FAIL: found {acceptance_critera.count()} rows where Respondent FTPA incorrectly included Appellant explanation", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "Scenario 2 PASS: Respondent FTPA correctly omitted Appellant explanation", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# ftpaAppellantOutOfTimeExplanation - Scenario 3
# IF M3.OutOfTime IS NOT 1 AND M3.Party IS 1 = OMIT
#######################
def test_ftpaAppellantOutOfTimeExplanation_test3(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("CaseStatus") == 39) & (col("EndedGroup") == 4)) 
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", "NO RECORDS TO TEST (No Status 39, Group 4 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: If not OutOfTime, the explanation must be OMITTED (Null)
        acceptance_critera = winning_records.filter(
            (~col("OutOfTime").cast("string").isin("1", "true")) &
            (col("Party") == 1) &
            (col("ftpaAppellantOutOfTimeExplanation").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"Scenario 3 FAIL: found {acceptance_critera.count()} rows where In-Time FTPA incorrectly included OOT explanation", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "Scenario 3 PASS: In-Time FTPA correctly omitted explanation", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# ftpaAppellantOutOfTimeExplanation - Scenario 4
# IF M3.OutOfTime IS NOT 1 AND M3.Party IS NOT 1 = OMIT
#######################
def test_ftpaAppellantOutOfTimeExplanation_test4(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("CaseStatus") == 39) & (col("EndedGroup") == 4)) 
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", "NO RECORDS TO TEST (No Status 39, Group 4 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Neither condition met -> must be NULL
        acceptance_critera = winning_records.filter(
            (~col("OutOfTime").cast("string").isin("1", "true")) &
            (col("Party") != 1) &
            (col("ftpaAppellantOutOfTimeExplanation").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"Scenario 4 FAIL: found {acceptance_critera.count()} rows where explanation was incorrectly included", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantOutOfTimeExplanation", "PASS", "Scenario 4 PASS: Correctly omitted explanation", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantOutOfTimeExplanation", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
# ftpaAppellantSubmissionOutOfTime - Scenario 1
# IF M3.OutOfTime IS 1 AND M3.Party IS 1 = "Yes" (MAX StatusID WHERE CaseStatus = 39 AND EndedGroup = 4)
#######################
def test_ftpaAppellantSubmissionOutOfTime_test1(test_df):
    try:
        test_from_state = "ended"
        # 1. Filter for CaseStatus 39 and EndedGroup 4
        target_records = test_df.filter((col("CaseStatus") == 39) & (col("EndedGroup") == 4)) 
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", "NO RECORDS TO TEST (No Status 39, Group 4 found)", test_from_state, inspect.stack()[0].function)
        
        # 2. Identify the MAX StatusID record per appeal
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: M3.OutOfTime is 1/true and Party is 1 -> Must be "Yes"
        acceptance_critera = winning_records.filter(
            (col("OutOfTime").cast("string").isin("1", "true")) &
            (col("Party") == 1) &
            (col("ftpaAppellantSubmissionOutOfTime") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"Scenario 1 FAIL: found {acceptance_critera.count()} rows where Appellant OOT is not 'Yes'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "PASS", "Scenario 1 PASS: All OOT Appellant records have 'Yes'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# ftpaAppellantSubmissionOutOfTime - Scenario 2
# IF M3.OutOfTime IS NOT 1 AND M3.Party IS 1 = "No"
#######################
def test_ftpaAppellantSubmissionOutOfTime_test2(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("CaseStatus") == 39) & (col("EndedGroup") == 4)) 
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", "NO RECORDS TO TEST (No Status 39, Group 4 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Appellant In-Time -> Must be "No"
        acceptance_critera = winning_records.filter(
            (~col("OutOfTime").cast("string").isin("1", "true")) &
            (col("Party") == 1) &
            (col("ftpaAppellantSubmissionOutOfTime") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"Scenario 2 FAIL: found {acceptance_critera.count()} rows where Appellant In-Time is not 'No'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "PASS", "Scenario 2 PASS: All In-Time Appellant records have 'No'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# ftpaAppellantSubmissionOutOfTime - Scenario 3
# IF M3.OutOfTime IS NOT 1 AND M3.Party IS NOT 1 = OMIT
#######################
def test_ftpaAppellantSubmissionOutOfTime_test3(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("CaseStatus") == 39) & (col("EndedGroup") == 4)) 
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", "NO RECORDS TO TEST (No Status 39, Group 4 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: Not Appellant and In-Time -> Field must be OMITTED
        acceptance_critera = winning_records.filter(
            (~col("OutOfTime").cast("string").isin("1", "true")) &
            (col("Party") != 1) &
            (col("ftpaAppellantSubmissionOutOfTime").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"Scenario 3 FAIL: found {acceptance_critera.count()} rows where field was incorrectly included", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "PASS", "Scenario 3 PASS: Field correctly omitted", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# ftpaAppellantSubmissionOutOfTime - Scenario 4
# IF M3.OutOfTime IS 1 AND M3.Party IS NOT 1 = OMIT
#######################
def test_ftpaAppellantSubmissionOutOfTime_test4(test_df):
    try:
        test_from_state = "ended"
        target_records = test_df.filter((col("CaseStatus") == 39) & (col("EndedGroup") == 4)) 
        
        if target_records.count() == 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", "NO RECORDS TO TEST (No Status 39, Group 4 found)", test_from_state, inspect.stack()[0].function)
        
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(col("StatusId").desc())
        ranked_df = target_records.withColumn("row_rank", row_number().over(window_spec))
        winning_records = ranked_df.filter(col("row_rank") == 1)

        # 3. Acceptance Criteria: OutOfTime but not Appellant -> Field must be OMITTED
        acceptance_critera = winning_records.filter(
            (col("OutOfTime").cast("string").isin("1", "true")) &
            (col("Party") != 1) &
            (col("ftpaAppellantSubmissionOutOfTime").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"Scenario 4 FAIL: found {acceptance_critera.count()} rows where non-Appellant record incorrectly included this field", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("ftpaAppellantSubmissionOutOfTime", "PASS", "Scenario 4 PASS: Field correctly omitted for non-Appellant records", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("ftpaAppellantSubmissionOutOfTime", "FAIL", f"EXCEPTION: {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

