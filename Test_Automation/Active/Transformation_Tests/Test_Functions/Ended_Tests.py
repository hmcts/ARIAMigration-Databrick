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
    """
    Mapping logic based on CaseStatusId and Outcome from mapping doc.
    """
    return df.withColumn("EndedGroup", 
        F.when((F.col("CaseStatus") == 37) & (F.col("Outcome") == 80), 3)
        .when((F.col("CaseStatus") == 38) & (F.col("Outcome") == 80), 3)
        .when((F.col("CaseStatus") == 10) & (F.col("Outcome") == 80), 1)
        .when((F.col("CaseStatus") == 10122) & (F.col("Outcome") == 80), 1)
        .when((F.col("CaseStatus") == 26) & (F.col("Outcome") == 80), 2)
        .when((F.col("CaseStatus") == 51) & (F.col("Outcome") == 194), 1)
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
        .when((F.col("CaseStatus") == 51) & (F.col("Outcome") == 193), 1)
        .when((F.col("CaseStatus") == 38) & (F.col("Outcome") == 72), 3)
        .when((F.col("CaseStatus") == 10) & (F.col("Outcome") == 120), 1)
        .when((F.col("CaseStatus") == 10) & (F.col("Outcome") == 2), 1)
        .when((F.col("CaseStatus") == 10) & (F.col("Outcome") == 105), 1)
        .otherwise(0)
    )

def test_default_mapping_init(json_data, M3_bronze):
    try:
        # Find latest status
        window_spec = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
        latest_status = M3_bronze.withColumn("rn", F.row_number().over(window_spec)).filter("rn = 1")
        status_with_groups = get_ended_group_id(latest_status)
        
        # Join Group to JSON
        test_df = json_data.join(
            status_with_groups.select("CaseNo", "EndedGroup"),
            json_data.appealReferenceNumber == status_with_groups.CaseNo,
            "left"
        )
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
