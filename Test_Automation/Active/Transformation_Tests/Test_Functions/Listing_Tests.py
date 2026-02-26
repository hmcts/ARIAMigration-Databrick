from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
import inspect

#Import Test Results class
from models.test_result import TestResult


test_from_state = "listing"


############################################################################################
#######################
#default mapping Init code
#######################

def test_default_mapping_init(json, M1_silver, bac, b):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "caseArgumentAvailable",
            "reasonsForAppealDecision",
            "isAppellantAttendingTheHearing",
            "isAppellantGivingOralEvidence",
            "isWitnessesAttending",
            "isEvidenceFromOutsideUkOoc",
            "isEvidenceFromOutsideUkInCountry",
            "isHearingRoomNeeded",
            "isHearingLoopNeeded",
            "remoteVideoCall",
            "remoteVideoCallDescription",
            "physicalOrMentalHealthIssues",
            "physicalOrMentalHealthIssuesDescription",
            "pastExperiences",
            "pastExperiencesDescription",
            "multimediaEvidence",
            "multimediaEvidenceDescription",
            "additionalRequests",
            "additionalRequestsDescription",
            "datesToAvoidYesNo",
            "appealReviewOutcome",
            "appealResponseAvailable",
            "reviewedHearingRequirements",
            "amendResponseActionAvailable",
            "currentHearingDetailsVisible",
            "reviewResponseActionAvailable",
            "reviewHomeOfficeResponseByLegalRep",
            "submitHearingRequirementsAvailable",
            "uploadHomeOfficeAppealResponseActionAvailable",
            "hearingRequirements"
        )

        # Join Category ID
        test_df = test_df.join(
            bac.select("CaseNo", "CategoryId"),
            json["appealReferenceNumber"] == bac["CaseNo"],
            "left"
        )

        # Join Sponsor Name
        test_df = test_df.join(
            b.select("CaseNo", "Sponsor_Name"),
            test_df["appealReferenceNumber"] == b["CaseNo"],
            "left"
        )

        # 4. Join Silver Mapping for Representation
        test_df = test_df.join(
            M1_silver.select("CaseNo", "dv_representation"),
            test_df["appealReferenceNumber"] == M1_silver["CaseNo"],
            "left"
        )

        return test_df, True

    except Exception as e:
        error_message = str(e)        
        return None, TestResult("Init_Listing", "FAIL", f"Data Setup Error: {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    
    

from pyspark.sql.functions import col, when, lit

def test_defaultValues(test_df):
    try:
        results_list = []
        
        # Expected 
        aria_notice_text = "This is a migrated ARIA case. Please see the documents provided as part of the notice of appeal."
        aria_reqs_text = "This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form."

        # Field Groups
        yes_fields = [
            "isAppellantAttendingTheHearing", "isAppellantGivingOralEvidence", 
            "isHearingRoomNeeded", "isHearingLoopNeeded", "remoteVideoCall", 
            "physicalOrMentalHealthIssues", "pastExperiences", "multimediaEvidence", 
            "additionalRequests", "appealResponseAvailable", "amendResponseActionAvailable", 
            "currentHearingDetailsVisible", "reviewHomeOfficeResponseByLegalRep", 
            "submitHearingRequirementsAvailable"
        ]
        
        no_fields = [
            "isWitnessesAttending", "datesToAvoidYesNo", "reviewedHearingRequirements", 
            "reviewResponseActionAvailable", "uploadHomeOfficeAppealResponseActionAvailable"
        ]
        
        aria_desc_fields = [
            "remoteVideoCallDescription", "physicalOrMentalHealthIssuesDescription", 
            "pastExperiencesDescription", "multimediaEvidenceDescription", 
            "additionalRequestsDescription"
        ]

        # Validate "Yes" Group
        for field in yes_fields:
            fail_count = test_df.filter((col(field) != "Yes") | col(field).isNull()).count()
            results_list.append(TestResult(field, "PASS" if fail_count == 0 else "FAIL", f"Expected 'Yes', found {fail_count} errors", test_from_state, inspect.stack()[0].function))

        # Validate "No" Group 
        for field in no_fields:
            fail_count = test_df.filter((col(field) != "No") | col(field).isNull()).count()
            results_list.append(TestResult(field, "PASS" if fail_count == 0 else "FAIL", f"Expected 'No', found {fail_count} errors", test_from_state, inspect.stack()[0].function))

        # Validate ARIA Descriptions
        for field in aria_desc_fields:
            fail_count = test_df.filter((col(field) != aria_reqs_text) | col(field).isNull()).count()
            results_list.append(TestResult(field, "PASS" if fail_count == 0 else "FAIL", "ARIA migration text mismatch", test_from_state, inspect.stack()[0].function))

        
        # caseArgumentAvailable: LR = 'Yes' | AIP = NULL
        lr_arg_fail = test_df.filter((col("dv_representation") == "LR") & (col("caseArgumentAvailable") != "Yes")).count()
        aip_arg_fail = test_df.filter((col("dv_representation") == "AIP") & (col("caseArgumentAvailable").isNotNull())).count()
        results_list.append(TestResult("caseArgumentAvailable_LR", "PASS" if lr_arg_fail == 0 else "FAIL", f"LR Error: {lr_arg_fail} records", test_from_state, inspect.stack()[0].function))
        results_list.append(TestResult("caseArgumentAvailable_AIP", "PASS" if aip_arg_fail == 0 else "FAIL", f"AIP Error: {aip_arg_fail} records (should be Null)", test_from_state, inspect.stack()[0].function))
        
        # reasonsForAppealDecision: AIP = Notice Text | LR = NULL
        aip_reason_fail = test_df.filter((col("dv_representation") == "AIP") & (col("reasonsForAppealDecision") != aria_notice_text)).count()
        lr_reason_fail = test_df.filter((col("dv_representation") == "LR") & (col("reasonsForAppealDecision").isNotNull())).count()
        results_list.append(TestResult("reasonsForAppealDecision_AIP", "PASS" if aip_reason_fail == 0 else "FAIL", f"AIP Text Error", test_from_state, inspect.stack()[0].function))
        results_list.append(TestResult("reasonsForAppealDecision_LR", "PASS" if lr_reason_fail == 0 else "FAIL", f"LR Reason Error: {lr_reason_fail} records (should be Null)", test_from_state, inspect.stack()[0].function))


        # OOC: IF Category 38 + Sponsor exists = 'Yes'
        ooc_fail = test_df.filter((col("CategoryId") == 38) & (col("Sponsor_Name").isNotNull()) & (col("isEvidenceFromOutsideUkOoc") != "Yes")).count()
        results_list.append(TestResult("isEvidenceFromOutsideUkOoc", "PASS" if ooc_fail == 0 else "FAIL", "Conditional OOC Check failed", test_from_state, inspect.stack()[0].function))

        # InCountry: IF Category 37 + Sponsor exists = 'Yes'
        ic_fail = test_df.filter((col("CategoryId") == 37) & (col("Sponsor_Name").isNotNull()) & (col("isEvidenceFromOutsideUkInCountry") != "Yes")).count()
        results_list.append(TestResult("isEvidenceFromOutsideUkInCountry", "PASS" if ic_fail == 0 else "FAIL", "Conditional InCountry Check failed", test_from_state, inspect.stack()[0].function))

        # Outcome & Empty Array Checks
        outcome_fail = test_df.filter(col("appealReviewOutcome") != "decisionMaintained").count()
        results_list.append(TestResult("appealReviewOutcome", "PASS" if outcome_fail == 0 else "FAIL", "Outcome mismatch", test_from_state, inspect.stack()[0].function))

        # Check for empty array []
        hr_fail = test_df.filter(size(col("hearingRequirements")) != 0).count()
        results_list.append(TestResult("hearingRequirements", "PASS" if hr_fail == 0 else "FAIL", "Array should be empty", test_from_state, inspect.stack()[0].function))

        return results_list

    except Exception as e:
        error_message = str(e)        
        return [TestResult("Listing_Mapping", "FAIL", f"Logic Error: {error_message[:300]}", test_from_state, inspect.stack()[0].function)]