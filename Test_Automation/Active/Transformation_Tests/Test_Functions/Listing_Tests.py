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
test_from_state = "listing"

def testcase1():    
    return TestResult("testcasefield", "FAIL", f"listing - test 1 complete", test_from_state, inspect.stack()[0].function)

def testcase2():    
    return TestResult("testcasefield", "PASS", f"listing - test 2 complete", test_from_state, inspect.stack()[0].function)


############################################################################################
#######################
#default mapping Init code
#######################

def test_default_mapping_init(json, M1_silver):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "caseArgumentAvailable",
            "reasonsForAppealDecision"
        )

        M1_silver = M1_silver.select(
            "CaseNo",
            "dv_representation"
        )

        test_df = test_df.join(
            M1_silver,
            json["appealReferenceNumber"] == M1_silver["CaseNo"],
            "inner"
        ).drop(M1_silver["CaseNo"])

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None, TestResult("DefaultMapping_Listing", "FAIL", f"Init Error: {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    
    from pyspark.sql.functions import col, when, lit

def test_defaultValues(test_df):
    try:
        results_list = []
        
        # 1. Define Field Groups for easier maintenance
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

        # --- VALIDATION LOGIC ---

        # 2. Check "Yes" Fields
        for field in yes_fields:
            fail_count = test_df.filter((col(field) != "Yes") | col(field).isNull()).count()
            status = "PASS" if fail_count == 0 else "FAIL"
            results_list.append(TestResult(field, status, f"Expected 'Yes', found {fail_count} mismatches", test_from_state, inspect.stack()[0].function))

        # 3. Check "No" Fields
        for field in no_fields:
            fail_count = test_df.filter((col(field) != "No") | col(field).isNull()).count()
            status = "PASS" if fail_count == 0 else "FAIL"
            results_list.append(TestResult(field, status, f"Expected 'No', found {fail_count} mismatches", test_from_state, inspect.stack()[0].function))

        # 4. Check Representations (LR vs AIP)
        # CaseArgumentAvailable (LR -> Yes)
        lr_fail = test_df.filter((col("dv_representation") == "LR") & (col("caseArgumentAvailable") != "Yes")).count()
        results_list.append(TestResult("caseArgumentAvailable", "PASS" if lr_fail == 0 else "FAIL", f"LR Check: found {lr_fail} errors", test_from_state, inspect.stack()[0].function))
        
        # ReasonsForAppealDecision (AIP -> Aria Text)
        aria_text = "This is a migrated ARIA case. Please see the documents provided as part of the notice of appeal."
        aip_fail = test_df.filter((col("dv_representation") == "AIP") & (col("reasonsForAppealDecision") != aria_text)).count()
        results_list.append(TestResult("reasonsForAppealDecision", "PASS" if aip_fail == 0 else "FAIL", f"AIP Check: found {aip_fail} errors", test_from_state, inspect.stack()[0].function))

        # 5. Check Conditional Logic (OOC & InCountry)
        # OOC: IF Cat 38 AND Sponsor NOT NULL -> Yes
        ooc_fail = test_df.filter(
            (col("CategoryId") == 38) & 
            (col("SponsorName").isNotNull()) & 
            (col("isEvidenceFromOutsideUkOoc") != "Yes")
        ).count()
        results_list.append(TestResult("isEvidenceFromOutsideUkOoc", "PASS" if ooc_fail == 0 else "FAIL", "Conditional OOC Check Failed", test_from_state, inspect.stack()[0].function))

        # InCountry: IF Cat 37 AND Sponsor NOT NULL -> Yes
        ic_fail = test_df.filter(
            (col("CategoryId") == 37) & 
            (col("SponsorName").isNotNull()) & 
            (col("isEvidenceFromOutsideUkInCountry") != "Yes")
        ).count()
        results_list.append(TestResult("isEvidenceFromOutsideUkInCountry", "PASS" if ic_fail == 0 else "FAIL", "Conditional InCountry Check Failed", test_from_state, inspect.stack()[0].function))

        # 6. Check appealReviewOutcome & hearingRequirements
        outcome_fail = test_df.filter(col("appealReviewOutcome") != "decisionMaintained").count()
        results_list.append(TestResult("appealReviewOutcome", "PASS" if outcome_fail == 0 else "FAIL", "Outcome mismatch", test_from_state, inspect.stack()[0].function))

        return results_list

    except Exception as e:
        return [TestResult("Listing_Mapping", "FAIL", f"Exception: {str(e)}", test_from_state, inspect.stack()[0].function)]
