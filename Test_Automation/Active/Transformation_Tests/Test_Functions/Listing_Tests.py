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

############################################################################################
#######################
#hearing reqs Init code
#######################
def test_hearingRequirements_init(json, M1_bronze):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "isInterpreterServicesNeeded",
            "singleSexCourt",
            "singleSexCourtType",
            "singleSexCourtTypeDescription",
            "inCameraCourt",
            "inCameraCourtDescription"
            # add language fields
        )

        M1_bronze = M1_bronze.select(
            "CaseNo",
            "Interpreter",
            "CourtPreference",
            "InCamera"
        )

        test_df = test_df.join(
            M1_bronze,
            json["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        ).drop(M1_bronze["CaseNo"])

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("hearingRequirements", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state,inspect.stack()[0].function)
    

#######################
# isInterpreterServicesNeeded - Check M1_bronze.Interpreter is 1 and isInterpreterServicesNeeded = Yes
#######################
def test_isInterpreterServicesNeeded_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("Interpreter").isNotNull()) &
            (col("Interpreter") == 1)
            ).count() == 0:
            return TestResult("isInterpreterServicesNeeded", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("Interpreter").isNotNull()) &
            (col("Interpreter") == 1)
        ) &
            (col("isInterpreterServicesNeeded") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isInterpreterServicesNeeded","FAIL", f"isInterpreterServicesNeeded acceptance criteria failed: found {acceptance_critera.count()} rows where M1_bronze.Interpreter is 1 and isInterpreterServicesNeeded != Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isInterpreterServicesNeeded","PASS", "isInterpreterServicesNeeded acceptance criteria pass: all rows where M1_bronze.Interpreter is 1 have isInterpreterServicesNeeded = Yes", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("isInterpreterServicesNeeded", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# isInterpreterServicesNeeded - Check M1_bronze.Interpreter is 0 and isInterpreterServicesNeeded = No
#######################
def test_isInterpreterServicesNeeded_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("Interpreter").isNotNull()) &
            (col("Interpreter") == 0)
            ).count() == 0:
            return TestResult("isInterpreterServicesNeeded", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("Interpreter").isNotNull()) &
            (col("Interpreter") == 1)
        ) &
            (col("isInterpreterServicesNeeded") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("isInterpreterServicesNeeded","FAIL", f"isInterpreterServicesNeeded acceptance criteria failed: found {acceptance_critera.count()} rows where M1_bronze.Interpreter is 0 and isInterpreterServicesNeeded != No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isInterpreterServicesNeeded","PASS", "isInterpreterServicesNeeded acceptance criteria pass: all rows where M1_bronze.Interpreter is 0 have isInterpreterServicesNeeded = No", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("isInterpreterServicesNeeded", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 
    
#######################
# isInterpreterServicesNeeded - Check isInterpreterServicesNeeded is not null (mandatory field) 
#######################
def test_isInterpreterServicesNeeded_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("isInterpreterServicesNeeded").isNotNull())
            ).count() == 0:
            return TestResult("isInterpreterServicesNeeded", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (col("isInterpreterServicesNeeded").isNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("isInterpreterServicesNeeded","FAIL", f"isInterpreterServicesNeeded acceptance criteria failed: found {acceptance_critera.count()} rows where isInterpreterServicesNeeded is null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("isInterpreterServicesNeeded","PASS", "isInterpreterServicesNeeded acceptance criteria pass: all rows have a value for isInterpreterServicesNeeded", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("isInterpreterServicesNeeded", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 
    
#######################
# singleSexCourt - Check where M1.CourtPreference = 0 and singleSexCourt = No
#######################
def test_singleSexCourt_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 0)
            ).count() == 0:
            return TestResult("singleSexCourt", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 0)
        ) & (col("singleSexCourt") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourt","FAIL", f"singleSexCourt acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference = 0 and singleSexCourt != No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourt","PASS", "singleSexCourt acceptance criteria pass: all rows where M1.CourtPreference = 0 have singleSexCourt = No", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourt", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 
    
#######################
# singleSexCourt - Check where M1.CourtPreference = 1 and singleSexCourt = Yes
#######################
def test_singleSexCourt_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 1)
            ).count() == 0:
            return TestResult("singleSexCourt", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 1)
        ) & (col("singleSexCourt") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourt","FAIL", f"singleSexCourt acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference = 1 and singleSexCourt != Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourt","PASS", "singleSexCourt acceptance criteria pass: all rows where M1.CourtPreference = 1 have singleSexCourt = Yes", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourt", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# singleSexCourt - Check where M1.CourtPreference = 2 and singleSexCourt = Yes
#######################
def test_singleSexCourt_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 2)
            ).count() == 0:
            return TestResult("singleSexCourt", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 2)
        ) & (col("singleSexCourt") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourt","FAIL", f"singleSexCourt acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference = 2 and singleSexCourt != Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourt","PASS", "singleSexCourt acceptance criteria pass: all rows where M1.CourtPreference = 2 have singleSexCourt = Yes", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourt", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# singleSexCourt - Check singleSexCourt is not null
#######################
def test_singleSexCourt_test4(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("singleSexCourt").isNotNull())
            ).count() == 0:
            return TestResult("singleSexCourt", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (col("singleSexCourt").isNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourt","FAIL", f"singleSexCourt acceptance criteria failed: found {acceptance_critera.count()} rows where singleSexCourt is null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourt","PASS", "singleSexCourt acceptance criteria pass: all rows have a value for singleSexCourt", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourt", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# singleSexCourtType - Check where M1.CourtPreference = 1 and singleSexCourtType = “All male”
#######################
def test_singleSexCourtType_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 1)
            ).count() == 0:
            return TestResult("singleSexCourtType", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 1)
        ) & (col("singleSexCourtType") != "All male")
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourtType","FAIL", f"singleSexCourtType acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference = 1 and singleSexCourtType != “All male”", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourtType","PASS", "singleSexCourtType acceptance criteria pass: all rows where M1.CourtPreference = 1 have singleSexCourtType = “All male”", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourtType", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# singleSexCourtType - Check where M1.CourtPreference = 2 and singleSexCourtType = “All female”
#######################
def test_singleSexCourtType_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 2)
            ).count() == 0:
            return TestResult("singleSexCourtType", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 2)
        ) & (col("singleSexCourtType") != "All female")
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourtType","FAIL", f"singleSexCourtType acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference = 2 and singleSexCourtType != “All female”", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourtType","PASS", "singleSexCourtType acceptance criteria pass: all rows where M1.CourtPreference = 2 have singleSexCourtType = “All female”", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourtType", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# singleSexCourtType - Check where M1.CourtPreference != 1 or 2 and singleSexCourtType isNull”
#######################
def test_singleSexCourtType_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") != 1) & 
            (col("CourtPreference") != 2)
            ).count() == 0:
            return TestResult("singleSexCourtType", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") != 1) & 
            (col("CourtPreference") != 2)
        ) & (col("singleSexCourtType").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourtType","FAIL", f"singleSexCourtType acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference != 1 or 2 and singleSexCourtType is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourtType","PASS", "singleSexCourtType acceptance criteria pass: all rows where M1.CourtPreference != 1 or 2 have singleSexCourtType is null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourtType", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# singleSexCourtTypeDescription - Check where M1.CourtPreference = 1 and singleSexCourtTypeDescription = "This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court."
#######################
def test_singleSexCourtTypeDescription_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 1)
            ).count() == 0:
            return TestResult("singleSexCourtTypeDescription", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 1)
        ) & (col("singleSexCourtTypeDescription") != "This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court.")
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourtTypeDescription","FAIL", f"singleSexCourtTypeDescription acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference = 1 and singleSexCourtTypeDescription != 'This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court.'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourtTypeDescription","PASS", "singleSexCourtTypeDescription acceptance criteria pass: all rows where M1.CourtPreference = 1 have singleSexCourtTypeDescription = 'This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court.'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourtTypeDescription", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# singleSexCourtTypeDescription - Check where M1.CourtPreference = 2 and singleSexCourtTypeDescription = "This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court."
#######################
def test_singleSexCourtTypeDescription_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 2)
            ).count() == 0:
            return TestResult("singleSexCourtTypeDescription", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 2)
        ) & (col("singleSexCourtTypeDescription") != "This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court.")
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourtTypeDescription","FAIL", f"singleSexCourtTypeDescription acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference = 2 and singleSexCourtTypeDescription != 'This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court.'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourtTypeDescription","PASS", "singleSexCourtTypeDescription acceptance criteria pass: all rows where M1.CourtPreference = 2 have singleSexCourtTypeDescription = 'This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court.'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourtTypeDescription", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# singleSexCourtTypeDescription - Check where M1.CourtPreference != 1 or 2 and singleSexCourtTypeDescription isNull
#######################
def test_singleSexCourtTypeDescription_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 1) &
            (col("CourtPreference") == 2)
            ).count() == 0:
            return TestResult("singleSexCourtTypeDescription", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("CourtPreference").isNotNull()) &
            (col("CourtPreference") == 1) &
            (col("CourtPreference") == 2)
        ) & (col("singleSexCourtTypeDescription").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("singleSexCourtTypeDescription","FAIL", f"singleSexCourtTypeDescription acceptance criteria failed: found {acceptance_critera.count()} rows where M1.CourtPreference != 1 or 2 and singleSexCourtTypeDescription is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("singleSexCourtTypeDescription","PASS", "singleSexCourtTypeDescription acceptance criteria pass: all rows where M1.CourtPreference != 1 or 2 and singleSexCourtTypeDescription is null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("singleSexCourtTypeDescription", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# inCameraCourt - Check where M1.inCamera = 1 and inCameraCourt = Yes
#######################
def test_inCameraCourt_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("inCamera").isNotNull()) &
            (col("inCamera") == 1)
            ).count() == 0:
            return TestResult("inCameraCourt", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("inCamera").isNotNull()) &
            (col("inCamera") == 1)
        ) & (col("inCameraCourt") != "Yes")
        )

        if acceptance_critera.count() != 0:
            return TestResult("inCameraCourt","FAIL", f"inCameraCourt acceptance criteria failed: found {acceptance_critera.count()} rows where M1.inCamera = 1 and inCameraCourt != Yes", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("inCameraCourt","PASS", "inCameraCourt acceptance criteria pass: all rows where M1.inCamera = 1 and inCameraCourt = Yes", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("inCameraCourt", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# inCameraCourt - Check where M1.inCamera = 0 and inCameraCourt = No
#######################
def test_inCameraCourt_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("inCamera").isNotNull()) &
            (col("inCamera") == 0)
            ).count() == 0:
            return TestResult("inCameraCourt", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("inCamera").isNotNull()) &
            (col("inCamera") == 0)
        ) & (col("inCameraCourt") != "No")
        )

        if acceptance_critera.count() != 0:
            return TestResult("inCameraCourt","FAIL", f"inCameraCourt acceptance criteria failed: found {acceptance_critera.count()} rows where M1.inCamera = 0 and inCameraCourt != No", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("inCameraCourt","PASS", "inCameraCourt acceptance criteria pass: all rows where M1.inCamera = 0 and inCameraCourt = No", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("inCameraCourt", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# inCameraCourt - Check inCameraCourt is not null
#######################
def test_inCameraCourt_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("inCameraCourt").isNotNull())
            ).count() == 0:
            return TestResult("inCameraCourt", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (col("inCameraCourt").isNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("inCameraCourt","FAIL", f"inCameraCourt acceptance criteria failed: found {acceptance_critera.count()} rows where inCameraCourt is null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("inCameraCourt","PASS", "inCameraCourt acceptance criteria pass: all rows have inCameraCourt is not null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("inCameraCourt", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# inCameraCourtDescription - Check where M1.InCamera = 1 and inCameraCourtDescription = "This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the appellants need for an in camera court."
#######################
def test_inCameraCourtDescription_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("inCamera").isNotNull()) &
            (col("inCamera") == "true")
            ).count() == 0:
            return TestResult("inCameraCourt", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("inCamera").isNotNull()) &
            (col("inCamera") == "true")
        ) & (col("inCameraCourtDescription") != "This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the appellants need for an in camera court.")
        )

        if acceptance_critera.count() != 0:
            return TestResult("inCameraCourtDescription","FAIL", f"inCameraCourtDescription acceptance criteria failed: found {acceptance_critera.count()} rows where M1.InCamera = 1 and inCameraCourtDescription != 'This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the appellants need for an in camera court.'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("inCameraCourtDescription","PASS", "inCameraCourtDescription acceptance criteria pass: all rows where M1.InCamera = 1 and inCameraCourtDescription = 'This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the appellants need for an in camera court.'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("inCameraCourtDescription", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 

#######################
# inCameraCourtDescription - Check where M1.InCamera != 1 and inCameraCourtDescription is omitted
#######################
def test_inCameraCourtDescription_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("inCamera") == "false")
            ).count() == 0:
            return TestResult("inCameraCourt", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (col("inCamera") == "false") & (col("inCameraCourtDescription").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("inCameraCourtDescription","FAIL", f"inCameraCourtDescription acceptance criteria failed: found {acceptance_critera.count()} where M1.InCamera != 1 and inCameraCourtDescription is not omitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("inCameraCourtDescription","PASS", "inCameraCourtDescription acceptance criteria pass: all rows where M1.InCamera != 1 have inCameraCourtDescription is omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("inCameraCourtDescription", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 
