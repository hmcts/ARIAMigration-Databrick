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

############################################################################################
#######################
#language tests Init code
#######################
def test_languages_init(json, M1_bronze):
    try:
        json = json.select(
            col("appealReferenceNumber"),
            col("appellantInterpreterLanguageCategory"),
            col("appellantInterpreterSpokenLanguage")
            # ,
            # col("appellantInterpreterSignLanguage")
        )

        M1_bronze = M1_bronze.select(
            col("CaseNo"),
            col("LanguageId")
        )

        test_df = json.join(
            M1_bronze,
            M1_bronze["CaseNo"] == json["appealReferenceNumber"],
            "inner"
).drop(M1_bronze["CaseNo"])

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("appellantInterpreterLanguageCategory, appellantInterpreterSpokenLanguage, appellantInterpreterSignLanguage", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state,inspect.stack()[0].function)
    
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
        cols = test_df.columns
        rows = test_df.collect()

        for row in rows:
            case_no = row['appealReferenceNumber']
            lang_id = row['LanguageId']
            
            if lang_id == 0:
                category = row['appellantInterpreterLanguageCategory'] if 'appellantInterpreterLanguageCategory' in cols else None
                if category is None or (isinstance(category, list) and len(category) == 0):
                    # results_list.append(f"PASS - {case_no}: ID 0 (No Interpreter)")
                    continue
                else:
                    results_list.append(f"FAIL - {case_no}: ID 0 expected null, found {category}")
                

            req = language_requirements.get(lang_id)
            if not req:
                results_list.append(f"FAIL - {case_no}: No requirement for ID {lang_id}")
                continue
            
            req_category, req_code, req_label, req_manual, req_desc = req
            field_name = "appellantInterpreterSpokenLanguage" if req_category == 'spokenLanguageInterpreter' else "appellantInterpreterSignLanguage"
            target_data = row[field_name] if field_name in cols else None

            if target_data is None:
                results_list.append(f"FAIL - {case_no}: ID {lang_id} field {field_name} is null")
                continue

            # ensure we are working with a dictionary
            d = target_data.asDict(recursive=True)

            actual_ref = d.get('languageRefData') or {}
            actual_val = actual_ref.get('value') or {}
            actual_code = actual_val.get('code')
            actual_label = actual_val.get('label')
            actual_manual = d.get('languageManualEntry') or []
            actual_desc = d.get('languageManualEntryDescription')

            # comparison
            errors = []
            if actual_code != req_code: 
                errors.append(f"Code: Expected '{req_code}', Found '{actual_code}'")
            if actual_label != req_label: 
                errors.append(f"Label: Expected '{req_label}', Found '{actual_label}'")
            if actual_manual != req_manual: 
                errors.append(f"ManualList: Expected {req_manual}, Found {actual_manual}")
            if actual_desc != req_desc: 
                errors.append(f"ManualDesc: Expected '{req_desc}', Found '{actual_desc}'")

            if not errors:
                # results_list.append(f"PASS - {case_no}: ID {lang_id} mapped correctly")
                continue
            else:
                # Joining with a clear separator for readability
                results_list.append(f"FAIL - {case_no} (ID {lang_id}): " + " | ".join(errors))

        if results_list != []:
            formatted_results = "|||".join(results_list)
            message = f"appellantInterpreterLanguageCategory, appellantInterpreterSpokenLanguage acceptance criteria failed: found {len(results_list)} rows which failed. {formatted_results}"
            return TestResult("appellantInterpreterLanguageCategory, appellantInterpreterSpokenLanguage","FAIL", message, test_from_state, inspect.stack()[0].function)
        else:
            message = f"appellantInterpreterLanguageCategory, appellantInterpreterSpokenLanguage acceptance criteria passed: all rows meet mapping document requirements."
            return TestResult("appellantInterpreterLanguageCategory, appellantInterpreterSpokenLanguage","PASS", message, test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("appellantInterpreterLanguageCategory, appellantInterpreterSpokenLanguage","FAIL", f"Crash in test: {str(e)}", test_from_state, inspect.stack()[0].function)

