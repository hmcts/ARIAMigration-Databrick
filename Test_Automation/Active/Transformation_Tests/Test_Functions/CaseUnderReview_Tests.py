from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
import inspect
from pyspark.sql import Window
import re

#Import Test Results class
from models.test_result import TestResult

test_from_state = "caseUnderReview"

############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json, M1_silver):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "caseArgumentAvailable"
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
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state,inspect.stack()[0].function)


def test_defaultValues(test_df):
    try:
        results_list = []

        # Check for LR (Expected: 'Yes')
        fail_lr = test_df.filter(
            (col("dv_representation") == "LR") & 
            ((col("caseArgumentAvailable") != "Yes") | col("caseArgumentAvailable").isNull())
        )

        # Check for AIP (Expected: Null)
        fail_aip = test_df.filter(
            (col("dv_representation") == "AIP") & 
            (col("caseArgumentAvailable").isNotNull())
        )

        # Log LR Results
        if fail_lr.count() != 0:
            results_list.append(TestResult(
                "caseArgumentAvailable_LR", 
                "FAIL", 
                f"LR Mapping Error: Expected 'Yes', found {fail_lr.count()} records with wrong values/nulls", 
                test_from_state,
                inspect.stack()[0].function
            ))
        else:
            results_list.append(TestResult("caseArgumentAvailable_LR", "PASS", "LR Mapping correct", test_from_state, inspect.stack()[0].function))

        # Log AIP Results
        if fail_aip.count() != 0:
            results_list.append(TestResult(
                "caseArgumentAvailable_AIP", 
                "FAIL", 
                f"AIP Mapping Error: Expected Null, found {fail_aip.count()} records with data", 
                test_from_state,
                inspect.stack()[0].function
            ))
        else:
            results_list.append(TestResult("caseArgumentAvailable_AIP", "PASS", "AIP Mapping correctly Null", test_from_state, inspect.stack()[0].function))
            
        return results_list

    except Exception as e:
        error_message = str(e)        
        return [TestResult("DefaultMapping", "FAIL", f"EXCEPTION: {error_message[:300]}", test_from_state, inspect.stack()[0].function)]
    
############################################################################################
#######################
#hearingResponse Init code
#######################
def test_hearingResponse_init(json, M3_silver, M6_bronze):
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
            "inner"
        ).filter(col("additionalInstructionsTribunalResponse").isNotNull()).drop("CaseNo")

        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("additionalInstructionsTribunalResponse", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state,inspect.stack()[0].function)
    

def test_hearingResponse(test_df):
    try:
        # clean whitespace
        def normalize(text):
            if not text: return ""
            # normalize all spacing
            t = " ".join(text.split())
            # ( Mrs ) -> (Mrs)
            t = re.sub(r'\s*\(\s*', ' (', t) 
            t = re.sub(r'\s*\)\s*', ')', t)   
            return t.strip()

        grouped_df = test_df.groupBy("appealReferenceNumber").agg(
            first("additionalInstructionsTribunalResponse").alias("actual"),
            first("hearingCentre").alias("HC"),
            first("HearingDate").alias("HD"),
            first("HearingType").alias("HT"),
            first("CourtName").alias("CN"),
            first("ListType").alias("LT"),
            first("TimeEstimate").alias("TE"),
            first("Notes").alias("Notes"),
            collect_list(struct("Judge_Surname", "Judge_Forenames", "Judge_Title", "Required")).alias("M6_Officers")
        ).collect()

        results_list = []

        for row in grouped_df:
            actual_norm = normalize(row['actual'] or "")
            case_no = row['appealReferenceNumber']
            errors = []

            # expected field mapping
            expected_fields = {
                "Hearing Centre": str(row.HC or "N/A"),
                "Hearing Date": str(row.HD or "N/A"),
                "Hearing Type": str(row.HT or "N/A"),
                "Court": str(row.CN or "N/A"),
                "List Type": str(row.LT or "N/A"),
                "Estimated Duration": str(row.TE or "N/A")
            }

            # comparison
            for key, val in expected_fields.items():
                snippet = normalize(f"{key}: {val}")
                if snippet not in actual_norm:
                    errors.append(f"Field: {key} | Expected: '{snippet}' | Found: Data not found in response")

            # expected officer mapping
            for officer in row['M6_Officers']:
                if officer.Judge_Surname:
                    req_status = "Required" if officer.Required else "Not Required"
                    snippet = normalize(f"{officer.Judge_Surname} {officer.Judge_Forenames} ({officer.Judge_Title}) : {req_status}")
                    
                    if snippet not in actual_norm:
                        errors.append(f"Field: Judicial Officer | Expected: '{snippet}' | Found: Data not found in response")

            if errors:
                results_list.append(f"FAIL - {case_no}: " + " | ".join(errors))

        if results_list != []:
            formatted_results = "|||".join(results_list)
            message = f"additionalInstructionsTribunalResponse acceptance criteria failed: found {len(results_list)} rows which failed. {formatted_results}"
            return TestResult("additionalInstructionsTribunalResponse","FAIL", message, test_from_state, inspect.stack()[0].function)
        else:
            message = f"additionalInstructionsTribunalResponse acceptance criteria passed: all values are correctly mapped in accordance with the correct CaseStatus, M3 and M6 values."
            return TestResult("additionalInstructionsTribunalResponse","PASS", message, test_from_state, inspect.stack()[0].function)


    except Exception as e:
        return TestResult("additionalInstructionsTribunalResponse","FAIL", f"Crash in test: {str(e)}", test_from_state, inspect.stack()[0].function)
