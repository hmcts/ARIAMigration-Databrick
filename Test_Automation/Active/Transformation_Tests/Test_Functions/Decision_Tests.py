from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
import inspect

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

#Import Test Results class
from models.test_result import TestResult

#Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "decision"

############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "scheduleOfIssuesAgreement",
            "scheduleOfIssuesDisagreementDescription",
            "immigrationHistoryAgreement",
            "immigrationHistoryDisagreementDescription",
            "hmcts",
            "stitchingStatus",
            "bundleConfiguration",
            "decisionAndReasonsAvailable",
            "caseBundles"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

def test_dec_defaultValues(test_df, fields_to_exclude):
    try:
        expected_defaults = {
            "scheduleOfIssuesAgreement": "No",
            "scheduleOfIssuesDisagreementDescription": "This is a migrated ARIA case. Please see the documents for information on the schedule of issues.",
            "immigrationHistoryAgreement": "No",
            "immigrationHistoryDisagreementDescription": "This is a migrated ARIA case. Please see the documents for information on the immigration history.",
            "stitchingStatus": "DONE",
            "bundleConfiguration": "iac-hearing-bundle-config.yaml",
            "decisionAndReasonsAvailable": "No"

        }

        expected_arrays = {
        #    "hmcts":[userImage:hmcts.png],
           "caseBundles": None
        }
        
        results_list = []

        for field, expected in expected_defaults.items():
            if field in fields_to_exclude:
                continue
            condition = (col(field) != expected)
            if test_df.filter(condition).count() > 0:
                results_list.append(TestResult(
                    field, 
                    "FAIL", 
                    f"Failed to check Default Mapping for : {field} - expected : {expected} - found {str(test_df.filter(condition).count())} records not matching", 
                    test_from_state,
                    inspect.stack()[0].function
                ))
            else:
                results_list.append(TestResult(
                    field, 
                    "PASS", 
                    f"Checked Default Mapping for : {field} - found correct value : {expected}", 
                    test_from_state,
                    inspect.stack()[0].function
                ))

        for field, contains_val in expected_arrays.items():
            if field in fields_to_exclude:
                continue
            if contains_val:
                condition = (~array_contains(col(field), contains_val))
            else:
                condition = (size(col(field)) != 0)
                
            if test_df.filter(condition).count() > 0:
                results_list.append(TestResult(
                    field, 
                    "FAIL", 
                    f"Failed to check Default Mapping for : {field} - expected : {expected} - found {str(test_df.filter(condition).count())} records not matching", 
                    test_from_state,
                    inspect.stack()[0].function
                ))
            else:
                results_list.append(TestResult(
                    field, 
                    "PASS", 
                    f"Checked Default Mapping for : {field} - found correct value : {expected}", 
                    test_from_state,
                    inspect.stack()[0].function
                ))

        return results_list
    except Exception as e:
        error_message = str(e)        
        return [TestResult("DefaultMapping", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)]

############################################################################################
#######################
#hearingDetails Init code
#######################
def test_hearingDetails_init(json, M3_bronze, bll):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "listCaseHearingLength",
            "listCaseHearingDate",
            "listCaseHearingCentre",
            "listCaseHearingCentreAddress"
        )

        M3_bronze = M3_bronze.select(
            "CaseNo",
            "TimeEstimate",
            "CaseStatus",
            "HearingCentre",
            "HearingDate",
            "StartTime",
            "StatusId"
        )

        test_df = test_df.join(
            M3_bronze,
            test_df["appealReferenceNumber"] == M3_bronze["CaseNo"],
            "inner"
        ).join(
            bll,
            M3_bronze["HearingCentre"] == bll["ListedCentre"],
            "left"
        ).drop(M3_bronze["CaseNo"]
        ).drop(bll["listCaseHearingCentre"]
        ).drop(bll["listCaseHearingCentreAddress"]
        )
        
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("hearingDetails", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
#listCaseHearingLength code
#######################
def test_listCaseHearingLength(test_df):
    try:
        # Filter where CaseStatus is 37/38
        target_records = test_df.filter(col("CaseStatus").isin(37, 38))

        if target_records.count() == 0:
            return TestResult("listingLength", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusId"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        # Rounding: divide by 30, round to nearest whole number, then multiply by 30
        final_df = winning_records.withColumn(
            "calculated_length", (F.round(F.col("TimeEstimate") / 30) * 30).cast("int")
        ).withColumn(
            "expected_length", F.greatest(F.lit(30), F.col("calculated_length"))
        )

        acceptance_critera = final_df.filter(F.col("listCaseHearingLength") != F.col("expected_length"))

        if acceptance_critera.count() > 0:
            return TestResult("listCaseHearingLength", "FAIL", f"listCaseHearingLength acceptance criteria failed: found {acceptance_critera.count()} rounding mismatches. JSON does not match nearest 30min increment.", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("listCaseHearingLength", "PASS", "listCaseHearingLength acceptance criteria passed: all listCaseHearingDate values correctly rounded to nearest 30m increment from Max Status row.", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("listCaseHearingLength", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
#listCaseHearingDate code
#######################
def test_listCaseHearingDate(test_df):
    try:
        # Filter where CaseStatus is 37/38
        target_records = test_df.filter(col("CaseStatus").isin(37, 38))

        if target_records.count() == 0:
            return TestResult("listCaseHearingDate", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        final_df = winning_records.withColumn("expected_hearing_date", 
        F.to_timestamp(
            F.concat(
                F.date_format(F.col("HearingDate"), "yyyy-MM-dd"), 
                F.lit(" "), 
                F.date_format(F.col("StartTime"), "HH:mm:ss")
            )
        ))

        acceptance_critera = final_df.filter(
            F.col("listCaseHearingDate").cast("timestamp") != F.col("expected_hearing_date")
        )

        if acceptance_critera.count() > 0:
            return TestResult("listCaseHearingDate", "FAIL", f"listCaseHearingDate acceptance criteria failed: found {acceptance_critera.count()} mismatches between listCaseHearingDate and M3.HearingDate and StartTime", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("listCaseHearingDate", "PASS", "listCaseHearingDate acceptance criteria passed: all listCaseHearingDate values correctly match between listCaseHearingDate and M3.HearingDate and StartTime", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("listCaseHearingDate", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
#listCaseHearingCentre & listCaseHearingCentreAddress code
#######################
def test_listCaseHearingCentre_Address(test_df, spark):
    try:
        # Filter where CaseStatus is 37/38
        target_records = test_df.filter(col("CaseStatus").isin(37, 38))

        if target_records.count() == 0:
            return TestResult("listCaseHearingCentre, listCaseHearingCentreAddress", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # Filter by max Status
        window_spec = Window.partitionBy("appealReferenceNumber").orderBy(F.desc("StatusID"))

        # Get the first Record per Case
        winning_records = target_records.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        winning_records = winning_records.select("appealReferenceNumber", "HearingCentre", "listCaseHearingCentre", "listCaseHearingCentreAddress")

        # Mapping requirements
        mapping_data = [
            ("Alloa Sheriff Court", "alloaSherrif", "Alloa Sheriff Court, 47 Drysdale Street, Alloa, FK10 1JA"),
            ("Belfast - Laganside", "belfast", "Belfast Laganside Court, 45 Donegall Quay, BT1 3LL"),
            ("Birmingham IAC (Priory Courts)", "birmingham", "Birmingham Civil And Family Justice Centre, Priory Courts, 33 Bull Street, B4 6DS"),
            ("Birmingham IAC Sheldon Court", "birmingham", "Birmingham Civil And Family Justice Centre, Priory Courts, 33 Bull Street, B4 6DS"),
            ("Bradford", "bradford", "Bradford Tribunal Hearing Centre, Rushton Avenue, BD3 7BH"),
            ("Bradford Magistrates Court", "bradfordKeighley", "Bradford and Keighley Magistrates Court and Family Court, The Tyrls, PO Box 187, BD1 1JL"),
            ("Coventry Magistrates' Court IAC", "coventry", "Coventry Magistrates Court, Little Park Street, CV1 2SQ"),
            ("Glasgow (Eagle Building)", "glasgowTribunalsCentre", "Atlantic Quay - Glasgow, 20 York Street, Glasgow, G2 8GT"),
            ("Glasgow (Tribunals Centre)", "glasgowTribunalsCentre", "Atlantic Quay - Glasgow, 20 York Street, Glasgow, G2 8GT"),
            ("Harmondsworth", "harmondsworth", "Harmondsworth Tribunal Hearing Centre, Colnbrook Bypass, UB7 0HB"),
            ("Harmondsworth (HX)", "harmondsworth", "Harmondsworth Tribunal Hearing Centre, Colnbrook Bypass, UB7 0HB"),
            ("Hatton Cross", "hattonCross", "Hatton Cross Tribunal Hearing Centre, York House And Wellington House, 2-3 Dukes Green, Feltham, Middlesex, TW14 0LS"),
            ("Hendon Magistrates Court (HX)", "hendon", "Hendon Magistrates Court, The Court House, The Hyde, NW9 7BY"),
            ("Hendon Magistrates Court (TH)", "hendon", "Hendon Magistrates Court, The Court House, The Hyde, NW9 7BY"),
            ("Manchester (Piccadilly)", "manchester", "Manchester Tribunal Hearing Centre - Piccadilly Exchange, Piccadilly Plaza, M1 4AH"),
            ("Newcastle CFCTC", "newcastle", "Newcastle Civil And Family Courts And Tribunals Centre, Barras Bridge, Newcastle-Upon-Tyne, NE1 8QF"),
            ("Newport (Columbus House)", "newport", "Newport Tribunal Centre - Columbus House, Langstone Business Park, Newport, NP18 2LX"),
            ("North Tyneside Magistrates Court", "nthTyneMags", "North Tyneside Magistrates Court, Tynemouth Road, The Court House, NE30 1AG"),
            ("Nottingham Justice Centre", "nottingham", "Nottingham Magistrates Court, Carrington Street, NG2 1EE"),
            ("Taylor House", "taylorHouse", "Taylor House Tribunal Hearing Centre, Rosebery Avenue, EC1R 4QU"),
            ("Yarl's Wood", "yarlsWood", "Yarls Wood Immigration And Asylum Hearing Centre, Twinwood Road, MK44 1FD")
        ]

        schema = StructType([
            StructField("ref_HearingCentre", StringType(), True),
            StructField("ref_CentreCode", StringType(), True),
            StructField("ref_Address", StringType(), True)
        ])
        
        # Create reference dataframe
        ref_df = spark.createDataFrame(mapping_data, schema)
        
        # Comparison dataframe
        comparison_df = winning_records.join(
            ref_df, 
            winning_records["HearingCentre"] == ref_df["ref_HearingCentre"], 
            "left"
        )

        def clean_address(col_name):
            return F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.col(col_name), r'\u00A0', ' '), # Handle non-breaking spaces
                    r'\s+', ' '                                       # Squash multiple spaces to one
                )
            )

        acceptance_critera = comparison_df.filter(
            (F.col("ref_HearingCentre").isNotNull()) & 
            (
                (F.col("listCaseHearingCentre") != F.col("ref_CentreCode")) | 
                (clean_address("listCaseHearingCentreAddress") != clean_address("ref_Address"))
            )
        )

        if acceptance_critera.count() > 0:
            return TestResult("listCaseHearingCentre,listCaseHearingCentreAddress", "FAIL", f"listCaseHearingCentre,listCaseHearingCentreAddress acceptance criteria failed: found {acceptance_critera.count()} mismatches between listCaseHearingCentre,listCaseHearingCentreAddress and M3.HearingCentre and address", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("listCaseHearingCentre,listCaseHearingCentreAddress", "PASS", "listCaseHearingDate acceptance criteria passed: all listCaseHearingCentre,listCaseHearingCentreAddress and M3.HearingCentre and address values correctly match", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        return TestResult("listCaseHearingCentre,listCaseHearingCentreAddress", "FAIL", f"TEST FAILED WITH EXCEPTION :  Error : {str(e)[:300]}", test_from_state, inspect.stack()[0].function)

############################################################################################
#######################
#general Init code
#######################
def test_general_init(json, M1_bronze, M2_bronze):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "bundleFileNamePrefix"
        )

        M1_bronze = M1_bronze.select(
            "CaseNo"
        )

        M2_bronze = M2_bronze.select(
            col("CaseNo").alias("M2_CaseNo"),
            "Appellant_Name"
        )

        test_df = test_df.join(
            M1_bronze,
            test_df["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        ).join(
            M2_bronze,
            test_df["appealReferenceNumber"] == M2_bronze["M2_CaseNo"],
            "inner"
        ).drop("M2_CaseNo")
                
        
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("hearingDetails", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    

#######################
#bundleFileNamePrefix - Concatenate to the format 'CaseNo-Appellant_Name' replacing the '/' from CaseNo with ' ' (space) e.g "XX 00000 0000-Smith"
#######################
def test_bundleFileNamePrefix(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            col("bundleFileNamePrefix").isNotNull()
            ).count() == 0:
            return TestResult("bundleFileNamePrefix", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)
        
        final_df = test_df.withColumn(
            "expected_prefix",
            F.concat(
                F.regexp_replace(F.col("appealReferenceNumber"), "/", " "),
                F.lit("-"),
                F.col("Appellant_Name")
            )
        )

        acceptance_criteria = final_df.filter(
            col("bundleFileNamePrefix") != col("expected_prefix")
        )

        if acceptance_criteria.count() != 0:
            return TestResult("bundleFileNamePrefix","FAIL", f"bundleFileNamePrefix acceptance criteria failed: found {acceptance_criteria.count()} rows where bundleFileNamePrefix does not match with the required format: 'CaseNo-Appellant_Name'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("bundleFileNamePrefix","PASS", "bundleFileNamePrefix acceptance criteria pass: all rows have bundleFileNamePrefix matching with the required format: 'CaseNo-Appellant_Name'", test_from_state, inspect.stack()[0].function)

    except Exception as e:
        error_message = str(e)
        return TestResult("bundleFileNamePrefix", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function) 










