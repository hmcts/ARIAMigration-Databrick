from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, 
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains
)
import inspect
from pyspark.sql.window import Window
from pyspark.sql import functions as F

#Import Test Results class
from models.test_result import TestResult

#Not working to set default value,
# TestResult.DEFAULT_TEST_FROM_STATE = "appealSubmitted"

test_from_state = "appealSubmitted"

############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json, M1_silver):
    try:
        test_df = json.select(
            "appealReferenceNumber",
            "paAppealTypePaymentOption",
            "paAppealTypeAipPaymentOption",
            "additionalPaymentInfo"
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

def test_AS_defaultValues(test_df,fields_to_exclude):
    try:
        results_list = []

        #AC1
        if field not in fields_to_exclude:                
            acceptance_critera_lr = test_df.filter(
                ((col("dv_representation") == "LR") & (col("paAppealTypePaymentOption") != "payLater"))
            )
            
            if acceptance_critera_lr.count() != 0:
                results_list.append(TestResult(
                    "paAppealTypePaymentOption", 
                    "FAIL", 
                    f"Failed to check Default Mapping for : paAppealTypePaymentOption - expected : 'payLater' - found {acceptance_critera_lr.count()} records not matching", 
                    test_from_state,
                    inspect.stack()[0].function
                ))
            else:
                results_list.append(TestResult(
                    "paAppealTypePaymentOption", 
                    "PASS", 
                    f"Checked Default Mapping for : paAppealTypePaymentOption - found correct value", 
                    test_from_state,
                    inspect.stack()[0].function
                ))


        #AC2
        if field not in fields_to_exclude:    
            acceptance_critera_aip = test_df.filter(
                ((col("dv_representation") == "AIP") & (col("paAppealTypeAipPaymentOption") != "payLater"))
            )
            
            if acceptance_critera_aip.count() != 0:
                results_list.append(TestResult(
                    "paAppealTypeAipPaymentOption", 
                    "FAIL", 
                    f"Failed to check Default Mapping for : paAppealTypeAipPaymentOption - expected : 'payLater' - found {acceptance_critera_aip.count()} records not matching", 
                    test_from_state,
                    inspect.stack()[0].function
                ))
            else:
                results_list.append(TestResult(
                    "paAppealTypeAipPaymentOption", 
                    "PASS", 
                    f"Checked Default Mapping for : paAppealTypeAipPaymentOption - found correct value", 
                    test_from_state,
                    inspect.stack()[0].function
                ))


        #AC3
        if field not in fields_to_exclude:    
            acceptance_critera_payment = test_df.filter(
                (col("additionalPaymentInfo") != "This is an ARIA Migrated Case. The payment was made in ARIA and the payment history can be found in the case notes.")
            )

            if acceptance_critera_payment.count() != 0:
                results_list.append(TestResult(
                    "additionalPaymentInfo", 
                    "FAIL", 
                    f"Failed to check Default Mapping for : paAppealTypeAipPaymentOption - expected : 'This is an ARIA Migrated Case. The payment was made in ARIA and the payment history can be found in the case notes.' - found {acceptance_critera_payment.count()} records not matching", 
                    test_from_state,
                    inspect.stack()[0].function
                ))
            else:
                results_list.append(TestResult(
                    "additionalPaymentInfo", 
                    "PASS", 
                    f"Checked Default Mapping for : acceptance_critera_payment - found correct value", 
                    test_from_state,
                    inspect.stack()[0].function
                ))
            
        return results_list
    except Exception as e:
        error_message = str(e)        
        return [TestResult("DefaultMapping", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)]

############################################################################################
#######################
#payment group
#######################
def test_payment_init(json, M1_bronze, M4_silver):
    try:
        json = json.select(
            "appealReferenceNumber",
            "paymentStatus",
            "rpDcAppealHearingOption",
            "paidDate",
            "paidAmount",
            "paymentDescription",
            "appealType"
        )

        M1_bronze = M1_bronze.select(
            "CaseNo",
            "VisitVisaType",
            "DateCorrectFeeReceived",
        )

        M4_silver = M4_silver.select(
            "CaseNo",
            "Amount",
            "TransactionTypeId",
            "TransactionId",
            "ReferringTransactionId",
            "Amount",
            "SumBalance",
            "SumTotalPay"
        )

        test_df = json.join(
            M1_bronze,
            json["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        ).join(
            M4_silver,
            json["appealReferenceNumber"] == M4_silver["CaseNo"],
            "inner" 
        ).drop(M1_bronze["CaseNo"])
        
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("payment", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

# EA = refusalOfEu
# EU = euSettlementScheme
# HU = refusalOfHumanRights
# PA = protection
#######################
# paymentStatus - Correct paymentStatus assignment
#######################
def test_paymentStatus_test1(test_df):
    try:
        # Start with SumBalance = 1
        test_df = test_df.filter(
            (col("SumBalance") == 1) &
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection"))
        )

        #Check we have Records To test
        if test_df.count() == 0:
            return TestResult("paymentStatus", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)
        
        # SELECT ReferringTransactionId FROM test_df WHERE TransactionTypeId IN (6,19
        excluded_ids = test_df.filter(F.col("TransactionTypeId").isin(6, 19)) \
                            .select(F.col("ReferringTransactionId").alias("ref_id")) \
                            .distinct()

        # Eliminate rows where TransactionID matches a ReferringTransactionId
        selected_rows = test_df.filter(F.col("SumBalance") == 1) \
            .join(excluded_ids, test_df.TransactionId == excluded_ids.ref_id, "left_anti")

        # Calculate SUM(Amount)
        case_window = Window.partitionBy("CaseNo")
        calculated_df = selected_rows.withColumn("Total_Amount", F.sum("Amount").over(case_window))

        # Select MAX(TransactionId)
        rank_window = Window.partitionBy("CaseNo").orderBy(F.col("TransactionId").desc())
        final_row_to_test = calculated_df.withColumn("rank", F.row_number().over(rank_window)).filter(F.col("rank") == 1)

        # Apply value logic:
        # IF sum > 0: Pending
        # IF sum = 0 AND Type = 19: Pending
        # ELSE: Paid
        acceptance_criteria = final_row_to_test.withColumn("Expected_Status", 
            F.when(F.col("Total_Amount") > 0, "Payment Pending")
            .when((F.col("Total_Amount") == 0) & (F.col("TransactionTypeId") == 19), "Payment Pending")
            .otherwise("Paid")
        )

        # Final comparison
        defects = acceptance_criteria.filter(
            (F.col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (F.upper(F.col("paymentStatus")) != F.upper(F.col("Expected_Status")))
        )

        if defects.count() != 0:
            return TestResult("paymentStatus","FAIL", f"paymentStatus acceptance criteria failed: found {defects.count()} case numbers where cases have been correctly selected (SumBalance = 1 and TransactionId does not equal ReferringTransactionId when TransactionTypeId is 6 or 19) and payment status is not as expected. This could be due to one of the following: 1) Sum(Amount) > 0, but paymentStatus != Payment pending. 2) Sum(Amount) = 0, TransactionTypeId = 19 with MAX(TransactionId), but paymentStatus != Payment pending. 3) Sum(Amount) = 0, TransactionTypeId != 19 with MAX(TransactionId), but paymentStatus != Paid.", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paymentStatus","PASS", "paymentStatus acceptance criteria pass: all case numbers where cases have been correctly selected (SumBalance = 1 and TransactionId does not equal ReferringTransactionId when TransactionTypeId is 6 or 19) match correctly to their correct status of Payment pending or Paid.", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)        
        return TestResult("paymentStatus", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
       
#######################
# paymentStatus - Where AppealType = EA,EU,HU,PA and paymentStatus is null
#######################
def test_paymentStatus_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")))
            ).count() == 0:
            return TestResult("paymentStatus", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection"))) &
            (col("paymentStatus").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("paymentStatus","FAIL", f"paymentStatus acceptance criteria failed: found {acceptance_critera.count()} rows where AppealType != EA,EU,HU,PA and paymentStatus is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paymentStatus","PASS", "paymentStatus acceptance criteria pass: all rows where AppealType != EA,EU,HU,PA have paymentStatus omitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("paymentStatus", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)    

# DC = deprivation
# RP = revocationOfProtection
#######################
# rpDcAppealHearingOption - Where AppealType = DC, RP + M1.VisitVisaType == 1 and rpDcAppealHearingOption = 'decisionWithoutHearing'
#######################
def test_rpDcAppealHearingOption_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("deprivation", "revocationOfProtection")) &
            (col("VisitVisaType") == 1)
            ).count() == 0:
            return TestResult("rpDcAppealHearingOption", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("AppealType").isin("deprivation", "revocationOfProtection")) &
            (col("VisitVisaType") == 1)
        ) &
            (col("rpDcAppealHearingOption") != "decisionWithoutHearing")
        )

        if acceptance_critera.count() != 0:
            return TestResult("rpDcAppealHearingOption","FAIL", f"rpDcAppealHearingOption acceptance criteria failed: found {acceptance_critera.count()} rows where AppealType = DC, RP + M1.VisitVisaType == 1 and rpDcAppealHearingOption != 'decisionWithoutHearing'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("rpDcAppealHearingOption","PASS", "rpDcAppealHearingOption acceptance criteria pass: all rows where AppealType = DC, RP + M1.VisitVisaType == 1 and rpDcAppealHearingOption = 'decisionWithoutHearing'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("rpDcAppealHearingOption", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)   

#######################
# rpDcAppealHearingOption - Where AppealType = DC, RP + M1.VisitVisaType == 2 and rpDcAppealHearingOption = 'decisionWithHearing'
#######################
def test_rpDcAppealHearingOption_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("deprivation", "revocationOfProtection")) &
            (col("VisitVisaType") == 2)
            ).count() == 0:
            return TestResult("rpDcAppealHearingOption", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("AppealType").isin("deprivation", "revocationOfProtection")) &
            (col("VisitVisaType") == 2)
        ) &
            (col("rpDcAppealHearingOption") != "decisionWithHearing")
        )

        if acceptance_critera.count() != 0:
            return TestResult("rpDcAppealHearingOption","FAIL", f"rpDcAppealHearingOption acceptance criteria failed: found {acceptance_critera.count()} rows where AppealType = DC, RP + M1.VisitVisaType == 1 and rpDcAppealHearingOption != 'decisionWithHearing'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("rpDcAppealHearingOption","PASS", "rpDcAppealHearingOption acceptance criteria pass: all rows where AppealType = DC, RP + M1.VisitVisaType == 1 and rpDcAppealHearingOption = 'decisionWithHearing'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("rpDcAppealHearingOption", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# rpDcAppealHearingOption - Where AppealType != DC, RP and rpDcAppealHearingOption is not null
#######################
def test_rpDcAppealHearingOption_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (~(col("AppealType").isin("deprivation", "revocationOfProtection")))
            ).count() == 0:
            return TestResult("rpDcAppealHearingOption", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (~(col("AppealType").isin("deprivation", "revocationOfProtection")))
        ) &
            (col("rpDcAppealHearingOption").isNotNull())
        )

        if acceptance_critera.count() != 0:
            return TestResult("rpDcAppealHearingOption","FAIL", f"rpDcAppealHearingOption acceptance criteria failed: found {acceptance_critera.count()} where AppealType != DC, RP and rpDcAppealHearingOption is not null'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("rpDcAppealHearingOption","PASS", "rpDcAppealHearingOption acceptance criteria pass: all rows where AppealType != DC, RP and rpDcAppealHearingOption is null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("rpDcAppealHearingOption", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# paidDate - Where Appeal Type = EA,EU,HU,PA + M1.DateCorrectFeeReceived == paidDate 
#######################
def test_paidDate_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("DateCorrectFeeReceived").isNotNull())
            ).count() == 0:
            return TestResult("paidDate", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)
        
        # SELECT CaseNo
        # FROM M4
        # WHERE 
        # TransactionID NOT IN 
        # (SELECT ReferringTransactionId FROM M4
        # WHERE TransactionTypeId NOT IN (6,19)) 
        # GROUP BY CaseNo
        # HAVING 
        #     SUM(CASE WHEN TransactionTypeID = 3 THEN 1 ELSE 0 END) > 0

        # --- STEP 1: Exclude ReferringTransactionIds where TransactionTypeId NOT IN (6, 19)
        excluded_ids = test_df.filter(~F.col("TransactionTypeId").isin(6, 19)) \
                            .select(F.col("ReferringTransactionId").alias("ref_id")) \
                            .filter(F.col("ref_id").isNotNull()) \
                            .distinct()

        # --- STEP 2: Filter rows where TransactionID should be excluded
        selected_rows = test_df.join(excluded_ids, test_df.TransactionId == excluded_ids.ref_id, "left_anti")

        # --- STEP 3: Apply the HAVING Clause & Total Amount, ensuring there is at least one TransactionTypeID = 3 in the group
        case_window = Window.partitionBy("CaseNo")

        final_selection = selected_rows.withColumn(
            "has_payment", F.max(F.when(F.col("TransactionTypeID") == 3, 1).otherwise(0)).over(case_window)
        ).filter(F.col("has_payment") == 1)

        # --- STEP 4: Select the latest TransactionId per Case ---
        rank_window = Window.partitionBy("CaseNo").orderBy(F.col("TransactionId").desc())

        final_row_to_test = final_selection.withColumn("rank", F.row_number().over(rank_window)) \
            .filter(F.col("rank") == 1)

        # --- STEP 5: Apply Value Logic (Identify Defects) ---
        acceptance_critera = final_row_to_test.filter(
            ~F.col("DateCorrectFeeReceived").cast("date").eqNullSafe(F.col("paidDate").cast("date"))
        )

        if acceptance_critera.count() != 0:
            return TestResult("paidDate","FAIL", f"paidDate acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type = EA,EU,HU,PA & M1.DateCorrectFeeReceived != paidDate", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paidDate","PASS", "paidDate acceptance criteria pass: all rows where Appeal Type = EA,EU,HU,PA + M1.DateCorrectFeeReceived == paidDate", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("paidDate", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# paidDate - Where Appeal Type != EA,EU,HU,PA and paidDate is not null
#######################
def test_paidDate_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")))
            ).count() == 0:
            return TestResult("paidDate", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")))
        ) &
            col("paidDate").isNotNull()
        )

        if acceptance_critera.count() != 0:
            return TestResult("paidDate","FAIL", f"paidDate acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type != EA,EU,HU,PA and paidDate is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paidDate","PASS", "paidDate acceptance criteria pass: all rows where Appeal Type != EA,EU,HU,PA, paidDate is null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("paidDate", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# paidAmount - Where Appeal Type = EA,EU,HU,PA + Sum(M4.Amount) == paidAmount
#######################
def test_paidAmount_test1(test_df):
    try:
        # Filter for SumTotalPay = 1
        test_df = test_df.filter(
            (col("SumTotalPay") == 1) &
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection"))
        )

        #Check we have Records To test
        if test_df.count() == 0:
            return TestResult("paidAmount", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # SELECT ReferringTransactionId FROM M4 WHERE TransactionTypeId NOT IN (6,19)
        m4_exclusion_ids = test_df.filter(~col("TransactionTypeId").isin(6, 19)) \
                                    .select(col("ReferringTransactionId").alias("ref_id")) \
                                    .distinct()

        # Eliminate rows where TransactionID matches a ReferringTransactionId
        clean_df = test_df.join(m4_exclusion_ids, 
                                test_df.TransactionId == m4_exclusion_ids.ref_id, 
                                "left_anti")
        
        # Calculate the sum(Amount) and a flag for whether they ever had a Type 3 (Payment) transaction
        case_window = Window.partitionBy("CaseNo")
        processed_df = clean_df.withColumn("Total_Amount", F.sum("Amount").over(case_window)) \
            .withColumn("Has_Payment_Tx", F.max(F.when(col("TransactionTypeId") == 3, 1).otherwise(0)).over(case_window))

        # Apply Acceptance Criteria - AppealType match AND Has_Payment_Tx > 0 AND Total_Amount matches paidAmount
        acceptance_critera = processed_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) & 
            (col("Has_Payment_Tx") > 0) & 
            (F.abs(col("Total_Amount")).cast("decimal(18,2)") != col("paidAmount").cast("decimal(18,2)"))
        )

        if acceptance_critera.count() != 0:
            failing_case_nos = acceptance_critera.select("appealReferenceNumber").distinct().count()
            return TestResult("paidAmount","FAIL", f"paidAmount acceptance criteria failed: found {failing_case_nos} where rows are selected correctly (SumBalance = 1, CaseNo where TransactionId does not equal ReferringTransactionId when TransactionTypeId is not 6 or 19 and with a TransactionTypeId equal to 3) but paidAmount is not equal to the absolute value of the total Amount.", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paidAmount","PASS", f"paidAmount acceptance criteria passed: all rows which are selected correctly (SumBalance = 1, CaseNo where TransactionId does not equal ReferringTransactionId when TransactionTypeId is not 6 or 19 and with a TransactionTypeId equal to 3) have a paidAmount equal to the absolute value of the total Amount.", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("paidAmount", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# paidAmount - Where Appeal Type != EA,EU,HU,PA and paidAmount is not null
#######################
def test_paidAmount_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")))
            ).count() == 0:
            return TestResult("paidAmount", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")))
        ) &
            col("paidAmount").isNotNull()
        )

        if acceptance_critera.count() != 0:
            return TestResult("paidAmount","FAIL", f"paidAmount acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type != EA,EU,HU,PA and paidAmount is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paidAmount","PASS", "paidAmount acceptance criteria pass: all rows where Appeal Type != EA,EU,HU,PA, paidAmount is null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("paidAmount", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# paymentDescription - Where Appeal Type = EA,EU,HU,PA + M1.VisitVisaType == 1 and paymentDescription = 'Appeal determined without a hearing'
#######################
def test_paymentDescription_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("VisitVisaType") == 1)
            ).count() == 0:
            return TestResult("paymentDescription", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        case_window = Window.partitionBy("CaseNo")

        acceptance_critera = test_df.filter(
            (
                (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
                (col("VisitVisaType") == 1)
            ) &
            (col("paymentDescription") != 'Appeal determined without a hearing')
        )

        if acceptance_critera.count() != 0:
            return TestResult("paymentDescription","FAIL", f"acceptance_critera.count() acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type = EA,EU,HU,PA + M1.VisitVisaType == 1 and paymentDescription != 'Appeal determined without a hearing'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paymentDescription","PASS", "paidAmount acceptance criteria pass: all rows where Appeal Type = EA,EU,HU,PA + M1.VisitVisaType == 1 have paymentDescription = 'Appeal determined without a hearing'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("paymentDescription", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# paymentDescription - Where Appeal Type = EA,EU,HU,PA + M1.VisitVisaType == 2 and paymentDescription = 'Appeal determined with a hearing'
#######################
def test_paymentDescription_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("VisitVisaType") == 2)
            ).count() == 0:
            return TestResult("paymentDescription", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        case_window = Window.partitionBy("CaseNo")

        acceptance_critera = test_df.filter(
            (
                (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
                (col("VisitVisaType") == 2)
            ) &
            (col("paymentDescription") != 'Appeal determined with a hearing')
        )

        if acceptance_critera.count() != 0:
            return TestResult("paymentDescription","FAIL", f"acceptance_critera.count() acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type = EA,EU,HU,PA + M1.VisitVisaType == 2 and paymentDescription != 'Appeal determined with a hearing'", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paymentDescription","PASS", "paidAmount acceptance criteria pass: all rows where Appeal Type = EA,EU,HU,PA + M1.VisitVisaType == 2 have paymentDescription = 'Appeal determined with a hearing'", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("paymentDescription", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# paymentDescription - Where Appeal Type != EA,EU,HU,PA and paidAmount is not null
#######################
def test_paymentDescription_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")))
            ).count() == 0:
            return TestResult("paidAmount", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")))
        ) &
            col("paymentDescription").isNotNull()
        )

        if acceptance_critera.count() != 0:
            return TestResult("paymentDescription","FAIL", f"paymentDescription acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type != EA,EU,HU,PA and paymentDescription is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paymentDescription","PASS", "paymentDescription acceptance criteria pass: all rows where Appeal Type != EA,EU,HU,PA, paymentDescription is null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("paymentDescription", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    
############################################################################################
#######################
#remission group
#######################
def test_remission_init(json, M1_bronze, M4_bronze):
    try:
        json = json.select(
            "appealReferenceNumber",
            "remissionDecision",
            "remissionDecisionReason",
            "amountRemitted",
            "amountLeftToPay",
            "appealType"
        )

        M1_bronze = M1_bronze.select(
            "CaseNo",
            "PaymentRemissionGranted"
        )

        M4_bronze = M4_bronze.select(
            "CaseNo",
            "Amount",
            "Status",
            "TransactionTypeId",
            "ReferringTransactionId",
            "SumTotalFee",
            "TransactionId"
        )

        test_df = json.join(
            M1_bronze,
            json["appealReferenceNumber"] == M1_bronze["CaseNo"],
            "inner"
        ).join(
            M4_bronze,
            json["appealReferenceNumber"] == M4_bronze["CaseNo"],
            "inner" 
        ).drop(M1_bronze["CaseNo"])
        
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("remission", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# remissionDecision - Where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 1 and remissionDecision = “approved”
#######################
def test_remissionDecision_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") == 1)
            ).count() == 0:
            return TestResult("remissionDecision", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") == 1)
        ) &
            (col("remissionDecision") != "approved")
        )

        if acceptance_critera.count() != 0:
            return TestResult("remissionDecision","FAIL", f"remissionDecision acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 1 and remissionDecision != “approved”", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("remissionDecision","PASS", "remissionDecision acceptance criteria pass: all rows where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 1 have remissionDecision = “approved”", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("remissionDecision", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    
#######################
# remissionDecision - Where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 2 and remissionDecision = rejected
#######################
def test_remissionDecision_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") == 2)
            ).count() == 0:
            return TestResult("remissionDecision", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") == 2)
        ) &
            (col("remissionDecision") != "rejected")
        )

        if acceptance_critera.count() != 0:
            return TestResult("remissionDecision","FAIL", f"remissionDecision acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 2 and remissionDecision != rejected", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("remissionDecision","PASS", "remissionDecision acceptance criteria pass: all rows where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 2 have remissionDecision = rejected", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("remissionDecision", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# remissionDecision - Where Appeal Type != EA,EU,HU,PA and remissionDecision is not null
#######################
def test_remissionDecision_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")))
            ).count() == 0:
            return TestResult("remissionDecision", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")))
        ) &
            col("remissionDecision").isNotNull()
        )

        if acceptance_critera.count() != 0:
            return TestResult("remissionDecision","FAIL", f"remissionDecision acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type != EA,EU,HU,PA and remissionDecision is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("remissionDecision","PASS", "remissionDecision acceptance criteria pass: all rows where Appeal Type != EA,EU,HU,PA, remissionDecision is null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("remissionDecision", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    
#######################
# remissionDecisionReason - Where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 1 and remissionDecisionReason = “This is a migrated case. The remission was granted.”
#######################
def test_remissionDecisionReason_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") == 1)
            ).count() == 0:
            return TestResult("remissionDecisionReason", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") == 1)
        ) &
            (col("remissionDecisionReason") != "This is a migrated case. The remission was granted.")
        )

        if acceptance_critera.count() != 0:
            return TestResult("remissionDecisionReason","FAIL", f"remissionDecisionReason acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 1 and remissionDecisionReason != “This is a migrated case. The remission was granted.”", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("remissionDecisionReason","PASS", "remissionDecisionReason acceptance criteria pass: all rows where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 1 have remissionDecisionReason = “This is a migrated case. The remission was granted.”", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("remissionDecisionReason", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    
#######################
# remissionDecisionReason - Where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 2 and remissionDecision = “This is a migrated case. The remission was rejected.”
#######################
def test_remissionDecisionReason_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") == 2)
            ).count() == 0:
            return TestResult("remissionDecisionReason", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") == 2)
        ) &
            (col("remissionDecisionReason") != "This is a migrated case. The remission was rejected.")
        )

        if acceptance_critera.count() != 0:
            return TestResult("remissionDecisionReason","FAIL", f"remissionDecisionReason acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 2 and remissionDecision != “This is a migrated case. The remission was rejected.”", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("remissionDecisionReason","PASS", "remissionDecisionReason acceptance criteria pass: all rows where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 2 have remissionDecision = “This is a migrated case. The remission was rejected.”", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("remissionDecisionReason", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# remissionDecisionReason - Where Appeal Type != EA,EU,HU,PA and remissionDecision is not null
#######################
def test_remissionDecisionReason_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")))
            ).count() == 0:
            return TestResult("remissionDecisionReason", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        acceptance_critera = test_df.filter(
        (
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")))
        ) &
            col("remissionDecisionReason").isNotNull()
        )

        if acceptance_critera.count() != 0:
            return TestResult("remissionDecisionReason","FAIL", f"remissionDecisionReason acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type != EA,EU,HU,PA and remissionDecisionReason is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("remissionDecisionReason","PASS", "remissionDecisionReason acceptance criteria pass: all rows where Appeal Type != EA,EU,HU,PA, remissionDecisionReason is null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("remissionDecisionReason", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# amountLeftToPay - Where Appeal Type = EA,EU,HU,PA + PaymentRemissionGranted == 1 and Sum(M4.Amount) == amountLeftToPay
#######################
def test_amountLeftToPay_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") == 1) &
            (col("Amount").isNotNull())
            ).count() == 0:
            return TestResult("amountLeftToPay", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)
        
        # --- STEP 1: Identify "Bad" IDs to exclude ---
        # SQL: SELECT ReferringTransactionId FROM m4 WHERE TransactionTypeId IN (6,19)
        excluded_ids = test_df.filter(F.col("TransactionTypeId").isin(6, 19)) \
                            .select(F.col("ReferringTransactionId").alias("ref_id")) \
                            .filter(F.col("ref_id").isNotNull()) \
                            .distinct()

        # --- STEP 2: Apply the Filter (SumTotalFee = 1 and Anti-Join) ---
        # SQL: WHERE SumTotalFee = 1 AND TransactionID NOT IN (...)
        selected_rows = test_df.filter(F.col("SumTotalFee") == 1) \
            .join(excluded_ids, test_df.TransactionId == excluded_ids.ref_id, "left_anti")

        case_window = Window.partitionBy("CaseNo")

        acceptance_critera = selected_rows.withColumn("Total_Amount", F.sum("Amount").over(case_window)) \
        .filter(
        (
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") == 1)
        ) & 
        (
            (col("Total_Amount").cast("decimal(18,2)") != col("amountLeftToPay").cast("decimal(18,2)"))
        )
        )

        if acceptance_critera.count() != 0:
            return TestResult("amountLeftToPay","FAIL", f"remissionDecisionReason acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 1 and Sum(M4.Amount) != amountLeftToPay", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("amountLeftToPay","PASS", "remissionDecisionReason acceptance criteria pass: all rows where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 1 have Sum(M4.Amount) == amountLeftToPay”", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("amountLeftToPay", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)
    
#######################
# amountLeftToPay - Where Appeal Type = EA,EU,HU,PA + PaymentRemissionGranted != 1 and amountLeftToPay is not null
#######################
def test_amountLeftToPay_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") != 1)
            ).count() == 0:
            return TestResult("amountLeftToPay", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # --- STEP 1: Identify "Bad" IDs to exclude ---
        # SQL: SELECT ReferringTransactionId FROM m4 WHERE TransactionTypeId IN (6,19)
        excluded_ids = test_df.filter(F.col("TransactionTypeId").isin(6, 19)) \
                            .select(F.col("ReferringTransactionId").alias("ref_id")) \
                            .filter(F.col("ref_id").isNotNull()) \
                            .distinct()

        # --- STEP 2: Apply the Filter (SumTotalFee = 1 and Anti-Join) ---
        # SQL: WHERE SumTotalFee = 1 AND TransactionID NOT IN (...)
        selected_rows = test_df.filter(F.col("SumTotalFee") == 1) \
            .join(excluded_ids, test_df.TransactionId == excluded_ids.ref_id, "left_anti")

        acceptance_critera = selected_rows.filter(
        (
            (col("PaymentRemissionGranted") != 1)
        ) &
            col("amountLeftToPay").isNotNull()
        )

        if acceptance_critera.count() != 0:
            return TestResult("amountLeftToPay","FAIL", f"amountLeftToPay acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type = EA,EU,HU,PA + PaymentRemissionGranted != 1 and amountLeftToPay is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("amountLeftToPay","PASS", "amountLeftToPay acceptance criteria pass: all rows where Appeal Type = EA,EU,HU,PA + PaymentRemissionGranted != 1 have amountLeftToPay is null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("amountLeftToPay", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# amountLeftToPay - Where Appeal Type != EA,EU,HU,PA + PaymentRemissionGranted = 1 and amountLeftToPay is not null
#######################
def test_amountLeftToPay_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection"))) &
            (col("PaymentRemissionGranted") == 1)
            ).count() == 0:
            return TestResult("amountLeftToPay", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        # --- STEP 1: Identify "Bad" IDs to exclude ---
        # SQL: SELECT ReferringTransactionId FROM m4 WHERE TransactionTypeId IN (6,19)
        excluded_ids = test_df.filter(F.col("TransactionTypeId").isin(6, 19)) \
                            .select(F.col("ReferringTransactionId").alias("ref_id")) \
                            .filter(F.col("ref_id").isNotNull()) \
                            .distinct()

        # --- STEP 2: Apply the Filter (SumTotalFee = 1 and Anti-Join) ---
        # SQL: WHERE SumTotalFee = 1 AND TransactionID NOT IN (...)
        selected_rows = test_df.filter(F.col("SumTotalFee") == 1) \
            .join(excluded_ids, test_df.TransactionId == excluded_ids.ref_id, "left_anti")

        acceptance_critera = selected_rows.filter(
        (
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection"))) &
            (col("PaymentRemissionGranted") == 1)
        ) &
            col("amountLeftToPay").isNotNull()
        )

        if acceptance_critera.count() != 0:
            return TestResult("amountLeftToPay","FAIL", f"amountLeftToPay acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type != EA,EU,HU,PA + PaymentRemissionGranted = 1 and amountLeftToPay is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("amountLeftToPay","PASS", "amountLeftToPay acceptance criteria pass: all rows where Appeal Type != EA,EU,HU,PA + PaymentRemissionGranted = 1 have amountLeftToPay is null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("amountLeftToPay", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# amountRemitted - Where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 1 and Sum(M4.Amount) == amountRemitted
#######################
def test_amountRemitted_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection"))) &
            (col("PaymentRemissionGranted") == 1)
            ).count() == 0:
            return TestResult("amountRemitted", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        test_df = test_df.filter(
            (col("TransactionTypeId") == 5) &
            (col("Status") != 3)
        )

        acceptance_critera = test_df.withColumn("Total_Amount", F.sum("Amount").over(case_window)) \
        .filter(
        (
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") == 1)
        ) & 
        (
            (col("Total_Amount").cast("decimal(18,2)") != col("amountRemitted").cast("decimal(18,2)"))
        )
        )

        if acceptance_critera.count() != 0:
            return TestResult("amountRemitted","FAIL", f"amountRemitted acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 1 and Sum(M4.Amount) != amountRemitted", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("amountRemitted","PASS", "amountRemitted acceptance criteria pass: all rows where Appeal Type = EA,EU,HU,PA + M1.PaymentRemissionGranted == 1 have Sum(M4.Amount) == amountRemitted", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("amountRemitted", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# amountRemitted - Where Appeal Type = EA,EU,HU,PA + PaymentRemissionGranted != 1 and amountRemitted is not null
#######################
def test_amountRemitted_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") != 1)
            ).count() == 0:
            return TestResult("amountRemitted", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        test_df = test_df.filter(
            (col("TransactionTypeId") == 5) &
            (col("Status") != 3)
        )

        acceptance_critera = test_df.filter(
        (
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("PaymentRemissionGranted") != 1)
        ) &
            col("amountRemitted").isNotNull()
        )

        if acceptance_critera.count() != 0:
            return TestResult("amountRemitted","FAIL", f"amountRemitted acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type = EA,EU,HU,PA + PaymentRemissionGranted != 1 and amountRemitted is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("amountRemitted","PASS", "amountRemitted acceptance criteria pass: all rows where Appeal Type = EA,EU,HU,PA + PaymentRemissionGranted != 1 have amountRemitted is null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("amountRemitted", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# amountRemitted - Where Appeal Type != EA,EU,HU,PA + PaymentRemissionGranted == 1 and amountRemitted is not null
#######################
def test_amountRemitted_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection"))) &
            (col("PaymentRemissionGranted") == 1)
            ).count() == 0:
            return TestResult("amountRemitted", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        test_df = test_df.filter(
            (col("TransactionTypeId") == 5) &
            (col("Status") != 3)
        )

        acceptance_critera = test_df.filter(
        (
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection"))) &
            (col("PaymentRemissionGranted") == 1)
        ) &
            col("amountRemitted").isNotNull()
        )

        if acceptance_critera.count() != 0:
            return TestResult("amountRemitted","FAIL", f"amountRemitted acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type != EA,EU,HU,PA + PaymentRemissionGranted == 1 and amountRemitted is not null", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("amountRemitted","PASS", "amountRemitted acceptance criteria pass: all rows where Appeal Type != EA,EU,HU,PA + PaymentRemissionGranted == 1 have amountRemitted is null", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("amountRemitted", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)