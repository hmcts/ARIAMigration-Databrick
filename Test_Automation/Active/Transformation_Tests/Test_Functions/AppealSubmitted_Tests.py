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

#Temp solution : using variable below, when each testresult instance is created, to tag with where test run from
test_from_state = "appealSubmitted"

############################################################################################
#######################
#default mapping Init code
#######################
def test_default_mapping_init(json):
    try:
        test_df = json.select(
            "paAppealTypePaymentOption",
            "paAppealTypeAipPaymentOption",
            "additionalPaymentInfo"
        )
        return test_df, True
    except Exception as e:
        error_message = str(e)        
        return None,TestResult("DefaultMapping", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}",test_from_state,inspect.stack()[0].function)

def test_defaultValues(test_df,fields_to_exclude):
    try:
        expected_defaults = {
            "paAppealTypePaymentOption": "payLater",
            "paAppealTypeAipPaymentOption": "payLater",
            "additionalPaymentInfo": "This is an ARIA Migrated Case. The payment was made in ARIA and the payment history can be found in the case notes."
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


        return results_list
    except Exception as e:
        error_message = str(e)        
        return [TestResult("DefaultMapping", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)]

############################################################################################
#######################
#payment group
#######################
def test_payment_init(json, M1_bronze, M4_bronze):
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

        M4_bronze = M4_bronze.select(
            "CaseNo",
            "Amount",
            "TransactionTypeId",
            "TransactionId",
            "Amount"
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
        return None,TestResult("payment", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

# EA = refusalOfEu
# EU = euSettlementScheme
# HU = refusalOfHumanRights
# PA = protection
#######################
# paymentStatus - Where AppealType = EA,EU,HU,PA + M4.Amount is > 0 and paymentStatus = ‘Payment Pending’
#######################
def test_paymentStatus_test1(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("Amount") > 0)
            ).count() == 0:
            return TestResult("paymentStatus", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)
        
        case_window = Window.partitionBy("CaseNo")

        acceptance_critera = test_df.withColumn("Total_Amount", F.sum("Amount").over(case_window)) \
        .filter(
            (F.col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) & 
            (F.col("Total_Amount") > 0) &
            (F.col("paymentStatus") != "Payment Pending")
        )

        if acceptance_critera.count() != 0:
            failed_caseNo_count = acceptance_critera.select("appealReferenceNumber").distinct().count()
            return TestResult("paymentStatus","FAIL", f"paymentStatus acceptance criteria failed: found {failed_caseNo_count} CaseNumbers where AppealType is in EA,EU,HU,PA + M4.Amount is > 0 but paymentStatus != Payment Pending", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paymentStatus","PASS", "paymentStatus acceptance criteria pass: all rows where AppealType is in EA,EU,HU,PA + M4.Amount is > 0, paymentStatus != Payment Pending", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)        
        return TestResult("paymentStatus", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

#######################
# paymentStatus - Where AppealType = EA,EU,HU,PA + M4.Amount is 0 + TransactionTypeId == 19, Select MAX(TransactionId) which should be paymentStatus = ‘Payment Pending’ 
#######################
def test_paymentStatus_test2(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("Amount") == 0) &
            (col("TransactionTypeId") == 19) & # only one of these in data
            (col("TransactionId").isNotNull())
            ).count() == 0:
            return TestResult("paymentStatus", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        sum_window = Window.partitionBy("CaseNo")
        latest_window = Window.partitionBy("CaseNo").orderBy(F.col("TransactionId").desc())

        acceptance_critera = test_df \
            .withColumn("Total_Case_Amount", F.sum("Amount").over(sum_window)) \
            .withColumn("Transaction_row_id", F.row_number().over(latest_window)) \
            .filter(
                # The Population Scope
                (F.col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
                (F.col("TransactionTypeId") == 19) &
                (F.col("Transaction_row_id") == 1) & # check the MAX(TransactionId)
                (
                    # sum is 0, but it is NOT 'Payment Pending'
                    ((F.col("Total_Case_Amount") == 0) & (F.col("paymentStatus") != "Payment Pending"))
                )
            )

        if acceptance_critera.count() != 0:
            return TestResult("paymentStatus","FAIL", f"paymentStatus acceptance criteria failed: found {acceptance_critera.count()} rows where AppealType = EA,EU,HU,PA + M4.Amount is 0 + TransactionTypeId == 19, the MAX(TransactionId) does not have paymentStatus = ‘Payment Pending’", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paymentStatus","PASS", "paymentStatus acceptance criteria pass: all rows where AppealType = EA,EU,HU,PA + M4.Amount is 0 + TransactionTypeId == 19, the MAX(TransactionId) has paymentStatus = ‘Payment Pending’", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("paymentStatus", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)        

#######################
# paymentStatus - Where AppealType = EA,EU,HU,PA + M4.Amount is 0 + TransactionTypeId == 19, Select non MAX(TransactionId) which should be paymentStatus = ‘Paid 
#######################
def test_paymentStatus_test3(test_df):
    try:
        #Check we have Records To test
        if test_df.filter(
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
            (col("Amount") == 0) &
            (col("TransactionTypeId") == 19) & # only one of these in data
            (col("TransactionId").isNotNull())
            ).count() == 0:
            return TestResult("paymentStatus", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        sum_window = Window.partitionBy("CaseNo")
        latest_window = Window.partitionBy("CaseNo").orderBy(F.col("TransactionId").desc())

        acceptance_critera = test_df \
            .withColumn("Total_Case_Amount", F.sum("Amount").over(sum_window)) \
            .withColumn("Transaction_row_id", F.row_number().over(latest_window)) \
            .filter(
                # The Population Scope
                (F.col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
                (F.col("TransactionTypeId") == 19) &
                (F.col("Transaction_row_id") != 1) & # check non MAX(TransactionId)
                (
                    # sum is 0, but it is NOT 'Paid'
                    ((F.col("Total_Case_Amount") == 0) & (F.col("paymentStatus") != "Paid"))
                )
            )

        if acceptance_critera.count() != 0:
            return TestResult("paymentStatus","FAIL", f"paymentStatus acceptance criteria failed: found {acceptance_critera.count()} rows where AppealType = EA,EU,HU,PA + M4.Amount is 0 + TransactionTypeId == 19, the MAX(TransactionId) does not have paymentStatus = ‘Payment Pending’", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paymentStatus","PASS", "paymentStatus acceptance criteria pass: all rows where AppealType = EA,EU,HU,PA + M4.Amount is 0 + TransactionTypeId == 19, the MAX(TransactionId) has paymentStatus = ‘Payment Pending’", test_from_state, inspect.stack()[0].function)
    except Exception as e:
        error_message = str(e)
        return TestResult("paymentStatus", "FAIL",f"TEST FAILED WITH EXCEPTION :  Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)        

#######################
# paymentStatus - Where AppealType = EA,EU,HU,PA and paymentStatus is not null
#######################
def test_paymentStatus_test4(test_df):
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

        acceptance_critera = test_df.filter(
        (
            col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")
        ) &
            ~((F.col("DateCorrectFeeReceived").cast("date")).eqNullSafe((F.col("paidDate")).cast("date")))
        )

        if acceptance_critera.count() != 0:
            return TestResult("paidDate","FAIL", f"paidDate acceptance criteria failed: found {acceptance_critera.count()} where Appeal Type = EA,EU,HU,PA & M1.DateCorrectFeeReceived != paidDate'", test_from_state, inspect.stack()[0].function)
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
        #Check we have Records To test
        if test_df.filter(
            (~(col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")))
            ).count() == 0:
            return TestResult("paidAmount", "FAIL", "NO RECORDS TO TEST", test_from_state, inspect.stack()[0].function)

        case_window = Window.partitionBy("CaseNo")

        acceptance_critera = test_df.withColumn("Total_Amount", F.sum("Amount").over(case_window)) \
        .filter(
        (
            col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")
        ) & 
        (
            col("Total_Amount").cast("decimal(18,2)") != col("paidAmount").cast("decimal(18,2)")
        )
        )

        if acceptance_critera.count() != 0:
            failing_case_nos = acceptance_critera.select("appealReferenceNumber").distinct().count()
            return TestResult("paidAmount","FAIL", f"paidAmount acceptance criteria failed: found {failing_case_nos} where Appeal Type = EA,EU,HU,PA + Sum(M4.Amount) != paidAmount", test_from_state, inspect.stack()[0].function)
        else:
            return TestResult("paidAmount","PASS", "paidAmount acceptance criteria pass: all rows where Appeal Type = EA,EU,HU,PA + Sum(M4.Amount) == paidAmount", test_from_state, inspect.stack()[0].function)
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
            "Amount"
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
        return None,TestResult("payment", "FAIL",f"Failed to Setup Data for Test : Error : {error_message[:300]}", test_from_state, inspect.stack()[0].function)

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

        case_window = Window.partitionBy("CaseNo")

        acceptance_critera = test_df.withColumn("Total_Amount", F.sum("Amount").over(case_window)) \
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

        acceptance_critera = test_df.filter(
        (
            (col("AppealType").isin("refusalOfEu", "euSettlementScheme", "refusalOfHumanRights", "protection")) &
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

        acceptance_critera = test_df.filter(
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

