from Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted import paymentType

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from unittest.mock import patch
import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("appealSubmitted_paymentType") \
        .getOrCreate()


class TestPaymentType:
    CASE_NO_COLUMNS = StructType([
        StructField("CaseNo", StringType())
    ])

    SILVER_M1_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("dv_CCDAppealType", StringType()),
        StructField("dv_representation", StringType()),
        StructField("VisitVisatype", IntegerType()),
        StructField("DateCorrectFeeReceived", TimestampType())
    ])

    SILVER_M4_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("TransactionId", IntegerType()),
        StructField("TransactionTypeId", IntegerType()),
        StructField("Amount", DoubleType()),
        StructField("SumBalance", IntegerType()),
        StructField("SumTotalPay", IntegerType()),
        StructField("ReferringTransactionId", IntegerType())
    ])

    def payment_pending_df(self, spark, num_of_cases=0):
        PP_COLUMNS = StructType([
            StructField("CaseNo", StringType()),
            StructField("feeAmountGbp", StringType()),
            StructField("feeDescription", StringType()),
            StructField("feeWithHearing", StringType()),
            StructField("feeWithoutHearing", StringType()),
            StructField("paymentDescription", StringType()),
            StructField("feePaymentAppealType", StringType()),
            StructField("feeVersion", StringType()),
            StructField("decisionHearingFeeOption", StringType()),
            StructField("hasServiceRequestAlready", StringType())
        ])

        data_tuple = ("0", "Desc", "0", "0", "Desc", "Type", "0", "Option", "Yes")
        caseList = []

        for i in range(1, num_of_cases + 1):
            caseList.append((i,) + data_tuple)

        return spark.createDataFrame(caseList, PP_COLUMNS), spark.createDataFrame(((n,) for n in range(1, num_of_cases + 1)), self.CASE_NO_COLUMNS)

    def test_paymentStatus(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.paymentType.return_value = self.payment_pending_df(spark, 11)

            m1_data = [
                ("1", "EA", "AIP", 0, datetime(2000, 1, 1)),   # EA Case with valid condition - paymentStatus is 'Paid' set
                ("2", "EU", "LR", 0, datetime(2000, 2, 5)),    # EU Case with valid condition - paymentStatus is 'Paid' set
                ("3", "HU", "AIP", 0, datetime(2000, 3, 10)),  # HU Case with valid condition - paymentStatus is 'Paid' set
                ("4", "PA", "LR", 0, datetime(2000, 4, 15)),   # PA Case with valid condition - paymentStatus is 'Paid' set
                ("5", "RP", "AIP", 0, datetime(2000, 1, 1)),   # RP Case - none
                ("6", "EA", "AIP", 0, datetime(2000, 1, 1)),   # EA Case with SumBalance = 0 - none
                ("7", "EA", "AIP", 0, datetime(2000, 1, 1)),   # EA Case with ReferringTransactionId = TransactionId with Type in 6 - none
                ("8", "EA", "AIP", 0, datetime(2000, 1, 1)),   # EA Case with ReferringTransactionId = TransactionId with Type in 19 - none
                ("9", "EA", "AIP", 0, datetime(2000, 1, 1)),   # SUM(AMOUNT) > 0 = 'Payment Pending'
                ("10", "EA", "AIP", 0, datetime(2000, 1, 1)),  # SUM(AMOUNT) = 0 AND TransactionTypeId = 19 for MAX(TransactionId), = 'Payment Pending'
                ("11", "EA", "AIP", 0, datetime(2000, 1, 1))   # SUM(AMOUNT) = 0 AND TransactionTypeId = 1 for MAX(TransactionId), = 'Paid'
            ]
            m4_data = [
                ("98", 1, 6, 0.0, 1, 0, 1),   # TransactionTypeId = 6, ReferringTransationId = 1
                ("99", 1, 19, 0.0, 1, 0, 2),  # TransactionTypeId = 19, ReferringTransationId = 2
                ("1", 3, 1, 0.0, 1, 0, 3),    # valid condition
                ("2", 3, 1, 0.0, 1, 0, 3),    # valid condition
                ("3", 3, 1, 0.0, 1, 0, 3),    # valid condition
                ("4", 3, 1, 0.0, 1, 0, 3),    # valid condition
                ("5", 3, 1, 0.0, 1, 0, 3),    # valid condition
                ("6", 3, 1, 0.0, 0, 0, 3),    # SumBalance = 0
                ("7", 1, 1, 0.0, 1, 0, 3),    # TransactionId = 1
                ("8", 2, 1, 0.0, 1, 0, 3),    # TransactionId = 2
                ("9", 3, 1, 100.0, 1, 0, 3),  # Amount > 0
                ("10", 4, 1, 0.0, 1, 0, 3),   # Case 10 - TransactionTypeId = 1, TransactionId = 4
                ("10", 5, 19, 0.0, 1, 0, 2),  # Case 10 - TransactionTypeId = 19, TransactionId = 5
                ("11", 6, 19, 0.0, 1, 0, 2),  # Case 11 - TransactionTypeId = 19, TransactionId = 6
                ("11", 7, 1, 0.0, 1, 0, 3)    # Case 11 - TransactionTypeId = 1, TransactionId = 7
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame(m4_data, self.SILVER_M4_SCHEMA)

            df, df_audit = paymentType(silver_m1, silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("paymentStatus").collect()

            assert resultList[0][0] == "Paid" and resultList[1][0] == "Paid" and resultList[2][0] == "Paid" and resultList[3][0] == "Paid"
            assert resultList[4][0] is None and resultList[5][0] is None and resultList[6][0] is None and resultList[7][0] is None
            assert resultList[8][0] == "Payment Pending" and resultList[9][0] == "Payment Pending"
            assert resultList[10][0] == "Paid"

    def test_paAppealTypePaymentOption(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.paymentType.return_value = self.payment_pending_df(spark, 6)

            m1_data = [
                ("1", "EA", "LR", 0, datetime(2000, 1, 1)),   # EA and LR Case - none
                ("2", "EU", "LR", 0, datetime(2000, 1, 1)),   # EU and LR Case - none
                ("3", "HU", "LR", 0, datetime(2000, 1, 1)),   # HU and LR Case - none
                ("4", "PA", "LR", 0, datetime(2000, 1, 1)),   # PA and LR Case - sets paAppealTypePaymentOption
                ("5", "EA", "AIP", 0, datetime(2000, 1, 1)),  # EA and AIP Case - none
                ("6", "PA", "AIP", 0, datetime(2000, 1, 1))   # PA and AIP Case - none
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame([], self.SILVER_M4_SCHEMA)

            df, df_audit = paymentType(silver_m1, silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("paAppealTypePaymentOption").collect()

            assert resultList[0][0] is None and resultList[1][0] is None and resultList[2][0] is None
            assert resultList[3][0] == "payLater"
            assert resultList[4][0] is None and resultList[5][0] is None

    def test_paAppealTypeAipPaymentOption(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.paymentType.return_value = self.payment_pending_df(spark, 6)

            m1_data = [
                ("1", "EA", "LR", 0, datetime(2000, 1, 1)),   # EA and LR Case - none
                ("2", "EU", "LR", 0, datetime(2000, 1, 1)),   # EU and LR Case - none
                ("3", "HU", "LR", 0, datetime(2000, 1, 1)),   # HU and LR Case - none
                ("4", "PA", "LR", 0, datetime(2000, 1, 1)),   # PA and LR Case - none
                ("5", "EA", "AIP", 0, datetime(2000, 1, 1)),  # EA and AIP Case - none
                ("6", "PA", "AIP", 0, datetime(2000, 1, 1))   # PA and AIP Case - sets paAppealTypeAipPaymentOption
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame([], self.SILVER_M4_SCHEMA)

            df, df_audit = paymentType(silver_m1, silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("paAppealTypeAipPaymentOption").collect()

            assert resultList[0][0] is None and resultList[1][0] is None and resultList[2][0] is None
            assert resultList[3][0] is None and resultList[4][0] is None
            assert resultList[5][0] == "payLater"

    def test_rpDcAppealHearingOption(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.paymentType.return_value = self.payment_pending_df(spark, 7)

            m1_data = [
                ("1", "EA", "AIP", 1, datetime(2000, 1, 1)),  # EA Case - none
                ("2", "DC", "LR", 1, datetime(2000, 1, 1)),   # DC Case and VisitVistaType = 1 - decisionWithoutHearing
                ("3", "RP", "AIP", 1, datetime(2000, 1, 1)),  # RP Case and VisitVistaType = 1 - decisionWithoutHearing
                ("4", "DC", "LR", 2, datetime(2000, 1, 1)),   # DC Case and VisitVistaType = 2 - decisionWithHearing
                ("5", "RP", "AIP", 2, datetime(2000, 1, 1)),  # RP Case and VisitVistaType = 2 - decisionWithHearing
                ("6", "DC", "LR", 3, datetime(2000, 1, 1)),   # DC Case and VisitVistaType = 3 - none
                ("7", "RP", "LR", 3, datetime(2000, 1, 1))    # RP Case and VisitVistaType = 3 - none
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame([], self.SILVER_M4_SCHEMA)

            df, df_audit = paymentType(silver_m1, silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("rpDcAppealHearingOption").collect()

            assert resultList[0][0] is None
            assert resultList[1][0] == "decisionWithoutHearing" and resultList[2][0] == "decisionWithoutHearing"
            assert resultList[3][0] == "decisionWithHearing" and resultList[4][0] == "decisionWithHearing"
            assert resultList[5][0] is None and resultList[6][0] is None

    def test_paidDate(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.paymentType.return_value = self.payment_pending_df(spark, 5)

            m1_data = [
                ("1", "EA", "AIP", 0, datetime(2000, 1, 1)),   # EA Case - paidDate set
                ("2", "EU", "LR", 0, datetime(2000, 2, 5)),    # EU Case - paidDate set
                ("3", "HU", "AIP", 0, datetime(2000, 3, 10)),  # HU Case - paidDate set
                ("4", "PA", "LR", 0, datetime(2000, 4, 15)),   # PA Case - paidDate set
                ("5", "RP", "AIP", 0, datetime(2000, 1, 1))    # RP Case - none
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame([], self.SILVER_M4_SCHEMA)

            df, df_audit = paymentType(silver_m1, silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("paidDate").collect()

            assert resultList[0][0] == "2000-01-01" and resultList[1][0] == "2000-02-05"
            assert resultList[2][0] == "2000-03-10" and resultList[3][0] == "2000-04-15"
            assert resultList[4][0] is None

    def test_paidAmount(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.paymentType.return_value = self.payment_pending_df(spark, 9)

            m1_data = [
                ("1", "EA", "AIP", 0, datetime(2000, 1, 1)),   # EA Case with valid condition - paidAmount set
                ("2", "EU", "LR", 0, datetime(2000, 2, 5)),    # EU Case with valid condition - paidAmount set
                ("3", "HU", "AIP", 0, datetime(2000, 3, 10)),  # HU Case with valid condition - paidAmount set
                ("4", "PA", "LR", 0, datetime(2000, 4, 15)),   # PA Case with valid condition - paidAmount set
                ("5", "RP", "AIP", 0, datetime(2000, 1, 1)),   # RP Case - none
                ("6", "EA", "AIP", 0, datetime(2000, 1, 1)),   # EA Case with SumTotalPay = 0 - none
                ("7", "EA", "AIP", 0, datetime(2000, 1, 1)),   # EA Case with ReferringTransactionId = TransactionId with Type in 6 - none
                ("8", "EA", "AIP", 0, datetime(2000, 1, 1)),   # EA Case with ReferringTransactionId = TransactionId with Type in 19 - none
                ("9", "EA", "AIP", 0, datetime(2000, 1, 1))    # EA Case with valid condition and multiple transactions - sum paidAmount set
            ]
            m4_data = [
                ("98", 1, 6, 100.0, 0, 1, 1),   # TransactionTypeId = 6, ReferringTransationId = 1
                ("99", 1, 19, 100.0, 0, 1, 2),  # TransactionTypeId = 19, ReferringTransationId = 2
                ("1", 3, 1, 100.0, 0, 1, 3),    # valid condition
                ("2", 3, 1, 100.0, 0, 1, 3),    # valid condition
                ("3", 3, 1, 100.0, 0, 1, 3),    # valid condition
                ("4", 3, 1, 100.0, 0, 1, 3),    # valid condition
                ("5", 3, 1, 100.0, 0, 1, 3),    # valid condition
                ("6", 3, 1, 100.0, 0, 0, 3),    # SumTotalPay = 0
                ("7", 1, 1, 100.0, 0, 1, 3),    # TransactionId = 1
                ("8", 2, 1, 100.0, 0, 1, 3),    # TransactionId = 2
                ("9", 3, 1, 100.0, 0, 1, 3),    # valid condition for same case
                ("9", 3, 1, 150.0, 0, 1, 3),    # valid condition for same case
                ("9", 3, 1, 250.0, 0, 1, 3)     # valid condition for same case
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame(m4_data, self.SILVER_M4_SCHEMA)

            df, df_audit = paymentType(silver_m1, silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("paidAmount").collect()

            assert resultList[0][0] == 100 and resultList[1][0] == 100 and resultList[2][0] == 100 and resultList[3][0] == 100
            assert resultList[4][0] is None and resultList[5][0] is None
            assert resultList[6][0] is None and resultList[7][0] is None
            assert resultList[8][0] == 500

    def test_additionalPaymentInfo(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.paymentType.return_value = self.payment_pending_df(spark, 5)

            m1_data = [
                ("1", "EA", "AIP", 0, datetime(2000, 1, 1)),   # EA Case - additionalPaymentInfo set
                ("2", "EU", "LR", 0, datetime(2000, 1, 1)),    # EU Case - additionalPaymentInfo set
                ("3", "HU", "AIP", 0, datetime(2000, 1, 1)),  # HU Case - additionalPaymentInfo set
                ("4", "PA", "LR", 0, datetime(2000, 1, 1)),   # PA Case - additionalPaymentInfo set
                ("5", "RP", "AIP", 0, datetime(2000, 1, 1))    # RP Case - none
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame([], self.SILVER_M4_SCHEMA)

            df, df_audit = paymentType(silver_m1, silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("additionalPaymentInfo").collect()

            expected_result_string = "This is an ARIA Migrated Case. The payment was made in ARIA and the payment history can be found in the case notes."

            assert resultList[0][0] == expected_result_string and resultList[1][0] == expected_result_string
            assert resultList[2][0] == expected_result_string and resultList[3][0] == expected_result_string
            assert resultList[4][0] is None
