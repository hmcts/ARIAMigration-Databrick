from Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted import remissionTypes

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from unittest.mock import MagicMock, patch
import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("appealSubmitted_remissionType") \
        .getOrCreate()


class TestAppealSubmittedRemissionType:
    CASE_NO_COLUMNS = StructType([
        StructField("CaseNo", StringType())
    ])

    SILVER_M1_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("dv_CCDAppealType", StringType()),
        StructField("dv_representation", StringType()),
        StructField("PaymentRemissionGranted", IntegerType())
    ])

    SILVER_M4_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("TransactionId", IntegerType()),
        StructField("TransactionTypeId", IntegerType()),
        StructField("Status", IntegerType()),
        StructField("Amount", DoubleType()),
        StructField("SumTotalFee", IntegerType()),
        StructField("ReferringTransactionId", IntegerType())
    ])

    def payment_pending_df(self, spark, num_of_cases=0):
        case_no_df = spark.createDataFrame(((n,) for n in range(1, num_of_cases + 1)), self.CASE_NO_COLUMNS)

        return case_no_df, case_no_df

    def test_remissionDecision(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.remissionTypes.return_value = self.payment_pending_df(spark, 7)

            m1_data = [
                ("1", "EA", "AIP", 1),  # EA Case - remissionDecision approved
                ("2", "EU", "LR", 1),   # EU Case - remissionDecision approved
                ("3", "HU", "AIP", 1),  # HU Case - remissionDecision approved
                ("4", "PA", "LR", 1),   # PA Case - remissionDecision approved
                ("5", "RP", "AIP", 1),  # RP Case - none
                ("6", "EA", "AIP", 0),  # PaymentRemissionGranted = 0 - none
                ("7", "EA", "AIP", 2)   # PaymentRemissionGranted = 2 - remissionDecision rejected
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame([], self.SILVER_M4_SCHEMA)

            df, df_audit = remissionTypes(silver_m1, MagicMock(), silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("remissionDecision").collect()

            assert resultList[0][0] == "approved" and resultList[1][0] == "approved"
            assert resultList[2][0] == "approved" and resultList[3][0] == "approved"
            assert resultList[4][0] is None and resultList[5][0] is None
            assert resultList[6][0] == "rejected"

    def test_remissionDecisionReason(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.remissionTypes.return_value = self.payment_pending_df(spark, 7)

            m1_data = [
                ("1", "EA", "AIP", 1),  # EA Case - remissionDecision approved
                ("2", "EU", "LR", 1),   # EU Case - remissionDecision approved
                ("3", "HU", "AIP", 1),  # HU Case - remissionDecision approved
                ("4", "PA", "LR", 1),   # PA Case - remissionDecision approved
                ("5", "RP", "AIP", 1),  # RP Case - none
                ("6", "EA", "AIP", 0),  # PaymentRemissionGranted = 0 - none
                ("7", "EA", "AIP", 2)   # PaymentRemissionGranted = 2 - rejected
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame([], self.SILVER_M4_SCHEMA)

            df, df_audit = remissionTypes(silver_m1, MagicMock(), silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("remissionDecisionReason").collect()

            expected_approved_string = "This is a migrated case. The remission was granted."
            expected_rejected_string = "This is a migrated case. The remission was rejected."

            assert resultList[0][0] == expected_approved_string and resultList[1][0] == expected_approved_string
            assert resultList[2][0] == expected_approved_string and resultList[3][0] == expected_approved_string
            assert resultList[4][0] is None and resultList[5][0] is None
            assert resultList[6][0] == expected_rejected_string

    def test_amountRemitted(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.remissionTypes.return_value = self.payment_pending_df(spark, 11)

            m1_data = [
                ("1", "EA", "AIP", 1),   # EA Case and valid conditions - amountRemitted set
                ("2", "EU", "LR", 1),    # EU Case and valid conditions - amountRemitted set
                ("3", "HU", "AIP", 1),   # HU Case and valid conditions - amountRemitted set
                ("4", "PA", "LR", 1),    # PA Case and valid conditions - amountRemitted set
                ("5", "RP", "AIP", 1),   # RP Case and valid conditions - none
                ("6", "EA", "AIP", 0),   # PaymentRemissionGranted = 0 - none
                ("7", "EA", "AIP", 2),   # PaymentRemissionGranted = 2 - none
                ("8", "EA", "AIP", 1),   # TransactionTypeId != 5 - none
                ("9", "EA", "AIP", 1),   # Status == 3 - none
                ("10", "EA", "AIP", 1),  # TransactionTypeId != 5 and Status == 3 - none
                ("11", "EA", "AIP", 1)   # Multiple matching conditions - sum of amountRemitted
            ]

            m4_data = [
                ("1", 1, 5, 1, 100.0, 1, 1),   # EA Case
                ("2", 1, 5, 1, 100.0, 1, 1),   # EU Case
                ("3", 1, 5, 1, 100.0, 1, 1),   # HU Case
                ("4", 1, 5, 1, 100.0, 1, 1),   # PA Case
                ("5", 1, 5, 1, 100.0, 1, 1),   # RP Case
                ("6", 1, 5, 1, 100.0, 1, 1),   # PaymentRemissionGranted = 0
                ("7", 1, 5, 1, 100.0, 1, 1),   # PaymentRemissionGranted = 2
                ("8", 1, 1, 1, 100.0, 1, 1),   # TransactionTypeId != 5
                ("9", 1, 5, 3, 100.0, 1, 1),   # Status == 3
                ("10", 1, 1, 3, 100.0, 1, 1),  # TransactionTypeId != 5 and Status == 3
                ("11", 1, 5, 1, 100.0, 1, 1),  # Multiple valid
                ("11", 1, 5, 1, 150.0, 1, 1),  # Multiple valid
                ("11", 1, 5, 1, 250.0, 1, 1)   # Multiple valid
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame(m4_data, self.SILVER_M4_SCHEMA)

            df, df_audit = remissionTypes(silver_m1, MagicMock(), silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("amountRemitted").collect()

            assert resultList[0][0] == 100 and resultList[1][0] == 100 and resultList[2][0] == 100 and resultList[3][0] == 100
            assert resultList[4][0] is None and resultList[5][0] is None and resultList[6][0] is None
            assert resultList[7][0] is None and resultList[8][0] is None and resultList[9][0] is None
            assert resultList[10][0] == 500

    def test_amountLeftToPay(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.remissionTypes.return_value = self.payment_pending_df(spark, 11)

            m1_data = [
                ("1", "EA", "AIP", 1),   # EA Case and valid conditions - amountLeftToPay set
                ("2", "EU", "LR", 1),    # EU Case and valid conditions - amountLeftToPay set
                ("3", "HU", "AIP", 1),   # HU Case and valid conditions - amountLeftToPay set
                ("4", "PA", "LR", 1),    # PA Case and valid conditions - amountLeftToPay set
                ("5", "RP", "AIP", 1),   # RP Case and valid conditions - none
                ("6", "EA", "AIP", 0),   # PaymentRemissionGranted = 0 - none
                ("7", "EA", "AIP", 2),   # PaymentRemissionGranted = 2 - none
                ("8", "EA", "AIP", 1),   # SumTotalFee == 0 - none
                ("9", "EA", "AIP", 1),   # TransactionId = 1 in ReferringTransactionId with TransactionTypeId 6 - none
                ("10", "EA", "AIP", 1),  # TransactionId = 2 in ReferringTransactionId with TransactionTypeId 19 - none
                ("11", "EA", "AIP", 1)   # Multiple matching conditions - sum of amountLeftToPay
            ]

            m4_data = [
                ("98", 3, 6, 1, 100.0, 1, 1),   # TransactionTypeId = 6, ReferringTransationId = 1
                ("99", 3, 19, 1, 100.0, 1, 2),  # TransactionTypeId = 19, ReferringTransationId = 2
                ("1", 3, 1, 1, 100.0, 1, 3),    # EA Case
                ("2", 3, 1, 1, 100.0, 1, 3),    # EU Case
                ("3", 3, 1, 1, 100.0, 1, 3),    # HU Case
                ("4", 3, 1, 1, 100.0, 1, 3),    # PA Case
                ("5", 3, 1, 1, 100.0, 1, 3),    # RP Case
                ("6", 3, 1, 1, 100.0, 1, 3),    # PaymentRemissionGranted = 0
                ("7", 3, 1, 1, 100.0, 1, 3),    # PaymentRemissionGranted = 2
                ("8", 3, 1, 1, 100.0, 0, 3),    # SumTotalFee = 0
                ("9", 1, 1, 3, 100.0, 1, 3),    # TransactionId 1 - for CaseNo98
                ("10", 2, 1, 3, 100.0, 1, 3),   # TransactionId 2 - for CaseNo 99
                ("11", 3, 1, 1, 100.0, 1, 3),   # Multiple valid
                ("11", 3, 1, 1, 150.0, 1, 3),   # Multiple valid
                ("11", 3, 1, 1, 250.0, 1, 3)    # Multiple valid
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame(m4_data, self.SILVER_M4_SCHEMA)

            df, df_audit = remissionTypes(silver_m1, MagicMock(), silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("amountLeftToPay").collect()

            assert resultList[0][0] == 100 and resultList[1][0] == 100 and resultList[2][0] == 100 and resultList[3][0] == 100
            assert resultList[4][0] is None and resultList[5][0] is None and resultList[6][0] is None
            assert resultList[7][0] is None and resultList[8][0] is None and resultList[9][0] is None
            assert resultList[10][0] == 500
