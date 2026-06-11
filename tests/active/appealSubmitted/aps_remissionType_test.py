from Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted import remissionTypes

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from unittest.mock import MagicMock, patch
import pytest


BRONZE_REMISSION_SCHEMA = StructType([
    StructField("PaymentRemissionReason", IntegerType()),
    StructField("PaymentRemissionRequested", IntegerType()),
    StructField("remissionType", StringType()),
    StructField("remissionClaim", StringType()),
    StructField("feeRemissionType", StringType()),
    StructField("exceptionalCircumstances", StringType()),
    StructField("legalAidAccountNumber", StringType()),
    StructField("asylumSupportReference", StringType()),
    StructField("helpWithFeesReferenceNumber", StringType()),
])

EXTENDED_M1_SCHEMA = StructType([
    StructField("CaseNo", StringType()),
    StructField("dv_CCDAppealType", StringType()),
    StructField("dv_representation", StringType()),
    StructField("PaymentRemissionGranted", IntegerType()),
    StructField("lu_appealType", StringType()),
    StructField("PaymentRemissionReason", IntegerType()),
    StructField("PaymentRemissionRequested", IntegerType()),
    StructField("LSCReference", StringType()),
    StructField("ASFReferenceNo", StringType()),
    StructField("PaymentRemissionReasonNote", StringType()),
])


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
        StructField("PaymentRemissionGranted", IntegerType()),
        StructField("lu_appealType", StringType()),
    ])

    SILVER_M4_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("TransactionId", IntegerType()),
        StructField("TransactionTypeId", IntegerType()),
        StructField("Status", IntegerType()),
        StructField("Amount", DoubleType()),
        StructField("SumTotalFee", BooleanType()),
        StructField("ReferringTransactionId", IntegerType())
    ])

    def payment_pending_df(self, spark, num_of_cases=0):
        case_no_df = spark.createDataFrame(((n,) for n in range(1, num_of_cases + 1)), self.CASE_NO_COLUMNS)

        return case_no_df, case_no_df

    def test_remissionDecision(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.remissionTypes.return_value = self.payment_pending_df(spark, 7)

            m1_data = [
                ("1", "EA", "AIP", 1,"protection"),  # EA Case - remissionDecision approved
                ("2", "EU", "LR", 1,"protection"),   # EU Case - remissionDecision approved
                ("3", "HU", "AIP", 1,"protection"),  # HU Case - remissionDecision approved
                ("4", "PA", "LR", 1,"protection"),   # PA Case - remissionDecision approved
                ("5", "RP", "AIP", 1,"protection"),  # RP Case - none
                ("6", "EA", "AIP", 0,"protection"),  # PaymentRemissionGranted = 0 - none
                ("7", "EA", "AIP", 2,"protection")   # PaymentRemissionGranted = 2 - remissionDecision rejected
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
                ("1", "EA", "AIP", 1,"protection"),  # EA Case - remissionDecision approved
                ("2", "EU", "LR", 1,"protection"),   # EU Case - remissionDecision approved
                ("3", "HU", "AIP", 1,"protection"),  # HU Case - remissionDecision approved
                ("4", "PA", "LR", 1,"protection"),   # PA Case - remissionDecision approved
                ("5", "RP", "AIP", 1,"protection"),  # RP Case - none
                ("6", "EA", "AIP", 0,"protection"),  # PaymentRemissionGranted = 0 - none
                ("7", "EA", "AIP", 2,"protection")   # PaymentRemissionGranted = 2 - rejected
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
                ("1", "EA", "AIP", 1,"protection"),   # EA Case and valid conditions - amountRemitted set
                ("2", "EU", "LR", 1,"protection"),    # EU Case and valid conditions - amountRemitted set
                ("3", "HU", "AIP", 1,"protection"),   # HU Case and valid conditions - amountRemitted set
                ("4", "PA", "LR", 1,"protection"),    # PA Case and valid conditions - amountRemitted set
                ("5", "RP", "AIP", 1,"protection"),   # RP Case and valid conditions - none
                ("6", "EA", "AIP", 0,"protection"),   # PaymentRemissionGranted = 0 - none
                ("7", "EA", "AIP", 2,"protection"),   # PaymentRemissionGranted = 2 - none
                ("8", "EA", "AIP", 1,"protection"),   # TransactionTypeId != 5 - 0 set
                ("9", "EA", "AIP", 1,"protection"),   # Status == 3 - 0 set
                ("10", "EA", "AIP", 1,"protection"),  # TransactionTypeId != 5 and Status == 3 - 0 set
                ("11", "EA", "AIP", 1,"protection")   # Multiple matching conditions - sum of amountRemitted
            ]

            m4_data = [
                ("1", 1, 5, 1, 100.0, True, 1),   # EA Case
                ("2", 1, 5, 1, 100.0, True, 1),   # EU Case
                ("3", 1, 5, 1, 100.0, True, 1),   # HU Case
                ("4", 1, 5, 1, 100.0, True, 1),   # PA Case
                ("5", 1, 5, 1, 100.0, True, 1),   # RP Case
                ("6", 1, 5, 1, 100.0, True, 1),   # PaymentRemissionGranted = 0
                ("7", 1, 5, 1, 100.0, True, 1),   # PaymentRemissionGranted = 2
                ("8", 1, 1, 1, 100.0, True, 1),   # TransactionTypeId != 5
                ("9", 1, 5, 3, 100.0, True, 1),   # Status == 3
                ("10", 1, 1, 3, 100.0, True, 1),  # TransactionTypeId != 5 and Status == 3
                ("11", 1, 5, 1, 100.0, True, 1),  # Multiple valid
                ("11", 1, 5, 1, 150.0, True, 1),  # Multiple valid
                ("11", 1, 5, 1, 250.0, True, 1)   # Multiple valid
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame(m4_data, self.SILVER_M4_SCHEMA)

            df, df_audit = remissionTypes(silver_m1, MagicMock(), silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("amountRemitted").collect()

            assert resultList[0][0] == "10000" and resultList[1][0] == "10000" and resultList[2][0] == "10000" and resultList[3][0] == "10000"
            assert resultList[4][0] is None and resultList[5][0] is None and resultList[6][0] is None
            assert resultList[7][0] is None and resultList[8][0] is None and resultList[9][0] is None
            assert resultList[10][0] == "50000"

    def test_amountLeftToPay(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP') as PP:
            PP.remissionTypes.return_value = self.payment_pending_df(spark, 11)

            m1_data = [
                ("1", "EA", "AIP", 1,"protection"),   # EA Case and valid conditions - amountLeftToPay set
                ("2", "EU", "LR", 1,"protection"),    # EU Case and valid conditions - amountLeftToPay set
                ("3", "HU", "AIP", 1,"protection"),   # HU Case and valid conditions - amountLeftToPay set
                ("4", "PA", "LR", 1,"protection"),    # PA Case and valid conditions - amountLeftToPay set
                ("5", "RP", "AIP", 1,"protection"),   # RP Case and valid conditions - none
                ("6", "EA", "AIP", 0,"protection"),   # PaymentRemissionGranted = 0 - none
                ("7", "EA", "AIP", 2,"protection"),   # PaymentRemissionGranted = 2 - none
                ("8", "EA", "AIP", 1,"protection"),   # SumTotalFee == 0 - 0 set
                ("9", "EA", "AIP", 1,"protection"),   # TransactionId = 1 in ReferringTransactionId with TransactionTypeId 6 - 0 set
                ("10", "EA", "AIP", 1,"protection"),  # TransactionId = 2 in ReferringTransactionId with TransactionTypeId 19 - 0 set
                ("11", "EA", "AIP", 1,"protection")   # Multiple matching conditions - sum of amountLeftToPay
            ]

            m4_data = [
                ("98", 3, 6, 1, 100.0, True, 1),   # TransactionTypeId = 6, ReferringTransationId = 1
                ("99", 3, 19, 1, 100.0, True, 2),  # TransactionTypeId = 19, ReferringTransationId = 2
                ("1", 3, 1, 1, 100.0, True, 3),    # EA Case
                ("2", 3, 1, 1, 100.0, True, 3),    # EU Case
                ("3", 3, 1, 1, 100.0, True, 3),    # HU Case
                ("4", 3, 1, 1, 100.0, True, 3),    # PA Case
                ("5", 3, 1, 1, 100.0, True, 3),    # RP Case
                ("6", 3, 1, 1, 100.0, True, 3),    # PaymentRemissionGranted = 0
                ("7", 3, 1, 1, 100.0, True, 3),    # PaymentRemissionGranted = 2
                ("8", 3, 1, 1, 100.0, False, 3),    # SumTotalFee = 0
                ("9", 1, 1, 3, 100.0, True, 3),    # TransactionId 1 - for CaseNo98
                ("10", 2, 1, 3, 100.0, True, 3),   # TransactionId 2 - for CaseNo 99
                ("11", 3, 1, 1, 100.0, True, 3),   # Multiple valid
                ("11", 3, 1, 1, 150.0, True, 3),   # Multiple valid
                ("11", 3, 1, 1, 250.0, True, 3)    # Multiple valid
            ]

            silver_m1 = spark.createDataFrame(m1_data, self.SILVER_M1_SCHEMA)
            silver_m4 = spark.createDataFrame(m4_data, self.SILVER_M4_SCHEMA)

            df, df_audit = remissionTypes(silver_m1, MagicMock(), silver_m4)

            resultList = df.orderBy(col("CaseNo").cast("int")).select("amountLeftToPay").collect()

            assert resultList[0][0] == "100" and resultList[1][0] == "100" and resultList[2][0] == "100" and resultList[3][0] == "100"
            assert resultList[4][0] is None and resultList[5][0] is None and resultList[6][0] is None
            assert resultList[7][0] is None and resultList[8][0] is None and resultList[9][0] is None
            assert resultList[10][0] == "500"


class TestRemissionJoinAndTransforms:
    """Tests for the extended remission join condition and field transforms in appealSubmitted."""

    CASE_NO_SCHEMA = StructType([StructField("CaseNo", StringType())])
    SILVER_M4_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("TransactionId", IntegerType()),
        StructField("TransactionTypeId", IntegerType()),
        StructField("Status", IntegerType()),
        StructField("Amount", DoubleType()),
        StructField("SumTotalFee", BooleanType()),
        StructField("ReferringTransactionId", IntegerType()),
    ])

    def _run(self, spark, m1_rows, bronze_rows, pp_case_nos):
        with patch("Databricks.ACTIVE.APPEALS.shared_functions.appealSubmitted.PP") as PP:
            case_no_df = spark.createDataFrame(((c,) for c in pp_case_nos), self.CASE_NO_SCHEMA)
            PP.remissionTypes.return_value = (case_no_df, case_no_df)

            silver_m1 = spark.createDataFrame(m1_rows, EXTENDED_M1_SCHEMA)
            bronze = spark.createDataFrame(bronze_rows, BRONZE_REMISSION_SCHEMA)
            silver_m4 = spark.createDataFrame([], self.SILVER_M4_SCHEMA)

            df, _ = remissionTypes(silver_m1, bronze, silver_m4)
            return {row["CaseNo"]: row.asDict() for row in df.collect()}

    # --- Extended join condition ---

    def test_extended_join_requested_zero(self, spark):
        """PaymentRemissionRequested=0 with Reason>0 should match the lookup."""
        results = self._run(
            spark,
            m1_rows=[("1", "EA", "AIP", 1, "protection", 1, 0, None, None, None)],
            bronze_rows=[(1, 1, "hoWaiverRemission", "asylumSupport", "Asylum Support", "OMIT", "OMIT", "OMIT", "OMIT")],
            pp_case_nos=["1"],
        )
        assert results["1"]["remissionType"] == "hoWaiverRemission"

    def test_extended_join_requested_null(self, spark):
        """PaymentRemissionRequested=NULL with Reason>0 should match the lookup."""
        results = self._run(
            spark,
            m1_rows=[("1", "EA", "AIP", 1, "protection", 2, None, None, None, None)],
            bronze_rows=[(2, 1, "exceptionalCircumstancesRemission", "OMIT", "OMIT", "Some reason", "OMIT", "OMIT", "OMIT")],
            pp_case_nos=["1"],
        )
        assert results["1"]["remissionType"] == "exceptionalCircumstancesRemission"

    def test_extended_join_not_applied_when_reason_zero(self, spark):
        """PaymentRemissionReason=0 must not trigger the extended condition."""
        results = self._run(
            spark,
            m1_rows=[("1", "EA", "AIP", 1, "protection", 0, 0, None, None, None)],
            bronze_rows=[(0, 1, "noRemission", "OMIT", "OMIT", "OMIT", "OMIT", "OMIT", "OMIT")],
            pp_case_nos=["1"],
        )
        assert results["1"]["remissionType"] is None

    def test_normal_join_still_works(self, spark):
        """Exact match on both columns continues to join correctly."""
        results = self._run(
            spark,
            m1_rows=[("1", "EA", "AIP", 1, "protection", 3, 1, "LSC-999", None, None)],
            bronze_rows=[(3, 1, "hoWaiverRemission", "legalAid", "Legal Aid", "OMIT", "M1.LSCReference; ELSE IF NULL 'Unknown'", "OMIT", "OMIT")],
            pp_case_nos=["1"],
        )
        assert results["1"]["remissionType"] == "hoWaiverRemission"
        assert results["1"]["legalAidAccountNumber"] == "LSC-999"

    # --- Field transform: OMIT / NO MAPPING REQUIRED ---

    def test_omit_becomes_null(self, spark):
        """OMIT sentinel in lookup fields should produce NULL output."""
        results = self._run(
            spark,
            m1_rows=[("1", "EA", "AIP", 1, "protection", 1, 1, None, None, None)],
            bronze_rows=[(1, 1, "noRemission", "OMIT", "OMIT", "OMIT", "OMIT", "OMIT", "OMIT")],
            pp_case_nos=["1"],
        )
        row = results["1"]
        assert row["remissionClaim"] is None
        assert row["feeRemissionType"] is None
        assert row["exceptionalCircumstances"] is None
        assert row["legalAidAccountNumber"] is None
        assert row["asylumSupportReference"] is None
        assert row["helpWithFeesReferenceNumber"] is None

    def test_no_mapping_required_becomes_null(self, spark):
        """NO MAPPING REQUIRED sentinel should produce NULL for all 7 fields."""
        results = self._run(
            spark,
            m1_rows=[("1", "EA", "AIP", 1, "protection", 1, 1, None, None, None)],
            bronze_rows=[(1, 1, "NO MAPPING REQUIRED", "NO MAPPING REQUIRED", "NO MAPPING REQUIRED",
                          "NO MAPPING REQUIRED", "NO MAPPING REQUIRED", "NO MAPPING REQUIRED", "NO MAPPING REQUIRED")],
            pp_case_nos=["1"],
        )
        row = results["1"]
        assert row["remissionType"] is None
        assert row["remissionClaim"] is None
        assert row["feeRemissionType"] is None

    # --- Field transforms: M1 sentinel lookups ---

    def test_legal_aid_account_number_from_lsc_reference(self, spark):
        """M1.LSCReference sentinel pads short LSC values with leading zeros."""
        results = self._run(
            spark,
            m1_rows=[
                ("1", "EA", "AIP", 1, "protection", 3, 1, "LSC12", None, None),   # 5-char → pad 1
                ("2", "EA", "AIP", 1, "protection", 3, 1, "L123", None, None),    # 4-char → pad 2
                ("3", "EA", "AIP", 1, "protection", 3, 1, "L12", None, None),     # 3-char → pad 3
                ("4", "EA", "AIP", 1, "protection", 3, 1, "L1234567", None, None),  # long → as-is
                ("5", "EA", "AIP", 1, "protection", 3, 1, None, None, None),      # null → Unknown
            ],
            bronze_rows=[(3, 1, "hoWaiverRemission", "legalAid", "Legal Aid", "OMIT",
                          "M1.LSCReference; ELSE IF NULL 'Unknown'", "OMIT", "OMIT")],
            pp_case_nos=["1", "2", "3", "4", "5"],
        )
        assert results["1"]["legalAidAccountNumber"] == "0LSC12"
        assert results["2"]["legalAidAccountNumber"] == "00L123"
        assert results["3"]["legalAidAccountNumber"] == "000L12"
        assert results["4"]["legalAidAccountNumber"] == "L1234567"
        assert results["5"]["legalAidAccountNumber"] == "Unknown"

    def test_asylum_support_reference_from_asf(self, spark):
        """M1.ASFReferenceNo sentinel uses ASFReferenceNo or falls back to Unknown."""
        results = self._run(
            spark,
            m1_rows=[
                ("1", "EA", "AIP", 1, "protection", 1, 1, None, "ASF-123", None),
                ("2", "EA", "AIP", 1, "protection", 1, 1, None, None, None),
            ],
            bronze_rows=[(1, 1, "hoWaiverRemission", "asylumSupport", "Asylum Support", "OMIT",
                          "OMIT", "M1.ASFReferenceNo ELSE IF NULL 'Unknown'", "OMIT")],
            pp_case_nos=["1", "2"],
        )
        assert results["1"]["asylumSupportReference"] == "ASF-123"
        assert results["2"]["asylumSupportReference"] == "Unknown"

    def test_help_with_fees_from_remission_reason_note(self, spark):
        """M1.PaymentRemissionReasonNote sentinel uses the note or falls back to Unknown."""
        results = self._run(
            spark,
            m1_rows=[
                ("1", "EA", "AIP", 1, "protection", 9, 1, None, None, "HWF-456"),
                ("2", "EA", "AIP", 1, "protection", 9, 1, None, None, None),
            ],
            bronze_rows=[(9, 1, "helpWithFees", "OMIT", "OMIT", "OMIT", "OMIT", "OMIT",
                          "M1.PaymentRemissionReasonNote; ELSE IF NULL 'Unknown'")],
            pp_case_nos=["1", "2"],
        )
        assert results["1"]["helpWithFeesReferenceNumber"] == "HWF-456"
        assert results["2"]["helpWithFeesReferenceNumber"] == "Unknown"
