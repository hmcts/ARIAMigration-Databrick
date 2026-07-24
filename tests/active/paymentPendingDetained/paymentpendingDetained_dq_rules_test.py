from Databricks.ACTIVE.APPEALS.shared_functions.dq_rules import paymentpendingDetained_dq_rules

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import ArrayType, BooleanType, IntegerType, StringType, StructField, StructType

import pytest


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
            .appName("paymentPendingDetainedDQRules")
            .getOrCreate()
    )


@pytest.fixture(scope="session")
def dq_checks():
    return paymentpendingDetained_dq_rules.paymentPendingDetainedDQRules().get_checks()


class TestPaymentPendingDetainedDQChecks():

    APPELLANT_IN_UK_COLUMNS = StructType([
        StructField("CaseNo", StringType()),
        StructField("Detained", IntegerType()),
        StructField("valid_categoryIdList", ArrayType(IntegerType())),
        StructField("dv_addressInUk", BooleanType()),
        StructField("appellantInUk", StringType()),
    ])

    OOC_APPEAL_ADMIN_J_COLUMNS = StructType([
        StructField("CaseNo", StringType()),
        StructField("Detained", IntegerType()),
        StructField("dv_appellantIsInUk", BooleanType()),
        StructField("lu_HORef", StringType()),
        StructField("HORef", StringType()),
        StructField("FCONumber", StringType()),
        StructField("oocAppealAdminJ", StringType()),
    ])

    def test_valid_appellantInUk_check(self, spark, dq_checks):
        df = spark.createDataFrame([
            ("1", 1, [], False, "Yes"),        # Detained wins - valid
            ("2", 0, [37], False, "Yes"),       # Category 37 - valid
            ("3", 0, [38], True, "No"),         # Category 38 wins over dv_addressInUk - valid
            ("4", 0, [], True, "Yes"),          # dv_addressInUk=True - valid
            ("5", 0, [], False, "No"),          # dv_addressInUk=False - valid
            ("6", 0, [], False, "Yes"),         # dv_addressInUk=False but appellantInUk='Yes' - invalid
            ("7", 1, [], False, "No"),          # Detained but appellantInUk='No' - invalid
        ], self.APPELLANT_IN_UK_COLUMNS)

        checks_to_run = dq_checks["valid_appellantInUk"]

        df = df.withColumn("is_valid", expr(checks_to_run)).sort(col("CaseNo").cast("int"))

        df_checks = df.select("is_valid").collect()

        assert df_checks[0][0] is True and df_checks[1][0] is True and df_checks[2][0] is True
        assert df_checks[3][0] is True and df_checks[4][0] is True
        assert df_checks[5][0] is not True
        assert df_checks[6][0] is not True

    def test_valid_oocAppealAdminJ_values_check(self, spark, dq_checks):
        df = spark.createDataFrame([
            ("1", 1, False, None, None, None, None),                  # Detained - IS NULL - valid
            ("2", 0, True, None, None, None, None),                   # In UK - IS NULL - valid
            ("3", 0, False, "GWF123", None, None, "entryClearanceDecision"),      # GWF via lu_HORef - valid
            ("4", 0, False, None, "GWF456", None, "entryClearanceDecision"),      # GWF via HORef fallback - valid
            ("5", 0, False, None, None, "GWF789", "entryClearanceDecision"),      # GWF via FCONumber fallback - valid
            ("6", 0, False, None, None, None, "none"),                # OOC, no GWF ref anywhere - valid
            ("7", 0, False, None, None, None, None),                  # OOC, no GWF ref, but left NULL - invalid
        ], self.OOC_APPEAL_ADMIN_J_COLUMNS)

        checks_to_run = dq_checks["valid_oocAppealAdminJ_values"]

        df = df.withColumn("is_valid", expr(checks_to_run)).sort(col("CaseNo").cast("int"))

        df_checks = df.select("is_valid").collect()

        assert df_checks[0][0] is True and df_checks[1][0] is True
        assert df_checks[2][0] is True and df_checks[3][0] is True and df_checks[4][0] is True
        assert df_checks[5][0] is True
        assert df_checks[6][0] is not True
