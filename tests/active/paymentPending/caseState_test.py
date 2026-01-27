import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import caseState
import uuid

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("caseStateTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def caseState_outputs(spark):

    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
    ])

    m1_data = [
        ("EA/01001/2025", "LR", "EA"),
        ("HU/01004/2025", "LR", "HU"),
    ]

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)

    # Call the function under test
    caseState_content, _ = caseState(silver_m1, desiredState="paymentPending")

    # Convert to dictionary keyed by CaseNo
    results = {row["CaseNo"]: row.asDict() for row in caseState_content.collect()}
    return results


@pytest.mark.parametrize("desired_state", ["pendingPayment"])
def test_case_state_is_constant(spark, desired_state):
    silver_m1 = spark.createDataFrame(
        [("EA/01001/2025",)], ["CaseNo"]
    )

    df = caseState(silver_m1, desired_state)
    row = df.collect()[0]

    assert row.ariaDesiredState == desired_state
    assert row.ariaMigrationTaskDueDays == "14"

