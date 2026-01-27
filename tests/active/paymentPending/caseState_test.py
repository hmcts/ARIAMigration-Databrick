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

def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row.get(k) == v, f"{k} expected {v} but got {row.get(k)}"

def test_case_state_is_constant(caseState_outputs):
    """Check that default case values are mapped accordingly."""
    row = caseState_outputs["EA/01001/2025"]
    assert_equals(row,
        ariaDesiredState = "paymentPending",
        ariaMigrationTaskDueDays = 14)