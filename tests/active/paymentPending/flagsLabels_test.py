import pytest
from pyspark.sql import SparkSession, types as T
from pyspark.sql.functions import col
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import flagsLabels


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("flagsLabelsTests")
        .getOrCreate()
    )

def run_flags_labels(spark, m1_data, m2_data, c_data):

    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("HORef", T.StringType(), True),
        T.StructField("CasePrefix", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
    ])

    m2_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("CategoryId", T.IntegerType(), True),
        T.StructField("Relationship", T.StringType(), True),
        T.StructField("Detained", T.IntegerType(), True),
    ])

    c_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
    ])

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)
    silver_m2 = spark.createDataFrame(m2_data, m2_schema)
    silver_c = spark.createDataFrame(c_data, c_schema)

    df = flagsLabels(silver_m1, silver_m2, silver_c)

    return {row["CaseNo"]: row.asDict() for row in df.collect()}

def assert_flag_contains(flag_struct, expected_code, expected_comment=None):
    assert flag_struct is not None, "Expected flags but got None"

    details = flag_struct["details"]

    codes = [d["value"]["flagCode"] for d in details]
    assert expected_code in codes, f"{expected_code} not found in {codes}"

    if expected_comment:
        comments = [d["value"]["flagComment"] for d in details]
        assert expected_comment in comments, f"{expected_comment} not found in {comments}"

# TESTS
def test_ot0001_triggered_by_horef(spark):
    """HORef should create OT0001 with 'Dropped Case'"""

    results = run_flags_labels(
        spark,
        m1_data=[
            ("CASE1", "H123", "EA", "LR", "type1"),
        ],
        m2_data=[
            ("CASE1", None, None, None),
        ],
        c_data=[("CASE1",)]
    )

    row = results["CASE1"]

    assert_flag_contains(row["caseFlags"], "OT0001", "Dropped Case")


def test_ot0001_triggered_by_category(spark):
    """CategoryId should create OT0001 with 'Expedite'"""

    results = run_flags_labels(
        spark,
        m1_data=[
            ("CASE2", None, "EA", "LR", "type1"),
        ],
        m2_data=[
            ("CASE2", 41, None, None),  # OT0001 category
        ],
        c_data=[("CASE2",)]
    )

    row = results["CASE2"]

    assert_flag_contains(row["caseFlags"], "OT0001", "Expedite")


def test_no_flags(spark):
    """No HORef and no categories → no flags"""

    results = run_flags_labels(
        spark,
        m1_data=[
            ("CASE3", None, "EA", "LR", "type1"),
        ],
        m2_data=[
            ("CASE3", None, None, None),
        ],
        c_data=[("CASE3",)]
    )

    row = results["CASE3"]

    assert row["caseFlags"] is None
    assert row["appellantLevelFlags"] is None


def test_pf0012_deduplicated(spark):
    """Multiple PF0012 categories should result in ONE flag"""

    results = run_flags_labels(
        spark,
        m1_data=[
            ("CASE4", None, "EA", "LR", "type1"),
        ],
        m2_data=[
            ("CASE4", 17, None, None),
            ("CASE4", 29, None, None),  # both PF0012
        ],
        c_data=[("CASE4",)]
    )

    row = results["CASE4"]

    flags = row["appellantLevelFlags"]["details"]

    pf_flags = [f for f in flags if f["value"]["flagCode"] == "PF0012"]

    assert len(pf_flags) == 1


def test_detained_flag(spark):
    """Detained should create PF0019"""

    results = run_flags_labels(
        spark,
        m1_data=[
            ("CASE5", None, "EA", "LR", "type1"),
        ],
        m2_data=[
            ("CASE5", None, None, 1),  # detained
        ],
        c_data=[("CASE5",)]
    )

    row = results["CASE5"]

    assert_flag_contains(row["appellantLevelFlags"], "PF0019")


def test_base_condition_blocks_output(spark):
    """If dv_representation not valid → everything should be NULL"""

    results = run_flags_labels(
        spark,
        m1_data=[
            ("CASE6", "H123", "EA", "INVALID", "type1"),
        ],
        m2_data=[
            ("CASE6", 41, None, None),
        ],
        c_data=[("CASE6",)]
    )

    row = results["CASE6"]

    assert row["caseFlags"] is None
    assert row["appellantLevelFlags"] is None
    assert row["isAdmin"] is None


def test_caseprefix_fee_exemption(spark):
    """CasePrefix DA → fee exemption = Yes"""

    results = run_flags_labels(
        spark,
        m1_data=[
            ("CASE7", None, "DA", "LR", "type1"),
        ],
        m2_data=[
            ("CASE7", None, None, None),
        ],
        c_data=[("CASE7",)]
    )

    row = results["CASE7"]

    assert row["isAriaMigratedFeeExemption"] == "Yes"


def test_relationship_filtering(spark):
    """Rows with Relationship should be filtered out"""

    results = run_flags_labels(
        spark,
        m1_data=[
            ("CASE8", None, "EA", "LR", "type1"),
        ],
        m2_data=[
            ("CASE8", 41, "CHILD", None),  # should be excluded
        ],
        c_data=[("CASE8",)]
    )

    row = results["CASE8"]

    assert row["caseFlags"] is None  # because category got filtered out