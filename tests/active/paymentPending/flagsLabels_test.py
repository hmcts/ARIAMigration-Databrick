import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import flagsLabels

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("flagsLabelsTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def flags_labels_outputs(spark):

    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("HORef", T.StringType(), True),
        T.StructField("CasePrefix", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
    ])

    m1_data = [
        ("CASE1", "H123", "EA", "LR", "type1"),   # HORef → Dropped Case
        ("CASE2", None, "EA", "LR", "type1"),     # Category → Expedite
        ("CASE3", None, "EA", "LR", "type1"),     # No flags
        ("CASE4", None, "EA", "LR", "type1"),     # PF0012 dedupe
        ("CASE5", None, "EA", "LR", "type1"),     # Detained
        ("CASE6", "H123", "EA", "INVALID", "type1"), # fails base condition
        ("CASE7", None, "DA", "LR", "type1"),     # Fee exemption
        ("CASE8", None, "EA", "LR", "type1"),     # Relationship filtered
    ]

    m2_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("CategoryId", T.IntegerType(), True),
        T.StructField("Relationship", T.StringType(), True),
        T.StructField("Detained", T.IntegerType(), True),
    ])

    m2_data = [
        ("CASE1", None, None, None),
        ("CASE2", 41, None, None),  # OT0001
        ("CASE3", None, None, None),
        ("CASE4", 17, None, None),
        ("CASE4", 29, None, None),  # PF0012 duplicate
        ("CASE5", None, None, 1),   # detained
        ("CASE6", 41, None, None),
        ("CASE7", None, None, None),
        ("CASE8", 41, "CHILD", None),  # filtered out
    ]

    c_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
    ])

    c_data = [(f"CASE{i}",) for i in range(1, 9)]

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)
    silver_m2 = spark.createDataFrame(m2_data, m2_schema)
    silver_c = spark.createDataFrame(c_data, c_schema)

    flagsLabels, _ = flagsLabels(silver_m1, silver_m2, silver_c)

    results = {row["CaseNo"]: row.asDict() for row in flagsLabels.collect()}
    return results

# ---------------------------------------------------
# Helper
# ---------------------------------------------------
def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row.get(k) == v, f"{k} expected {v} but got {row.get(k)}"


def get_flag_codes(flag_struct):
    if not flag_struct:
        return []
    return [d["value"]["flagCode"] for d in flag_struct["details"]]


def get_flag_comments(flag_struct):
    if not flag_struct:
        return []
    return [d["value"]["flagComment"] for d in flag_struct["details"]]


# ---------------------------------------------------
# TESTS
# ---------------------------------------------------

def test_horef_creates_dropped_case(flags_labels_outputs):
    row = flags_labels_outputs["CASE1"]

    assert "OT0001" in get_flag_codes(row["caseFlags"])
    assert "Dropped Case" in get_flag_comments(row["caseFlags"])


def test_category_creates_expedite(flags_labels_outputs):
    row = flags_labels_outputs["CASE2"]

    assert "OT0001" in get_flag_codes(row["caseFlags"])
    assert "Expedite" in get_flag_comments(row["caseFlags"])


def test_no_flags(flags_labels_outputs):
    row = flags_labels_outputs["CASE3"]

    assert row["caseFlags"] is None
    assert row["appellantLevelFlags"] is None


def test_pf0012_deduplicated(flags_labels_outputs):
    row = flags_labels_outputs["CASE4"]

    codes = get_flag_codes(row["appellantLevelFlags"])

    assert codes.count("PF0012") == 1


def test_detained_flag(flags_labels_outputs):
    row = flags_labels_outputs["CASE5"]

    assert "PF0019" in get_flag_codes(row["appellantLevelFlags"])


def test_base_condition_blocks_output(flags_labels_outputs):
    row = flags_labels_outputs["CASE6"]

    assert_equals(row,
        caseFlags=None,
        appellantLevelFlags=None,
        isAdmin=None
    )


def test_fee_exemption(flags_labels_outputs):
    row = flags_labels_outputs["CASE7"]

    assert row["isAriaMigratedFeeExemption"] == "Yes"


def test_relationship_filtered(flags_labels_outputs):
    row = flags_labels_outputs["CASE8"]

    assert row["caseFlags"] is None