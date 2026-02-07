from Databricks.ACTIVE.APPEALS.shared_functions.ended import ended
import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, TimestampType
)

# IMPORTANT: import your function here
# from your_module import ended

# ---------- Spark fixture ----------
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("ended-unit-tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# ---------- Test data builders ----------
@pytest.fixture
def bronze_ended_states_df(spark):
    """
    Mapping table joined by (CaseStatus, Outcome).
    Keep types as integers to match the main DF join.
    """
    data = [
        # CaseStatus, Outcome, endAppealOutcome, endAppealOutcomeReason, stateBeforeEndAppeal
        (10, 80,  "Outcome_10_80", "Reason_10_80", "State_10"),
        (46, 31,  "Outcome_46_31", "Reason_46_31", "State_46"),
        (51, 94,  "Outcome_51_94", "Reason_51_94", "State_51"),
        (36, 1,   "Outcome_36_1",  "Reason_36_1",  "State_36"),
        (39, 25,  "Outcome_39_25", "Reason_39_25", "State_39"),
        (52, 91,  "Outcome_52_91", "Reason_52_91", "State_52"),
        (37, 80,  "Outcome_37_80", "Reason_37_80", "State_37"),
        # Add more as needed for expanded coverage
    ]
    schema = "CaseStatus INT, Outcome INT, endAppealOutcome STRING, endAppealOutcomeReason STRING, stateBeforeEndAppeal STRING"
    return spark.createDataFrame(data, schema=schema)

@pytest.fixture
def silver_m3_df(spark):
    """
    Build a small input table to exercise:
     - Status 46 with existence rule (needs 10/51/52 in same CaseNo)
     - Ranking by StatusId (choose max)
     - Non-matching rows get excluded
     - DecisionDate parsing
     - Approver type/name fields
    """
    data = [
        # Case A: Should PASS and select the (46,31) row due to max StatusId + existence rule satisfied via CaseStatus=10
        # Provide adjudicator fields for name concatenation
        ("A1", 10, 80, 1,  "2024-01-01 09:00:00", "Mr", "John", "Smith"),
        ("A1", 46, 31, 5,  "2024-03-04T10:11:12.123+00:00", "Dr", "Alice", "Brown"),  # pick this row
        # Case B: 46/31 but no 10/51/52 in same CaseNo -> should be EXCLUDED
        ("B1", 46, 31, 2,  "2024-02-15T12:00:00Z", None, None, None),
        # Case C: multiple filtered rows; pick MAX(StatusId). Approver type should be Case Worker.
        ("C1", 51,  0, 3,  "2023-12-01 09:30:00", None, None, None),
        ("C1", 51, 94, 7,  "2023-12-02 09:30:00", None, None, None),  # pick this row (highest StatusId)
        # Case D: CaseStatus=10 but Outcome NOT in allowed list -> EXCLUDED
        ("D1", 10, 99, 4,  "2023-10-10 10:10:10", None, None, None),
        # Case E: Another allowed branch (36, 1)
        ("E1", 36, 1,  6,  "2022-08-05T08:15:30.000+01:00", None, None, None),
        # Case F: Allowed branch (39, 25)
        ("F1", 39, 25,  8,  "2024-05-06 18:20:00", None, None, None),
        # Case G: Allowed branch (52, 91)
        ("G1", 52, 91,  9,  "2024-06-01T10:00:00+00:00", None, None, None),
        # Case H: Allowed (37, 80)
        ("H1", 37, 80,  10, "2024-07-01 11:11:11", None, None, None),
    ]

    schema = StructType([
        StructField("CaseNo", StringType(), False),
        StructField("CaseStatus", IntegerType(), False),
        StructField("Outcome", IntegerType(), False),
        StructField("StatusId", LongType(), False),
        StructField("DecisionDate", StringType(), True),
        StructField("Adj_Determination_Title", StringType(), True),
        StructField("Adj_Determination_Forenames", StringType(), True),
        StructField("Adj_Determination_Surname", StringType(), True),
    ])
    return spark.createDataFrame(data, schema=schema)


# ---------- Helper to get a row by CaseNo ----------
def get_row_by_case(df, case_no: str):
    rows = df.filter(F.col("CaseNo") == case_no).collect()
    assert len(rows) <= 1, f"Expected at most 1 row for CaseNo={case_no}, got {len(rows)}"
    return rows[0] if rows else None


# ---------- Tests ----------

def test_schema_and_columns_present(spark, silver_m3_df, bronze_ended_states_df):
    # Import function here to ensure the test module compiles even if function import path changes

    out = ended(silver_m3_df, bronze_ended_states_df)

    expected_cols = {
        "CaseNo",
        "endAppealOutcome",
        "endAppealOutcomeReason",
        "endAppealApproverType",
        "endAppealApproverName",
        "endAppealDate",
        "stateBeforeEndAppeal",
    }
    assert expected_cols.issubset(set(out.columns)), f"Missing columns: {expected_cols - set(out.columns)}"


def test_case_a_fields_status46_with_existence_rule(spark, silver_m3_df, bronze_ended_states_df):
    """
    Case A1 should produce a single row selected on (46,31), because:
      - A1 has a (10,80) row -> satisfies existence rule for 46/31
      - The (46,31) row has higher StatusId (5) than the (10,80) row (1), so it's chosen
      - Approver is 'Judge'; name is concatenated; date formatted dd/MM/yyyy
      - Join picks (46,31) mapping
    """

    out = ended(silver_m3_df, bronze_ended_states_df)
    row = get_row_by_case(out, "A1")
    assert row is not None, "Expected a row for CaseNo A1"

    # Validate each output field
    assert row.CaseNo == "A1"
    assert row.endAppealOutcome == "Outcome_46_31"
    assert row.endAppealOutcomeReason == "Reason_46_31"
    assert row.endAppealApproverType == "Judge"
    assert row.endAppealApproverName == "Dr Alice Brown"
    assert row.endAppealDate == "04/03/2024"  # from 2024-03-04T10:11:12.123+00:00
    assert row.stateBeforeEndAppeal == "State_46"


def test_case_b_excluded_without_existence_for_46_31(spark, silver_m3_df, bronze_ended_states_df):
    """
    Case B1: has only (46,31) but no 10/51/52 in the same case; should be excluded.
    """

    out = ended(silver_m3_df, bronze_ended_states_df)
    row = get_row_by_case(out, "B1")
    assert row is None, "B1 should not appear because existence rule is not met"


def test_case_c_max_statusid_selected_and_approver_type_name(spark, silver_m3_df, bronze_ended_states_df):
    """
    Case C1: two (51,*) rows; both pass filter; should pick StatusId=7 row.
    Approver type should be 'Case Worker' and name default message.
    """

    out = ended(silver_m3_df, bronze_ended_states_df)
    row = get_row_by_case(out, "C1")
    assert row is not None, "Expected a row for CaseNo C1"
    assert row.endAppealOutcome == "Outcome_51_94"
    assert row.endAppealOutcomeReason == "Reason_51_94"
    assert row.endAppealApproverType == "Case Worker"
    assert row.endAppealApproverName == "This is a migrated ARIA case"
    assert row.endAppealDate == "02/12/2023"
    assert row.stateBeforeEndAppeal == "State_51"


def test_excluded_non_matching_branch(spark, silver_m3_df, bronze_ended_states_df):
    """
    Case D1: CaseStatus 10, Outcome 99 -> not in allowed list, should be excluded.
    """

    out = ended(silver_m3_df, bronze_ended_states_df)
    row = get_row_by_case(out, "D1")
    assert row is None, "D1 should be excluded as Outcome 99 is not allowed for CaseStatus 10"


def test_other_allowed_branches_and_join_fields(spark, silver_m3_df, bronze_ended_states_df):
    """
    Validate additional branches: (36,1), (39,25), (52,91), (37,80)
    Each should appear once and pick the correct join fields and formatted date.
    """

    out = ended(silver_m3_df, bronze_ended_states_df)

    # E1 -> (36,1)
    e1 = get_row_by_case(out, "E1")
    assert e1 is not None
    assert e1.endAppealOutcome == "Outcome_36_1"
    assert e1.endAppealOutcomeReason == "Reason_36_1"
    assert e1.stateBeforeEndAppeal == "State_36"
    assert e1.endAppealApproverType == "Case Worker"
    assert e1.endAppealApproverName == "This is a migrated ARIA case"
    assert isinstance(e1.endAppealDate, str) and len(e1.endAppealDate) == 10  # dd/MM/yyyy

    # F1 -> (39,25)
    f1 = get_row_by_case(out, "F1")
    assert f1 is not None
    assert f1.endAppealOutcome == "Outcome_39_25"
    assert f1.endAppealOutcomeReason == "Reason_39_25"
    assert f1.stateBeforeEndAppeal == "State_39"

    # G1 -> (52,91)
    g1 = get_row_by_case(out, "G1")
    assert g1 is not None
    assert g1.endAppealOutcome == "Outcome_52_91"
    assert g1.endAppealOutcomeReason == "Reason_52_91"
    assert g1.stateBeforeEndAppeal == "State_52"

    # H1 -> (37,80)
    h1 = get_row_by_case(out, "H1")
    assert h1 is not None
    assert h1.endAppealOutcome == "Outcome_37_80"
    assert h1.endAppealOutcomeReason == "Reason_37_80"
    assert h1.stateBeforeEndAppeal == "State_37"


def test_output_row_count_expected(spark, silver_m3_df, bronze_ended_states_df):
    """
    Sanity-check the total number of rows produced:
     A1, C1, E1, F1, G1, H1 -> 6 rows.
     B1 and D1 are excluded.
    """

    out = ended(silver_m3_df, bronze_ended_states_df)
    assert out.count() == 6, f"Unexpected output row count: {out.count()}"




    