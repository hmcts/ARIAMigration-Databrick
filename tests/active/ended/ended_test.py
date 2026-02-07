from Databricks.ACTIVE.APPEALS.shared_functions.ended import ended
from pyspark.sql import SparkSession
import pytest
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("generalTests")
        .getOrCreate()
    )

##### Testing the ended field grouping function #####
@pytest.fixture(scope="session")
def ended_outputs(spark):

    es_data = [
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

    es_schema = F.StructType([
        T.StructField("CaseStatus", T.IntegerType(), False),
        T.StructField("Outcome", T.IntegerType(), False),
        T.StructField("endAppealOutcome", T.StringType(), True),
        T.StructField("endAppealOutcomeReason", T.StringType(), True),
        T.StructField("stateBeforeEndAppeal", T.StringType(), True),
    ])

    m3_data = [
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

    m3_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), False),
        T.StructField("CaseStatus", T.IntegerType(), False),
        T.StructField("Outcome", T.IntegerType(), False),
        T.StructField("StatusId", T.LongType(), False),
        T.StructField("DecisionDate", T.StringType(), True),
        T.StructField("Adj_Determination_Title", T.StringType(), True),
        T.StructField("Adj_Determination_Forenames", T.StringType(), True),
        T.StructField("Adj_Determination_Surname", T.StringType(), True),
    ])

    df_m3 =  spark.createDataFrame(m3_data, m3_schema)
    df_es =  spark.createDataFrame(es_data, es_schema)

    ended_content,_ = ended(df_m3, df_es)
    results = {row["CaseNo"]: row.asDict() for row in ended_content.collect()}
    
    return results


def test_endAppealOutcome(spark,ended_outputs):

    results = ended_outputs

    assert results["A1"]["endAppealOutcome"] == 'Outcome_46_31'
    assert results["C1"]["endAppealOutcome"] == 'Outcome_51_94'
    assert results["E1"]["endAppealOutcome"] == 'Outcome_36_1'
    assert results["F1"]["endAppealOutcome"] == 'Outcome_39_25'
    assert results["G1"]["endAppealOutcome"] == 'Outcome_52_91'
    assert results["H1"]["endAppealOutcome"] == 'Outcome_37_80'

def test_endAppealOutcomeReason(spark,ended_outputs):

    results = ended_outputs

    assert results["A1"]["endAppealOutcomeReason"] == 'Reason_46_31'
    assert results["C1"]["endAppealOutcomeReason"] == 'Reason_51_94'
    assert results["E1"]["endAppealOutcomeReason"] == 'Reason_36_1'
    assert results["F1"]["endAppealOutcomeReason"] == 'Reason_39_25'
    assert results["G1"]["endAppealOutcomeReason"] == 'Reason_52_91'
    assert results["H1"]["endAppealOutcomeReason"] == 'Reason_37_80'

def test_endAppealApproverType(spark,ended_outputs):

    results = ended_outputs

    assert results["A1"]["endAppealApproverType"] == 'Judge'
    assert results["C1"]["endAppealApproverType"] == 'Case Worker'
    assert results["E1"]["endAppealApproverType"] == 'Case Worker'
    assert results["F1"]["endAppealApproverType"] == 'Case Worker'
    assert results["G1"]["endAppealApproverType"] == 'Case Worker'
    assert results["H1"]["endAppealApproverType"] == 'Case Worker'


def test_endAppealApproverName(spark,ended_outputs):

    results = ended_outputs

    assert results["A1"]["endAppealApproverName"] == 'Dr Alice Brown'
    assert results["C1"]["endAppealApproverName"] == 'This is a migrated ARIA case'
    assert results["E1"]["endAppealApproverName"] == 'This is a migrated ARIA case'
    assert results["F1"]["endAppealApproverName"] == 'This is a migrated ARIA case'
    assert results["G1"]["endAppealApproverName"] == 'This is a migrated ARIA case'
    assert results["H1"]["endAppealApproverName"] == 'This is a migrated ARIA case'


def test_endAppealDate(spark,ended_outputs):

    results = ended_outputs

    assert results["A1"]["endAppealDate"] == '04/03/2024'
    assert results["C1"]["endAppealDate"] == '02/12/2023'
    assert results["E1"]["endAppealDate"] == '05/08/2022'
    assert results["F1"]["endAppealDate"] == '06/05/2024'
    assert results["G1"]["endAppealDate"] == '01/06/2024'
    assert results["H1"]["endAppealDate"] == '01/07/2024'

def test_stateBeforeEndAppeal(spark,ended_outputs):

    results = ended_outputs

    assert results["A1"]["stateBeforeEndAppeal"] == 'State_46'
    assert results["C1"]["stateBeforeEndAppeal"] == 'State_51'
    assert results["E1"]["stateBeforeEndAppeal"] == 'State_36'
    assert results["F1"]["stateBeforeEndAppeal"] == 'State_39'
    assert results["G1"]["stateBeforeEndAppeal"] == 'State_52'
    assert results["H1"]["stateBeforeEndAppeal"] == 'State_37'
