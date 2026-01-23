from Databricks.ACTIVE.APPEALS.shared_functions.decision import substantiveDecision
from pyspark.sql import SparkSession
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("substantiveDecisionTests")
        .getOrCreate()
    )

##### Testing the substantiveDecision field grouping function #####
@pytest.fixture(scope="session")
def substantiveDecision_outputs(spark):
    data = [
        ("CASE001", "AIP", "FT"),
        ("CASE002", "LR", "FT"),
        ("CASE003", None, None)
    ]
    columns = ["CaseNo", "dv_representation", "lu_appealType"]
    df =  spark.createDataFrame(data, columns)

    substantiveDecision_content,_ = substantiveDecision(df)

    results = {row["CaseNo"]: row.asDict() for row in substantiveDecision_content.collect()}
    return results


def test_scheduleOfIssuesAgreement(spark,substantiveDecision_outputs):

    results = substantiveDecision_outputs

    assert results["CASE001"]["scheduleOfIssuesAgreement"] == "No"
    assert results["CASE002"]["scheduleOfIssuesAgreement"] == "No"
    assert results["CASE003"]["scheduleOfIssuesAgreement"] == "No"

def test_scheduleOfIssuesDisagreementDescription(spark,substantiveDecision_outputs):

    results = substantiveDecision_outputs

    assert results["CASE001"]["scheduleOfIssuesDisagreementDescription"] == "This is a migrated ARIA case. Please see the documents for information on the schedule of issues."
    assert results["CASE002"]["scheduleOfIssuesDisagreementDescription"] == "This is a migrated ARIA case. Please see the documents for information on the schedule of issues."
    assert results["CASE003"]["scheduleOfIssuesDisagreementDescription"] == "This is a migrated ARIA case. Please see the documents for information on the schedule of issues."

def test_immigrationHistoryAgreement(spark,substantiveDecision_outputs):

    results = substantiveDecision_outputs

    assert results["CASE001"]["immigrationHistoryAgreement"] == "No"
    assert results["CASE002"]["immigrationHistoryAgreement"] == "No"
    assert results["CASE003"]["immigrationHistoryAgreement"] == "No"

def test_immigrationHistoryDisagreementDescription(spark,substantiveDecision_outputs):

    results = substantiveDecision_outputs

    assert results["CASE001"]["immigrationHistoryDisagreementDescription"] == "This is a migrated ARIA case. Please see the documents for information on the immigration history."
    assert results["CASE002"]["immigrationHistoryDisagreementDescription"] == "This is a migrated ARIA case. Please see the documents for information on the immigration history."
    assert results["CASE003"]["immigrationHistoryDisagreementDescription"] == "This is a migrated ARIA case. Please see the documents for information on the immigration history."

