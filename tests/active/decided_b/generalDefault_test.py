from Databricks.ACTIVE.APPEALS.shared_functions.decided_b import generalDefault
from pyspark.sql import SparkSession
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("generalDefaultTests")
        .getOrCreate()
    )

##### Testing the documents field grouping function #####
@pytest.fixture(scope="session")
def generalDefault_outputs(spark):
    data = [
        ("CASE001", "AIP", "FT"),
        ("CASE002", "LR", "FT"),
        ("CASE003", None, None)
    ]
    columns = ["CaseNo", "dv_representation", "lu_appealType"]
    df =  spark.createDataFrame(data, columns)

    generalDefault_content = generalDefault(df)

    results = {row["CaseNo"]: row.asDict() for row in generalDefault_content.collect()}
    return results

def test_isDlrmSetAsideEnabled(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["isDlrmSetAsideEnabled"] == "Yes"
    assert results["CASE002"]["isDlrmSetAsideEnabled"] == "Yes"
    assert results["CASE003"]["isDlrmSetAsideEnabled"] == "Yes"

def test_isReheardAppealEnabled(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["isReheardAppealEnabled"] == "Yes"
    assert results["CASE002"]["isReheardAppealEnabled"] == "Yes"
    assert results["CASE003"]["isReheardAppealEnabled"] == "Yes"

def test_secondFtpaDecisionExists(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["secondFtpaDecisionExists"] == "No"
    assert results["CASE002"]["secondFtpaDecisionExists"] == "No"
    assert results["CASE003"]["secondFtpaDecisionExists"] == "No"

def test_caseFlagSetAsideReheardExists(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["caseFlagSetAsideReheardExists"] == "Yes"
    assert results["CASE002"]["caseFlagSetAsideReheardExists"] == "Yes"
    assert results["CASE003"]["caseFlagSetAsideReheardExists"] == "Yes"

def test_uploadAddendumEvidenceActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadAddendumEvidenceActionAvailable"] == "Yes"
    assert results["CASE002"]["uploadAddendumEvidenceActionAvailable"] == "Yes"
    assert results["CASE003"]["uploadAddendumEvidenceActionAvailable"] == "Yes"

def test_markAddendumEvidenceAsReviewedActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["markAddendumEvidenceAsReviewedActionAvailable"] == "Yes"
    assert results["CASE002"]["markAddendumEvidenceAsReviewedActionAvailable"] == "Yes"
    assert results["CASE003"]["markAddendumEvidenceAsReviewedActionAvailable"] == "Yes"

def test_uploadAddendumEvidenceLegalRepActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadAddendumEvidenceLegalRepActionAvailable"] == "Yes"
    assert results["CASE002"]["uploadAddendumEvidenceLegalRepActionAvailable"] == "Yes"
    assert results["CASE003"]["uploadAddendumEvidenceLegalRepActionAvailable"] == "Yes"

def test_uploadAddendumEvidenceHomeOfficeActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadAddendumEvidenceHomeOfficeActionAvailable"] == "Yes"
    assert results["CASE002"]["uploadAddendumEvidenceHomeOfficeActionAvailable"] == "Yes"
    assert results["CASE003"]["uploadAddendumEvidenceHomeOfficeActionAvailable"] == "Yes"

def test_uploadAddendumEvidenceAdminOfficerActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadAddendumEvidenceAdminOfficerActionAvailable"] == "Yes"
    assert results["CASE002"]["uploadAddendumEvidenceAdminOfficerActionAvailable"] == "Yes"
    assert results["CASE003"]["uploadAddendumEvidenceAdminOfficerActionAvailable"] == "Yes"

def test_reviewResponseActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["reviewResponseActionAvailable"] == "Yes"
    assert results["CASE002"]["reviewResponseActionAvailable"] == "Yes"
    assert results["CASE003"]["reviewResponseActionAvailable"] == "Yes"

def test_uploadHomeOfficeAppealResponseActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadHomeOfficeAppealResponseActionAvailable"] == "No"
    assert results["CASE002"]["uploadHomeOfficeAppealResponseActionAvailable"] == "No"
    assert results["CASE003"]["uploadHomeOfficeAppealResponseActionAvailable"] == "No"
