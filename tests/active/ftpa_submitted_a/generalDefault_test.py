from Databricks.ACTIVE.APPEALS.shared_functions.ftpa_submitted_a import generalDefault
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

def test_isFtpaListVisible(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["isFtpaListVisible"] == "Yes"
    assert results["CASE002"]["isFtpaListVisible"] == "Yes"
    assert results["CASE003"]["isFtpaListVisible"] == "Yes"

def test_uploadAddendumEvidenceActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadAddendumEvidenceActionAvailable"] == "No"
    assert results["CASE002"]["uploadAddendumEvidenceActionAvailable"] == "No"
    assert results["CASE003"]["uploadAddendumEvidenceActionAvailable"] == "No"

def test_markAddendumEvidenceAsReviewedActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["markAddendumEvidenceAsReviewedActionAvailable"] == "No"
    assert results["CASE002"]["markAddendumEvidenceAsReviewedActionAvailable"] == "No"
    assert results["CASE003"]["markAddendumEvidenceAsReviewedActionAvailable"] == "No"

def test_uploadAddendumEvidenceLegalRepActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadAddendumEvidenceLegalRepActionAvailable"] == "No"
    assert results["CASE002"]["uploadAddendumEvidenceLegalRepActionAvailable"] == "No"
    assert results["CASE003"]["uploadAddendumEvidenceLegalRepActionAvailable"] == "No"

def test_uploadAddendumEvidenceHomeOfficeActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadAddendumEvidenceHomeOfficeActionAvailable"] == "No"
    assert results["CASE002"]["uploadAddendumEvidenceHomeOfficeActionAvailable"] == "No"
    assert results["CASE003"]["uploadAddendumEvidenceHomeOfficeActionAvailable"] == "No"

def test_uploadAddendumEvidenceAdminOfficerActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadAddendumEvidenceAdminOfficerActionAvailable"] == "No"
    assert results["CASE002"]["uploadAddendumEvidenceAdminOfficerActionAvailable"] == "No"
    assert results["CASE003"]["uploadAddendumEvidenceAdminOfficerActionAvailable"] == "No"

def test_uploadHomeOfficeAppealResponseActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadHomeOfficeAppealResponseActionAvailable"] == "Yes"
    assert results["CASE002"]["uploadHomeOfficeAppealResponseActionAvailable"] == "Yes"
    assert results["CASE003"]["uploadHomeOfficeAppealResponseActionAvailable"] == "Yes"
