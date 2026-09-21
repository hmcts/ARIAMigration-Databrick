from Databricks.ACTIVE.APPEALS.shared_functions.decision import generalDefault
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

def test_hmcts(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["hmcts"] == "[userImage:hmcts.png]"
    assert results["CASE002"]["hmcts"] == "[userImage:hmcts.png]"
    assert results["CASE003"]["hmcts"] == "[userImage:hmcts.png]"

def test_stitchingStatus(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["stitchingStatus"] == "DONE"
    assert results["CASE002"]["stitchingStatus"] == "DONE"
    assert results["CASE003"]["stitchingStatus"] == "DONE"

def test_bundleConfiguration(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["bundleConfiguration"] == "iac-hearing-bundle-config.yaml"
    assert results["CASE002"]["bundleConfiguration"] == "iac-hearing-bundle-config.yaml"
    assert results["CASE003"]["bundleConfiguration"] == "iac-hearing-bundle-config.yaml"

def test_decisionAndReasonsAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["decisionAndReasonsAvailable"] == "No"
    assert results["CASE002"]["decisionAndReasonsAvailable"] == "No"
    assert results["CASE003"]["decisionAndReasonsAvailable"] == "No"

def test_sendDirectionActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["sendDirectionActionAvailable"] == "No"
    assert results["CASE002"]["sendDirectionActionAvailable"] == "No"
    assert results["CASE003"]["sendDirectionActionAvailable"] == "No"

def test_changeDirectionDueDateActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["changeDirectionDueDateActionAvailable"] == "No"
    assert results["CASE002"]["changeDirectionDueDateActionAvailable"] == "No"
    assert results["CASE003"]["changeDirectionDueDateActionAvailable"] == "No"

def test_markEvidenceAsReviewedActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["markEvidenceAsReviewedActionAvailable"] == "No"
    assert results["CASE002"]["markEvidenceAsReviewedActionAvailable"] == "No"
    assert results["CASE003"]["markEvidenceAsReviewedActionAvailable"] == "No"

def test_uploadAddendumEvidenceActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadAddendumEvidenceActionAvailable"] == "Yes"
    assert results["CASE002"]["uploadAddendumEvidenceActionAvailable"] == "Yes"
    assert results["CASE003"]["uploadAddendumEvidenceActionAvailable"] == "Yes"

def test_uploadAdditionalEvidenceActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadAdditionalEvidenceActionAvailable"] == "No"
    assert results["CASE002"]["uploadAdditionalEvidenceActionAvailable"] == "No"
    assert results["CASE003"]["uploadAdditionalEvidenceActionAvailable"] == "No"

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

def test_uploadAdditionalEvidenceHomeOfficeActionAvailable(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["uploadAdditionalEvidenceHomeOfficeActionAvailable"] == "No"
    assert results["CASE002"]["uploadAdditionalEvidenceHomeOfficeActionAvailable"] == "No"
    assert results["CASE003"]["uploadAdditionalEvidenceHomeOfficeActionAvailable"] == "No"
