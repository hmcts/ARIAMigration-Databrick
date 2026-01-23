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

    generalDefault_content,_ = generalDefault(df)

    results = {row["CaseNo"]: row.asDict() for row in generalDefault_content.collect()}
    return results

        .withColumn("hmcts", lit("[userImage:hmcts.png]"))
        .withColumn("stitchingStatus", lit("DONE"))
        .withColumn("bundleConfiguration", lit("iac-hearing-bundle-config.yaml"))
        .withColumn("decisionAndReasonsAvailable", lit("No"))

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

