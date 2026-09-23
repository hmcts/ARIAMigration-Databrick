from Databricks.ACTIVE.APPEALS.shared_functions.prepareForHearing import generalDefault
from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("generalDefaultTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def generalDefault_outputs(spark):
    data = [
        ("CASE001", "AIP", "FT"),
        ("CASE002", "LR", "FT"),
        ("CASE003", None, None)
    ]
    columns = ["CaseNo", "dv_representation", "lu_appealType"]
    df = spark.createDataFrame(data, columns)

    generalDefault_content = generalDefault(df)

    results = {row["CaseNo"]: row.asDict() for row in generalDefault_content.collect()}
    return results, generalDefault_content.columns

def test_reviewedHearingRequirements(spark, generalDefault_outputs):

    results, _ = generalDefault_outputs

    assert results["CASE001"]["reviewedHearingRequirements"] == "Yes"
    assert results["CASE002"]["reviewedHearingRequirements"] == "Yes"
    assert results["CASE003"]["reviewedHearingRequirements"] == "Yes"

def test_reviewedHearingRequirements_not_duplicated(spark, generalDefault_outputs):

    _, columns = generalDefault_outputs

    assert columns.count("reviewedHearingRequirements") == 1
