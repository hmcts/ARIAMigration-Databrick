from Databricks.ACTIVE.APPEALS.shared_functions.decision import documents
from pyspark.sql import SparkSession
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("DocumentsTests")
        .getOrCreate()
    )

##### Testing the documents field grouping function #####
@pytest.fixture(scope="session")
def documents_outputs(spark):
    data = [
        ("CASE001", "AIP", "FT"),
        ("CASE002", "LR", "FT"),
        ("CASE003", None, None)
    ]
    columns = ["CaseNo", "dv_representation", "lu_appealType"]
    df =  spark.createDataFrame(data, columns)

    documents_content,_ = documents(df)

    results = {row["CaseNo"]: row.asDict() for row in documents_content.collect()}
    return results


def test_caseBundles(spark,documents_outputs):

    results = documents_outputs

    assert results["CASE001"]["caseBundles"] == []
    assert results["CASE002"]["caseBundles"] == []
    assert results["CASE003"]["caseBundles"] == []

