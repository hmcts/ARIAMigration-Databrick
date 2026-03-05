from Databricks.ACTIVE.APPEALS.shared_functions.remitted import generalDefault
from pyspark.sql import SparkSession
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("remittedGeneralDefaultTests")
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

def test_caseFlagSetAsideReheardExists(spark,generalDefault_outputs):

    results = generalDefault_outputs

    assert results["CASE001"]["caseFlagSetAsideReheardExists"] == "Yes"
    assert results["CASE002"]["caseFlagSetAsideReheardExists"] == "Yes"
    assert results["CASE003"]["caseFlagSetAsideReheardExists"] == "Yes"


