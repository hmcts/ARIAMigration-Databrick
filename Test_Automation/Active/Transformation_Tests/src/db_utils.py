import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

# --------------------------
# Get runs / results
# --------------------------
def get_runs():
    return spark.table("test_automation_runs")

def get_results(run_id=None):
    if run_id:
        return spark.sql(f"SELECT * FROM test_automation_results WHERE run_id = '{run_id}'")
    else:
        return spark.table("test_automation_results")
