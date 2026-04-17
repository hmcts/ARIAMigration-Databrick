import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

# --------------------------
# Get runs / results
# --------------------------
def get_runs(runs_table):
    return spark.table(runs_table)

# def get_results(results_table,run_id=None):
#     if run_id:
#         return spark.sql(f"SELECT * FROM test_automation_results WHERE run_id = '{run_id}'")
#     else:
#         return spark.table(results_table)

from pyspark.sql.functions import col, trim, lower

def get_results(table_name, run_id):
    return (
        spark.table(table_name)
        .filter(trim(lower(col("run_id"))) == run_id.lower())
    )
