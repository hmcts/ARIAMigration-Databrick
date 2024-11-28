# Databricks notebook source
display(dbutils.fs.ls("/mnt/ingest00landingsboxlanding"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_timestamp
import re
 
# Initialize Spark session
# spark = SparkSession.builder.appName("SingleFileParquetWriter").getOrCreate()
 
# Define schema for an empty DataFrame
review_specific_direction_schema = StructType([
    StructField("ReviewSpecificDirectionId", IntegerType(), False),
    StructField("CaseNo", StringType(), False),
    StructField("StatusId", IntegerType(), False),
    StructField("SpecificDirection", StringType(), True),
    StructField("DateRequiredIND", TimestampType(), True),
    StructField("DateRequiredAppellantRep", TimestampType(), True),
    StructField("DateReceivedIND", TimestampType(), True),
    StructField("DateReceivedAppellantRep", TimestampType(), True)
])
 
# Create an empty DataFrame with the defined schema
review_specific_direction_df = spark.createDataFrame([], review_specific_direction_schema)
 
# Generate timestamp for unique file naming
datesnap = spark.sql("select date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]
 
# Temporary output path (to generate a single .parquet file within a folder)
temp_output_path = f"/mnt/ingest00landingsboxlanding/test/ReviewSpecificDirection/temp_{datesnap}"
review_specific_direction_df.coalesce(1).write.format("parquet").mode("overwrite").save(temp_output_path)
 
# Get the single .parquet file generated in the temporary folder
files = dbutils.fs.ls(temp_output_path)
parquet_file = [file.path for file in files if re.match(r".*\.parquet$", file.path)][0]
 
# Final output path for the single .parquet file
final_output_path = f"/mnt/ingest00landingsboxlanding/test/ReviewSpecificDirection//full/SQLServer_Sales_IRIS_dbo_ReviewSpecificDirection_{datesnap}.parquet"
 
# Move the single .parquet file to the desired location
dbutils.fs.mv(parquet_file, final_output_path)
 
# Clean up the temporary folder
dbutils.fs.rm(temp_output_path, True)
 
# Read and display schema to confirm the file output
df = spark.read.format("parquet").load(final_output_path)
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.ariadm_arm_joh.raw_adjudicator

# COMMAND ----------

name = "adjudicator_role"
print(name.capitalize())

# COMMAND ----------


