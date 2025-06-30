# Databricks notebook source
# MAGIC %pip install pyspark azure-storage-blob

# COMMAND ----------

# MAGIC %md
# MAGIC # Tribunal Decision Archive
# MAGIC <table style = 'float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Name: </b></td>
# MAGIC          <td>ARIADM_ARM_TD</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Description: </b></td>
# MAGIC          <td>Notebook to generate a set of HTML, JSON, and A360 files, each representing the data about Tribunal Decision stored in ARIA.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>First Created: </b></td>
# MAGIC          <td>Sep-2024 </td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <th style='text-align: left; '><b>Changelog(JIRA ref/initials./date):</b></th>
# MAGIC          <th>Comments </th>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-124">ARIADM-124</a>/NSA/SEP-2024</td>
# MAGIC          <td>Tribunal Decision : Compete Landing to Bronze Notebook</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-65">ARIADM-65</a>/NSA/SEP-2024</td>
# MAGIC          <td>Tribunal Decision : Compete Bronze silver Notebook</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-125">ARIADM-125</a>/NSA/SEP-2024</td>
# MAGIC          <td>Tribunal Decision : Compete Gold Outputs </td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC     <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-137">ARIADM-137</a>/NSA/16-OCT-2024</td>
# MAGIC     <td>TD: Tune Performance, Refactor Code for Reusability, Manage Broadcast Effectively, Implement Repartitioning Strategy</td>
# MAGIC </tr>
# MAGIC       <tr>
# MAGIC     <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-145">ARIADM-145</a>/NSA/28-OCT-2024</td>
# MAGIC     <td>Tribunal Decision IRIS : Compete Landing to Bronze Notebook</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC     <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-146">ARIADM-146</a>/NSA/28-OCT-2024</td>
# MAGIC     <td>Tribunal Decision IRIS : Update Sliver layer logic</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC     <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-147">ARIADM-147</a>/NSA/28-OCT-2024</td>
# MAGIC     <td>Tribunal Decision IRIS : Update/Optmize Gold Outputs</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC     <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-368">ARIADM-368</a>/NSA/20-JAN-2025</td>
# MAGIC     <td>Update Datetype for Gold OutPuts</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC     <td style='text-align: left; '><a href=https://tools.hmcts.net/jira/browse/ARIADM-294">ARIADM-294</a>/NSA/28-JAN-2025</td>
# MAGIC     <td>Optimize Spark Workflows</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC     <td style='text-align: left; '><a href=https://tools.hmcts.net/jira/browse/ARIADM-431">ARIADM-431</a>/<a href=https://tools.hmcts.net/jira/browse/ARIADM-430">ARIADM-430</a>/NSA/03-FEB-2025</td>
# MAGIC     <td>BugFix</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC     <td style='text-align: left; '><a href=https://tools.hmcts.net/jira/browse/ARIADM-473">ARIADM-473</a>/NSA/07-MAR-2025</td>
# MAGIC     <td>Implement A360 batching for TD</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC     <td style='text-align: left; '><a href=https://tools.hmcts.net/jira/browse/ARIADM-432">ARIADM-432</a>/NSA/07-MAR-2025</td>
# MAGIC     <td>TD - Uniqueness for file names and null values</td>
# MAGIC </tr>
# MAGIC
# MAGIC
# MAGIC    </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import packages

# COMMAND ----------

pip install azure-storage-blob

# COMMAND ----------

# run custom functions
import sys
import os
# Append the parent directory to sys.path
# sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..','..')))

import dlt
import json
# from pyspark.sql.functions import when, col,coalesce, current_timestamp, lit, date_format
from pyspark.sql.functions import *
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions to Read Latest Landing Files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Variables

# COMMAND ----------

# DBTITLE 1,Extract Environment Details and Generate KeyVault Name
config = spark.read.option("multiline", "true").json("dbfs:/configs/config.json")
env_name = config.first()["env"].strip().lower()
lz_key = config.first()["lz_key"].strip().lower()

print(f"env_code: {lz_key}")  # This won't be redacted
print(f"env_name: {env_name}")  # This won't be redacted

KeyVault_name = f"ingest{lz_key}-meta002-{env_name}"
print(f"KeyVault_name: {KeyVault_name}") 

# COMMAND ----------

# DBTITLE 1,Configure SP OAuth

# Service principal credentials
client_id = dbutils.secrets.get(KeyVault_name, "SERVICE-PRINCIPLE-CLIENT-ID")
client_secret = dbutils.secrets.get(KeyVault_name, "SERVICE-PRINCIPLE-CLIENT-SECRET")
tenant_id = dbutils.secrets.get(KeyVault_name, "SERVICE-PRINCIPLE-TENANT-ID")

# Storage account names
curated_storage = f"ingest{lz_key}curated{env_name}"
checkpoint_storage = f"ingest{lz_key}xcutting{env_name}"
raw_storage = f"ingest{lz_key}raw{env_name}"
landing_storage = f"ingest{lz_key}landing{env_name}"

# Spark config for curated storage (Delta table)
spark.conf.set(f"fs.azure.account.auth.type.{curated_storage}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{curated_storage}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{curated_storage}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{curated_storage}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{curated_storage}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Spark config for checkpoint storage
spark.conf.set(f"fs.azure.account.auth.type.{checkpoint_storage}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{checkpoint_storage}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{checkpoint_storage}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{checkpoint_storage}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{checkpoint_storage}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Spark config for checkpoint storage
spark.conf.set(f"fs.azure.account.auth.type.{raw_storage}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{raw_storage}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{raw_storage}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{raw_storage}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{raw_storage}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Spark config for checkpoint storage
spark.conf.set(f"fs.azure.account.auth.type.{landing_storage}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{landing_storage}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{landing_storage}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{landing_storage}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{landing_storage}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC Please note that running the DLT pipeline with the parameter `read_hive = true` will ensure the creation of the corresponding Hive tables. However, during this stage, none of the gold outputs (HTML, JSON, and A360) are processed. To generate the gold outputs, a secondary run with `read_hive = true` is required.

# COMMAND ----------

# DBTITLE 1,Set Paths and Hive Schema Variables
read_hive = False

# raw_mnt = "/mnt/ingest00rawsboxraw/ARIADM/ARM/TD"
# raw_mnt = f"abfss://raw@ingest00rawsbox.dfs.core.windows.net/ARIADM/ARM/TD"
raw_mnt = f"abfss://raw@ingest{lz_key}raw{env_name}.dfs.core.windows.net/ARIADM/ARM/TD"
landing_mnt = f"abfss://landing@ingest{lz_key}landing{env_name}.dfs.core.windows.net/SQLServer/Sales/IRIS/dbo/"
bronze_mnt = f"abfss://bronze@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/ARM/TD"
silver_mnt = f"abfss://silver@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/ARM/TD"
gold_mnt = f"abfss://gold@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/ARM/TD"
file_path = f"abfss://landing@ingest{lz_key}landing{env_name}.dfs.core.windows.net/SQLServer/Sales/IRIS/dbo/IRIS-TD-CSV/Example IRIS tribunal decisions data file.csv"
gold_outputs = "ARIADM/ARM/TD"
hive_schema = "ariadm_arm_td"
audit_delta_path = f"abfss://silver@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/ARM/AUDIT/TD/td_cr_audit_table"
audit_mnt = f"abfss://silver@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/ARM/AUDIT/TD"
html_mnt = f"abfss://html-template@ingest{lz_key}landing{env_name}.dfs.core.windows.net/"

# audit_table_name = audit_delta_path.split("/")[-1]

# Print all variables
variables = {
    "read_hive": read_hive,
    "raw_mnt": raw_mnt,
    "landing_mnt": landing_mnt,
    "bronze_mnt": bronze_mnt,
    "silver_mnt": silver_mnt,
    "gold_mnt": gold_mnt,
    "html_mnt": html_mnt,
    "gold_outputs": gold_outputs,
    "hive_schema": hive_schema,
    "key_vault": KeyVault_name,
    "audit_delta_path": audit_delta_path,
    "audit_mnt": audit_mnt,
    "file_path": file_path
}

display(variables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw DLT Tables Creation
# MAGIC
# MAGIC ```
# MAGIC AppealCase
# MAGIC CaseAppellant
# MAGIC Appellant
# MAGIC FileLocation
# MAGIC Department
# MAGIC HearingCentre
# MAGIC Status
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Read Latest Parquet File

# Function to recursively list all files in the ADLS directory
def deep_ls(path: str, depth: int = 0, max_depth: int = 10) -> list:
    """
    Recursively list all files and directories in ADLS directory.
    Returns a list of all paths found.
    """
    output = set()  # Using a set to avoid duplicates
    if depth > max_depth:
        return list(output)

    try:
        children = dbutils.fs.ls(path)
        for child in children:
            if child.path.endswith(".parquet"):
                output.add(child.path.strip())  # Add only .parquet files to the set

            if child.isDir:
                output.update(deep_ls(child.path, depth=depth + 1, max_depth=max_depth))

    except Exception as e:
        print(f"Error accessing {path}: {e}")

    return list(output)

# Main function to read the latest parquet file, add audit columns, and return the DataFrame
def read_latest_parquet(folder_name: str, view_name: str, process_name: str, base_path: str = landing_mnt) -> "DataFrame":
    """
    Reads the latest .parquet file from a specified folder, adds audit columns, creates a temporary Spark view, and returns the DataFrame.
    
    Parameters:
    - folder_name (str): The name of the folder to look for the .parquet files (e.g., "AdjudicatorRole").
    - view_name (str): The name of the temporary view to create (e.g., "tv_AdjudicatorRole").
    - process_name (str): The name of the process adding the audit information (e.g., "ARIA_ARM_JOH").
    - base_path (str): The base path for the folders in the data lake.
    
    Returns:
    - DataFrame: The DataFrame created from the latest .parquet file with added audit columns.
    """
    # Construct the full folder path
    folder_path = f"{base_path}{folder_name}/full/"
    
    # List all .parquet files in the folder
    all_files = deep_ls(folder_path)
    
    # Check if files were found
    if not all_files:
        print(f"No .parquet files found in {folder_path}")
        return None

    # Create a DataFrame from the file paths
    file_df = spark.createDataFrame([(f,) for f in all_files], ["file_path"])
    
    # Extract timestamp from the file name using a regex pattern (assuming it's the last underscore-separated part before ".parquet")
    file_df = file_df.withColumn("timestamp", regexp_extract("file_path", r"_(\d+)\.parquet$", 1).cast("long"))
    
    # Find the maximum timestamp
    max_timestamp = file_df.agg(max("timestamp")).collect()[0][0]
    
    # Filter to get the file with the maximum timestamp
    latest_file_df = file_df.filter(col("timestamp") == max_timestamp)
    latest_file = latest_file_df.first()["file_path"]
    
    # Print the latest file being loaded for logging purposes
    print(f"Reading latest file: {latest_file}")
    
    # Read the latest .parquet file into a DataFrame
    df = spark.read.option("inferSchema", "true").parquet(latest_file)
    
    # Add audit columns
    df = df.withColumn("AdtclmnFirstCreatedDatetime", current_timestamp()) \
           .withColumn("AdtclmnModifiedDatetime", current_timestamp()) \
           .withColumn("SourceFileName", lit(latest_file)) \
           .withColumn("InsertedByProcessName", lit(process_name))
    
    # Create or replace a temporary view
    df.createOrReplaceTempView(view_name)
    
    print(f"Loaded the latest file for {folder_name} into view {view_name} with audit columns")
    
    # Return the DataFrame
    return df


# COMMAND ----------

@dlt.table(
    name="raw_appealcase",
    comment="Delta Live Table ARIA AppealCase.",
    path=f"{raw_mnt}/Raw_AppealCase"
)
def Raw_AppealCase():
    return read_latest_parquet("AppealCase", "tv_AppealCase", "ARIA_ARM_TD")

@dlt.table(
    name="raw_caseappellant",
    comment="Delta Live Table ARIA CaseAppellant.",
    path=f"{raw_mnt}/Raw_CaseAppellant"
)
def Raw_CaseAppellant():
    return read_latest_parquet("CaseAppellant", "tv_CaseAppellant", "ARIA_ARM_JOH")

@dlt.table(
    name="raw_appellant",
    comment="Delta Live Table ARIA Appellant.",
    path=f"{raw_mnt}/Raw_Appellant"
)
def raw_Appellant():
     return read_latest_parquet("Appellant", "tv_Appellant", "ARIA_ARM_JOH")

@dlt.table(
    name="raw_filelocation",
    comment="Delta Live Table ARIA FileLocation.",
    path=f"{raw_mnt}/Raw_FileLocation"
)
def Raw_FileLocation():
    return read_latest_parquet("FileLocation", "tv_FileLocation", "ARIA_ARM_JOH")

@dlt.table(
    name="raw_department",
    comment="Delta Live Table ARIA Department.",
    path=f"{raw_mnt}/Raw_Department"
)
def Raw_Department():
    return read_latest_parquet("Department", "tv_Department", "ARIA_ARM_JOH")

@dlt.table(
    name="raw_hearingcentre",
    comment="Delta Live Table ARIA HearingCentre.",
    path=f"{raw_mnt}/Raw_HearingCentre"
)
def Raw_HearingCentre():
    return read_latest_parquet("ARIAHearingCentre", "tv_HearingCentre", "ARIA_ARM_JOH")

@dlt.table(
    name="raw_status",
    comment="Delta Live Table ARIA Status.",
    path=f"{raw_mnt}/Raw_Status"
)
def Raw_Status():
    return read_latest_parquet("Status", "tv_Status", "ARIA_ARM_JOH_ARA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Function

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from delta.tables import DeltaTable

# COMMAND ----------

# audit_schema = StructType([
#     StructField("Runid", StringType(), True),
#     StructField("Unique_identifier_desc", StringType(), True),
#     StructField("Unique_identifier", StringType(), True),
#     StructField("Table_name", StringType(), True),
#     StructField("Stage_name", StringType(), True),
#     StructField("Record_count", IntegerType(), True),
#     StructField("Run_dt",TimestampType(), True),
#     StructField("Batch_id", StringType(), True),
#     StructField("Description", StringType(), True),
#     StructField("File_name", StringType(), True),
#     StructField("Status", StringType(), True)
# ])

# COMMAND ----------

# def create_audit_df(df: DataFrame, unique_identifier_desc: str,table_name: str, stage_name: str, description: str, file_name = False,status = False) -> None:
#     """
#     Creates an audit DataFrame and writes it to Delta format.

#     :param df: Input DataFrame from which unique identifiers are extracted.
#     :param unique_identifier_desc: Column name that acts as a unique identifier.
#     :param table_name: Name of the source table.
#     :param stage_name: Name of the data processing stage.
#     :param description: Description of the table.
#     :param additional_columns: options File_name or Status. List of additional columns to include in the audit DataFrame.
#     """

#     dt_desc = datetime.utcnow()

#     additional_columns = []
#     if file_name is True:
#         additional_columns.append("File_name")
#     if status is True:
#         additional_columns.append("Status")


#      # Default to an empty list if None   
#     additional_columns = [col(c) for c in additional_columns if c is not None]  # Filter out None values

#     audit_df = df.select(col(unique_identifier_desc).alias("unique_identifier"),*additional_columns)\
#     .withColumn("Runid", lit(run_id_value))\
#         .withColumn("Unique_identifier_desc", lit(unique_identifier_desc))\
#             .withColumn("Stage_name", lit(stage_name))\
#                 .withColumn("Table_name", lit(table_name))\
#                     .withColumn("Run_dt", lit(dt_desc).cast(TimestampType()))\
#                         .withColumn("Description", lit(description))

#     list_cols = audit_df.columns

#     final_audit_df = audit_df.groupBy(*list_cols).agg(count("*").cast(IntegerType()).alias("Record_count"))

#     final_audit_df.write.format("delta").mode("append").option("mergeSchema","true").save(audit_delta_path)



# COMMAND ----------

# import uuid


# def datetime_uuid():
#     dt_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
#     return str(uuid.uuid5(uuid.NAMESPACE_DNS,dt_str))

# run_id_value = datetime_uuid()

# COMMAND ----------

# # Define Delta Table Path in Azure Storage


# if not DeltaTable.isDeltaTable(spark, audit_delta_path):
#     print(f"🛑 Delta table '{audit_delta_path}' does not exist. Creating an empty Delta table...")

#     # Create an empty DataFrame
#     empty_df = spark.createDataFrame([], audit_schema)

#     # Write the empty DataFrame in Delta format to create the table
#     empty_df.write.format("delta").mode("overwrite").save(audit_delta_path)

#     print("✅ Empty Delta table successfully created in Azure Storage.")
# else:
#     print(f"⚡ Delta table '{audit_delta_path}' already exists.")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Bronze DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_ac_ca_ant_fl_dt_hc 

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC SELECT ac.CaseNo, 
# MAGIC        a.Forenames, 
# MAGIC        a.Name, 
# MAGIC        a.BirthDate, 
# MAGIC        ac.DestructionDate, -- for those not already destroyed, which date to use here? 
# MAGIC        ac.HORef, 
# MAGIC        a.PortReference, 
# MAGIC        hc.Description, 
# MAGIC        d.Description, 
# MAGIC        fl.Note 
# MAGIC FROM [dbo].[AppealCase] ac 
# MAGIC LEFT OUTER JOIN [dbo].[CaseAppellant] ca 
# MAGIC     ON ac.CaseNo = ca.CaseNo 
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Appellant] a 
# MAGIC     ON ca.AppellantId = a.AppellantId 
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[FileLocation] fl 
# MAGIC     ON ac.CaseNo = fl.CaseNo 
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Department] d 
# MAGIC     ON fl.DeptId = d.DeptId 
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingCentre] hc 
# MAGIC     ON d.CentreId = hc.CentreId
# MAGIC ```

# COMMAND ----------

checks = {}
checks["PrimaryKeyCheckforNULLS"] = "(CaseNo IS NOT NULL)"

@dlt.table(
    name="bronze_ac_ca_ant_fl_dt_hc",
    comment="Delta Live Table combining Appeal Case data with Case Appellant, Appellant, File Location, Department, and Hearing Centre.",
    path=f"{bronze_mnt}/bronze_ac_ca_ant_fl_dt_hc"
)
@dlt.expect_all_or_fail(checks)
def bronze_ac_ca_ant_fl_dt_hc():
    df = dlt.read("raw_appealcase").alias("ac") \
        .join(
            dlt.read("raw_caseappellant").alias("ca"),
            col("ac.CaseNo") == col("ca.CaseNo"),
            "left_outer"
        ) \
        .join(
            dlt.read("raw_appellant").alias("a"),
            col("ca.AppellantId") == col("a.AppellantId"),
            "left_outer"
        ) \
        .join(
            dlt.read("raw_filelocation").alias("fl"),
            col("ac.CaseNo") == col("fl.CaseNo"),
            "left_outer"
        ) \
        .join(
            dlt.read("raw_department").alias("d"),
            col("fl.DeptId") == col("d.DeptId"),
            "left_outer"
        ) \
        .join(
            dlt.read("raw_hearingcentre").alias("hc"),
            col("d.CentreId") == col("hc.CentreId"),
            "left_outer"
        ).filter(col("ca.RelationShip").isNull()) \
        .select(
            col("ac.CaseNo"),
            col("a.Forenames"),
            col("a.Name"),
            col("a.BirthDate"),
            col("ac.DestructionDate"),
            col("ac.HORef"),
            col("a.PortReference"),
            col("hc.Description").alias("HearingCentreDescription"),
            col("d.Description").alias("DepartmentDescription"),
            col("fl.Note"),
            col("ca.RelationShip"),
            col("ac.AdtclmnFirstCreatedDatetime"),
            col("ac.AdtclmnModifiedDatetime"),
            col("ac.SourceFileName"),
            col("ac.InsertedByProcessName")
        )

    return df


# COMMAND ----------

# df = spark.read.table("hive_metastore.ariadm_arm_td.raw_appealcase").alias("ac") \
#         .join(
#             spark.read.table("hive_metastore.ariadm_arm_td.raw_caseappellant").alias("ca"),
#             col("ac.CaseNo") == col("ca.CaseNo"),
#             "left_outer"
#         ) \
#         .join(
#             spark.read.table("hive_metastore.ariadm_arm_td.raw_appellant").alias("a"),
#             col("ca.AppellantId") == col("a.AppellantId"),
#             "left_outer"
#         ) \
#         .join(
#             spark.read.table("hive_metastore.ariadm_arm_td.raw_filelocation").alias("fl"),
#             col("ac.CaseNo") == col("fl.CaseNo"),
#             "left_outer"
#         ) \
#         .join(
#             spark.read.table("hive_metastore.ariadm_arm_td.raw_department").alias("d"),
#             col("fl.DeptId") == col("d.DeptId"),
#             "left_outer"
#         ) \
#         .join(
#             spark.read.table("hive_metastore.ariadm_arm_td.raw_hearingcentre").alias("hc"),
#             col("d.CentreId") == col("hc.CentreId"),
#             "left_outer"
#         ).filter(col("ca.RelationShip").isNull()) \
#         .select(
#             col("ac.CaseNo"),
#             col("a.Forenames"),
#             col("a.Name"),
#             col("a.BirthDate"),
#             col("ac.DestructionDate"),
#             col("ac.HORef"),
#             col("a.PortReference"),
#             col("hc.Description").alias("HearingCentreDescription"),
#             col("d.Description").alias("DepartmentDescription"),
#             col("fl.Note"),
#             col("ca.RelationShip"),
#             col("ac.AdtclmnFirstCreatedDatetime"),
#             col("ac.AdtclmnModifiedDatetime"),
#             col("ac.SourceFileName"),
#             col("ac.InsertedByProcessName")
#         )

# # display(df.filter(col("CaseNo") == lit("VA/00003/2009")))
# display(df.filter(col("Forenames").isNull()))

# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_td.bronze_ac_ca_ant_fl_dt_hc
# where CaseNo = 'VA/00003/2009'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_iris_extract 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Note: Temporarily Excluded IRIS with a filter using .filter(lit(False))

# COMMAND ----------

@dlt.table(
    name="bronze_iris_extract",
    comment="Delta Live Table extracted from the IRIS Tribunal decision file extract.",
    path=f"{bronze_mnt}/bronze_iris_extract"
)
def bronze_iris_extract():
    df_iris = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(file_path) \
    .withColumn("AdtclmnFirstCreatedDatetime", current_timestamp()) \
    .withColumn("AdtclmnModifiedDatetime", current_timestamp()) \
    .withColumn("SourceFileName", lit(file_path)) \
    .withColumn("InsertedByProcessName", lit('ARIA_ARM_IRIS_TD')) \
    .select(
        col('AppCaseNo').alias('CaseNo'),
        col('Fornames').alias('Forenames'),
        col('Name'),
        col('BirthDate').cast("timestamp"),
        col('DestructionDate').cast("timestamp"),
        col('HORef'),
        col('PortReference'),
        col('File_Location').alias('HearingCentreDescription'),
        col('Description').alias('DepartmentDescription'),
        col('Note'),
        lit(None).alias("RelationShip").cast("string"),
        col('AdtclmnFirstCreatedDatetime'),
        col('AdtclmnModifiedDatetime'),
        col('SourceFileName'),
        col('InsertedByProcessName')
    ).filter(lit(False))

    return df_iris

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segmentation query (to be applied in silver):  - stg_td_filtered 
# MAGIC
# MAGIC ```sql
# MAGIC /* 
# MAGIC ARIA Data Segmentation 
# MAGIC Archive 
# MAGIC Tribunal Decisions 
# MAGIC 16/09/2024 
# MAGIC */ 
# MAGIC
# MAGIC SELECT  
# MAGIC     ac.CaseNo 
# MAGIC FROM dbo.AppealCase ac 
# MAGIC LEFT OUTER JOIN ( 
# MAGIC     SELECT MAX(StatusId) max_ID, Caseno 
# MAGIC     FROM dbo.Status 
# MAGIC     WHERE ISNULL(outcome, -1) NOT IN (38,111)  
# MAGIC     AND ISNULL(casestatus, -1) != 17 
# MAGIC     GROUP BY Caseno 
# MAGIC ) AS s ON ac.caseno = s.caseno 
# MAGIC LEFT OUTER JOIN dbo.Status t ON t.caseno = s.caseno AND t.statusID = s.max_ID 
# MAGIC LEFT OUTER JOIN (
# MAGIC     SELECT MAX(StatusID) as Prev_ID, CaseNo  
# MAGIC     FROM dbo.Status WHERE ISNULL(casestatus, -1) NOT IN (52,36) 
# MAGIC     GROUP BY CaseNo
# MAGIC ) AS Prev ON ac.CaseNo = prev.caseNo 
# MAGIC LEFT OUTER JOIN dbo.Status st ON st.caseno = prev.caseno AND st.StatusId = prev.Prev_ID  
# MAGIC LEFT OUTER JOIN dbo.FileLocation fl ON ac.caseNo = fl.caseNo 
# MAGIC WHERE	 
# MAGIC     ac.CaseType = 1 
# MAGIC     AND  
# MAGIC     CASE  
# MAGIC         WHEN ac.CasePrefix IN ('LP','LR', 'LD', 'LH', 'LE' ,'IA') AND ac.HOANRef IS NOT NULL THEN 'Skeleton Case' -- Excluding Skeleton cases 
# MAGIC         WHEN (  t.CaseStatus IN ('40','41','42','43','44','45','53','27','28','29','34','32','33') 
# MAGIC         AND t.Outcome IN ('0','86') ) THEN 'UT Active/Remitted Case' -- Excluding UT Active Cases & UT Remitted cases 
# MAGIC         WHEN fl.DeptId = 519 THEN 'Tribunal Decision' -- All National Archive, File Destroyed Cases 
# MAGIC         WHEN (	t.CaseStatus IS NULL 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 10 AND t.Outcome IN ('0','109','104','82','99','121','27','39') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 46 AND t.Outcome IN ('1','86') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 26 AND t.Outcome IN ('0','27','39','50','40','52','89') 
# MAGIC         OR 
# MAGIC         t.CaseStatus IN ('37','38') AND t.Outcome IN ('39','40','37','50','27','0','5') 
# MAGIC         OR  
# MAGIC         t.CaseStatus = 39 AND t.Outcome IN ('0','86') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 50 AND t.Outcome = 0 
# MAGIC         OR  
# MAGIC         t.CaseStatus IN ('52','36') AND t.Outcome = 0 AND st.DecisionDate IS NULL 
# MAGIC         ) THEN 'Active - CCD' -- Excluding FT Active Appeals 
# MAGIC         WHEN (	t.CaseStatus = 10 AND t.Outcome IN ('13','80','122','25','120','2','105','119') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 46 AND t.Outcome IN ('31','2','50') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 26 AND t.Outcome IN ('80','13','25','1','2') 
# MAGIC         OR 
# MAGIC         t.CaseStatus IN ('37','38') AND t.Outcome IN ('1','2','80','13','25','72','14') 
# MAGIC         OR  
# MAGIC         t.CaseStatus = 39 AND t.Outcome IN ('30','31','25','14') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 51 AND t.Outcome IN ('94','93') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 36 AND t.Outcome = 25 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 52 AND t.Outcome IN ('91','95') AND (st.CaseStatus NOT IN ('37','38','39','17') OR st.CaseStatus IS NULL) 
# MAGIC         ) AND DATEADD(MONTH,6,t.decisiondate) > GETDATE() THEN 'Retain - CCD' 	-- Excluding FT Retained Appeals | Using decision date from final/substantive decision where most recent status is the final/substantive decision 
# MAGIC         WHEN (	t.CaseStatus IN (52, 36) AND t.Outcome = 0 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 36 AND t.Outcome IN ('1','2','50','108') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 52 AND t.Outcome IN ('91','95') AND st.CaseStatus IN ('37','38','39','17') 
# MAGIC         ) AND DATEADD(MONTH,6,st.decisiondate) > GETDATE() THEN 'Retain - CCD' 	-- Excluding FT Retained Appeals | Using decision date from the Substantive decision where most recent status isn't the final/substantive decision	 
# MAGIC         ELSE 'Tribunal Decision' -- Every other appeal case needs a tribunal decision 
# MAGIC     END = 'Tribunal Decision' -- Filtering for just cases requiring a tribunal decision 
# MAGIC ORDER BY ac.CaseNo
# MAGIC ```

# COMMAND ----------

# import dlt
# from pyspark.sql.functions import col, when, coalesce

@dlt.table(
    name="stg_td_filtered",
    comment="Delta Live Table for appeal cases requiring tribunal decisions.",
    path=f"{silver_mnt}/stg_td_filtered"
)
def bronze_appeal_case_tribunal_decision():
    # Subquery for the max StatusId and Caseno filtering by outcome and casestatus
    status_subquery = (
        dlt.read("raw_status")
        .filter(
            (col("outcome").isNotNull() & (~col("outcome").isin(38,111))) &
            (col("casestatus").isNotNull() & (col("casestatus") != 17))
        )
        .groupBy("CaseNo")
        .agg({"StatusId": "max"})
        .withColumnRenamed("max(StatusId)", "max_ID")
    )

    # Subquery for the previous status excluding certain casestatus
    prev_subquery = (
        dlt.read("raw_status")
        .filter(
            (col("casestatus").isNotNull() & (col("casestatus").isin(52,36)))
        )
        .groupBy("CaseNo")
        .agg({"StatusId": "max"})
        .withColumnRenamed("max(StatusId)", "Prev_ID")
    )

    # Joining the tables
    result_df = (
        dlt.read("raw_appealcase").alias("ac")
        .join(status_subquery.alias("s"), col("ac.CaseNo") == col("s.CaseNo"), "left_outer")
        .join(dlt.read("raw_status").alias("t"), (col("t.CaseNo") == col("s.CaseNo")) & (col("t.StatusId") == col("s.max_ID")), "left_outer")
        .join(prev_subquery.alias("prev"), col("ac.CaseNo") == col("prev.CaseNo"), "left_outer")
        .join(dlt.read("raw_status").alias("st"), (col("st.CaseNo") == col("prev.CaseNo")) & (col("st.StatusId") == col("prev.Prev_ID")), "left_outer")
        .join(dlt.read("raw_filelocation").alias("fl"), col("ac.CaseNo") == col("fl.CaseNo"), "left_outer")
        .filter(
            (col("ac.CaseType") == 1) &
            (when(
                (col("ac.CasePrefix").isin("LP", "LR", "LD", "LH", "LE", "IA")) & (col("ac.HOANRef").isNotNull()),
                "Skeleton Case"
            )
            .when(
                (col("t.CaseStatus").isin("40", "41", "42", "43", "44", "45", "53", "27", "28", "29", "34", "32", "33")) &
                (col("t.Outcome").isin("0", "86")),
                "UT Active/Remitted Case"
            )
            .when(
                col("fl.DeptId") == 519,
                "Tribunal Decision"
            )
            .when(
                (col("t.CaseStatus").isNull()) |
                ((col("t.CaseStatus") == 10) & (col("t.Outcome").isin("0", "109", "104", "82", "99", "121", "27", "39"))) |
                ((col("t.CaseStatus") == 46) & (col("t.Outcome").isin("1", "86"))) |
                ((col("t.CaseStatus") == 26) & (col("t.Outcome").isin("0", "27", "39", "50", "40", "52", "89"))) |
                ((col("t.CaseStatus").isin("37", "38")) & (col("t.Outcome").isin("39", "40", "37", "50", "27", "0", "5"))) |
                ((col("t.CaseStatus") == 39) & (col("t.Outcome") == "0")) |
                ((col("t.CaseStatus") == 50) & (col("t.Outcome") == "0")) |
                ((col("t.CaseStatus").isin("52", "36")) & (col("t.Outcome") == "0") & col("st.DecisionDate").isNull()),
                "Active - CCD"
            )
            .when(
                (col("t.CaseStatus") == 10) & (col("t.Outcome").isin("13", "80", "122", "25", "120", "2", "105", "119")) |
                (col("t.CaseStatus") == 46) & (col("t.Outcome").isin("31", "2", "50")) |
                (col("t.CaseStatus") == 26) & (col("t.Outcome").isin("80", "13", "25", "1", "2")) |
                (col("t.CaseStatus").isin("37", "38")) & (col("t.Outcome").isin("1", "2", "80", "13", "25", "72", "14")) |
                (col("t.CaseStatus") == 39) & (col("t.Outcome").isin("30", "31", "25", "14")) |
                (col("t.CaseStatus") == 51) & (col("t.Outcome").isin("94", "93")) |
                (col("t.CaseStatus") == 36) & (col("t.Outcome") == 25) |
                (col("t.CaseStatus") == 52) & (col("t.Outcome").isin("91", "95")) &
                (col("st.CaseStatus").isNull() | ~col("st.CaseStatus").isin("37", "38", "39", "17")),
                "Retain - CCD"
            )
            .when(
                (col("t.CaseStatus").isin(52, 36)) & (col("t.Outcome") == 0) |
                (col("t.CaseStatus") == 36) & (col("t.Outcome").isin("1", "2", "50", "108")) |
                (col("t.CaseStatus") == 52) & (col("t.Outcome").isin("91", "95")) & col("st.CaseStatus").isin("37", "38", "39", "17"),
                "Retain - CCD"
            )
            .otherwise("Tribunal Decision") == "Tribunal Decision")
        )
        .select("ac.CaseNo")
        .orderBy("ac.CaseNo")
    )

    
    # table_name = "stg_td_filtered"
    # stage_name = "silver_stage"

    # description = "The stg_td_filtered - segmentation Table for appeal cases requiring tribunal decisions with unique list of CaseNo's"

    # unique_identifier_desc = "CaseNo"

    # create_audit_df(result_df,unique_identifier_desc,table_name,stage_name,description)


    return result_df

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Silver DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_tribunaldecision_detail
# MAGIC
# MAGIC

# COMMAND ----------

@dlt.table(
    name="silver_tribunaldecision_detail",
    comment="Delta Live silver Table for Tribunal Decision information.",
    path=f"{silver_mnt}/silver_tribunaldecision_detail"
)
def silver_tribunaldecision_detail():
    td_df = dlt.read("bronze_ac_ca_ant_fl_dt_hc").alias("td")
    flt_df = dlt.read("stg_td_filtered").alias('flt')
    iris_df = dlt.read("bronze_iris_extract").alias('iris')
    
    joined_df = td_df.join(flt_df, col("td.CaseNo") == col("flt.CaseNo"), "inner").select("td.*")

    df = joined_df.unionByName(iris_df)
    
    return df

# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_td.silver_tribunaldecision_detail
# where CaseNo = 'VA/00003/2009'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_archive_metadata
# MAGIC <table style='float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>Field</b></td>
# MAGIC          <td style='text-align: left;'><b>Maps to</b></td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>client_identifier</td>
# MAGIC          <td>CaseNo</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>event_date*</td>
# MAGIC          <td>Date of migration/generation.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>recordDate*</td>
# MAGIC          <td>Date of migration/generation.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>region*</td>
# MAGIC          <td>GBR</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>publisher*</td>
# MAGIC          <td>ARIA</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>record_class*</td>
# MAGIC          <td>ARIA Tribunal Decision</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>entitlement_tag/td>
# MAGIC          <td>IA_Tribunal</td>
# MAGIC       </tr>
# MAGIC    </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```
# MAGIC * = mandatory field. 
# MAGIC
# MAGIC The following fields will need to be configured as business metadata fields for this record class: 
# MAGIC ```
# MAGIC
# MAGIC <table style='float:left; margin-top: 20px;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>Field</b></td>
# MAGIC          <td style='text-align: left;'><b>Type</b></td>
# MAGIC          <td style='text-align: left;'><b>Maps to</b></td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>Forename</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>name</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>Birth Date</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>Date</td>
# MAGIC          <td>HO Reference</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>Port Reference</td>
# MAGIC       </tr>
# MAGIC    </tbody>
# MAGIC </table>
# MAGIC
# MAGIC ```
# MAGIC Please note: 
# MAGIC the bf_xxx indexes may change while being finalised with Through Technology 
# MAGIC Dates must be provided in Zulu time format ```
# MAGIC

# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_td.silver_archive_metadata 
# where bf_010 is null

# COMMAND ----------

@dlt.table(
    name="silver_archive_metadata",
    comment="Delta Live Silver Table for Archive Metadata data.",
    path=f"{silver_mnt}/silver_archive_metadata"
)
def silver_archive_metadata():
    td_df = dlt.read("bronze_ac_ca_ant_fl_dt_hc").alias("td").join(dlt.read("stg_td_filtered").alias('flt'), col("td.CaseNo") == col("flt.CaseNo"), "inner").select(
        col('td.CaseNo').alias('client_identifier'),
        # date_format(col('td.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_date"),
        # date_format(col('td.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("recordDate"),
        when(env_name == lit('sbox'), date_format(coalesce(col('td.AdtclmnFirstCreatedDatetime'), current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'")).otherwise(date_format(col('td.AdtclmnFirstCreatedDatetime'), 
        "yyyy-MM-dd'T'HH:mm:ss'Z'")).alias('event_date'),
        when(env_name == lit('sbox'), date_format(coalesce(col('td.AdtclmnFirstCreatedDatetime'), current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'")).otherwise(date_format(col('td.AdtclmnFirstCreatedDatetime'), 
        "yyyy-MM-dd'T'HH:mm:ss'Z'")).alias('recordDate'),
        lit("GBR").alias("region"),
        lit("ARIA").alias("publisher"),
        when(env_name == lit('sbox'), lit("ARIATDDEV")).otherwise(lit("ARIATD")).alias("record_class"),
        #lit("ARIATDDEV" if env_name == "sbox" else "ARIATD").alias("record_class"),
        # lit("ARIATD").alias("record_class"),
        # lit('IA_Tribunal').alias("entitlement_tag"),
        col("td.HORef").alias('bf_001'),
        col('td.Forenames').alias('bf_002'),
        col('td.Name').alias('bf_003'),
        # when(workspace_env["env"] == lit('dev-sbox'), date_format(coalesce(col('td.BirthDate'), current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'")).otherwise(date_format(col('td.BirthDate'), "yyyy-MM-dd'T'HH:mm:ss'Z'")).alias('bf_004'),
         when(env_name == lit('sbox'), date_format(coalesce(col('td.BirthDate'), current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'")).otherwise(date_format(col('td.BirthDate'), "yyyy-MM-dd'T'HH:mm:ss'Z'")).alias('bf_004'),
        col('td.PortReference').alias('bf_005'),
        col('td.HearingCentreDescription').alias('bf_006'),
        col("td.DepartmentDescription").alias('bf_007'),
        col("td.Note").alias('bf_008'),
         when((env_name == lit('sbox')), date_format(coalesce(col('td.DestructionDate'), current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'")).otherwise(date_format(col('td.DestructionDate'), "yyyy-MM-dd'T'HH:mm:ss'Z'")).alias('bf_010')
        
    )
    iris_df = dlt.read("bronze_iris_extract").alias("iris").select(
        col('iris.CaseNo').alias('client_identifier'),
        # date_format(col('iris.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_date"),
        # date_format(col('iris.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("recordDate"),
        when(env_name == lit('sbox'), date_format(coalesce(col('iris.AdtclmnFirstCreatedDatetime'), current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'")).otherwise(date_format(col('iris.AdtclmnFirstCreatedDatetime'), 
        "yyyy-MM-dd'T'HH:mm:ss'Z'")).alias('event_date'),
        when(env_name == lit('sbox'), date_format(coalesce(col('iris.AdtclmnFirstCreatedDatetime'), current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'")).otherwise(date_format(col('iris.AdtclmnFirstCreatedDatetime'), 
        "yyyy-MM-dd'T'HH:mm:ss'Z'")).alias('recordDate'),
        lit("GBR").alias("region"),
        lit("ARIA").alias("publisher"),
        lit("ARIATD").alias("record_class"),
        # lit('IA_Tribunal').alias("entitlement_tag"),
        col("iris.HORef").alias('bf_001'),
        col('iris.Forenames').alias('bf_002'),
        col('iris.Name').alias('bf_003'),
        # when(workspace_env["env"] == lit('dev-sbox'), date_format(coalesce(col('iris.BirthDate'), current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'")).otherwise(date_format(col('iris.BirthDate'), "yyyy-MM-dd'T'HH:mm:ss'Z'")).alias('bf_004'),
         when(env_name == lit('sbox'), date_format(coalesce(col('iris.BirthDate'), current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'")).otherwise(date_format(col('iris.BirthDate'), "yyyy-MM-dd'T'HH:mm:ss'Z'")).alias('bf_004'),
        col('iris.PortReference').alias('bf_005'),
        col('iris.HearingCentreDescription').alias('bf_006'),
        col("iris.DepartmentDescription").alias('bf_007'),
        col("iris.Note").alias('bf_008'),
        when((env_name == lit('sbox') ), date_format(coalesce(col('iris.DestructionDate'), current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'")).otherwise(date_format(col('iris.DestructionDate'), "yyyy-MM-dd'T'HH:mm:ss'Z'")).alias('bf_010')
    )
    df = td_df.unionByName(iris_df)


    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver DLT staging table for gold transformation

# COMMAND ----------

# DBTITLE 1,Secret Retrieval for Database Connection
secret = dbutils.secrets.get(KeyVault_name, "CURATED-sbox-SAS-TOKEN")

# COMMAND ----------

# DBTITLE 1,Azure Blob Storage Container Access
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

# Set up the BlobServiceClient with your connection string
connection_string = secret

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Specify the container name
container_name = "gold"
container_client = blob_service_client.get_container_client(container_name)


# COMMAND ----------

# DBTITLE 1,HTML UDF
# Helper to format dates in ISO format (YYYY-MM-DD)
def format_date_dd_mm_yyyy(date_value):
    """
    Formats a date into dd/MM/yyyy format.
    Args:
        date_value: A datetime object or string in 'yyyy-MM-dd' format.
    Returns:
        A string formatted as 'dd/MM/yyyy'.
    """
    try:
        if date_value is None:
            return ''
        if isinstance(date_value, str):
            date_value = datetime.strptime(date_value, "%Y-%m-%d")
        return date_value.strftime("%d/%m/%Y")
    except Exception as e:
        raise ValueError(f"Invalid date input: {date_value}. Error: {e}")

# Load template
html_template_list = spark.read.text(f"{html_mnt}/TD/TD-Details-no-js-v1.html").collect()
html_template = "".join([row.value for row in html_template_list])

# Modify the UDF to accept a row object
def generate_html(row, html_template=html_template):
    try:
        # Load template
        # html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/TD-Details-no-js-v1.html"
        # with open(html_template_path, "r") as f:
        #     html_template = "".join([l for l in f])

        # Replace placeholders in the template with row data
        replacements = {
            "{{Archivedate}}":  format_date_dd_mm_yyyy(row['AdtclmnFirstCreatedDatetime']),
            "{{CaseNo}}": str(row['CaseNo'] or ''),
            "{{Forenames}}": str(row['Forenames'] or ''),
            "{{Name}}": str(row['Name'] or ''),
            "{{BirthDate}}": format_date_dd_mm_yyyy(row['BirthDate']),
            "{{DestructionDate}}": format_date_dd_mm_yyyy(row['DestructionDate']),
            "{{HORef}}": str(row['HORef'] or ''),
            "{{PortReference}}": str(row['PortReference'] or ''),
            "{{HearingCentreDescription}}": str(row['HearingCentreDescription'] or ''),
            "{{DepartmentDescription}}": str(row['DepartmentDescription'] or ''),
            "{{Note}}": str(row['Note'] or '')
        }

        for key, value in replacements.items():
            html_template = html_template.replace(key, value)
        
        return html_template
    except Exception as e:
        # return f"Error generating HTML for tribunal_decision_{row['CaseNo'].replace('/', '_')}_{row['Forenames']}_{row['Name']}.html: {e}"
        return f"Failure Error: {e}"

# Register UDF
generate_html_udf = udf(generate_html, StringType())

# Upload HTML to Azure Blob Storage
def upload_to_blob(file_name, file_content):
    try:
        # blob_client = container_client.get_blob_client(f"{gold_outputs}/HTML/{file_name}")
        blob_client = container_client.get_blob_client(f"{file_name}")
        blob_client.upload_blob(file_content, overwrite=True)
        return "success"
    except Exception as e:
        return f"Failure Error: {e}"

# Register the upload function as a UDF
upload_udf = udf(upload_to_blob)



# COMMAND ----------

# DBTITLE 1,A360  UDF
def generate_a360(row):
    try:
        metadata_data = {
            "operation": "create_record",
            "relation_id": row.client_identifier,
            "record_metadata": {
                "publisher": row.publisher,
                "record_class": row.record_class ,
                "region": row.region,
                "recordDate": str(row.recordDate),
                "event_date": str(row.event_date),
                "client_identifier": row.client_identifier,
                "bf_001": row.bf_001 or "",
                "bf_002": row.bf_002 or "",
                "bf_003": str(row.bf_003) or "",
                "bf_004": str(row.bf_004) or "",
                "bf_005": row.bf_005 or "",
                "bf_006": row.bf_006 or "",
                "bf_007": row.bf_007 or "",
                "bf_008": row.bf_008 or "",
                "bf_010": row.bf_010 or ""
            }
        }

        html_data = {
            "operation": "upload_new_file",
            "relation_id": row.client_identifier,
            "file_metadata": {
                "publisher": row.publisher,
                "dz_file_name": f"tribunal_decision_{row.client_identifier.replace('/', '_')}.html",
                "file_tag": "html"
            }
        }

        json_data = {
            "operation": "upload_new_file",
            "relation_id": row.client_identifier,
            "file_metadata": {
                "publisher": row.publisher,
                "dz_file_name": f"tribunal_decision_{row.client_identifier.replace('/', '_')}.json",
                "file_tag": "json"
            }
        }

        # Convert dictionaries to JSON strings
        metadata_data_str = json.dumps(metadata_data, separators=(',', ':'))
        html_data_str = json.dumps(html_data, separators=(',', ':'))
        json_data_str = json.dumps(json_data, separators=(',', ':'))

        # Combine the data
        all_data_str = f"{metadata_data_str}\n{html_data_str}\n{json_data_str}"

        return all_data_str
    except Exception as e:
        return f"Failure Error: generating A360 for client_identifier {row.client_identifier}: {e}"

# Register UDF
generate_a360_udf = udf(generate_a360, StringType())

# COMMAND ----------

# DBTITLE 1,Transformation: stg_create_td_iris_json_content
@dlt.table(
    name="stg_create_td_iris_json_content",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{silver_mnt}/stg_create_td_iris_json_content"
)
def stg_create_td_iris_json_content():

    df_tribunaldecision_detail = dlt.read("silver_tribunaldecision_detail")
    # df_td_metadata = dlt.read("silver_archive_metadata")

     # Optional: Load from Hive if not an initial load
    if read_hive:
        df_tribunaldecision_detail = spark.read.table(f"hive_metastore.{hive_schema}.silver_tribunaldecision_detail")

    
    # Repartition to optimize parallelism
    repartitioned_df = df_tribunaldecision_detail.repartition(64)

    
    # Apply the UDF to the combined DataFrame
    df_with_json = repartitioned_df.withColumn("JSON_Content", to_json(struct(*df_tribunaldecision_detail.columns))) \
                                            .withColumn("File_Name", concat(lit(f"{gold_outputs}/JSON/tribunal_decision_"), regexp_replace(col("CaseNo"), "/", "_"), lit(".json")))


    df_with_json = df_with_json.withColumn("Status", when((col("JSON_Content").like("Failure%") | col("JSON_Content").isNull()), "Failure on Create JSON Content").otherwise("Successful creating JSON Content"))

    return df_with_json

# COMMAND ----------

# DBTITLE 1,Transformation: stg_create_td_iris_html_content
@dlt.table(
    name="stg_create_td_iris_html_content",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{silver_mnt}/stg_create_td_iris_html_content"
)
def stg_create_td_iris_html_content():

    df_tribunaldecision_detail = dlt.read("silver_tribunaldecision_detail")
    # df_with_json_content = dlt.read("stg_create_td_iris_json_content")

    

     # Optional: Load from Hive if not an initial load
    if read_hive:
        df_tribunaldecision_detail = spark.read.table(f"hive_metastore.{hive_schema}.silver_tribunaldecision_detail")
        # df_with_json_content = spark.read.table(f"hive_metastore.{hive_schema}.stg_create_td_iris_json_content")

    
    # Repartition to optimize parallelism
    repartitioned_df = df_tribunaldecision_detail.repartition(64)

    
    df = repartitioned_df.withColumn("HTML_Content", generate_html_udf(struct(*df_tribunaldecision_detail.columns))) \
                                           .withColumn("File_Name", concat(lit(f"{gold_outputs}/HTML/tribunal_decision_"), regexp_replace(col("CaseNo"), "/", "_"), lit(".html"))) \



    df_with_html = df.withColumn("Status", when((col("HTML_Content").like("Failure%") | col("HTML_Content").isNull()), "Failure on Create HTML Content").otherwise("Successful creating HTML Content"))

    return df_with_html

# COMMAND ----------

# DBTITLE 1,Transformation: stg_create_td_iris_a360_content
@dlt.table(
    name="stg_create_td_iris_a360_content",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{silver_mnt}/stg_create_td_iris_a360_content"
)
def stg_create_td_iris_a360_content():

    # df_tribunaldecision_detail = dlt.read("silver_tribunaldecision_detail")
    df_td_metadata = dlt.read("silver_archive_metadata")
    # df_with_json_html = dlt.read("stg_create_td_iris_html_content")
   
    # Optional: Load from Hive if not an initial load
    if read_hive:
        df_td_metadata = spark.read.table(f"hive_metastore.{hive_schema}.silver_archive_metadata")
        # df_with_json_html = spark.read.table(f"hive_metastore.{hive_schema}.stg_create_td_iris_html_content")

    
    repartitioned_df = df_td_metadata.repartition(64)

    
    # Generate A360 content and associated file names
    df = repartitioned_df.withColumn(
        "A360_Content", generate_a360_udf(struct(*df_td_metadata.columns))
    )

    metadata_df = df.withColumn("Status",when(col("A360_Content").like("Failure%"), "Failure on Creating A360 Content").otherwise("Successful creating A360 Content"))

    return metadata_df

# COMMAND ----------

# DBTITLE 1,Transformation: stg_td_iris_unified
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import dlt

# Define the Delta Live Table
@dlt.table(
    name="stg_td_iris_unified",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{silver_mnt}/stg_td_iris_unified"
)
@dlt.expect_or_drop("No errors in HTML content", "NOT (lower(HTML_Content) LIKE 'failure%')")
@dlt.expect_or_drop("No errors in JSON content", "NOT (lower(JSON_Content) LIKE 'failure%')")
@dlt.expect_or_drop("No errors in A360 content", "NOT (lower(A360_Content) LIKE 'failure%')")
def stg_td_iris_unified():
    
    # Read DLT sources
    a360_df = dlt.read("stg_create_td_iris_a360_content").alias("a360")
    html_df = dlt.read("stg_create_td_iris_html_content").withColumn("HTML_File_Name",col("File_Name")).withColumn("HTML_Status",col("Status")).drop("File_Name","Status").alias("html")
    json_df = dlt.read("stg_create_td_iris_json_content").alias("json")



    # Perform joins
    df_unified = (
        html_df
        .join(json_df,  ((col("html.CaseNo") == col("json.CaseNo")) & (col("html.Forenames") == col("json.Forenames")) & (col("html.Name") == col("json.Name"))), "inner")
        .join(
            a360_df,
            (col("json.CaseNo") == col("a360.client_identifier")) &
            (col("json.Forenames") == col("a360.bf_002")) &
            (col("json.Name") == col("a360.bf_003")),
            "inner"
        )
        .select(
            col("a360.client_identifier"),
            col("a360.bf_002"),
            col("a360.bf_003"),
            col("html.*"),
            col("json.JSON_Content"),
            col("json.File_Name").alias("JSON_File_Name"),
            col("json.Status").alias("JSON_Status"),
            col("a360.A360_Content"),
            col("a360.Status").alias("Status")
        )
        .filter(
            (~col("html.HTML_Content").like("Failure%")) &
            (~col("a360.A360_Content").like("Failure%")) &
            (~col("json.JSON_Content").like("Failure%"))
        )
    )

    # Define a window specification for batching  
    window_spec = Window.orderBy(F.col("client_identifier"), F.col("bf_003"), F.col("bf_003"))
    
    df_batch = df_unified.withColumn("row_num", F.row_number().over(window_spec)) \
                         .withColumn("A360_BatchId", F.floor((F.col("row_num") - 1) / 250) + 1) \
                         .withColumn(
                             "File_Name", 
                             F.concat(F.lit(f"{gold_outputs}/A360/tribunal_decision_"), 
                                      F.col("A360_BatchId"), 
                                      F.lit(".a360"))
                         )

    return df_batch.drop("row_num")


# COMMAND ----------

num_cores = spark.sparkContext.defaultParallelism  # Get available cores
optimal_partitions = 32 * 2  # 2x cores for parallelism
# repartitioned_df = df_combined.repartition(optimal_partitions)
optimal_partitions 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Outputs and Tracking DLT Table Creation

# COMMAND ----------

# DBTITLE 1,Transformation gold_td_iris_with_html
checks = {}
checks["html_content_no_error"] = "(HTML_Content NOT LIKE 'Error%')"
checks["html_content_no_error"] = "(HTML_Content IS NOT NULL)"
checks["UploadStatus_no_error"] = "(Status NOT LIKE 'Error%')"

@dlt.table(
    name="gold_td_iris_with_html",
    comment="Delta Live Gold Table with HTML content.",
    path=f"{gold_mnt}/Data/gold_td_iris_with_html"
)
@dlt.expect_all_or_fail(checks)
def gold_td_iris_with_html():
    # Load source data
    df_combined = dlt.read("stg_td_iris_unified")

    # Optional: Load from Hive if not an initial load
    if read_hive:
        df_combined = spark.read.table(f"hive_metastore.{hive_schema}.stg_td_iris_unified")

    # Repartition to optimize parallelism
    repartitioned_df = df_combined.repartition(optimal_partitions)

    # # Upload HTML files to Azure Blob Storage
    # df_combined.select("CaseNo","Forenames","Name", "HTMLContent","HTMLFileName").repartition(64).foreachPartition(upload_html_partition)

    df_with_upload_status = repartitioned_df.withColumn(
        "Status", upload_udf(col("HTML_File_Name"), col("HTML_Content"))
    )
    

    # Return the DataFrame for DLT table creation
    return df_with_upload_status.select("CaseNo","Forenames","Name", "A360_BatchId","HTML_Content",col("HTML_File_Name").alias("File_Name"),"Status")

# COMMAND ----------

# DBTITLE 1,Transformation gold_td_iris_with_json
checks = {}
checks["json_content_no_error"] = "(JSON_Content IS NOT NULL)"
checks["UploadStatus_no_error"] = "(Status NOT LIKE 'Error%')"



@dlt.table(
    name="gold_td_iris_with_json",
    comment="Delta Live Gold Table with JSON content.",
    path=f"{gold_mnt}/Data/gold_td_iris_with_json"
)
@dlt.expect_all_or_fail(checks)
def gold_td_iris_with_json():
    """
    Delta Live Table for creating and uploading JSON content for judicial officers.
    """
    # Load source data
    df_combined = dlt.read("stg_td_iris_unified")

    # Optionally load data from Hive if needed
    if read_hive:
        df_combined = spark.read.table(f"hive_metastore.{hive_schema}.stg_td_iris_unified")

    # Repartition to optimize parallelism
    repartitioned_df = df_combined.repartition(optimal_partitions)

    df_with_upload_status = repartitioned_df.withColumn(
        "Status", upload_udf(col("JSON_File_Name"), col("JSON_Content"))
    )

    # Return the DataFrame for DLT table creation
    return df_with_upload_status.select("CaseNo","Forenames","Name","A360_BatchId","JSON_Content",col("JSON_File_Name").alias("File_Name"),"Status")


# COMMAND ----------

# DBTITLE 1,Transformation gold_td_iris_with_a360
checks = {}
checks["A360Content_no_error"] = "(consolidate_A360Content NOT LIKE 'Error%')"
checks["A360_content_no_error"] = "(consolidate_A360Content IS NOT NULL)"
checks["UploadStatus_no_error"] = "(Status NOT LIKE 'Error%')"

@dlt.table(
    name="gold_td_iris_with_a360",
    comment="Delta Live Gold Table with A360 content.",
    path=f"{gold_mnt}/Data/gold_td_iris_with_a360"
)
@dlt.expect_all_or_fail(checks)
def gold_td_iris_with_a360():
    
    # df_joh_metadata = dlt.read("stg_td_iris_unified")
    # df_a360 = dlt.read("stg_td_iris_unified")

    df_a360 = dlt.read("stg_td_iris_unified")

    # Optionally load data from Hive
    if read_hive:
        df_a360 = spark.read.table(f"hive_metastore.{hive_schema}.stg_td_iris_unified")

    # Group by 'A360FileName' with Batching and consolidate the 'sets' texts, separated by newline
    df_agg = df_a360.groupBy("File_Name", "A360_BatchId") \
            .agg(concat_ws("\n", collect_list("A360_Content")).alias("consolidate_A360Content")) \
            .select(col("File_Name"), col("consolidate_A360Content"), col("A360_BatchId"))

    # Repartition the DataFrame to optimize parallelism
    repartitioned_df = df_agg.repartition(optimal_partitions)

    # Remove existing files
    # dbutils.fs.rm(f"{gold_outputs}/A360", True)

    # Generate A360 content
    df_with_a360 = repartitioned_df.withColumn(
        "Status", upload_udf(col("File_Name"), col("consolidate_A360Content"))
    )

    # df_with_a360_review = df_with_a360.withColumn("A360BatchId_str", col("A360_BatchId").cast("string"))
   
    return df_with_a360.select("A360_BatchId", "consolidate_A360Content", "File_Name", "Status")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Configuration

# COMMAND ----------

# dbutils.notebook.run("./ARIADM_ARM_TD_AUDIT_DataProcessing", timeout_seconds=60)

# COMMAND ----------

# DBTITLE 1,Generate Audit DataFrame with Unique Identifiers
# # # def log_audit_entry(df,unique_identifier):
# import uuid

# def datetime_uuid():
#     dt_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
#     return str(uuid.uuid5(uuid.NAMESPACE_DNS,dt_str))

# run_id_value = datetime_uuid()

# audit_schema = StructType([
#     StructField("Runid", StringType(), True),
#     StructField("Unique_identifier_desc", StringType(), True),
#     StructField("Unique_identifier", StringType(), True),
#     StructField("Table_name", StringType(), True),
#     StructField("Stage_name", StringType(), True),
#     StructField("Record_count", IntegerType(), True),
#     StructField("Run_dt",TimestampType(), True),
#     StructField("Batch_id", StringType(), True),
#     StructField("Description", StringType(), True),
#     StructField("File_name", StringType(), True),
#     StructField("Status", StringType(), True)
# ])

# def create_audit_df(df: DataFrame, unique_identifier_desc: str, table_name: str, stage_name: str, description: str, additional_columns: list = None) -> DataFrame:
#     """
#     Creates an audit DataFrame and writes it to Delta format.

#     :param df: Input DataFrame from which unique identifiers are extracted.
#     :param unique_identifier_desc: Column name that acts as a unique identifier.
#     :param table_name: Name of the source table.
#     :param stage_name: Name of the data processing stage.
#     :param description: Description of the table.
#     :param additional_columns: List of additional columns to include in the audit DataFrame.
#     :return: DataFrame containing the audit information.
#     """

#     dt_desc = datetime.utcnow()

#     additional_columns = additional_columns or []  # Default to an empty list if None   
#     additional_columns = [col(c) for c in additional_columns if c is not None]  # Filter out None values

#     audit_df = df.select(col(unique_identifier_desc).alias("unique_identifier"), *additional_columns) \
#         .withColumn("Runid", lit(run_id_value)) \
#         .withColumn("Unique_identifier_desc", lit(unique_identifier_desc)) \
#         .withColumn("Stage_name", lit(stage_name)) \
#         .withColumn("Table_name", lit(table_name)) \
#         .withColumn("Run_dt", lit(dt_desc).cast(TimestampType())) \
#         .withColumn("Description", lit(description))

#     list_cols = audit_df.columns

#     final_audit_df = audit_df.groupBy(*list_cols).agg(count("*").cast(IntegerType()).alias("Record_count"))

#     # final_audit_df.write.format("delta").mode("append").option("mergeSchema", "true").save(audit_delta_path)
    
#     return final_audit_df


# COMMAND ----------

# %sql
# DESCRIBE HISTORY hive_metastore.ariadm_arm_td.td_cr_audit_table

# COMMAND ----------

# from delta.tables import DeltaTable

# # Load Delta Table
# delta_table = DeltaTable.forName(spark, "hive_metastore.ariadm_arm_td.td_cr_audit_table")

# # Get history as DataFrame
# history_df = delta_table.history()

# # Collect versions
# versions = [row['version'] for row in history_df.select("version").collect()]

# # Loop through and get counts
# for v in versions:
#     df = spark.read.format("delta").option("versionAsOf", v).table("hive_metastore.ariadm_arm_td.td_cr_audit_table")
#     print(f"Version {v} has {df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Transformation: td_cr_audit_table
# @dlt.table(
#     name='td_cr_audit_table',
#     comment="Delta Live unified stage Gold Table for gold outputs.",
#     path=f"{audit_mnt}/td_cr_audit_table"
# )
# def td_cr_audit_table():
#     audit_params = [
#          {
#             "unique_identifier_cols": ["CaseNo", "Forenames", "Name"],
#             "table_name": "bronze_ac_ca_ant_fl_dt_hc",
#             "stage_name": "bronze_stage",
#             "description": "The bronze_ac_ca_ant_fl_dt_hc table Delta Live Table combining Appeal Case data with Case Appellant, Appellant, File Location, Department, and Hearing Centre."
#         },
#         {
#             "unique_identifier_cols": ["CaseNo", "Forenames", "Name"],
#             "table_name": "bronze_iris_extract",
#             "stage_name": "bronze_stage",
#             "description": "Delta Live Table extracted from the IRIS Tribunal decision file extract."
#         },
#         {
#             "unique_identifier_cols": ["CaseNo"],
#             "table_name": "stg_td_filtered",
#             "stage_name": "silver_stage",
#             "description": "The stg_td_filtered - segmentation Table for appeal cases requiring tribunal decisions with unique list of CaseNo's"
#         },
#         {
#             "unique_identifier_cols": ["CaseNo", "Forenames", "Name"],
#             "table_name": "silver_tribunaldecision_detail",
#             "stage_name": "silver_stage",
#             "description": "The silver_tribunaldecision_detail - or Tribunal Decision information"
#         },
#         {
#             "unique_identifier_cols": ["client_identifier", "bf_002", "bf_003"],
#             "table_name": "silver_archive_metadata",
#             "stage_name": "silver_stage",
#             "description": "The silver_archive_metadata table consolidates keys metadata for Archive Metadata da"
#         },
#         {
#             "unique_identifier_cols": ["CaseNo", "Forenames", "Name"],
#             "table_name": "stg_create_td_iris_json_content",
#             "stage_name": "silver_stage",
#             "description": "The stg_create_td_iris_json_content table generates JSON content for TD cases",
#             "Extra_columns_mapping": {"File_name": "JSONFileName", "Status": "JSONStatus"}
#         },
#         {
#             "unique_identifier_cols": ["CaseNo", "Forenames", "Name"],
#             "table_name": "stg_create_td_iris_html_content",
#             "stage_name": "silver_stage",
#             "description": "The stg_create_td_iris_html_content table generates HTML content for TD cases",
#             "Extra_columns_mapping": {"File_name": "HTMLFileName", "Status": "HTMLStatus"}
#         },
#         {
#             "unique_identifier_cols": ["client_identifier", "bf_002", "bf_003"],
#             "table_name": "stg_create_td_iris_a360_content",
#             "stage_name": "silver_stage",
#             "description": "The stg_create_td_iris_a360_content table generates A360 content for TD cases",
#             "Extra_columns_mapping": {"File_name": "NotYetBatched", "Status": "A360Status"}
#         },
#         {
#             "unique_identifier_cols": ["CaseNo", "Forenames", "Name"],
#             "table_name": "stg_td_iris_unified",
#             "stage_name": "silver_stage",
#             "description": "The stg_td_iris_unified table generates A360 BatchId for TD cases",
#             "Extra_columns_mapping": {"File_name": "A360FileName", "Status": "A360Status"}
#         },
#         {
#             "unique_identifier_cols": ["CaseNo", "Forenames", "Name"],
#             "table_name": "gold_td_iris_with_html",
#             "stage_name": "silver_stage",
#             "description": "The gold_td_iris_with_html with HTML Outputs Uploded..",
#             "Extra_columns_mapping": {"File_name": "HTMLFileName", "Status": "UploadStatus"}
#         },
#         {
#             "unique_identifier_cols": ["CaseNo", "Forenames", "Name"],
#             "table_name": "gold_td_iris_with_json",
#             "stage_name": "silver_stage",
#             "description": "The gold_td_iris_with_json with HTML Outputs Uploded..",
#             "Extra_columns_mapping": {"File_name": "JSONFileName", "Status": "UploadStatus"}
#         },
#         {
#             "unique_identifier_cols": ["A360BatchId"],
#             "table_name": "gold_td_iris_with_a360",
#             "stage_name": "silver_stage",
#             "description": "The gold_td_iris_with_a360 with HTML Outputs Uploded..",
#             "Extra_columns_mapping": {"File_name": "A360FileName", "Status": "UploadStatus"}
#         },
#         {
#             "unique_identifier_cols": ["A360BatchId"],
#             "table_name": "temp",
#             "stage_name": "silver_stage",
#             "description": "The gold_td_iris_with_a360 with HTML Outputs Uploded..",
#             "Extra_columns_mapping": {"File_name": "A360FileName", "Status": "UploadStatus"}
#         }
#     ]

#     audit_dataframes = []

#     for params in audit_params:
#         table_name = params["table_name"]
#         stage_name = params["stage_name"]
#         unique_identifier_cols = params["unique_identifier_cols"]
#         description = params["description"]
#         extra_columns_mapping = params.get("Extra_columns_mapping", {})
#         unique_identifier_desc = "_".join(unique_identifier_cols)

#         try:

#             df_logging = dlt.read(table_name)

#             df_audit = df_logging
#             if len(unique_identifier_cols) > 1:
#                 df_audit = df_audit.withColumn(
#                     unique_identifier_desc, 
#                     concat_ws("_", *[col(c).cast("string") for c in unique_identifier_cols])
#                 )
#             else:
#                 df_audit = df_audit.withColumn(unique_identifier_desc, col(unique_identifier_desc))

#             # Apply extra column mappings dynamically
#             for new_col, source_col in extra_columns_mapping.items():
#                 if source_col == "NotYetBatched":
#                     df_audit = df_audit.withColumn(new_col, lit("NotYetBatched"))
#                 else:
#                     df_audit = df_audit.withColumn(new_col, col(source_col))

#             # Generate the audit DataFrame
#             df_audit_appended = create_audit_df(
#                 df_audit,
#                 unique_identifier_desc=unique_identifier_desc,
#                 table_name=table_name,
#                 stage_name=stage_name,
#                 description=description
#             )

#             audit_dataframes.append(df_audit_appended)

#         except Exception as e:

#             # Table does not exist, create an audit entry for it
#             status = f"Failed - Table {table_name} does not exist"

#             row_data = {
#                 "Runid": run_id_value,
#                 "Unique_identifier_desc": unique_identifier_desc,
#                 "Unique_identifier": None,
#                 "Table_name": table_name,
#                 "Stage_name": stage_name,
#                 "Record_count": 0,
#                 "Run_dt": datetime.now(),
#                 "Batch_id": None,
#                 "Description": description,
#                 "File_name": None,
#                 "Status": status
#             }

#             row_df = spark.createDataFrame([row_data], schema=audit_schema)
#             audit_dataframes.append(row_df)

#     df_final_audit = audit_dataframes[0]
#     for df in audit_dataframes[1:]:
#         df_final_audit = df_final_audit.unionByName(df, allowMissingColumns=True)

#     return df_final_audit

# COMMAND ----------

# Runid = run_id_value
# unique_identifier_cols = ["CaseNo", "Forenames", "Name"]
# unique_identifier_desc = "_".join(unique_identifier_cols)
# Unique_identifier = None
# Table_name = "stg"
# Stage_name = "silver_stage"
# Run_dt = datetime.now()
# Description = "The stg_create_td_iris_json_content table generates JSON content for TD cases"
# Status = "Failed"

# # Create an empty DataFrame
# empty_df = spark.createDataFrame([], audit_schema)

# # Create a row
# row_data = {
#     "Runid": Runid,
#     "Unique_identifier_desc": unique_identifier_desc,
#     "Unique_identifier": Unique_identifier,
#     "Table_name": Table_name,
#     "Stage_name": Stage_name,
#     "Record_count": 0,
#     "Run_dt": Run_dt,
#     "Batch_id": None,
#     "Description": Description,
#     "File_name": None,
#     "Status": Status
# }

# row_df = spark.createDataFrame([row_data], schema=audit_schema)

# # Append the row to the empty DataFrame
# final_df = empty_df.unionByName(row_df, allowMissingColumns=True)

# display(final_df)

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Success Message
dbutils.notebook.exit("Notebook completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Appendix

# COMMAND ----------

# DBTITLE 1,Count of Files in HTML, JSON, and A360 Directories
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/HTML").count())
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/JSON").count())
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/A360").count())

# COMMAND ----------

# dbutils.secrets.listScopes()

# COMMAND ----------

# DBTITLE 1,HTML Failed Upload Status
# %sql
# select * from hive_metastore.ariadm_arm_td.gold_td_iris_with_html where UploadStatus != 'success' and HTMLContent like '%ERROR%'

# COMMAND ----------

# DBTITLE 1,JSON Failed Upload Status
# %sql
# select * from hive_metastore.ariadm_arm_td.gold_td_iris_with_json where UploadStatus != 'success'  and JSONCollection like '%ERROR%'

# COMMAND ----------

# DBTITLE 1,A360 Failed Upload Status
# %sql
# select * from hive_metastore.ariadm_arm_td.gold_td_iris_with_a360 --where UploadStatus != 'success'  and A360Content like '%ERROR%'

# COMMAND ----------

# %sql
# drop schema hive_metastore.ariadm_arm_td cascade

# COMMAND ----------

# %sql
# dbutils.fs.ls("/mnt/")

# COMMAND ----------

# df = spark.read.format("delta").load("abfss://gold@ingest00curatedsbox.dfs.core.windows.net/ARIADM/ARM/ARIATD/gold_td_iris_with_a360/")
# display(df)

# COMMAND ----------

# %sql
# select count(*) from hive_metastore.ariadm_arm_td.gold_td_iris_with_json

# COMMAND ----------

# %sql
# select count(*) from hive_metastore.ariadm_arm_td.gold_td_iris_with_html

# COMMAND ----------

# %sql
# select count(*), A360BatchId from hive_metastore.ariadm_arm_td.gold_td_iris_with_html group by A360BatchId

# COMMAND ----------

# display(spark.read.format("delta").load("/mnt/ingest00curatedsboxsilver/ARIADM/ARM/AUDIT/TD/td_cr_audit_table").filter("Table_name LIKE '%bronze%'").groupBy("Table_name").agg({"Run_dt": "max", "*": "count"}))

# COMMAND ----------

# display(spark.read.format("delta").load("/mnt/ingest00curatedsboxsilver/ARIADM/ARM/AUDIT/TD/td_cr_audit_table").filter("Table_name LIKE '%silver%'").groupBy("Table_name").agg({"Run_dt": "max", "*": "count"}))

# COMMAND ----------

# display(spark.read.format("delta").load("/mnt/ingest00curatedsboxsilver/ARIADM/ARM/AUDIT/TD/td_cr_audit_table").filter("Table_name LIKE '%gold%'").groupBy("Table_name").agg({"Run_dt": "max", "*": "count"}))

# COMMAND ----------

# display(spark.read.format("delta").load("/mnt/ingest00curatedsboxsilver/ARIADM/ARM/AUDIT/TD/td_cr_audit_table").filter("Table_name LIKE '%stg%'").groupBy("Table_name").agg({"Run_dt": "max", "*": "count"}))

# COMMAND ----------

# display(spark.read.format("delta").load("/mnt/ingest00curatedsboxsilver/ARIADM/ARM/AUDIT/JOH/joh_cr_audit_table").filter("Table_name LIKE '%bronze%'").groupBy("Table_name").agg({"Run_dt": "max", "*": "count"}))

# COMMAND ----------

# %sql
# drop schema hive_metastore.ariadm_arm_td cascade

# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_td.gold_td_iris_with_html
# where CaseNo = 'VA/00003/2009'


# COMMAND ----------

# %sql
# select CaseNo,count(*) from hive_metastore.ariadm_arm_td.gold_td_iris_with_html
# group by CaseNo
# having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.ariadm_arm_td.gold_td_iris_with_a360
