# Databricks notebook source
# MAGIC %md
# MAGIC # Judicial Office Holder Archive
# MAGIC <table style = 'float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Name: </b></td>
# MAGIC          <td>ARIADM_ARM_JOH</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Description: </b></td>
# MAGIC          <td>Notebook to generate a set of HTML, JSON, and A360 files, each representing the data about judges stored in ARIA.</td>
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
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-95">ARIADM-95</a>/NSA/SEP-2024</td>
# MAGIC          <td>JOH :Compete Landing to Bronze Notebook</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-97">ARIADM-97</a>/NSA/SEP-2024</td>
# MAGIC          <td>JOH: Create Silver and gold HTML outputs: </td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-123">ARIADM-123</a>/NSA/SEP-2024</td>
# MAGIC          <td>JOH: Create Gold outputs- Json and A360</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC     <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-130">ARIADM-130</a>/NSA/-07-OCT-2024</td>
# MAGIC     <td>JOH: Tune Performance, Refactor Code for Reusability, Manage Broadcast Effectively, Implement Repartitioning Strategy</td>
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
# MAGIC     <td style='text-align: left; '><a href=https://tools.hmcts.net/jira/browse/ARIADM-472">ARIADM-472</a>/NSA/07-MAR-2025</td>
# MAGIC     <td>Implement A360 batching for JOH</td>
# MAGIC </tr>
# MAGIC
# MAGIC     
# MAGIC    </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import packages

# COMMAND ----------

import dlt
import json
from pyspark.sql.functions import when, col,coalesce, current_timestamp, lit, date_format
from pyspark.sql.functions import *
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pyspark.sql.window import Window

# COMMAND ----------

# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# spark.conf.set("pipelines.tableManagedByMultiplePipelinesCheck.enabled", "false")

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
# read_hive = False

# Setting variables for use in subsequent cells
raw_mnt = f"abfss://raw@ingest{lz_key}raw{env_name}.dfs.core.windows.net/ARIADM/CCD/APPEALS"
landing_mnt = f"abfss://landing@ingest{lz_key}landing{env_name}.dfs.core.windows.net/SQLServer/Sales/IRIS/dbo/"
bronze_mnt = f"abfss://bronze@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/CCD/APPEALS"
silver_mnt = f"abfss://silver@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/CCD/APPEALS"
gold_mnt = f"abfss://gold@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/CCD/APPEALS"
gold_outputs = "ARIADM/CCD/APPEALS"
hive_schema = "ariadm_ccd_apl"
# key_vault = "ingest00-keyvault-sbox"

html_mnt = f"abfss://html-template@ingest{lz_key}landing{env_name}.dfs.core.windows.net/"

# Print all variables
variables = {
    # "read_hive": read_hive,
    "raw_mnt": raw_mnt,
    "landing_mnt": landing_mnt,
    "bronze_mnt": bronze_mnt,
    "silver_mnt": silver_mnt,
    "gold_mnt": gold_mnt,
    "html_mnt": html_mnt,
    "gold_outputs": gold_outputs,
    "hive_schema": hive_schema,
    "key_vault": KeyVault_name
}

display(variables)

# COMMAND ----------

# DBTITLE 1,Determine and Print Environment
# try:
#     env_value = dbutils.secrets.get(KeyVault_name, "Environment")
#     env = "dev" if env_value == "development" else None
#     print(f"Environment: {env}")
# except:
#     env = "unkown"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions to Read Latest Landing Files

# COMMAND ----------


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
def read_latest_parquet(folder_name: str, view_name: str, process_name: str, base_path: str = f"{landing_mnt}/") -> "DataFrame":
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

# MAGIC %md
# MAGIC ## Audit Function

# COMMAND ----------

# from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
# from delta.tables import DeltaTable

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

# # Define Delta Table Path in Azure Storage
# audit_delta_path = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/AUDIT/JOH/joh_cr_audit_table"


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

# MAGIC %md
# MAGIC ## Raw DLT Tables Creation

# COMMAND ----------


@dlt.table(
    name="raw_appealcase",
    comment="Delta Live Table ARIA AppealCase.",
    path=f"{raw_mnt}/Raw_AppealCase"
)
def Raw_AppealCase():
    return read_latest_parquet("AppealCase", "tv_AppealCase", "ARIA_ARM_APPEALS")

@dlt.table(
    name="raw_caserep",
    comment="Delta Live Table ARIA CaseRep.",
    path=f"{raw_mnt}/Raw_CaseRep"
)
def raw_CaseRep():
     return read_latest_parquet("CaseRep", "tv_CaseRep", "ARIA_ARM_APPEALS") 
 

@dlt.table(
    name="raw_representative",
    comment="Delta Live Table ARIA Representative.",
    path=f"{raw_mnt}/Raw_Representative"
)
def raw_Representative():
     return read_latest_parquet("Representative", "tv_Representative", "ARIA_ARM_APPEALS")

@dlt.table(
    name="raw_casesponsor",
    comment="Delta Live Table ARIA CaseSponsor.",
    path=f"{raw_mnt}/Raw_CaseSponsor"
)
def raw_CaseSponsor():
     return read_latest_parquet("CaseSponsor", "tv_CaseSponsor", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_caserespondent",
    comment="Delta Live Table ARIA CaseRespondent.",
    path=f"{raw_mnt}/Raw_CaseRespondent"
)
def CaseRespondent():
    return read_latest_parquet("CaseRespondent", "tv_CaseRespondent", "ARIA_ARM_APPEALS")

@dlt.table(
    name="raw_filelocation",
    comment="Delta Live Table ARIA FileLocation.",
    path=f"{raw_mnt}/Raw_FileLocation"
)
def raw_FileLocation():
     return read_latest_parquet("FileLocation", "tv_FileLocation", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_casefeesummary",
    comment="Delta Live Table ARIA CaseFeeSummary.",
    path=f"{raw_mnt}/Raw_CaseFeeSummary"
)
def raw_CaseFeeSummary():
    return read_latest_parquet("CaseFeeSummary", "tv_CaseFeeSummary", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_caseappellant",
    comment="Delta Live Table ARIA CaseAppellant.",
    path=f"{raw_mnt}/Raw_CaseAppellant"
)
def raw_CaseAppellant():
     return read_latest_parquet("CaseAppellant", "tv_CaseAppellant", "ARIA_ARM_APPEALS")
 
@dlt.table(
    name="raw_appellant",
    comment="Delta Live Table ARIA Appellant.",
    path=f"{raw_mnt}/Raw_Appellant"
)
def raw_Appellant():
     return read_latest_parquet("Appellant", "tv_Appellant", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_transaction",
    comment="Delta Live Table ARIA Transaction.",
    path=f"{raw_mnt}/Raw_Transaction"
)
def raw_Transaction():
     return read_latest_parquet("Transaction", "tv_Transaction", "ARIA_ARM_APPEALS")   

@dlt.table(
    name="raw_transactiontype",
    comment="Delta Live Table ARIA TransactionType.",
    path=f"{raw_mnt}/Raw_TransactionType"
)
def raw_TransactionType():
     return read_latest_parquet("TransactionType", "tv_TransactionType", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_link",
    comment="Delta Live Table ARIA Link.",
    path=f"{raw_mnt}/Raw_Link"
)
def raw_Link():
     return read_latest_parquet("Link", "tv_Link", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_linkdetail",
    comment="Delta Live Table ARIA LinkDetail.",
    path=f"{raw_mnt}/Raw_LinkDetail"
)
def raw_LinkDetail():
     return read_latest_parquet("LinkDetail", "tv_LinkDetail", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_caseadjudicator",
    comment="Delta Live Table ARIA AppealTypeCategory.",
    path=f"{raw_mnt}/raw_caseadjudicator"
)
def raw_caseadjudicator():
     return read_latest_parquet("CaseAdjudicator", "tv_caseadjudicator", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_adjudicator",
    comment="Delta Live Table ARIA Adjudicator.",
    path=f"{raw_mnt}/Raw_Adjudicator"
)
def raw_Adjudicator():
     return read_latest_parquet("Adjudicator", "tv_Adjudicator", "ARIA_ARM_APPEALS")
 
@dlt.table(
    name="raw_appealcategory",
    comment="Delta Live Table ARIA AppealCategory.",
    path=f"{raw_mnt}/Raw_AppealCategory"
)
def raw_AppealCategory():
     return read_latest_parquet("AppealCategory", "tv_AppealCategory", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_documentsreceived",
    comment="Delta Live Table ARIA DocumentsReceived.",
    path=f"{raw_mnt}/Raw_DocumentsReceived"
)
def raw_DocumentsReceived():
     return read_latest_parquet("DocumentsReceived", "tv_DocumentsReceived", "ARIA_ARM_APPEALS")  
 

@dlt.table(
    name="raw_history",
    comment="Delta Live Table ARIA History.",
    path=f"{raw_mnt}/Raw_History"
)
def raw_History():
     return read_latest_parquet("History", "tv_History", "ARIA_ARM_APPEALS")  






# @dlt.table(
#     name="raw_adjudicatorrole",
#     comment="Delta Live Table ARIA AdjudicatorRole.",
#     path=f"{raw_mnt}/Raw_AdjudicatorRole"
# )
# def Raw_AdjudicatorRole():
#     return read_latest_parquet("AdjudicatorRole", "tv_AdjudicatorRole", "ARIA_ARM_JOH_ARA")


# @dlt.table(
#     name="raw_adjudicator",
#     comment="Delta Live Table ARIA Adjudicator.",
#     path=f"{raw_mnt}/Raw_Adjudicator"
# )
# def Raw_Adjudicator():
#     return read_latest_parquet("Adjudicator", "tv_Adjudicator", "ARIA_ARM_JOH_ARA")


# @dlt.table(
#     name="raw_employmentterm",
#     comment="Delta Live Table ARIA EmploymentTerm.",
#     path=f"{raw_mnt}/Raw_EmploymentTerm"
# )
# def Raw_EmploymentTerm():
#      return read_latest_parquet("ARIAEmploymentTerm", "tv_EmploymentTerm", "ARIA_ARM_JOH_ARA")


# @dlt.table(
#     name="raw_donotusereason",
#     comment="Delta Live Table ARIA DoNotUseReason.",
#     path=f"{raw_mnt}/Raw_DoNotUseReason"
# )
# def Raw_DoNotUseReason():
#     return read_latest_parquet("DoNotUseReason", "tv_DoNotUseReason", "ARIA_ARM_JOH_ARA")


# @dlt.table(
#     name="raw_johistory",
#     comment="Delta Live Table ARIA JoHistory.",
#     path=f"{raw_mnt}/Raw_JoHistory"
# )
# def Raw_JoHistory():
#     return read_latest_parquet("JoHistory", "tv_JoHistory", "ARIA_ARM_JOH_ARA")


# @dlt.table(
#     name="raw_othercentre",
#     comment="Delta Live Table ARIA OtherCentre.",
#     path=f"{raw_mnt}/Raw_OtherCentre"
# )
# def Raw_OtherCentre():
#     return read_latest_parquet("OtherCentre", "tv_OtherCentre", "ARIA_ARM_JOH_ARA")


# @dlt.table(
#     name="raw_hearingcentre",
#     comment="Delta Live Table ARIA HearingCentre.",
#     path=f"{raw_mnt}/Raw_HearingCentre"
# )
# def Raw_HearingCentre():
#     return read_latest_parquet("ARIAHearingCentre", "tv_HearingCentre", "ARIA_ARM_JOH_ARA")


# @dlt.table(
#     name="raw_users",
#     comment="Delta Live Table ARIA Users.",
#     path=f"{raw_mnt}/Raw_Users"
# )
# def Raw_Users():
#     return read_latest_parquet("Users", "tv_Users", "ARIA_ARM_JOH_ARA")


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Bronze DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M1 bronze_appealcase_crep_rep_floc_cspon_cfs

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- m1.bronze_appealcase_crep_rep_floc_cspon_cfs
# MAGIC SELECT
# MAGIC -- AppealCase
# MAGIC ac.CaseNo,
# MAGIC ac.CasePrefix,
# MAGIC ac.OutOfTimeIssue,
# MAGIC ac.DateLodged,
# MAGIC ac.DateAppealReceived,
# MAGIC ac.CentreId,
# MAGIC ac.NationalityId,
# MAGIC ac.AppealTypeId,
# MAGIC ac.DeportationDate,
# MAGIC ac.RemovalDate,
# MAGIC ac.VisitVisaType,
# MAGIC ac.DateOfApplicationDecision,
# MAGIC ac.HORef,
# MAGIC ac.InCamera,
# MAGIC ac.CourtPreference,
# MAGIC ac.LanguageId,
# MAGIC ac.Interpreter,
# MAGIC -- CaseRep
# MAGIC crep.RepresentativeId,
# MAGIC crep.Name AS CaseRepName,
# MAGIC crep.Address1 AS CaseRepAddress1,
# MAGIC crep.Address2 AS CaseRepAddress2,
# MAGIC crep.Address3 AS CaseRepAddress3,
# MAGIC crep.Address4 AS CaseRepAddress4,
# MAGIC crep.Address5 AS CaseRepAddress5,
# MAGIC crep.Postcode AS CaseRepPostcode,
# MAGIC crep.Contact,
# MAGIC crep.Email AS CaseRepEmail,
# MAGIC crep.FileSpecificEmail,
# MAGIC -- Representative
# MAGIC rep.Name AS RepName,
# MAGIC rep.Address1 AS RepAddress1,
# MAGIC rep.Address2 AS RepAddress2,
# MAGIC rep.Address3 AS RepAddress3,
# MAGIC rep.Address4 AS RepAddress4,
# MAGIC rep.Address5 AS RepAddress5,
# MAGIC rep.Postcode AS RepPostcode,
# MAGIC rep.Email AS RepEmail,
# MAGIC -- CaseSponsor
# MAGIC cspon.Name AS SponsorName,
# MAGIC cspon.Forenames AS SponsorForenames,
# MAGIC cspon.Address1 AS SponsorAddress1,
# MAGIC cspon.Address2 AS SponsorAddress2,
# MAGIC cspon.Address3 AS SponsorAddress3,
# MAGIC cspon.Address4 AS SponsorAddress4,
# MAGIC cspon.Address5 AS SponsorAddress5,
# MAGIC cspon.Postcode AS SponsorPostcode,
# MAGIC cspon.Email AS SponsorEmail,
# MAGIC cspon.Telephone AS SponsorTelephone,
# MAGIC cspon.Authorised AS SponsorAuthorisation,
# MAGIC -- CaseRespondent
# MAGIC cr.MainRespondentId,
# MAGIC -- FileLocation
# MAGIC floc.DeptId,
# MAGIC -- CaseFeeSummary
# MAGIC cfs.PaymentRemissionRequested,
# MAGIC cfs.PaymentRemissionReason,
# MAGIC cfs.PaymentRemissionGranted,
# MAGIC cfs.PaymentRemissionReasonNote,
# MAGIC cfs.LSCReference,
# MAGIC cfs.ASFReferenceNo,
# MAGIC cfs.DateCorrectFeeReceived
# MAGIC FROM dbo.AppealCase ac
# MAGIC LEFT OUTER JOIN dbo.CaseRep crep ON ac.CaseNo = crep.CaseNo
# MAGIC LEFT OUTER JOIN dbo.Representative rep ON crep.RepresentativeId = rep.RepresentativeId
# MAGIC LEFT OUTER JOIN dbo.CaseSponsor cspon ON cspon.CaseNo = ac.CaseNo
# MAGIC LEFT OUTER JOIN dbo.CaseRespondent cr on ac.CaseNo = cr.CaseNo
# MAGIC LEFT OUTER JOIN dbo.FileLocation floc ON floc.CaseNo = ac.CaseNo
# MAGIC LEFT OUTER JOIN dbo.CaseFeeSummary cfs ON cfs.CaseNo = ac.CaseNo
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col
import dlt

@dlt.table(
    name="bronze_appealcase_crep_rep_floc_cspon_cfs",
    comment="Delta Live Table combining AppealCase with CaseRep, Representative, CaseSponsor, CaseRespondent, FileLocation, and CaseFeeSummary data.",
    path=f"{bronze_mnt}/bronze_appealcase_crep_rep_floc_cspon_cfs"
)
def bronze_appealcase_crep_rep_floc_cspon_cfs():
    df = (
        dlt.read("raw_appealcase").alias("ac")
        .join(dlt.read("raw_caserep").alias("crep"), col("ac.CaseNo") == col("crep.CaseNo"), "left_outer")
        .join(dlt.read("raw_representative").alias("rep"), col("crep.RepresentativeId") == col("rep.RepresentativeId"), "left_outer")
        .join(dlt.read("raw_casesponsor").alias("cspon"), col("ac.CaseNo") == col("cspon.CaseNo"), "left_outer")
        .join(dlt.read("raw_caserespondent").alias("cr"), col("ac.CaseNo") == col("cr.CaseNo"), "left_outer")
        .join(dlt.read("raw_filelocation").alias("floc"), col("ac.CaseNo") == col("floc.CaseNo"), "left_outer")
        .join(dlt.read("raw_casefeesummary").alias("cfs"), col("ac.CaseNo") == col("cfs.CaseNo"), "left_outer")
        .select(
            # AppealCase
            col("ac.CaseNo"),
            col("ac.CasePrefix"),
            col("ac.OutOfTimeIssue"),
            col("ac.DateLodged"),
            col("ac.DateAppealReceived"),
            col("ac.CentreId"),
            col("ac.NationalityId"),
            col("ac.AppealTypeId"),
            col("ac.DeportationDate"),
            col("ac.RemovalDate"),
            col("ac.VisitVisaType"),
            col("ac.DateOfApplicationDecision"),
            col("ac.HORef"),
            col("ac.InCamera"),
            col("ac.CourtPreference"),
            col("ac.LanguageId"),
            col("ac.Interpreter"),

            # CaseRep
            col("crep.RepresentativeId"),
            col("crep.Name").alias("CaseRepName"),
            col("crep.Address1").alias("CaseRepAddress1"),
            col("crep.Address2").alias("CaseRepAddress2"),
            col("crep.Address3").alias("CaseRepAddress3"),
            col("crep.Address4").alias("CaseRepAddress4"),
            col("crep.Address5").alias("CaseRepAddress5"),
            col("crep.Postcode").alias("CaseRepPostcode"),
            col("crep.Contact"),
            col("crep.Email").alias("CaseRepEmail"),
            col("crep.FileSpecificEmail"),

            # Representative
            col("rep.Name").alias("RepName"),
            col("rep.Address1").alias("RepAddress1"),
            col("rep.Address2").alias("RepAddress2"),
            col("rep.Address3").alias("RepAddress3"),
            col("rep.Address4").alias("RepAddress4"),
            col("rep.Address5").alias("RepAddress5"),
            col("rep.Postcode").alias("RepPostcode"),
            col("rep.Email").alias("RepEmail"),

            # CaseSponsor
            col("cspon.Name").alias("SponsorName"),
            col("cspon.Forenames").alias("SponsorForenames"),
            col("cspon.Address1").alias("SponsorAddress1"),
            col("cspon.Address2").alias("SponsorAddress2"),
            col("cspon.Address3").alias("SponsorAddress3"),
            col("cspon.Address4").alias("SponsorAddress4"),
            col("cspon.Address5").alias("SponsorAddress5"),
            col("cspon.Postcode").alias("SponsorPostcode"),
            col("cspon.Email").alias("SponsorEmail"),
            col("cspon.Telephone").alias("SponsorTelephone"),
            col("cspon.Authorised").alias("SponsorAuthorisation"),

            # CaseRespondent
            col("cr.MainRespondentId"),

            # FileLocation
            col("floc.DeptId"),

            # CaseFeeSummary
            col("cfs.PaymentRemissionRequested"),
            col("cfs.PaymentRemissionReason"),
            col("cfs.PaymentRemissionGranted"),
            col("cfs.PaymentRemissionReasonNote"),
            col("cfs.LSCReference"),
            col("cfs.ASFReferenceNo"),
            col("cfs.DateCorrectFeeReceived")
        )
    )

    return df


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Transformation M2 bronze_appealcase_caseappellant_appellant

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- m2.bronze_appealcase_caseappellant_appellant 
# MAGIC -- This query *could* pull back multiple rows per case however for all the cases migrating to CCD there is only ever 1 row per CaseNo. 
# MAGIC
# MAGIC SELECT 
# MAGIC ac.CaseNo, 
# MAGIC -- Appellant 
# MAGIC ap.Name AS AppellantName, 
# MAGIC ap.Forenames AS AppellantForenames, 
# MAGIC ap.BirthDate, 
# MAGIC ap.Email AS AppellantEmail, 
# MAGIC ap.Telephone AS AppellantTelephone, 
# MAGIC ap.Address1 AS AppellantAddress1, 
# MAGIC ap.Address2 AS AppellantAddress2, 
# MAGIC ap.Address3 AS AppellantAddress3, 
# MAGIC ap.Address4 AS AppellantAddress4, 
# MAGIC ap.Address5 AS AppellantAddress5, 
# MAGIC ap.Postcode AS AppellantPostcode, 
# MAGIC ap.AppellantCountryId, 
# MAGIC ap.FCONumber 
# MAGIC FROM dbo.AppealCase ac 
# MAGIC LEFT OUTER JOIN dbo.CaseAppellant cap ON cap.CaseNO = ac.CaseNo 
# MAGIC LEFT OUTER JOIN dbo.Appellant ap ON cap.AppellantId = ap.AppellantId 
# MAGIC ```

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_caseappellant_appellant",
    comment="DLT table joining AppealCase with CaseAppellant and Appellant details.",
    path=f"{bronze_mnt}/bronze_appealcase_caseappellant_appellant"
)
def bronze_appealcase_caseappellant_appellant():
    df = (
        dlt.read("raw_appealcase").alias("ac")
        .join(dlt.read("raw_caseappellant").alias("cap"), col("ac.CaseNo") == col("cap.CaseNo"), "left_outer")
        .join(dlt.read("raw_appellant").alias("ap"), col("cap.AppellantId") == col("ap.AppellantId"), "left_outer")
        .select(
            col("ac.CaseNo"),

            # Appellant
            col("ap.Name").alias("AppellantName"),
            col("ap.Forenames").alias("AppellantForenames"),
            col("ap.BirthDate"),
            col("ap.Email").alias("AppellantEmail"),
            col("ap.Telephone").alias("AppellantTelephone"),
            col("ap.Address1").alias("AppellantAddress1"),
            col("ap.Address2").alias("AppellantAddress2"),
            col("ap.Address3").alias("AppellantAddress3"),
            col("ap.Address4").alias("AppellantAddress4"),
            col("ap.Address5").alias("AppellantAddress5"),
            col("ap.Postcode").alias("AppellantPostcode"),
            col("ap.AppellantCountryId"),
            col("ap.FCONumber")
        )
    )

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M3 bronze_status_htype_clist_list_ltype_court_lsitting_adj

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- m3.bronze_status_htype_clist_list_ltype_court_lsitting_adj 
# MAGIC -- This will return multiple rows per CaseNo  
# MAGIC SELECT  
# MAGIC s.StatusId, 
# MAGIC s.CaseNo, 
# MAGIC s.CaseStatus, 
# MAGIC s.Outcome, 
# MAGIC s.KeyDate AS HearingDate, 
# MAGIC s.CentreId, 
# MAGIC s.DecisionDate, 
# MAGIC s.Party, 
# MAGIC s.DateReceived, 
# MAGIC s.OutOfTime, 
# MAGIC s.DecisionReserved, 
# MAGIC s.AdjudicatorId, 
# MAGIC adjs.Surname AS AdjSurname,  
# MAGIC adjs.Forenames AS AdjForenames,  
# MAGIC adjs.Title AS AdjTitle,  
# MAGIC s.Promulgated AS DateOfService, 
# MAGIC s.AdditionalLanguageId, 
# MAGIC -- Listing details fields 
# MAGIC s.ListedCentre AS HearingCentre, 
# MAGIC c.CourtName, 
# MAGIC l.ListName, 
# MAGIC lt.Description AS ListType, 
# MAGIC ht.Description AS HearingType, 
# MAGIC cl.StartTime, 
# MAGIC cl.TimeEstimate, 
# MAGIC -- Assigned Judges & Clerk for Listing Details 
# MAGIC adjLS1.Surname AS Judge1FTSurname, 
# MAGIC adjLS1.Forenames AS Judge1FTForenames, 
# MAGIC adjLS1.Title AS Judge1FTTitle,	 
# MAGIC adjLS2.Surname AS Judge2FTSurname, 
# MAGIC adjLS2.Forenames AS Judge2FTForenames, 
# MAGIC adjLS2.Title AS Judge2FTTitle,	 
# MAGIC adjLS3.Surname AS Judge3FTSurname, 
# MAGIC adjLS3.Forenames AS Judge3FTForenames, 
# MAGIC adjLS3.Title AS Judge3FTTitle,	 
# MAGIC adjLS4.Surname AS CourtClerkSurname, 
# MAGIC adjLS4.Forenames AS CourtClerkForenames, 
# MAGIC adjLS4.Title AS CourtClerkTitle, 
# MAGIC ac.Notes 
# MAGIC FROM dbo.Status s 
# MAGIC LEFT OUTER JOIN dbo.CaseList cl ON cl.StatusId = s.StatusId 
# MAGIC LEFT OUTER JOIN dbo.List l ON l.ListId = cl.ListId 
# MAGIC LEFT OUTER JOIN dbo.HearingType ht ON cl.HearingTypeId = ht.HearingTypeId 
# MAGIC LEFT OUTER JOIN dbo.Adjudicator adjs ON s.AdjudicatorId = adjs.AdjudicatorId 
# MAGIC LEFT OUTER JOIN dbo.ListType lt ON l.ListTypeId = lt.ListTypeId 
# MAGIC LEFT OUTER JOIN dbo.Court c ON c.CourtId = l.CourtId 
# MAGIC LEFT OUTER JOIN dbo.AppealCase ac ON ac.CaseNO = s.CaseNo 
# MAGIC LEFT OUTER JOIN dbo.ListSitting LS1 ON l.ListId = LS1.ListId AND LS1.Position = 10 AND LS1.Cancelled = 0 
# MAGIC LEFT OUTER JOIN dbo.Adjudicator adjLS1 ON LS1.AdjudicatorId = adjLS1.AdjudicatorId 
# MAGIC LEFT OUTER JOIN dbo.ListSitting LS2 ON l.ListId = LS2.ListId AND LS2.Position = 11 AND LS2.Cancelled = 0 
# MAGIC LEFT OUTER JOIN dbo.Adjudicator adjLS2 ON LS2.AdjudicatorId = adjLS2.AdjudicatorId 
# MAGIC LEFT OUTER JOIN dbo.ListSitting LS3 ON l.ListId = LS3.ListId AND LS3.Position = 12 AND LS3.Cancelled = 0 
# MAGIC LEFT OUTER JOIN dbo.Adjudicator adjLS3 ON LS3.AdjudicatorId = adjLS3.AdjudicatorId 
# MAGIC LEFT OUTER JOIN dbo.ListSitting LS4 ON l.ListId = LS4.ListId AND LS4.Position = 3 AND LS4.Cancelled = 0 
# MAGIC LEFT OUTER JOIN dbo.Adjudicator adjLS4 ON LS4.AdjudicatorId = adjLS4.AdjudicatorId
# MAGIC
# MAGIC ```

# COMMAND ----------

dlt.table(
    name="bronze_status_htype_clist_list_ltype_court_lsitting_adj",
    comment="DLT table joining Status with hearing types, court listings, adjudicators, and associated metadata.",
    path=f"{bronze_mnt}/bronze_status_htype_clist_list_ltype_court_lsitting_adj"
)
def bronze_status_htype_clist_list_ltype_court_lsitting_adj():
    df = (
        dlt.read("raw_status").alias("s")
        .join(dlt.read("raw_caselist").alias("cl"), col("s.StatusId") == col("cl.StatusId"), "left_outer")
        .join(dlt.read("raw_list").alias("l"), col("cl.ListId") == col("l.ListId"), "left_outer")
        .join(dlt.read("raw_hearingtype").alias("ht"), col("cl.HearingTypeId") == col("ht.HearingTypeId"), "left_outer")
        .join(dlt.read("raw_adjudicator").alias("adjs"), col("s.AdjudicatorId") == col("adjs.AdjudicatorId"), "left_outer")
        .join(dlt.read("raw_listtype").alias("lt"), col("l.ListTypeId") == col("lt.ListTypeId"), "left_outer")
        .join(dlt.read("raw_court").alias("c"), col("l.CourtId") == col("c.CourtId"), "left_outer")
        .join(dlt.read("raw_appealcase").alias("ac"), col("s.CaseNo") == col("ac.CaseNo"), "left_outer")
        # Judges & Clerks by Position in ListSitting
        .join(dlt.read("raw_listsitting").alias("LS1"), (col("l.ListId") == col("LS1.ListId")) & (col("LS1.Position") == 10) & (col("LS1.Cancelled") == 0), "left_outer")
        .join(dlt.read("raw_adjudicator").alias("adjLS1"), col("LS1.AdjudicatorId") == col("adjLS1.AdjudicatorId"), "left_outer")
        .join(dlt.read("raw_listsitting").alias("LS2"), (col("l.ListId") == col("LS2.ListId")) & (col("LS2.Position") == 11) & (col("LS2.Cancelled") == 0), "left_outer")
        .join(dlt.read("raw_adjudicator").alias("adjLS2"), col("LS2.AdjudicatorId") == col("adjLS2.AdjudicatorId"), "left_outer")
        .join(dlt.read("raw_listsitting").alias("LS3"), (col("l.ListId") == col("LS3.ListId")) & (col("LS3.Position") == 12) & (col("LS3.Cancelled") == 0), "left_outer")
        .join(dlt.read("raw_adjudicator").alias("adjLS3"), col("LS3.AdjudicatorId") == col("adjLS3.AdjudicatorId"), "left_outer")
        .join(dlt.read("raw_listsitting").alias("LS4"), (col("l.ListId") == col("LS4.ListId")) & (col("LS4.Position") == 3) & (col("LS4.Cancelled") == 0), "left_outer")
        .join(dlt.read("raw_adjudicator").alias("adjLS4"), col("LS4.AdjudicatorId") == col("adjLS4.AdjudicatorId"), "left_outer")
        .select(
            col("s.StatusId"),
            col("s.CaseNo"),
            col("s.CaseStatus"),
            col("s.Outcome"),
            col("s.KeyDate").alias("HearingDate"),
            col("s.CentreId"),
            col("s.DecisionDate"),
            col("s.Party"),
            col("s.DateReceived"),
            col("s.OutOfTime"),
            col("s.DecisionReserved"),
            col("s.AdjudicatorId"),
            col("adjs.Surname").alias("AdjSurname"),
            col("adjs.Forenames").alias("AdjForenames"),
            col("adjs.Title").alias("AdjTitle"),
            col("s.Promulgated").alias("DateOfService"),
            col("s.AdditionalLanguageId"),
            col("s.ListedCentre").alias("HearingCentre"),
            col("c.CourtName"),
            col("l.ListName"),
            col("lt.Description").alias("ListType"),
            col("ht.Description").alias("HearingType"),
            col("cl.StartTime"),
            col("cl.TimeEstimate"),
            # Assigned Judges & Clerk
            col("adjLS1.Surname").alias("Judge1FTSurname"),
            col("adjLS1.Forenames").alias("Judge1FTForenames"),
            col("adjLS1.Title").alias("Judge1FTTitle"),
            col("adjLS2.Surname").alias("Judge2FTSurname"),
            col("adjLS2.Forenames").alias("Judge2FTForenames"),
            col("adjLS2.Title").alias("Judge2FTTitle"),
            col("adjLS3.Surname").alias("Judge3FTSurname"),
            col("adjLS3.Forenames").alias("Judge3FTForenames"),
            col("adjLS3.Title").alias("Judge3FTTitle"),
            col("adjLS4.Surname").alias("CourtClerkSurname"),
            col("adjLS4.Forenames").alias("CourtClerkForenames"),
            col("adjLS4.Title").alias("CourtClerkTitle"),
            col("ac.Notes")
        )
    )

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M4 bronze_appealcase_transaction_transactiontype

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- m4.bronze_appealcase_transaction_transactiontype
# MAGIC -- This will return multiple rows per CaseNo
# MAGIC SELECT
# MAGIC -- Transaction
# MAGIC t.CaseNo,
# MAGIC t.TransactionId,
# MAGIC t.TransactionTypeId,
# MAGIC t.ReferringTransactionId,
# MAGIC t.Amount,
# MAGIC t.TransactionDate,
# MAGIC t.Status,
# MAGIC -- TransactionType
# MAGIC tt.SumBalance,
# MAGIC tt.SumTotalFee,
# MAGIC tt.SumTotalPay
# MAGIC FROM dbo.[Transaction] t
# MAGIC LEFT OUTER JOIN dbo.TransactionType tt ON t.TransactionTypeId = tt.TransactionTypeId
# MAGIC ```

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_transaction_transactiontype",
    comment="DLT table joining Transaction with TransactionType to return multiple rows per CaseNo.",
    path=f"{bronze_mnt}/bronze_appealcase_transaction_transactiontype"
)
def bronze_appealcase_transaction_transactiontype():
    df = (
        dlt.read("raw_transaction").alias("t")
        .join(
            dlt.read("raw_transactiontype").alias("tt"),
            col("t.TransactionTypeId") == col("tt.TransactionTypeId"),
            "left_outer"
        )
        .select(
            col("t.CaseNo"),
            col("t.TransactionId"),
            col("t.TransactionTypeId"),
            col("t.ReferringTransactionId"),
            col("t.Amount"),
            col("t.TransactionDate"),
            col("t.Status"),
            col("tt.SumBalance"),
            col("tt.SumTotalFee"),
            col("tt.SumTotalPay")
        )
    )

    return df



# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M5 bronze_appealcase_transaction_transactiontype

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- m5.bronze_appealcase_link_linkdetail
# MAGIC -- This can return multiple rows per CaseNo
# MAGIC SELECT
# MAGIC -- Link
# MAGIC l.CaseNo,
# MAGIC l.LinkNo,
# MAGIC -- LinkDetail
# MAGIC ld.ReasonLinkId
# MAGIC FROM dbo.Link l
# MAGIC LEFT OUTER JOIN dbo.LinkDetail ld ON l.LinkNo = ld.LinkNo
# MAGIC ORDER BY CaseNO
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col
import dlt

@dlt.table(
    name="bronze_appealcase_link_linkdetail",
    comment="DLT table joining Link with LinkDetail. Can return multiple rows per CaseNo.",
    path=f"{bronze_mnt}/bronze_appealcase_link_linkdetail"
)
def bronze_appealcase_link_linkdetail():
    df = (
        dlt.read("raw_link").alias("l")
        .join(
            dlt.read("raw_linkdetail").alias("ld"),
            col("l.LinkNo") == col("ld.LinkNo"),
            "left_outer"
        )
        .select(
            col("l.CaseNo"),
            col("l.LinkNo"),
            col("ld.ReasonLinkId")
        )
    )

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M6 bronze_caseadjudicator_adjudicator

# COMMAND ----------

@dlt.table(
    name="bronze_caseadjudicator_adjudicator",
    comment="DLT table joining CaseAdjudicator with Adjudicator. Can return multiple rows per CaseNo.",
    path=f"{bronze_mnt}/bronze_caseadjudicator_adjudicator"
)
def bronze_caseadjudicator_adjudicator():
    df = (
        dlt.read("raw_caseadjudicator").alias("ca")
        .join(
            dlt.read("raw_adjudicator").alias("adj"),
            (col("ca.AdjudicatorId") == col("adj.AdjudicatorId")) & (col("adj.DoNotList") == 0),
            "inner"
        )
        .select(
            col("ca.CaseNo"),
            col("ca.Required"),
            col("adj.Surname").alias("JudgeSurname"),
            col("adj.Forenames").alias("JudgeForenames"),
            col("adj.Title").alias("JudgeTitle")
        )
    )

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_appealcategory

# COMMAND ----------

@dlt.table(
    name="bronze_appealcategory",
    comment="DLT table for AppealCategory. Returns multiple rows per CaseNo.",
    path=f"{bronze_mnt}/bronze_appealcategory"
)
def bronze_appealcategory():
    return (
        dlt.read("raw_appealcategory")
        .select(
            col("CaseNo"),
            col("CategoryId")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_documentsreceived

# COMMAND ----------

@dlt.table(
    name="bronze_documentsreceived",
    comment="DLT table for DocumentsReceived; may contain multiple rows per CaseNo.",
    path=f"{bronze_mnt}/bronze_documentsreceived"
)
def bronze_documentsreceived():
    return (
        dlt.read("raw_documentsreceived")      # adjust if your raw table name differs
            .select(
                col("CaseNo"),
                col("ReceivedDocumentId"),
                col("DateReceived")
            )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_history

# COMMAND ----------

@dlt.table(
    name="bronze_history",
    comment="DLT table for History; may contain multiple rows per CaseNo.",
    path=f"{bronze_mnt}/bronze_history"
)
def bronze_history():
    return (
        dlt.read("raw_history")   # Replace with actual raw source name if different
            .select(
                col("HistoryId"),
                col("CaseNo"),
                col("HistType"),
                col("Comment")
            )
    )


# COMMAND ----------



@dlt.table(
    name="bronze_balance_due",
    comment="DLT table for balance due per CaseNo based on specific transaction rules.",
    path=f"{bronze_mnt}/bronze_balance_due"
)
def bronze_balance_due():
    # Read transaction and transaction type data
    tx = dlt.read("bronze_appealcase_transaction_transactiontype")
    
    # Filter for balance-related transactions
    filtered_tx = (
        tx.filter((col("SumBalance") == 1) & (~col("TransactionId").isin(
            dlt.read("bronze_appealcase_transaction_transactiontype")
                .filter(col("TransactionTypeId").isin([6, 19]))
                .select("ReferringTransactionId")
                .rdd.flatMap(lambda x: x).collect()
        )))
    )

    # Aggregate by CaseNo
    return (
        filtered_tx
            .groupBy("CaseNo")
            .agg(sum("Amount").alias("BalanceDue"))
    )


# COMMAND ----------


@dlt.table(
    name="bronze_representation",
    comment="DLT table to identify representation type (AIP or LR) per CaseNo based on CaseRep data.",
    path=f"{bronze_mnt}/bronze_representation"
)
def bronze_representation():
    df = dlt.read("bronze_appealcase_caserep_rep")  # corresponds to M1

    return (
        df.filter(col("CaseType") == 1)
          .select("CaseNo", "RepresentativeId", "Name")
          .withColumn(
              "Representation",
              when((col("RepresentativeId") == 957) |
                   (col("RepresentativeId").isNull()) |
                   (col("Name").rlike("(?i)no\\s*rep")) |
                   (col("Name").rlike("(?i)none")) |
                   (col("Name").rlike("(?i)unrepresented")) |
                   (col("Name").rlike("(?i)self")) |
                   (col("Name").rlike("(?i)not\\s*rep")) |
                   (col("Name") == "-") |
                   (col("Name").rlike("(?i)\\(spo")) |
                   (col("Name").rlike("(?i)spons")),
                   "AIP"
              ).otherwise("LR")
          )
          .select("CaseNo", "Representation")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Segmentation DLT Tables Creation - stg_joh_filtered
# MAGIC Segmentation query (to be applied in silver): ???
# MAGIC  
# MAGIC ```sql
# MAGIC SELECT a.[AdjudicatorId] 
# MAGIC FROM [ARIAREPORTS].[dbo].[Adjudicator] a 
# MAGIC     LEFT JOIN [ARIAREPORTS].[dbo].[AdjudicatorRole] jr 
# MAGIC         ON jr.AdjudicatorId = a.AdjudicatorId 
# MAGIC WHERE jr.Role NOT IN ( 7, 8 ) 
# MAGIC GROUP BY a.[AdjudicatorId]
# MAGIC ```
# MAGIC The below staging table is joined with other silver table to esure the Role NOT IN ( 7, 8 ) 

# COMMAND ----------

@dlt.table(
    name="stg_joh_filtered",
    comment="Delta Live silver Table segmentation with judges only using bronze_adjudicator_et_hc_dnur.",
    path=f"{silver_mnt}/stg_joh_filtered"
)
def stg_joh_filtered():
    df = (
        dlt.read("bronze_adjudicator_et_hc_dnur").alias("a")
        .join(
            dlt.read("bronze_adjudicator_role").alias("jr"),
            col("a.AdjudicatorId") == col("jr.AdjudicatorId"), 
            "left"
        )
        .filter((~col("jr.Role").isin(7, 8)) | (col("jr.Role").isNull()))
        .groupBy(col("a.AdjudicatorId"))
        .count()
        .select(col("a.AdjudicatorId"))
    )

    return df


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Silver DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_adjudicator_detail
# MAGIC
# MAGIC

# COMMAND ----------

@dlt.table(
    name="silver_adjudicator_detail",
    comment="Delta Live Silver Table for Adjudicator details enhanced with Hearing Centre and DNUR information.",
    path=f"{silver_mnt}/silver_adjudicator_detail"
)
def silver_adjudicator_detail():
    df =  (
        dlt.read("bronze_adjudicator_et_hc_dnur").alias("adj").join(dlt.read("stg_joh_filtered").alias('flt'), col("adj.AdjudicatorId") == col("flt.AdjudicatorId"), "inner").select(
            col("adj.AdjudicatorId"),
            col("adj.Surname"),
            col("adj.Forenames"),
            col("adj.Title"),
            col("adj.DateOfBirth"),
            when(col("adj.CorrespondenceAddress") == 1, "Business").otherwise("Home").alias("CorrespondenceAddress"),
            col("adj.ContactDetails"),
            when(col("adj.ContactTelephone") == 1, "Business")
                .when(col("adj.ContactTelephone") == 2, "Home")
                .when(col("adj.ContactTelephone") == 3, "Mobile")
                .otherwise(col("adj.ContactTelephone")).alias("ContactTelephone"),
            col("adj.AvailableAtShortNotice"),
            col("adj.DesignatedCentre"),
            col("adj.EmploymentTerm"),
            when(col("adj.FullTime") == 1,'Yes').otherwise("No").alias("FullTime"),
            col("adj.IdentityNumber"),
            col("adj.DateOfRetirement"),
            col("adj.ContractEndDate"),
            col("adj.ContractRenewalDate"),
            col("adj.DoNotUse"),
            col("adj.DoNotUseReason"),
            when(col("adj.JudicialStatus") == 1, "Deputy President")
                .when(col("adj.JudicialStatus") == 2, "Designated Immigration Judge")
                .when(col("adj.JudicialStatus") == 3, "Immigration Judge")
                .when(col("adj.JudicialStatus") == 4, "President")
                .when(col("adj.JudicialStatus") == 5, "Senior Immigration Judge")
                .when(col("adj.JudicialStatus") == 6, "Deputy Senior Immigration Judge")
                .when(col("adj.JudicialStatus") == 7, "Senior President")
                .when(col("adj.JudicialStatus") == 21, "Vice President of the Upper Tribunal IAAC")
                .when(col("adj.JudicialStatus") == 22, "Designated Judge of the First-tier Tribunal")
                .when(col("adj.JudicialStatus") == 23, "Judge of the First-tier Tribunal")
                .when(col("adj.JudicialStatus") == 25, "Upper Tribunal Judge")
                .when(col("adj.JudicialStatus") == 26, "Deputy Judge of the Upper Tribunal")
                .when(col("adj.JudicialStatus") == 28, "Resident Judge of the First-tier Tribunal")
                .when(col("adj.JudicialStatus") == 29, "Principle Resident Judge of the Upper Tribunal")
                .otherwise(col("adj.JudicialStatus")).alias("JudicialStatus"),
            col("adj.Address1"),
            col("adj.Address2"),
            col("adj.Address3"),
            col("adj.Address4"),
            col("adj.Address5"),
            col("adj.Postcode"),
            col("adj.Telephone"),
            col("adj.Mobile"),
            col("adj.Email"),
            col("adj.BusinessAddress1"),
            col("adj.BusinessAddress2"),
            col("adj.BusinessAddress3"),
            col("adj.BusinessAddress4"),
            col("adj.BusinessAddress5"),
            col("adj.BusinessPostcode"),
            col("adj.BusinessTelephone"),
            col("adj.BusinessFax"),
            col("adj.BusinessEmail"),
            col("adj.JudicialInstructions"),
            col("adj.JudicialInstructionsDate"),
            col("adj.Notes"),
            col("adj.AdtclmnFirstCreatedDatetime"),
            col("adj.AdtclmnModifiedDatetime"),
            col("adj.SourceFileName"),
            col("adj.InsertedByProcessName")
        )
    )

    return df



# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Transformation  silver_history_detail

# COMMAND ----------

@dlt.table(
    name="silver_history_detail",
    comment="Delta Live Silver Table combining JoHistory data with Users information.",
    path=f"{silver_mnt}/silver_history_detail"
)
def silver_history_detail():
    df = (
        dlt.read("bronze_johistory_users").alias("his").join(dlt.read("stg_joh_filtered").alias('flt'), col("his.AdjudicatorId") == col("flt.AdjudicatorId"), "inner").select(
            col('his.AdjudicatorId'),
            col('his.HistDate'),
            when(col("his.HistType") == 1, "Adjournment")
            .when(col("his.HistType") == 2, "Adjudicator Process")
            .when(col("his.HistType") == 3, "Bail Process")
            .when(col("his.HistType") == 4, "Change of Address")
            .when(col("his.HistType") == 5, "Decisions")
            .when(col("his.HistType") == 6, "File Location")
            .when(col("his.HistType") == 7, "Interpreters")
            .when(col("his.HistType") == 8, "Issue")
            .when(col("his.HistType") == 9, "Links")
            .when(col("his.HistType") == 10, "Listing")
            .when(col("his.HistType") == 11, "SIAC Process")
            .when(col("his.HistType") == 12, "Superior Court")
            .when(col("his.HistType") == 13, "Tribunal Process")
            .when(col("his.HistType") == 14, "Typing")
            .when(col("his.HistType") == 15, "Parties edited")
            .when(col("his.HistType") == 16, "Document")
            .when(col("his.HistType") == 17, "Document Received")
            .when(col("his.HistType") == 18, "Manual Entry")
            .when(col("his.HistType") == 19, "Interpreter")
            .when(col("his.HistType") == 20, "File Detail Changed")
            .when(col("his.HistType") == 21, "Dedicated hearing centre changed")
            .when(col("his.HistType") == 22, "File Linking")
            .when(col("his.HistType") == 23, "Details")
            .when(col("his.HistType") == 24, "Availability")
            .when(col("his.HistType") == 25, "Cancel")
            .when(col("his.HistType") == 26, "De-allocation")
            .when(col("his.HistType") == 27, "Work Pattern")
            .when(col("his.HistType") == 28, "Allocation")
            .when(col("his.HistType") == 29, "De-Listing")
            .when(col("his.HistType") == 30, "Statutory Closure")
            .when(col("his.HistType") == 31, "Provisional Destruction Date")
            .when(col("his.HistType") == 32, "Destruction Date")
            .when(col("his.HistType") == 33, "Date of Service")
            .when(col("his.HistType") == 34, "IND Interface")
            .when(col("his.HistType") == 35, "Address Changed")
            .when(col("his.HistType") == 36, "Contact Details")
            .when(col("his.HistType") == 37, "Effective Date")
            .when(col("his.HistType") == 38, "Centre Changed")
            .when(col("his.HistType") == 39, "Appraisal Added")
            .when(col("his.HistType") == 40, "Appraisal Removed")
            .when(col("his.HistType") == 41, "Costs Deleted")
            .when(col("his.HistType") == 42, "Credit/Debit Card Payment received")
            .when(col("his.HistType") == 43, "Bank Transfer Payment received")
            .when(col("his.HistType") == 44, "Chargeback Taken")
            .when(col("his.HistType") == 45, "Remission request Rejected")
            .when(col("his.HistType") == 46, "Refund Event Added")
            .when(col("his.HistType") == 47, "WriteOff, Strikeout Write-Off or Threshold Write-off Event Added")
            .when(col("his.HistType") == 48, "Aggregated Payment Taken")
            .when(col("his.HistType") == 49, "Case Created")
            .when(col("his.HistType") == 50, "Tracked Document")
            .otherwise(col("his.HistType")).alias("HistType"),
            col('his.UserName'),
            col('his.Comment'),
            col('his.AdtclmnFirstCreatedDatetime'),
            col('his.AdtclmnModifiedDatetime'),
            col('his.SourceFileName'),
            col('his.InsertedByProcessName'),
        )
    )

    return df



# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_othercentre_detail

# COMMAND ----------

@dlt.table(
    name="silver_othercentre_detail",
    comment="Delta Live silver Table combining OtherCentre data with HearingCentre information.",
    path=f"{silver_mnt}/silver_othercentre_detail"
)
def silver_othercentre_detail():
    df = (dlt.read("bronze_othercentre_hearingcentre").alias("hc").join(dlt.read("stg_joh_filtered").alias('flt'), col("hc.AdjudicatorId") == col("flt.AdjudicatorId"), "inner").select("hc.*"))

    return df



# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_appointment_detail

# COMMAND ----------

@dlt.table(
    name="silver_appointment_detail",
    comment="Delta Live Silver Table for Adjudicator Role data.",
    path=f"{silver_mnt}/silver_appointment_detail"
)
def silver_appointment_detail():
    df = (
        dlt.read("bronze_adjudicator_role").alias("rol").join(dlt.read("stg_joh_filtered").alias('flt'), col("rol.AdjudicatorId") == col("flt.AdjudicatorId"), "inner").select(
            col('rol.AdjudicatorId'),
            when(col("rol.Role") == 2, "Chairman")
            .when(col("rol.Role") == 5, "Adjudicator")
            .when(col("rol.Role") == 6, "Lay Member")
            .when(col("rol.Role") == 7, "Court Clerk")
            .when(col("rol.Role") == 8, "Usher")
            .when(col("rol.Role") == 9, "Qualified Member")
            .when(col("rol.Role") == 10, "Senior Immigration Judge")
            .when(col("rol.Role") == 11, "Immigration Judge")
            .when(col("rol.Role") == 12, "Non Legal Member")
            .when(col("rol.Role") == 13, "Designated Immigration Judge")
            .when(col("rol.Role") == 20, "Upper Tribunal Judge")
            .when(col("rol.Role") == 21, "Judge of the First-tier Tribunal")
            .when(col("rol.Role") == 23, "Designated Judge of the First-tier Tribunal")
            .when(col("rol.Role") == 24, "Upper Tribunal Judge acting As A Judge Of The First-tier Tribunal")
            .when(col("rol.Role") == 25, "Deputy Judge of the Upper Tribunal")
            .otherwise(col("rol.Role")).alias("Role"),
            col('rol.DateOfAppointment'),
            col('rol.EndDateOfAppointment'),
            col('rol.AdtclmnFirstCreatedDatetime'),
            col('rol.AdtclmnModifiedDatetime'),
            col('rol.SourceFileName'),
            col('rol.InsertedByProcessName')
        )
    )

    return df


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
# MAGIC          <td>Client Identifier</td>
# MAGIC          <td>IdentityNumber</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Event Date*</td>
# MAGIC          <td>DateOfRetirement or ContractEndDate, whichever is later. If NULL, use date of migration/generation.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Record Date*</td>
# MAGIC          <td>Date of migration/generation</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Region*</td>
# MAGIC          <td>GBR</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Publisher*</td>
# MAGIC          <td>ARIA</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Record Class*</td>
# MAGIC          <td>ARIA Judicial Records</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Entitlement Tag</td>
# MAGIC          <td>IA_Judicial_Office</td>
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
# MAGIC          <td>Title</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>Forenames</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>Surname</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>Date</td>
# MAGIC          <td>DateOfBirth</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>DesignatedCentre</td>
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

@dlt.table(
    name="silver_archive_metadata",
    comment="Delta Live Silver Table for Archive Metadata data.",
    path=f"{silver_mnt}/silver_archive_metadata"
)
def silver_archive_metadata():
    # Read and join
    # Read and join
    df_base = (
        dlt.read("silver_adjudicator_detail").alias("adj")
        .join(
            dlt.read("stg_joh_filtered").alias("flt"),
            col("adj.AdjudicatorId") == col("flt.AdjudicatorId"),
            "inner"
        )
    )

    # Add environment as a literal column
    # env = workspace_env["env"]
    df_with_env = df_base.withColumn("env", lit(env_name))



    # Now use Spark-native `when()` with column condition
    df_final = (
        df_with_env.select(
            col("adj.AdjudicatorId").alias("client_identifier"),
            date_format(
                coalesce(col("adj.DateOfRetirement"), col("adj.ContractEndDate"), col("adj.AdtclmnFirstCreatedDatetime")),
                "yyyy-MM-dd'T'HH:mm:ss'Z'"
            ).alias("event_date"),
            date_format(col("adj.AdtclmnFirstCreatedDatetime"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("recordDate"),
            lit("GBR").alias("region"),
            lit("ARIA").alias("publisher"),
            lit("ARIAJR").alias("record_class"),
            lit("IA_Judicial_Office").alias("entitlement_tag"),
            col("adj.Title").alias("bf_001"),
            col("adj.Forenames").alias("bf_002"),
            col("adj.Surname").alias("bf_003"),
            when(
                col("env") == lit("sbox"),
                date_format(coalesce(col("adj.DateOfBirth"), current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'")
            ).otherwise(
                date_format(col("adj.DateOfBirth"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
            ).alias("bf_004"),
            # col("env"),
            col("adj.DesignatedCentre").alias("bf_005")
        )
    )

    return df_final


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver DLT staging table for gold transformation

# COMMAND ----------

secret = dbutils.secrets.get(KeyVault_name, "curated-connection-string")

# COMMAND ----------

# DBTITLE 1,Azure Blob Storage Connection Setup in Python
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

# Set up the BlobServiceClient with your connection string
connection_string = secret

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Specify the container name
container_name = "gold"
container_client = blob_service_client.get_container_client(container_name)

# COMMAND ----------

# DBTITLE 1,Spark SQL Shuffle Partitions
spark.conf.set("spark.sql.shuffle.partitions", 32)  # Set this to 32 for your 8-worker cluster

# COMMAND ----------

# Helper to format dates in ISO format (YYYY-MM-DD)
def format_date_iso(date_value):
    try:
        if isinstance(date_value, str):
            date_value = datetime.strptime(date_value, "%Y-%m-%d")
        return date_value.strftime("%Y-%m-%d")
    except Exception:
        return ""

# Helper to format dates in dd/MM/YYYY format
def format_date(date_value):
    try:
        if isinstance(date_value, str):
            date_value = datetime.strptime(date_value, "%Y-%m-%d")
        return date_value.strftime("%d/%m/%Y")
    except Exception:
        return ""
    
# Upload HTML to Azure Blob Storage
def upload_to_blob(file_name, file_content):
    try:
        # blob_client = container_client.get_blob_client(f"{gold_outputs}/HTML/{file_name}")
        blob_client = container_client.get_blob_client(f"{file_name}")
        blob_client.upload_blob(file_content, overwrite=True)
        return "success"
    except Exception as e:
        return f"error: {str(e)}"

# Register the upload function as a UDF
upload_udf = udf(upload_to_blob)

# Load template
html_template_list = spark.read.text("/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html").collect()
html_template = "".join([row.value for row in html_template_list])

# Modify the UDF to accept a row object
def generate_html(row, html_template=html_template):
    try:
        # Load template
        # html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
        # with open(html_template_path, "r") as f:
        #     html_template = f.read()

        # Replace placeholders in the template with row data
        replacements = {
            "{{AdjudicatorId}}": str(row.AdjudicatorId),
            "{{Surname}}": row.Surname or "",
            "{{Forenames}}": row.Forenames or "",
            "{{Title}}": row.Title or "",
            "{{DateOfBirth}}": format_date_iso(row.DateOfBirth),
            "{{CorrespondenceAddress}}": row.CorrespondenceAddress or "",
            "{{Telephone}}": row.ContactTelephone or "",
            "{{ContactDetails}}": row.ContactDetails or "",
            "{{DesignatedCentre}}": row.DesignatedCentre or "",
            "{{EmploymentTerm}}": row.EmploymentTerm or "",
            "{{FullTime}}": row.FullTime or "",
            "{{IdentityNumber}}": row.IdentityNumber or "",
            "{{DateOfRetirement}}": format_date_iso(row.DateOfRetirement),
            "{{ContractEndDate}}": format_date_iso(row.ContractEndDate),
            "{{ContractRenewalDate}}": format_date_iso(row.ContractRenewalDate),
            "{{DoNotUseReason}}": row.DoNotUseReason or "",
            "{{JudicialStatus}}": row.JudicialStatus or "",
            "{{Address1}}": row.Address1 or "",
            "{{Address2}}": row.Address2 or "",
            "{{Address3}}": row.Address3 or "",
            "{{Address4}}": row.Address4 or "",
            "{{Address5}}": row.Address5 or "",
            "{{Postcode}}": row.Postcode or "",
            "{{Mobile}}": row.Mobile or "",
            "{{Email}}": row.Email or "",
            "{{BusinessAddress1}}": row.BusinessAddress1 or "",
            "{{BusinessAddress2}}": row.BusinessAddress2 or "",
            "{{BusinessAddress3}}": row.BusinessAddress3 or "",
            "{{BusinessAddress4}}": row.BusinessAddress4 or "",
            "{{BusinessAddress5}}": row.BusinessAddress5 or "",
            "{{BusinessPostcode}}": row.BusinessPostcode or "",
            "{{BusinessTelephone}}": row.BusinessTelephone or "",
            "{{BusinessFax}}": row.BusinessFax or "",
            "{{BusinessEmail}}": row.BusinessEmail or "",
            "{{JudicialInstructions}}": row.JudicialInstructions or "",
            "{{JudicialInstructionsDate}}": format_date_iso(row.JudicialInstructionsDate),
            "{{Notes}}": row.Notes or "",
            "{{OtherCentre}}": "\n".join(
                f"<tr><td id=\"midpadding\">{i+1}</td><td id=\"midpadding\">{centre}</td></tr>"
                for i, centre in enumerate(row.OtherCentres or [])
            ),
            "{{AppointmentPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{i+1}</td><td id=\"midpadding\">{role.Role}</td><td id=\"midpadding\">{format_date(role.DateOfAppointment)}</td><td id=\"midpadding\">{format_date(role.EndDateOfAppointment)}</td></tr>"
                for i, role in enumerate(row.Roles or [])
            ),
            "{{HistoryPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{format_date(hist.HistDate)}</td><td id=\"midpadding\">{hist.HistType}</td><td id=\"midpadding\">{hist.UserName}</td><td id=\"midpadding\">{hist.Comment}</td></tr>"
                for hist in (row.History or [])
            ),
        }

    
        
        for key, value in replacements.items():
            html_template = html_template.replace(key, value)
        
        return html_template
    except Exception as e:
        return f"Failure Error: {e}"

# Register UDF
generate_html_udf = udf(generate_html, StringType())

# COMMAND ----------

def generate_a360(row):
    try:
        metadata_data = {
            "operation": "create_record",
            "relation_id": str(row.client_identifier),
            "record_metadata": {
                "publisher": row.publisher,
                "record_class": row.record_class ,
                "region": row.region,
                "recordDate": str(row.recordDate),
                "event_date": str(row.event_date),
                "client_identifier": str(row.client_identifier),
                "bf_001": row.bf_001 or "",
                "bf_002": row.bf_002 or "",
                "bf_003": row.bf_003 or "",
                "bf_004": str(row.bf_004) or "",
                "bf_005": row.bf_005 or ""
            }
        }

        html_data = {
            "operation": "upload_new_file",
            "relation_id": str(row.client_identifier),
            "file_metadata": {
                "publisher": row.publisher,
                "dz_file_name": f"judicial_officer_{row.client_identifier}.html",
                "file_tag": "html"
            }
        }

        json_data = {
            "operation": "upload_new_file",
            "relation_id": str(row.client_identifier),
            "file_metadata": {
                "publisher": row.publisher,
                "dz_file_name": f"judicial_officer_{row.client_identifier}.json",
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
        return f"Failure Error: {e}"

# Register UDF
generate_a360_udf = udf(generate_a360, StringType())

# COMMAND ----------

# DBTITLE 1,Transformation stg_judicial_officer_combined
@dlt.table(
    name="stg_judicial_officer_combined",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{silver_mnt}/stg_judicial_officer_combined"
)
def stg_judicial_officer_combined():

    df_judicial_officer_details = dlt.read("silver_adjudicator_detail")
    df_other_centres = dlt.read("silver_othercentre_detail")
    df_roles = dlt.read("silver_appointment_detail")
    df_history = dlt.read("silver_history_detail")

    df_joh_metadata = dlt.read("silver_archive_metadata")


    # Aggregate Other Centres
    grouped_centres = df_other_centres.groupBy("AdjudicatorId").agg(
        collect_list("OtherCentres").alias("OtherCentres")
    )

    # Aggregate Roles
    grouped_roles = df_roles.groupBy("AdjudicatorId").agg(
        collect_list(
            struct("Role", "DateOfAppointment", "EndDateOfAppointment")
        ).alias("Roles")
    ).cache()


    # Aggregate History
    grouped_history = df_history.groupBy("AdjudicatorId").agg(
        collect_list(
            struct("HistDate", "HistType", "UserName", "Comment")
        ).alias("History")
    )

    # Join all aggregated data with JudicialOfficerDetails
    df_combined = (
        df_judicial_officer_details
        .join(grouped_centres, "AdjudicatorId", "left")
        .join(grouped_roles, "AdjudicatorId", "left")
        .join(grouped_history, "AdjudicatorId", "left")
    )

    return df_combined


    

# COMMAND ----------

# DBTITLE 1,Transformation  stg_create_joh_json_content
@dlt.table(
    name="stg_create_joh_json_content",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{silver_mnt}/stg_create_joh_json_content"
)
def stg_create_joh_json_content():

    df_combined = dlt.read("stg_judicial_officer_combined")

    df = df_combined.withColumn("JSON_Content", to_json(struct(*df_combined.columns))) \
                    .withColumn("File_Name", concat(lit(f"{gold_outputs}/JSON/judicial_officer_"), col("AdjudicatorId"), lit(f".json")))

    
    df_with_json = df.withColumn("Status", when((col("JSON_Content").like("Failure%") | col("JSON_Content").isNull()), "Failure on Create JSON Content").otherwise("Successful creating JSON Content"))

    return df_with_json
  

    

# COMMAND ----------

# DBTITLE 1,Transformation stg_create_joh_iris_html_content
@dlt.table(
    name="stg_create_joh_html_content",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{silver_mnt}/stg_create_joh_html_content"
)
def stg_create_joh_html_content():

    df_combined = dlt.read("stg_judicial_officer_combined")

    df = df_combined.withColumn("HTML_Content", generate_html_udf(struct(*df_combined.columns))) \
                        .withColumn("File_Name", concat(lit(f"{gold_outputs}/HTML/judicial_officer_"), col("AdjudicatorId"), lit(f".html"))) \

    
    df_with_html = df.withColumn("Status", when((col("HTML_Content").like("Failure%") | col("HTML_Content").isNull()), "Failure on Create HTML Content").otherwise("Successful creating HTML Content"))

    return df_with_html


# COMMAND ----------

# DBTITLE 1,Transformation stg_create_joh_a360_content
@dlt.table(
    name="stg_create_joh_a360_content",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{silver_mnt}/stg_create_joh_a360_content"
)
def stg_create_joh_a360_content():

    df_td_metadata = dlt.read("silver_archive_metadata")
   
    # Optional: Load from Hive if not an initial load
    # if read_hive:
    #   df_td_metadata = spark.read.table(f"hive_metastore.{hive_schema}.silver_archive_metadata")

    # Generate A360 content and associated file names
    df = df_td_metadata.withColumn(
        "A360_Content", generate_a360_udf(struct(*df_td_metadata.columns))
    )

    metadata_df = df.withColumn("Status",when(col("A360_Content").like("Failure%"), "Failure on Creating A360 Content").otherwise("Successful creating A360 Content"))
    
    return metadata_df

  

# COMMAND ----------

#  if read_hive:
#      print("Loading data from Hive")

# COMMAND ----------

# DBTITLE 1,Transformation stg_judicial_officer_unified
@dlt.table(
    name="stg_judicial_officer_unified",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{silver_mnt}/stg_judicial_officer_unified"
)
@dlt.expect_or_drop("No errors in HTML content", "NOT (lower(HTML_Content) LIKE 'failure%')")
@dlt.expect_or_drop("No errors in JSON content", "NOT (lower(JSON_Content) LIKE 'failure%')")
@dlt.expect_or_drop("No errors in A360 content", "NOT (lower(A360_Content) LIKE 'failure%')")
def stg_judicial_officer_unified():

    # Read DLT sources
    a360_df = dlt.read("stg_create_joh_a360_content").alias("a360")
    html_df = dlt.read("stg_create_joh_html_content").withColumn("HTML_File_Name",col("File_Name")).withColumn("HTML_Status",col("Status")).drop("File_name","Status").alias("html")
    json_df = dlt.read("stg_create_joh_json_content").alias("json")


   
    # Perform joins
    df_unified = (
        html_df
        .join(json_df,  ((col("html.AdjudicatorId") == col("json.AdjudicatorId"))), "inner")
        .join(
            a360_df,
            (col("json.AdjudicatorId") == col("a360.client_identifier")),
            "inner"
        )
        .select(
            col("a360.client_identifier"),
            col("a360.bf_001"),
            col("a360.bf_002"),
            col("html.*"),
            col("json.JSON_Content"),
            col("json.File_Name").alias("JSON_File_name"),
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
    window_spec = Window.orderBy(col("client_identifier"), col("bf_001"), col("bf_002"))
    
    df_batch = df_unified.withColumn("row_num", row_number().over(window_spec)) \
                         .withColumn("A360_BatchId", floor((col("row_num") - 1) / 250) + 1) \
                         .withColumn(
                             "File_name", 
                             concat(lit(f"{gold_outputs}/A360/judicial_officer_"), 
                                      col("A360_BatchId"), 
                                      lit(".a360"))
                         ).drop("row_num")

    return df_batch
 

# COMMAND ----------

# DBTITLE 1,Transformation gold_judicial_officer_with_html
checks = {}
checks["html_content_no_error"] = "(HTML_Content NOT LIKE 'Error%')"


@dlt.table(
    name="gold_judicial_officer_with_html",
    comment="Delta Live Gold Table with HTML content and uploads.",
    path=f"{gold_mnt}/gold_judicial_officer_with_html"
)
@dlt.expect_all_or_fail(checks)
def gold_judicial_officer_with_html():
    # Load source data
    df_combined = dlt.read("stg_judicial_officer_unified")

    # # Optional: Load from Hive if not an initial load
    # if read_hive:
    #     df_combined = spark.read.table(f"hive_metastore.{hive_schema}.stg_judicial_officer_unified")

    # Repartition to optimize parallelism
    repartitioned_df = df_combined.repartition(64, col("AdjudicatorId"))

    # Trigger upload logic for each row
    df_with_upload_status = repartitioned_df.withColumn(
        "Status", upload_udf(col("HTML_File_Name"), col("HTML_Content"))
    )


    return df_with_upload_status.select("AdjudicatorId","A360_BatchId", "HTML_Content", col("HTML_File_Name").alias("File_Name"), col("Status"))


# COMMAND ----------

# DBTITLE 1,Transformation gold_judicial_officer_with_json
@dlt.table(
    name="gold_judicial_officer_with_json",
    comment="Delta Live Gold Table with JSON content.",
    path=f"{gold_mnt}/gold_judicial_officer_with_json"
)
def gold_judicial_officer_with_json():
    """
    Delta Live Table for creating and uploading JSON content for judicial officers.
    """
    # Load source data
    df_combined = dlt.read("stg_judicial_officer_unified")
    

    # # Optionally load data from Hive if needed
    # if read_hive:
    #     df_combined = spark.read.table(f"hive_metastore.{hive_schema}.stg_judicial_officer_unified")

     # Repartition to optimize parallelism
    repartitioned_df = df_combined.repartition(64, col("AdjudicatorId"))

    df_with_upload_status = repartitioned_df.withColumn(
        "Status", upload_udf(col("JSON_File_name"), col("JSON_Content"))
    )


    return df_with_upload_status.select("AdjudicatorId","A360_BatchId", "JSON_Content",col("JSON_File_name").alias("File_Name"),"Status")   


# COMMAND ----------

# DBTITLE 1,Transformation gold_judicial_officer_with_a360
checks = {}
checks["A360Content_no_error"] = "(consolidate_A360Content NOT LIKE 'Error%')"

@dlt.table(
    name="gold_judicial_officer_with_a360",
    comment="Delta Live Gold Table with A360 content.",
    path=f"{gold_mnt}/gold_judicial_officer_with_a360"
)
@dlt.expect_all_or_fail(checks)
def gold_judicial_officer_with_a360():
    df_a360 = dlt.read("stg_judicial_officer_unified")

    # # Optionally load data from Hive
    # if read_hive:
    #     df_a360 = spark.read.table(f"hive_metastore.{hive_schema}.stg_appeals_unified")

    # Group by 'A360FileName' with Batching and consolidate the 'sets' texts, separated by newline
    df_agg = df_a360.groupBy("File_Name", "A360_BatchId") \
            .agg(concat_ws("\n", collect_list("A360_Content")).alias("consolidate_A360Content")) \
            .select(col("File_Name"), col("consolidate_A360Content"), col("A360_BatchId"))

    # Repartition the DataFrame to optimize parallelism
    repartitioned_df = df_agg.repartition(64)

    # Remove existing files
    dbutils.fs.rm(f"{gold_outputs}/A360", True)

    # Generate A360 content
    df_with_a360 = repartitioned_df.withColumn(
        "Status", upload_udf(col("File_Name"), col("consolidate_A360Content"))
    )

    return df_with_a360.select("A360_BatchId", "consolidate_A360Content", "File_Name", "Status")

   
    

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Success Message
dbutils.notebook.exit("Notebook completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Appendix

# COMMAND ----------

# %sql
# drop schema ariadm_arm_joh cascade

# COMMAND ----------

# DBTITLE 1,Count of Files in HTML, JSON, and A360 Directories
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/HTML").count())
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/JSON").count())
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/A360").count())

# COMMAND ----------

# df = (
#     spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail").alias("adj").join(spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered").alias('flt'), col("adj.AdjudicatorId") == col("flt.AdjudicatorId"), "inner").select(
#         col('adj.AdjudicatorId').alias('client_identifier'),
#         date_format(coalesce(col('adj.DateOfRetirement'), col('adj.ContractEndDate'), col('adj.AdtclmnFirstCreatedDatetime')), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_date"),
#         date_format(col('adj.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("recordDate"),
#         lit("GBR").alias("region"),
#         lit("ARIA").alias("publisher"),
#         lit("ARIA Judicial Records").alias("record_class"),
#         lit('IA_Judicial_Office').alias("entitlement_tag"),
#         col('adj.Title').alias('bf_001'),
#         col('adj.Forenames').alias('bf_002'),
#         col('adj.Surname').alias('bf_003'),
#         when(
#             workspace_env["env"] == 'dev-sbox',
#             date_format(coalesce(col('adj.DateOfBirth'), current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'")
#         ).otherwise(
#             date_format(col('adj.DateOfBirth'), "yyyy-MM-dd'T'HH:mm:ss'Z'")
#         ).alias('bf_004'),
#         col('adj.DesignatedCentre').alias('bf_005')
#     )
# )

# display(df)

# COMMAND ----------

# DBTITLE 1,HTML Failed Upload Status
# %sql
# select * from hive_metastore.ariadm_arm_joh.gold_judicial_officer_with_html  --where UploadStatus != 'success' and HTMLContent not like '%ERROR%'

# COMMAND ----------

# # case_no = 'IM/00048/2003'
# df = spark.sql("SELECT * FROM hive_metastore.ariadm_arm_joh.gold_judicial_officer_with_html")

# display(df)
# # Filter for the specific case and extract the JSON collection
# filtered_row = df.filter(col("AdjudicatorId") == 1809).select("HTMLContent").first()

# displayHTML(filtered_row["HTMLContent"])

# COMMAND ----------

# DBTITLE 1,JSON Failed Upload Status
# %sql
# select * from hive_metastore.ariadm_arm_joh.gold_judicial_officer_with_json --where UploadStatus != 'success'  and A360Content like '%ERROR%'

# COMMAND ----------

# DBTITLE 1,A360 Failed Upload Status
# %sql
# select * from hive_metastore.ariadm_arm_joh.gold_judicial_officer_with_A360 where UploadStatus != 'success' and A360Content like '%ERROR%'

# COMMAND ----------

# display(spark.read.format("delta").load("/mnt/ingest00curatedsboxsilver/ARIADM/ARM/AUDIT/JOH/joh_cr_audit_table").filter("Table_name LIKE '%bronze%'").groupBy("Table_name").agg({"Run_dt": "max", "*": "count"}))

# COMMAND ----------

# %sql
# drop schema hive_metastore.ariadm_arm_joh cascade

# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_joh.stg_create_joh_json_content --where UploadStatus != 'success' and A360Content like '%ERROR%'

# COMMAND ----------

# from pyspark.sql.functions import col, from_unixtime

# files_df = spark.createDataFrame(dbutils.fs.ls("/mnt/dropzoneariajr/ARIAJR/submission/"))
# files_df = files_df.withColumn("modificationTime", from_unixtime(col("modificationTime") / 1000).cast("timestamp"))

# display(files_df.orderBy(col("modificationTime").desc()))

# COMMAND ----------

# from pyspark.sql.functions import col, from_unixtime

# files_df = spark.createDataFrame(dbutils.fs.ls("/mnt/dropzoneariajr/ARIAJR/response/"))
# files_df = files_df.withColumn("modificationTime", from_unixtime(col("modificationTime") / 1000).cast("timestamp"))

# display(files_df.orderBy(col("modificationTime").desc()))

# COMMAND ----------

# DBTITLE 1,failures
# from pyspark.sql.functions import col, from_unixtime

# files_df = spark.createDataFrame(dbutils.fs.ls("/mnt/dropzoneariajr/ARIAJR/response/"))
# files_df = files_df.withColumn("modificationTime", from_unixtime(col("modificationTime") / 1000).cast("timestamp"))
# files_df = files_df.filter(col("path").contains("_0_"))

# display(files_df.orderBy(col("modificationTime").desc()))
