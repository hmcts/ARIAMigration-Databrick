# Databricks notebook source
# MAGIC %pip install pyspark azure-storage-blob

# COMMAND ----------

# MAGIC %md
# MAGIC # Appeals Active HTML
# MAGIC <table style = 'float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Name: </b></td>
# MAGIC          <td>ARIADM_ARM_APPEALS</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Description: </b></td>
# MAGIC          <td>Notebook to generate a set of CaseNote Document (HTML), for active CDAM integration</td>
# MAGIC       </tr>
# MAGIC    </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import packages

# COMMAND ----------

# run custom functions
import sys
import os
# Append the parent directory to sys.path
# sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..','..')))
# from pyspark.sql.functions import col, max

import dlt
import json
from pyspark.sql.functions import * #when, col,coalesce, current_timestamp, lit, date_format, trim, max
from pyspark.sql.types import *
# from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number
from delta.tables import DeltaTable

# COMMAND ----------

spark.conf.set("pipelines.tableManagedByMultiplePipelinesCheck.enabled", "false")


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
# MAGIC Please note that running the DLT pipeline with the parameter `initial_load = true` will ensure the creation of the corresponding Hive tables. However, during this stage, none of the gold outputs (HTML, JSON, and A360) are processed. To generate the gold outputs, a secondary run with `initial_load = true` is required.

# COMMAND ----------

# DBTITLE 1,Set Paths and Hive Schema Variables
# read_hive = False

AppealCategory = "CDAM"

# Setting variables for use in subsequent cells
raw_mnt = f"abfss://raw@ingest{lz_key}raw{env_name}.dfs.core.windows.net/ARIADM/ACTIVE/APPEALS/{AppealCategory}"
landing_mnt =  f"abfss://landing@ingest{lz_key}landing{env_name}.dfs.core.windows.net/SQLServer/Sales/IRIS/dbo/"  # CORE FULL LOAD SQL TABLES parquest
bronze_mnt = f"abfss://bronze@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/ACTIVE/APPEALS/{AppealCategory}"
silver_mnt =  f"abfss://silver@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/ACTIVE/APPEALS/{AppealCategory}"
gold_mnt =f"abfss://gold@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/ACTIVE/APPEALS/{AppealCategory}"
html_mnt = f"abfss://html-template@ingest{lz_key}landing{env_name}.dfs.core.windows.net/"

gold_outputs = f"ARIADM/ACTIVE/APPEALS/{AppealCategory}"
hive_schema = f"ariadm_active_appeals_cdam_html"
# key_vault = "ingest00-keyvault-sbox"

audit_delta_path = f"abfss://silver@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/ACTIVE/AUDIT/APPEALS/{AppealCategory}/apl_{AppealCategory[-3:].lower()}_cr_audit_table"

# Print all variables
variables = {
    # "read_hive": read_hive,
    "AppealCategory": AppealCategory,
    "raw_mnt": raw_mnt,
    "landing_mnt": landing_mnt,
    "bronze_mnt": bronze_mnt,
    "silver_mnt": silver_mnt,
    "gold_mnt": gold_mnt,
    "html_mnt": html_mnt,
    "gold_outputs": gold_outputs,
    "hive_schema": hive_schema,
    "key_vault": KeyVault_name,
    "audit_delta_path": audit_delta_path
}

display(variables)

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
    # print(f"Reading latest file: {latest_file}")
    
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

# DBTITLE 1,Create or Validate Audit Delta Table in Azure
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

# # Define Delta Table Path in Azure Storage
# # audit_delta_path = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/AUDIT/BAILS/bl_cr_audit_table"


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

# def create_audit_df(df: DataFrame, unique_identifier_desc: str,table_name: str, stage_name: str, description: str, additional_columns: list = None) -> None:
#     """
#     Creates an audit DataFrame and writes it to Delta format.

#     :param df: Input DataFrame from which unique identifiers are extracted.
#     :param unique_identifier_desc: Column name that acts as a unique identifier.
#     :param table_name: Name of the source table.
#     :param stage_name: Name of the data processing stage.
#     :param description: Description of the table.
#     :param additional_columns: List of additional columns to include in the audit DataFrame.
#     """

#     dt_desc = datetime.utcnow()

#     additional_columns = additional_columns or []  # Default to an empty list if None   
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

# # # def log_audit_entry(df,unique_identifier):
# import uuid


# def datetime_uuid():
#     dt_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
#     return str(uuid.uuid5(uuid.NAMESPACE_DNS,dt_str))

# run_id_value = datetime_uuid()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw DLT Tables Creation
# MAGIC

# COMMAND ----------

@dlt.table(
    name="raw_appealcase",
    comment="Delta Live Table ARIA AppealCase.",
    path=f"{raw_mnt}/Raw_AppealCase"
)
def Raw_AppealCase():
    return read_latest_parquet("AppealCase", "tv_AppealCase", "ARIA_ARM_APPEALS")

@dlt.table(
    name="raw_caserespondent",
    comment="Delta Live Table ARIA CaseRespondent.",
    path=f"{raw_mnt}/Raw_CaseRespondent"
)
def CaseRespondent():
    return read_latest_parquet("CaseRespondent", "tv_CaseRespondent", "ARIA_ARM_APPEALS")

@dlt.table(
    name="raw_mainrespondent",
    comment="Delta Live Table ARIA MainRespondent.",
    path=f"{raw_mnt}/Raw_MainRespondent"
)
def raw_MainRespondent():
     return read_latest_parquet("MainRespondent", "tv_MainRespondent", "ARIA_ARM_APPEALS")
@dlt.table(
    name="raw_respondent",
    comment="Delta Live Table ARIA Respondent.",
    path=f"{raw_mnt}/Raw_Respondent"
)
def raw_Respondent():
     return read_latest_parquet("Respondent", "tv_Respondent", "ARIA_ARM_APPEALS")

@dlt.table(
    name="raw_filelocation",
    comment="Delta Live Table ARIA FileLocation.",
    path=f"{raw_mnt}/Raw_FileLocation"
)
def raw_FileLocation():
     return read_latest_parquet("FileLocation", "tv_FileLocation", "ARIA_ARM_APPEALS") 
 
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
    name="raw_language",
    comment="Delta Live Table ARIA Language.",
    path=f"{raw_mnt}/Raw_Language"
)
def raw_Language():
     return read_latest_parquet("Language", "tv_Language", "ARIA_ARM_APPEALS") 
 
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
    name="raw_detentioncentre",
    comment="Delta Live Table ARIA DetentionCentre.",
    path=f"{raw_mnt}/Raw_DetentionCentre"
)
def raw_DetentionCentre():
     return read_latest_parquet("DetentionCentre", "tv_DetentionCentre", "ARIA_ARM_APPEALS") 


@dlt.table(
    name="raw_country",
    comment="Delta Live Table ARIA Country.",
    path=f"{raw_mnt}/Raw_Country"
)
def raw_Country():
     return read_latest_parquet("Country", "tv_Country", "ARIA_ARM_APPEALS") 
 


@dlt.table(
    name="raw_caselist",
    comment="Delta Live Table ARIA CaseList.",
    path=f"{raw_mnt}/Raw_CaseList"
)
def raw_CaseList():
     return read_latest_parquet("CaseList", "tv_CaseList", "ARIA_ARM_APPEALS") 
 

@dlt.table(
    name="raw_status",
    comment="Delta Live Table ARIA Status.",
    path=f"{raw_mnt}/Raw_Status"
)
def raw_Status():
     return read_latest_parquet("Status", "tv_Status", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_hearingtype",
    comment="Delta Live Table ARIA HearingType.",
    path=f"{raw_mnt}/Raw_HearingType"
)
def raw_HearingType():
     return read_latest_parquet("HearingType", "tv_HearingType", "ARIA_ARM_APPEALS") 
 

@dlt.table(
    name="raw_list",
    comment="Delta Live Table ARIA List.",
    path=f"{raw_mnt}/Raw_List"
)
def raw_List():
     return read_latest_parquet("List", "tv_List", "ARIA_ARM_APPEALS") 
 

@dlt.table(
    name="raw_listtype",
    comment="Delta Live Table ARIA ListType.",
    path=f"{raw_mnt}/Raw_ListType"
)
def raw_ListType():
     return read_latest_parquet("ListType", "tv_ListType", "ARIA_ARM_APPEALS")
 

@dlt.table(
    name="raw_court",
    comment="Delta Live Table ARIA Court.",
    path=f"{raw_mnt}/Raw_Court"
)
def raw_Court():
     return read_latest_parquet("Court", "tv_Court", "ARIA_ARM_APPEALS")
 
@dlt.table(
    name="raw_hearingcentre",
    comment="Delta Live Table ARIA HearingCentre.",
    path=f"{raw_mnt}/Raw_HearingCentre"
)
def raw_HearingCentre():
     return read_latest_parquet("HearingCentre", "tv_HearingCentre", "ARIA_ARM_APPEALS")
 
@dlt.table(
    name="raw_listsitting",
    comment="Delta Live Table ARIA ListSitting.",
    path=f"{raw_mnt}/Raw_ListSitting"
)
def raw_ListSitting():
     return read_latest_parquet("ListSitting", "tv_ListSitting", "ARIA_ARM_APPEALS")
 
@dlt.table(
    name="raw_adjudicator",
    comment="Delta Live Table ARIA Adjudicator.",
    path=f"{raw_mnt}/Raw_Adjudicator"
)
def raw_Adjudicator():
     return read_latest_parquet("Adjudicator", "tv_Adjudicator", "ARIA_ARM_APPEALS")
 
@dlt.table(
    name="raw_bfdiary",
    comment="Delta Live Table ARIA BFDiary.",
    path=f"{raw_mnt}/Raw_BFDiary"
)
def raw_BFDiary():
     return read_latest_parquet("BFDiary", "tv_BFDiary", "ARIA_ARM_APPEALS")
 

@dlt.table(
    name="raw_bfType",
    comment="Delta Live Table ARIA BFType.",
    path=f"{raw_mnt}/Raw_BFType"
)
def raw_BFType():
     return read_latest_parquet("BFType", "tv_BFType", "ARIA_ARM_APPEALS") 
 

@dlt.table(
    name="raw_history",
    comment="Delta Live Table ARIA History.",
    path=f"{raw_mnt}/Raw_History"
)
def raw_History():
     return read_latest_parquet("History", "tv_History", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_users",
    comment="Delta Live Table ARIA Users.",
    path=f"{raw_mnt}/Raw_Users"
)
def raw_Users():
     return read_latest_parquet("Users", "tv_Users", "ARIA_ARM_APPEALS") 
 

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
    name="raw_casestatus",
    comment="Delta Live Table ARIA CaseStatus.",
    path=f"{raw_mnt}/Raw_CaseStatus"
)
def raw_CaseStatus():
     return read_latest_parquet("CaseStatus", "tv_CaseStatus", "ARIA_ARM_APPEALS")  
 

@dlt.table(
    name="raw_statuscontact",
    comment="Delta Live Table ARIA StatusContact.",
    path=f"{raw_mnt}/Raw_StatusContact"
)
def raw_StatusContact():
     return read_latest_parquet("StatusContact", "tv_StatusContact", "ARIA_ARM_APPEALS")   
 
@dlt.table(
    name="raw_reasonadjourn",
    comment="Delta Live Table ARIA ReasonAdjourn.",
    path=f"{raw_mnt}/Raw_ReasonAdjourn"
)
def raw_ReasonAdjourn():
     return read_latest_parquet("ReasonAdjourn", "tv_ReasonAdjourn", "ARIA_ARM_APPEALS")  
  

# @dlt.table(
#     name="raw_Language",
#     comment="Delta Live Table ARIA Language.",
#     path=f"{raw_mnt}/Raw_Language"
# )
# def raw_Language():
#      return read_latest_parquet("Language", "tv_Language", "ARIA_ARM_APPEALS")  
 

@dlt.table(
    name="raw_decisiontype",
    comment="Delta Live Table ARIA DecisionType.",
    path=f"{raw_mnt}/Raw_DecisionType"
)
def raw_DecisionType():
     return read_latest_parquet("DecisionType", "tv_DecisionType", "ARIA_ARM_APPEALS") 
 

@dlt.table(
    name="raw_appealcategory",
    comment="Delta Live Table ARIA AppealCategory.",
    path=f"{raw_mnt}/Raw_AppealCategory"
)
def raw_AppealCategory():
     return read_latest_parquet("AppealCategory", "tv_AppealCategory", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_category",
    comment="Delta Live Table ARIA Category.",
    path=f"{raw_mnt}/Raw_Category"
)
def raw_Category():
     return read_latest_parquet("Category", "tv_Category", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_casefeesummary",
    comment="Delta Live Table ARIA CaseFeeSummary.",
    path=f"{raw_mnt}/Raw_CaseFeeSummary"
)
def raw_CaseFeeSummary():
     return read_latest_parquet("CaseFeeSummary", "tv_CaseFeeSummary", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_feesatisfaction",
    comment="Delta Live Table ARIA FeeSatisfaction.",
    path=f"{raw_mnt}/Raw_FeeSatisfaction"
)
def raw_FeeSatisfaction():
     return read_latest_parquet("FeeSatisfaction", "tv_FeeSatisfaction", "ARIA_ARM_APPEALS")  

 
@dlt.table(
    name="raw_paymentremissionreason",
    comment="Delta Live Table ARIA PaymentRemissionReason.",
    path=f"{raw_mnt}/Raw_PaymentRemissionReason"
)
def raw_PaymentRemissionReason():
     return read_latest_parquet("PaymentRemissionReason", "tv_PaymentRemissionReason", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_port",
    comment="Delta Live Table ARIA Port.",
    path=f"{raw_mnt}/Raw_Port"
)
def raw_Port():
     return read_latest_parquet("Port", "tv_Port", "ARIA_ARM_APPEALS")   
 
@dlt.table(
    name="raw_embassy",
    comment="Delta Live Table ARIA Embassy.",
    path=f"{raw_mnt}/Raw_Embassy"
)
def raw_Embassy():
     return read_latest_parquet("Embassy", "tv_Embassy", "ARIA_ARM_APPEALS")    
 
@dlt.table(
    name="raw_casesponsor",
    comment="Delta Live Table ARIA CaseSponsor.",
    path=f"{raw_mnt}/Raw_CaseSponsor"
)
def raw_CaseSponsor():
     return read_latest_parquet("CaseSponsor", "tv_CaseSponsor", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_appealgrounds",
    comment="Delta Live Table ARIA AppealGrounds.",
    path=f"{raw_mnt}/Raw_AppealGrounds"
)
def raw_AppealGrounds():
     return read_latest_parquet("AppealGrounds", "tv_AppealGrounds", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_appealtype",
    comment="Delta Live Table ARIA AppealType.",
    path=f"{raw_mnt}/Raw_AppealType"
)
def raw_AppealType():
     return read_latest_parquet("AppealType", "tv_AppealType", "ARIA_ARM_APPEALS") 

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
    name="raw_transactionstatus",
    comment="Delta Live Table ARIA TransactionStatus.",
    path=f"{raw_mnt}/Raw_TransactionStatus"
)
def raw_TransactionStatus():
     return read_latest_parquet("TransactionStatus", "tv_TransactionStatus", "ARIA_ARM_APPEALS")   

@dlt.table(
    name="raw_transactionmethod",
    comment="Delta Live Table ARIA TransactionMethod.",
    path=f"{raw_mnt}/Raw_TransactionMethod"
)
def raw_TransactionMethod():
     return read_latest_parquet("TransactionMethod", "tv_TransactionMethod", "ARIA_ARM_APPEALS")   
 
@dlt.table(
    name="raw_appealhumanright",
    comment="Delta Live Table ARIA AppealHumanRight.",
    path=f"{raw_mnt}/Raw_AppealHumanRight"
)
def raw_AppealHumanRight():
     return read_latest_parquet("AppealHumanRight", "tv_AppealHumanRight", "ARIA_ARM_APPEALS")   
 
@dlt.table(
    name="raw_humanright",
    comment="Delta Live Table ARIA HumanRight.",
    path=f"{raw_mnt}/Raw_HumanRight"
)
def raw_HumanRight():
     return read_latest_parquet("HumanRight", "tv_HumanRight", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_appealnewmatter",
    comment="Delta Live Table ARIA AppealNewMatter.",
    path=f"{raw_mnt}/Raw_AppealNewMatter"
)
def raw_AppealNewMatter():
     return read_latest_parquet("AppealNewMatter", "tv_AppealNewMatter", "ARIA_ARM_APPEALS")  

@dlt.table(
    name="raw_newmatter",
    comment="Delta Live Table ARIA NewMatter.",
    path=f"{raw_mnt}/Raw_NewMatter"
)
def raw_NewMatter():
     return read_latest_parquet("NewMatter", "tv_NewMatter", "ARIA_ARM_APPEALS")  
 

@dlt.table(
    name="raw_documentsreceived",
    comment="Delta Live Table ARIA DocumentsReceived.",
    path=f"{raw_mnt}/Raw_DocumentsReceived"
)
def raw_DocumentsReceived():
     return read_latest_parquet("DocumentsReceived", "tv_DocumentsReceived", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_receiveddocument",
    comment="Delta Live Table ARIA ReceivedDocument.",
    path=f"{raw_mnt}/Raw_ReceivedDocument"
)
def raw_ReceivedDocument():
     return read_latest_parquet("ReceivedDocument", "tv_ReceivedDocument", "ARIA_ARM_APPEALS")  

@dlt.table(
    name="raw_reviewstandarddirection",
    comment="Delta Live Table ARIA ReviewStandardDirection.",
    path=f"{raw_mnt}/Raw_ReviewStandardDirection"
)
def raw_ReviewStandardDirection():
     return read_latest_parquet("ReviewStandardDirection", "tv_ReviewStandardDirection", "ARIA_ARM_APPEALS")  
 

@dlt.table(
    name="raw_StandardDirection",
    comment="Delta Live Table ARIA StandardDirection.",
    path=f"{raw_mnt}/Raw_StandardDirection"
)
def raw_StandardDirection():
     return read_latest_parquet("StandardDirection", "tv_StandardDirection", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_reviewspecificdirection",
    comment="Delta Live Table ARIA ReviewSpecificDirection.",
    path=f"{raw_mnt}/Raw_ReviewSpecificDirection"
)
def raw_ReviewSpecificDirection():
     return read_latest_parquet("ReviewSpecificDirection", "tv_ReviewSpecificDirection", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_costaward",
    comment="Delta Live Table ARIA CostAward.",
    path=f"{raw_mnt}/Raw_CostAward"
)
def raw_CostAward():
     return read_latest_parquet("CostAward", "tv_CostAward", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_costorder",
    comment="Delta Live Table ARIA CostOrder.",
    path=f"{raw_mnt}/Raw_CostOrder"
)
def raw_CostOrder():
     return read_latest_parquet("CostOrder", "tv_CostOrder", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_hearingpointschangereason",
    comment="Delta Live Table ARIA HearingPointsChangeReason.",
    path=f"{raw_mnt}/Raw_HearingPointsChangeReason"
)
def raw_HearingPointsChangeReason():
     return read_latest_parquet("HearingPointsChangeReason", "tv_HearingPointsChangeReason", "ARIA_ARM_APPEALS")  
 
@dlt.table(
    name="raw_hearingpointshistory",
    comment="Delta Live Table ARIA HearingPointsHistory.",
    path=f"{raw_mnt}/Raw_HearingPointsHistory"
)
def raw_HearingPointsHistory():
     return read_latest_parquet("HearingPointsHistory", "tv_HearingPointsHistory", "ARIA_ARM_APPEALS")  
  
@dlt.table(
    name="raw_appealtypecategory",
    comment="Delta Live Table ARIA AppealTypeCategory.",
    path=f"{raw_mnt}/Raw_AppealTypeCategory"
)
def raw_AppealTypeCategory():
     return read_latest_parquet("AppealTypeCategory", "tv_AppealTypeCategory", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_pou",
    comment="Delta Live Table ARIA AppealTypeCategory.",
    path=f"{raw_mnt}/raw_pou"
)
def raw_pou():
    if env_name == "sbox":
     return read_latest_parquet("ARIAPou", "tv_ARIAPou", "ARIA_ARM_APPEALS") 
    else:
      return read_latest_parquet("Pou", "tv_ARIAPou", "ARIA_ARM_APPEALS") 
 
@dlt.table(
    name="raw_caseadjudicator",
    comment="Delta Live Table ARIA AppealTypeCategory.",
    path=f"{raw_mnt}/raw_caseadjudicator"
)
def raw_caseadjudicator():
     return read_latest_parquet("CaseAdjudicator", "tv_caseadjudicator", "ARIA_ARM_APPEALS") 
 
@dlt.table(
name="raw_stmcases",
comment="Delta Live Table ARIA AppealTypeCategory.",
path=f"{raw_mnt}/raw_stmcases"
)
def raw_stmcases():
    return read_latest_parquet("STMCases", "tv_stmcases", "ARIA_ARM_APPEALS") 


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Bronze DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M1. bronze_appealcase_cr_cs_ca_fl_cres_mr_res_lang 

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_cr_cs_ca_fl_cres_mr_res_lang",
    comment="Delta Live Table combining Appeal Case data with Case Respondent, Main Respondent, Respondent, File Location, Case Representative, Representative, and Language.",
    path=f"{bronze_mnt}/bronze_appealcase_cr_cs_ca_fl_cres_mr_res_lang"
)
def bronze_appealcase_cr_cs_ca_fl_cres_mr_res_lang():
    
    df = dlt.read("raw_appealcase").alias("ac") \
        .join(
            dlt.read("raw_caserespondent").alias("cr"),
            col("ac.CaseNo") == col("cr.CaseNo"),
            "left_outer"
        ) \
        .join(
            dlt.read("raw_mainrespondent").alias("mr"),
            col("cr.MainRespondentId") == col("mr.MainRespondentId"),
            "left_outer"
        ) \
        .join(
            dlt.read("raw_respondent").alias("r"),
            col("cr.RespondentId") == col("r.RespondentId"),
            "left_outer"
        ) \
        .join(
            dlt.read("raw_filelocation").alias("fl"),
            col("ac.CaseNo") == col("fl.CaseNo"),
            "left_outer"
        ) \
        .join(
            dlt.read("raw_caserep").alias("crep"),
            col("ac.CaseNo") == col("crep.CaseNo"),
            "left_outer"
        ) \
        .join(
            dlt.read("raw_representative").alias("rep"),
            col("crep.RepresentativeId") == col("rep.RepresentativeId"),
            "left_outer"
        ) \
        .join(
            dlt.read("raw_language").alias("l"),
            col("ac.LanguageId") == col("l.LanguageId"),
            "left_outer"
        ) \
        .join(
            dlt.read("raw_country").alias("c1"),
            col("ac.CountryId") == col("c1.CountryId"),
            "left_outer"
        ).join(
            dlt.read("raw_country").alias("c2"),
            col("ac.ThirdCountryId") == col("c2.CountryId"),
            "left_outer"
        ).join(
            dlt.read("raw_country").alias("n"),
            col("ac.NationalityId") == col("n.CountryId"),
            "left_outer"
        ).join(
            dlt.read("raw_feesatisfaction").alias("fs"),
            col("ac.FeeSatisfactionId") == col("fs.FeeSatisfactionId"),
            "left_outer"
        ).join(
            dlt.read("raw_pou").alias("p"),
            col("cr.RespondentId") == col("p.PouId"),
            "left_outer"
        ).join(
            dlt.read("raw_embassy").alias("e"),
            col("cr.RespondentId") == col("e.EmbassyId"),
            "left_outer"
        ).select(
            # Appeal Case columns
            trim(col("ac.CaseNo")).alias('CaseNo'), col("ac.CasePrefix"), col("ac.CaseYear"), col("ac.CaseType"),
            col("ac.AppealTypeId"), col("ac.DateLodged"), col("ac.DateReceived"),
            col("ac.PortId"), col("ac.HORef"), col("ac.DateServed"), 
            col("ac.Notes").alias("AppealCaseNote"), col("ac.NationalityId"),
            col('n.Nationality').alias('Nationality'), 
            col("ac.Interpreter"), col("ac.CountryId"),col('c1.Country').alias("CountryOfTravelOrigin"),
            col("ac.DateOfIssue"), 
            col("ac.FamilyCase"), col("ac.OakingtonCase"), col("ac.VisitVisaType"),
            col("ac.HOInterpreter"), col("ac.AdditionalGrounds"), col("ac.AppealCategories"),
            col("ac.DateApplicationLodged"), col("ac.ThirdCountryId"),col('c2.Country').alias("ThirdCountry"),
            col("ac.StatutoryClosureDate"), col("ac.PubliclyFunded"), col("ac.NonStandardSCPeriod"),
            col("ac.CourtPreference"), col("ac.ProvisionalDestructionDate"), col("ac.DestructionDate"),
            col("ac.FileInStatutoryClosure"), col("ac.DateOfNextListedHearing"),
            col("ac.DocumentsReceived"), col("ac.OutOfTimeIssue"), col("ac.ValidityIssues"),
            col("ac.ReceivedFromRespondent"), col("ac.DateAppealReceived"), col("ac.RemovalDate"),
            col("ac.CaseOutcomeId"), col("ac.AppealReceivedBy"), col("ac.InCamera"),col('ac.SecureCourtRequired'),
            col("ac.DateOfApplicationDecision"), col("ac.UserId"), col("ac.SubmissionURN"),
            col("ac.DateReinstated"), col("ac.DeportationDate"), col("ac.HOANRef").alias("CCDAppealNum"),
            col("ac.HumanRights"), col("ac.TransferOutDate"), col("ac.CertifiedDate"),
            col("ac.CertifiedRecordedDate"), col("ac.NoticeSentDate"),
            col("ac.AddressRecordedDate"), col("ac.ReferredToJudgeDate"),
            col("fs.Description").alias("CertOfFeeSatisfaction"),

            # Case Respondent columns
            col("cr.Respondent").alias("CRRespondent"),
            col("cr.Reference").alias("CRReference"),
            col("cr.Contact").alias("CRContact"),
            
            # Main Respondent columns
            col("mr.Name").alias("MRName"),
            # col("mr.Embassy").alias("MREmbassy"),
            # col("mr.POU").alias("MRPOU"),
            # col("mr.Respondent").alias("MRRespondent"),
            
            # Respondent columns
            col("r.ShortName").alias("RespondentName"),
            col("r.PostalName").alias("RespondentPostalName"),
            col("r.Department").alias("RespondentDepartment"),
            col("r.Address1").alias("RespondentAddress1"),
            col("r.Address2").alias("RespondentAddress2"),
            col("r.Address3").alias("RespondentAddress3"),
            col("r.Address4").alias("RespondentAddress4"),
            col("r.Address5").alias("RespondentAddress5"),
            col("r.Postcode").alias("RespondentPostcode"),
            col("r.Email").alias("RespondentEmail"),
            col("r.Fax").alias("RespondentFax"),
            col("r.Telephone").alias("RespondentTelephone"),
            # col("r.Sdx").alias("RespondentSdx"),
            
            # File Location columns
            col("fl.Note").alias("FileLocationNote"),
            col("fl.TransferDate").alias("FileLocationTransferDate"),
            
            # Case Representative columns
            col("crep.RepresentativeRef"),
            col("crep.Name").alias("CaseRepName"),
            col("crep.RepresentativeId"),
            col("crep.Address1").alias("CaseRepAddress1"),
            col("crep.Address2").alias("CaseRepAddress2"),
            col("crep.Address3").alias("CaseRepAddress3"),
            col("crep.Address4").alias("CaseRepAddress4"),
            col("crep.Address5").alias("CaseRepAddress5"),
            col("crep.Postcode").alias("CaseRepPostcode"),
            col("crep.Telephone").alias("FileSpecificPhone"),
            col("crep.Fax").alias("CaseRepFax"),
            col("crep.Contact").alias("Contact"),
            col("crep.DXNo1").alias("CaseRepDXNo1"),
            col("crep.DXNo2").alias("CaseRepDXNo2"),
            col("crep.TelephonePrime").alias("CaseRepTelephone"),
            col("crep.Email").alias("CaseRepEmail"),
            col("crep.FileSpecificFax"),
            col("crep.FileSpecificEmail"),
            col("crep.LSCCommission"),
            
            # Representative columns
            col("rep.Name").alias("RepName"),
            col("rep.Title").alias("RepTitle"),
            col("rep.Forenames").alias("RepForenames"),
            col("rep.Address1").alias("RepAddress1"),
            col("rep.Address2").alias("RepAddress2"),
            col("rep.Address3").alias("RepAddress3"),
            col("rep.Address4").alias("RepAddress4"),
            col("rep.Address5").alias("RepAddress5"),
            col("rep.Postcode").alias("RepPostcode"),
            col("rep.Telephone").alias("RepTelephone"),
            col("rep.Fax").alias("RepFax"),
            col("rep.Email").alias("RepEmail"),
            # col("rep.Sdx").alias("RepSdx"),
            col("rep.DXNo1").alias("RepDXNo1"),
            col("rep.DXNo2").alias("RepDXNo2"),
            
            # Language columns
            col("l.Description").alias("Language"),
            col("l.DoNotUse").alias("DoNotUseLanguage"),

            # POU columns
            col("p.ShortName").alias("POUShortName"),
            col("p.PostalName").alias("POUPostalName"),
            col("p.Address1").alias("POUAddress1"),
            col("p.Address2").alias("POUAddress2"),
            col("p.Address3").alias("POUAddress3"),
            col("p.Address4").alias("POUAddress4"),
            col("p.Address5").alias("POUAddress5"),
            col("p.Postcode").alias("POUPostcode"),
            col("p.Telephone").alias("POUTelephone"),
            col("p.Fax").alias("POUFax"),
            col("p.Email").alias("POUEmail"),

            # Embassy columns
            col("e.Location").alias("EmbassyLocation"),
            col("e.Embassy"),
            col("e.Surname"),
            col("e.Forename"),
            col("e.Title"),
            col("e.OfficialTitle"),
            col("e.Address1").alias("EmbassyAddress1"),
            col("e.Address2").alias("EmbassyAddress2"),
            col("e.Address3").alias("EmbassyAddress3"),
            col("e.Address4").alias("EmbassyAddress4"),
            col("e.Address5").alias("EmbassyAddress5"),
            col("e.Postcode").alias("EmbassyPostcode"),
            col("e.Telephone").alias("EmbassyTelephone"),
            col("e.Fax").alias("EmbassyFax"),
            col("e.Email").alias("EmbassyEmail"),
            # col("e.DoNotUse").alias("DoNotUseEmbassy")
        )
        
    return df
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M2. bronze_ appealcase _ca_apt_country_detc 

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_ca_apt_country_detc",
    comment="Delta Live Table combining Case Appellant data with Appellant, Detention Centre, and Country information.",
    path=f"{bronze_mnt}/bronze_appealcase_ca_apt_country_detc"
)
def bronze_appealcase_ca_apt_country_detc():
    
    df = dlt.read("raw_caseappellant").alias("ca") \
        .join(
            dlt.read("raw_appellant").alias("a"),
            col("ca.AppellantId") == col("a.AppellantId"),
            "left_outer"
        ).join(
            dlt.read("raw_detentioncentre").alias("dc"),
            col("a.DetentionCentreId") == col("dc.DetentionCentreId"),
            "left_outer"
        ).join(
            dlt.read("raw_country").alias("c"),
            col("a.AppellantCountryId") == col("c.CountryId"),
            "left_outer"
        ).select(
            # Case Appellant fields
            col("ca.AppellantId"),
            trim(col("ca.CaseNo")).alias('CaseNo'),
            col("ca.Relationship").alias("CaseAppellantRelationship"),
            
            # Appellant fields
            col("a.PortReference"),
            col("a.Name").alias("AppellantName"),
            col("a.Forenames").alias("AppellantForenames"),
            col("a.Title").alias("AppellantTitle"),
            col("a.BirthDate").alias("AppellantBirthDate"),
            col("a.Address1").alias("AppellantAddress1"),
            col("a.Address2").alias("AppellantAddress2"),
            col("a.Address3").alias("AppellantAddress3"),
            col("a.Address4").alias("AppellantAddress4"),
            col("a.Address5").alias("AppellantAddress5"),
            col("a.Postcode").alias("AppellantPostcode"),
            col("a.Telephone").alias("AppellantTelephone"),
            col("a.Fax").alias("AppellantFax"),
            col("a.Detained"),
            col("a.Email").alias("AppellantEmail"),
            col("a.FCONumber"),
            col("a.PrisonRef"),
            
            # Detention Centre fields
            col("dc.Centre").alias("DetentionCentre"),
            col("dc.CentreTitle"),
            col("dc.DetentionCentreType"),
            col("dc.Address1").alias("DCAddress1"),
            col("dc.Address2").alias("DCAddress2"),
            col("dc.Address3").alias("DCAddress3"),
            col("dc.Address4").alias("DCAddress4"),
            col("dc.Address5").alias("DCAddress5"),
            col("dc.Postcode").alias("DCPostcode"),
            col("dc.Fax").alias("DCFax"),
            col("dc.Sdx").alias("DCSdx"),
            
            # Country fields
            col("c.Country"),
            col("c.Nationality"),
            col("c.Code"),
            col("c.DoNotUse").alias("DoNotUseCountry"),
            col("c.Sdx").alias("CountrySdx"),
            col("c.DoNotUseNationality")
        )
        
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M3. bronze_ appealcase _cl_ht_list_lt_hc_c_ls_adj

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_cl_ht_list_lt_hc_c_ls_adj",
    comment="Delta Live Table combining Status, Case List, Hearing Type, Adjudicator, Court, and other related details.",
    path=f"{bronze_mnt}/bronze_appealcase_cl_ht_list_lt_hc_c_ls_adj"
)
def bronze_appealcase_cl_ht_list_lt_hc_c_ls_adj():
    df =   dlt.read("raw_status").alias("s") \
        .join(
            dlt.read("raw_caselist").alias("cl"),
            col("s.StatusId") == col("cl.StatusId"),
            "left_outer"
        ).join(
            dlt.read("raw_hearingtype").alias("ht"),
            col("cl.HearingTypeId") == col("ht.HearingTypeId"),
            "left_outer"
        ).join(
            dlt.read("raw_list").alias("l"),
            col("cl.ListId") == col("l.ListId"),
            "left_outer"
        ).join(
            dlt.read("raw_listtype").alias("lt"),
            col("l.ListTypeId") == col("lt.ListTypeId"),
            "left_outer"
        ).join(
            dlt.read("raw_court").alias("c"),
            col("l.CourtId") == col("c.CourtId"),
            "left_outer"
        ).join(
            dlt.read("raw_hearingcentre").alias("hc"),
            col("l.CentreId") == col("hc.CentreId"),
            "left_outer"
        ).join(
            dlt.read("raw_listsitting").alias("ls"),
            col("l.ListId") == col("ls.ListId"),
            "left_outer"
        ).join(
            dlt.read("raw_adjudicator").alias("a"),
            col("ls.AdjudicatorId") == col("a.AdjudicatorId"),
            "left_outer"
        ).select(
            # Status fields
            trim(col("s.CaseNo")).alias('CaseNo'),
            col("s.Outcome"),
            col("s.CaseStatus"),
            col("s.StatusId"), 

            # CaseList fields
            col("cl.TimeEstimate"),
            col("cl.ListNumber"),
            col("cl.HearingDuration"),
            col("cl.StartTime"),
            
            # HearingType fields
            col("ht.Description").alias("HearingTypeDesc"),
            col("ht.TimeEstimate").alias("HearingTypeEst"),
            col("ht.DoNotUse"),
            
            # Adjudicator fields
            col("a.AdjudicatorId").alias("ListAdjudicatorId"), 
            col("a.Surname").alias("ListAdjudicatorSurname"),
            col("a.Forenames").alias("ListAdjudicatorForenames"),
            col("a.Notes").alias("ListAdjudicatorNote"),
            col("a.Title").alias("ListAdjudicatorTitle"),
            
            # List and related fields
            col("l.ListName"),
            col("l.StartTime").alias("ListStartTime"), 
            col("lt.Description").alias("ListTypeDesc"),
            col("lt.ListType"),
            col("lt.DoNotUse").alias("DoNotUseListType"),
            col("l.NumReqSeniorImmigrationJudge").alias("UpperTribJudge"),
            col("l.NumReqDesignatedImmigrationJudge").alias("DesJudgeFirstTier"),
            col("l.NumReqImmigrationJudge").alias("JudgeFirstTier"),
            col("l.NumReqNonLegalMember").alias("NonLegalMember"),
            
            # Court fields
            col("c.CourtName"),
            col("c.DoNotUse").alias("DoNotUseCourt"),
            
            # Hearing Centre fields
            col("hc.Description").alias("HearingCentreDesc"),

            # ListSitting fields
            col("ls.Chairman"),
            col("ls.Position")
        )

        
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M4. bronze_ appealcase _bfdiary_bftype

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_bfdiary_bftype",
    comment="Delta Live Table combining BFDiary and BFType details.",
    path=f"{bronze_mnt}/bronze_appealcase_bfdiary_bftype"
)
def bronze_appealcase_bfdiary_bftype():
    df =  dlt.read("raw_bfdiary").alias("bfd") \
            .join(
                dlt.read("raw_bfType").alias("bft"),
                col("bfd.BFTypeId") == col("bft.BFTypeId"),
                "left_outer"
            ).select(
                # BFDiary fields
                trim(col("bfd.CaseNo")).alias('CaseNo'),
                col("bfd.Entry"),
                col("bfd.EntryDate"),
                col("bfd.BFDate"),
                col("bfd.DateCompleted"),
                col("bfd.Reason"),
                
                # BFType fields
                col("bft.Description").alias("BFTypeDescription"),
                col("bft.DoNotUse")
            )

        
    return df    


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M5: bronze_ appealcase _history_users 

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_history_users",
    comment="Delta Live Table combining History and Users details.",
    path=f"{bronze_mnt}/bronze_appealcase_history_users"
)
def bronze_appealcase_history_users():
    df = dlt.read("raw_history").alias("h") \
        .join(
            dlt.read("raw_users").alias("u"),
            col("h.UserId") == col("u.UserId"),
            "left_outer"
        ).join(
            dlt.read("raw_users").alias("udb"),
            col("h.DeletedBy") == col("udb.UserId"),
            "left_outer"
        ).select(
            # History fields
            col("h.HistoryId"),
            trim(col("h.CaseNo")).alias('CaseNo'),
            col("h.HistDate"),
            col("h.HistType"),
            col("h.Comment").alias("HistoryComment"),
            col("h.StatusId"),
            col("udb.Fullname").alias("DeletedByUser"),
            # User fields
            col("u.Name").alias("UserName"),
            col("u.UserType"),
            col("u.Fullname"),
            col("u.Extension"),
            col("u.DoNotUse")
        )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M6: bronze_appealcase_link_linkdetail 

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_link_linkdetail",
    comment="Delta Live Table combining Link and LinkDetail details.",
    path=f"{bronze_mnt}/bronze_appealcase_link_linkdetail"
)
def bronze_appealcase_link_linkdetail():
    df =  dlt.read("raw_link").alias("l")\
            .join(
                dlt.read("raw_linkdetail").alias("ld"),
                col("l.LinkNo") == col("ld.LinkNo"),
                "left_outer"
            ).join(
                dlt.read("raw_caseappellant").alias("ca"),
                col("l.LinkNo") == col("ca.CaseNo"),
                "left_outer"
            ).join(
                dlt.read("raw_appellant").alias("a"),
                col("ca.AppellantId") == col("a.AppellantId"),
                "left_outer"
            ).filter(col("ca.AppellantId").isNull()).select(
                # Link fields
                trim(col("l.CaseNo")).alias('CaseNo'),
                col("l.LinkNo"),
                
                # LinkDetail fields
                col("ld.Comment").alias("LinkDetailComment"),

                #Appellant fields
                col("Name").alias("LinkName"),
                col("ForeNames").alias("LinkForeNames"),
                col("Title").alias("LinkTitle")
                
            )


        
    return df  



# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M7: bronze_appealcase_status_sc_ra_cs 

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_status_sc_ra_cs",
    comment="Delta Live Table joining Status, CaseStatus, StatusContact, ReasonAdjourn, Language, and DecisionType details.",
    path=f"{bronze_mnt}/bronze_appealcase_status_sc_ra_cs"
)
def bronze_appealcase_status_sc_ra_cs():
    df =   dlt.read("raw_status").alias("s")\
            .join(
                dlt.read("raw_casestatus").alias("cs"),
                col("s.CaseStatus") == col("cs.CaseStatusId"),
                "left_outer"
            ).join(
                dlt.read("raw_statuscontact").alias("sc"),
                col("s.StatusId") == col("sc.StatusId"),
                "left_outer"
            ).join(
                dlt.read("raw_reasonadjourn").alias("ra"),
                col("s.ReasonAdjournId") == col("ra.ReasonAdjournId"),
                "left_outer"
            ).join(
                dlt.read("raw_language").alias("l"),
                col("s.AdditionalLanguageId") == col("l.LanguageId"),
                "left_outer"
            ).join(
                dlt.read("raw_decisiontype").alias("dt"),
                col("s.Outcome") == col("dt.DecisionTypeId"),
                "left_outer"
            ).join(
                dlt.read("raw_adjudicator").alias("a"),
                col("s.AdjudicatorId") == col("a.AdjudicatorId"),
                "left_outer"
            ).join(
                dlt.read("raw_adjudicator").alias("dAdj"),
                col("s.DeterminationBy") == col("dAdj.AdjudicatorId"),
                "left_outer"
            ).join(
                dlt.read("raw_stmcases").alias("stm"),
                col("s.StatusId") == col("stm.NewStatusId"),
                "left_outer"
            ).join(
                dlt.read("raw_listtype").alias("lt"),
                col("s.ListTypeId") == col("lt.ListTypeId"),
                "left_outer"
            ).join(
                dlt.read("raw_hearingtype").alias("ht"),
                col("s.HearingTypeId") == col("ht.HearingTypeId"),
                "left_outer"
            ).join(
                dlt.read("raw_adjudicator").alias("a1"),
                col("stm.Judiciary1Id") == col("a1.AdjudicatorId"),
                "left_outer"
            ).join(
                dlt.read("raw_adjudicator").alias("a2"),
                col("stm.Judiciary2Id") == col("a2.AdjudicatorId"),
                "left_outer"
            ).join(
                dlt.read("raw_adjudicator").alias("a3"),
                col("stm.Judiciary3Id") == col("a3.AdjudicatorId"),
                "left_outer"
            ).select(
                # Status fields
                col("s.StatusId"),
                trim(col("s.CaseNo")).alias('CaseNo'),
                col("s.CaseStatus"),
                col("s.DateReceived"),
                col("s.AdjudicatorId").alias("StatusDetailAdjudicatorId"), 
                col("s.Allegation"),
                col("s.KeyDate"),
                col("s.MiscDate1"),
                col("s.Notes1"),
                col("s.Party"),
                col("s.InTime"),
                col("s.MiscDate2"),
                col("s.MiscDate3"),
                col("s.Notes2"),
                col("s.DecisionDate"),
                col("s.Outcome"),
                col("s.Promulgated"),
                col("s.InterpreterRequired"),
                col("s.AdminCourtReference"),
                col("s.UKAITNo"),
                col("s.FC"),
                col("s.Process"),
                col("s.COAReferenceNumber"),
                col("s.HighCourtReference"),
                col("s.OutOfTime"),
                col("s.ReconsiderationHearing"),
                col("s.DecisionSentToHO"),
                col("s.DecisionSentToHODate"),
                col("s.MethodOfTyping"),
                col("s.CourtSelection"),
                col("s.VideoLink"),
                col("s.DecidingCentre"),
                col("s.Tier"),
                col("s.RemittalOutcome"),
                col("s.UpperTribunalAppellant"),
                col("s.ListRequirementTypeId"),
                col("s.UpperTribunalHearingDirectionId"),
                col("s.ApplicationType"),
                col("s.NoCertAwardDate"),
                col("s.CertRevokedDate"),
                col("s.WrittenOffFileDate"),
                col("s.ReferredEnforceDate"),
                col("s.Letter1Date"),
                col("s.Letter2Date"),
                col("s.Letter3Date"),
                col("s.ReferredFinanceDate"),
                col("s.WrittenOffDate"),
                col("s.CourtActionAuthDate"),
                col("s.BalancePaidDate"),
                col("s.WrittenReasonsRequestedDate"),
                col("s.TypistSentDate"),
                col("s.TypistReceivedDate"),
                col("s.WrittenReasonsSentDate"),
                col("s.ExtemporeMethodOfTyping"),
                col("s.Extempore"),
                col("s.DecisionByTCW"),
                col("s.InitialHearingPoints"),
                col("s.FinalHearingPoints"),
                col("s.HearingPointsChangeReasonId"),
                col("s.OtherCondition"),
                col("s.OutcomeReasons"),
                col("s.AdjournmentParentStatusId"),
                col("s.AdditionalLanguageId"),  
                col("s.CostOrderAppliedFor"),  
                col("s.HearingCourt"),  
                col("s.IRISStatusOfCase"),
                col("s.ListedCentre").alias("HearingCentre"),
                col("s.ListTypeId"),
                col("s.HearingTypeId"),
                
                # CaseStatus fields
                col("cs.Description").alias("CaseStatusDescription"),
                col("cs.DoNotUse").alias("DoNotUseCaseStatus"),
                col("cs.HearingPoints").alias("CaseStatusHearingPoints"),
                
                # StatusContact fields
                col("sc.Contact").alias("ContactStatus"),
                col("sc.CourtName").alias("SCCourtName"),
                col("sc.Address1").alias("SCAddress1"),
                col("sc.Address2").alias("SCAddress2"), 
                col("sc.Address3").alias("SCAddress3"), 
                col("sc.Address4").alias("SCAddress4"), 
                col("sc.Address5").alias("SCAddress5"), 
                col("sc.Postcode").alias("SCPostcode"), 
                col("sc.Telephone").alias("SCTelephone"), 
                col("sc.Forenames").alias("SCForenames"), 
                col("sc.Title").alias("SCTitle"),
                
                # ReasonAdjourn fields
                col("ra.Reason").alias("ReasonAdjourn"),
                col("ra.DoNotUse").alias("DoNotUseReason"),
                
                # Language fields
                col("l.Description").alias("LanguageDescription"), 
                col("l.DoNotUse").alias("DoNotUseLanguage"), 
                
                # DecisionType fields
                col("dt.Description").alias("DecisionTypeDescription"),
                col("dt.DeterminationRequired"),
                col("dt.DoNotUse"),
                col("dt.State"),
                col("dt.BailRefusal"),
                col("dt.BailHOConsent"),

                # Adjudicator
                col("a.Surname").alias("StatusDetailAdjudicatorSurname"),
                col("a.Forenames").alias("StatusDetailAdjudicatorForenames"),
                col("a.Title").alias("StatusDetailAdjudicatorTitle"),
                col("a.Notes").alias('StatusDetailAdjudicatorNote'),

                # Adjudicator DeterminationBy 
                col("dAdj.Surname").alias("DeterminationByJudgeSurname"),
                col("dAdj.Forenames").alias("DeterminationByJudgeForenames"),
                col("dAdj.Title").alias("DeterminationByJudgeTitle"),

                # ListType fields
                col("lt.Description").alias("ListTypeDescription"),

                # HearingType fields
                col("ht.Description").alias("HearingTypeDescription"),

                # STM Cases fields
                col("stm.Judiciary1Id"),
                expr("COALESCE(a1.Surname, '') || ' ' || COALESCE(a1.Forenames, '') || ' ' || COALESCE(a1.Title, '')").alias("Judiciary1Name"),
                col("stm.Judiciary2Id"),
                expr("COALESCE(a2.Surname, '') || ' ' || COALESCE(a2.Forenames, '') || ' ' || COALESCE(a2.Title, '')").alias("Judiciary2Name"),
                col("stm.Judiciary3Id"),
                expr("COALESCE(a3.Surname, '') || ' ' || COALESCE(a3.Forenames, '') || ' ' || COALESCE(a3.Title, '')").alias("Judiciary3Name")
            )

        
    return df  


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M8: bronze_appealcase_appealcatagory_catagory 

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_appealcatagory_catagory",
    comment="Delta Live Table for joining AppealCategory and Category tables to retrieve case and category details.",
    path=f"{bronze_mnt}/bronze_appealcase_appealcatagory_catagory"
)
def bronze_appealcase_appealcatagory_catagory():
    df =  dlt.read("raw_appealcategory").alias("ap")\
            .join(
                dlt.read("raw_category").alias("c"),
                col("ap.CategoryId") == col("c.CategoryId"),
                "left_outer"
            ).select(
                trim(col("ap.CaseNo")).alias('CaseNo'),
                col("c.Description").alias("CategoryDescription"),
                col("c.Flag"),
                col("c.Priority")
            )
        
    return df    


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M10: bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at",
    comment="Delta Live Table for joining AppealCase, CaseFeeSummary, and other related tables to retrieve comprehensive case details.",
    path=f"{bronze_mnt}/bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at"
)
def bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at():
    appeal_case = dlt.read("raw_appealcase").alias("ac")
    case_fee_summary = dlt.read("raw_casefeesummary").alias("cfs")
    fee_satisfaction = dlt.read("raw_feesatisfaction").alias("fs")
    payment_remission_reason = dlt.read("raw_paymentremissionreason").alias("prr")
    port = dlt.read("raw_port").alias("p")
    embassy = dlt.read("raw_embassy").alias("e")
    hearing_centre = dlt.read("raw_hearingcentre").alias("hc")
    case_sponsor = dlt.read("raw_casesponsor").alias("cs")
    # appeal_grounds = dlt.read("raw_appealgrounds").alias("ag")
    appeal_type = dlt.read("raw_appealtype").alias("at")

    ## .join(appeal_grounds, col("ac.CaseNo") == col("ag.CaseNo"), "left_outer") 

    df = appeal_case \
        .join(case_fee_summary, col("ac.CaseNo") == col("cfs.CaseNo"), "left_outer") \
        .join(fee_satisfaction, col("ac.FeeSatisfactionId") == col("fs.FeeSatisfactionId"), "left_outer") \
        .join(payment_remission_reason, col("cfs.PaymentRemissionReason") == col("prr.PaymentRemissionReasonId"), "left_outer") \
        .join(port, col("ac.PortId") == col("p.PortId"), "left_outer") \
        .join(embassy, col("ac.VVEmbassyId") == col("e.EmbassyId"), "left_outer") \
        .join(hearing_centre, col("ac.CentreId") == col("hc.CentreId"), "left_outer") \
        .join(case_sponsor, col("ac.CaseNo") == col("cs.CaseNo"), "left_outer") \
        .join(appeal_type, col("ac.AppealTypeId") == col("at.AppealTypeId"), "left_outer") \
        .select(
            # Appeal Case 
            trim(col("ac.CaseNo")).alias('CaseNo'),
            col("cfs.CaseFeeSummaryId"),
            col("cfs.DatePosting1stTier"),
            col("cfs.DatePostingUpperTier"),
            col("cfs.DateCorrectFeeReceived"),
            col("cfs.DateCorrectFeeDeemedReceived"),
            col("cfs.PaymentRemissionrequested"),
            col("cfs.PaymentRemissionGranted"),
            col("cfs.PaymentRemissionReason"),
            col("cfs.PaymentRemissionReasonNote"),
            col("cfs.ASFReferenceNo"),
            col("cfs.ASFReferenceNoStatus"),
            col("cfs.LSCReference"),
            col("cfs.LSCStatus"),
            col("cfs.LCPRequested"),
            col("cfs.LCPOutcome"),
            col("cfs.S17Reference"),
            col("cfs.S17ReferenceStatus"),
            col("cfs.SubmissionURNCopied"),
            col("cfs.S20Reference"),
            col("cfs.S20ReferenceStatus"),
            col("cfs.HomeOfficeWaiverStatus"),
            # Payment Remission Reason
            col("prr.Description").alias("PaymentRemissionReasonDescription"),
            col("prr.DoNotUse").alias("PaymentRemissionReasonDoNotUse"),
            # Port
            col("p.PortName").alias("POUPortName"),
            col("p.Address1").alias("PortAddress1"),
            col("p.Address2").alias("PortAddress2"),
            col("p.Address3").alias("PortAddress3"),
            col("p.Address4").alias("PortAddress4"),
            col("p.Address5").alias("PortAddress5"),
            col("p.Postcode").alias("PortPostcode"),
            col("p.Telephone").alias("PortTelephone"),
            col("p.Sdx").alias("PortSdx"),
            # Embassy
            col("e.Location").alias("EmbassyLocation"),
            col("e.Embassy"),
            col("e.Surname").alias("Surname"),
            col("e.Forename").alias("Forename"),
            col("e.Title"),
            col("e.OfficialTitle"),
            col("e.Address1").alias("EmbassyAddress1"),
            col("e.Address2").alias("EmbassyAddress2"),
            col("e.Address3").alias("EmbassyAddress3"),
            col("e.Address4").alias("EmbassyAddress4"),
            col("e.Address5").alias("EmbassyAddress5"),
            col("e.Postcode").alias("EmbassyPostcode"),
            col("e.Telephone").alias("EmbassyTelephone"),
            col("e.Fax").alias("EmbassyFax"),
            col("e.Email").alias("EmbassyEmail"),
            col("e.DoNotUse").alias("DoNotUseEmbassy"),
            # HearingCentre
            col("hc.Description").alias('DedicatedHearingCentre'),
            col("hc.Prefix"),
            col("hc.CourtType"),
            col("hc.Address1").alias("HearingCentreAddress1"),
            col("hc.Address2").alias("HearingCentreAddress2"),
            col("hc.Address3").alias("HearingCentreAddress3"),
            col("hc.Address4").alias("HearingCentreAddress4"),
            col("hc.Address5").alias("HearingCentreAddress5"),
            col("hc.Postcode").alias("HearingCentrePostcode"),
            col("hc.Telephone").alias("HearingCentreTelephone"),
            col("hc.Fax").alias("HearingCentreFax"),
            col("hc.Email").alias("HearingCentreEmail"),
            col("hc.Sdx").alias("HearingCentreSdx"),
            col("hc.STLReportPath"),
            col("hc.STLHelpPath"),
            col("hc.LocalPath"),
            col("hc.GlobalPath"),
            col("hc.PouId"),
            col("hc.MainLondonCentre"),
            col("hc.DoNotUse"),
            col("hc.CentreLocation"),
            col("hc.OrganisationId"),
            # Case Sponsor
            col("cs.Name").alias("CaseSponsorName"),
            col("cs.Forenames").alias("CaseSponsorForenames"),
            col("cs.Title").alias("CaseSponsorTitle"),
            col("cs.Address1").alias("CaseSponsorAddress1"),
            col("cs.Address2").alias("CaseSponsorAddress2"),
            col("cs.Address3").alias("CaseSponsorAddress3"),
            col("cs.Address4").alias("CaseSponsorAddress4"),
            col("cs.Address5").alias("CaseSponsorAddress5"),
            col("cs.Postcode").alias("CaseSponsorPostcode"),
            col("cs.Telephone").alias("CaseSponsorTelephone"),
            col("cs.Email").alias("CaseSponsorEmail"),
            col("cs.Authorised"),
            # Appeal Grounds
            # col("ag.AppealTypeId"),
            # Appeal Type 
            col("at.Description").alias("AppealTypeDescription"),
            col("at.Prefix").alias("AppealTypePrefix"),
            col("at.Number").alias("AppealTypeNumber"),
            col("at.FullName").alias("AppealTypeFullName"),
            col("at.Category").alias("AppealTypeCategory"),
            col("at.AppealType"),
            col("at.DoNotUse").alias("AppealTypeDoNotUse"),
            col("at.DateStart").alias("AppealTypeDateStart"),
            col("at.DateEnd").alias("AppealTypeDateEnd")
        )

        
    return df 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M11: bronze_status_decisiontype

# COMMAND ----------

@dlt.table(
    name="bronze_status_decisiontype",
    comment="Delta Live Table for joining Status and DecisionType tables to retrieve case and decision type details.",
    path=f"{bronze_mnt}/bronze_status_decisiontype"
)
def bronze_status_decisiontype():
    df =  dlt.read("raw_status").alias("s")\
            .join(
                dlt.read("raw_decisiontype").alias("dt"),
                col("s.Outcome") == col("dt.DecisionTypeId"),
                "left_outer"
            ).select(
                trim(col("s.CaseNo")).alias('CaseNo'),
                col("dt.Description").alias("DecisionTypeDescription"),
                col("dt.DeterminationRequired"),
                col("dt.DoNotUse"),
                col("dt.State"),
                col("dt.BailRefusal"),
                col("dt.BailHOConsent")
            )
        
    return df        


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M12: bronze_appealcase_t_tt_ts_tm

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_t_tt_ts_tm",
    comment="Delta Live Table for joining Transaction, TransactionType, TransactionStatus, TransactionMethod, and Users tables to retrieve transaction details.",
    path=f"{bronze_mnt}/bronze_appealcase_t_tt_ts_tm"
)
def bronze_appealcase_t_tt_ts_tm():
    df =  dlt.read("raw_transaction").alias("t")\
            .join(
                dlt.read("raw_transactiontype").alias("tt"),
                col("t.TransactionTypeId") == col("tt.TransactionTypeID"),
                "left_outer"
            ).join(
                dlt.read("raw_transactionstatus").alias("ts"),
                col("t.Status") == col("ts.TransactionStatusID"),
                "left_outer"
            ).join(
                dlt.read("raw_transactionmethod").alias("tm"),
                col("t.TransactionMethodId") == col("tm.TransactionMethodID"),
                "left_outer"
            ).join(
                dlt.read("raw_users").alias("u1"),
                col("u1.UserId") == col("t.CreateUserId"),
                "left_outer"
            ).join(
                dlt.read("raw_users").alias("u2"),
                col("u2.UserId") == col("t.LastEditUserId"),
                "left_outer"
            ).select(
                # Transaction fields
                col("t.TransactionId"),
                col("t.CaseNo"),
                col("t.TransactionTypeId"),
                col("t.TransactionMethodId"),
                col("t.TransactionDate"),
                col("t.Amount"),
                col("t.ClearedDate"),
                col("t.Status").alias("TransactionStatusId"),
                col("t.OriginalPaymentReference"),
                col("t.PaymentReference"),
                col("t.AggregatedPaymentURN"),
                col("t.PayerForename"),
                col("t.PayerSurname"),
                col("t.LiberataNotifiedDate"),
                col("t.LiberataNotifiedAggregatedPaymentDate"),
                col("t.BarclaycardTransactionId"),
                col("t.Last4DigitsCard"),
                col("t.Notes").alias("TransactionNotes"),
                col("t.ExpectedDate"),
                col("t.ReferringTransactionId"),
                # col("t.CreateUserId"),
                # col("t.LastEditUserId"),
                col("u1.Name").alias("CreateUserId"), 
                col("u2.Name").alias("LastEditUserID"), 
                
                # TransactionType fields
                col("tt.Description").alias("TransactionDescription"),
                col("tt.InterfaceDescription"),
                col("tt.AllowIfNew"),
                col("tt.DoNotUse"),
                col("tt.SumFeeAdjustment"),
                col("tt.SumPayAdjustment"),
                col("tt.SumTotalFee"),
                col("tt.SumTotalPay"),
                col("tt.SumBalance"),
                col("tt.GridFeeColumn"),
                col("tt.GridPayColumn"),
                col("tt.IsReversal"),
                
                # TransactionStatus fields
                col("ts.Description").alias("TransactionStatusDesc"),
                col("ts.InterfaceDescription").alias("TransactionStatusIntDesc"),
                col("ts.DoNotUse").alias("DoNotUseTransactionStatus"),
                
                # TransactionMethod fields
                col("tm.Description").alias("TransactionMethodDesc"),
                col("tm.InterfaceDescription").alias("TransactionMethodIntDesc"),
                col("tm.DoNotUse").alias("DoNotUseTransactionMethod"),

            )
        
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M13: bronze_appealcase_ahr_hr 

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_ahr_hr",
    comment="Delta Live Table for joining AppealHumanRight and HumanRight tables to retrieve case and human rights details.",
    path=f"{bronze_mnt}/bronze_appealcase_ahr_hr"
)
def bronze_appealcase_ahr_hr():
    df =  dlt.read("raw_appealhumanright").alias("ahr")\
            .join(
                dlt.read("raw_humanright").alias("hr"),
                col("ahr.HumanRightId") == col("hr.HumanRightId"),
                "left_outer"
            ).select(
                col("ahr.CaseNo"),
                col("ahr.HumanRightId"),
                col("hr.Description").alias("HumanRightDescription"),
                col("hr.DoNotShow"),
                col("hr.Priority")
            )
        
    return df      


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M14: bronze_appealcase_anm_nm 

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_anm_nm",
    comment="Delta Live Table for joining AppealNewMatter and NewMatter tables to retrieve appeal and new matter details.",
    path=f"{bronze_mnt}/bronze_appealcase_anm_nm"
)
def bronze_appealcase_anm_nm():
    df =  dlt.read("raw_appealnewmatter").alias("anm")\
            .join(
                dlt.read("raw_newmatter").alias("nm"),
                col("anm.NewMatterId") == col("nm.NewMatterId"),
                "left_outer"
            ).select(
                col("anm.AppealNewMatterId"),
                col("anm.CaseNo"),
                col("anm.NewMatterId"),
                col("anm.Notes").alias("AppealNewMatterNotes"),
                col("anm.DateReceived"),
                col("anm.DateReferredToHO"),
                col("anm.HODecision"),
                col("anm.DateHODecision"),
                col("nm.Description").alias("NewMatterDescription"),
                col("nm.NotesRequired"),
                col("nm.DoNotUse")
            )
        
    return df      


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M15: bronze_appealcase_dr_rd 

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_dr_rd",
    comment="Delta Live Table for joining DocumentsReceived and ReceivedDocument tables to retrieve document details.",
    path=f"{bronze_mnt}/bronze_appealcase_dr_rd"
)
def bronze_appealcase_dr_rd():
    df =  dlt.read("raw_documentsreceived").alias("dr")\
            .join(
                dlt.read("raw_receiveddocument").alias("rd"),
                col("dr.ReceivedDocumentId") == col("rd.ReceivedDocumentId"),
                "left_outer"
            ).select(
                col("dr.CaseNo"),
                col("dr.ReceivedDocumentId"),
                col("dr.DateRequested"),
                col("dr.DateRequired"),
                col("dr.DateReceived"),
                col("dr.NoLongerRequired"),
                col("dr.RepresentativeDate"),
                col("dr.POUDate"),
                col("rd.Description").alias("DocumentDescription"),
                col("rd.DoNotUse"),
                col("rd.Auditable")
            )
        
    return df      


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M16: bronze_appealcase_rsd_sd

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_rsd_sd",
    comment="Delta Live Table for joining ReviewStandardDirection and StandardDirection tables to retrieve review standard direction details.",
    path=f"{bronze_mnt}/bronze_appealcase_rsd_sd"
)
def bronze_appealcase_rsd_sd():
    df =  dlt.read("raw_reviewstandarddirection").alias("rsd")\
            .join(
                dlt.read("raw_StandardDirection").alias("sd"),
                col("rsd.StandardDirectionId") == col("sd.StandardDirectionId"),
                "left_outer"
            ).select(
                col("rsd.ReviewStandardDirectionId"),
                col("rsd.CaseNo"),
                col("rsd.StatusId"),
                col("rsd.StandardDirectionId"),
                col("rsd.DateRequiredIND"),
                col("rsd.DateRequiredAppellantRep"),
                col("rsd.DateReceivedIND"),
                col("rsd.DateReceivedAppellantRep"),
                col("sd.Description"),
                col("sd.DoNotUse")
            )

        
    return df      


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: bronze_review_specific_direction

# COMMAND ----------

@dlt.table(
    name="bronze_review_specific_direction",
    comment="Delta Live Table for retrieving details from the ReviewSpecificDirection table.",
    path=f"{bronze_mnt}/bronze_review_specific_direction"
)
def bronze_review_specific_direction():
    df =  dlt.read("raw_reviewspecificdirection")\
            .select(
                col("ReviewSpecificDirectionId"),
                col("CaseNo"),
                col("StatusId"),
                col("SpecificDirection"),
                col("DateRequiredIND"),
                col("DateRequiredAppellantRep"),
                col("DateReceivedIND"),
                col("DateReceivedAppellantRep")
            )
        
    return df        


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: bronze_cost_award

# COMMAND ----------

# DBTITLE 1,Costward
@dlt.table(
    name="bronze_cost_award",
    comment="Delta Live Table for retrieving details from the CostAward table.",
    path=f"{bronze_mnt}/bronze_cost_award"
)
def bronze_cost_award():
    return (
        dlt.read("raw_costaward").alias("ca")\
            # .join(
            #     dlt.read("raw_link").alias("1"),
            #     col("ca.CaseNo") == col("1.CaseNo"),
            #     "left_outer" 
            # )
            .join(
                dlt.read("raw_caseappellant").alias("cap"),
                col("ca.CaseNo") == col("cap.CaseNo"),
                "left_outer"  
            ).join(
                dlt.read("raw_appellant").alias("a"),
                col("cap.AppellantId") == col("a.AppellantId"),
                "left_outer"  
            ).join(
                dlt.read("raw_casestatus").alias("cs"),
                col("ca.AppealStage") == col("cs.CaseStatusId"),
                "left_outer"
            ).select(
                # Case Appellant columns
                col("ca.CostAwardId"),
                col("ca.CaseNo"),

                # # Link columns
                # col("1.LinkNo"),
                
                # Appellant columns
                col("a.Name"),
                col("a.Forenames"), 
                col("a.Title"),
                
                # Case Status columns
                col("cs.Description").alias("AppealStageDescription"),

                # Cost Award columns
                col("ca.DateOfApplication"),
                col("ca.TypeOfCostAward"),
                col("ca.ApplyingParty"),
                col("ca.PayingParty"),
                col("ca.MindedToAward"),
                col("ca.ObjectionToMindedToAward"),
                col("ca.CostsAwardDecision"),
                col("ca.DateOfDecision"),
                col("ca.CostsAmount"),
                col("ca.OutcomeOfAppeal"),
                col("ca.AppealStage")
            )
    )
        
    return df      

# COMMAND ----------

# DBTITLE 1,Cost award linked
@dlt.table(
    name="bronze_cost_award_linked",
    comment="Delta Live Table for retrieving details from the CostAward_linked table.",
    path=f"{bronze_mnt}/bronze_cost_award_linked"
)
def bronze_cost_award_linked():
    df =  dlt.read("raw_costaward").alias("ca")\
            .join(
                dlt.read("raw_link").alias("l"),
                col("ca.CaseNo") == col("l.CaseNo"),
                "left_outer" 
            ).join(
                dlt.read("raw_caseappellant").alias("cap"),
                col("ca.CaseNo") == col("cap.CaseNo"),
                "left_outer"  
            ).join(
                dlt.read("raw_appellant").alias("a"),
                col("cap.AppellantId") == col("a.AppellantId"),
                "left_outer"  
            ).join(
                dlt.read("raw_casestatus").alias("cs"),
                col("ca.AppealStage") == col("cs.CaseStatusId"),
                "left_outer"
            ).filter(col("l.LinkNo").isNotNull()).select(
                col("ca.CostAwardId"),
                col("l.LinkNo"),
                col("ca.CaseNo"),
                col("a.Name"),
                col("a.Forenames"),
                col("a.Title"),
                col("ca.DateOfApplication"),
                col("ca.TypeOfCostAward"),
                col("ca.ApplyingParty"),
                col("ca.PayingParty"),
                col("ca.MindedToAward"),
                col("ca.ObjectionToMindedToAward"),
                col("ca.CostsAwardDecision"),
                col("ca.DateOfDecision"),
                col("ca.CostsAmount"),
                col("ca.OutcomeOfAppeal"),
                col("ca.AppealStage"),
                col("cs.Description").alias("AppealStageDescription")
            )
        
    return df       

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: bronze_costorder

# COMMAND ----------

# DBTITLE 1,Cost Order
@dlt.table(
    name="bronze_costorder",
    comment="Delta Live Table for retrieving details from the CostOrder table.",
    path=f"{bronze_mnt}/bronze_cost_order"
)
def bronze_costorder():
    df =  dlt.read("raw_costorder").alias("co")\
            .join(
                dlt.read("raw_caserep").alias("cr"),
                col("co.CaseNo") == col("cr.CaseNo"),
                "left_outer"
            ).join(
                dlt.read("raw_representative").alias("r"),
                col("cr.RepresentativeId") == col("r.RepresentativeId"),
                "left_outer"
            ).join(
                dlt.read("raw_decisiontype").alias("dt"),
                col("co.OutcomeOfAppealWhereDecisionMade") == col("dt.DecisionTypeId"),
                "left_outer"
            ).select(
                col("co.CostOrderId"),
                col("co.CaseNo"),
                col("co.AppealStageWhenApplicationMade"),
                col("co.DateOfApplication"),
                col("co.AppealStageWhenDecisionMade"),
                col("co.OutcomeOfAppealWhereDecisionMade"),
                col("dt.Description").alias("OutcomeOfAppealWhereDecisionMadeDescription"),
                col("co.DateOfDecision"),
                col("co.CostOrderDecision"),
                col("co.ApplyingRepresentativeId"),
                col("co.ApplyingRepresentativeName").alias("ApplyingRepresentativeNameCaseRep"),
                col("r.Name").alias("ApplyingRepresentativeNameRep")
            )
        
    return df       


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: bronze_hearing_points_change_reason

# COMMAND ----------

@dlt.table(
    name="bronze_hearing_points_change_reason",
    comment="Delta Live Table for retrieving details from the HearingPointsChangeReason table.",
    path=f"{bronze_mnt}/bronze_hearing_points_change_reason"
)
def bronze_hearing_points_change_reason():
    df =  dlt.read("raw_hearingpointschangereason").alias("hpcr")\
            .join(
                dlt.read("raw_status").alias("s"),
                col("s.HearingPointsChangeReasonId") == col("hpcr.HearingPointsChangeReasonId"),
                "left_outer"
            ) .select(
                col("s.CaseNo"),
                col("s.StatusId"),
                col("hpcr.HearingPointsChangeReasonId"),
                col("hpcr.Description"),
                col("hpcr.DoNotUse")
            )
        
    return df       

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: bronze_hearing_points_history

# COMMAND ----------

@dlt.table(
    name="bronze_hearing_points_history",
    comment="Delta Live Table for retrieving details from the HearingPointsHistory table.",
    path=f"{bronze_mnt}/bronze_hearing_points_history"
)
def bronze_hearing_points_history():
    df =  dlt.read("raw_hearingpointshistory").alias("hph")\
            .join(
                dlt.read("raw_status").alias("s"),
                col("hph.StatusId") == col("s.StatusId"),
                "left_outer"
            ).select(
                col("s.CaseNo"),
                col("s.StatusId"),
                col("hph.HearingPointsHistoryId"),
                col("hph.HistDate"),
                col("hph.HistType"),
                col("hph.UserId"),
                col("hph.DefaultPoints"),
                col("hph.InitialPoints"),
                col("hph.FinalPoints")
            )
        
    return df       

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: bronze_appeal_type_category

# COMMAND ----------

@dlt.table(
    name="bronze_appeal_type_category",
    comment="Delta Live Table for retrieving details from the AppealTypeCategory table.",
    path=f"{bronze_mnt}/bronze_appeal_type_category"
)
def bronze_appeal_type_category():
    appeal_case = dlt.read("raw_appealcase")
    appeal_type = dlt.read("raw_appealtype")
    appeal_type_category = dlt.read("raw_appealtypecategory")
    
    df =  appeal_case.alias("ac")\
        .join(appeal_type.alias("at"), col("ac.AppealTypeId") == col("at.AppealTypeId"), "left_outer")\
        .join(appeal_type_category.alias("atc"), col("at.AppealTypeId") == col("atc.AppealTypeId"), "left_outer")\
        .select(
            col("ac.CaseNo"),
            col("atc.AppealTypeCategoryId"),
            col("atc.AppealTypeId"),
            col("atc.CategoryId"),
            col("atc.FeeExempt")
        )
        
    return df    


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: bronze_appeal_grounds

# COMMAND ----------

@dlt.table(
    name="bronze_appeal_grounds",
    comment="Delta Live Table for retrieving Appeal Grounds with Appeal Type descriptions.",
    path=f"{bronze_mnt}/bronze_appeal_grounds"
)
def bronze_appeal_grounds():
    appeal_grounds = dlt.read("raw_appealgrounds")
    appeal_type = dlt.read("raw_appealtype")
    
    df =  appeal_grounds.alias("ag")\
        .join(appeal_type.alias("at"), col("ag.AppealTypeId") == col("at.AppealTypeId"), "left_outer")\
        .select(
            col("ag.CaseNo"),
            col("ag.AppealTypeId"),
            col("at.Description").alias("AppealTypeDescription")
        )
        
    return df        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: bronze_required_adjudicator

# COMMAND ----------

@dlt.table(
    name="bronze_required_incompatible_adjudicator",
    comment="Delta Live Table for retrieving Appeal Grounds with Appeal Type descriptions.",
    path=f"{bronze_mnt}/bronze_required_incompatible_adjudicator"
)
def bronze_required_incompatible_adjudicator():
    case_adjudicator = dlt.read("raw_caseadjudicator")
    adjudicator = dlt.read("raw_adjudicator")
    
    df = case_adjudicator.alias("ca")\
        .join(adjudicator.alias("adj"), (col("ca.AdjudicatorId") == col("adj.Adjudicatorid")) & (col("adj.DoNotList") == 0), "inner")\
        .select(
            col("ca.CaseNo"),
            col("ca.Required"),
            col("adj.Surname").alias("JudgeSurname"),
            col("adj.Forenames").alias("JudgeForenames"),
            col("adj.Title").alias("JudgeTitle")
        )
        
    return df        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: bronze_case_adjudicator

# COMMAND ----------

@dlt.table(
    name="bronze_case_adjudicator",
    comment="Delta Live Table for retrieving Appeal Grounds with Appeal Type descriptions.",
    path=f"{bronze_mnt}/bronze_case_adjudicator"
)
def bronze_case_adjudicator():
    case_adjudicator = dlt.read("raw_caseadjudicator")
    adjudicator = dlt.read("raw_adjudicator")

    df = case_adjudicator.alias("ca")\
        .join(adjudicator.alias("adj"), (col("ca.AdjudicatorId") == col("adj.Adjudicatorid")) & (col("adj.DoNotList") == 0), "inner")\
        .select(
            col("ca.CaseNo"),
            col("ca.Required"),
            col("adj.Surname").alias("JudgeSurname"),
            col("adj.Forenames").alias("JudgeForenames"),
            col("adj.Title").alias("JudgeTitle")
        )
        
    return df      

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Segmentation tables

# COMMAND ----------

stg_appeals_filtered = spark.read.table("ariadm_active_appeals.stg_segmentation_states")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Silver DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_appealcase_detail

# COMMAND ----------

@dlt.table(
    name="silver_appealcase_detail",
    comment="Delta Live silver Table for Appeals case details.",
    path=f"{silver_mnt}/silver_appealcase_detail"
)
def silver_appealcase_detail():
    appeals_df = dlt.read("bronze_appealcase_cr_cs_ca_fl_cres_mr_res_lang").alias("ap")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = appeals_df.join(flt_df, col("ap.CaseNo") == col("flt.CaseNo"), "inner").select(
        "ap.CaseNo",
        "ap.CasePrefix",
        "ap.CaseYear",
        "ap.CaseType",
        "ap.AppealTypeId",
        "ap.DateLodged",
        "ap.DateReceived",
        "ap.PortId",
        "ap.HORef",
        "ap.DateServed",
        "ap.AppealCaseNote",
        "ap.NationalityId",
        "ap.Nationality",
        when(col("ap.Interpreter") == 1, 'YES').when(col("ap.Interpreter") == 2, 'NO').alias("Interpreter"),
        "ap.CountryId",
        "ap.CountryOfTravelOrigin",
        "ap.DateOfIssue",
        "ap.FamilyCase",
        "ap.OakingtonCase",
        when(col("ap.VisitVisaType") == 0, '').when(col("ap.VisitVisaType") == 1, 'On Papers').when(col("ap.VisitVisaType") == 2, 'Oral Hearing').alias("VisitVisaType"),
        "ap.HOInterpreter",
        when(col("ap.AdditionalGrounds") == 0, '').when(col("ap.AdditionalGrounds") == 1, 'YES').when(col("ap.AdditionalGrounds") == 2, 'NO').alias("AdditionalGrounds"),
        when(col("ap.AppealCategories") == 1, 'YES').when(col("ap.AppealCategories") == 2, 'NO').alias("AppealCategories"),
        "ap.DateApplicationLodged",
        "ap.ThirdCountryId",
        "ap.ThirdCountry",
        "ap.StatutoryClosureDate",
        when(col("ap.PubliclyFunded") == 1, 'checked').when(col("ap.PubliclyFunded") == 0, 'disabled').otherwise('disabled').alias("PubliclyFunded"),
        when(col("ap.NonStandardSCPeriod") == True, 'checked').when(col("ap.NonStandardSCPeriod") == False, 'disabled').otherwise('disabled').alias("NonStandardSCPeriod"),
        when(col("ap.CourtPreference") == 1, 'All-Male').when(col("ap.CourtPreference") == 2, 'All-Female').otherwise('').alias("CourtPreference"),
        "ap.ProvisionalDestructionDate",
        "ap.DestructionDate",
        when(col("ap.FileInStatutoryClosure") == True,'checked').when(col("ap.FileInStatutoryClosure") == False, 'disabled').otherwise('disabled').alias("FileInStatutoryClosure"),
        when(col("ap.DateOfNextListedHearing") == True,'checked').when(col("ap.DateOfNextListedHearing") == False, 'disabled').otherwise('disabled').alias("DateOfNextListedHearing"),
        when(col("ap.DocumentsReceived") == 0, 'Documents exist').when(col("ap.DocumentsReceived") == 1, 'Documents exist').when(col("ap.DocumentsReceived") == 2, '').otherwise('').alias("DocumentsReceived"),
        when(col("ap.OutOfTimeIssue") == 1, 'checked').when(col("ap.OutOfTimeIssue") == 0, 'disabled').otherwise('disabled').alias("OutOfTimeIssue"),
        when(col("ap.ValidityIssues") == 1, 'checked').when(col("ap.ValidityIssues") == 0, 'disabled').otherwise('disabled').alias("ValidityIssues"),
        "ap.ReceivedFromRespondent",
        "ap.DateAppealReceived",
        "ap.RemovalDate",
        "ap.CaseOutcomeId",
        when(col("ap.AppealReceivedBy") == 1, 'Fax').when(col("ap.AppealReceivedBy") == 2, 'Fax and Post').when(col("ap.AppealReceivedBy") == 3, 'Hand').when(col("ap.AppealReceivedBy") == 5, 'On-Line').when(col("ap.AppealReceivedBy") == 4, 'Post').when(col("ap.AppealReceivedBy") == 0, '').otherwise('').alias("AppealReceivedBy"),
        when(col("ap.InCamera") == 1, 'checked').when(col("ap.InCamera") == 0, 'disabled').otherwise('disabled').alias("InCamera"),
        "ap.DateOfApplicationDecision",
        "ap.UserId",
        "ap.SubmissionURN",
        "ap.DateReinstated",
        # "ap.DeportationDate",
        "ap.CCDAppealNum",
        when(col("ap.HumanRights") == 0, '').when(col("ap.HumanRights") == 1, 'YES').when(col("ap.HumanRights") == 2, 'NO').alias("HumanRights"),
        "ap.TransferOutDate",
        "ap.CertifiedDate",
        "ap.CertifiedRecordedDate",
        "ap.NoticeSentDate",
        (col("ap.NoticeSentDate") + expr("INTERVAL 28 DAYS")).alias("DeportationDate"),

        "ap.AddressRecordedDate",
        "ap.ReferredToJudgeDate",
        when(col("ap.SecureCourtRequired") == 1, 'checked').when(col("ap.SecureCourtRequired") == 0, 'disabled').otherwise('disabled').alias("SecureCourtRequired"),
        "ap.CertOfFeeSatisfaction",
        when(col("ap.CRRespondent") == 1, 'respondent').when(col("ap.CRRespondent") == 2, 'embassy').when(col("ap.CRRespondent") == 3, 'POU').alias("CRRespondent"),
        "ap.CRReference",
        "ap.CRContact",
        "ap.MRName",
        # "ap.MREmbassy",
        # "ap.MRPOU",
        # "ap.MRRespondent",
        when(col("ap.CRRespondent") == 3, col("POUShortName")).otherwise("").alias("POUShortName"),
        when(col("ap.CRRespondent") == 1, col("RespondentName")).otherwise("").alias("RespondentName"),
        when(col("ap.CRRespondent") == 1, col("RespondentAddress1")).when(col("ap.CRRespondent") == 2, col("EmbassyAddress1")).when(col("ap.CRRespondent") == 3, col("POUAddress1")).alias("RespondentAddress1"),
        when(col("ap.CRRespondent") == 1, col("RespondentAddress2")).when(col("ap.CRRespondent") == 2, col("EmbassyAddress2")).when(col("ap.CRRespondent") == 3, col("POUAddress2")).alias("RespondentAddress2"),
        when(col("ap.CRRespondent") == 1, col("RespondentAddress3")).when(col("ap.CRRespondent") == 2, col("EmbassyAddress3")).when(col("ap.CRRespondent") == 3, col("POUAddress3")).alias("RespondentAddress3"),
        when(col("ap.CRRespondent") == 1, col("RespondentAddress4")).when(col("ap.CRRespondent") == 2, col("EmbassyAddress4")).when(col("ap.CRRespondent") == 3, col("POUAddress4")).alias("RespondentAddress4"),
        when(col("ap.CRRespondent") == 1, col("RespondentAddress5")).when(col("ap.CRRespondent") == 2, col("EmbassyAddress5")).when(col("ap.CRRespondent") == 3, col("POUAddress5")).alias("RespondentAddress5"),
        when(col("ap.CRRespondent") == 1, col("RespondentPostcode")).when(col("ap.CRRespondent") == 2, col("EmbassyPostcode")).when(col("ap.CRRespondent") == 3, col("POUFax")).alias("RespondentPostcode"),
        when(col("ap.CRRespondent") == 1, col("RespondentTelephone")).when(col("ap.CRRespondent") == 2, col("EmbassyTelephone")).when(col("ap.CRRespondent") == 3, col("POUTelephone")).alias("RespondentTelephone"),
        when(col("ap.CRRespondent") == 1, col("RespondentFax")).when(col("ap.CRRespondent") == 2, col("EmbassyFax")).when(col("ap.CRRespondent") == 3, col("POUFax")).alias("RespondentFax"),
        when(col("ap.CRRespondent") == 1, col("RespondentEmail")).when(col("ap.CRRespondent") == 2, col("EmbassyEmail")).when(col("ap.CRRespondent") == 3, col("POUEmail")).alias("RespondentEmail"),

        # "ap.RespondentName",
        "ap.RespondentPostalName",
        "ap.RespondentDepartment",
        # "ap.RespondentAddress1",
        # "ap.RespondentAddress2",
        # "ap.RespondentAddress3",
        # "ap.RespondentAddress4",
        # "ap.RespondentAddress5",
        # "ap.RespondentPostcode",
        # "ap.RespondentEmail",
        # "ap.RespondentFax",
        # "ap.RespondentTelephone",
        # "ap.RespondentSdx",
        "ap.FileLocationNote",
        "ap.FileLocationTransferDate",
        "ap.RepresentativeRef",
        # "ap.CaseRepName",
        "ap.RepresentativeId",
        # "ap.CaseRepAddress1",
        # "ap.CaseRepAddress2",
        # "ap.CaseRepAddress3",
        # "ap.CaseRepAddress4",
        # "ap.CaseRepAddress5",
        # "ap.CaseRepPostcode",
        "ap.FileSpecificPhone",
        # "ap.CaseRepFax",
        "ap.Contact",
        # "ap.CaseRepDXNo1",
        # "ap.CaseRepDXNo2",
        # "ap.CaseRepTelephone",
        # "ap.CaseRepEmail",
        "ap.FileSpecificFax",
        "ap.FileSpecificEmail",
        when(col("ap.LSCCommission").isNull(), '').when(col("ap.LSCCommission") == 0, '').when(col("ap.LSCCommission") == 1, 'England & Wales').when(col("ap.LSCCommission") == 2, 'Northern Ireland').when(col("ap.LSCCommission") == 3, 'Scotland').alias("LSCCommission"),
        # "ap.RepName",
        # "ap.RepTitle",
        # "ap.RepForenames",
        # "ap.RepAddress1",
        # "ap.RepAddress2",
        # "ap.RepAddress3",
        # "ap.RepAddress4",
        # "ap.RepAddress5",
        # "ap.RepPostcode",
        # "ap.RepTelephone",
        # "ap.RepFax",
        # "ap.RepEmail",
        # "ap.RepSdx",
        # "ap.RepDXNo1",
        # "ap.RepDXNo2",
        "ap.Language",
        # "ap.DoNotUseLanguage",
        when(col("ap.RepresentativeId") == 0, col("CaseRepName")).otherwise(col("RepName")).alias("RepresentativeName"),
        when(col("ap.RepresentativeId") == 0, col("CaseRepAddress1")).otherwise(col("RepAddress1")).alias("RepresentativeAddress1"),
        when(col("ap.RepresentativeId") == 0, col("CaseRepAddress2")).otherwise(col("RepAddress2")).alias("RepresentativeAddress2"),
        when(col("ap.RepresentativeId") == 0, col("CaseRepAddress3")).otherwise(col("RepAddress3")).alias("RepresentativeAddress3"),
        when(col("ap.RepresentativeId") == 0, col("CaseRepAddress4")).otherwise(col("RepAddress4")).alias("RepresentativeAddress4"),
        when(col("ap.RepresentativeId") == 0, col("CaseRepAddress5")).otherwise(col("RepAddress5")).alias("RepresentativeAddress5"),
        when(col("ap.RepresentativeId") ==0, col("CaseRepPostcode")).otherwise(col("RepPostcode")).alias("RepresentativePostcode"),
        when(col("ap.RepresentativeId") == 0, col("CaseRepTelephone")).otherwise(col("RepTelephone")).alias("RepresentativeTelephone"),
        when(col("ap.RepresentativeId") == 0, col("CaseRepFax")).otherwise(col("RepFax")).alias("RepresentativeFax"),
        when(col("ap.RepresentativeId") == 0, col("CaseRepEmail")).otherwise(col("RepEmail")).alias("RepresentativeEmail"),
        when(col("ap.RepresentativeId") == 0, col("CaseRepDXNo1")).otherwise(col("RepDXNo1")).alias("RepresentativeDXNo1"),
        when(col("ap.RepresentativeId") == 0, col("CaseRepDXNo2")).otherwise(col("RepDXNo2")).alias("RepresentativeDXNo2")
    )

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_caseapplicant_detail

# COMMAND ----------

# DBTITLE 1,silver_applicant_detail
@dlt.table(
    name="silver_applicant_detail",
    comment="Delta Live silver Table for casenapplicant detail.",
    path=f"{silver_mnt}/silver_applicant_detail" 
)
def silver_applicant_detail():
    appeals_df = dlt.read("bronze_appealcase_ca_apt_country_detc").alias("ca")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = appeals_df.join(flt_df, col("ca.CaseNo") == col("flt.CaseNo"), "inner").filter(col("ca.CaseAppellantRelationship").isNull()).select(
        "ca.AppellantId",
        "ca.CaseNo",
        "ca.CaseAppellantRelationship",
        "ca.PortReference",
        "ca.AppellantName",
        "ca.AppellantForenames",
        "ca.AppellantTitle",
        "ca.AppellantBirthDate",
        "ca.AppellantAddress1",
        "ca.AppellantAddress2",
        "ca.AppellantAddress3",
        "ca.AppellantAddress4",
        "ca.AppellantAddress5",
        "ca.AppellantPostcode",
        "ca.AppellantTelephone",
        "ca.AppellantFax",
        when(col("ca.Detained") == 1, 'HMP')
            .when(col("ca.Detained") == 2, 'IRC')
            .when(col("ca.Detained") == 3, 'No')
            .when(col("ca.Detained") == 4, 'Other')
            .alias("Detained"),
        "ca.AppellantEmail",
        "ca.FCONumber",
        "ca.PrisonRef",
        "ca.DetentionCentre",
        "ca.CentreTitle",
        "ca.DetentionCentreType",
        "ca.DCAddress1",
        "ca.DCAddress2",
        "ca.DCAddress3",
        "ca.DCAddress4",
        "ca.DCAddress5",
        "ca.DCPostcode",
        "ca.DCFax",
        "ca.DCSdx",
        "ca.Country",
        col("ca.Nationality").alias("ApplicantNationality"),
        "ca.Code",
        "ca.DoNotUseCountry",
        "ca.CountrySdx",
        "ca.DoNotUseNationality"
    )
  
    return joined_df

# COMMAND ----------

# DBTITLE 1,silver_dependent_detail
@dlt.table(
    name="silver_dependent_detail",
    comment="Delta Live silver Table for casenapplicant detail.",
    path=f"{silver_mnt}/silver_dependent_detail" 
)
def silver_dependent_detail():
    appeals_df = dlt.read("bronze_appealcase_ca_apt_country_detc").alias("ca")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = appeals_df.join(flt_df, col("ca.CaseNo") == col("flt.CaseNo"), "inner").filter(col("ca.CaseAppellantRelationship").isNotNull()).select(
        "ca.AppellantId",
        "ca.CaseNo",
        "ca.CaseAppellantRelationship",
        "ca.PortReference",
        "ca.AppellantName",
        "ca.AppellantForenames",
        "ca.AppellantTitle",
        "ca.AppellantBirthDate",
        "ca.AppellantAddress1",
        "ca.AppellantAddress2",
        "ca.AppellantAddress3",
        "ca.AppellantAddress4",
        "ca.AppellantAddress5",
        "ca.AppellantPostcode",
        "ca.AppellantTelephone",
        "ca.AppellantFax",
        when(col("ca.Detained") == 1, 'HMP')
            .when(col("ca.Detained") == 2, 'IRC')
            .when(col("ca.Detained") == 3, 'No')
            .when(col("ca.Detained") == 4, 'Other')
            .alias("Detained"),
        "ca.AppellantEmail",
        "ca.FCONumber",
        "ca.PrisonRef",
        "ca.DetentionCentre",
        "ca.CentreTitle",
        "ca.DetentionCentreType",
        "ca.DCAddress1",
        "ca.DCAddress2",
        "ca.DCAddress3",
        "ca.DCAddress4",
        "ca.DCAddress5",
        "ca.DCPostcode",
        "ca.DCFax",
        "ca.DCSdx",
        "ca.Country",
        col("ca.Nationality").alias("DependentNationality"),
        "ca.Code",
        "ca.DoNotUseCountry",
        "ca.CountrySdx",
        "ca.DoNotUseNationality"
    )
    
    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_list_detail

# COMMAND ----------

@dlt.table(
    name="silver_list_detail",
    comment="Delta Live silver Table for list detail.",
    path=f"{silver_mnt}/silver_list_detail"
)
def silver_list_detail():
    appeals_df = dlt.read("bronze_appealcase_cl_ht_list_lt_hc_c_ls_adj").alias("ca")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    adjudicator_details = (
        appeals_df
        .filter(col("Position").isin(10, 11, 12, 3))
        .select(
            "CaseNo",
            "Position",
            "StatusId",
            "CaseStatus",
            concat_ws(
                "", 
                col("ListAdjudicatorSurname"), 
                lit(", "), 
                col("ListAdjudicatorForenames"), 
                lit(" ("), 
                col("ListAdjudicatorTitle"), 
                lit(")")
            ).alias("JudgeValue")
        )
    )

    joined_df = appeals_df.join(flt_df, col("ca.CaseNo") == col("flt.CaseNo"), "inner")\
                          .join(adjudicator_details.alias("adj1"), 
                                (col("adj1.CaseNo") == col("ca.CaseNo")) & 
                                (col("adj1.StatusId") == col("ca.StatusId")) & 
                                (col("adj1.Position") == lit(10)) & 
                                (col("adj1.CaseStatus") == col("ca.CaseStatus")), 
                                "left")\
                          .join(adjudicator_details.alias("adj2"), 
                                (col("adj2.CaseNo") == col("ca.CaseNo")) & 
                                (col("adj2.StatusId") == col("ca.StatusId")) & 
                                (col("adj2.Position") == lit(11)) & 
                                (col("adj2.CaseStatus") == col("ca.CaseStatus")), 
                                "left")\
                          .join(adjudicator_details.alias("adj3"), 
                                (col("adj3.CaseNo") == col("ca.CaseNo")) & 
                                (col("adj3.StatusId") == col("ca.StatusId")) & 
                                (col("adj3.Position") == lit(12)) & 
                                (col("adj3.CaseStatus") == col("ca.CaseStatus")), 
                                "left")\
                          .join(adjudicator_details.alias("adj4"), 
                                (col("adj4.CaseNo") == col("ca.CaseNo")) & 
                                (col("adj4.StatusId") == col("ca.StatusId")) & 
                                (col("adj4.Position") == lit(3)) & 
                                (col("adj4.CaseStatus") == col("ca.CaseStatus")), 
                                "left")\
                          .withColumn("TimeEstimate_hh_mm", 
                                      expr("floor(ca.TimeEstimate / 60) || ':' || lpad(cast(ca.TimeEstimate % 60 as string), 2, '0')"))\
                          .withColumn("utj", coalesce(col("ca.UpperTribJudge"), lit(0)))\
                          .withColumn("djt", coalesce(col("ca.DesJudgeFirstTier"), lit(0)))\
                          .withColumn("jt", coalesce(col("ca.JudgeFirstTier"), lit(0)))\
                          .withColumn("nlm", coalesce(col("ca.NonLegalMember"), lit(0)))\
                          .withColumn("JudgeLabel1", 
                                      expr("""
                                      case 
                                        when utj >= 1 then 'Upper Trib Judge'
                                        when utj = 0 and djt >= 1 then 'Des Judge First Tier'
                                        when utj = 0 and djt = 0 and jt >= 1 then 'Judge First Tier'
                                        when utj = 0 and djt = 0 and jt = 0 and nlm >= 1 then 'Non-Legal Member'
                                        else null
                                      end
                                      """))\
                          .withColumn("JudgeLabel2", 
                                      expr("""
                                      case 
                                        when utj >= 2 then 'Upper Trib Judge'
                                        when utj in (1) and djt >= 1 then 'Des Judge First Tier'
                                        when utj = 0 and djt >= 2 then 'Des Judge First Tier'
                                        when utj in (1) and djt = 0 and jt >= 1 then 'Judge First Tier'
                                        when utj = 0 and djt in (1) and jt >= 1 then 'Judge First Tier'
                                        when utj = 0 and djt = 0 and jt >= 2 then 'Judge First Tier'
                                        when utj in (1) and djt = 0 and jt = 0 and nlm >= 1 then 'Non-Legal Member'
                                        when utj = 0 and djt in (1) and jt = 0 and nlm >= 1 then 'Non-Legal Member'
                                        when utj = 0 and djt = 0 and jt in (1) and nlm >= 1 then 'Non-Legal Member'
                                        when utj = 0 and djt = 0 and jt = 0 and nlm >= 2 then 'Non-Legal Member'
                                        else null
                                      end
                                      """))\
                          .withColumn("JudgeLabel3", 
                                      expr("""
                                      case 
                                        when utj >= 3 then 'Upper Trib Judge'
                                        when utj in (2) and djt >= 1 then 'Des Judge First Tier'
                                        when utj in (1) and djt >= 2 then 'Des Judge First Tier'
                                        when utj = 0 and djt >= 3 then 'Des Judge First Tier'
                                        when utj in (2) and djt = 0 and jt >= 1 then 'Judge First Tier'
                                        when utj in (1) and djt in (1) and jt >= 1 then 'Judge First Tier'
                                        when utj = 0 and djt in (2) and jt >= 1 then 'Judge First Tier'
                                        when utj = 0 and djt in (1) and jt >= 2 then 'Judge First Tier'
                                        when utj = 0 and djt = 0 and jt >= 3 then 'Judge First Tier'
                                        when utj in (2) and djt = 0 and jt = 0 and nlm >= 1 then 'Non-Legal Member'
                                        when utj in (1) and djt in (1) and jt = 0 and nlm >= 1 then 'Non-Legal Member'
                                        when utj = 0 and djt in (2) and jt = 0 and nlm >= 1 then 'Non-Legal Member'
                                        when utj = 0 and djt in (1) and jt in (1) and nlm >= 1 then 'Non-Legal Member'
                                        when utj = 0 and djt = 0 and jt in (2) and nlm >= 1 then 'Non-Legal Member'
                                        when utj = 0 and djt = 0 and jt in (1) and nlm >= 2 then 'Non-Legal Member'
                                        when utj = 0 and djt = 0 and jt = 0 and nlm >= 3 then 'Non-Legal Member'
                                        else null
                                      end
                                      """))\
                          .withColumn(
                                      "JudgeValue_full", 
                                      concat_ws("", 
                                          col("ca.ListAdjudicatorSurname"), 
                                          lit(", "), 
                                          col("ca.ListAdjudicatorForenames"), 
                                          lit(" ("), 
                                          col("ca.ListAdjudicatorTitle"), 
                                          lit(")")
                                      )
                                  )\
                          .select(
                              "ca.CaseNo",
                              "ca.Outcome",
                              "ca.CaseStatus",
                              "ca.StatusId",
                              col("TimeEstimate_hh_mm").alias("TimeEstimate"),
                              "ca.ListNumber",
                              "ca.HearingDuration",
                              date_format(col("ca.StartTime"), 'HH:mm').alias("StartTime"),
                              "ca.HearingTypeDesc",
                              "ca.HearingTypeEst",
                              "ca.DoNotUse",
                              "ca.ListAdjudicatorId",
                              "ca.ListAdjudicatorSurname",
                              "ca.ListAdjudicatorForenames",
                              "ca.ListAdjudicatorNote",
                              "ca.ListAdjudicatorTitle",
                              "ca.ListName",
                            #   "ca.ListStartTime",
                              date_format(col("ca.ListStartTime"), 'h:mm a').alias("ListStartTime"),
                              "ca.ListTypeDesc",
                              "ca.ListType",
                              "ca.DoNotUseListType",
                              "ca.CourtName",
                              "ca.DoNotUseCourt",
                              "ca.HearingCentreDesc",
                              "ca.Chairman",
                              "ca.Position",
                              "ca.UpperTribJudge",
                              "ca.DesJudgeFirstTier",
                              "ca.JudgeFirstTier",
                              "ca.NonLegalMember",
                              "JudgeLabel1",
                              "JudgeLabel2",
                              "JudgeLabel3",
                              col("JudgeValue_full").alias("JudgeValue"),
                              col("adj1.JudgeValue").alias("Label1_JudgeValue"),
                              col("adj2.JudgeValue").alias("Label2_JudgeValue"),
                              col("adj3.JudgeValue").alias("Label3_JudgeValue"),
                              col("adj4.JudgeValue").alias("CourtClerkUsher")
                          )


    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_dfdairy_detail

# COMMAND ----------

@dlt.table(
    name="silver_dfdairy_detail",
    comment="Delta Live silver Table for dfdairy detail.",
    path=f"{silver_mnt}/silver_dfdairy_detail"
)
def silver_dfdairy_detail():
    appeals_df = dlt.read("bronze_appealcase_bfdiary_bftype").alias("df")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = appeals_df.join(flt_df, col("df.CaseNo") == col("flt.CaseNo"), "inner").select("df.*")

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_history_detail

# COMMAND ----------

@dlt.table(
    name="silver_history_detail",
    comment="Delta Live silver Table for history detail.",
    path=f"{silver_mnt}/silver_history_detail"
)
def silver_history_detail():
    appeals_df = dlt.read("bronze_appealcase_history_users").alias("hu")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    # Select only one row for fileLocation (latest for HistType = 6)
    file_location_df = appeals_df.filter((col("HistType") == 6))\
        .withColumn("row_num", row_number().over(Window.partitionBy("CaseNo").orderBy(col("HistDate").desc())))\
                            .filter(col("row_num") == 1)\
                            .drop("row_num")\
        .select(col("CaseNo"), col("HistoryComment").alias("fileLocation"))

    # display(file_location_df)

    # Select only one row for lastDocument (latest for HistType = 16)
    last_document_df = appeals_df.filter((col("HistType") == 16))\
                            .withColumn("row_num", row_number().over(Window.partitionBy("CaseNo").orderBy(col("HistDate").desc())))\
                            .filter(col("row_num") == 1)\
                            .drop("row_num")\
                            .select(col("CaseNo"), col("HistoryComment").alias("lastDocument"))
    # display(last_document_df)

    # Join the filtered results back to the main appeals_df
    result_df = appeals_df.join(flt_df, "CaseNo", "inner")\
        .join(file_location_df, "CaseNo", "left")\
        .join(last_document_df, "CaseNo", "left")\
        .select(
            "HistoryId",
            "CaseNo",
            "HistDate",
            "fileLocation",
            "lastDocument",
            "HistType",
            "HistoryComment",
            "StatusId",
            "DeletedByUser",
            "UserName",
            "UserType",
            "Fullname",
            "Extension",
            "DoNotUse",
            when(col("HistType") == 1, "Adjournment")
                .when(col("HistType") == 2, "Adjudicator Process")
                .when(col("HistType") == 3, "Bail Process")
                .when(col("HistType") == 4, "Change of Address")
                .when(col("HistType") == 5, "Decisions")
                .when(col("HistType") == 6, "File Location")
                .when(col("HistType") == 7, "Interpreters")
                .when(col("HistType") == 8, "Issue")
                .when(col("HistType") == 9, "Links")
                .when(col("HistType") == 10, "Listing")
                .when(col("HistType") == 11, "SIAC Process")
                .when(col("HistType") == 12, "Superior Court")
                .when(col("HistType") == 13, "Tribunal Process")
                .when(col("HistType") == 14, "Typing")
                .when(col("HistType") == 15, "Parties edited")
                .when(col("HistType") == 16, "Document")
                .when(col("HistType") == 17, "Document Received")
                .when(col("HistType") == 18, "Manual Entry")
                .when(col("HistType") == 19, "Interpreter")
                .when(col("HistType") == 20, "File Detail Changed")
                .when(col("HistType") == 21, "Dedicated hearing centre changed")
                .when(col("HistType") == 22, "File Linking")
                .when(col("HistType") == 23, "Details")
                .when(col("HistType") == 24, "Availability")
                .when(col("HistType") == 25, "Cancel")
                .when(col("HistType") == 26, "De-allocation")
                .when(col("HistType") == 27, "Work Pattern")
                .when(col("HistType") == 28, "Allocation")
                .when(col("HistType") == 29, "De-Listing")
                .when(col("HistType") == 30, "Statutory Closure")
                .when(col("HistType") == 31, "Provisional Destruction Date")
                .when(col("HistType") == 32, "Destruction Date")
                .when(col("HistType") == 33, "Date of Service")
                .when(col("HistType") == 34, "IND Interface")
                .when(col("HistType") == 35, "Address Changed")
                .when(col("HistType") == 36, "Contact Details")
                .when(col("HistType") == 37, "Effective Date")
                .when(col("HistType") == 38, "Centre Changed")
                .when(col("HistType") == 39, "Appraisal Added")
                .when(col("HistType") == 40, "Appraisal Removed")
                .when(col("HistType") == 41, "Costs Deleted")
                .when(col("HistType") == 42, "Credit/Debit Card Payment received")
                .when(col("HistType") == 43, "Bank Transfer Payment received")
                .when(col("HistType") == 44, "Chargeback Taken")
                .when(col("HistType") == 45, "Remission request Rejected")
                .when(col("HistType") == 46, "Refund Event Added")
                .when(col("HistType") == 47, "Write-Off, Strikeout Write-Off or Threshold Write-off Event Added")
                .when(col("HistType") == 48, "Aggregated Payment Taken")
                .when(col("HistType") == 49, "Case Created")
                .when(col("HistType") == 50, "Tracked Document")
                .when(col("HistType") == 51, "Withdrawal")
            .alias("HistTypeDescription")
        )

    return result_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_link_detail

# COMMAND ----------

@dlt.table(
    name="silver_link_detail",
    comment="Delta Live silver Table for list detail.",
    path=f"{silver_mnt}/silver_link_detail"
)
def silver_link_detail():
    appeals_df = dlt.read("bronze_appealcase_link_linkdetail").alias("ld")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = appeals_df.join(flt_df, col("ld.CaseNo") == col("flt.CaseNo"), "inner").select(
        "ld.CaseNo",
        "ld.LinkNo",
        "ld.LinkDetailComment",
        "ld.LinkName",
        "ld.LinkForeNames",
        "ld.LinkTitle"
    )

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_status_detail

# COMMAND ----------

@dlt.table(
    name="silver_status_detail",
    comment="Delta Live silver Table for status detail.",
    path=f"{silver_mnt}/silver_status_detail"
)
def silver_status_detail():
    appeals_df = dlt.read("bronze_appealcase_status_sc_ra_cs").alias("st")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    max_statusid = appeals_df.groupBy("CaseNo").agg(max("StatusId").alias("StatusId"))

    df_currentstatus = appeals_df.alias("a").join(max_statusid.alias("b"), (col("a.CaseNo") == col("b.CaseNo")) & (col("a.StatusId") == col("b.StatusId"))) \
    .select(col("a.CaseStatus"), col("CaseStatusDescription"), col("a.CaseNo"))

    joined_df = appeals_df.join(flt_df, col("st.CaseNo") == col("flt.CaseNo"), "inner") \
                          .join(df_currentstatus.alias("mx"), (col("st.CaseNo") == col("mx.CaseNo")), "left") \
                          .select(
                              "st.StatusId",
                              "st.CaseNo",
                              "st.CaseStatus",
                              "st.DateReceived",
                              "st.StatusDetailAdjudicatorId",
                              when(col("st.Allegation") == 1, "No right of appeal")
                              .when(col("st.Allegation") == 2, "Out of time")
                              .alias("Allegation"),
                              "st.KeyDate",
                              "st.MiscDate1",
                              "st.Notes1",
                              when(col("st.Party") == 1, "Appellant")
                              .when(col("st.Party") == 2, "Respondent")
                              .when(col("st.Party") == 0, "")
                              .alias("Party"),
                              when((col("st.InTime") == 1) & (col("st.CaseStatus") == 17), "Yes")
                              .when((col("st.InTime") == 2) & (col("st.CaseStatus") == 17), "No")
                              .when((col("st.InTime") == 0) & (col("st.CaseStatus") == 17), "")
                              .alias("InTime"),
                              "st.MiscDate2",
                              "st.MiscDate3",
                              "st.Notes2",
                              "st.DecisionDate",
                              "st.Outcome",
                              "st.Promulgated",
                              when(col("st.InterpreterRequired") == 0, "Zero")
                              .when(col("st.InterpreterRequired") == 1, "One")
                              .when(col("st.InterpreterRequired") == 2, "Two")
                              .when(col("st.InterpreterRequired") == 3, "Zero")
                              .alias("InterpreterRequired"),
                              "st.AdminCourtReference",
                              "st.UKAITNo",
                              when(col("st.FC") == True, "checked").otherwise("disabled").alias("FC"),
                              when(col("st.VideoLink") == True, "checked").otherwise("disabled").alias("VideoLink"),
                              # "st.Process",
                              when(col("st.Process") == 1, "On Paper")
                                .when(col("st.Process") == 2, "Oral")
                                .when(col("st.Process") == 3, "By Telephone")
                                .when((col("st.Process") == 0) | (col("st.Process").isNull()), "")
                                .otherwise("").alias("Process"),
                              "st.COAReferenceNumber",
                              "st.HighCourtReference",
                              "st.OutOfTime",
                              when(col("st.ReconsiderationHearing") == True, "checked").otherwise("disabled").alias("ReconsiderationHearing"),
                              when(col("st.DecisionSentToHO") == 1, "Yes").when(col("st.DecisionSentToHO") == 2, "No").otherwise("").alias("DecisionSentToHO"),
                              "st.DecisionSentToHODate",
                              when(col("st.MethodOfTyping") == 1, "IA Typed")
                              .when(col("st.MethodOfTyping") == 2, "Self Type")
                              .when(col("st.MethodOfTyping") == 3, "3rd Party")
                              .alias("MethodOfTyping"),
                              when(col("st.CourtSelection").isNull(), " ") \
                              .when(col("st.CourtSelection") == 0, " ") \
                              .when(col("st.CourtSelection") == 1, "Court of Appeal") \
                              .when(col("st.CourtSelection") == 2, "Court of Session") \
                              .when(col("st.CourtSelection") == 3, "Northern Ireland") \
                              .otherwise(" ").alias("CourtSelection"),
                              "st.DecidingCentre",
                              when(col("st.Tier").isNull(), "") \
                              .when(col("st.Tier") == 0, "") \
                              .when(col("st.Tier") == 1, "First Tier") \
                              .when(col("st.Tier") == 2, "Upper Tier") \
                                .otherwise("").alias("Tier"),
                              when(col("st.RemittalOutcome").isNull(), "")
                              .when(col("st.RemittalOutcome") == 0, "")
                              .when(col("st.RemittalOutcome") == 1, "YES")
                              .when(col("st.RemittalOutcome") == 2, "NO")
                              .alias("RemittalOutcome"),
                              when(col("st.UpperTribunalAppellant").isNull(), "")
                              .when(col("st.UpperTribunalAppellant") == 0, "")
                              .when(col("st.UpperTribunalAppellant") == 1, "Appellant")
                              .when(col("st.UpperTribunalAppellant") == 2, "Respondent")
                              .alias("UpperTribunalAppellant"),
                              "st.ListRequirementTypeId",
                              "st.UpperTribunalHearingDirectionId",
                              when((col("st.CaseStatus") == 17) & (col("st.ApplicationType") == 1), "Adjournment")
                                .when((col("st.CaseStatus") == 17) & (col("st.ApplicationType") == 2), "Withdrawal")
                                .when((col("st.CaseStatus") == 17) & (col("st.ApplicationType") == 0), "")
                                .otherwise(" ").alias("ApplicationType"),
                              "st.NoCertAwardDate",
                              "st.CertRevokedDate",
                              "st.WrittenOffFileDate",
                              "st.ReferredEnforceDate",
                              when(col("st.CaseStatus") == 17, col("st.Letter1Date")).otherwise(lit(None)).alias("Letter1Date"),
                              when(col("st.CaseStatus") == 17, col("st.Letter2Date")).otherwise(lit(None)).alias("Letter2Date"),
                              "st.Letter3Date",
                              "st.ReferredFinanceDate",
                              "st.WrittenOffDate",
                              "st.CourtActionAuthDate",
                              "st.BalancePaidDate",
                              "st.WrittenReasonsRequestedDate",
                              "st.TypistSentDate",
                              "st.TypistReceivedDate",
                              "st.WrittenReasonsSentDate",
                              when(
                                  col("st.ExtemporeMethodOfTyping").isNull(), ""
                              ).when(
                                  col("st.ExtemporeMethodOfTyping") == "0", ""
                              ).when(
                                  col("st.ExtemporeMethodOfTyping") == "1", "IA typed"
                              ).when(
                                  col("st.ExtemporeMethodOfTyping") == "2", "Self typed"
                              ).when(
                                  col("st.ExtemporeMethodOfTyping") == "3", "3rd party typed"
                              ).when(
                                  col("st.ExtemporeMethodOfTyping") == "4", "Unknown"
                              ).otherwise(col("st.ExtemporeMethodOfTyping")).alias("ExtemporeMethodOfTyping"),
                              when(col("st.Extempore") == True, "enabled").otherwise("disabled").alias("Extempore"),
                              when(col("st.DecisionByTCW") == True, "enabled").otherwise("disabled").alias("DecisionByTCW"),
                              "st.InitialHearingPoints",
                              "st.FinalHearingPoints",
                              "st.HearingPointsChangeReasonId",
                              "st.OtherCondition",
                              "st.OutcomeReasons",
                              "st.AdditionalLanguageId",
                              when(col("st.CostOrderAppliedFor") == True, "enabled").otherwise("disabled").alias("CostOrderAppliedFor"),
                              "st.HearingCourt",
                              "st.CaseStatusDescription",
                              "st.DoNotUseCaseStatus",
                              "st.CaseStatusHearingPoints",
                              "st.ContactStatus",
                              "st.SCCourtName",
                              "st.SCAddress1",
                              "st.SCAddress2",
                              "st.SCAddress3",
                              "st.SCAddress4",
                              "st.SCAddress5",
                              "st.SCPostcode",
                              "st.SCTelephone",
                              "st.SCForenames",
                              "st.SCTitle",
                              "st.ReasonAdjourn",
                              "st.DoNotUseReason",
                              "st.LanguageDescription",
                              "st.DoNotUseLanguage",
                              "st.DecisionTypeDescription",
                              "st.DeterminationRequired",
                              "st.DoNotUse",
                              "st.State",
                              "st.BailRefusal",
                              "st.BailHOConsent",
                              "st.StatusDetailAdjudicatorSurname",
                              "st.StatusDetailAdjudicatorForenames",
                              "st.StatusDetailAdjudicatorTitle",
                              "st.StatusDetailAdjudicatorNote",
                              "st.DeterminationByJudgeSurname",
                              "st.DeterminationByJudgeForenames",
                              "st.DeterminationByJudgeTitle",
                              col("st.CaseStatusDescription").alias("CurrentStatus"),
                              col("st.AdjournmentParentStatusId"),
                              when(col("st.IRISStatusOfCase") == 1, "Adjudicator Appeal")
                              .when(col("st.IRISStatusOfCase") == 10, "Preliminary Issue")
                              .when(col("st.IRISStatusOfCase") == 11, "Scottish Forfeiture")
                              .when(col("st.IRISStatusOfCase") == 12, "Tribunal Appeal")
                              .when(col("st.IRISStatusOfCase") == 14, "Tribunal Direct")
                              .when(col("st.IRISStatusOfCase") == 18, "Bail Renewal")
                              .when(col("st.IRISStatusOfCase") == 19, "Bail Variation")
                              .when(col("st.IRISStatusOfCase") == 21, "Tribunal Review")
                              .when(col("st.IRISStatusOfCase") == 4, "Bail Application")
                              .when(col("st.IRISStatusOfCase") == 5, "Direct to Divisional Court")
                              .when(col("st.IRISStatusOfCase") == 6, "Forfeiture")
                              .when(col("st.IRISStatusOfCase") == 9, "Paper Case")
                              .otherwise(None)
                              .alias("IRISStatusOfCase"),
                              col("HearingCentre"),
                              col("st.ListTypeId"),
                              col("ListTypeDescription"),
                              col("st.HearingTypeId"),
                              col("HearingTypeDescription"),
                              col("st.Judiciary1Id"),
                              col("st.Judiciary1Name"),
                              col("st.Judiciary2Id"),
                              col("st.Judiciary2Name"),
                              col("st.Judiciary3Id"),
                              col("st.Judiciary3Name")
                          )            

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_appealcategory_detail

# COMMAND ----------

@dlt.table(
    name="silver_appealcategory_detail",
    comment="Delta Live silver Table for status detail.",
    path=f"{silver_mnt}/silver_appealcategory_detail"
)
def silver_appealcategory_detail():
    appeals_df = dlt.read("bronze_appealcase_appealcatagory_catagory").alias("ac")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = appeals_df.join(flt_df, col("ac.CaseNo") == col("flt.CaseNo"), "inner").select("ac.*")

    
    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_case_detail

# COMMAND ----------

@dlt.table(
    name="silver_case_detail",
    comment="Delta Live silver Table for case detail.", 
    path=f"{silver_mnt}/silver_case_detail"
)
def silver_case_detail():
    case_df = dlt.read("bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at").alias("case")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = case_df.join(flt_df, col("case.CaseNo") == col("flt.CaseNo"), "inner").select(
        col("case.CaseNo").alias("CaseNo"),
        col("case.CaseFeeSummaryId").alias("CaseFeeSummaryId"),
        col("case.DatePosting1stTier").alias("DatePosting1stTier"),
        col("case.DatePostingUpperTier").alias("DatePostingUpperTier"),
        col("case.DateCorrectFeeReceived").alias("DateCorrectFeeReceived"),
        col("case.DateCorrectFeeDeemedReceived").alias("DateCorrectFeeDeemedReceived"),
        when(col("case.PaymentRemissionrequested") == 1, "YES").when(col("case.PaymentRemissionrequested") == 2, "NO").otherwise(None).alias("PaymentRemissionrequested"),
        when(col("case.PaymentRemissionGranted") == 1, "YES").when(col("case.PaymentRemissionGranted") == 2, "NO").otherwise(None).alias("PaymentRemissionGranted"),
        col("case.PaymentRemissionReason").alias("PaymentRemissionReason"),
        col("case.PaymentRemissionReasonNote").alias("PaymentRemissionReasonNote"),
        col("case.ASFReferenceNo").alias("ASFReferenceNo"),
        when(col("case.ASFReferenceNoStatus") == 1, "Unverified")
        .when(col("case.ASFReferenceNoStatus") == 2, "Verified")
        .when(col("case.ASFReferenceNoStatus") == 3, "Invalid")
        .otherwise("").alias("ASFReferenceNoStatus"),
        col("case.LSCReference").alias("LSCReference"),
        when(col("case.LSCStatus") == 1, "Unverified")
        .when(col("case.LSCStatus") == 2, "Verified")
        .when(col("case.LSCStatus") == 3, "Invalid")
        .otherwise(None).alias("LSCStatus"),
        when(col("case.LCPRequested") == 1, "YES").when(col("case.LCPRequested") == 2, "NO").otherwise("").alias("LCPRequested"),
        when(col("case.LCPOutcome") == 1, "Refused")
        .when(col("case.LCPOutcome") == 2, "Full Remission")
        .when(col("case.LCPOutcome") == 3, "Part Remission")
        .when(col("case.LCPOutcome") == 4, "Part Remission Deferred")
        .when(col("case.LCPOutcome") == 5, "Deferred")
        .otherwise(None).alias("LCPOutcome"),
        col("case.S17Reference").alias("S17Reference"),
        when(col("case.S17ReferenceStatus") == 1, "Unverified")
        .when(col("case.S17ReferenceStatus") == 2, "Verified")
        .when(col("case.S17ReferenceStatus") == 3, "Invalid")
        .otherwise("").alias("S17ReferenceStatus"),
        col("case.SubmissionURNCopied").alias("SubmissionURNCopied"),
        col("case.S20Reference").alias("S20Reference"),
        when(col("case.S20ReferenceStatus") == 1, "YES").when(col("case.S20ReferenceStatus") == 2, "NO").otherwise("").alias("S20ReferenceStatus"),
        # col("case.HomeOfficeWaiverStatus").alias("HomeOfficeWaiverStatus"),
        when(col("case.HomeOfficeWaiverStatus") == 1, "Unverified")
        .when(col("case.HomeOfficeWaiverStatus") == 2, "Verified")
        .when(col("case.HomeOfficeWaiverStatus") == 3, "Invalid")
        .otherwise("").alias("HomeOfficeWaiverStatus"),
        col("case.PaymentRemissionReasonDescription").alias("PaymentRemissionReasonDescription"),
        col("case.PaymentRemissionReasonDoNotUse").alias("PaymentRemissionReasonDoNotUse"),
        col("case.POUPortName").alias("POUPortName"),
        col("case.PortAddress1").alias("PortAddress1"),
        col("case.PortAddress2").alias("PortAddress2"),
        col("case.PortAddress3").alias("PortAddress3"),
        col("case.PortAddress4").alias("PortAddress4"),
        col("case.PortAddress5").alias("PortAddress5"),
        col("case.PortPostcode").alias("PortPostcode"),
        col("case.PortTelephone").alias("PortTelephone"),
        col("case.PortSdx").alias("PortSdx"),
        col("case.EmbassyLocation").alias("EmbassyLocation"),
        col("case.Embassy").alias("Embassy"),
        col("case.Surname").alias("Surname"),
        col("case.Forename").alias("Forename"),
        col("case.Title").alias("Title"),
        col("case.OfficialTitle").alias("OfficialTitle"),
        col("case.EmbassyAddress1").alias("EmbassyAddress1"),
        col("case.EmbassyAddress2").alias("EmbassyAddress2"),
        col("case.EmbassyAddress3").alias("EmbassyAddress3"),
        col("case.EmbassyAddress4").alias("EmbassyAddress4"),
        col("case.EmbassyAddress5").alias("EmbassyAddress5"),
        col("case.EmbassyPostcode").alias("EmbassyPostcode"),
        col("case.EmbassyTelephone").alias("EmbassyTelephone"),
        col("case.EmbassyFax").alias("EmbassyFax"),
        col("case.EmbassyEmail").alias("EmbassyEmail"),
        col("case.DoNotUseEmbassy").alias("DoNotUseEmbassy"),
        col("case.DedicatedHearingCentre").alias("DedicatedHearingCentre"),
        col("case.Prefix").alias("Prefix"),
        col("case.CourtType").alias("CourtType"),
        col("case.HearingCentreAddress1").alias("HearingCentreAddress1"),
        col("case.HearingCentreAddress2").alias("HearingCentreAddress2"),
        col("case.HearingCentreAddress3").alias("HearingCentreAddress3"),
        col("case.HearingCentreAddress4").alias("HearingCentreAddress4"),
        col("case.HearingCentreAddress5").alias("HearingCentreAddress5"),
        col("case.HearingCentrePostcode").alias("HearingCentrePostcode"),
        col("case.HearingCentreTelephone").alias("HearingCentreTelephone"),
        col("case.HearingCentreFax").alias("HearingCentreFax"),
        col("case.HearingCentreEmail").alias("HearingCentreEmail"),
        col("case.HearingCentreSdx").alias("HearingCentreSdx"),
        col("case.STLReportPath").alias("STLReportPath"),
        col("case.STLHelpPath").alias("STLHelpPath"),
        col("case.LocalPath").alias("LocalPath"),
        col("case.GlobalPath").alias("GlobalPath"),
        col("case.PouId").alias("PouId"),
        col("case.MainLondonCentre").alias("MainLondonCentre"),
        col("case.DoNotUse").alias("DoNotUse"),
        col("case.CentreLocation").alias("CentreLocation"),
        col("case.OrganisationId").alias("OrganisationId"),
        col("case.CaseSponsorName").alias("CaseSponsorName"),
        col("case.CaseSponsorForenames").alias("CaseSponsorForenames"),
        col("case.CaseSponsorTitle").alias("CaseSponsorTitle"),
        col("case.CaseSponsorAddress1").alias("CaseSponsorAddress1"),
        col("case.CaseSponsorAddress2").alias("CaseSponsorAddress2"),
        col("case.CaseSponsorAddress3").alias("CaseSponsorAddress3"),
        col("case.CaseSponsorAddress4").alias("CaseSponsorAddress4"),
        col("case.CaseSponsorAddress5").alias("CaseSponsorAddress5"),
        col("case.CaseSponsorPostcode").alias("CaseSponsorPostcode"),
        col("case.CaseSponsorTelephone").alias("CaseSponsorTelephone"),
        col("case.CaseSponsorEmail").alias("CaseSponsorEmail"),
        when(col("case.Authorised") == True, 'checked')
        .when(col("case.Authorised") == False, 'disabled')
        .otherwise('disabled').alias("Authorised"),
        col("case.AppealTypeDescription").alias("AppealTypeDescription"),
        col("case.AppealTypePrefix").alias("AppealTypePrefix"),
        col("case.AppealTypeNumber").alias("AppealTypeNumber"),
        col("case.AppealTypeFullName").alias("AppealTypeFullName"),
        col("case.AppealTypeCategory").alias("AppealTypeCategory"),
        col("case.AppealType").alias("AppealType"),
        col("case.AppealTypeDoNotUse").alias("AppealTypeDoNotUse"),
        col("case.AppealTypeDateStart").alias("AppealTypeDateStart"),
        col("case.AppealTypeDateEnd").alias("AppealTypeDateEnd")
    )

    return joined_df

# COMMAND ----------

@dlt.table(
    name="silver_statusdecisiontype_detail",
    comment="Delta Live silver Table for transaction detail.",
    path=f"{silver_mnt}/silver_statusdecisiontype_detail"
)
def silver_statusdecisiontype_detail():
    status_decision_df = dlt.read("bronze_status_decisiontype").alias("status")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = status_decision_df.join(flt_df, col("status.CaseNo") == col("flt.CaseNo"), "inner").select("status.*")
   
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_transaction_detail

# COMMAND ----------

@dlt.table(
    name="silver_transaction_detail",
    comment="Delta Live silver Table for transaction detail.",
    path=f"{silver_mnt}/silver_transaction_detail"
)
def silver_transaction_detail():
    status_decision_df = dlt.read("bronze_appealcase_t_tt_ts_tm").alias("tran")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')
                                                                               
    # Extract ReferringTransactionId values into a list
    referring_transaction_ids = status_decision_df.filter(
        col("TransactionTypeId").isin(6, 19)
    ).select("ReferringTransactionId").distinct().rdd.flatMap(lambda x: x).collect()

    # Updated FirstTierFee_df filter logic
    FirstTierFee_df = status_decision_df.filter(
        (col("TransactionTypeId") == 1) &
        (~col("TransactionStatusId").isin(referring_transaction_ids))
    ).groupBy("CaseNo").agg(sum("Amount").alias("FirstTierFee"))

    # Updated TotalFeeAdjustments filter logic
    TotalFeeAdjustments_df = status_decision_df.filter(
        (col("SumFeeAdjustment") == 1) &
        (~col("TransactionStatusId").isin(referring_transaction_ids))
    ).groupBy("CaseNo").agg(sum("Amount").alias("TotalFeeAdjustments"))


    # Updated TotalFeeDue filter logic
    TotalFeeDue_df = status_decision_df.filter(
        (col("SumTotalFee") == 1) &
        (~col("TransactionStatusId").isin(referring_transaction_ids))
    ).groupBy("CaseNo").agg(sum("Amount").alias("TotalFeeDue"))

    # Updated TotalPaymentsReceived filter logic
    TotalPaymentsReceived_df = status_decision_df.filter(
        (col("SumTotalPay") == 1) &
        (~col("TransactionStatusId").isin(referring_transaction_ids))
    ).groupBy("CaseNo").agg(sum("Amount").alias("TotalPaymentsReceived"))

    # Updated TotalPaymentAdjustments filter logic
    TotalPaymentAdjustments_df = status_decision_df.filter(
        (col("SumPayAdjustment") == 1) &
        (~col("TransactionStatusId").isin(referring_transaction_ids))
    ).groupBy("CaseNo").agg(sum("Amount").alias("TotalPaymentAdjustments"))

     # Updated TotalPaymentAdjustments filter logic
    BalanceDue_df = status_decision_df.filter(
        (col("SumBalance") == 1) &
        (~col("TransactionStatusId").isin(referring_transaction_ids))
    ).groupBy("CaseNo").agg(sum("Amount").alias("BalanceDue"))

    joined_df = status_decision_df.join(flt_df, col("tran.CaseNo") == col("flt.CaseNo"), "inner")\
                                  .join(FirstTierFee_df.alias("FirstTierFee"), "CaseNo", "left") \
                                  .join(TotalFeeAdjustments_df.alias("TotalFeeAdjustments"), "CaseNo", "left") \
                                  .join(TotalFeeDue_df.alias("TotalFeeDue"), "CaseNo", "left") \
                                  .join(TotalPaymentsReceived_df.alias("TotalPaymentsReceived"), "CaseNo", "left") \
                                  .join(TotalPaymentAdjustments_df.alias("TotalPaymentAdjustments"), "CaseNo", "left") \
                                  .join(BalanceDue_df.alias("BalanceDue"), "CaseNo", "left") \
        .select(
        "tran.TransactionId",
        "tran.CaseNo",
        "tran.TransactionTypeId",
        "tran.TransactionMethodId",
        "tran.TransactionDate",
        "tran.Amount",
        "tran.ClearedDate",
        "tran.TransactionStatusId",
        "tran.OriginalPaymentReference",
        "tran.PaymentReference",
        "tran.AggregatedPaymentURN",
        "tran.PayerForename",
        "tran.PayerSurname",
        "tran.LiberataNotifiedDate",
        "tran.LiberataNotifiedAggregatedPaymentDate",
        "tran.BarclaycardTransactionId",
        "tran.Last4DigitsCard",
        "tran.TransactionNotes",
        "tran.ExpectedDate",
        "tran.ReferringTransactionId",
        "tran.CreateUserId",
        "tran.LastEditUserId",
        "tran.TransactionDescription",
        "tran.InterfaceDescription",
        "tran.AllowIfNew",
        "tran.DoNotUse",
        "tran.SumFeeAdjustment",
        "tran.SumPayAdjustment",
        "tran.SumTotalFee",
        "tran.SumTotalPay",
        "tran.SumBalance",
        "tran.GridFeeColumn",
        "tran.GridPayColumn",
        "tran.IsReversal",
        "tran.TransactionStatusDesc",
        "tran.TransactionStatusIntDesc",
        "tran.DoNotUseTransactionStatus",
        "tran.TransactionMethodDesc",
        "tran.TransactionMethodIntDesc",
        "tran.DoNotUseTransactionMethod",
        when(col("tran.TransactionTypeId") != 3, col("tran.Amount")).otherwise(lit("")).alias("AmountDue"),
        when(col("tran.TransactionTypeId") == 3, col("tran.Amount")).otherwise(lit("")).alias("AmountPaid"),
        col("FirstTierFee.FirstTierFee").alias("FirstTierFee"),
        col("TotalFeeAdjustments.TotalFeeAdjustments").alias("TotalFeeAdjustments"),
        col("TotalFeeDue.TotalFeeDue").alias("TotalFeeDue"),
        col("TotalPaymentsReceived.TotalPaymentsReceived").alias("TotalPaymentsReceived"),
        col("TotalPaymentAdjustments.TotalPaymentAdjustments").alias("TotalPaymentAdjustments"),
        col("BalanceDue.BalanceDue").alias("BalanceDue")
    )

  
    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_humanright_detail

# COMMAND ----------

@dlt.table(
    name="silver_humanright_detail",
    comment="Delta Live silver Table for human rights detail.",
    path=f"{silver_mnt}/silver_humanright_detail"
)
def silver_humanright_detail():
    humanright_df = dlt.read("bronze_appealcase_ahr_hr").alias("hr")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = humanright_df.join(flt_df, col("hr.CaseNo") == col("flt.CaseNo"), "inner").select("hr.*")

    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_newmatter_detail"

# COMMAND ----------

@dlt.table(
    name="silver_newmatter_detail",
    comment="Delta Live silver Table for new matter detail.",
    path=f"{silver_mnt}/silver_newmatter_detail"
)
def silver_newmatter_detail():
    newmatter_df = dlt.read("bronze_appealcase_anm_nm").alias("nm")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = newmatter_df.join(flt_df, col("nm.CaseNo") == col("flt.CaseNo"), "inner").select(
        "nm.AppealNewMatterId",
        "nm.CaseNo",
        "nm.NewMatterId",
        "nm.AppealNewMatterNotes",
        "nm.DateReceived",
        "nm.DateReferredToHO",
        when(col("nm.HODecision") == 0, "").when(col("nm.HODecision") == 1, "Granted").when(col("nm.HODecision") == 2, "Refused").alias("HODecision"),
        "nm.DateHODecision",
        "nm.NewMatterDescription",
        "nm.NotesRequired",
        "nm.DoNotUse"
    )
 
    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_documents_detail

# COMMAND ----------

@dlt.table(
    name="silver_documents_detail",
    comment="Delta Live silver Table for documents detail.",
    path=f"{silver_mnt}/silver_documents_detail"
)
def silver_documents_detail():
    documents_df = dlt.read("bronze_appealcase_dr_rd").alias("doc")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = documents_df.join(flt_df, col("doc.CaseNo") == col("flt.CaseNo"), "inner").select("doc.*")
     
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : sliver_direction_detail

# COMMAND ----------

@dlt.table(
    name="sliver_direction_detail",
    comment="Delta Live silver Table for direction details.",
    path=f"{silver_mnt}/sliver_direction_detail"
)
def sliver_direction_detail():
    direction_df = dlt.read("bronze_appealcase_rsd_sd").alias("dir")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = direction_df.join(flt_df, col("dir.CaseNo") == col("flt.CaseNo"), "inner").select("dir.*")
       
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_reviewspecificdirection_detail

# COMMAND ----------

@dlt.table(
    name="silver_reviewspecificdirection_detail",
    comment="Delta Live silver Table for review-specific direction details.",
    path=f"{silver_mnt}/Silver_reviewspecificdirection_detail"
)
def Silver_reviewspecificdirection_detail():
    review_specific_direction_df = dlt.read("bronze_review_specific_direction").alias("rsd")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = review_specific_direction_df.join(flt_df, col("rsd.CaseNo") == col("flt.CaseNo"), "inner").select("rsd.*")
  
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_costaward_detail

# COMMAND ----------

# DBTITLE 1,silver_linkedcostaward_detail
@dlt.table(
    name="silver_linkedcostaward_detail",
    comment="Delta Live silver Table for cost award detail.",
    path=f"{silver_mnt}/silver_linkedcostaward_detail"
)
def silver_linkedcostaward_detail():
    costaward_df = dlt.read("bronze_cost_award_linked").alias("ca")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = costaward_df.join(flt_df, col("ca.CaseNo") == col("flt.CaseNo"), "inner").select(
        "ca.CostAwardId", 
        "ca.CaseNo", 
        "ca.LinkNo", 
        "ca.Name", 
        "ca.Forenames", 
        "ca.Title",
        "ca.DateOfApplication", 
        when(col("ca.TypeOfCostAward") == 1, "Fee Costs")
            .when(col("ca.TypeOfCostAward") == 2, "Wasted Costs")
            .when(col("ca.TypeOfCostAward") == 3, "Unreasonable Behaviour")
            .when(col("ca.TypeOfCostAward") == 4, "General Costs")
            .alias("TypeOfCostAward"), 
        when(col("ca.ApplyingParty") == 1, "Appellant")
            .when(col("ca.ApplyingParty") == 2, "Respondent")
            .when(col("ca.ApplyingParty") == 3, "Tribunal")
            .alias("ApplyingParty"), 
        when(col("ca.PayingParty") == 1, "Appellant")
            .when(col("ca.PayingParty") == 2, "Respondent")
            .when(col("ca.PayingParty") == 3, "Surety/Cautioner")
            .when(col("ca.PayingParty") == 4, "Interested Party")
            .when(col("ca.PayingParty") == 5, "Appellant Rep")
            .when(col("ca.PayingParty") == 6, "Respondent Rep")
            .alias("PayingParty"),
        "ca.MindedToAward", 
        "ca.ObjectionToMindedToAward", 
        when(col("ca.CostsAwardDecision") == 0, "Blank")
            .when(col("ca.CostsAwardDecision") == 1, "Granted")
            .when(col("ca.CostsAwardDecision") == 2, "Refused")
            .when(col("ca.CostsAwardDecision") == 3, "Interim")
            .alias("CostsAwardDecision"),
        "ca.DateOfDecision", 
        "ca.CostsAmount", 
        "ca.OutcomeOfAppeal", 
        "ca.AppealStage",
        "ca.AppealStageDescription"
    )

      
    return joined_df

# COMMAND ----------

# DBTITLE 1,silver_costaward_detail
@dlt.table(
    name="silver_costaward_detail",
    comment="Delta Live silver Table for cost award detail.",
    path=f"{silver_mnt}/silver_costaward_detail"
)
def silver_costaward_detail():
    costaward_df = dlt.read("bronze_cost_award").alias("ca")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = costaward_df.join(flt_df, col("ca.CaseNo") == col("flt.CaseNo"), "inner").select(
        "ca.CostAwardId", 
        "ca.CaseNo", 
        # "ca.LinkNo", 
        "ca.Name", 
        "ca.Forenames", 
        "ca.Title",
        "ca.DateOfApplication", 
        when(col("ca.TypeOfCostAward") == 1, "Fee Costs")
            .when(col("ca.TypeOfCostAward") == 2, "Wasted Costs")
            .when(col("ca.TypeOfCostAward") == 3, "Unreasonable Behaviour")
            .when(col("ca.TypeOfCostAward") == 4, "General Costs")
            .alias("TypeOfCostAward"), 
        when(col("ca.ApplyingParty") == 1, "Appellant")
            .when(col("ca.ApplyingParty") == 2, "Respondent")
            .when(col("ca.ApplyingParty") == 3, "Tribunal")
            .alias("ApplyingParty"), 
        when(col("ca.PayingParty") == 1, "Appellant")
            .when(col("ca.PayingParty") == 2, "Respondent")
            .when(col("ca.PayingParty") == 3, "Surety/Cautioner")
            .when(col("ca.PayingParty") == 4, "Interested Party")
            .when(col("ca.PayingParty") == 5, "Appellant Rep")
            .when(col("ca.PayingParty") == 6, "Respondent Rep")
            .alias("PayingParty"),
        "ca.MindedToAward", 
        "ca.ObjectionToMindedToAward", 
        when(col("ca.CostsAwardDecision") == 0, "Blank")
            .when(col("ca.CostsAwardDecision") == 1, "Granted")
            .when(col("ca.CostsAwardDecision") == 2, "Refused")
            .when(col("ca.CostsAwardDecision") == 3, "Interim")
            .alias("CostsAwardDecision"),
        "ca.DateOfDecision", 
        "ca.CostsAmount", 
        "ca.OutcomeOfAppeal", 
        "ca.AppealStage",
        "ca.AppealStageDescription"
    )

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_costorder_detail 

# COMMAND ----------

@dlt.table(
    name="silver_costorder_detail",
    comment="Delta Live silver Table for cost order detail.",
    path=f"{silver_mnt}/silver_costorder_detail"
)
def silver_costorder_detail():
    costorder_df = dlt.read("bronze_costorder").alias("co")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')



    joined_df = costorder_df.join(flt_df, col("co.CaseNo") == col("flt.CaseNo"), "inner").select(
        "co.CostOrderID",
        "co.CaseNo",
        # "co.AppealStageWhenApplicationMade",
        "co.DateOfApplication",
        # "co.AppealStageWhenDecisionMade",
        "co.OutcomeOfAppealWhereDecisionMade",
        "co.DateOfDecision",
        # "co.CostOrderDecision",
        "co.ApplyingRepresentativeId",
        when(col("co.ApplyingRepresentativeId") == 0, col("ApplyingRepresentativeNameCaseRep")).otherwise(col("ApplyingRepresentativeNameRep")).alias("ApplyingRepresentativeName"),
        # "co.ApplyingRepresentativeNameCaseRep",
        # "co.ApplyingRepresentativeNameRep",
        "co.OutcomeOfAppealWhereDecisionMadeDescription",
        when(col("co.AppealStageWhenApplicationMade") == 0, "Blank")
            .when(col("co.AppealStageWhenApplicationMade") == 1, "HCR")
            .when(col("co.AppealStageWhenApplicationMade") == 2, "HCR (Filter)")
            .when(col("co.AppealStageWhenApplicationMade") == 3, "IJ – Hearing (Recon)")
            .when(col("co.AppealStageWhenApplicationMade") == 4, "IJ – Paper (Recon)")
            .when(col("co.AppealStageWhenApplicationMade") == 5, "Panel – Legal (Recon)")
            .when(col("co.AppealStageWhenApplicationMade") == 6, "Panel – Legal / Non Legal")
            .alias("AppealStageWhenApplicationMade"),
        when(col("co.AppealStageWhenDecisionMade") == 0, "Blank")
            .when(col("co.AppealStageWhenDecisionMade") == 1, "HCR")
            .when(col("co.AppealStageWhenDecisionMade") == 2, "HCR (Filter)")
            .when(col("co.AppealStageWhenDecisionMade") == 3, "IJ – Hearing (Recon)")
            .when(col("co.AppealStageWhenDecisionMade") == 4, "IJ – Paper (Recon)")
            .when(col("co.AppealStageWhenDecisionMade") == 5, "Panel – Legal (Recon)")
            .when(col("co.AppealStageWhenDecisionMade") == 6, "Panel – Legal / Non Legal")
            .when(col("co.AppealStageWhenDecisionMade") == 7, "Permission to Appeal")
            .alias("AppealStageWhenDecisionMade"),
        when(col("co.CostOrderDecision") == 0, "Blank")
            .when(col("co.CostOrderDecision") == 1, "Granted")
            .when(col("co.CostOrderDecision") == 2, "Refused")
            .when(col("co.CostOrderDecision") == 3, "Withdrawn")
            .when(col("co.CostOrderDecision") == 4, "Not Valid Application")
            .alias("CostOrderDecision")
    )
    
    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_hearingpointschange_detail

# COMMAND ----------

@dlt.table(
    name="silver_hearingpointschange_detail",
    comment="Delta Live silver Table for hearing points change reason detail.",
    path=f"{silver_mnt}/silver_hearingpointschange_detail"
)
def silver_hearingpointschange_detail():
    hearingpointschange_df = dlt.read("bronze_hearing_points_change_reason").alias("hpc")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = hearingpointschange_df.join(flt_df, col("hpc.CaseNo") == col("flt.CaseNo"), "inner").select(
        "hpc.CaseNo",
        "hpc.StatusId",
        "hpc.HearingPointsChangeReasonId",
        "hpc.Description",
        col("hpc.DoNotUse").alias("HearingPointsChangeDoNotUse")
    )

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation :silver_hearingpointshistory_detail

# COMMAND ----------

@dlt.table(
    name="silver_hearingpointshistory_detail",
    comment="Delta Live silver Table for hearing points history detail.",
    path=f"{silver_mnt}/silver_hearingpointshistory_detail"
)
def silver_hearingpointshistory_detail():
    hearingpointshistory_df = dlt.read("bronze_hearing_points_history").alias("hph")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = hearingpointshistory_df.join(flt_df, col("hph.CaseNo") == col("flt.CaseNo"), "inner").select("hph.*")
       
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : silver_appealtypecategory_detail

# COMMAND ----------

@dlt.table(
    name="silver_appealtypecategory_detail",
    comment="Delta Live silver Table for appeal type category detail.",
    path=f"{silver_mnt}/silver_appealtypecategory_detail"
)
def silver_appealtypecategory_detail():
    appealtypecategory_df = dlt.read("bronze_appeal_type_category").alias("atc")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = appealtypecategory_df.join(flt_df, col("atc.CaseNo") == col("flt.CaseNo"), "inner").select("atc.*")

    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: silver_appealgrounds_detail

# COMMAND ----------

@dlt.table(
    name="silver_appealgrounds_detail",
    comment="Delta Live silver Table for appeal ground  detail.",
    path=f"{silver_mnt}/silver_appealgrounds_detail"
)
def silver_appealgrounds_detail():
    appealtypecategory_df = dlt.read("bronze_appeal_grounds").alias("agt")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = appealtypecategory_df.join(flt_df, col("agt.CaseNo") == col("flt.CaseNo"), "inner").select("agt.*")
  
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: silver_required_incompatible_adjudicator

# COMMAND ----------

@dlt.table(
    name="silver_required_incompatible_adjudicator",
    comment="Delta Live silver Table for appeal ground  detail.",
    path=f"{silver_mnt}/silver_required_incompatible_adjudicator"
)
def silver_required_incompatible_adjudicator():
    appealtypecategory_df = dlt.read("bronze_required_incompatible_adjudicator").alias("adj")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = appealtypecategory_df.join(flt_df, col("adj.CaseNo") == col("flt.CaseNo"), "inner").select("adj.*")

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: silver_case_adjudicator

# COMMAND ----------

@dlt.table(
    name="silver_case_adjudicator",
    comment="Delta Live silver Table for appeal ground  detail.",
    path=f"{silver_mnt}/silver_case_adjudicator"
)
def silver_case_adjudicator():
    appealtypecategory_df = dlt.read("bronze_case_adjudicator").alias("adj")
    flt_df = spark.read.table("ariadm_active_appeals.stg_segmentation_states").alias('flt')

    joined_df = appealtypecategory_df.join(flt_df, col("adj.CaseNo") == col("flt.CaseNo"), "inner").select("adj.*")
  
    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver DLT staging table for gold Transformation

# COMMAND ----------

# DBTITLE 1,Secret Retrieval for Database Connection
secret = dbutils.secrets.get(KeyVault_name, f"CURATED-{env_name}-SAS-TOKEN")

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

# DBTITLE 1,Function: Format Dates for UploadBlob Storage
import json
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
 
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
    
# Load templates
template_paths_and_names = [
    (f"{html_mnt}/Appeals/appeals-no-js-v5-template.html", "html_template"),
    (f"{html_mnt}/Appeals/dependentsdetails/Dependentsdetailstemplate.html", "Dependentsdetailstemplate"),
    (f"{html_mnt}/Appeals/paymentdetails/PaymentDetailstemplate.html", "PaymentDetailstemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailPreliminaryIssueTemplate.html", "StatusDetailPreliminaryIssueTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailCaseManagementReviewTemplate.html", "StatusDetailCaseManagementReviewTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailAppellateCourtTemplate.html", "StatusDetailAppellateCourtTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailHighCourtReviewTemplate.html", "StatusDetailHighCourtReviewTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailHighCourtReviewFilterTemplate.html", "StatusDetailHighCourtReviewFilterTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailImmigrationJudgeHearingTemplate.html", "StatusDetailImmigrationJudgeHearingTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailPanelHearingLegalTemplate.html", "StatusDetailPanelHearingLegalTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailPermissiontoAppealTemplate.html", "StatusDetailPermissiontoAppealTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailReviewofCostOrderTemplate.html", "StatusDetailReviewOfCostOrderTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailFirstTierHearingTemplate.html", "StatusDetailFirstTierHearingTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailFirstTierPaperTemplate.html", "StatusDetailFirstTierPaperTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailFirstTierPermissionApplicationTemplate.html", "StatusDetailFirstTierPermissionApplicationTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailUpperTribunalPermissionApplicationTemplate.html", "StatusDetailUpperTribunalPermissionApplicationTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailUpperTribunalOralPermissionApplicationTemplate.html", "StatusDetailUpperTribunalOralPermissionApplicationTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailUpperTribunalHearingTemplate.html", "StatusDetailUpperTribunalHearingTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailUpperTribunalHearingContinuanceTemplate.html", "StatusDetailUpperTribunalHearingContinuanceTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailUpperTribunalOralPermissionHearingTemplate.html", "StatusDetailUpperTribunalOralPermissionHearingTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailPTADirecttoAppellateCourtTemplate.html", "StatusDetailPTADirecttoAppellateCourtTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailSetAsideApplicationTemplate.html", "StatusDetailSetAsideApplicationTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailJudicialReviewPermissionApplicationTemplate.html", "StatusDetailJudicialReviewPermissionApplicationTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailJudicialReviewHearingTemplate.html", "StatusDetailJudicialReviewHearingTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailOnHoldChargebackTakenTemplate.html", "StatusDetailOnHoldChargebackTakenTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailClosedFeeNotPaidTemplate.html", "StatusDetailClosedFeeNotPaidTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailCaseClosedFeeOutstandingTemplate.html", "StatusDetailCaseClosedFeeOutstandingTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/StatusDetailMigrationTemplate.html", "StatusDetailMigrationTemplate"),
    (f"{html_mnt}/Appeals/statusdetails/DefaultStatusDetail.html", "DefaultStatusDetail")  

]

templates = {name: "".join([row.value for row in spark.read.text(path).collect()]) for path, name in template_paths_and_names}


# COMMAND ----------

# DBTITLE 1,Function: Generate HTML with Dynamic Row and Template Data
# Modify the UDF to accept a row object and templates
def generate_html(row, templates=templates):
    try:
        html_template = templates["html_template"]
        Dependentsdetailstemplate = templates["Dependentsdetailstemplate"]
        PaymentDetailstemplate = templates["PaymentDetailstemplate"]

        #Convert row to a dictionary
        row_dict = row.asDict()

        # Define the fields that need date formatting
        # date_fields = {"DateApplicationLodged", "DateOfApplicationDecision"}  # Add other timestamp fields if needed

        date_fields = [
            "DateApplicationLodged", "DateOfApplicationDecision", "DateLodged", "DateReceived", "DateOfIssue",
            "TransferOutDate", "RemovalDate", "DeportationDate", "ProvisionalDestructionDate", "NoticeSentDate",
            "CertifiedDate", "CertifiedRecordedDate", "ReferredToJudgeDate", "StatutoryClosureDate", "DateReinstated",
            "AppellantBirthDate", "dateCorrectFeeReceived", "DateCorrectFeeDeemedReceived","DateServed","DateAppealReceived","BFDate"
        ]

    

        # Create dynamic placeholder replacements
        replacements = {
            f"{{{{{key}}}}}": format_date_iso(value) if key in date_fields and value is not None else str(value) if value is not None else ""
            for key, value in row_dict.items()
        }

        # # Create placeholder replacements dynamically
        # replacements = {f"{{{{{key}}}}}": str(value) if value is not None else "" for key, value in row_dict.items()}

        # Replace placeholders in the template
        for placeholder, value in replacements.items():
            html_template = html_template.replace(placeholder, value)

       
        
        # Replace placeholders in the template with row data
        replacements = {
            "{{CaseNo}}": str(row.CaseNo),
            "{{AdditionalGroundsPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\" style=\"text-align:center\"></td><td id=\"midpadding\">{AdditionalGrounds.AppealTypeDescription}</td></tr>"
                for AdditionalGrounds in (row.AppealGroundsDetails or [])) or '<tr><td id="midpadding"></td><td id="midpadding"></td></tr>',
            "{{LinkedFilesPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\"></td><td id=\"midpadding\">{str(row.CaseNo)}</td><td id=\"midpadding\">{str(row.AppellantName)}, {str(row.AppellantForenames)} ({str(row.AppellantTitle)})</td><td id=\"midpadding\">{str(Link.LinkDetailComment)}</td></tr>"
                for i, Link in enumerate(row.LinkedCaseDetails or [])
            ),
            "{{PaymentEventsSummaryPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{format_date(transaction.TransactionDate)}</td><td id=\"midpadding\">{transaction.TransactionDescription}</td><td id=\"midpadding\">{transaction.TransactionStatusDesc}</td><td id=\"midpadding\">{transaction.AmountDue}</td><td id=\"midpadding\">{transaction.AmountPaid}</td><td id=\"midpadding\">{format_date(transaction.ClearedDate)}</td><td id=\"midpadding\">{transaction.PaymentReference}</td><td id=\"midpadding\">{transaction.AggregatedPaymentURN}</td></tr>"
                for i, transaction in enumerate(row.TransactionDetails or [])
            ),
            "{{CostorderdetailsPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{costorder.AppealStageWhenApplicationMade}</td><td id=\"midpadding\">{format_date(costorder.DateOfApplication)}</td><td id=\"midpadding\">{costorder.AppealStageWhenDecisionMade}</td><td id=\"midpadding\">{costorder.OutcomeOfAppealWhereDecisionMadeDescription}</td><td id=\"midpadding\">{format_date(costorder.DateOfDecision)}</td><td id=\"midpadding\">{costorder.CostOrderDecision}</td><td id=\"midpadding\">{costorder.ApplyingRepresentativeName}</td></tr>"
                for i, costorder in enumerate(row.CostOrderDetails or [])
            ),
            "{{MaintainCostAwardsPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{row.CaseNo}</td><td id=\"midpadding\">{costaward.Name},{costaward.Forenames}({costaward.Title})</td><td id=\"midpadding\">{costaward.AppealStageDescription}</td><td id=\"midpadding\">{format_date(costaward.DateOfApplication)}</td><td id=\"midpadding\">{costaward.TypeOfCostAward}</td><td id=\"midpadding\">{costaward.ApplyingParty}</td><td id=\"midpadding\">{costaward.PayingParty}</td><td id=\"midpadding\">{costaward.MindedToAward}</td><td id=\"midpadding\">{costaward.ObjectionToMindedToAward}</td><td id=\"midpadding\">{costaward.CostsAwardDecision}</td><td id=\"midpadding\">{format_date(costaward.DateOfDecision)}</td><td id=\"midpadding\">{costaward.CostsAmount}</td></tr>"
                for i, costaward in enumerate(row.CostAwardDetails or [])
            ),
            "{{LinkedCasesviewonlyPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{row.CaseNo}</td><td id=\"midpadding\">{linkedcostaward.Name},{linkedcostaward.Forenames}({linkedcostaward.Title})</td><td id=\"midpadding\">{linkedcostaward.AppealStageDescription}</td><td id=\"midpadding\">{format_date(linkedcostaward.DateOfApplication)}</td><td id=\"midpadding\">{linkedcostaward.TypeOfCostAward}</td><td id=\"midpadding\">{linkedcostaward.ApplyingParty}</td><td id=\"midpadding\">{linkedcostaward.PayingParty}</td><td id=\"midpadding\">{linkedcostaward.MindedToAward}</td><td id=\"midpadding\">{linkedcostaward.ObjectionToMindedToAward}</td><td id=\"midpadding\">{linkedcostaward.CostsAwardDecision}</td><td id=\"midpadding\">{format_date(linkedcostaward.DateOfDecision)}</td><td id=\"midpadding\">{linkedcostaward.CostsAmount}</td></tr>"
                for i, linkedcostaward in enumerate(row.LinkedCostAwardDetails or [])
            ),
            "{{DocumentTrackingPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{doc.DocumentDescription}</td><td id=\"midpadding\">{format_date(doc.DateRequested)}</td><td id=\"midpadding\">{format_date(doc.DateRequired)}</td><td id=\"midpadding\">{format_date(doc.DateReceived)}</td><td id=\"midpadding\">{format_date(doc.RepresentativeDate)}</td><td id=\"midpadding\">{format_date(doc.POUDate)}</td><td id=\"midpadding\" style=\"text-align:center\">{'&#9745;' if doc.NoLongerRequired else '&#9744'}</td></tr>"
                for i, doc in enumerate(row.DocumentDetails or [])
            ),
            "{{NewMattersPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{newmatter.NewMatterDescription}</td><td id=\"midpadding\">{newmatter.AppealNewMatterNotes}</td><td id=\"midpadding\">{format_date(newmatter.DateReceived)}</td><td id=\"midpadding\">{format_date(newmatter.DateReferredToHO)}</td><td id=\"midpadding\">{newmatter.HODecision}</td><td id=\"midpadding\">{format_date(newmatter.DateHODecision)}</td></tr>"
                for i, newmatter in enumerate(row.NewMatterDetails or [])
            ),
            "{{HumanRightsPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{hr.HumanRightDescription}</td><td id=\"midpadding\" style=\"text-align:center\">✓</td></tr>"
                for i, hr in enumerate(row.HumanRightDetails or [])
            ),
            "{{AppealCategoriesPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{ac.CategoryDescription}</td><td id=\"midpadding\">{ac.Flag}</td><td id=\"midpadding\" style=\"text-align:center\">✓</td></tr>"
                for i, ac in enumerate(row.AppealCategoryDetails or [])
            ),
            "{{StatusPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{status.CaseStatusDescription}</td><td id=\"midpadding\">{format_date(status.LatestKeyDate)}</td><td id=\"midpadding\">{status.InterpreterRequired}</td><td id=\"midpadding\">{format_date(status.DecisionDate)}</td><td id=\"midpadding\">{status.DecisionTypeDescription}</td><td id=\"midpadding\">{format_date(status.Promulgated)}</td></tr>"
                for i, status in enumerate(sorted(row.TempCaseStatusDetails or [], key=lambda x: x.StatusId, reverse=True), start=1)
            ),
            "{{HistoryPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{format_date(history.HistDate)}</td><td id=\"midpadding\">{history.HistTypeDescription}</td><td id=\"midpadding\">{history.Fullname}</td><td id=\"midpadding\">{history.HistoryComment}</td><td id=\"midpadding\">{history.DeletedByUser}</td></tr>"
                for i, history in enumerate(sorted(row.HistoryDetails or [], key=lambda x: x.HistDate, reverse=True), start=1)
            ),
            "{{bfdiaryPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{format_date(bfdiary.BFDate)}</td><td id=\"midpadding\">{bfdiary.BFTypeDescription}</td><td id=\"midpadding\">{bfdiary.Entry}</td><td id=\"midpadding\">{format_date(bfdiary.DateCompleted)}</td></tr>"
                for i, bfdiary in enumerate(sorted(row.BFDairyDetails or [], key=lambda x: x.BFDate, reverse=True), start=1)
            ),
            "{{DependentsPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{dependent.AppellantName}</td><td id=\"midpadding\">{dependent.CaseAppellantRelationship}</td></tr>"
                for i, dependent in enumerate(row.DependentDetails or [])
            )
        }

        # "{{StatusPlaceHolder}}": "\n".join(
        #         f"<tr><td id=\"midpadding\">{status.CaseStatusDescription}</td><td id=\"midpadding\">{format_date(status.KeyDate)}</td><td id=\"midpadding\">{status.InterpreterRequired}</td><td id=\"midpadding\">{format_date(status.DecisionDate)}</td><td id=\"midpadding\">{status.DecisionTypeDescription}</td><td id=\"midpadding\">{format_date(status.Promulgated)}</td></tr>"
        #         for i, status in enumerate(row.StatusDetails or [])
        #     ),

       
        for key, value in replacements.items():
            html_template = html_template.replace(key, value)


        # Dependent details handling dynamically
        dependent_details_Code = ""
        if row.DependentDetails:
            for dependent in row.DependentDetails:
                dependent_dict = {f"{{{{{key}}}}}": str(getattr(dependent, key, "") or "") for key in [
                    "AppellantName", "AppellantForenames", "AppellantTitle", "CaseAppellantRelationship",
                    "AppellantAddress1", "AppellantAddress2", "AppellantAddress3", "AppellantAddress4",
                    "AppellantAddress5", "AppellantPostcode", "AppellantTelephone"
                ]}
                dependent_details_Code += Dependentsdetailstemplate
                for key, value in dependent_dict.items():
                    dependent_details_Code = dependent_details_Code.replace(key, value)
                dependent_details_Code += "\n"

            html_template = html_template.replace("{{Dependents}}", "Dependent Details Exists")
        else:
            empty_dict = {f"{{{{{key}}}}}": "" for key in [
                "AppellantName", "AppellantForenames", "AppellantTitle", "CaseAppellantRelationship",
                "AppellantAddress1", "AppellantAddress2", "AppellantAddress3", "AppellantAddress4",
                "AppellantAddress5", "AppellantPostcode", "AppellantTelephone"
            ]}
            dependent_details_Code = Dependentsdetailstemplate
            for key, value in empty_dict.items():
                dependent_details_Code = dependent_details_Code.replace(key, value)
            html_template = html_template.replace("{{Dependents}}", "")

        # Replace the placeholder with generated dependent details
        html_template = html_template.replace("{{DependentsdetailsPlaceHolder}}", dependent_details_Code)

        # Payment details
        payments_details_Code = ''
        nested_table_number = 99
        payment_number = 0
        if row.TransactionDetails:   
            for index, payment in enumerate(row.TransactionDetails, start=1):
                nested_table_number += 1
                payment_number += 1
                line = PaymentDetailstemplate.replace("{{TransactionDate}}", format_date_iso(payment.TransactionDate)) \
                                             .replace("{{TransactionDescription}}", str(payment.TransactionDescription)) \
                                             .replace("{{TransactionStatusDesc}}", str(payment.TransactionStatusDesc)) \
                                             .replace("{{LiberataNotifiedDate}}", format_date_iso(payment.LiberataNotifiedDate)) \
                                             .replace("{{BarclaycardTransactionId}}", str(payment.BarclaycardTransactionId)) \
                                             .replace("{{PaymentReference}}", str(payment.PaymentReference or '')) \
                                             .replace("{{OriginalPaymentReference}}", str(payment.OriginalPaymentReference or '')) \
                                             .replace("{{AggregatedPaymentURN}}", str(payment.AggregatedPaymentURN or '')) \
                                             .replace("{{payerSurname}}", str(payment.PayerSurname or '')) \
                                             .replace("{{TransactionNotes}}", str(payment.TransactionNotes or '')) \
                                             .replace("{{ExpectedDate}}", format_date_iso(payment.ExpectedDate)) \
                                             .replace("{{clearedDate}}", format_date_iso(payment.ClearedDate)) \
                                             .replace("{{Amount}}", str(payment.Amount or '')) \
                                             .replace("{{TransactionMethodDesc}}", str(payment.TransactionMethodDesc or '')) \
                                             .replace("{{Last4DigitsCard}}", str(payment.Last4DigitsCard or '')) \
                                             .replace("{{CreateUserId}}", str(payment.CreateUserId or '')) \
                                             .replace("{{LastEditUserId}}", str(payment.LastEditUserId or '')) \
                                             .replace("{{payerForename}}", str(payment.PayerForename or '')) \
                                             .replace("{{nested_table_number}}", str(nested_table_number)) \
                                             .replace("{{payment_number}}", str(payment_number))

                payments_details_Code += line + '\n'
        else:
            nested_table_number += 1
            payment_number += 1
            payments_details_Code = PaymentDetailstemplate.replace("{{TransactionDate}}", "") \
                                                          .replace("{{TransactionDescription}}", "") \
                                                          .replace("{{TransactionStatusDesc}}", "") \
                                                          .replace("{{LiberataNotifiedDate}}", "") \
                                                          .replace("{{BarclaycardTransactionId}}", "") \
                                                          .replace("{{PaymentReference}}", "") \
                                                          .replace("{{OriginalPaymentReference}}", "") \
                                                          .replace("{{AggregatedPaymentURN}}", "") \
                                                          .replace("{{payerSurname}}", "") \
                                                          .replace("{{TransactionNotes}}", "") \
                                                          .replace("{{ExpectedDate}}", "") \
                                                          .replace("{{clearedDate}}", "") \
                                                          .replace("{{Amount}}", "") \
                                                          .replace("{{TransactionMethodDesc}}", "") \
                                                          .replace("{{Last4DigitsCard}}", "") \
                                                          .replace("{{CreateUserId}}", "") \
                                                          .replace("{{LastEditUserId}}", "") \
                                                          .replace("{{payerForename}}", "") \
                                                          .replace("{{nested_table_number}}", str(nested_table_number)) \
                                                          .replace("{{payment_number}}", str(payment_number))
        html_template = html_template.replace(f"{{{{paymentdetailsPlaceHolder}}}}", payments_details_Code)

        # # CCS Updates
        statuscount = len(row.StatusDetails) if row.StatusDetails else 1 # 2
        status_details_code = ''
        nested_table_number = 30
        nested_tab_group_number = 1
        tabs_min_height = 200 + (statuscount * 600) if statuscount > 1 else 200
        print(tabs_min_height)
        content_height = 1000 + (statuscount * 440) if statuscount > 1 else 1000
        print(content_height)
        additional_tabs_size = 10 + ((statuscount-1) * 420) if statuscount > 1 else 200
        print(additional_tabs_size)
        nested_tabs_size = 10

        html_template = html_template.replace(f"{{{{tabs-min-height}}}}", str(tabs_min_height))
        html_template = html_template.replace(f"{{{{content-height}}}}", str(content_height))
        html_template = html_template.replace(f"{{{{additional-tabs-size}}}}", str(additional_tabs_size))


        # StatusDetails tabs
        nested_table_number = 999
        nested_tab_group_number = 999
        # for count in range(statuscount):
        if row.TempCaseStatusDetails:   
            # for index, SDP in enumerate(row.TempCaseStatusDetails, start=1):
            for index, SDP in enumerate(sorted(row.TempCaseStatusDetails or [], key=lambda x: x.StatusId, reverse=True), start=1):
                
                #Read relevent template
                casestatusTemplate = templates[SDP.HTMLName]

                nested_table_number += 1
                nested_tab_group_number += 1
                nested_tabs_size  = 10 if index == 1 else 400
                # 320
                line = casestatusTemplate.replace("{{nested_table_number}}", str(nested_table_number))  \
                                        .replace("{{nested_tab_group_number}}", str(nested_tab_group_number))  \
                                        .replace("{{nested_tabs_size}}", str(nested_tabs_size)) \
                                        .replace("{{CaseStatusDescription}}", str(SDP.CaseStatusDescription)) \
                                        .replace("{{KeyDate}}", format_date_iso(SDP.LatestKeyDate or '')) \
                                        .replace("{{InterpreterRequired}}", str(SDP.InterpreterRequired or '')) \
                                        .replace("{{AdjudicatorSurname}}", str(SDP.LatestAdjudicatorSurname or '')) \
                                        .replace("{{AdjudicatorForenames}}", str(SDP.LatestAdjudicatorForenames or ''))  \
                                        .replace("{{AdjudicatorTitle}}", str(SDP.LatestAdjudicatorTitle or '')) \
                                        .replace("{{AdjudicatorFullName}}", str(SDP.LatestAdjudicatorFullName or '')) \
                                        .replace("{{StatusDetailAdjudicatorFullName}}", str(SDP.StatusDetailAdjudicatorFullName or '')) \
                                        .replace("{{MiscDate2}}", format_date_iso(SDP.MiscDate2 or '')) \
                                        .replace("{{VideoLink}}", str(SDP.VideoLink or '')) \
                                        .replace("{{RemittalOutcome}}", str(SDP.RemittalOutcome or '')) \
                                        .replace("{{UpperTribunalAppellant}}", str(SDP.UpperTribunalAppellant or '')) \
                                        .replace("{{DecisionSentToHO}}", str(SDP.DecisionSentToHO or '')) \
                                        .replace("{{DecisionSentToHODate}}", format_date_iso(SDP.DecisionSentToHODate or '')) \
                                        .replace("{{InitialHearingPoints}}", str(SDP.InitialHearingPoints or '')) \
                                        .replace("{{FinalHearingPoints}}", str(SDP.FinalHearingPoints or '')) \
                                        .replace("{{HearingPointsChangeReasondesc}}", str(SDP.HearingPointsChangeReasondesc or '')) \
                                        .replace("{{CostOrderAppliedFor}}", str(SDP.CostOrderAppliedFor or '')) \
                                        .replace("{{HearingPointsChangeReasonId}}", format_date_iso(SDP.HearingPointsChangeReasonId or '')) \
                                        .replace("{{DecisionDate}}", format_date_iso(SDP.DecisionDate or '')) \
                                        .replace("{{DecisionByTCW}}", str(SDP.DecisionByTCW or '')) \
                                        .replace("{{Allegation}}", str(SDP.Allegation or '')) \
                                        .replace("{{DecidingCentre}}", format_date_iso(SDP.DecidingCentre or '')) \
                                        .replace("{{Process}}", str(SDP.Process or '')) \
                                        .replace("{{Tier}}", str(SDP.Tier or '')) \
                                        .replace("{{NoCertAwardDate}}", format_date_iso(SDP.NoCertAwardDate or '')) \
                                        .replace("{{WrittenOffDate}}", format_date_iso(SDP.WrittenOffDate or '')) \
                                        .replace("{{WrittenOffFileDate}}", format_date_iso(SDP.WrittenOffFileDate or '')) \
                                        .replace("{{ReferredEnforceDate}}", format_date_iso(SDP.ReferredEnforceDate or '')) \
                                        .replace("{{DeterminationByJudgeSurname}}", format_date_iso(SDP.DeterminationByJudgeSurname or '')) \
                                        .replace("{{DeterminationByJudgeForenames}}", format_date_iso(SDP.DeterminationByJudgeForenames or '')) \
                                        .replace("{{DeterminationByJudgeTitle}}", format_date_iso(SDP.DeterminationByJudgeTitle or '')) \
                                        .replace("{{DeterminationByJudgeFullName}}", str(SDP.DeterminationByJudgeFullName or '')) \
                                        .replace("{{MethodOfTyping}}", str(SDP.MethodOfTyping or '')) \
                                        .replace("{{adjournDecisionTypeDescription}}", str(SDP.adjournDecisionTypeDescription or '')) \
                                        .replace("{{DecisionTypeDescription}}", str(SDP.DecisionTypeDescription or '')) \
                                        .replace("{{Promulgated}}", format_date_iso(SDP.Promulgated or '')) \
                                        .replace("{{UKAITNo}}", str(SDP.UKAITNo or '')) \
                                        .replace("{{Extempore}}", str(SDP.Extempore or '')) \
                                        .replace("{{WrittenReasonsRequestedDate}}", format_date_iso(SDP.WrittenReasonsRequestedDate or '')) \
                                        .replace("{{TypistSentDate}}", format_date_iso(SDP.TypistSentDate or '')) \
                                        .replace("{{ExtemporeMethodOfTyping}}", str(SDP.ExtemporeMethodOfTyping or ''))  \
                                        .replace("{{TypistReceivedDate}}", format_date_iso(SDP.TypistReceivedDate or '')) \
                                        .replace("{{typingReasonsReceived}}", format_date_iso(SDP.WrittenReasonsRequestedDate or '')) \
                                        .replace("{{WrittenReasonsSentDate}}", format_date_iso(SDP.WrittenReasonsSentDate or '')) \
                                        .replace("{{DateReceived}}", format_date_iso(SDP.DateReceived or '')) \
                                        .replace("{{MiscDate1}}", format_date_iso(SDP.MiscDate1 or '')) \
                                        .replace("{{Party}}", str(SDP.Party or '')) \
                                        .replace("{{OutOfTime}}", format_date_iso(SDP.OutOfTime or '')) \
                                        .replace("{{adjournInTime}}", str(SDP.adjournInTime or '')) \
                                        .replace("{{Letter1Date}}", format_date_iso(SDP.Letter1Date or '')) \
                                        .replace("{{Letter2Date}}", format_date_iso(SDP.Letter2Date or '')) \
                                        .replace("{{Letter3Date}}", format_date_iso(SDP.Letter3Date or '')) \
                                        .replace("{{adjournNotes1}}", str(SDP.adjournNotes1 or '')) \
                                        .replace("{{DecisionDate}}", format_date_iso(SDP.DecisionDate or '')) \
                                        .replace("{{ListName}}", str(SDP.ListName or '')) \
                                        .replace("{{ListName}}", str(SDP.ListName or '')) \
                                        .replace("{{ListTypeDesc}}", str(SDP.ListTypeDesc or '')) \
                                        .replace("{{HearingTypeDesc}}", str(SDP.HearingTypeDesc or '')) \
                                        .replace("{{ListStartTime}}", str(SDP.ListStartTime or '')) \
                                        .replace("{{AdjudicatorId}}", str(SDP.LatestAdjudicatorId or '')) \
                                        .replace("{{LanguageId}}", str(SDP.LanguageDescription or '')) \
                                        .replace("{{ReferredFinanceDate}}", format_date_iso(SDP.ReferredFinanceDate or '')) \
                                        .replace("{{CourtActionAuthDate}}", format_date_iso(SDP.CourtActionAuthDate or '')) \
                                        .replace("{{BalancePaidDate}}", format_date_iso(SDP.BalancePaidDate or '')) \
                                        .replace("{{ReconsiderationHearing}}", str(SDP.ReconsiderationHearing or '')) \
                                        .replace("{{UpperTribunalHearingDirectionId}}", str(SDP.UpperTribunalHearingDirectionId or '')) \
                                        .replace("{{ListRequirementTypeId}}", str(SDP.ListRequirementTypeId or '')) \
                                        .replace("{{CourtSelection}}", str(SDP.CourtSelection or '')) \
                                        .replace("{{COAReferenceNumber}}", str(SDP.COAReferenceNumber or '')) \
                                        .replace("{{Notes2}}", str(SDP.Notes2 or '')) \
                                        .replace("{{HighCourtReference}}", str(SDP.HighCourtReference or '')) \
                                        .replace("{{AdminCourtReference}}", str(SDP.AdminCourtReference or '')) \
                                        .replace("{{HearingCourt}}", str(SDP.HearingCourt or '')) \
                                        .replace("{{ApplicationType}}", str(SDP.ApplicationType or '')) \
                                        .replace("{{adjournApplicationType}}", str(SDP.adjournApplicationType or '')) \
                                        .replace("{{adjournKeyDate}}", str(SDP.adjournKeyDate or '')) \
                                        .replace("{{adjournDateReceived}}", format_date_iso(SDP.adjournDateReceived or '')) \
                                        .replace("{{adjournmiscdate2}}", format_date_iso(SDP.adjournmiscdate2 or '')) \
                                        .replace("{{adjournParty}}", str(SDP.adjournParty or '')) \
                                        .replace("{{adjournInTime}}", str(SDP.adjournInTime or '')) \
                                        .replace("{{adjournLetter1Date}}", format_date_iso(SDP.adjournLetter1Date or '')) \
                                        .replace("{{adjournLetter2Date}}", format_date_iso(SDP.adjournLetter2Date or '')) \
                                        .replace("{{adjournAdjudicatorSurname}}", str(SDP.adjournAdjudicatorSurname or '')) \
                                        .replace("{{adjournAdjudicatorForenames}}", str(SDP.adjournAdjudicatorForenames or '')) \
                                        .replace("{{adjournAdjudicatorTitle}}", str(SDP.adjournAdjudicatorTitle or '')) \
                                        .replace("{{adjournAdjudicatorFullName}}", str(SDP.adjournAdjudicatorFullName or '')) \
                                        .replace("{{adjournNotes1}}", str(SDP.adjournNotes1 or '')) \
                                        .replace("{{adjournDecisionDate}}", format_date_iso(SDP.adjournDecisionDate or '')) \
                                        .replace("{{adjournPromulgated}}", format_date_iso(SDP.adjournPromulgated or '')) \
                                        .replace("{{HearingCentreDesc}}", str(SDP.HearingCentreDesc or '')) \
                                        .replace("{{CourtName}}", str(SDP.CourtName or '')) \
                                        .replace("{{ListName}}", str(SDP.ListName or '')) \
                                        .replace("{{ListTypeDesc}}", str(SDP.ListTypeDesc or '')) \
                                        .replace("{{HearingTypeDesc}}", str(SDP.HearingTypeDesc or '')) \
                                        .replace("{{ListStartTime}}", str(SDP.ListStartTime or '')) \
                                        .replace("{{StartTime}}", str(SDP.StartTime or '')) \
                                        .replace("{{TimeEstimate}}", str(SDP.TimeEstimate or '')) \
                                        .replace("{{LanguageDescription}}", str(SDP.LanguageDescription or '')) \
                                        .replace("{{AppealCaseNote}}", str(row.AppealCaseNote or '')) \
                                        .replace("{{Language}}", str(row.Language or '')) \
                                        .replace("{{IRISStatusOfCase}}", str(SDP.IRISStatusOfCase or '')) \
                                        .replace("{{ListTypeDescription}}", str(SDP.ListTypeDescription or '')) \
                                        .replace("{{HearingTypeDescription}}", str(SDP.HearingTypeDescription or '')) \
                                        .replace("{{Judiciary1Name}}", str(SDP.Judiciary1Name or '')) \
                                        .replace("{{Judiciary2Name}}", str(SDP.Judiciary2Name or '')) \
                                        .replace("{{Judiciary3Name}}", str(SDP.Judiciary3Name or '')) \
                                        .replace("{{ReasonAdjourn}}", str(SDP.ReasonAdjourn or '')) \
                                        .replace("{{JudgeLabel1}}", str(SDP.JudgeLabel1 or '')) \
                                        .replace("{{JudgeLabel2}}", str(SDP.JudgeLabel2 or '')) \
                                        .replace("{{JudgeLabel3}}", str(SDP.JudgeLabel3 or '')) \
                                        .replace("{{Label1_JudgeValue}}", str(SDP.Label1_JudgeValue or '')) \
                                        .replace("{{Label2_JudgeValue}}", str(SDP.Label2_JudgeValue or '')) \
                                        .replace("{{Label3_JudgeValue}}", str(SDP.Label3_JudgeValue or '')) \
                                        .replace("{{CourtClerkUsher}}", str(SDP.CourtClerkUsher or '')) \
                                        .replace("{{CMROrder}}", str(SDP.CMROrder or '')) \
                                        .replace("{{RequiredIncompatiblejudicialofficersPlaceHolder}}", str("\n".join(
                                                f"<tr><td id=\"midpadding\">{judge.JudgeSurname}, {judge.JudgeForenames} {judge.JudgeTitle}</td><td id=\"midpadding\" style=\"text-align:center\">{'✓' if judge.Required else ''}</td></tr>"
                                                for i, judge in enumerate(SDP.CaseAdjudicatorsDetails or [])
                                            ) or '<tr><td id="midpadding"></td><td id="midpadding"></td></tr>')) \
                                        .replace("{{SpecificdirectionsPlaceHolder}}", str("\n".join(
                                                f"<tr><td id=\"midpadding\">{rspecd.SpecificDirection}</td><td id=\"midpadding\">{rspecd.DateRequiredIND}</td><td id=\"midpadding\">{rspecd.DateRequiredAppellantRep}</td><td id=\"midpadding\">{rspecd.DateReceivedIND}</td><td id=\"midpadding\">{rspecd.DateReceivedAppellantRep}</td></tr>"
                                                for i, rspecd in enumerate(SDP.ReviewSpecficDirectionDetails or [])
                                            ) or '<tr><td id="midpadding"></td><td id="midpadding"></td></tr>')) \
                                        .replace("{{StandarddirectionsPlacHolder}}", str("\n".join(
                                                f"<tr><td id=\"midpadding\">{rstd.ReviewStandardDirectionId}</td><td id=\"midpadding\">{format_date(rstd.DateRequiredIND)}</td><td id=\"midpadding\">{format_date(rstd.DateRequiredAppellantRep)}</td><td id=\"midpadding\">{format_date(rstd.DateReceivedIND)}</td><td id=\"midpadding\">{format_date(rstd.DateReceivedAppellantRep)}</td></tr>"
                                                for i, rstd in enumerate(SDP.ReviewStandardDirectionDirectionDetails or [])
                                            ) or '<tr><td id="midpadding"></td><td id="midpadding"></td></tr>')) \
                                        # .replace("{{AssignedjudicialofficersPlaceHolder}}", str("\n".join(
                                        #         f"<tr><td id=\"midpadding\">{adjd.JudgeFT}</td><td id=\"midpadding\">{adjd.CourtClerkUsher}</td><td id=\"midpadding\"></td><td id=\"midpadding\"></td></tr>"
                                        #         for i, adjd in enumerate(SDP.CaseStatusAdjudicatorDetails or [])
                                        #     ) or '<tr><td id="midpadding"></td><td id="midpadding"></td><td id="midpadding"></td><td id="midpadding"></td></tr>'))     
 

                status_details_code += line + '\n'
        else:
            casestatusTemplate = templates["DefaultStatusDetail"]
            line = casestatusTemplate.replace("{{nested_table_number}}", str(nested_table_number))  \
                                    .replace("{{nested_tab_group_number}}", str(nested_tab_group_number))  \
                                    .replace("{{nested_tabs_size}}", str(nested_tabs_size)) \
                                        
                                                                                
            status_details_code += line + '\n'

            # displayHTML(status_details_code)

        html_template = html_template.replace(f"{{{{StatusDetailsPlaceHolder}}}}", status_details_code)
        
        return html_template
    
    except Exception as e:
        return f"Error generating HTML for CaseNo {row.CaseNo}: {e}"

# Register UDF
generate_html_udf = udf(generate_html, StringType())

# COMMAND ----------

# DBTITLE 1,Case Types LookUP
data = [
    (1, "Adjudicator Appeal", None, None),
    (2, "Application for Adjournment", None, None),
    (3, "Adjudicator Typing", None, None),
    (4, "Bail Application", None, None),
    (5, "Direct to Divisional Court", None, None),
    (6, "Forfeiture", None, None),
    (7, "Permission to Divisional Court", None, None),
    (8, "Lodgement", None, None),
    (9, "Paper Case", None, None),
    (10, "Preliminary Issue", "StatusDetailPreliminaryIssueTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailPreliminaryIssueTemplate.html"),
    (11, "Scottish Forfeiture", None, None),
    (12, "Tribunal Appeal", None, None),
    (13, "Tribunal Application", None, None),
    (14, "Tribunal Direct", None, None),
    (15, "Tribunal Typing", None, None),
    (16, "Judicial Review", None, None),
    (17, "Application to Adjourn", None, None),
    (18, "Bail Renewal", None, None),
    (19, "Bail Variation", None, None),
    (20, "Chief Adjudicator’s Review", None, None),
    (21, "Tribunals Review", None, None),
    (22, "Record Hearing Outcome – Bail", None, None),
    (23, "Record Hearing Outcome – Case", None, None),
    (24, "Record Hearing Outcome – Visit Visa", None, None),
    (25, "Statutory Review", None, None),
    (26, "Case Management Review", "StatusDetailCaseManagementReviewTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailCaseManagementReviewTemplate.html"),
    (27, "Court of Appeal", "StatusDetailAppellateCourtTemplate",f"{html_mnt}/Appeals/statusdetails/StatusDetailAppellateCourtTemplate.html"),
    (28, "High Court Review", "StatusDetailHighCourtReviewTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailHighCourtReviewTemplate.html"),
    (29, "High Court Review (Filter)", "StatusDetailHighCourtReviewFilterTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailHighCourtReviewFilterTemplate.html"),
    (30, "Immigration Judge – Hearing", "StatusDetailImmigrationJudgeHearingTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailImmigrationJudgeHearingTemplate.html"),
    (31, "Immigration Judge – Paper", "StatusDetailImmigrationJudgeHearingTemplate",f"{html_mnt}/Appeals/statusdetails/StatusDetailImmigrationJudgeHearingTemplate.html"),
    (32, "Panel Hearing (Legal)", "StatusDetailPanelHearingLegalTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailPanelHearingLegalTemplate.html"),
    (33, "Panel Hearing (Legal/Non Legal)", "StatusDetailPanelHearingLegalTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailPanelHearingLegalTemplate.html"),
    (34, "Permission to Appeal", "StatusDetailPermissiontoAppealTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailPermissiontoAppealTemplate.html"),
    (35, "Migration", "StatusDetailMigrationTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailMigrationTemplate.html"),
    (36, "Review of Cost Order", "StatusDetailReviewOfCostOrderTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailReviewofCostOrderTemplate.html"),
    (37, "First Tier – Hearing", "StatusDetailFirstTierHearingTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailFirstTierHearingTemplate.html"),
    (38, "First Tier – Paper", "StatusDetailFirstTierPaperTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailFirstTierPaperTemplate.html"),
    (39, "First Tier Permission Application", "StatusDetailFirstTierPermissionApplicationTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailFirstTierPermissionApplicationTemplate.html"),
    (40, "Upper Tribunal Permission Application", "StatusDetailUpperTribunalPermissionApplicationTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailUpperTribunalPermissionApplicationTemplate.html"),
    (41, "Upper Tribunal Oral Permission Application", "StatusDetailUpperTribunalOralPermissionApplicationTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailUpperTribunalOralPermissionApplicationTemplate.html"),
    (42, "Upper Tribunal Hearing", "StatusDetailUpperTribunalHearingTemplate",f"{html_mnt}/Appeals/statusdetails/StatusDetailUpperTribunalHearingTemplate.html"),
    (43, "Upper Tribunal Hearing – Continuance", "StatusDetailUpperTribunalHearingContinuanceTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailUpperTribunalHearingContinuanceTemplate.html"),
    (44, "Upper Tribunal Oral Permission Hearing", "StatusDetailUpperTribunalOralPermissionHearingTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailUpperTribunalOralPermissionHearingTemplate.html"),
    (45, "PTA Direct to Appellate Court", "StatusDetailPTADirecttoAppellateCourtTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailPTADirecttoAppellateCourtTemplate.html"),
    (46, "Set Aside Application", "StatusDetailSetAsideApplicationTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailSetAsideApplicationTemplate.html"),
    (47, "Judicial Review Permission Application", "StatusDetailJudicialReviewPermissionApplicationTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailJudicialReviewPermissionApplicationTemplate.html"),
    (48, "Judicial Review Hearing", "StatusDetailJudicialReviewHearingTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailJudicialReviewHearingTemplate.html"),
    (49, "Judicial Review Oral Permission Hearing", "StatusDetailJudicialReviewHearingTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailJudicialReviewHearingTemplate.html"),
    (50, "On Hold – Chargeback Taken", "StatusDetailOnHoldChargebackTakenTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailOnHoldChargebackTakenTemplate.html"),
    (51, "Closed – Fee Not Paid", "StatusDetailClosedFeeNotPaidTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailClosedFeeNotPaidTemplate.html"),
    (52, "Case closed fee outstanding", "StatusDetailCaseClosedFeeOutstandingTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailCaseClosedFeeOutstandingTemplate.html"),
    (53, "Upper Trib Case On Hold – Fee Not Paid", "StatusDetailOnHoldChargebackTakenTemplate", f"{html_mnt}/Appeals/statusdetails/StatusDetailOnHoldChargebackTakenTemplate.html"),
    (54, "Ork", None, None)
]



columns = ["id", "description", "HTMLName", "path"]
lookup_df = spark.createDataFrame(data, columns).filter(col("path").isNotNull())
casestatus_array = lookup_df.select(col("id")).distinct().rdd.flatMap(lambda x: x).collect()
lookup_list = lookup_df.collect()
# display(lookup_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging tables for Gold Outputs

# COMMAND ----------

# DBTITLE 1,Transformation: stg_statichtml_data
@dlt.table(
    name="stg_statichtml_data",
    comment="Delta Live Silver Table for Archive Metadata data.",
    path=f"{silver_mnt}/stg_statichtml_data"
)
def stg_statichtml_data():
    # df_transaction_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_transaction_detail")
    # df_history_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_history_detail")
    # df_status_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_status_detail")
    # df_link_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_link_detail")



    # df_status = spark.table("hive_metastore.ariadm_arm_appeals.silver_status_detail")
    # df_category = spark.table("hive_metastore.ariadm_arm_fta.silver_appealcategory_detail")
    # df_transaction = spark.table("hive_metastore.ariadm_arm_appeals.silver_transaction_detail")
    # df_case = spark.table("hive_metastore.ariadm_arm_fta.silver_appealcase_detail")
    # df_applicant = spark.table("hive_metastore.ariadm_arm_fta.silver_applicant_detail")

    df_transaction_details = dlt.read("silver_transaction_detail")
    df_category = dlt.read("silver_appealcategory_detail")
    df_history_details =  dlt.read("silver_history_detail")
    df_status_details =  dlt.read("silver_status_detail")
    df_link_details =  dlt.read("silver_link_detail")
    df_case =  dlt.read("silver_appealcase_detail")
    df_applicant =  dlt.read("silver_applicant_detail")

    # Get the latest transaction details
    window_spec = Window.partitionBy("CaseNo").orderBy("transactionid")
    df_transaction_details_derived = df_transaction_details.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).select('CaseNo', 'FirstTierFee', 'TotalFeeAdjustments', 'TotalFeeDue', 'TotalPaymentsReceived', 'TotalPaymentAdjustments', 'BalanceDue')

    # Get the latest history details
    window_spec = Window.partitionBy("CaseNo").orderBy(col("HistDate").desc())
    df_latest_history_details = df_history_details.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num").select("CaseNo", "lastDocument", "fileLocation")

    # Get the latest status details
    window_spec = Window.partitionBy("CaseNo").orderBy(col("KeyDate").desc())
    df_latest_status_details = df_status_details.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num").select("CaseNo", col("CaseStatusDescription").alias("currentstatus"))

    # Derive connectedFiles column
    df_link_details_derived = df_link_details.withColumn("connectedFiles", lit("Connected Files exist")).select("CaseNo", "connectedFiles").distinct()

    # Join all dataframes on CaseNo
    df_stg_static_html_data = df_transaction_details_derived.join(df_latest_history_details, "CaseNo", "outer") \
                                                            .join(df_latest_status_details, "CaseNo", "outer") \
                                                            .join(df_link_details_derived, "CaseNo", "outer")

    # display(df_stg_static_html_data)


    # *********************************************************************Red Text Flag logic***************************************************/
    cte = (df_status_details
        .filter(col("CaseStatus").isin([27, 28, 29, 30, 31, 32, 33, 34]))
        .select("CaseNo", when(col("ReconsiderationHearing") == 'enabled', 'REC').otherwise(lit(None)).alias("ReconsiderationHearing"))
        .distinct())

    cte_rec = cte.filter(col("ReconsiderationHearing") == 'REC').select("CaseNo", "ReconsiderationHearing").distinct()

    # CTE for Flag_List
    flag_list = (df_category
        # .join(df_status_details, "CaseNo")
        # .filter(col("CaseStatus") == 35)
        .groupby("CaseNo")
        .agg(sort_array(collect_list(struct(col("Priority"), concat_ws('', lit('*'), col("Flag"), lit('*')))), asc=True).alias("sorted_flags")))

    cte_flag_3 = (flag_list
        .select(
            "CaseNo",
            when(size(col("sorted_flags")) > 3,
                concat(concat_ws(", ", expr("slice(transform(sorted_flags, x -> x.Col2), 1, 3)")), lit(", ...")))  # Fixing concat order
            .otherwise(concat_ws(", ", expr("transform(sorted_flags, x -> x.Col2)")))  # If <= 3, take all
            .alias("Flag_List")
        )
    )


    # display(flag_list.filter((size(col("sorted_flags")) > 3)))
    # # AS/00003/2005


    # display(cte_flag_3.filter(col("CaseNo") == lit("AS/00003/2005")))

    # display(cte_rec)

    # CTE for Fee Flags (Fixing count() issue)
    df_transaction_filtered = df_transaction_details.filter(col("TransactionTypeId") == 1).groupby("CaseNo").agg(count("*").alias("txn_count"))

    cte_feeflag = (df_transaction_details
        .join(df_transaction_filtered, "CaseNo", "left")
        .withColumn("Fee_Flag", when((col("BalanceDue") == 0) & (col("TransactionTypeId") == 5), "FEE REMISSION")
            .when((col("BalanceDue") == 0) & (col("TransactionTypeId") == 17), "FEE EXEMPT")
            .when((col("TotalPaymentsReceived") >= col("TotalFeeDue")) & (col("txn_count") > 0), "FEE PAID")
            .when((col("BalanceDue") > 0) & (col("txn_count") > 0), "FEE DUE")
            .otherwise(lit(None))))

    # cte_feeflagordered = cte_feeflag.withColumn(
    #     "Fee_Flag_num",
    #     when(col("Fee_Flag") == "FEE REMISSION", 1)
    #     .when(col("Fee_Flag") == "FEE EXEMPT", 2)
    #     .when(col("Fee_Flag") == "FEE PAID", 3)
    #     .when(col("Fee_Flag") == "FEE DUE", 4)
    #     .otherwise(0)
    # ).filter(col("Fee_Flag_num") != 0)

    # max_fee_flag_num = cte_feeflagordered.groupby("CaseNo").agg(min("Fee_Flag_num").alias("max_Fee_Flag_num"))

    # final_fee_flag = (cte_feeflagordered
    #     .join(max_fee_flag_num, ["CaseNo"], "inner")
    #     .filter((col("max_Fee_Flag_num") != 0) | (col("max_Fee_Flag_num") != " "))
    #     .select("CaseNo", "Fee_Flag", "Fee_Flag_num"))


    # Assign numerical order based on priority
    cte_feeflagordered = cte_feeflag.withColumn(
        "Fee_Flag_num",
        when(col("Fee_Flag") == "FEE REMISSION", 1)
        .when(col("Fee_Flag") == "FEE EXEMPT", 2)
        .when(col("Fee_Flag") == "FEE PAID", 3)
        .when(col("Fee_Flag") == "FEE DUE", 4)
        .otherwise(5)  # Default to lowest priority if Fee_Flag doesn't match
    ).filter(col("Fee_Flag_num") != 5)  # Remove unrecognized flags

    # Define window to get the top 1 priority Fee_Flag per CaseNo
    window_spec = Window.partitionBy("CaseNo").orderBy("Fee_Flag_num")

    # Assign row number based on priority order
    cte_ranked = cte_feeflagordered.withColumn("rank", row_number().over(window_spec))

    # Filter only the top 1 priority record per CaseNo
    final_fee_flag = cte_ranked.filter(col("rank") == 1).select("CaseNo", "Fee_Flag", "Fee_Flag_num")


    # Final query combining all CTEs
    final_df = (df_case
        .join(df_applicant.alias("a"), "CaseNo", "left")
        .join(cte_rec, "CaseNo", "left")
        .join(cte_flag_3, "CaseNo", "left")
        .join(final_fee_flag, "CaseNo", "left")
        .select(
            "CaseNo",       
            concat(
                when(col("VisitVisaType") == 'Oral Hearing', "*ORAL*")
                    .when(col("VisitVisaType") == 'On Papers', "*PAPER*")
                    .otherwise('     '),
                when(col("Detained").isin('HMP', 'IRC', 'OTHER'), concat_ws("", lit('*'), lit("DET"), lit('*'))).otherwise('    '),
                when(col("ReconsiderationHearing") == 'REC', '*REC*').otherwise('')
            ).alias("flag1"),
            concat_ws("",
                when(col("InCamera") == 'checked', '*CAM*').otherwise(''),
                when(col("Fee_Flag").isNotNull(),concat( lit('*'), col("Fee_Flag"), lit('*'))).otherwise('')
            ).alias("flag2"),
            coalesce(col("Flag_List"), lit('')).alias("flag3")
        )).distinct()
    
    # *********************************************************************Red Text Flag logic***************************************************/

    # df_stg_static_html_data = df_stg_static_html_data.alias("a").join(final_df.alias("b"),"CaseNo", "left").select("a.*","flag1","flag2","flag3")

    stg_static_html_data_columns_list = [col for col in df_stg_static_html_data.columns if col != 'CaseNo']

    # df_stg_static_html_final_data = df_case.alias("a").join(final_df.alias("b"), col("a.CaseNo") == col("b.CaseNo"), "outer") \
    #                                               .join(df_stg_static_html_data.alias("c"), col("a.CaseNo") == col("c.CaseNo"), "outer") \
    #                                               .select(col("a.CaseNo"), *[col(c) for c in stg_static_html_data_columns_list], col("flag1"), col("flag2"), col("flag3"))
    
    df_stg_static_html_final_data = df_case.alias("a").join(final_df.alias("b"), col("a.CaseNo") == col("b.CaseNo"), "outer") \
                                                  .join(df_stg_static_html_data.alias("c"), col("a.CaseNo") == col("c.CaseNo"), "outer") \
                                                  .select(col("a.CaseNo"), *[col(c) for c in stg_static_html_data_columns_list], col("flag1"), col("flag2"), expr("regexp_replace(flag3, ',', '')").alias("flag3"))



    return df_stg_static_html_final_data

# COMMAND ----------

# DBTITLE 1,Transformation: stg_statusdetail_data
@dlt.table(
    name="stg_statusdetail_data",
    comment="Delta Live Silver Table for Archive Metadata data.",
    path=f"{silver_mnt}/stg_statusdetail_data"
)
def stg_statusdetail_data():
    # df_list_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_list_detail")
    # df_status_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_status_detail")
    # df_hearingpointschange_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_hearingpointschange_detail")

    df_list_details = dlt.read("silver_list_detail")
    # # df_list_details = spark.read.table("hive_metastore.ariadm_arm_fta.silver_list_detail")
    # window_spec = Window.partitionBy("CaseNo", "CaseStatus", "StatusId").orderBy("ListStartTime")
    # df_list_details = df_list_details.withColumn("row_num", row_number().over(window_spec))
    # df_list_details = df_list_details.filter(col("row_num") == 1).drop("row_num")

    df_status_details = dlt.read("silver_status_detail")
    df_hearingpointschange_details = dlt.read("silver_hearingpointschange_detail")
    df_reviewspecificdirection_details = dlt.read("silver_reviewspecificdirection_detail")

    df_case_adjudicator = dlt.read("silver_case_adjudicator").groupBy("CaseNo").agg(
        collect_list(struct( 'Required', 'JudgeSurname', 'JudgeForenames', 'JudgeTitle')).alias("CaseAdjudicatorsDetails")
    )

    df_reviewspecificdirection = dlt.read("silver_reviewspecificdirection_detail").groupBy("CaseNo").agg(
        collect_list(struct(
            'ReviewSpecificDirectionId', 'CaseNo', 'StatusId', 'SpecificDirection', 
            'DateRequiredIND', 'DateRequiredAppellantRep', 'DateReceivedIND', 'DateReceivedAppellantRep'
        )).alias("ReviewSpecficDirectionDetails")
    )

    df_reviewstandarddirection = dlt.read("sliver_direction_detail").groupBy("CaseNo").agg(
        collect_list(struct(
            'ReviewStandardDirectionId', 'CaseNo', 'StatusId', 'StandardDirectionId', 
            'DateRequiredIND', 'DateRequiredAppellantRep', 'DateReceivedIND', 'DateReceivedAppellantRep'
        )).alias("ReviewStandardDirectionDirectionDetails")
    )

    # casestatus with templates
    casestatus_array = [
        26, 29, 27, 28, 30, 35, 39, 41, 37, 38, 42, 40, 10, 34, 32, 31, 33, 36, 50, 
        43, 51, 52, 48, 44, 49, 46, 45, 47, 53
    ]

    # this returns the parent StatusID to the application to adjourn
    # this returns the parent StatusID to the application to adjourn
    adjournment_parents = df_status_details.filter(col("CaseStatus") == 17) \
    .select(col("AdjournmentParentStatusId"), col("ApplicationType"),col("InTime"),col("DecisionTypeDescription")) \
    .withColumnRenamed("AdjournmentParentStatusId", "ParentStatusId") \
    .withColumnRenamed("ApplicationType", "adjournApplicationType") \
    .withColumnRenamed("InTime", "adjournInTime") \
    .withColumnRenamed("DecisionTypeDescription", "adjournDecisionTypeDescription") \

    adjourned_withdrawal_df = df_status_details.join(
    adjournment_parents.alias("adjournment_parents"),
    df_status_details.StatusId == adjournment_parents.ParentStatusId,
    "inner"
    ).select(df_status_details["*"], col("adjournment_parents.adjournApplicationType"), col("adjournment_parents.adjournInTime"),col("adjournment_parents.adjournDecisionTypeDescription"))

    # Join to merge M3 and M7
    status_joined_df = df_list_details.alias("list").join(df_status_details.alias('status'), 
                                                        (col("list.CaseNo") == col("status.CaseNo")) & 
                                                        (col("list.Statusid") == col("status.Statusid")), "inner") \
                                                    .join(df_hearingpointschange_details.alias('hearing'), 
                                                                (col("status.CaseNo") == col("hearing.CaseNo")) & 
                                                                (col("status.Statusid") == col("hearing.Statusid")) & 
                                                                (col("status.HearingPointsChangeReasonId") == col("hearing.HearingPointsChangeReasonId")), "left") \
                                                        .withColumn("HearingPointsChangeReasondesc", col("hearing.Description")) \
                                                        .drop("list.CaseNo", "list.Statusid")

    # Select and refine columns from the joined dataframe
    status_refined_df = status_joined_df.select( "status.*", "list.Outcome",
        "list.TimeEstimate",
        "list.ListNumber",
        "list.HearingDuration",
        "list.StartTime",
        "list.HearingTypeDesc",
        "list.HearingTypeEst",
        "list.DoNotUse",
        "list.ListAdjudicatorId",
        "list.ListAdjudicatorSurname",
        "list.ListAdjudicatorForenames",
        "list.ListAdjudicatorNote",
        "list.ListAdjudicatorTitle",
        "list.ListName",
        "list.ListStartTime",
        "list.ListTypeDesc",
        "list.ListType",
        "list.DoNotUseListType",
        "list.CourtName",
        "list.DoNotUseCourt",
        "list.HearingCentreDesc",
        "list.Position",
        "JudgeLabel1",
        "JudgeLabel2",
        "JudgeLabel3",
        "Label1_JudgeValue",
        "Label2_JudgeValue",
        "Label3_JudgeValue",
        "CourtClerkUsher",
        "HearingPointsChangeReasondesc") \
        .join(adjourned_withdrawal_df.alias("adj"), 
            
            ((col("status.StatusId") == col("adj.StatusId"))
            & (col("status.CaseNo") == col("adj.CaseNo"))
            & (col("status.CaseStatus") == col("adj.CaseStatus"))),
            
            "left") \
        .withColumn("adjourned_withdrawal_enabled", when(col("adj.StatusId").isNotNull(), lit(True)).otherwise(lit(False))) \
        .withColumn("adjournDecisionTypeDescription",  when(col("adj.StatusId").isNotNull(),col("adj.adjournDecisionTypeDescription")).otherwise(lit(None))) \
        .withColumn("adjournDateReceived", when(col("adj.StatusId").isNotNull(),col("adj.DateReceived")).otherwise(lit(None))) \
        .withColumn("adjournmiscdate1", when(col("adj.StatusId").isNotNull(),col("adj.miscdate1")).otherwise(lit(None))) \
        .withColumn("adjournmiscdate2", when(col("adj.StatusId").isNotNull(),col("adj.miscdate2")).otherwise(lit(None))) \
        .withColumn("adjournParty", when(col("adj.StatusId").isNotNull(),col("adj.Party")).otherwise(lit(None))) \
        .withColumn("adjournInTime", when(col("adj.StatusId").isNotNull(),col("adj.adjournInTime")).otherwise(lit(None))) \
        .withColumn("adjournLetter1Date", when(col("adj.StatusId").isNotNull(),col("adj.Letter1Date")).otherwise(lit(None))) \
        .withColumn("adjournLetter2Date", when(col("adj.StatusId").isNotNull(),col("adj.Letter2Date")).otherwise(lit(None))) \
        .withColumn("adjournAdjudicatorSurname", when(col("adj.StatusId").isNotNull(),col("adj.StatusDetailAdjudicatorSurname")).otherwise(lit(None))) \
        .withColumn("adjournAdjudicatorForenames", when(col("adj.StatusId").isNotNull(),col("adj.StatusDetailAdjudicatorForenames")).otherwise(lit(None))) \
        .withColumn("adjournAdjudicatorTitle", when(col("adj.StatusId").isNotNull(),col("adj.StatusDetailAdjudicatorTitle")).otherwise(lit(None))) \
        .withColumn("adjournNotes1", when(col("adj.StatusId").isNotNull(),col("adj.Notes1")).otherwise(lit(None))) \
        .withColumn("adjournDecisionDate", when(col("adj.StatusId").isNotNull(),col("adj.DecisionDate")).otherwise(lit(None))) \
        .withColumn("adjournPromulgated", when(col("adj.StatusId").isNotNull(),col("adj.Promulgated")).otherwise(lit(None))) \
        .withColumn("adjournUKAITNo", when(col("adj.StatusId").isNotNull(),col("adj.UKAITNo")).otherwise(lit(None))) \
        .withColumn("AdjudicatorSurname", when(col("status.KeyDate").isNull(), col("status.StatusDetailAdjudicatorSurname")).otherwise(col("list.ListAdjudicatorSurname"))) \
        .withColumn("AdjudicatorForenames", when(col("status.KeyDate").isNull(), col("status.StatusDetailAdjudicatorForenames")).otherwise(col("list.ListAdjudicatorForenames"))) \
        .withColumn("AdjudicatorTitle", when(col("status.KeyDate").isNull(), col("status.StatusDetailAdjudicatorTitle")).otherwise(col("list.ListAdjudicatorTitle"))) \
        .withColumn("AdjudicatorId", when(col("status.KeyDate").isNull(), col("status.StatusDetailAdjudicatorId")).otherwise(col("list.ListAdjudicatorId"))) \
        .withColumn("AdjudicatorNote", when(col("status.KeyDate").isNull(), col("status.StatusDetailAdjudicatorNote")).otherwise(col("list.ListAdjudicatorNote"))) \
        .withColumn("adjournApplicationType", when(col("adj.StatusId").isNotNull(), col("adj.adjournApplicationType")).otherwise(lit(None))) \
        .withColumn("adjournKeyDate", when(col("adj.StatusId").isNotNull(), col("status.KeyDate")).otherwise(lit(None)))

    # Filter out only CaseStatus that are relevant for appeals
    join_df = status_refined_df.filter((col("status.CaseStatus").cast("integer")).isin(casestatus_array)) \
        .join(df_case_adjudicator.alias('cadj'), 'CaseNo', 'left') \
        .join(df_reviewspecificdirection.alias('rsd'), 'CaseNo', 'left') \
        .join(df_reviewstandarddirection.alias('rsdd'), 'CaseNo', 'left')  \
        .join(df_reviewspecificdirection_details.alias("rsd_raw"), 
            ((col("status.StatusId") == col("rsd_raw.StatusId"))
            & (col("status.CaseNo") == col("rsd_raw.CaseNo"))),
            "left") \
        .withColumn("CMROrder", when(col("rsd_raw.StatusId").isNotNull(), lit('Directions Exist')).otherwise(lit(None)))
        # .withColumn("JudgeFT", when(col("Position") != 3, concat(col("AdjudicatorSurname"), lit(", "), col("AdjudicatorForenames"), lit(" ("), col("AdjudicatorTitle"), lit(")"))).otherwise(lit(None))) \
        # .withColumn("CourtClerkUsher", when(col("Position") == 3, concat(col("AdjudicatorSurname"), lit(", "), col("AdjudicatorForenames"), lit(" ("), col("AdjudicatorTitle"), lit(")"))).otherwise(lit(None)))
        

    # df_agg01 = join_df.groupBy("status.CaseNo", "status.CaseStatus", "status.StatusId").agg(
    #     collect_list(struct("AdjudicatorSurname", "AdjudicatorForenames", "AdjudicatorTitle", 'status.KeyDate', 'AdjudicatorId'
    #     )).alias("CaseStatusAdjudicatorDetails")
    # )

    df_agg01 = join_df.groupBy("status.CaseNo", "status.CaseStatus", "status.StatusId").agg(
        collect_list(struct("AdjudicatorSurname", "AdjudicatorForenames", "AdjudicatorTitle", "status.KeyDate", "AdjudicatorId","Position")).alias("CaseStatusAdjudicatorDetails"),
        max("status.KeyDate").alias("LatestKeyDate"),
        max_by("AdjudicatorSurname", "status.KeyDate").alias("LatestAdjudicatorSurname"),
        max_by("AdjudicatorForenames", "status.KeyDate").alias("LatestAdjudicatorForenames"),
        max_by("AdjudicatorTitle", "status.KeyDate").alias("LatestAdjudicatorTitle"),
        max_by("AdjudicatorId", "status.KeyDate").alias("LatestAdjudicatorId"),
        max_by("JudgeLabel1", "status.KeyDate").alias("JudgeLabel1"),
        max_by("JudgeLabel2", "status.KeyDate").alias("JudgeLabel2"),
        max_by("JudgeLabel3", "status.KeyDate").alias("JudgeLabel3"),
        max_by("Label1_JudgeValue", "status.KeyDate").alias("Label1_JudgeValue"),
        max_by("Label2_JudgeValue", "status.KeyDate").alias("Label2_JudgeValue"),
        max_by("Label3_JudgeValue", "status.KeyDate").alias("Label3_JudgeValue"),
        max_by("CourtClerkUsher", "status.KeyDate").alias("CourtClerkUsher")
    )


    df_agg2 = join_df.select("status.CaseNo","status.CaseStatus", "status.StatusId", 'status.CaseStatusDescription',  'status.InterpreterRequired',  'status.MiscDate2', 'status.VideoLink', 'status.RemittalOutcome', 'status.UpperTribunalAppellant', 'status.DecisionSentToHO', 
            'status.InitialHearingPoints', 'status.FinalHearingPoints', 'HearingPointsChangeReasondesc', 'status.CostOrderAppliedFor', 'status.DecisionDate', 
            'status.DeterminationByJudgeSurname', 'status.DeterminationByJudgeForenames', 'status.DeterminationByJudgeTitle', 'status.MethodOfTyping', 
            'adjournDecisionTypeDescription', 'status.Promulgated', 'status.UKAITNo', 'status.Extempore', 'status.WrittenReasonsRequestedDate', 
            'status.TypistSentDate', 'status.ExtemporeMethodOfTyping', 'status.TypistReceivedDate', 'status.WrittenReasonsSentDate', 'status.DecisionSentToHODate', 
            'status.DecisionTypeDescription', 'status.DateReceived', 'status.Party', 'status.OutOfTime', 'status.MiscDate1', 
            'status.HearingPointsChangeReasonId', 'status.DecisionByTCW', 'status.Allegation', 'status.DecidingCentre', 'status.Process', 'status.Tier', 'status.NoCertAwardDate', 
            'status.WrittenOffDate', 'status.WrittenOffFileDate', 'status.ReferredEnforceDate', 'status.Letter1Date', 'status.Letter2Date', 'status.Letter3Date', 
            'status.ReferredFinanceDate', 'status.CourtActionAuthDate', 'status.BalancePaidDate', 'status.ReconsiderationHearing', 
            'status.UpperTribunalHearingDirectionId', 'status.ListRequirementTypeId', 'status.CourtSelection', 'status.COAReferenceNumber', 'status.Notes2', 
            'status.HighCourtReference', 'status.AdminCourtReference', 'status.HearingCourt', 'status.ApplicationType', 
            'status.IRISStatusOfCase','status.ListTypeDescription','status.HearingTypeDescription','status.Judiciary1Name','status.Judiciary2Name','status.Judiciary3Name','status.ReasonAdjourn', 
            'adjournDateReceived', 'adjournmiscdate2', 'adjournParty', 'adjournInTime', 'adjournLetter1Date', 'adjournLetter2Date', 
            'adjournAdjudicatorSurname', 'adjournAdjudicatorForenames', 'adjournAdjudicatorTitle',  'adjournNotes1', 
            'adjournDecisionDate', 'adjournPromulgated', 'HearingCentreDesc', 'CourtName', 'ListName', 'ListTypeDesc', 
            'HearingTypeDesc', 'ListStartTime', 'StartTime', 'TimeEstimate',  'status.LanguageDescription','cadj.CaseAdjudicatorsDetails','rsd.ReviewSpecficDirectionDetails','rsdd.ReviewStandardDirectionDirectionDetails',"status.StatusDetailAdjudicatorSurname","status.StatusDetailAdjudicatorForenames","status.StatusDetailAdjudicatorTitle","adjournApplicationType","adjournKeyDate","CMROrder").distinct()


        
    df_final = df_agg2.alias("casestatus").join(df_agg01.alias("adjj"), ((col("casestatus.StatusId") == col("adjj.StatusId"))
            & (col("casestatus.CaseNo") == col("adjj.CaseNo"))
            & (col("casestatus.CaseStatus") == col("adjj.CaseStatus"))), 'left')\
                .join(lookup_df.alias("lookup"), col("casestatus.CaseStatus") == col("lookup.id")) \
                .orderBy(col("casestatus.StatusId").desc()) \
            .groupBy("casestatus.CaseNo").agg(collect_list(struct( "casestatus.CaseStatus", "casestatus.StatusId", "CaseStatusAdjudicatorDetails",'casestatus.CaseStatusDescription',  'casestatus.InterpreterRequired',  'casestatus.MiscDate2', 'casestatus.VideoLink', 'casestatus.RemittalOutcome', 'casestatus.UpperTribunalAppellant', 'casestatus.DecisionSentToHO', 
            'casestatus.InitialHearingPoints', 'casestatus.FinalHearingPoints', 'HearingPointsChangeReasondesc', 'casestatus.CostOrderAppliedFor', 'casestatus.DecisionDate', 
            'casestatus.DeterminationByJudgeSurname', 'casestatus.DeterminationByJudgeForenames', 'casestatus.DeterminationByJudgeTitle', concat_ws(" ", concat_ws(", ", col("casestatus.DeterminationByJudgeSurname"), col("casestatus.DeterminationByJudgeForenames")), when(col("casestatus.DeterminationByJudgeTitle").isNotNull(), concat(lit("("), col("casestatus.DeterminationByJudgeTitle"), lit(")")))).alias("DeterminationByJudgeFullName"), 'casestatus.MethodOfTyping', 
            'adjournDecisionTypeDescription', 'casestatus.Promulgated', 'casestatus.UKAITNo', 'casestatus.Extempore', 'casestatus.WrittenReasonsRequestedDate', 
            'casestatus.TypistSentDate', 'casestatus.ExtemporeMethodOfTyping', 'casestatus.TypistReceivedDate', 'casestatus.WrittenReasonsSentDate', 'casestatus.DecisionSentToHODate', 
            'casestatus.DecisionTypeDescription', 'casestatus.DateReceived', 'casestatus.Party', 'casestatus.OutOfTime', 'casestatus.MiscDate1', 
            'casestatus.HearingPointsChangeReasonId', 'casestatus.DecisionByTCW', 'casestatus.Allegation', 'casestatus.DecidingCentre', 'casestatus.Process', 'casestatus.Tier', 'casestatus.NoCertAwardDate', 
            'casestatus.WrittenOffDate', 'casestatus.WrittenOffFileDate', 'casestatus.ReferredEnforceDate', 'casestatus.Letter1Date', 'casestatus.Letter2Date', 'casestatus.Letter3Date', 
            'casestatus.ReferredFinanceDate', 'casestatus.CourtActionAuthDate', 'casestatus.BalancePaidDate', 'casestatus.ReconsiderationHearing', 
            'casestatus.UpperTribunalHearingDirectionId', 'casestatus.ListRequirementTypeId', 'casestatus.CourtSelection', 'casestatus.COAReferenceNumber', 'casestatus.Notes2', 
            'casestatus.HighCourtReference', 'casestatus.AdminCourtReference', 'casestatus.HearingCourt', 'casestatus.ApplicationType',  
            'IRISStatusOfCase','ListTypeDescription','HearingTypeDescription','Judiciary1Name','Judiciary2Name','Judiciary3Name','ReasonAdjourn', 
            'adjournDateReceived', 'adjournmiscdate2', 'adjournParty', 'adjournInTime', 'adjournLetter1Date', 'adjournLetter2Date', 
            'adjournAdjudicatorSurname', 'adjournAdjudicatorForenames', 'adjournAdjudicatorTitle', concat_ws(" ", concat_ws(", ", col("adjournAdjudicatorSurname"), col("adjournAdjudicatorForenames")), when(col("adjournAdjudicatorTitle").isNotNull(), concat(lit("("), col("adjournAdjudicatorTitle"), lit(")")))).alias("adjournAdjudicatorFullName"), 'adjournNotes1', 
            'adjournDecisionDate', 'adjournPromulgated', 'HearingCentreDesc', 'CourtName', 'ListName', 'ListTypeDesc', 
            'HearingTypeDesc', 'ListStartTime', 'StartTime', 'TimeEstimate',  'casestatus.LanguageDescription','casestatus.CaseAdjudicatorsDetails','casestatus.ReviewSpecficDirectionDetails','casestatus.ReviewStandardDirectionDirectionDetails','lookup.HTMLName','LatestKeyDate','LatestAdjudicatorSurname','LatestAdjudicatorForenames','LatestAdjudicatorId','LatestAdjudicatorTitle', concat_ws(" ", concat_ws(", ", col("LatestAdjudicatorSurname"), col("LatestAdjudicatorForenames")), when(col("LatestAdjudicatorTitle").isNotNull(), concat(lit("("), col("LatestAdjudicatorTitle"), lit(")")))).alias("LatestAdjudicatorFullName"),'JudgeLabel1','JudgeLabel2','JudgeLabel3','Label1_JudgeValue','Label2_JudgeValue','Label3_JudgeValue','CourtClerkUsher', concat_ws(" ", concat_ws(", ", col("StatusDetailAdjudicatorSurname"), col("StatusDetailAdjudicatorForenames")), when(col("StatusDetailAdjudicatorTitle").isNotNull(), concat(lit("("), col("StatusDetailAdjudicatorTitle"), lit(")")))).alias("StatusDetailAdjudicatorFullName"),"adjournApplicationType","adjournKeyDate","CMROrder")).alias("TempCaseStatusDetails"))
    
    return df_final

# COMMAND ----------

# DBTITLE 1,Transformation: stg_apl_combined
@dlt.table(
    name="stg_apl_combined",
    comment="Delta Live unified stage created all consolidated data.",
    path=f"{silver_mnt}/stg_apl_combined"
)
def stg_apl_combined():

    # Read unique CaseNo tables
    # M1
    df_appealcase = dlt.read("silver_appealcase_detail")
    # M2
    df_applicant = dlt.read("silver_applicant_detail")
    # M10
    df_case_detail = dlt.read("silver_case_detail")
    #M22
    # df_hearingpointschange = dlt.read("silver_hearingpointschange_detail")

    df_hearingpointschange = dlt.read("silver_hearingpointschange_detail").groupBy("CaseNo").agg(
    collect_list(
        struct(
            'StatusId',
            'HearingPointsChangeReasonId',
            'Description',
            'HearingPointsChangeDoNotUse'
        )
    ).alias("hearingpointschangedetail")
    )
    


    # Read duplicate CaseNo tables and aggregate them
    df_appealcategory = dlt.read("silver_appealcategory_detail").groupBy("CaseNo").agg(
        collect_list(
            struct( 'CategoryDescription', 'Flag',"Priority")
        ).alias("AppealCategoryDetails")
    )

    df_appealgrounds = dlt.read("silver_appealgrounds_detail").groupBy("CaseNo").agg(
        collect_list(struct( 'AppealTypeId', 'AppealTypeDescription')).alias("AppealGroundsDetails")
    )

    df_appealtypecategory = dlt.read("silver_appealtypecategory_detail").groupBy("CaseNo").agg(
        collect_list(struct('AppealTypeCategoryId', 'AppealTypeId', 'CategoryId', 'FeeExempt')).alias("AppealTypeCategorieDetails")
    )

    df_case_adjudicator = dlt.read("silver_case_adjudicator").groupBy("CaseNo").agg(
        collect_list(struct( 'Required', 'JudgeSurname', 'JudgeForenames', 'JudgeTitle')).alias("CaseAdjudicatorDetails")
    )

    df_costaward = dlt.read("silver_costaward_detail").groupBy("CaseNo").agg(
        collect_list(struct('CostAwardId', 'Name', 'Forenames', 'Title', 'DateOfApplication', 'TypeOfCostAward', 'ApplyingParty', 'PayingParty', 'MindedToAward', 'ObjectionToMindedToAward', 'CostsAwardDecision', 'DateOfDecision', 'CostsAmount', 'OutcomeOfAppeal', 'AppealStage', 'AppealStageDescription')).alias("CostAwardDetails")
    )

    df_dependent = dlt.read("silver_dependent_detail").groupBy("CaseNo").agg(
        collect_list(struct('AppellantId', 'CaseAppellantRelationship', 'PortReference', 'AppellantName', 'AppellantForenames', 'AppellantTitle', 'AppellantBirthDate', 'AppellantAddress1', 'AppellantAddress2', 'AppellantAddress3', 'AppellantAddress4', 'AppellantAddress5', 'AppellantPostcode', 'AppellantTelephone', 'AppellantFax', 'Detained', 'AppellantEmail', 'FCONumber', 'PrisonRef', 'DetentionCentre', 'CentreTitle', 'DetentionCentreType', 'DCAddress1', 'DCAddress2', 'DCAddress3', 'DCAddress4', 'DCAddress5', 'DCPostcode', 'DCFax', 'DCSdx', 'Country', 'DependentNationality', 'Code', 'DoNotUseCountry', 'CountrySdx', 'DoNotUseNationality')).alias("DependentDetails")
    )

    df_documents = dlt.read("silver_documents_detail").groupBy("CaseNo").agg(
        collect_list(struct( 'ReceivedDocumentId', 'DateRequested', 'DateRequired', 'DateReceived', 'NoLongerRequired', 'RepresentativeDate', 'POUDate', 'DocumentDescription', 'DoNotUse', 'Auditable')).alias("DocumentDetails")
    )

    df_hearingpointshistory = dlt.read("silver_hearingpointshistory_detail").groupBy("CaseNo").agg(
        collect_list(struct('CaseNo', 'StatusId', 'HearingPointsHistoryId', 'HistDate', 'HistType', 'UserId', 'DefaultPoints', 'InitialPoints', 'FinalPoints')).alias("HearingPointHistoryDetails")
    )

    df_history = dlt.read("silver_history_detail").groupBy("CaseNo").agg(
        collect_list(struct('HistoryId', 'CaseNo', 'HistDate', 'fileLocation', 'lastDocument', 'HistType', 'HistoryComment','DeletedByUser', 'StatusId', 'UserName', 'UserType', 'Fullname', 'Extension', 'DoNotUse', 'HistTypeDescription')).alias("HistoryDetails")
    )

    df_humanright = dlt.read("silver_humanright_detail").groupBy("CaseNo").agg(
        collect_list(struct('HumanRightId', 'HumanRightDescription', 'DoNotShow', 'Priority')).alias("HumanRightDetails")
    )

    df_newmatter = dlt.read("silver_newmatter_detail").groupBy("CaseNo").agg(
        collect_list(struct(
            'AppealNewMatterId', 'NewMatterId', 'AppealNewMatterNotes', 'DateReceived', 
            'DateReferredToHO', 'HODecision', 'DateHODecision', 'NewMatterDescription', 
            'NotesRequired', 'DoNotUse'
        )).alias("NewMatterDetails")
    )

    df_link = dlt.read("silver_link_detail").groupBy("CaseNo").agg(
        collect_list(struct( 'LinkNo', 'LinkDetailComment', 'LinkName', 'LinkForeNames', 'LinkTitle')).alias("LinkedCaseDetails")
    )

    df_linkedcostaward = dlt.read("silver_linkedcostaward_detail").groupBy("CaseNo").agg(
        collect_list(struct('CostAwardId', 'CaseNo', 'LinkNo', 'Name', 'Forenames', 'Title', 'DateOfApplication', 'TypeOfCostAward', 'ApplyingParty', 'PayingParty', 'MindedToAward', 'ObjectionToMindedToAward', 'CostsAwardDecision', 'DateOfDecision', 'CostsAmount', 'OutcomeOfAppeal', 'AppealStage', 'AppealStageDescription')).alias("LinkedCostAwardDetails")
    )

    df_costorder = dlt.read("silver_costorder_detail").groupBy("CaseNo").agg(
        collect_list(
            struct('CostOrderID', 'DateOfApplication', 'OutcomeOfAppealWhereDecisionMade', 
                'DateOfDecision', 'ApplyingRepresentativeId', 'ApplyingRepresentativeName', 
                'OutcomeOfAppealWhereDecisionMadeDescription', 'AppealStageWhenApplicationMade', 
                'AppealStageWhenDecisionMade', 'CostOrderDecision')
        ).alias("CostOrderDetails")
    )


    df_list_detail = dlt.read("silver_list_detail").groupBy("CaseNo").agg(
        collect_list(struct('Outcome', 'CaseStatus', 'StatusId', 'TimeEstimate', 'ListNumber', 'HearingDuration', 'StartTime', 'HearingTypeDesc', 'HearingTypeEst', 'DoNotUse', 'ListAdjudicatorId', 'ListAdjudicatorSurname', 'ListAdjudicatorForenames', 'ListAdjudicatorNote', 'ListAdjudicatorTitle', 'ListName', 'ListStartTime', 'ListTypeDesc', 'ListType', 'DoNotUseListType', 'CourtName', 'DoNotUseCourt', 'HearingCentreDesc','UpperTribJudge','DesJudgeFirstTier','JudgeFirstTier','NonLegalMember')).alias("ListDetails")
    )


    df_dfdairy = dlt.read("silver_dfdairy_detail").groupBy("CaseNo").agg(
        collect_list(struct('CaseNo', 'Entry', 'EntryDate','BFDate', 'DateCompleted', 'Reason', 'BFTypeDescription', 'DoNotUse')).alias("BFDairyDetails")
    )
    

    df_required_incompatible_adjudicator = dlt.read("silver_required_incompatible_adjudicator").groupBy("CaseNo").agg(
        collect_list(struct('Required', 'JudgeSurname', 'JudgeForenames', 'JudgeTitle')).alias("RequiredIncompatibleAdjudicatorDetails")
    )

    df_status = dlt.read("silver_status_detail").groupBy("CaseNo").agg(
        collect_list(struct('StatusId', 'CaseNo', 'CaseStatus', 'DateReceived', 'StatusDetailAdjudicatorId', 'Allegation', 'KeyDate', 'MiscDate1', 'Notes1', 'Party', 'InTime', 'MiscDate2', 'MiscDate3', 'Notes2', 'DecisionDate', 'Outcome', 'Promulgated', 'InterpreterRequired', 'AdminCourtReference', 'UKAITNo', 'FC', 'VideoLink', 'Process', 'COAReferenceNumber', 'HighCourtReference', 'OutOfTime', 'ReconsiderationHearing', 'DecisionSentToHO', 'DecisionSentToHODate', 'MethodOfTyping', 'CourtSelection', 'DecidingCentre', 'Tier', 'RemittalOutcome', 'UpperTribunalAppellant', 'ListRequirementTypeId', 'UpperTribunalHearingDirectionId', 'ApplicationType', 'NoCertAwardDate', 'CertRevokedDate', 'WrittenOffFileDate', 'ReferredEnforceDate', 'Letter1Date', 'Letter2Date', 'Letter3Date', 'ReferredFinanceDate', 'WrittenOffDate', 'CourtActionAuthDate', 'BalancePaidDate', 'WrittenReasonsRequestedDate', 'TypistSentDate', 'TypistReceivedDate', 'WrittenReasonsSentDate', 'ExtemporeMethodOfTyping', 'Extempore', 'DecisionByTCW', 'InitialHearingPoints', 'FinalHearingPoints', 'HearingPointsChangeReasonId', 'OtherCondition', 'OutcomeReasons', 'AdditionalLanguageId', 'CostOrderAppliedFor', 'HearingCourt', 'CaseStatusDescription', 'DoNotUseCaseStatus', 'CaseStatusHearingPoints', 'ContactStatus', 'SCCourtName', 'SCAddress1', 'SCAddress2', 'SCAddress3', 'SCAddress4', 'SCAddress5', 'SCPostcode', 'SCTelephone', 'SCForenames', 'SCTitle', 'ReasonAdjourn', 'DoNotUseReason', 'LanguageDescription', 'DoNotUseLanguage', 'DecisionTypeDescription', 'DeterminationRequired', 'DoNotUse', 'State', 'BailRefusal', 'BailHOConsent', 'StatusDetailAdjudicatorSurname', 'StatusDetailAdjudicatorForenames', 'StatusDetailAdjudicatorTitle', 'StatusDetailAdjudicatorNote', 'DeterminationByJudgeSurname', 'DeterminationByJudgeForenames', 'DeterminationByJudgeTitle', 'CurrentStatus', 'AdjournmentParentStatusId')).alias("StatusDetails")
    )

    df_statusdecisiontype = dlt.read("silver_statusdecisiontype_detail").groupBy("CaseNo").agg(
        collect_list(struct('DecisionTypeDescription', 'DeterminationRequired', 'DoNotUse', 'State', 'BailRefusal', 'BailHOConsent')).alias("StatusDecisionTypeDetails")
    )

    df_transaction = dlt.read("silver_transaction_detail").groupBy("CaseNo").agg(
        collect_list(struct('TransactionId', 'TransactionTypeId', 'TransactionMethodId', 'TransactionDate', 'Amount', 'ClearedDate', 'TransactionStatusId', 'OriginalPaymentReference', 'PaymentReference', 'AggregatedPaymentURN', 'PayerForename', 'PayerSurname', 'LiberataNotifiedDate', 'LiberataNotifiedAggregatedPaymentDate', 'BarclaycardTransactionId', 'Last4DigitsCard', 'TransactionNotes', 'ExpectedDate', 'ReferringTransactionId', 'CreateUserId', 'LastEditUserId', 'TransactionDescription', 'InterfaceDescription', 'AllowIfNew', 'DoNotUse', 'SumFeeAdjustment', 'SumPayAdjustment', 'SumTotalFee', 'SumTotalPay', 'SumBalance', 'GridFeeColumn', 'GridPayColumn', 'IsReversal', 'TransactionStatusDesc', 'TransactionStatusIntDesc', 'DoNotUseTransactionStatus', 'TransactionMethodDesc', 'TransactionMethodIntDesc', 'DoNotUseTransactionMethod', 'AmountDue', 'AmountPaid', 'FirstTierFee', 'TotalFeeAdjustments', 'TotalFeeDue', 'TotalPaymentsReceived', 'TotalPaymentAdjustments', 'BalanceDue')).alias("TransactionDetails")
    )

    df_standarddirection = dlt.read("sliver_direction_detail").groupBy("CaseNo").agg(
        collect_list(struct(
            'ReviewStandardDirectionId', 'CaseNo', 'StatusId', 'StandardDirectionId', 
            'DateRequiredIND', 'DateRequiredAppellantRep', 'DateReceivedIND', 
            'DateReceivedAppellantRep', 'Description', 'DoNotUse'
        )).alias("StandardDirectionDetails")
    )

    df_reviewspecificdirection = dlt.read("silver_reviewspecificdirection_detail").groupBy("CaseNo").agg(
        collect_list(struct(
            'ReviewSpecificDirectionId', 'CaseNo', 'StatusId', 'SpecificDirection', 
            'DateRequiredIND', 'DateRequiredAppellantRep', 'DateReceivedIND', 'DateReceivedAppellantRep'
        )).alias("ReviewSpecficDirectionDetails")
    )

  
    # Join all tables
    df_combined = (
        df_appealcase
        .join(df_applicant, "CaseNo", "left")
        .join(df_dependent, "CaseNo", "left")
        .join(df_list_detail, "CaseNo", "left")
        .join(df_dfdairy, "CaseNo", "left")
        .join(df_history, "CaseNo", "left")
        .join(df_link, "CaseNo", "left")
        .join(df_status, "CaseNo", "left")
        .join(df_appealcategory, "CaseNo", "left")
        .join(df_case_detail, "CaseNo", "left")
        .join(df_transaction, "CaseNo", "left")
        .join(df_humanright, "CaseNo", "left")
        .join(df_newmatter, "CaseNo", "left")
        .join(df_documents, "CaseNo", "left")
        .join(df_standarddirection, "CaseNo", "left")
        .join(df_reviewspecificdirection, "CaseNo", "left")
        .join(df_costaward, "CaseNo", "left")
        .join(df_linkedcostaward, "CaseNo", "left")
        .join(df_costorder, "CaseNo", "left")
        .join(df_hearingpointschange, "CaseNo", "left")
        .join(df_hearingpointshistory, "CaseNo", "left")
        .join(df_appealtypecategory, "CaseNo", "left")
        .join(df_appealgrounds, "CaseNo", "left")
        .join(df_required_incompatible_adjudicator, "CaseNo", "left")
        .join(df_case_adjudicator, "CaseNo", "left")
        .join(df_statusdecisiontype, "CaseNo", "left") 
        
    )

    df_with_json_content = df_combined.withColumn("JSONcollection", to_json(struct(*df_combined.columns)))

    return df_with_json_content

# COMMAND ----------

# DBTITLE 1,Transformation: stg_apl_create_html_content
@dlt.table(
    name="stg_apl_create_html_content",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{silver_mnt}/stg_apl_create_html_content"
)
def stg_apl_create_html_content():

    # Read unique CaseNo tables
    # M1
    df_combined = dlt.read("stg_apl_combined")
    stg_appeals_filtered = spark.read.table("ariadm_active_appeals.stg_segmentation_states")

    #HTML extra requirement- status details data
    df_with_statusdetail_data = df_combined.join(dlt.read("stg_statusdetail_data"), "CaseNo", "left").join(dlt.read("stg_statichtml_data"), "CaseNo", "left").join(stg_appeals_filtered, "CaseNo", "left")

    Datetime_name = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    df_with_html_content = df_with_statusdetail_data.withColumn("HTML_Content", generate_html_udf(struct(*df_with_statusdetail_data.columns))).withColumn(
        "File_Name", concat(lit(f"{gold_outputs}/{Datetime_name}/"),upper(col("TargetState")),lit("/HTML/appeals_"), regexp_replace(col("CaseNo"), "/", "_"), lit(".html")) ).withColumn("Status", when((col("HTML_Content").like("Failure%") | col("HTML_Content").isNull()), "Failure on Create HTML Content").otherwise("Successful creating HTML Content"))
   

    return df_with_html_content.select("CaseNo","HTML_Content","File_Name","Status")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Outputs and Tracking DLT Table Creation

# COMMAND ----------

# DBTITLE 1,Transformation gold_appeals_with_html
checks = {}
checks["html_content_no_error"] = "(HTML_Content NOT LIKE 'Error%')"

@dlt.table(
    name="gold_appeals_with_html",
    comment="Delta Live Gold Table with HTML content and uploads.",
    path=f"{gold_mnt}/gold_appeals_with_html"
)
@dlt.expect_all_or_fail(checks)
def gold_appeals_with_html():
    # Load source data
    df_combined = dlt.read("stg_apl_create_html_content")

    # Repartition to optimize parallelism
    repartitioned_df = df_combined.repartition(200)

    # Trigger upload logic for each row
    df_with_upload_status = repartitioned_df.filter(~col("HTML_Content").like("Error%")).withColumn(
        "Status", upload_udf(col("File_Name"), col("HTML_Content"))
    )

    # Return the DataFrame for DLT table creation, including the upload status
    return df_with_upload_status.select("CaseNo", "HTML_Content", col("File_Name").alias("File_Name"), "Status")

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Success Message
dbutils.notebook.exit("Notebook completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix

# COMMAND ----------

# df = spark.sql("SELECT * FROM hive_metastore.ariadm_active_appeals_cdam_html.gold_appeals_with_html")
# display(df)
# display(df.filter(col("HTML_Content").contains("Error generating HTML")))

# COMMAND ----------

# DBTITLE 1,Display HTML Content
# case_no = 'HU/00008/2015'
# case_no = 'PA/00009/2015'
# df = spark.sql("SELECT * FROM hive_metastore.ariadm_active_appeals_cdam_html.stg_apl_create_html_content")

# # display(df)
# # Filter for the specific case and extract the JSON collection
# filtered_row = df.filter(col("CaseNo") == case_no).select("HTML_Content").first()

# displayHTML(filtered_row["HTML_Content"])
