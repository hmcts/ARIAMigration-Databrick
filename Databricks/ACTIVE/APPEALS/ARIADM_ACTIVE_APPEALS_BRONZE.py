# Databricks notebook source
# MAGIC %md
# MAGIC # Active Appeals CCD MVP Base Build (State: PaymentPending)
# MAGIC <table style='float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>Name: </b></td>
# MAGIC          <td>ARIADM_CCD_PaymentPending</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>Description: </b></td>
# MAGIC          <td>Notebook to generate a set of JSON files, each representing data about active appeals and covering payment pending state as part of this initial notebook.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>First Created: </b></td>
# MAGIC          <td>May-2025</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <th style='text-align: left;'><b>Changelog (JIRA ref/initials/date):</b></th>
# MAGIC          <th>Comments</th>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><a href="https://tools.hmcts.net/jira/browse/ARIADM-634">ARIADM-634</a>/NSA/SEP-2024</td>
# MAGIC          <td>Active CCD MVP Base Build: Create Raw Bronze Tables from Landing Data</td>
# MAGIC       </tr>
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

# DBTITLE 1,Set Paths and Hive Schema Variables
# read_hive = False

# Setting variables for use in subsequent cells
raw_mnt = f"abfss://raw@ingest{lz_key}raw{env_name}.dfs.core.windows.net/ARIADM/ACTIVE/CCD/APPEALS"
landing_mnt = f"abfss://landing@ingest{lz_key}landing{env_name}.dfs.core.windows.net/SQLServer/Sales/IRIS/dbo/"
bronze_mnt = f"abfss://bronze@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/ACTIVE/CCD/APPEALS"
silver_mnt = f"abfss://silver@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/ACTIVE/CCD/APPEALS"
gold_mnt = f"abfss://gold@ingest{lz_key}curated{env_name}.dfs.core.windows.net/ARIADM/ACTIVE/CCD/APPEALS"
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
# MAGIC ## Raw DLT Tables Creation

# COMMAND ----------

@dlt.table(
    name="raw_appealcase",
    comment="Delta Live Table ARIA AppealCase.",
    path=f"{raw_mnt}/Raw_AppealCase"
)
def Raw_AppealCase():
    return read_latest_parquet("AppealCase", "tv_AppealCase", "ARIA_ACTIVE_APPEALS")

@dlt.table(
    name="raw_caserep",
    comment="Delta Live Table ARIA CaseRep.",
    path=f"{raw_mnt}/Raw_CaseRep"
)
def raw_CaseRep():
     return read_latest_parquet("CaseRep", "tv_CaseRep", "ARIA_ACTIVE_APPEALS") 
 

@dlt.table(
    name="raw_representative",
    comment="Delta Live Table ARIA Representative.",
    path=f"{raw_mnt}/Raw_Representative"
)
def raw_Representative():
     return read_latest_parquet("Representative", "tv_Representative", "ARIA_ACTIVE_APPEALS")

@dlt.table(
    name="raw_casesponsor",
    comment="Delta Live Table ARIA CaseSponsor.",
    path=f"{raw_mnt}/Raw_CaseSponsor"
)
def raw_CaseSponsor():
     return read_latest_parquet("CaseSponsor", "tv_CaseSponsor", "ARIA_ACTIVE_APPEALS") 
 
@dlt.table(
    name="raw_caserespondent",
    comment="Delta Live Table ARIA CaseRespondent.",
    path=f"{raw_mnt}/Raw_CaseRespondent"
)
def CaseRespondent():
    return read_latest_parquet("CaseRespondent", "tv_CaseRespondent", "ARIA_ACTIVE_APPEALS")

@dlt.table(
    name="raw_filelocation",
    comment="Delta Live Table ARIA FileLocation.",
    path=f"{raw_mnt}/Raw_FileLocation"
)
def raw_FileLocation():
     return read_latest_parquet("FileLocation", "tv_FileLocation", "ARIA_ACTIVE_APPEALS") 
 
@dlt.table(
    name="raw_casefeesummary",
    comment="Delta Live Table ARIA CaseFeeSummary.",
    path=f"{raw_mnt}/Raw_CaseFeeSummary"
)
def raw_CaseFeeSummary():
    return read_latest_parquet("CaseFeeSummary", "tv_CaseFeeSummary", "ARIA_ACTIVE_APPEALS")  
 
@dlt.table(
    name="raw_caseappellant",
    comment="Delta Live Table ARIA CaseAppellant.",
    path=f"{raw_mnt}/Raw_CaseAppellant"
)
def raw_CaseAppellant():
     return read_latest_parquet("CaseAppellant", "tv_CaseAppellant", "ARIA_ACTIVE_APPEALS")
 
@dlt.table(
    name="raw_appellant",
    comment="Delta Live Table ARIA Appellant.",
    path=f"{raw_mnt}/Raw_Appellant"
)
def raw_Appellant():
     return read_latest_parquet("Appellant", "tv_Appellant", "ARIA_ACTIVE_APPEALS") 
 
@dlt.table(
    name="raw_transaction",
    comment="Delta Live Table ARIA Transaction.",
    path=f"{raw_mnt}/Raw_Transaction"
)
def raw_Transaction():
     return read_latest_parquet("Transaction", "tv_Transaction", "ARIA_ACTIVE_APPEALS")   

@dlt.table(
    name="raw_transactiontype",
    comment="Delta Live Table ARIA TransactionType.",
    path=f"{raw_mnt}/Raw_TransactionType"
)
def raw_TransactionType():
     return read_latest_parquet("TransactionType", "tv_TransactionType", "ARIA_ACTIVE_APPEALS")  
 
@dlt.table(
    name="raw_link",
    comment="Delta Live Table ARIA Link.",
    path=f"{raw_mnt}/Raw_Link"
)
def raw_Link():
     return read_latest_parquet("Link", "tv_Link", "ARIA_ACTIVE_APPEALS")  
 
@dlt.table(
    name="raw_linkdetail",
    comment="Delta Live Table ARIA LinkDetail.",
    path=f"{raw_mnt}/Raw_LinkDetail"
)
def raw_LinkDetail():
     return read_latest_parquet("LinkDetail", "tv_LinkDetail", "ARIA_ACTIVE_APPEALS")  
 
@dlt.table(
    name="raw_caseadjudicator",
    comment="Delta Live Table ARIA AppealTypeCategory.",
    path=f"{raw_mnt}/raw_caseadjudicator"
)
def raw_caseadjudicator():
     return read_latest_parquet("CaseAdjudicator", "tv_caseadjudicator", "ARIA_ACTIVE_APPEALS") 
 
@dlt.table(
    name="raw_adjudicator",
    comment="Delta Live Table ARIA Adjudicator.",
    path=f"{raw_mnt}/Raw_Adjudicator"
)
def raw_Adjudicator():
     return read_latest_parquet("Adjudicator", "tv_Adjudicator", "ARIA_ACTIVE_APPEALS")
 
@dlt.table(
    name="raw_appealcategory",
    comment="Delta Live Table ARIA AppealCategory.",
    path=f"{raw_mnt}/Raw_AppealCategory"
)
def raw_AppealCategory():
     return read_latest_parquet("AppealCategory", "tv_AppealCategory", "ARIA_ACTIVE_APPEALS") 
 
@dlt.table(
    name="raw_documentsreceived",
    comment="Delta Live Table ARIA DocumentsReceived.",
    path=f"{raw_mnt}/Raw_DocumentsReceived"
)
def raw_DocumentsReceived():
     return read_latest_parquet("DocumentsReceived", "tv_DocumentsReceived", "ARIA_ACTIVE_APPEALS")  
 
@dlt.table(
    name="raw_history",
    comment="Delta Live Table ARIA History.",
    path=f"{raw_mnt}/Raw_History"
)
def raw_History():
     return read_latest_parquet("History", "tv_History", "ARIA_ACTIVE_APPEALS")
 

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
    name="raw_list",
    comment="Delta Live Table ARIA Status.",
    path=f"{raw_mnt}/raw_list"
)
def raw_list():
     return read_latest_parquet("List", "tv_List", "ARIA_ACTIVE_APPEALS") 
 

@dlt.table(
    name="raw_listtype",
    comment="Delta Live Table ARIA Status.",
    path=f"{raw_mnt}/raw_listtype"
)
def raw_listtype():
     return read_latest_parquet("ListType", "tv_ListType", "ARIA_ACTIVE_APPEALS") 
 
@dlt.table(
    name="raw_hearingtype",
    comment="Delta Live Table ARIA Status.",
    path=f"{raw_mnt}/raw_hearingtype"
)
def raw_hearingtype():
     return read_latest_parquet("HearingType", "tv_HearingType", "ARIA_ACTIVE_APPEALS") 
 
@dlt.table(
    name="raw_court",
    comment="Delta Live Table ARIA Status.",
    path=f"{raw_mnt}/raw_court"
)
def raw_court():
     return read_latest_parquet("Court", "tv_Court", "ARIA_ACTIVE_APPEALS")


@dlt.table(
    name="raw_listsitting",
    comment="Delta Live Table ARIA Status.",
    path=f"{raw_mnt}/raw_listsitting"
)
def raw_listsitting():
     return read_latest_parquet("ListSitting", "tv_ListSitting", "ARIA_ACTIVE_APPEALS") 
 

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

@dlt.table(
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
            # TransactionType 
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

# MAGIC %md
# MAGIC ## INDEV: Segmentation DLT Tables Creation - stg_segmentation_states 
# MAGIC

# COMMAND ----------

# DBTITLE 1,stg_balance_due
# @dlt.table(
#     name="stg_balance_due",
#     comment="DLT table for balance due per CaseNo based on specific transaction rules.",
#     path=f"{silver_mnt}/stg_balance_due"
# )
# def stg_balance_due():
#     # Read transaction and transaction type data
#     tx = dlt.read("bronze_appealcase_transaction_transactiontype")
    
#     # Filter for balance-related transactions
#     filtered_tx = (
#         tx.filter((col("SumBalance") == 1) & (~col("TransactionId").isin(
#             dlt.read("bronze_appealcase_transaction_transactiontype")
#                 .filter(col("TransactionTypeId").isin([6, 19]))
#                 .select("ReferringTransactionId")
#                 .rdd.flatMap(lambda x: x).collect()
#         )))
#     )

#     # Aggregate by CaseNo
#     return (
#         filtered_tx
#             .groupBy("CaseNo")
#             .agg(sum("Amount").alias("BalanceDue"))
#     )


# COMMAND ----------

# DBTITLE 1,stg_representation
# @dlt.table(
#     name="stg_representation",
#     comment="DLT table to determine if each case is AIP or LR based on CaseRep information.",
#     path=f"{silver_mnt}/stg_representation"
# )
# def stg_representation():
#     # Read the AppealCase and CaseRep DLT tables
#     appeal_case = dlt.read("raw_appealcase").filter(col("CaseType") == 1)
#     case_rep = dlt.read("raw_caserep")

#     # Join AppealCase and CaseRep
#     df = appeal_case.join(case_rep, on="CaseNo", how="left")

#     # Clean logic for determining Representation
#     aip_conditions = (
#         (col("RepresentativeID") == 957) |  # RepresentativeID is 957
#         (col("RepresentativeID").isNull()) |  # RepresentativeID is NULL
#         lower(col("Name")).rlike(".*no\\s+rep.*") |  # Name contains 'no rep'
#         lower(col("Name")).rlike(".*none.*") |  # Name contains 'none'
#         lower(col("Name")).rlike(".*unrepresented.*") |  # Name contains 'unrepresented'
#         lower(col("Name")).rlike(".*self.*") |  # Name contains 'self'
#         lower(col("Name")).rlike(".*not\\s+rep.*") |  # Name contains 'not rep'
#         (col("Name") == "-") |  # Name is '-'
#         lower(col("Name")).rlike(".*\\(spo.*") |  # Name contains '(spo'
#         lower(col("Name")).rlike(".*spons.*")  # Name contains 'spons'
#     )

#     return df.select(
#         col("CaseNo"),
#         when(aip_conditions, "AIP").otherwise("LR").alias("Representation")
#     )

# COMMAND ----------

# spark.sql("select * from ariadm_active_appeals.raw_status").printSchema()

# COMMAND ----------

# from pyspark.sql.functions import when, max as spark_max, add_months
# from pyspark.sql import functions as F

# # from pyspark.sql.functions import col, current_date, add_months, add_years

# @dlt.table(
#     name="stg_segmentation_states",
#     comment="Bronze DLT table replicating the AppealCase join logic.",
#     path=f"{silver_mnt}/stg_segmentation_states"
# )
# def stg_segmentation_states():
#     # Base tables
#     appealcase = dlt.read("raw_appealcase")
#     status = dlt.read("raw_status")
#     caselist = dlt.read("raw_caselist")
#     documentsreceived = dlt.read("raw_documentsreceived")
#     filelocation = dlt.read("raw_filelocation")
#     balancedue = dlt.read("stg_balance_due")
#     representation = dlt.read("stg_representation")
                  
#     s_subquery01 = (
#         status.filter(((~col("outcome").isin([38, 111]))) &
#                   ((~col("casestatus").isin(['17']))))
#           .groupBy("CaseNo")
#           .agg(spark_max("StatusId").alias("max_ID"))
#     )
                  
#     # Previous status logic
#     prev_subquery02 = (
#         status.filter((col("casestatus").isNull()) | (~col("casestatus").isin(['50', '52', '36'])))
#           .groupBy("CaseNo")
#           .agg(spark_max("StatusId").alias("Prev_ID"))
#     )

#     # UT status
#     ut_subquery03 = (
#         status.filter(col("CaseStatus").isin(['40', '41', '42', '43', '44', '45', '53', '27', '28', '29', '34', '32', '33']))
#           .groupBy("CaseNo")
#           .agg(spark_max("StatusId").alias("UT_ID"))
#     )

#     # Latest document received
#     docreceived_subquery04 = (
#         documentsreceived.groupBy("CaseNo")
#           .agg(spark_max("ReceivedDocumentId").alias("max_ReceivedDocumentId"))
#     )

#     # SA previous status
#     sa_prev_subquery05 = (
#         status.filter((col("casestatus").isNull()) | (col("casestatus") != '46'))
#           .groupBy("CaseNo")
#           .agg(spark_max("StatusId").alias("Prev_ID"))
#     )

#     # Final join
#     result = (
#         appealcase.alias("ac")
#         .join(s_subquery01.alias("s"), col("ac.CaseNo") == col("s.CaseNo"), "left")
#         .join(status.alias("t"), (col("t.CaseNo") == col("s.CaseNo")) & (col("t.StatusId") == col("s.max_ID")), "left")
#         .join(caselist.alias("cl"), col("t.StatusId") == col("cl.StatusId"), "left")
#         .join(prev_subquery02.alias("prev"), col("ac.CaseNo") == col("prev.CaseNo"), "left")
#         .join(status.alias("st"), (col("st.CaseNo") == col("prev.CaseNo")) & (col("st.StatusId") == col("prev.Prev_ID")), "left")
#         .join(ut_subquery03.alias("ut"), col("ac.CaseNo") == col("ut.CaseNo"), "left")
#         .join(status.alias("us"), (col("us.CaseNo") == col("ut.CaseNo")) & (col("us.StatusId") == col("ut.UT_ID")), "left")
#         .join(docreceived_subquery04.alias("doc_received"), col("ac.CaseNo") == col("doc_received.CaseNo"), "left")
#         .join(documentsreceived.alias("dr"), (col("dr.CaseNo") == col("doc_received.CaseNo")) & (col("dr.ReceivedDocumentId") == col("doc_received.max_ReceivedDocumentId")), "left")
#         .join(sa_prev_subquery05.alias("sa_prev"), col("ac.CaseNo") == col("sa_prev.CaseNo"), "left")
#         .join(status.alias("sa"), (col("sa.CaseNo") == col("sa_prev.CaseNo")) & (col("sa.StatusId") == col("sa_prev.Prev_ID")), "left")
#         .join(filelocation.alias("fl"), col("ac.CaseNo") == col("fl.CaseNo"), "left")
#         .join(balancedue.alias("bd"), col("ac.CaseNo") == col("bd.CaseNo"), "left")
#         .join(representation.alias("r"), col("ac.CaseNo") == col("r.CaseNo"), "left")
#         .filter((col("ac.CaseType") == 1) & (~col("fl.DeptId").isin(519, 520)))
#         .select(
#             "ac.CaseNo", 
#             # "ac.CasePrefix",
#             when(
#                 (col("ac.CasePrefix").isin("EA", "HU")) &
#                 ((col("t.CaseStatus").isNull()) |
#                  ((col("t.CaseStatus") == '10') & col("t.Outcome").isin("0", "109", "99", "104", "82", "121")) |
#                  ((col("t.CaseStatus") == '46') & (col("t.Outcome") == 1) & (col("sa.CaseStatus") == '10'))) &
#                 ((col("dr.ReceivedDocumentId") <= 2) | col("dr.ReceivedDocumentId").isNull()) &
#                 (col("bd.BalanceDue") > 0), "paymentPending"
#             ).when(
#                 (col("ac.CasePrefix").isin("EA", "HU")) &
#                 (col("t.CaseStatus").isin("50", "52", "36")) & (col("t.Outcome") == 0) &
#                 ((col("st.CaseStatus") == '10') & (col("st.Outcome") == 0) | col("st.CaseStatus").isNull()) &
#                 ((col("dr.ReceivedDocumentId") <= 2) | col("dr.ReceivedDocumentId").isNull()) &
#                 (col("bd.BalanceDue") > 0), "paymentPending"
#             ).when(
#                 ((col("ac.CasePrefix").isin("EA", "HU") & ((col("bd.BalanceDue") <= 0) | col("bd.BalanceDue").isNull())) |
#                  col("ac.CasePrefix").isin("PA", "DC", "RP", "DA", "IA", "LE", "LP", "LR", "LD", "LH")) &
#                 ((col("t.CaseStatus").isNull()) |
#                  ((col("t.CaseStatus") == '10') & col("t.Outcome").isin("0", "109", "99", "104", "82", "121")) |
#                  ((col("t.CaseStatus") == '46') & (col("t.Outcome") == 1) & (col("sa.CaseStatus") == '10'))) &
#                 ((col("dr.ReceivedDocumentId") <= 2) | col("dr.ReceivedDocumentId").isNull()), "appealSubmitted"
#             ).when(
#                 ((col("ac.CasePrefix").isin("EA", "HU") & ((col("bd.BalanceDue") <= 0) | col("bd.BalanceDue").isNull())) |
#                  col("ac.CasePrefix").isin("PA", "DC", "RP", "DA", "IA", "LE", "LP", "LR", "LD", "LH")) &
#                 (col("t.CaseStatus").isin("50", "52", "36")) & (col("t.Outcome") == 0) &
#                 ((col("st.CaseStatus") == '10') & (col("st.Outcome") == 0) | col("st.CaseStatus").isNull()) &
#                 ((col("dr.ReceivedDocumentId") <= 2) | col("dr.ReceivedDocumentId").isNull()), "appealSubmitted"
#             ).when(
#                 ((col("t.CaseStatus").isNull()) |
#                  ((col("t.CaseStatus") == '10') & col("t.Outcome").isin("0", "109", "99", "121", "82", "104")) |
#                  ((col("t.CaseStatus") == '46') & (col("t.Outcome") == 1) & (col("sa.CaseStatus") == '10'))) &
#                 (col("dr.ReceivedDocumentId") == 3) &
#                 col("dr.DateReceived").isNull(), "awaitingRespondentEvidence(a)"
#             ).when(
#                 (col("t.CaseStatus").isin("50", "52", "36")) & (col("t.Outcome") == 0) &
#                 ((col("st.CaseStatus") == '10') & (col("st.Outcome") == 0) | col("st.CaseStatus").isNull()) &
#                 (col("dr.ReceivedDocumentId") == 3) &
#                 col("dr.DateReceived").isNull(), "awaitingRespondentEvidence(a)"
#             ).when(
#                 ((col("t.CaseStatus").isNull()) |
#                  ((col("t.CaseStatus") == '10') & (col("t.Outcome") == 0)) |
#                  ((col("t.CaseStatus") == '46') & (col("t.Outcome") == 1) & (col("sa.CaseStatus") == '10'))) &
#                 (col("dr.ReceivedDocumentId") == 3) &
#                 col("dr.DateReceived").isNotNull(), "awaitingRespondentEvidence(b)"
#             ).when(
#                 (col("t.CaseStatus").isin("50", "52", "36")) & (col("t.Outcome") == 0) &
#                 ((col("st.CaseStatus") == '10') & (col("st.Outcome") == 0) | col("st.CaseStatus").isNull()) &
#                 (col("dr.ReceivedDocumentId") == 3) &
#                 col("dr.DateReceived").isNotNull(), "awaitingRespondentEvidence(b)"
#             ).when(
#                 ((col("t.CaseStatus") == '10') & col("t.Outcome").isin("121", "109", "99", "82", "104") &
#                  (col("dr.ReceivedDocumentId") == 3) & col("dr.DateReceived").isNotNull()) |
#                 ((col("t.CaseStatus") == '26') & col("t.Outcome").isin("0", "27", "39", "50", "89")) &
#                 (col("r.Representation") == "LR"), "caseUnderReview"
#             ).when(
#                 (col("t.CaseStatus").isin("50", "52", "36")) & (col("t.Outcome") == 0) &
#                 ((col("st.CaseStatus") == '26') & col("st.Outcome").isin("0", "27", "39", "50", "89")) &
#                 (col("r.Representation") == "LR"), "caseUnderReview"
#             ).when(
#                 ((col("t.CaseStatus") == '10') & col("t.Outcome").isin("121", "109", "99", "82", "104") &
#                  (col("dr.ReceivedDocumentId") == 3) & col("dr.DateReceived").isNotNull()) |
#                 ((col("t.CaseStatus") == '26') & col("t.Outcome").isin("0", "27", "39", "50", "89")) &
#                 (col("r.Representation") == "AIP"), "reasonsForAppealSubmitted"
#             ).when(
#                 (col("t.CaseStatus").isin("50", "52", "36")) & (col("t.Outcome") == 0) &
#                 ((col("st.CaseStatus") == '26') & col("st.Outcome").isin("0", "27", "39", "50", "89")) &
#                 (col("r.Representation") == "AIP"), "reasonsForAppealSubmitted"
#             ).when(
#                 ((col("t.CaseStatus") == '26') & col("t.Outcome").isin("40", "52")) |
#                 ((col("t.CaseStatus").isin("37", "38")) & col("t.Outcome").isin("39", "40", "37", "50", "27", "5")) |
#                 ((col("t.CaseStatus").isin("37", "38")) & (col("t.Outcome") == 0) & col("t.KeyDate").isNull()), "listing"
#             ).when(
#                 (col("t.CaseStatus").isin("50", "52", "36")) & (col("t.Outcome") == 0) &
#                 ((col("st.CaseStatus") == '26') & col("st.Outcome").isin("40", "52")), "listing"
#             ).when(
#                 ((col("t.CaseStatus").isin("37", "38")) & (col("t.Outcome") == 0) &
#                  (col("t.KeyDate") > current_date()) & (col("t.DecisionReserved") != 1) &
#                  col("cl.ListId").isNotNull()), "prepareForHearing"
#             ).when(
#                 ((col("t.CaseStatus").isin("37", "38")) & (col("t.Outcome") == 0) &
#                  col("t.KeyDate").isNotNull() & (col("t.DecisionReserved") == 1)), "decision"
#             ).when(
#                 ((col("t.CaseStatus").isin("46", "39")) & (col("t.Outcome") == 86)) |
#                 ((col("t.CaseStatus") == '46') & (col("t.Outcome") == 1) & (col("sa.CaseStatus") != 10)), "decided(b)"
#             ).when(
#                 ((col("t.CaseStatus") == '39') & (col("t.Outcome") == 0) & (col("t.AdjudicatorId") == 0)), "fptaSubmitted(a)"
#             ).when(
#                 ((col("t.CaseStatus") == '39') & (col("t.Outcome") == 0) & (col("t.AdjudicatorId") != 0)), "ftpaSubmitted(b)"
#             ).when(
#                 ((col("t.CaseStatus") == '46') & (col("t.Outcome") == 31) & col("sa.CaseStatus").isin("37", "38")) |
#                 ((col("t.CaseStatus") == '26') & col("t.Outcome").isin("1", "2")) |
#                 ((col("t.CaseStatus").isin("37", "38")) & col("t.Outcome").isin("1", "2")) |
#                 ((col("t.CaseStatus") == '52') & col("t.Outcome").isin("91", "95") & col("st.CaseStatus").isin("37", "38", "17")) |
#                 ((col("t.CaseStatus") == '36') & col("t.Outcome").isin("1", "2", "25") & col("st.CaseStatus").isin("37", "38", "17")), "decided(a)"
#             ).when(
#                 (col("t.CaseStatus").isin("50", "52", "36")) & (col("t.Outcome") == 0) &
#                 ((col("st.CaseStatus").isin("37", "38")) & col("st.Outcome").isin("1", "2") | (col("st.CaseStatus") == 17)), "decided(a)"
#             ).when(
#                 ((col("t.CaseStatus") == '46') & (col("t.Outcome") == 31) & (col("sa.CaseStatus") == 39)) |
#                 ((col("t.CaseStatus") == '39') & col("t.Outcome").isin("30", "31", "14")) |
#                 ((col("t.CaseStatus") == '52') & col("t.Outcome").isin("91", "95") & (col("st.CaseStatus") == 39)) |
#                 ((col("t.CaseStatus") == '36') & col("t.Outcome").isin("1", "2", "25") & (col("st.CaseStatus") == 39)), "ftpaDecided"
#             ).when(
#                 (col("t.CaseStatus").isin("50", "52", "36")) & (col("t.Outcome") == 0) &
#                 ((col("st.CaseStatus") == '39') & col("st.Outcome").isin("31", "30", "14")), "ftpaDecided"
#             ).when(
#                 ((col("t.CaseStatus") == '10') & col("t.Outcome").isin("80", "122", "25", "120", "2", "105", "13")) |
#                 ((col("t.CaseStatus") == '46') & (col("t.Outcome") == 31) & col("sa.CaseStatus").isin("10", "51", "52")) |
#                 ((col("t.CaseStatus") == '26') & col("t.Outcome").isin("80", "13", "25")) |
#                 ((col("t.CaseStatus").isin("37", "38")) & col("t.Outcome").isin("80", "13", "25", "72", "125")) |
#                 ((col("t.CaseStatus") == '39') & (col("t.Outcome") == 25)) |
#                 ((col("t.CaseStatus") == '51') & col("t.Outcome").isin("94", "93")) |
#                 ((col("t.CaseStatus") == '52') & col("t.Outcome").isin("91", "95") & (~col("st.CaseStatus").isin("37", "38", "39", "17") | col("st.CaseStatus").isNull())) |
#                 ((col("t.CaseStatus") == '36') & col("t.Outcome").isin("1", "2", "25") & (~col("st.CaseStatus").isin("37", "38", "39", "17"))), "ended"
#             ).when(
#                 (col("t.CaseStatus").isin("50", "52", "36")) & (col("t.Outcome") == 0) &
#                 ((col("st.CaseStatus").isin("10", "26")) & col("st.Outcome").isin("80", "122", "2", "105", "120", "25") | (col("st.CaseStatus") == '51')), "ended"
#             ).when(
#                 (col("t.CaseStatus").isin("40", "41", "42", "43", "44", "53")) & (col("t.Outcome") == 86), "remitted"
#             ).otherwise("Not Sure").alias("TargetState"),


#             when(
#             (col("t.CaseStatus").isin('40','41','42','43','44','45','53','27','28','29','34','32','33')) & (col("t.Outcome") == '86'),
#             'CCD'
#             ).when(
#                 (col("t.CaseStatus").isin('40','41','42','43','44','45','53','27','28','29','34','32','33')) & (col("t.Outcome") == '0'),
#                 'UT Active'
#             ).when(
#                 (col("ac.CasePrefix").isin('VA','AA','AS','CC','HR','HX','IM','NS','OA','OC','RD','TH','XX')) &
#                 (col("t.CaseStatus").isin('40','41','42','43','44','45','53','27','28','29','34','32','33')),
#                 'UT Overdue'
#             ).when(
#                 col("ac.CasePrefix").isin('VA','AA','AS','CC','HR','HX','IM','NS','OA','OC','RD','TH','XX'),
#                 'FT Overdue'
#             ).when(
#                 col("us.CaseStatus").isNotNull() & (
#                     col("t.CaseStatus").isNull() |
#                     ((col("t.CaseStatus") == '10') & col("t.Outcome").isin('0','109','104','82','99','121','27','39')) |
#                     ((col("t.CaseStatus") == '46') & col("t.Outcome").isin('1','86')) |
#                     ((col("t.CaseStatus") == '26') & col("t.Outcome").isin('0','27','39','50','40','52','89')) |
#                     ((col("t.CaseStatus").isin('37','38')) & col("t.Outcome").isin('39','40','37','50','27','0','5','52')) |
#                     ((col("t.CaseStatus") == '39') & col("t.Outcome").isin('0','86')) |
#                     ((col("t.CaseStatus") == '50') & (col("t.Outcome") == '0')) |
#                     ((col("t.CaseStatus").isin('52','36')) & (col("t.Outcome") == '0') & col("st.DecisionDate").isNull())
#                 ),
#                 'FT - CCD'
#             ).when(
#                 (
#                     col("ac.CasePrefix").isin('DA','DC','EA','HU','PA','RP') |
#                     (col("ac.CasePrefix").isin('LP','LR','LD','LH','LE','IA') & col("ac.HOANRef").isNull())
#                 ) & (
#                     col("t.CaseStatus").isNull() |
#                     ((col("t.CaseStatus") == '10') & col("t.Outcome").isin('0','109','104','82','99','121','27','39')) |
#                     ((col("t.CaseStatus") == '46') & col("t.Outcome").isin('1','86')) |
#                     ((col("t.CaseStatus") == '26') & col("t.Outcome").isin('0','27','39','50','40','52','89')) |
#                     ((col("t.CaseStatus").isin('37','38')) & col("t.Outcome").isin('39','40','37','50','27','0','5','52')) |
#                     ((col("t.CaseStatus") == '39') & col("t.Outcome").isin('0','86')) |
#                     ((col("t.CaseStatus") == '50') & (col("t.Outcome") == '0')) |
#                     ((col("t.CaseStatus").isin('52','36')) & (col("t.Outcome") == '0') & col("st.DecisionDate").isNull())
#                 ),
#                 'CCD'
#             ).when(
#                 (
#                     (col("ac.CasePrefix").isin('DA','DC','EA','HU','PA','RP') & col("us.CaseStatus").isNull()) |
#                     (col("ac.CasePrefix").isin('LP','LR','LD','LH','LE','IA') & col("ac.HOANRef").isNull()) |
#                     (
#                         col("ac.CasePrefix").isin('LP','LR','LD','LH','LE','IA') & col("ac.HOANRef").isNotNull() &
#                         col("us.CaseStatus").isNotNull() &
#                         (add_months(col("us.DecisionDate"), 60) < add_months(col("t.DecisionDate"), 24))
#                     )
#                 ) & (
#                     ((col("t.CaseStatus") == '10') & col("t.Outcome").isin('13','80','122','25','120','2','105','119')) |
#                     ((col("t.CaseStatus") == '46') & col("t.Outcome").isin('31','2','50')) |
#                     ((col("t.CaseStatus") == '26') & col("t.Outcome").isin('80','13','25','1','2')) |
#                     ((col("t.CaseStatus").isin('37','38')) & col("t.Outcome").isin('1','2','80','13','25','72','14','125')) |
#                     ((col("t.CaseStatus") == '39') & col("t.Outcome").isin('30','31','25','14','80')) |
#                     ((col("t.CaseStatus") == '51') & col("t.Outcome").isin('94','93')) |
#                     ((col("t.CaseStatus") == '52') & col("t.Outcome").isin('91','95') &
#                     (~col("st.CaseStatus").isin('37','38','39','17','40','41','42','43','44','45','53','27','28','29','34','32','33')) | col("st.CaseStatus").isNull()) |
#                     ((col("t.CaseStatus") == '36') & (col("t.Outcome") == '25') &
#                     (~col("st.CaseStatus").isin('40','41','42','43','44','45','53','27','28','29','34','32','33')))
#                 ),
#                 'FT - CCD'
#             ).otherwise("Other").alias("CaseType")
#         )
#     )

#     result = result.filter(col('CaseType') != 'Other')

#     return result

# COMMAND ----------

# spark.sql("select * from ariadm_active_appeals.raw_status").printSchema()

# COMMAND ----------

# DBTITLE 1,stg_segmentation_states
# from pyspark.sql.functions import when, max as spark_max, add_months
# from pyspark.sql import functions as F

# # from pyspark.sql.functions import col, current_date, add_months, add_years

# @dlt.table(
#     name="stg_segmentation_states",
#     comment="Bronze DLT table replicating the AppealCase join logic.",
#     path=f"{silver_mnt}/stg_segmentation_states"
# )
# def stg_segmentation_states():
#     # Base tables
#     appealcase = dlt.read("raw_appealcase")
#     status = dlt.read("raw_status")
#     caselist = dlt.read("raw_caselist")
#     documentsreceived = dlt.read("raw_documentsreceived")
#     filelocation = dlt.read("raw_filelocation")
#     balancedue = dlt.read("stg_balance_due")
#     representation = dlt.read("stg_representation")
                  
#     s_subquery01 = (
#         status.filter(((~col("outcome").isin([38, 111]))) &
#                   ((~col("casestatus").isin(['17']))))
#           .groupBy("CaseNo")
#           .agg(spark_max("StatusId").alias("max_ID"))
#     )
                  
#     # Previous status logic
#     prev_subquery02 = (
#         status.filter((col("casestatus").isNull()) | (~col("casestatus").isin(['50', '52', '36'])))
#           .groupBy("CaseNo")
#           .agg(spark_max("StatusId").alias("Prev_ID"))
#     )

#     # UT status
#     ut_subquery03 = (
#         status.filter(col("CaseStatus").isin(['40', '41', '42', '43', '44', '45', '53', '27', '28', '29', '34', '32', '33']))
#           .groupBy("CaseNo")
#           .agg(spark_max("StatusId").alias("UT_ID"))
#     )

#     # Latest document received
#     docreceived_subquery04 = (
#         documentsreceived.groupBy("CaseNo")
#           .agg(spark_max("ReceivedDocumentId").alias("max_ReceivedDocumentId"))
#     )

#     # SA previous status
#     sa_prev_subquery05 = (
#         status.filter((col("casestatus").isNull()) | (col("casestatus") != '46'))
#           .groupBy("CaseNo")
#           .agg(spark_max("StatusId").alias("Prev_ID"))
#     )

#     # Final join
#     result = (
#         appealcase.alias("ac")
#         .join(s_subquery01.alias("s"), col("ac.CaseNo") == col("s.CaseNo"), "left")
#         .join(status.alias("t"), (col("t.CaseNo") == col("s.CaseNo")) & (col("t.StatusId") == col("s.max_ID")), "left")
#         .join(caselist.alias("cl"), col("t.StatusId") == col("cl.StatusId"), "left")
#         .join(prev_subquery02.alias("prev"), col("ac.CaseNo") == col("prev.CaseNo"), "left")
#         .join(status.alias("st"), (col("st.CaseNo") == col("prev.CaseNo")) & (col("st.StatusId") == col("prev.Prev_ID")), "left")
#         .join(ut_subquery03.alias("ut"), col("ac.CaseNo") == col("ut.CaseNo"), "left")
#         .join(status.alias("us"), (col("us.CaseNo") == col("ut.CaseNo")) & (col("us.StatusId") == col("ut.UT_ID")), "left")
#         .join(docreceived_subquery04.alias("doc_received"), col("ac.CaseNo") == col("doc_received.CaseNo"), "left")
#         .join(documentsreceived.alias("dr"), (col("dr.CaseNo") == col("doc_received.CaseNo")) & (col("dr.ReceivedDocumentId") == col("doc_received.max_ReceivedDocumentId")), "left")
#         .join(sa_prev_subquery05.alias("sa_prev"), col("ac.CaseNo") == col("sa_prev.CaseNo"), "left")
#         .join(status.alias("sa"), (col("sa.CaseNo") == col("sa_prev.CaseNo")) & (col("sa.StatusId") == col("sa_prev.Prev_ID")), "left")
#         .join(filelocation.alias("fl"), col("ac.CaseNo") == col("fl.CaseNo"), "left")
#         .join(balancedue.alias("bd"), col("ac.CaseNo") == col("bd.CaseNo"), "left")
#         .join(representation.alias("r"), col("ac.CaseNo") == col("r.CaseNo"), "left")
#         .select(
#             "ac.CaseNo", 
#             # "ac.CasePrefix",
        
#             when(
#             (col("t.CaseStatus").isin('40','41','42','43','44','45','53','27','28','29','34','32','33')) & (col("t.Outcome") == '86'),
#             'CCD'
#             ).when(
#                 (col("t.CaseStatus").isin('40','41','42','43','44','45','53','27','28','29','34','32','33')) & (col("t.Outcome") == '0'),
#                 'UT Active'
#             ).when(
#                 (col("ac.CasePrefix").isin('VA','AA','AS','CC','HR','HX','IM','NS','OA','OC','RD','TH','XX')) &
#                 (col("t.CaseStatus").isin('40','41','42','43','44','45','53','27','28','29','34','32','33')),
#                 'UT Overdue'
#             ).when(
#                 col("ac.CasePrefix").isin('VA','AA','AS','CC','HR','HX','IM','NS','OA','OC','RD','TH','XX'),
#                 'FT Overdue'
#             ).when(
#                 col("us.CaseStatus").isNotNull() & (
#                     col("t.CaseStatus").isNull() |
#                     ((col("t.CaseStatus") == '10') & col("t.Outcome").isin('0','109','104','82','99','121','27','39')) |
#                     ((col("t.CaseStatus") == '46') & col("t.Outcome").isin('1','86')) |
#                     ((col("t.CaseStatus") == '26') & col("t.Outcome").isin('0','27','39','50','40','52','89')) |
#                     ((col("t.CaseStatus").isin('37','38')) & col("t.Outcome").isin('39','40','37','50','27','0','5','52')) |
#                     ((col("t.CaseStatus") == '39') & col("t.Outcome").isin('0','86')) |
#                     ((col("t.CaseStatus") == '50') & (col("t.Outcome") == '0')) |
#                     ((col("t.CaseStatus").isin('52','36')) & (col("t.Outcome") == '0') & col("st.DecisionDate").isNull())
#                 ),
#                 'FT - CCD'
#             ).when(
#                 (
#                     col("ac.CasePrefix").isin('DA','DC','EA','HU','PA','RP') |
#                     (col("ac.CasePrefix").isin('LP','LR','LD','LH','LE','IA') & col("ac.HOANRef").isNull())
#                 ) & (
#                     col("t.CaseStatus").isNull() |
#                     ((col("t.CaseStatus") == '10') & col("t.Outcome").isin('0','109','104','82','99','121','27','39')) |
#                     ((col("t.CaseStatus") == '46') & col("t.Outcome").isin('1','86')) |
#                     ((col("t.CaseStatus") == '26') & col("t.Outcome").isin('0','27','39','50','40','52','89')) |
#                     ((col("t.CaseStatus").isin('37','38')) & col("t.Outcome").isin('39','40','37','50','27','0','5','52')) |
#                     ((col("t.CaseStatus") == '39') & col("t.Outcome").isin('0','86')) |
#                     ((col("t.CaseStatus") == '50') & (col("t.Outcome") == '0')) |
#                     ((col("t.CaseStatus").isin('52','36')) & (col("t.Outcome") == '0') & col("st.DecisionDate").isNull())
#                 ),
#                 'CCD'
#             ).when(
#                 (
#                     (col("ac.CasePrefix").isin('DA','DC','EA','HU','PA','RP') & col("us.CaseStatus").isNull()) |
#                     (col("ac.CasePrefix").isin('LP','LR','LD','LH','LE','IA') & col("ac.HOANRef").isNull()) |
#                     (
#                         col("ac.CasePrefix").isin('LP','LR','LD','LH','LE','IA') & col("ac.HOANRef").isNotNull() &
#                         col("us.CaseStatus").isNotNull() &
#                         (add_months(col("us.DecisionDate"), 60) < add_months(col("t.DecisionDate"), 24))
#                     )
#                 ) & (
#                     ((col("t.CaseStatus") == '10') & col("t.Outcome").isin('13','80','122','25','120','2','105','119')) |
#                     ((col("t.CaseStatus") == '46') & col("t.Outcome").isin('31','2','50')) |
#                     ((col("t.CaseStatus") == '26') & col("t.Outcome").isin('80','13','25','1','2')) |
#                     ((col("t.CaseStatus").isin('37','38')) & col("t.Outcome").isin('1','2','80','13','25','72','14','125')) |
#                     ((col("t.CaseStatus") == '39') & col("t.Outcome").isin('30','31','25','14','80')) |
#                     ((col("t.CaseStatus") == '51') & col("t.Outcome").isin('94','93')) |
#                     ((col("t.CaseStatus") == '52') & col("t.Outcome").isin('91','95') &
#                     (~col("st.CaseStatus").isin('37','38','39','17','40','41','42','43','44','45','53','27','28','29','34','32','33')) | col("st.CaseStatus").isNull()) |
#                     ((col("t.CaseStatus") == '36') & (col("t.Outcome") == '25') &
#                     (~col("st.CaseStatus").isin('40','41','42','43','44','45','53','27','28','29','34','32','33')))
#                 ),
#                 'FT - CCD'
#             ).otherwise("Unknown").alias("CaseType") 
#         )
#     )

#     return result

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Silver DLT Tables Creation

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Success Message
dbutils.notebook.exit("Notebook completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Appendix

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ariadm_active_appeals.bronze_appealcase_crep_rep_floc_cspon_cfs
