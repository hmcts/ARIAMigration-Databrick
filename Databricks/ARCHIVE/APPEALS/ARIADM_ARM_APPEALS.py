# Databricks notebook source
# MAGIC %md
# MAGIC # Appeals Archive
# MAGIC <table style = 'float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Name: </b></td>
# MAGIC          <td>ARIADM_ARM_APPEALS</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Description: </b></td>
# MAGIC          <td>Notebook to generate a set of HTML, JSON, and A360 files, each representing the data about Appeals stored in ARIA.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>First Created: </b></td>
# MAGIC          <td>OCT-2024 </td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <th style='text-align: left; '><b>Changelog(JIRA ref/initials./date):</b></th>
# MAGIC          <th>Comments </th>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-141">ARIADM-141</a>/NSA/OCT-2024</td>
# MAGIC          <td>Appeals : Compete Landing to Bronze Notebook</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-139">ARIADM-139</a>/NSA/OCT-2024</td>
# MAGIC          <td>Appeals : Compete Bronze to silver Notebook</td>
# MAGIC       </tr>
# MAGIC       
# MAGIC
# MAGIC
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
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..','..')))
# from pyspark.sql.functions import col, max

import dlt
import json
from pyspark.sql.functions import * #when, col,coalesce, current_timestamp, lit, date_format, trim, max
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions to Read Latest Landing Files

# COMMAND ----------

# from SharedFunctionsLib.custom_functions import *

# COMMAND ----------

pip install azure-storage-blob


# COMMAND ----------

# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Variables

# COMMAND ----------

# MAGIC %md
# MAGIC Please note that running the DLT pipeline with the parameter `initial_load = true` will ensure the creation of the corresponding Hive tables. However, during this stage, none of the gold outputs (HTML, JSON, and A360) are processed. To generate the gold outputs, a secondary run with `initial_load = true` is required.

# COMMAND ----------


initial_Load = False

# Setting variables for use in subsequent cells
raw_mnt = "/mnt/ingest00rawsboxraw/ARIADM/ARM/APPEALS"
landing_mnt = "/mnt/ingest00landingsboxlanding/"
bronze_mnt = "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/APPEALS"
silver_mnt = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/APPEALS"
gold_mnt = "/mnt/ingest00curatedsboxgold/ARIADM/ARM/APPEALS"

# file_path = '/mnt/ingest00landingsboxlanding/IRIS-TD-CSV/Example IRIS tribunal decisions data file.csv'

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

# # from pyspark.sql.functions import current_timestamp, lit
# from builtins import max as builtins_max 

# # Function to recursively list all files in the ADLS directory
# def deep_ls(path: str, depth: int = 0, max_depth: int = 10) -> list:
#     """
#     Recursively list all files and directories in ADLS directory.
#     Returns a list of all paths found.
#     """
#     output = set()  # Using a set to avoid duplicates
#     if depth > max_depth:
#         return output

#     try:
#         children = dbutils.fs.ls(path)
#         for child in children:
#             if child.path.endswith(".parquet"):
#                 output.add(child.path.strip())  # Add only .parquet files to the set

#             if child.isDir:
#                 # Recursively explore directories
#                 output.update(deep_ls(child.path, depth=depth + 1, max_depth=max_depth))

#     except Exception as e:
#         print(f"Error accessing {path}: {e}")

#     return list(output)  # Convert the set back to a list before returning

# # Function to extract timestamp from the file path
# def extract_timestamp(file_path):
#     """
#     Extracts timestamp from the parquet file name based on an assumed naming convention.
#     """
#     # Split the path and get the filename part
#     filename = file_path.split('/')[-1]
#     # Extract the timestamp part from the filename
#     timestamp_str = filename.split('_')[-1].replace('.parquet', '')
#     return timestamp_str

# # Main function to read the latest parquet file, add audit columns, and return the DataFrame
# def read_latest_parquet(folder_name: str, view_name: str, process_name: str, base_path: str = "/mnt/ingest00landingsboxlanding/") -> "DataFrame":
#     """
#     Reads the latest .parquet file from a specified folder, adds audit columns, creates a temporary Spark view, and returns the DataFrame.
    
#     Parameters:
#     - folder_name (str): The name of the folder to look for the .parquet files (e.g., "AdjudicatorRole").
#     - view_name (str): The name of the temporary view to create (e.g., "tv_AdjudicatorRole").
#     - process_name (str): The name of the process adding the audit information (e.g., "ARIA_ARM_JOH").
#     - base_path (str): The base path for the folders in the data lake.
    
#     Returns:
#     - DataFrame: The DataFrame created from the latest .parquet file with added audit columns.
#     """
#     # Construct the full folder path
#     folder_path = f"{base_path}{folder_name}/full/"
    
#     # List all .parquet files in the folder
#     all_files = deep_ls(folder_path)
    
#     # Ensure that files were found
#     if not all_files:
#         print(f"No .parquet files found in {folder_path}")
#         return None
    
#     # Find the latest .parquet file
#     latest_file = builtins_max(all_files, key=extract_timestamp)
    
#     # Print the latest file being loaded for logging purposes
#     print(f"Reading latest file: {latest_file}")
    
#     # Read the latest .parquet file into a DataFrame
#     df = spark.read.option("inferSchema", "true").parquet(latest_file)
    
#     # Add audit columns
#     df = df.withColumn("AdtclmnFirstCreatedDatetime", current_timestamp()) \
#            .withColumn("AdtclmnModifiedDatetime", current_timestamp()) \
#            .withColumn("SourceFileName", lit(latest_file)) \
#            .withColumn("InsertedByProcessName", lit(process_name))
    
#     # Create or replace a temporary view
#     df.createOrReplaceTempView(view_name)
    
#     print(f"Loaded the latest file for {folder_name} into view {view_name} with audit columns")
    
#     # Return the DataFrame
#     return df



# # # read the data from different folders, with audit columns and process name
# # df_Adjudicator = read_latest_parquet("Adjudicator", "tv_Adjudicator", "ARIA_ARM_JOH")
# # df_HearingCentre = read_latest_parquet("ARIAHearingCentre", "tv_HearingCentre", "ARIA_ARM_JOH")
# # df_DoNotUseReason = read_latest_parquet("ARIADoNotUseReason", "tv_DoNotUseReason", "ARIA_ARM_JOH")
# # df_EmploymentTerm = read_latest_parquet("EmploymentTerm", "tv_EmploymentTerms", "ARIA_ARM_JOH")
# # df_JoHistory = read_latest_parquet("JoHistory", "tv_JoHistory", "ARIA_ARM_JOH")
# # df_Users = read_latest_parquet("Users", "tv_Users", "ARIA_ARM_JOH")
# # df_OtherCentre = read_latest_parquet("OtherCentre", "tv_OtherCentre", "ARIA_ARM_JOH")
# # df_AdjudicatorRole = read_latest_parquet("AdjudicatorRole", "tv_AdjudicatorRole", "ARIA_ARM_JOH")


# COMMAND ----------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import current_timestamp, lit, regexp_extract, col
# from pyspark.sql import functions as F

# # Initialize Spark session (if not already done in your environment)
# spark = SparkSession.builder.getOrCreate()

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
def read_latest_parquet(folder_name: str, view_name: str, process_name: str, base_path: str = "/mnt/ingest00landingsboxlanding/") -> "DataFrame":
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
    return (
        dlt.read("raw_appealcase").alias("ac")
            .join(
                dlt.read("raw_caserespondent").alias("cr"),
                col("ac.CaseNo") == col("cr.CaseNo"),
                "left_outer"
            )
            .join(
                dlt.read("raw_mainrespondent").alias("mr"),
                col("cr.MainRespondentId") == col("mr.MainRespondentId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_respondent").alias("r"),
                col("cr.RespondentId") == col("r.RespondentId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_filelocation").alias("fl"),
                col("ac.CaseNo") == col("fl.CaseNo"),
                "left_outer"
            )
            .join(
                dlt.read("raw_caserep").alias("crep"),
                col("ac.CaseNo") == col("crep.CaseNo"),
                "left_outer"
            )
            .join(
                dlt.read("raw_representative").alias("rep"),
                col("crep.RepresentativeId") == col("rep.RepresentativeId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_language").alias("l"),
                col("ac.LanguageId") == col("l.LanguageId"),
                "left_outer"
            )
            .select(
                # Appeal Case columns
                trim(col("ac.CaseNo")).alias('CaseNo'), col("ac.CasePrefix"), col("ac.CaseYear"), col("ac.CaseType"),
                col("ac.AppealTypeId"), col("ac.DateLodged"), col("ac.DateReceived"),
                col("ac.PortId"), col("ac.HORef"), col("ac.DateServed"), 
                col("ac.Notes").alias("AppealCaseNote"), col("ac.NationalityId"),
                col("ac.Interpreter"), col("ac.CountryId"), col("ac.DateOfIssue"), 
                col("ac.FamilyCase"), col("ac.OakingtonCase"), col("ac.VisitVisaType"),
                col("ac.HOInterpreter"), col("ac.AdditionalGrounds"), col("ac.AppealCategories"),
                col("ac.DateApplicationLodged"), col("ac.ThirdCountryId"),
                col("ac.StatutoryClosureDate"), col("ac.PubliclyFunded"), col("ac.NonStandardSCPeriod"),
                col("ac.CourtPreference"), col("ac.ProvisionalDestructionDate"), col("ac.DestructionDate"),
                col("ac.FileInStatutoryClosure"), col("ac.DateOfNextListedHearing"),
                col("ac.DocumentsReceived"), col("ac.OutOfTimeIssue"), col("ac.ValidityIssues"),
                col("ac.ReceivedFromRespondent"), col("ac.DateAppealReceived"), col("ac.RemovalDate"),
                col("ac.CaseOutcomeId"), col("ac.AppealReceivedBy"), col("ac.InCamera"),
                col("ac.DateOfApplicationDecision"), col("ac.UserId"), col("ac.SubmissionURN"),
                col("ac.DateReinstated"), col("ac.DeportationDate"), col("ac.HOANRef").alias("CCDAppealNum"),
                col("ac.HumanRights"), col("ac.TransferOutDate"), col("ac.CertifiedDate"),
                col("ac.CertifiedRecordedDate"), col("ac.NoticeSentDate"),
                col("ac.AddressRecordedDate"), col("ac.ReferredToJudgeDate"),
                col("ac.CentreId").alias("DedicaredHearingCentre"),
                col("ac.FeeSatisfactionId").alias("CertOfFeeSatisfaction"),
                # ac.LanguageId AS Language, 

                # Case Respondent columns
                col("cr.Respondent").alias("CRRespondent"),
                col("cr.Reference").alias("CRReference"),
                col("cr.Contact").alias("CRContact"),
                
                # Main Respondent columns
                col("mr.Name").alias("MRName"),
                col("mr.Embassy").alias("MREmbassy"),
                col("mr.POU").alias("MRPOU"),
                col("mr.Respondent").alias("MRRespondent"),
                
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
                col("r.Sdx").alias("RespondentSdx"),
                
                # File Location columns
                col("fl.Note").alias("FileLocationNote"),
                col("fl.TransferDate").alias("FileLocationTransferDate"),
                
                # Case Representative columns
                col("crep.RepresentativeRef"),
                col("crep.Name").alias("CaseRepName"),
                col("crep.Address1").alias("CaseRepAddress1"),
                col("crep.Address2").alias("CaseRepAddress2"),
                col("crep.Address3").alias("CaseRepAddress3"),
                col("crep.Address4").alias("CaseRepAddress4"),
                col("crep.Address5").alias("CaseRepAddress5"),
                col("crep.Postcode").alias("CaseRepPostcode"),
                col("crep.Telephone").alias("CaseRepTelephone"),
                col("crep.Fax").alias("CaseRepFax"),
                col("crep.Contact").alias("CaseRepContact"),
                col("crep.DXNo1").alias("CaseRepDXNo1"),
                col("crep.DXNo2").alias("CaseRepDXNo2"),
                col("crep.TelephonePrime"),
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
                col("rep.Sdx").alias("RepSdx"),
                col("rep.DXNo1").alias("RepDXNo1"),
                col("rep.DXNo2").alias("RepDXNo2"),
                
                # Language columns
                col("l.Description").alias("Language"),
                col("l.DoNotUse").alias("DoNotUseLanguage")
            )
    )


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
    return (
        dlt.read("raw_caseappellant").alias("ca")
            .join(
                dlt.read("raw_appellant").alias("a"),
                col("ca.AppellantId") == col("a.AppellantId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_detentioncentre").alias("dc"),
                col("a.DetentionCentreId") == col("dc.DetentionCentreId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_country").alias("c"),
                col("a.AppellantCountryId") == col("c.CountryId"),
                "left_outer"
            )
            .select(
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
    )

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
    return (
        dlt.read("raw_status").alias("s")
            .join(
                dlt.read("raw_caselist").alias("cl"),
                col("s.StatusId") == col("cl.StatusId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_hearingtype").alias("ht"),
                col("cl.HearingTypeId") == col("ht.HearingTypeId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_list").alias("l"),
                col("cl.ListId") == col("l.ListId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_listtype").alias("lt"),
                col("l.ListTypeId") == col("lt.ListTypeId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_court").alias("c"),
                col("l.CourtId") == col("c.CourtId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_hearingcentre").alias("hc"),
                col("l.CentreId") == col("hc.CentreId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_listsitting").alias("ls"),
                col("l.ListId") == col("ls.ListId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_adjudicator").alias("a"),
                col("ls.AdjudicatorId") == col("a.AdjudicatorId"),
                "left_outer"
            )
            .select(
                # Status fields
                trim(col("s.CaseNo")).alias('CaseNo'),
                col("s.Outcome"),
                
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
                col("a.AdjudicatorId"), 
                col("a.Surname").alias("AdjudicatorSurname"),
                col("a.Forenames").alias("AdjudicatorForenames"),
                col("a.Notes").alias("AdjudicatorNote"),
                col("a.Title").alias("AdjudicatorTitle"),
                
                # List and related fields
                col("l.ListName"),
                col("l.StartTime").alias("ListStartTime"), ##review
                col("lt.Description").alias("ListTypeDesc"),
                col("lt.ListType"),
                col("lt.DoNotUse").alias("DoNotUseListType"),
                
                # Court fields
                col("c.CourtName"),
                col("c.DoNotUse").alias("DoNotUseCourt"),
                
                # Hearing Centre fields
                col("hc.Description").alias("HearingCentreDesc")
            )
    )


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
    return (
        dlt.read("raw_bfdiary").alias("bfd")
            .join(
                dlt.read("raw_bfType").alias("bft"),
                col("bfd.BFTypeId") == col("bft.BFTypeId"),
                "left_outer"
            )
            .select(
                # BFDiary fields
                trim(col("bfd.CaseNo")).alias('CaseNo'),
                col("bfd.Entry"),
                col("bfd.EntryDate"),
                col("bfd.DateCompleted"),
                col("bfd.Reason"),
                
                # BFType fields
                col("bft.Description").alias("BFTypeDescription"),
                col("bft.DoNotUse")
            )
    )


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
    return (
        dlt.read("raw_history").alias("h")
            .join(
                dlt.read("raw_users").alias("u"),
                col("h.UserId") == col("u.UserId"),
                "left_outer"
            )
            .select(
                # History fields
                col("h.HistoryId"),
                trim(col("h.CaseNo")).alias('CaseNo'),
                col("h.HistDate"),
                col("h.HistType"),
                col("h.Comment").alias("HistoryComment"),
                col("h.StatusId"),
                
                # User fields
                col("u.Name").alias("UserName"),
                col("u.UserType"),
                col("u.Fullname"),
                col("u.Extension"),
                col("u.DoNotUse")
            )
    )


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
    return (
        dlt.read("raw_link").alias("l")
            .join(
                dlt.read("raw_linkdetail").alias("ld"),
                col("l.LinkNo") == col("ld.LinkNo"),
                "left_outer"
            )
            .select(
                # Link fields
                trim(col("l.CaseNo")).alias('CaseNo'),
                col("l.LinkNo"),
                
                # LinkDetail fields
                col("ld.Comment").alias("LinkDetailComment")
                
            )
    )


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
    return (
        dlt.read("raw_status").alias("s")
            .join(
                dlt.read("raw_casestatus").alias("cs"),
                col("s.CaseStatus") == col("cs.CaseStatusId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_statuscontact").alias("sc"),
                col("s.StatusId") == col("sc.StatusId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_reasonadjourn").alias("ra"),
                col("s.ReasonAdjournId") == col("ra.ReasonAdjournId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_language").alias("l"),
                col("s.AdditionalLanguageId") == col("l.LanguageId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_decisiontype").alias("dt"),
                col("s.Outcome") == col("dt.DecisionTypeId"),
                "left_outer"
            )
            .select(
                # Status fields
                col("s.StatusId"),
                trim(col("s.CaseNo")).alias('CaseNo'),
                col("s.CaseStatus"),
                col("s.DateReceived"),
                col("s.Allegation"),
                col("s.KeyDate"),
                #col("s.AdjudicatorId"),
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
                col("s.AdditionalLanguageId"),
                col("s.CostOrderAppliedFor"),
                col("s.HearingCourt"),
                # CaseStatus fields
                col("cs.Description").alias("CaseStatusDescription"),
                col("cs.DoNotUse").alias("DoNotUseCaseStatus"),
                col("cs.HearingPoints").alias("CaseStatusHearingPoints"),
                
                # StatusContact fields
                col("sc.Contact").alias("ContactStatus"),
                col("sc.CourtName").alias("SCCContactName"),
                col("sc.Address1").alias("SCAddress1"),
                col("sc.Address1").alias("SCAddress2"),
                col("sc.Address1").alias("SCAddress3"),
                col("sc.Address1").alias("SCAddress4"),
                col("sc.Address1").alias("SCAddress5"),
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
                col("dt.BailHOConsent")
            )
    )


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
    return (
        dlt.read("raw_appealcategory").alias("ap")
            .join(
                dlt.read("raw_category").alias("c"),
                col("ap.CategoryId") == col("c.CategoryId"),
                "left_outer"
            )
            .select(
                trim(col("ap.CaseNo")).alias('CaseNo'),
                col("c.Description").alias("CategoryDescription"),
                col("c.Flag")
            )
    )


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
    appeal_grounds = dlt.read("raw_appealgrounds").alias("ag")
    appeal_type = dlt.read("raw_appealtype").alias("at")

    return (
        appeal_case
        .join(case_fee_summary, col("ac.CaseNo") == col("cfs.CaseNo"), "left_outer")
        .join(fee_satisfaction, col("ac.FeeSatisfactionId") == col("fs.FeeSatisfactionId"), "left_outer")
        .join(payment_remission_reason, col("cfs.PaymentRemissionReason") == col("prr.PaymentRemissionReasonId"), "left_outer")
        .join(port, col("ac.PortId") == col("p.PortId"), "left_outer")
        .join(embassy, col("ac.VVEmbassyId") == col("e.EmbassyId"), "left_outer")
        .join(hearing_centre, col("ac.CentreId") == col("hc.CentreId"), "left_outer")
        .join(case_sponsor, col("ac.CaseNo") == col("cs.CaseNo"), "left_outer")
        .join(appeal_grounds, col("ac.CaseNo") == col("ag.CaseNo"), "left_outer")
        .join(appeal_type, col("ag.AppealTypeId") == col("at.AppealTypeId"), "left_outer")
        .select(
            trim(col("ac.CaseNo")).alias('CaseNo'),
           # col("ac.FeeSatisfactionId"),
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
            col("prr.Description").alias("PaymentRemissionReasonDescription"),
            col("prr.DoNotUse").alias("PaymentRemissionReasonDoNotUse"),
            col("p.PortName").alias("POUPortName"),
            col("p.Address1").alias("PortAddress1"),
            col("p.Address2").alias("PortAddress2"),
            col("p.Address3").alias("PortAddress3"),
            col("p.Address4").alias("PortAddress4"),
            col("p.Address5").alias("PortAddress5"),
            col("p.Postcode").alias("PortPostcode"),
            col("p.Telephone").alias("PortTelephone"),
            col("p.Sdx").alias("PortSdx"),
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
            col("hc.Description"),
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
            col("ag.AppealTypeId"),
            #this hads been alias as there had been multiple appeal columns
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
    )


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
    return (
        dlt.read("raw_status").alias("s")
            .join(
                dlt.read("raw_decisiontype").alias("dt"),
                col("s.Outcome") == col("dt.DecisionTypeId"),
                "left_outer"
            )
            .select(
                trim(col("s.CaseNo")).alias('CaseNo'),
                col("dt.Description").alias("DecisionTypeDescription"),
                col("dt.DeterminationRequired"),
                col("dt.DoNotUse"),
                col("dt.State"),
                col("dt.BailRefusal"),
                col("dt.BailHOConsent")
            )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M12: bronze_appealcase_t_tt_ts_tm

# COMMAND ----------

@dlt.table(
    name="bronze_appealcase_t_tt_ts_tm",
    comment="Delta Live Table for joining Transaction, TransactionType, TransactionStatus, and TransactionMethod tables to retrieve transaction details.",
    path=f"{bronze_mnt}/bronze_appealcase_t_tt_ts_tm"
)
def bronze_appealcase_t_tt_ts_tm():
    return (
        dlt.read("raw_transaction").alias("t")
            .join(
                dlt.read("raw_transactiontype").alias("tt"),
                col("t.TransactionTypeId") == col("tt.TransactionTypeID"),
                "left_outer"
            )
            .join(
                dlt.read("raw_transactionstatus").alias("ts"),
                col("t.Status") == col("ts.TransactionStatusID"),
                "left_outer"
            )
            .join(
                dlt.read("raw_transactionmethod").alias("tm"),
                col("t.TransactionMethodId") == col("tm.TransactionMethodID"),
                "left_outer"
            )
            .select(
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
                col("t.CreateUserId"),
                col("t.LastEditUserId"),
                
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
                col("tm.DoNotUse").alias("DoNotUseTransactionMethod")
            )
    )


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
    return (
        dlt.read("raw_appealhumanright").alias("ahr")
            .join(
                dlt.read("raw_humanright").alias("hr"),
                col("ahr.HumanRightId") == col("hr.HumanRightId"),
                "left_outer"
            )
            .select(
                col("ahr.CaseNo"),
                col("ahr.HumanRightId"),
                col("hr.Description").alias("HumanRightDescription"),
                col("hr.DoNotShow"),
                col("hr.Priority")
            )
    )


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
    return (
        dlt.read("raw_appealnewmatter").alias("anm")
            .join(
                dlt.read("raw_newmatter").alias("nm"),
                col("anm.NewMatterId") == col("nm.NewMatterId"),
                "left_outer"
            )
            .select(
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
    )


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
    return (
        dlt.read("raw_documentsreceived").alias("dr")
            .join(
                dlt.read("raw_receiveddocument").alias("rd"),
                col("dr.ReceivedDocumentId") == col("rd.ReceivedDocumentId"),
                "left_outer"
            )
            .select(
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
    )


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
    return (
        dlt.read("raw_reviewstandarddirection").alias("rsd")
            .join(
                dlt.read("raw_StandardDirection").alias("sd"),
                col("rsd.StandardDirectionId") == col("sd.StandardDirectionId"),
                "left_outer"
            )
            .select(
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
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M17: bronze_review_specific_direction
# MAGIC _**raw_review_specific_direction table not avalible**_

# COMMAND ----------

@dlt.table(
    name="bronze_review_specific_direction",
    comment="Delta Live Table for retrieving details from the ReviewSpecificDirection table.",
    path=f"{bronze_mnt}/bronze_review_specific_direction"
)
def bronze_review_specific_direction():
    return (
        dlt.read("raw_reviewspecificdirection")
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
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### TBC

# COMMAND ----------

@dlt.table(
    name="bronze_cost_award",
    comment="Delta Live Table for retrieving details from the CostAward table.",
    path=f"{bronze_mnt}/bronze_cost_award"
)
def bronze_cost_award():
    return (
        dlt.read("raw_costaward")
            .select(
                col("CostAwardId"),
                col("CaseNo"),
                col("DateOfApplication"),
                col("TypeOfCostAward"),
                col("ApplyingParty"),
                col("PayingParty"),
                col("MindedToAward"),
                col("ObjectionToMindedToAward"),
                col("CostsAwardDecision"),
                col("DateOfDecision"),
                col("CostsAmount"),
                col("OutcomeOfAppeal"),
                col("AppealStage")
            )
    )


# COMMAND ----------

@dlt.table(
    name="bronze_costorder",
    comment="Delta Live Table for retrieving details from the CostOrder table.",
    path=f"{bronze_mnt}/bronze_cost_order"
)
def bronze_costorder():
    return (
        dlt.read("raw_costorder")
            .select(
                col("CostOrderId"),
                col("CaseNo"),
                col("AppealStageWhenApplicationMade"),
                col("DateOfApplication"),
                col("AppealStageWhenDecisionMade"),
                col("OutcomeOfAppealWhereDecisionMade"),
                col("DateOfDecision"),
                col("CostOrderDecision"),
                col("ApplyingRepresentativeId"),
                col("ApplyingRepresentativeName")
            )
    )


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.ariadm_arm_appeals.raw_hearingpointschangereason

# COMMAND ----------

@dlt.table(
    name="bronze_hearing_points_change_reason",
    comment="Delta Live Table for retrieving details from the HearingPointsChangeReason table.",
    path=f"{bronze_mnt}/bronze_hearing_points_change_reason"
)
def bronze_hearing_points_change_reason():
    return (
        dlt.read("raw_hearingpointschangereason")
            .select(
                col("HearingPointsChangeReasonId"),
                col("Description"),
                col("DoNotUse")
            )
    )


# COMMAND ----------

@dlt.table(
    name="bronze_hearing_points_history",
    comment="Delta Live Table for retrieving details from the HearingPointsHistory table.",
    path=f"{bronze_mnt}/bronze_hearing_points_history"
)
def bronze_hearing_points_history():
    return (
        dlt.read("raw_hearingpointshistory")
            .select(
                col("HearingPointsHistoryId"),
                col("StatusId"),
                col("HistDate"),
                col("HistType"),
                col("UserId"),
                col("DefaultPoints"),
                col("InitialPoints"),
                col("FinalPoints"),
                col("HearingPointsChangeReasonId")
            )
    )


# COMMAND ----------

@dlt.table(
    name="bronze_appeal_type_category",
    comment="Delta Live Table for retrieving details from the AppealTypeCategory table.",
    path=f"{bronze_mnt}/bronze_appeal_type_category"
)
def bronze_appeal_type_category():
    return (
        dlt.read("raw_appealtypecategory")
            .select(
                col("AppealTypeCategoryId"),
                col("AppealTypeId"),
                col("CategoryId"),
                col("FeeExempt")
            )
    )


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Segmentation tables

# COMMAND ----------

def add_years(date_col, years):
    return date_col + expr(f'INTERVAL {years} YEARS')
@dlt.table(
    name="stg_appealcasestatus_filtered",
    comment="Delta Live Table for filtering First Tier Overdue records based on specified conditions.",
    path=f"{bronze_mnt}/stg_appealcasestatus_filtered"
)
def stg_appealcasestatus_filtered():
    # Reading base tables
    appeal_case = dlt.read("raw_appealcase")
    status = dlt.read("raw_status")
    case_status = dlt.read("raw_casestatus")
    decision_type = dlt.read("raw_decisiontype")
    file_location = dlt.read("raw_filelocation")

    # Subqueries to handle aggregations
    max_status = (
        status
        .filter(
            (col("outcome").isNull() | ~col("outcome").cast("int").isin(38, 111)) &
            (col("casestatus").isNull() | ~col("casestatus").cast("int").isin(17))
        )
        .groupBy("CaseNo")
        .agg(max("StatusId").alias("max_ID"))
    )

    prev_status = (
            status
            .filter((col("casestatus").isNull() | ~col("casestatus").cast("int").isin(52, 36)))
            .groupBy("CaseNo")
            .agg(max("StatusID").alias("Prev_ID"))
        )

    ut_status = (
        status
        .filter(
            col("CaseStatus").isin(
                ['40', '41', '42', '43', '44', '45', '53', '27', '28', '29', '34', '32', '33']
            )
        )
        .groupBy("CaseNo")
        .agg(max("StatusID").alias("UT_ID"))
    )

    # Joins to construct the main DataFrame
    result_df = (
        appeal_case.alias("ac")
        .join(max_status.alias("s"), col("ac.CaseNo") == col("s.CaseNo"), "left_outer")
        .join(status.alias("t"), (col("t.CaseNo") == col("s.CaseNo")) & (col("t.StatusId") == col("s.max_ID")), "left_outer")
        .join(case_status.alias("cst"), col("t.CaseStatus") == col("cst.CaseStatusId"), "left_outer")
        .join(decision_type.alias("dt"),col("t.Outcome") == col("dt.DecisionTypeId"), "left_outer")
        .join(file_location.alias("fl"), col("ac.CaseNo") == col("fl.CaseNo"), "left_outer")
        .join(prev_status.alias("prev"), col("ac.CaseNo") == col("prev.CaseNo"), "left_outer")
        .join(status.alias("st"), (col("st.CaseNo") == col("prev.CaseNo")) & (col("st.StatusId") == col("prev.Prev_ID")), "left_outer")
        .join(ut_status.alias("UT"), col("ac.CaseNo") == col("UT.CaseNo"), "left_outer")
        .join(status.alias("us"), (col("us.CaseNo") == col("UT.CaseNo")) & (col("us.StatusId") == col("UT.UT_ID")), "left_outer")
    )

    # Filtering based on the WHERE clause
    # Filtering based on the WHERE clause
    filtered_df = (
        result_df
        .filter((col("ac.CaseType") == 1) & ~col("fl.DeptId").isin(519, 520))
        .withColumn("CaseStatusCategory", 
            when(
                (col("t.CaseStatus").isin('40', '41', '42', '43', '44', '45', '53', '27', '28', '29', '34', '32', '33')) & 
                (col("t.Outcome") == 86), "UT Remitted"
            ).when(
                (col("t.CaseStatus").isin('40', '41', '42', '43', '44', '45', '53', '27', '28', '29', '34', '32', '33')) & 
                (col("t.Outcome") == 0), "UT Active"
            ).when(
                (col("ac.CasePrefix").isin('VA', 'AA', 'AS', 'CC', 'HR', 'HX', 'IM', 'NS', 'OA', 'OC', 'RD', 'TH', 'XX')) & 
                (col("t.CaseStatus").isin('40', '41', '42', '43', '44', '45', '53', '27', '28', '29', '34', '32', '33')), "UT Overdue"
            ).when(
                col("ac.CasePrefix").isin('VA', 'AA', 'AS', 'CC', 'HR', 'HX', 'IM', 'NS', 'OA', 'OC', 'RD', 'TH', 'XX'), "FT Overdue"
            ).when(
                (col("us.CaseStatus").isNotNull()) & (
                    col("t.CaseStatus").isNull() | (
                        (col("t.CaseStatus") == 10) & 
                        (col("t.Outcome").isin('0', '109', '104', '82', '99', '121', '27', '39'))
                    ) | (
                        (col("t.CaseStatus") == 46) & 
                        (col("t.Outcome").isin('1', '86'))
                    ) | (
                        (col("t.CaseStatus") == 26) & 
                        (col("t.Outcome").isin('0', '27', '39', '50', '40', '52', '89'))
                    ) | (
                        col("t.CaseStatus").isin('37', '38') & 
                        col("t.Outcome").isin('39', '40', '37', '50', '27', '0', '5', '52')
                    ) | (
                        (col("t.CaseStatus") == 39) & 
                        (col("t.Outcome").isin('0', '86'))
                    ) | (
                        (col("t.CaseStatus") == 50) & 
                        (col("t.Outcome") == 0)
                    ) | (
                        (col("t.CaseStatus").isin('52', '36')) & 
                        (col("t.Outcome") == 0) & 
                        (col("st.DecisionDate").isNull())
                    )
                ), "FT Active Case"
            ).when(
                (col("ac.CasePrefix").isin('DA', 'DC', 'EA', 'HU', 'PA', 'RP', 'LP', 'LR', 'LD', 'LH', 'LE', 'IA')) & 
                (col("ac.HOANRef").isNull()) & (
                    col("t.CaseStatus").isNull() | (
                        (col("t.CaseStatus") == 10) & 
                        (col("t.Outcome").isin('0', '109', '104', '82', '99', '121', '27', '39'))
                    ) | (
                        (col("t.CaseStatus") == 46) & 
                        (col("t.Outcome").isin('1', '86'))
                    ) | (
                        (col("t.CaseStatus") == 26) & 
                        (col("t.Outcome").isin('0', '27', '39', '50', '40', '52', '89'))
                    ) | (
                        col("t.CaseStatus").isin('37', '38') & 
                        col("t.Outcome").isin('39', '40', '37', '50', '27', '0', '5', '52')
                    ) | (
                        (col("t.CaseStatus") == 39) & 
                        (col("t.Outcome").isin('0', '86'))
                    ) | (
                        (col("t.CaseStatus") == 50) & 
                        (col("t.Outcome") == 0)
                    ) | (
                        (col("t.CaseStatus").isin('52', '36')) & 
                        (col("t.Outcome") == 0) & 
                        (col("st.DecisionDate").isNull())
                    )
                ), "FT Active Case"
            ).when(
                (col("ac.CasePrefix").isin('DA', 'DC', 'EA', 'HU', 'PA', 'RP')) & 
                (col("us.CaseStatus").isNull() | (
                    (col("us.CaseStatus").isNotNull()) & 
                    (add_months(col("us.DecisionDate"), 60) < add_months(col("t.DecisionDate"), 24))
                )) | (
                    (col("ac.CasePrefix").isin('LP', 'LR', 'LD', 'LH', 'LE', 'IA')) & 
                    (col("ac.HOANRef").isNull() | (
                        (col("ac.HOANRef").isNotNull()) & 
                        (col("us.CaseStatus").isNotNull()) & 
                        (add_months(col("us.DecisionDate"), 60) < add_months(col("t.DecisionDate"), 24))
                    ))
                ) & (
                    (col("t.CaseStatus") == 10) & 
                    (col("t.Outcome").isin('13', '80', '122', '25', '120', '2', '105', '119'))
                ) | (
                    (col("t.CaseStatus") == 46) & 
                    (col("t.Outcome").isin('31', '2', '50'))
                ) | (
                    (col("t.CaseStatus") == 26) & 
                    (col("t.Outcome").isin('80', '13', '25', '1', '2'))
                ) | (
                    col("t.CaseStatus").isin('37', '38') & 
                    col("t.Outcome").isin('1', '2', '80', '13', '25', '72', '14')
                ) | (
                    (col("t.CaseStatus") == 39) & 
                    (col("t.Outcome").isin('30', '31', '25', '14', '80'))
                ) | (
                    (col("t.CaseStatus") == 51) & 
                    (col("t.Outcome").isin('94', '93'))
                ) | (
                    (col("t.CaseStatus") == 52) & 
                    (col("t.Outcome").isin('91', '95')) & (
                        col("st.CaseStatus").isNull() | ~col("st.CaseStatus").isin('37', '38', '39', '17', '40', '41', '42', '43', '44', '45', '53', '27', '28', '29', '34', '32', '33')
                    )
                ) | (
                    (col("t.CaseStatus") == 36) & 
                    (col("t.Outcome") == 25) & (
                        col("st.CaseStatus").isNull() | ~col("st.CaseStatus").isin('40', '41', '42', '43', '44', '45', '53', '27', '28', '29', '34', '32', '33')
                    )
                ) & (add_months(col("t.DecisionDate"), 24) >= current_date()), "FT Retained - ARM"
            ).when(
                (col("ac.CasePrefix").isin('DA', 'DC', 'EA', 'HU', 'PA', 'RP')) & 
                (col("us.CaseStatus").isNull() | (
                    (col("us.CaseStatus").isNotNull()) & 
                    (add_months(col("us.DecisionDate"), 60) < add_months(col("t.DecisionDate"), 24))
                )) | (
                    (col("ac.CasePrefix").isin('LP', 'LR', 'LD', 'LH', 'LE', 'IA')) & 
                    (col("ac.HOANRef").isNull() | (
                        (col("ac.HOANRef").isNotNull()) & 
                        (col("us.CaseStatus").isNotNull()) & 
                        (add_months(col("us.DecisionDate"), 60) < add_months(col("t.DecisionDate"), 24))
                    ))
                ) & (
                    (col("t.CaseStatus").isin('52', '36')) & 
                    (col("t.Outcome") == 0) & 
                    (col("st.DecisionDate").isNotNull())
                ) | (
                    (col("t.CaseStatus") == 36) & 
                    (col("t.Outcome").isin('1', '2', '50', '108'))
                ) | (
                    (col("t.CaseStatus") == 52) & 
                    (col("t.Outcome").isin('91', '95')) & 
                    col("st.CaseStatus").isin('37', '38', '39', '17')
                ) & (add_months(col("st.DecisionDate"), 24) >= current_date()), "FT Retained - ARM"
            ).when(
                (col("ac.CasePrefix").isin('DA', 'DC', 'EA', 'HU', 'PA', 'RP')) & 
                (col("us.CaseStatus").isNull() | (
                    (col("us.CaseStatus").isNotNull()) & 
                    (add_months(col("us.DecisionDate"), 60) < add_months(col("t.DecisionDate"), 24))
                )) | (
                    (col("ac.CasePrefix").isin('LP', 'LR', 'LD', 'LH', 'LE', 'IA')) & 
                    (col("ac.HOANRef").isNull() | (
                        (col("ac.HOANRef").isNotNull()) & 
                        (col("us.CaseStatus").isNotNull()) & 
                        (add_months(col("us.DecisionDate"), 60) < add_months(col("t.DecisionDate"), 24))
                    ))
                ) & (
                    (col("t.CaseStatus") == 10) & 
                    (col("t.Outcome").isin('13', '80', '122', '25', '120', '2', '105', '119'))
                ) | (
                    (col("t.CaseStatus") == 46) & 
                    (col("t.Outcome").isin('31', '2', '50'))
                ) | (
                    (col("t.CaseStatus") == 26) & 
                    (col("t.Outcome").isin('80', '13', '25', '1', '2'))
                ) | (
                    col("t.CaseStatus").isin('37', '38') & 
                    col("t.Outcome").isin('1', '2', '80', '13', '25', '72', '14')
                ) | (
                    (col("t.CaseStatus") == 39) & 
                    (col("t.Outcome").isin('30', '31', '25', '14', '80'))
                ) | (
                    (col("t.CaseStatus") == 51) & 
                    (col("t.Outcome").isin('94', '93'))
                ) | (
                    (col("t.CaseStatus") == 52) & 
                    (col("t.Outcome").isin('91', '95')) & (
                        col("st.CaseStatus").isNull() | ~col("st.CaseStatus").isin('37', '38', '39', '17', '40', '41', '42', '43', '44', '45', '53', '27', '28', '29', '34', '32', '33')
                    )
                ) | (
                    (col("t.CaseStatus") == 36) & 
                    (col("t.Outcome") == 25) & (
                        col("st.CaseStatus").isNull() | ~col("st.CaseStatus").isin('40', '41', '42', '43', '44', '45', '53', '27', '28', '29', '34', '32', '33')
                    )
                ) & (add_months(col("t.DecisionDate"), 24) < current_date()), "FT Overdue"
            ).when(
                (col("ac.CasePrefix").isin('DA', 'DC', 'EA', 'HU', 'PA', 'RP')) & 
                (col("us.CaseStatus").isNull() | (
                    (col("us.CaseStatus").isNotNull()) & 
                    (add_months(col("us.DecisionDate"), 60) < add_months(col("t.DecisionDate"), 24))
                )) | (
                    (col("ac.CasePrefix").isin('LP', 'LR', 'LD', 'LH', 'LE', 'IA')) & 
                    (col("ac.HOANRef").isNull() | (
                        (col("ac.HOANRef").isNotNull()) & 
                        (col("us.CaseStatus").isNotNull()) & 
                        (add_months(col("us.DecisionDate"), 60) < add_months(col("t.DecisionDate"), 24))
                    ))
                ) & (
                    (col("t.CaseStatus").isin('52', '36')) & 
                    (col("t.Outcome") == 0) & 
                    (col("st.DecisionDate").isNotNull())
                ) | (
                    (col("t.CaseStatus") == 36) & 
                    (col("t.Outcome").isin('1', '2', '50', '108'))
                ) | (
                    (col("t.CaseStatus") == 52) & 
                    (col("t.Outcome").isin('91', '95')) & 
                    col("st.CaseStatus").isin('37', '38', '39', '17')
                ) & (add_months(col("st.DecisionDate"), 24) < current_date()), "FT Overdue"
            ).when(
                (col("ac.CasePrefix") == 'IA') & 
                (col("t.CaseStatus").isin(30, 31)), "FT Overdue"
            ).when(
                (col("us.CaseStatus").isNotNull()) & 
                (~col("t.CaseStatus").isin('36', '52')) & 
                (add_years(col("us.DecisionDate"), 5) >= add_years(col("t.DecisionDate"), 2)) & 
                (add_years(col("us.DecisionDate"), 5) >= current_date()), "UT Retained"
            ).when(
                (col("us.CaseStatus").isNotNull()) & 
                (col("t.CaseStatus").isin('36', '52')) & 
                (add_years(col("us.DecisionDate"), 5) >= add_years(col("st.DecisionDate"), 2)) & 
                (add_years(col("us.DecisionDate"), 5) >= current_date()), "UT Retained"
            ).when(
                (col("us.CaseStatus").isNotNull()) & 
                (add_years(col("us.DecisionDate"), 5) >= add_years(col("t.DecisionDate"), 2)) & 
                (add_years(col("us.DecisionDate"), 5) < current_date()), "UT Overdue"
            ).when(
                (col("ac.CasePrefix").isin('IA', 'LD', 'LE', 'LH', 'LP', 'LR')) & 
                (col("ac.HOANRef").isNotNull()) & 
                (col("us.CaseStatus").isNull()), "Skeleton Case"
            ).otherwise("Not sure?")
        )
    )

    # Select the CaseNo column as output
    appeal_casesstatus = filtered_df.select("ac.CaseNo", "CaseStatusCategory")

    return appeal_casesstatus

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: stg_ftirstTier_filtered 
# MAGIC Segmentation query – First Tier appeal cases between 6 months of disposal and date of destruction. 
# MAGIC

# COMMAND ----------

def add_years(date_col, years):
    return date_col + expr(f'INTERVAL {years} YEARS')

@dlt.table(
    name="stg_firsttier_filtered",
    comment="Delta Live Table for filtering AppealCase records to archive or delete based on complex conditions.",
    path=f"{bronze_mnt}/stg_firsttier_filtered"
)
def stg_firsttier_filtered():
    # Reading base tables
    appeal_cases =  dlt.read("stg_appealcasestatus_filtered")
    FTRetained_cases = appeal_cases.alias("ac").filter(col('CaseStatusCategory') == 'FT Retained - ARM').select("ac.CaseNo",lit('FirstTier').alias('Segment'))

    return FTRetained_cases.orderBy("ac.CaseNo")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: stg_skeleton_filtered 
# MAGIC Segmentation query –  Skeleton Cases

# COMMAND ----------


@dlt.table(
    name="stg_skeleton_filtered",
    comment="Delta Live Table for filtering AppealCase records based on specified conditions.",
    path=f"{bronze_mnt}/stg_skeleton_filtered"
)
def stg_skeleton_filtered():
    # Reading base tables
    appeal_case =  dlt.read("raw_appealcase")
    status =  dlt.read("raw_status")
    file_location =  dlt.read("raw_filelocation")

    # Subqueries for each LEFT JOIN condition
    max_status = (
        status
        .filter(
            (col("outcome").isNull() | (col("outcome") != 38) & (col("outcome") != 111)) &
            (col("casestatus").isNull() | (col("casestatus").cast("int") != 17))
        )
        .groupBy("CaseNo")
        .agg(max("StatusId").alias("max_ID"))
    )

    ut_status = (
        status
        .filter(col("CaseStatus").cast("int").isin(40, 41, 42, 43, 44, 45, 53, 27, 28, 29, 34, 32, 33))
        .groupBy("CaseNo")
        .agg(max("StatusID").alias("UT_ID"))
    )

    # Join subqueries to the main AppealCase table
    result = (
        appeal_case.alias("ac")
        .join(max_status.alias("s"), col("ac.CaseNo") == col("s.CaseNo"), "left")
        .join(status.alias("t"), (col("t.CaseNo") == col("s.CaseNo")) & (col("t.StatusID") == col("s.max_ID")), "left")
        .join(ut_status.alias("UT"), col("ac.CaseNo") == col("UT.CaseNo"), "left")
        .join(status.alias("us"), (col("us.CaseNo") == col("UT.CaseNo")) & (col("us.StatusID") == col("UT.UT_ID")), "left")
        .join(file_location.alias("fl"), col("ac.CaseNo") == col("fl.CaseNo"), "left")
    )

    # Filter logic based on complex conditions
    archive_cases = result.filter(
        (col("ac.CaseType") == "1") &
        (~col("fl.DeptId").isin(519, 520)) &
        (
            ((col("ac.CasePrefix").isin("IA", "LD", "LE", "LH", "LP", "LR")) & col("ac.HOANRef").isNull()) &
            ((col("ac.CasePrefix").isin("IA", "LD", "LE", "LH", "LP", "LR")) & col("us.CaseStatus").isNotNull()) &
            (
                (col("ac.CasePrefix").isin("IA", "LD", "LE", "LH", "LP", "LR")) &
                col("ac.HOANRef").isNotNull() &
                ((add_years(col("t.DecisionDate"), 2) >= current_date()) | col("t.DecisionDate").isNull())
            )
        )
    ).select("ac.CaseNo", lit('FirstTier').alias('Segment'))

    return archive_cases.orderBy("ac.CaseNo")




# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: stg_uppertribunalretained_filtered 
# MAGIC Segmentation query – Upper Tribunal retained cases. 

# COMMAND ----------

@dlt.table(
    name="stg_uppertribunalretained_filtered",
    comment="Delta Live Table for filtering second tier AppealCase records for archive or delete based on complex conditions.",
    path=f"{bronze_mnt}/stg_uppertribunalretained_filtered"
)
def stg_uppertribunalretained_filtered():
    appeal_cases =  dlt.read("stg_appealcasestatus_filtered")
    
    UTRetained_cases = appeal_cases.alias('ac').filter(col("CaseStatusCategory") == 'UT Retained').select("ac.CaseNo", lit('UpperTribunal').alias('Segment'))

    return UTRetained_cases.orderBy("ac.CaseNo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: stg_firsttieroverdue_filtered 
# MAGIC Segmentation query – First Tier Appeal cases overdue destruction 
# MAGIC

# COMMAND ----------

@dlt.table(
    name="stg_firsttieroverdue_filtered",
    comment="Delta Live Table for filtering First Tier Overdue records based on specified conditions.",
    path=f"{bronze_mnt}/stg_firsttieroverdue_filtered"
)
def stg_firsttieroverdue_filtered():
    # Reading base tables
    appeal_cases =  dlt.read("stg_appealcasestatus_filtered")

    FTOverdue_cases = appeal_cases.alias('ac').filter(col("CaseStatusCategory") == 'FT Overdue').select("ac.CaseNo", lit('FirstTier').alias('Segment'))


    return FTOverdue_cases.orderBy("ac.CaseNo")


# COMMAND ----------

# MAGIC  %md
# MAGIC ### Transformation: stg_uppertribunaloverdue_filtered 
# MAGIC Segmentation query – Upper Tribunal Appeal cases overdue destruction 

# COMMAND ----------

@dlt.table(
    name="stg_uppertribunaloverdue_filtered",
    comment="Delta Live Table for filtering First Tier Overdue records based on specified conditions.",
    path=f"{bronze_mnt}/stg_uppertribunaloverdue_filtered"
)
def stg_uppertribunaloverdue_filtered():
    # Reading base tables
    appeal_cases =  dlt.read("stg_appealcasestatus_filtered")

    FTOverdue_cases = appeal_cases.alias('ac').filter(col("CaseStatusCategory") == 'UT Overdue').select("ac.CaseNo", lit('UpperTribunal').alias('Segment'))

    return FTOverdue_cases.orderBy("ac.CaseNo")


# COMMAND ----------

# MAGIC  %md
# MAGIC ### Transformation: stg_filepreservedcases_filtered 
# MAGIC Segmentation query – File preserved cases. 

# COMMAND ----------

@dlt.table(
    name="stg_filepreservedcases_filtered",
    comment="Delta Live Table for filtering AppealCase records where CaseType is '1' and DeptId is 520.",
    path=f"{bronze_mnt}/stg_filepreservedcases_filtered"
)
def stg_filepreservedcases_filtered():
    # Reading the base tables
    appeal_case =  dlt.read("raw_appealcase")
    file_location =  dlt.read("raw_filelocation")

    # Joining AppealCase with FileLocation and applying filters
    filtered_cases = (
        appeal_case.alias("ac")
        .join(file_location.alias("fl"), col("ac.CaseNo") == col("fl.CaseNo"), "left")
        .filter(
            (col("ac.CaseType") == '1') &
            (col("fl.DeptId") == 520)
        )
        .select("ac.CaseNo", lit('FilePreservedCases').alias('Segment'))
    )

    return filtered_cases


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation: stg_appeals_filtered
# MAGIC Segmentation query – amalgamated segmentations

# COMMAND ----------

@dlt.table(
    name="stg_appeals_filtered",
    comment="Delta Live Table that combines filtered cases from First Tier, Skeleton, Upper Tribunal, and Preserved Cases tables.",
    path=f"{bronze_mnt}/stg_appeals_filtered"
)
def stg_appeals_filtered():
    # Reading individual filtered tables
    stg_firsttier_filtered =  dlt.read("stg_firsttier_filtered")
    stg_skeleton_filtered =  dlt.read("stg_skeleton_filtered")
    stg_uppertribunalretained_filtered =  dlt.read("stg_uppertribunalretained_filtered")
    stg_filepreservedcases_filtered =  dlt.read("stg_filepreservedcases_filtered")
    stg_firsttieroverdue_filtered = dlt.read("stg_firsttieroverdue_filtered")



    # Using unionAll to combine all cases from the tables into one DataFrame
    combined_cases = (
        stg_firsttier_filtered
        .unionByName(stg_skeleton_filtered)
        .unionByName(stg_uppertribunalretained_filtered)
        .unionByName(stg_filepreservedcases_filtered)
        # .unionByName(stg_uppertribunaloverdue_filtered)
        .unionByName(stg_filepreservedcases_filtered)
    )

    # Selecting all columns from the combined cases
    return stg_firsttieroverdue_filtered


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Silver DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_appealcase_detail

# COMMAND ----------

@dlt.table(
    name="silver_appealcase_detail",
    comment="Delta Live silver Table for Appeals case detailsn.",
    path=f"{silver_mnt}/silver_appealcase_detail"
)
def silver_appealcase_detail():
    appeals_df = dlt.read("bronze_appealcase_cr_cs_ca_fl_cres_mr_res_lang").alias("ap")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

    joined_df = appeals_df.join(flt_df, col("ap.CaseNo") == col("flt.CaseNo"), "inner").select("ap.*")

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_caseapplicant_detail

# COMMAND ----------

@dlt.table(
    name="silver_caseapplicant_detail",
    comment="Delta Live silver Table for casenapplicant detail.",
    path=f"{silver_mnt}/silver_caseapplicant_detail"
)
def silver_caseapplicant_detail():
    appeals_df = dlt.read("bronze_appealcase_ca_apt_country_detc").alias("ca")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

    joined_df = appeals_df.join(flt_df, col("ca.CaseNo") == col("flt.CaseNo"), "inner").select("ca.*")

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_list_detail

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.ariadm_arm_appeals.bronze_appealcase_cl_ht_list_lt_hc_c_ls_adj

# COMMAND ----------

@dlt.table(
    name="silver_list_detail",
    comment="Delta Live silver Table for list detail.",
    path=f"{silver_mnt}/silver_list_detail"
)
def silver_list_detail():
    appeals_df = dlt.read("bronze_appealcase_cl_ht_list_lt_hc_c_ls_adj").alias("ca")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

    joined_df = appeals_df.join(flt_df, col("ca.CaseNo") == col("flt.CaseNo"), "inner").select("ca.*")

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_dfdairy_detail

# COMMAND ----------

@dlt.table(
    name="silver_dfdairy_detail",
    comment="Delta Live silver Table for dfdairy detail.",
    path=f"{silver_mnt}/silver_dfdairy_detail"
)
def silver_dfdairy_detail():
    appeals_df = dlt.read("bronze_appealcase_bfdiary_bftype").alias("df")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

    joined_df = appeals_df.join(flt_df, col("df.CaseNo") == col("flt.CaseNo"), "inner").select("df.*")

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_history_detail

# COMMAND ----------

@dlt.table(
    name="silver_history_detail",
    comment="Delta Live silver Table for list detail.",
    path=f"{silver_mnt}/silver_history_detail"
)
def silver_history_detail():
    appeals_df = dlt.read("bronze_appealcase_history_users").alias("hu")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

    joined_df = appeals_df.join(flt_df, col("hu.CaseNo") == col("flt.CaseNo"), "inner").select("hu.*")

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_link_detail

# COMMAND ----------

@dlt.table(
    name="silver_link_detail",
    comment="Delta Live silver Table for list detail.",
    path=f"{silver_mnt}/silver_link_detail"
)
def silver_link_detail():
    appeals_df = dlt.read("bronze_appealcase_link_linkdetail").alias("ld")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

    joined_df = appeals_df.join(flt_df, col("ld.CaseNo") == col("flt.CaseNo"), "inner").select("ld.*")

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_status_detail

# COMMAND ----------

@dlt.table(
    name="silver_status_detail",
    comment="Delta Live silver Table for status detail.",
    path=f"{silver_mnt}/silver_status_detail"
)
def silver_status_detail():
    appeals_df = dlt.read("bronze_appealcase_status_sc_ra_cs").alias("st")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

    joined_df = appeals_df.join(flt_df, col("st.CaseNo") == col("flt.CaseNo"), "inner").select("st.*")

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_appealcategory_detail

# COMMAND ----------

@dlt.table(
    name="silver_appealcategory_detail",
    comment="Delta Live silver Table for status detail.",
    path=f"{silver_mnt}/silver_appealcategory_detail"
)
def silver_appealcategory_detail():
    appeals_df = dlt.read("bronze_appealcase_appealcatagory_catagory").alias("ac")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

    joined_df = appeals_df.join(flt_df, col("ac.CaseNo") == col("flt.CaseNo"), "inner").select("ac.*")

    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_case_detail

# COMMAND ----------

# %sql
# WITH CTE AS (
#   SELECT * FROM hive_metastore.ariadm_arm_appeals.silver_case_detail
# ),
# cte02 as (
# SELECT CaseNo,CaseSponsorName,CaseSponsorForenames,CaseSponsorTitle, count(*) as count FROM CTE 
# GROUP BY CaseNo,CaseSponsorName,CaseSponsorForenames,CaseSponsorTitle HAVING count(*) > 1
# ) 

# SELECT CaseNo,CaseSponsorName,CaseSponsorForenames,CaseSponsorTitle, count(*) FROM cte02
# GROUP BY CaseNo,CaseSponsorName,CaseSponsorForenames,CaseSponsorTitle HAVING count(*) > 1

# COMMAND ----------

# %sql
# WITH CTE AS (
#   SELECT * FROM hive_metastore.ariadm_arm_appeals.silver_case_detail
# )
# SELECT CaseNo,CaseSponsorName,CaseSponsorForenames,CaseSponsorTitle, count(*) as count FROM CTE 
# GROUP BY CaseNo,CaseSponsorName,CaseSponsorForenames,CaseSponsorTitle HAVING count(*) > 1

# COMMAND ----------

@dlt.table(
    name="silver_case_detail",
    comment="Delta Live silver Table for case detail.", 
    path=f"{silver_mnt}/silver_case_detail"
)
def silver_case_detail():
    case_df = dlt.read("bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at").alias("case")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

    joined_df = case_df.join(flt_df, col("case.CaseNo") == col("flt.CaseNo"), "inner").select("case.*")
    return joined_df


# COMMAND ----------

@dlt.table(
    name="silver_statusdecisiontype_detail",
    comment="Delta Live silver Table for transaction detail.",
    path=f"{silver_mnt}/silver_statusdecisiontype_detail"
)
def silver_statusdecisiontype_detail():
    status_decision_df = dlt.read("bronze_status_decisiontype").alias("status")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = status_decision_df.join(flt_df, col("status.CaseNo") == col("flt.CaseNo"), "inner").select("status.*")
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_transaction_detail

# COMMAND ----------

@dlt.table(
    name="silver_transaction_detail",
    comment="Delta Live silver Table for transaction detail.",
    path=f"{silver_mnt}/silver_transaction_detail"
)
def silver_transaction_detail():
    status_decision_df = dlt.read("bronze_appealcase_t_tt_ts_tm").alias("tran")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = status_decision_df.join(flt_df, col("tran.CaseNo") == col("flt.CaseNo"), "inner").select("tran.*")
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_humanright_detail

# COMMAND ----------

@dlt.table(
    name="silver_humanright_detail",
    comment="Delta Live silver Table for human rights detail.",
    path=f"{silver_mnt}/silver_humanright_detail"
)
def silver_humanright_detail():
    humanright_df = dlt.read("bronze_appealcase_ahr_hr").alias("hr")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = humanright_df.join(flt_df, col("hr.CaseNo") == col("flt.CaseNo"), "inner").select("hr.*")
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_newmatter_detail"

# COMMAND ----------

@dlt.table(
    name="silver_newmatter_detail",
    comment="Delta Live silver Table for new matter detail.",
    path=f"{silver_mnt}/silver_newmatter_detail"
)
def silver_newmatter_detail():
    newmatter_df = dlt.read("bronze_appealcase_anm_nm").alias("nm")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = newmatter_df.join(flt_df, col("nm.CaseNo") == col("flt.CaseNo"), "inner").select("nm.*")
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_documents_detail

# COMMAND ----------

@dlt.table(
    name="silver_documents_detail",
    comment="Delta Live silver Table for documents detail.",
    path=f"{silver_mnt}/silver_documents_detail"
)
def silver_documents_detail():
    documents_df = dlt.read("bronze_appealcase_dr_rd").alias("doc")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = documents_df.join(flt_df, col("doc.CaseNo") == col("flt.CaseNo"), "inner").select("doc.*")
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : sliver_direction_detail

# COMMAND ----------

@dlt.table(
    name="sliver_direction_detail",
    comment="Delta Live silver Table for direction details.",
    path=f"{silver_mnt}/sliver_direction_detail"
)
def sliver_direction_detail():
    direction_df = dlt.read("bronze_appealcase_rsd_sd").alias("dir")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = direction_df.join(flt_df, col("dir.CaseNo") == col("flt.CaseNo"), "inner").select("dir.*")
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : Silver_reviewspecificdirection_detail

# COMMAND ----------

@dlt.table(
    name="Silver_reviewspecificdirection_detail",
    comment="Delta Live silver Table for review-specific direction details.",
    path=f"{silver_mnt}/Silver_reviewspecificdirection_detail"
)
def Silver_reviewspecificdirection_detail():
    review_specific_direction_df = dlt.read("bronze_review_specific_direction").alias("rsd")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = review_specific_direction_df.join(flt_df, col("rsd.CaseNo") == col("flt.CaseNo"), "inner").select("rsd.*")
    return joined_df


# COMMAND ----------

@dlt.table(
    name="silver_costaward_detail",
    comment="Delta Live silver Table for cost award detail.",
    path=f"{silver_mnt}/silver_costaward_detail"
)
def silver_costaward_detail():
    costaward_df = dlt.read("bronze_cost_award").alias("ca")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = costaward_df.join(flt_df, col("ca.CaseNo") == col("flt.CaseNo"), "inner").select("ca.*")
    return joined_df


# COMMAND ----------

@dlt.table(
    name="silver_costorder_detail",
    comment="Delta Live silver Table for cost order detail.",
    path=f"{silver_mnt}/silver_costorder_detail"
)
def silver_costorder_detail():
    costorder_df = dlt.read("bronze_costorder").alias("co")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = costorder_df.join(flt_df, col("co.CaseNo") == col("flt.CaseNo"), "inner").select("co.*")
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : NO CASENO

# COMMAND ----------

# DBTITLE 1,hearingpointschange
# @dlt.table(
#     name="silver_hearingpointschange_detail",
#     comment="Delta Live silver Table for hearing points change reason detail.",
#     path=f"{silver_mnt}/silver_hearingpointschange_detail"
# )
# def silver_hearingpointschange_detail():
#     hearingpointschange_df = dlt.read("bronze_hearing_points_change_reason").alias("hpc")
#     flt_df = dlt.read("stg_appeals_filtered").alias("flt")

#     joined_df = hearingpointschange_df.join(flt_df, col("hpc.CaseNo") == col("flt.CaseNo"), "inner").select("hpc.*")
#     return joined_df


# COMMAND ----------

# DBTITLE 1,hearingpointshistory
# @dlt.table(
#     name="silver_hearing_points_history_detail",
#     comment="Delta Live silver Table for hearing points history detail.",
#     path=f"{silver_mnt}/silver_hearing_points_history_detail"
# )
# def silver_hearing_points_history_detail():
#     hearingpointshistory_df = dlt.read("bronze_hearing_points_history").alias("hph")
#     flt_df = dlt.read("stg_appeals_filtered").alias("flt")

#     joined_df = hearingpointshistory_df.join(flt_df, col("hph.CaseNo") == col("flt.CaseNo"), "inner").select("hph.*")
#     return joined_df


# COMMAND ----------

# DBTITLE 1,appealtypecategory
# @dlt.table(
#     name="silver_appealtypecategory_detail",
#     comment="Delta Live silver Table for appeal type category detail.",
#     path=f"{silver_mnt}/silver_appealtypecategory_detail"
# )
# def silver_appealtypecategory_detail():
#     appealtypecategory_df = dlt.read("bronze_appeal_type_category").alias("atc")
#     flt_df = dlt.read("stg_appeals_filtered").alias("flt")

#     joined_df = appealtypecategory_df.join(flt_df, col("atc.CaseNo") == col("flt.CaseNo"), "inner").select("atc.*")
#     return joined_df


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
# MAGIC          <td>Date of decision .</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>recordDate*</td>
# MAGIC          <td>Date of decision .</td>
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

# “ARIAFTA” for First tier appeals 

# “ARIAUTA” for Upper Tribunal appeals 

# COMMAND ----------

@dlt.table(
    name="silver_archive_metadata",
    comment="Delta Live Silver Table for Archive Metadata data.",
    path=f"{silver_mnt}/silver_archive_metadata"
)
def silver_archive_metadata():
    metadata_df = dlt.read("silver_appealcase_detail").alias("ac")\
            .join(dlt.read("silver_caseapplicant_detail").alias('ca'), col("ac.CaseNo") == col("ca.CaseNo"), "inner")\
            .join(dlt.read("stg_appeals_filtered").alias('flt'), col("ac.CaseNo") == col("flt.CaseNo"), "inner")\
            .filter(col("ca.CaseAppellantRelationship").isNull())\
    .select(
        col('ac.CaseNo').alias('client_identifier'),
        date_format(col('ac.DateOfApplicationDecision'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_date"),
        date_format(col('ac.DateOfApplicationDecision'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("recordDate"),
        lit("GBR").alias("region"),
        lit("ARIA").alias("publisher"),
        when(col('flt.Segment') == 'FirstTier', 'ARIAFTA')
        .when(col('flt.Segment') == 'UpperTribunal', 'ARIAUTA')
        .alias("record_class"),
        lit('IA_Tribunal').alias("entitlement_tag"),
        col('ac.HORef').alias('bf_001'),
        col('ca.AppellantForenames').alias('bf_002'),
        col('ca.AppellantName').alias('bf_003'),
        col('ca.AppellantBirthDate').alias('bf_004'),
        col('ca.PortReference').alias('bf_005'),
        col('ca.AppellantPostcode').alias('bf_006'))
    
    return metadata_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Outputs and Tracking DLT Table Creation

# COMMAND ----------

# DBTITLE 1,Secret Retrieval for Database Connection
secret = dbutils.secrets.get("ingest00-keyvault-sbox", "ingest00-adls-ingest00curatedsbox-connection-string-sbox")

# COMMAND ----------

# DBTITLE 1,Azure Blob Storage Container Access
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

# Set up the BlobServiceClient with your connection string
connection_string = f"BlobEndpoint=https://ingest00curatedsbox.blob.core.windows.net/;QueueEndpoint=https://ingest00curatedsbox.queue.core.windows.net/;FileEndpoint=https://ingest00curatedsbox.file.core.windows.net/;TableEndpoint=https://ingest00curatedsbox.table.core.windows.net/;SharedAccessSignature={secret}"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Specify the container name
container_name = "gold"
container_client = blob_service_client.get_container_client(container_name)


# COMMAND ----------

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType
import dlt

# Helper functions for date formatting
def format_date_iso(date_value):
    if date_value:
        return datetime.strftime(date_value, "%Y-%m-%d")
    return ""

def format_date(date_value):
    if date_value:
        return datetime.strftime(date_value, "%d/%m/%Y")
    return ""

# Helper function to find data from a list by CaseNo
def find_data_in_list(data_list, case_no):
    for row in data_list:
        if row['CaseNo'] == case_no:
            return row
    return None

# Define function to generate HTML content for a given CaseNo
def generate_html_content(case_no, appealcase_details_list, appellant_details_list, history_list):
    try:
        appealcase_details = [row for row in appealcase_details_list if row['CaseNo'] == case_no]
        appellant_details = [row for row in appellant_details_list if row['CaseNo'] == case_no]
        history = [row for row in history_list if row['CaseNo'] == case_no]

        if not appealcase_details:
            print(f"No details found for CaseNo: {case_no}")
            return None, "No details found"

        html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/appeals-no-js-v4-template.html"
        with open(html_template_path, "r") as f:
            html_template = "".join([l for l in f])

        row_dict = appealcase_details[0].asDict()
        appellant_row_dict = appellant_details[0].asDict()

        replacements = {
            "{{CaseNo}}": str(row_dict.get('CaseNo', '') or ''),
            "{{hoRef}}": str(row_dict.get('hoRef', '') or ''),
            "{{DateApplicationLodged}}": format_date_iso(row_dict.get('DateApplicationLodged')),
            "{{DateOfApplicationDecision}}": format_date_iso(row_dict.get('DateOfApplicationDecision')),
            "{{DateLodged}}": format_date_iso(row_dict.get('DateLodged')),
            "{{DateReceived}}": format_date_iso(row_dict.get('DateReceived')),
            "{{AdditionalGrounds}}": str(row_dict.get('AdditionalGrounds', '') or ''),
            "{{AppealCategories}}": str(row_dict.get('AppealCategories', '') or ''),
            "{{MREmbassy}}": str(row_dict.get('MREmbassy', '') or ''),
            "{{NationalityId}}": str(row_dict.get('NationalityId', '') or ''),
            "{{CountryId}}": str(row_dict.get('CountryId', '') or ''),
            "{{PortId}}": str(row_dict.get('PortId', '') or ''),
            "{{DateOfIssue}}": format_date_iso(row_dict.get('DateOfIssue')),
            "{{fileLocationNote}}": str(row_dict.get('fileLocationNote', '') or ''),
            "{{DocumentsReceived}}": str(row_dict.get('DocumentsReceived', '') or ''),
            "{{TransferOutDate}}": format_date_iso(row_dict.get('TransferOutDate')),
            "{{RemovalDate}}": format_date_iso(row_dict.get('RemovalDate')),
            "{{DeportationDate}}": format_date_iso(row_dict.get('DeportationDate')),
            "{{ProvisionalDestructionDate}}": format_date_iso(row_dict.get('ProvisionalDestructionDate')),
            "{{NoticeSentDate}}": format_date_iso(row_dict.get('NoticeSentDate')),
            "{{AppealReceivedBy}}": str(row_dict.get('AppealReceivedBy', '') or ''),
            "{{CRRespondent}}": str(row_dict.get('CRRespondent', '') or ''),
            "{{RepresentativeRef}}": str(row_dict.get('RepresentativeRef', '') or ''),
            "{{Language}}": str(row_dict.get('Language', '') or ''),
            "{{HOInterpreter}}": str(row_dict.get('HOInterpreter', '') or ''),
            "{{CourtPreference}}": str(row_dict.get('CourtPreference', '') or ''),
            "{{CertifiedDate}}": format_date_iso(row_dict.get('CertifiedDate')),
            "{{CertifiedRecordedDate}}": format_date_iso(row_dict.get('CertifiedRecordedDate')),
            "{{ReferredToJudgeDate}}": format_date_iso(row_dict.get('ReferredToJudgeDate')),
            "{{RespondentName}}": str(row_dict.get('RespondentName', '') or ''),
            "{{MRPOU}}": str(row_dict.get('MRPOU', '') or ''),
            "{{RespondentAddress1}}": str(row_dict.get('RespondentAddress1', '') or ''),
            "{{RespondentAddress2}}": str(row_dict.get('RespondentAddress2', '') or ''),
            "{{RespondentAddress3}}": str(row_dict.get('RespondentAddress3', '') or ''),
            "{{RespondentAddress4}}": str(row_dict.get('RespondentAddress4', '') or ''),
            "{{RespondentAddress5}}": str(row_dict.get('RespondentAddress5', '') or ''),
            "{{RespondentPostcode}}": str(row_dict.get('RespondentPostcode', '') or ''),
            "{{RespondentTelephone}}": str(row_dict.get('RespondentTelephone', '') or ''),
            "{{RespondentFax}}": str(row_dict.get('RespondentFax', '') or ''),
            "{{RespondentEmail}}": str(row_dict.get('RespondentEmail', '') or ''),
            "{{CRReference}}": str(row_dict.get('CRReference', '') or ''),
            "{{CRContact}}": str(row_dict.get('CRContact', '') or ''),
            "{{CaseRepName}}": str(row_dict.get('CaseRepName', '') or ''),
            "{{CaseRepPostcode}}": str(row_dict.get('CaseRepPostcode', '') or ''),
            "{{CaseRepTelephone}}": str(row_dict.get('CaseRepTelephone', '') or ''),
            "{{CaseRepFax}}": str(row_dict.get('CaseRepFax', '') or ''),
            "{{CaseRepEmail}}": str(row_dict.get('CaseRepEmail', '') or ''),
            "{{LSCCommission}}": str(row_dict.get('LSCCommission', '') or ''),
            "{{RepTelephone}}": str(row_dict.get('RepTelephone', '') or ''),
            "{{StatutoryClosureDate}}": format_date_iso(row_dict.get('StatutoryClosureDate')),
            "{{ThirdCountryId}}": str(row_dict.get('ThirdCountryId', '') or ''),
            "{{SubmissionURN}}": str(row_dict.get('SubmissionURN', '') or ''),
            "{{DateReinstated}}": format_date_iso(row_dict.get('DateReinstated')),
            "{{CaseOutcomeId}}": str(row_dict.get('CaseOutcomeId', '') or '')
        }

        for key, value in replacements.items():
            html_template = html_template.replace(key, value)

        History_Code = ''
        for index, row in enumerate(history, start=1):
            line = f"<tr><td id=\"midpadding\">{format_date(row['HistDate'])}</td><td id=\"midpadding\">{row['HistType']}</td><td id=\"midpadding\">{row['UserName']}</td><td id=\"midpadding\">{row['HistoryComment']}</td></tr>"
            History_Code += line + '\n'
        html_template = html_template.replace(f"{{{{HistoryPlaceHolder}}}}", History_Code)

        return html_template, "Success"

    except Exception as e:
        print(f"Error writing file for CaseNo: {case_no}: {str(e)}")
        return None, f"Error writing file: {str(e)}"

# Define a function to run the HTML generation for each partition
def process_partition(partition, appealcase_details_bc, appellant_details_bc, history_bc):
    results = []
    for row in partition:
        case_no = row['CaseNo']
        html_content, status = generate_html_content(
            case_no,
            appealcase_details_bc.value,
            appellant_details_bc.value,
            history_bc.value
        )
        
        target_path = f"ARIADM/ARM/APPEALS/HTML/appeal_case_{case_no.replace('/', '_')}.html"
        blob_client = container_client.get_blob_client(target_path)

        if status == "Success":
            try:
                blob_client.upload_blob(html_content, overwrite=True)
            except Exception as e:
                print(f"Error uploading HTML for CaseNo {case_no}: {str(e)}")
                continue

        results.append((case_no, target_path, status))
    return results

@dlt.table(
    name="gold_appeal_html_generation_status",
    comment="Delta Live Table for Appeal Case HTML Generation Status.",
    path=f"{gold_mnt}/gold_appeal_html_generation_status"
)
def gold_appeal_html_generation_status():
    df_appealcase_details = dlt.read("silver_appealcase_detail")
    df_appellant_details = dlt.read("silver_caseapplicant_detail").filter(col("CaseAppellantRelationship").isNotNull())
    df_history = dlt.read("silver_history_detail")

    # Load the necessary dataframes from Hive metastore if not initial load
    if not initial_Load:
        df_appealcase_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealcase_detail")
        df_appellant_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_caseapplicant_detail").filter(col("CaseAppellantRelationship").isNotNull())
        df_history = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_history_detail")

    appealcase_details_bc = spark.sparkContext.broadcast(df_appealcase_details.collect())
    appellant_details_bc = spark.sparkContext.broadcast(df_appellant_details.collect())
    history_bc = spark.sparkContext.broadcast(df_history.collect())

    num_partitions = 32
    repartitioned_df = df_appealcase_details.repartition(num_partitions, "CaseNo")

    result_rdd = repartitioned_df.rdd.mapPartitions(lambda partition: process_partition(partition, appealcase_details_bc, appellant_details_bc, history_bc))
    results = result_rdd.collect()

    schema = StructType([
        StructField("CaseNo", StringType(), True),
        StructField("TargetPath", StringType(), True),
        StructField("Status", StringType(), True)
    ])
    df_results = spark.createDataFrame(results, schema)
    return df_results

# COMMAND ----------

# df_appealcase_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealcase_detail")
# df_appealcase_details.select("CaseNo").distinct().count()

# COMMAND ----------

# DBTITLE 1,test function
# df_appealcase_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealcase_detail")
# df_appellant_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_caseapplicant_detail").filter(col("CaseAppellantRelationship").isNotNull())
# df_history = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_history_detail")

# appealcase_details_bc = spark.sparkContext.broadcast(df_appealcase_details.collect())
# appellant_details_bc = spark.sparkContext.broadcast(df_appellant_details.collect())
# history_bc = spark.sparkContext.broadcast(df_history.collect())

# # generate_html_conentent('EA/00007/2023', appealcase_details_list.value, history_list.value)

# # case_no = 'EA/00007/2023'

# case_no = 'IA/00001/2011'

# # appeal_case_IA_00001_2011.html

# html_content, status = generate_html_content(
#         case_no,
#         # appellant_name,
#         appealcase_details_bc.value,
#         history_bc.value
#     )

# displayHTML(html_content)       

# COMMAND ----------

Dependentsdetailstemplate = """
<tr>
    <td colspan="2" style="vertical-align: top;">
        <table id="table4">
            <tbody><tr>
                <td id="labels"><label for="depName">Name : </label></td>
                <td><input type="text"  id="depName"  value="{{AppellantName}}" size="50"></td>
            </tr>
            <tr>
                <td id="labels"><label for="depForename">Forename(s) : </label></td>
                <td><input type="text" id="depForename" value="{{AppellantForenames}}" size="50"></td>
            </tr>
            <tr>
                <td id="labels"><label for="depTitle">Title : </label></td>
                <td><input type="text" id="depTitle" value="{{AppellantTitle}}"></td>
            </tr>
            <tr>
                <td id="labels"><label for="depRelationship">Relationship : </label></td>
                <td><input type="text" id="depRelationship" value="{{CaseAppellantRelationship}}" ></td>
            </tr>
            <tr>
                <td id="labels" style="vertical-align: top;"><label for="depAddress">Address: </label></td>
                <td style="vertical-align: top;">
                    <input type="text" id="addressLine1" name="addressLine1" value="{{AppellantAddress1}}" size="50"><br>
                    <input type="text" id="addressLine2" name="addressLine2" value="{{AppellantAddress2}}" size="50"><br>
                    <input type="text" id="addressLine3" name="addressLine3" value="{{AppellantAddress3}}" size="50"><br>
                    <input type="text" id="addressLine4" name="addressLine4" value="{{AppellantAddress4}}" size="50"><br>
                    <input type="text" id="addressLine5" name="addressLine5" value="{{AppellantAddress5}}" size="50"><br>
                </td>
            </tr>
            <tr>
                <td id="labels"><label for="depPostcode">Postcode : </label></td>
                <td><input type="text" id="depPostcode" value="{{AppellantPostcode}}" ></td>
            </tr>
            <tr>
                <td id="labels"><label for="depPhone">Phone : </label></td>
                <td><input type="text" id="depPhone" value="{{AppellantTelephone}}" ></td>
            </tr>
        </tbody></table>
    </td>
</tr>
"""

# displayHTML(Dependentsdetailstemplate)

# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_appeals.silver_history_detail where CaseNo like 'EA/00007/2023'

# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_appeals.silver_history_detail
# join hive_metastore.ariadm_arm_appeals.stg_appeals_filtered on silver_history_detail.CaseNo = stg_appeals_filtered.CaseNo

# COMMAND ----------

# HistoryComment = [row['HistoryComment'] for row in history if row['HistType'] == 16]
# HistoryComment

# fileLocationNote = [row['HistoryComment'] for row in history if row['HistType'] == 6]
# fileLocationNote

# COMMAND ----------

# DBTITLE 1,latest hist
# from pyspark.sql.functions import row_number
# from pyspark.sql.window import Window

# # # df_Latest_history = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_history_detail")


# windowSpec = Window.partitionBy("HistType", "CaseNo").orderBy(df_history_details["HistDate"].desc())
# df_Latest_history = df_history_details.withColumn("row_number", row_number().over(windowSpec)).filter("row_number = 1").drop("row_number")

# df_Latest_history = df_Latest_history.select("HistType", "HistoryComment", "HistDate", "CaseNo")
# display(df_Latest_history)

# COMMAND ----------

# df_case_detail.createOrReplaceTempView("case_detail")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select caseNo, PaymentRemissionReasonDescription from case_detail

# COMMAND ----------

# DBTITLE 1,test function code
# df_appealcase_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealcase_detail")
# df_appellant_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_caseapplicant_detail").filter(col("CaseAppellantRelationship").isNull())
# df_dependent_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_caseapplicant_detail").filter(col("CaseAppellantRelationship").isNotNull())
# df_bfdiary_details = spark.read.table("hive_metastore.ariadm_arm_appeals.bronze_appealcase_bfdiary_bftype")
# df_history_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_history_detail")
# df_link_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_link_detail")
# df_status_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_status_detail")
# df_appealcategory_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealcategory_detail")
# df_case_detail = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_case_detail")
# df_sponsor = df_case_detail.select("CaseNo","CaseSponsorName","CaseSponsorForenames","CaseSponsorTitle","CaseSponsorAddress1","CaseSponsorAddress2","CaseSponsorAddress3","CaseSponsorAddress4","CaseSponsorAddress5","CaseSponsorPostcode","CaseSponsorTelephone","CaseSponsorEmail").distinct()
# df_transaction_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_transaction_detail")
# df_humanright_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_humanright_detail")
# df_newmatter_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_newmatter_detail")
# df_documents_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_documents_detail")
# df_reviewstandarddirection_details = spark.read.table("hive_metastore.ariadm_arm_appeals.sliver_direction_detail")
# df_reviewspecificdirection_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_reviewspecificdirection_detail")
# df_costaward_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_costaward_detail")
# df_costorder_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_costorder_detail")
# # df_hearingpointschange_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_hearingpointschange_detail")
# # df_hearing_points_history_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_hearing_points_history_detail")
# # df_appealtypecategory_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealtypecategory_detail")


# appealcase_details_bc = spark.sparkContext.broadcast(df_appealcase_details.collect())
# appellant_details_bc = spark.sparkContext.broadcast(df_appellant_details.collect())
# dependent_details_bc = spark.sparkContext.broadcast(df_dependent_details.collect())
# df_bfdiary_details_bc = spark.sparkContext.broadcast(df_bfdiary_details.collect())
# history_bc = spark.sparkContext.broadcast(df_history_details.collect())
# link_bc = spark.sparkContext.broadcast(df_link_details.collect())
# status_details_bc = spark.sparkContext.broadcast(df_status_details.collect())
# appealcategory_detail_bc = spark.sparkContext.broadcast(df_appealcategory_details.collect())
# case_detail_bc = spark.sparkContext.broadcast(df_case_detail.collect())
# sponsor_bc = spark.sparkContext.broadcast(df_sponsor.collect())
# transaction_detail_bc = spark.sparkContext.broadcast(df_transaction_details.collect())
# humanright_detail_bc = spark.sparkContext.broadcast(df_humanright_details.collect())
# newmatter_detail_bc = spark.sparkContext.broadcast(df_newmatter_details.collect())

# documents_detail_bc = spark.sparkContext.broadcast(df_documents_details.collect())
# reviewstandarddirection_detail_bc = spark.sparkContext.broadcast(df_reviewstandarddirection_details.collect())
# reviewspecificdirection_detail_bc = spark.sparkContext.broadcast(df_reviewspecificdirection_details.collect())
# costaward_detail_bc = spark.sparkContext.broadcast(df_costaward_details.collect())
# costorder_detail_bc = spark.sparkContext.broadcast(df_costorder_details.collect())

# # hearingpointschange_detail_bc = spark.sparkContext.broadcast(df_hearingpointschange_details.collect())
# # hearing_points_history_detail_bc = spark.sparkContext.broadcast(df_hearing_points_history_details.collect())
# # appealtypecategory_detail_bc = spark.sparkContext.broadcast(df_appealtypecategory_details.collect())

# # case_no = 'IA/00001/2011'
# # case_no = 'EA/00007/2023'
# case_no = 'IA/00001/2005' 
# # case_no = 'IA/00050/2014' #human rights
# # case_no = 'IA/00175/2014' # new matter
# # case_no = 'IA/00001/2007' # document
# # case_no = 'IA/00001/2015'
# # case_no = 'IA/00006/2005' # reviewstandard
# # case_no = 'IA/00001/2006' # costorder
# # case_no = 'IA/00189/2014'
# # case_no = 'IA/00014/2011' # payment Event Summary
# # case_no = 'IA/00002/2015' # linked files

# appealcase_details_list = appealcase_details_bc.value
# appellant_details_list = [row for row in appellant_details_bc.value if row['CaseNo'] == case_no]
# dependent_details_list = dependent_details_bc.value
# bfdiary_details_list = df_bfdiary_details_bc.value
# history_list = history_bc.value
# link_list = link_bc.value
# status_details_list = status_details_bc.value
# appealcategory_details_list = appealcategory_detail_bc.value
# case_detail_list = case_detail_bc.value
# sponsor_list = sponsor_bc.value
# transaction_detail_list = transaction_detail_bc.value
# humanright_detail_list = humanright_detail_bc.value
# newmatter_detail_list = newmatter_detail_bc.value
# documents_detail_list = documents_detail_bc.value
# reviewstandarddirection_detail_list  = reviewstandarddirection_detail_bc.value
# reviewspecificdirection_detail_list  = reviewspecificdirection_detail_bc.value
# costaward_detail_list  = costaward_detail_bc.value
# costorder_detail_list  = costorder_detail_bc.value
# # hearingpointschange_detail_list  = hearingpointschange_detail_bc.value
# # hearing_points_history_detail_list  = hearing_points_history_detail_bc.value
# # appealtypecategory_detail_list  = appealtypecategory_detail_bc.value


# appealcase_details = [row for row in appealcase_details_list if row['CaseNo'] == case_no]
# appellant_details = [row for row in appellant_details_list if row['CaseNo'] == case_no]
# dependent_details = [row for row in dependent_details_list if row['CaseNo'] == case_no]
# bfdiary_details = [row for row in bfdiary_details_list if row['CaseNo'] == case_no]
# history = [row for row in history_list if row['CaseNo'] == case_no]
# link_details = [row for row in link_list if row['CaseNo'] == case_no]
# status_details = [row for row in status_details_list if row['CaseNo'] == case_no]
# appealcategory_details = [row for row in appealcategory_details_list if row['CaseNo'] == case_no]
# case_detail_details = [row for row in case_detail_list if row['CaseNo'] == case_no]
# sponsor_list_details = [row for row in sponsor_list if row['CaseNo'] == case_no]
# transaction_details = [row for row in transaction_detail_list if row['CaseNo'] == case_no]
# humanright_details = [row for row in humanright_detail_list if row['CaseNo'] == case_no]
# newmatter_details = [row for row in newmatter_detail_list if row['CaseNo'] == case_no]
# documents_details = [row for row in documents_detail_list if row['CaseNo'] == case_no]
# reviewstandarddirection_details = [row for row in reviewstandarddirection_detail_list if row['CaseNo'] == case_no]
# reviewspecificdirection_details = [row for row in reviewspecificdirection_detail_list if row['CaseNo'] == case_no]
# costaward_details = [row for row in costaward_detail_list if row['CaseNo'] == case_no]
# costorder_details = [row for row in costorder_detail_list if row['CaseNo'] == case_no]

# if not appealcase_details:
#     print(f"No details found for CaseNo: {case_no}")
#     # return None, "No details found"

# # https://ingest00landingsbox.blob.core.windows.net/html-template/appeals-no-js-v4-template.html
# # html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/appeals-no-js-v2-template.html"
# html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/appeals-no-js-v4-template.html"
# with open(html_template_path, "r") as f:
#     html_template = "".join([l for l in f])

# row_dict = appealcase_details[0].asDict()
# appellant_dict = appellant_details[0].asDict()
# sponsor_dist = sponsor_list_details[0].asDict()

# if link_details:
#     connectedFiles = 'Connected Files exist'
# else:
#     connectedFiles = ''

# # Replace placeholders with data from the tables
# replacements = {
# "{{CaseNo}}": str(row_dict.get('CaseNo', '') or ''),
# "{{hoRef}}": str(row_dict.get('hoRef', '') or ''),
# "{{CCDAppealNum}}": str(row_dict.get('CCDAppealNum', '') or ''),
# "{{CaseRepDXNo1}}": str(row_dict.get('CaseRepDXNo1', '') or ''),
# "{{CaseRepDXNo2}}": str(row_dict.get('CaseRepDXNo2', '') or ''),
# "{{AppealTypeId}}": str(row_dict.get('AppealTypeId', '') or ''),
# "{{DateApplicationLodged}}": format_date_iso(row_dict.get('DateApplicationLodged')),
# "{{DateOfApplicationDecision}}": format_date_iso(row_dict.get('DateOfApplicationDecision')),
# "{{DateLodged}}": format_date_iso(row_dict.get('DateLodged')),
# "{{DateReceived}}": format_date_iso(row_dict.get('DateReceived')),
# "{{AdditionalGrounds}}": str(row_dict.get('AdditionalGrounds', '') or ''),
# "{{AppealCategories}}": str(row_dict.get('AppealCategories', '') or ''),
# "{{MREmbassy}}": str(row_dict.get('MREmbassy', '') or ''),
# "{{NationalityId}}": str(row_dict.get('NationalityId', '') or ''),
# "{{CountryId}}": str(row_dict.get('CountryId', '') or ''),
# "{{PortId}}": str(row_dict.get('PortId', '') or ''),
# "{{HumanRights}}": str(row_dict.get('HumanRights', '') or ''),
# "{{DateOfIssue}}": format_date_iso(row_dict.get('DateOfIssue')),
# "{{fileLocationNote}}": str(row_dict.get('fileLocationNote', '') or ''),
# "{{DocumentsReceived}}": str(row_dict.get('DocumentsReceived', '') or ''),
# "{{TransferOutDate}}": format_date_iso(row_dict.get('TransferOutDate')),
# "{{RemovalDate}}": format_date_iso(row_dict.get('RemovalDate')),
# "{{DeportationDate}}":  format_date_iso(row_dict.get('DeportationDate')),
# "{{ProvisionalDestructionDate}}": format_date_iso(row_dict.get('ProvisionalDestructionDate')),
# "{{NoticeSentDate}}": format_date_iso(row_dict.get('NoticeSentDate')),
# "{{AppealReceivedBy}}": str(row_dict.get('AppealReceivedBy', '') or ''),
# "{{CRRespondent}}": str(row_dict.get('CRRespondent', '') or ''),
# "{{RepresentativeRef}}": str(row_dict.get('RepresentativeRef', '') or ''),
# "{{Language}}": str(row_dict.get('Language', '') or ''),
# "{{HOInterpreter}}": str(row_dict.get('HOInterpreter', '') or ''),
# "{{CourtPreference}}": str(row_dict.get('CourtPreference', '') or ''),
# "{{CertifiedDate}}": format_date_iso(row_dict.get('CertifiedDate')),
# "{{CertifiedRecordedDate}}": format_date_iso(row_dict.get('CertifiedRecordedDate')),
# "{{ReferredToJudgeDate}}": format_date_iso(row_dict.get('ReferredToJudgeDate')),
# "{{RespondentName}}": str(row_dict.get('RespondentName', '') or ''),
# "{{MRPOU}}": str(row_dict.get('MRPOU', '') or ''),
# "{{RespondentAddress1}}": str(row_dict.get('RespondentAddress1', '') or ''),
# "{{RespondentAddress2}}": str(row_dict.get('RespondentAddress2', '') or ''),
# "{{RespondentAddress3}}": str(row_dict.get('RespondentAddress3', '') or ''),
# "{{RespondentAddress4}}": str(row_dict.get('RespondentAddress4', '') or ''),
# "{{RespondentAddress5}}": str(row_dict.get('RespondentAddress5', '') or ''),
# "{{RespondentPostcode}}": str(row_dict.get('RespondentPostcode', '') or ''),
# "{{RespondentTelephone}}": str(row_dict.get('RespondentTelephone', '') or ''),
# "{{RespondentFax}}": str(row_dict.get('RespondentFax', '') or ''),
# "{{RespondentEmail}}": str(row_dict.get('RespondentEmail', '') or ''),
# "{{CRReference}}": str(row_dict.get('CRReference', '') or ''),
# "{{CRContact}}": str(row_dict.get('CRContact', '') or ''),
# "{{CaseRepName}}": str(row_dict.get('CaseRepName', '') or ''),
# "{{CaseRepPostcode}}": str(row_dict.get('CaseRepPostcode', '') or ''),
# "{{CaseRepTelephone}}": str(row_dict.get('CaseRepTelephone', '') or ''),
# "{{CaseRepFax}}": str(row_dict.get('CaseRepFax', '') or ''),
# "{{CaseRepEmail}}": str(row_dict.get('CaseRepEmail', '') or ''),
# "{{LSCCommission}}": str(row_dict.get('LSCCommission', '') or ''),
# "{{RepTelephone}}": str(row_dict.get('RepTelephone', '') or ''),
# "{{StatutoryClosureDate}}": format_date_iso(row_dict.get('StatutoryClosureDate')),
# "{{ThirdCountryId}}": str(row_dict.get('ThirdCountryId', '') or ''),
# "{{SubmissionURN}}": str(row_dict.get('SubmissionURN', '') or ''),
# "{{DateReinstated}}": format_date_iso(row_dict.get('DateReinstated')),
# "{{CaseOutcomeId}}": str(row_dict.get('CaseOutcomeId', '') or ''),
# "{{AppealCaseNote}}": str(row_dict.get('AppealCaseNote', '') or ''),
# "{{Interpreter}}": str(row_dict.get('Interpreter', '') or ''),
# "{{LanguageId}}": str(row_dict.get('LanguageId', '') or ''),
# "{{CertOfFeeSatisfaction}}": str(row_dict.get('CertOfFeeSatisfaction', '') or ''),


# # Appellant rows
# "{{AppellantName}}": str(appellant_dict.get('AppellantName', '') or ''),
# "{{AppellantForenames}}": str(appellant_dict.get('AppellantForenames', '') or ''),
# "{{AppellantTitle}}": str(appellant_dict.get('AppellantTitle', '') or ''),
# "{{AppellantBirthDate}}": format_date_iso(appellant_dict.get('AppellantBirthDate')),
# "{{Detained}}" : str(appellant_dict.get('Detained', '') or ''),
# "{{DetentionCentre}}": str(appellant_dict.get('DetentionCentre', '') or ''),
# "{{AppellantAddress1}}": str(appellant_dict.get('AppellantAddress1', '') or ''),
# "{{AppellantAddress2}}": str(appellant_dict.get('AppellantAddress2', '') or ''),
# "{{AppellantAddress3}}": str(appellant_dict.get('AppellantAddress3', '') or ''),
# "{{AppellantAddress4}}": str(appellant_dict.get('AppellantAddress4', '') or ''),
# "{{AppellantAddress5}}": str(appellant_dict.get('AppellantAddress5', '') or ''),
# "{{Country}}": str(appellant_dict.get('Country', '') or ''),
# "{{DCPostcode}}": str(appellant_dict.get('DCPostcode', '') or ''),
# "{{AppellantTelephone}}": str(appellant_dict.get('AppellantTelephone', '') or ''),
# "{{AppellantEmail}}": str(appellant_dict.get('AppellantEmail', '') or ''),
# "{{PrisonRef}}": str(appellant_dict.get('PrisonRef', '') or ''),
# "{{FCONumber}}": str(appellant_dict.get('FCONumber', '') or ''),
# "{{PortReference}}": str(appellant_dict.get('PortReference', '') or ''),
# # "{{AppellantId}}": str(appellant_dict.get('AppellantId', '') or ''),
# # Add more placeholders and replace them as necessary

# #linked details
# "{{connectedFiles}}": str(connectedFiles),

# # sponsor details
# "{{CaseSponsorName}}": str(sponsor_dist.get('CaseSponsorName', '') or ''),
# "{{CaseSponsorForenames}}": str(sponsor_dist.get('CaseSponsorForenames', '') or ''),
# "{{CaseSponsorTitle}}": str(sponsor_dist.get('CaseSponsorTitle', '') or ''),
# "{{CaseSponsorAddress1}}": str(sponsor_dist.get('CaseSponsorAddress1', '') or ''),
# "{{CaseSponsorAddress2}}": str(sponsor_dist.get('CaseSponsorAddress2', '') or ''),
# "{{CaseSponsorAddress3}}": str(sponsor_dist.get('CaseSponsorAddress3', '') or ''),
# "{{CaseSponsorAddress4}}": str(sponsor_dist.get('CaseSponsorAddress4', '') or ''),
# "{{CaseSponsorAddress5}}": str(sponsor_dist.get('CaseSponsorAddress5', '') or ''),
# "{{CaseSponsorPostcode}}": str(sponsor_dist.get('CaseSponsorPostcode', '') or ''),
# "{{CaseSponsorTelephone}}": str(sponsor_dist.get('CaseSponsorTelephone', '') or ''),
# "{{CaseSponsorEmail}}": str(sponsor_dist.get('CaseSponsorEmail', '') or ''),

# }

# for key, value in replacements.items():
#     html_template = html_template.replace(key, value)

# # AdditionalGrounds detail -- PaymentRemissionReasonDescription need to be Description
# AdditionalGrounds_Code = ''
# for index, row in enumerate(case_detail_details, start=1):
#     line = f"<tr><td id=\"midpadding\"></td><td id=\"midpadding\">{row['PaymentRemissionReasonDescription']}</td></tr>"
#     AdditionalGrounds_Code += line + '\n'
# html_template = html_template.replace(f"{{{{AdditionalGroundsPlaceHolder}}}}", AdditionalGrounds_Code)




# # linkfiles_details -- Appellant Name??
# linkfiles_Code = ''
# for index, row in enumerate(link_details, start=1):
#     line = f"<tr><td id=\"midpadding\"></td><td id=\"midpadding\">{row['LinkNo']}</td><td id=\"midpadding\"></td><td id=\"midpadding\">{row['LinkDetailComment']}</td></tr>"
#     linkfiles_Code += line + '\n'
# html_template = html_template.replace(f"{{{{LinkedFilesPlaceHolder}}}}", linkfiles_Code)




# # costorder_details Details
# PaymentEventsSummary_Code = ''
# for index, row in enumerate(transaction_details, start=1):
#     line = f"<tr><td id=\"midpadding\">{format_date(row['TransactionDate'])}</td><td id=\"midpadding\">{row['TransactionTypeId']}</td><td id=\"midpadding\">{row['TransactionStatusId']}</td><td id=\"midpadding\">{row['Amount']}</td><td id=\"midpadding\">{row['SumTotalPay']}</td><td id=\"midpadding\">{format_date(row['ClearedDate'])}</td><td id=\"midpadding\">{row['PaymentReference']}</td><td id=\"midpadding\">{row['AggregatedPaymentURN']}</td></tr>"
#     PaymentEventsSummary_Code += line + '\n'
# html_template = html_template.replace(f"{{{{PaymentEventsSummaryPlaceHolder}}}}", PaymentEventsSummary_Code)



# # costorder_details Details
# costorder_Code = ''
# for index, row in enumerate(costorder_details, start=1):
#     line = f"<tr><td id=\"midpadding\">{row['AppealStageWhenApplicationMade']}</td><td id=\"midpadding\">{format_date(row['DateOfApplication'])}</td><td id=\"midpadding\">{row['AppealStageWhenDecisionMade']}</td><td id=\"midpadding\">{row['OutcomeOfAppealWhereDecisionMade']}</td><td id=\"midpadding\">{format_date(row['DateOfDecision'])}</td><td id=\"midpadding\">{row['CostOrderDecision']}</td><td id=\"midpadding\">{row['ApplyingRepresentativeId']}</td></tr>"
#     costorder_Code += line + '\n'
# html_template = html_template.replace(f"{{{{CostorderdetailsPlaceHolder}}}}", costorder_Code)

# # reviewSpecificdirections Details
# Specificdirections_Code = ''
# for index, row in enumerate(reviewspecificdirection_details, start=1):
#     line = f"<tr><td id=\"midpadding\">{row['ReviewSpecificDirectionId']}</td><td id=\"midpadding\">{format_date(row['DateRequiredIND'])}</td><td id=\"midpadding\">{format_date(row['DateRequiredAppellantRep'])}</td><td id=\"midpadding\">{format_date(row['DateReceivedIND'])}</td><td id=\"midpadding\">{format_date(row['DateReceivedAppellantRep'])}</td></tr>"
#     Specificdirections_Code += line + '\n'
# html_template = html_template.replace(f"{{{{SpecificdirectionsPlaceHolder}}}}", Specificdirections_Code)


# # reviewstandarddirection Details
# reviewstandarddirection_Code = ''
# for index, row in enumerate(reviewstandarddirection_details, start=1):
#     line = f"<tr><td id=\"midpadding\">{row['ReviewStandardDirectionId']}</td><td id=\"midpadding\">{format_date(row['DateRequiredIND'])}</td><td id=\"midpadding\">{format_date(row['DateRequiredAppellantRep'])}</td><td id=\"midpadding\">{format_date(row['DateReceivedIND'])}</td><td id=\"midpadding\">{format_date(row['DateReceivedAppellantRep'])}</td></tr>"
#     reviewstandarddirection_Code += line + '\n'
# html_template = html_template.replace(f"{{{{StandarddirectionsPlacHolder}}}}", reviewstandarddirection_Code)


# # MaintainCostAwards Details
# MaintainCostAwards_Code = ''
# for index, row in enumerate(costaward_details, start=1):
#     line = f"<tr><td id=\"midpadding\">{row['CaseNo']}</td><td id=\"midpadding\">{str(appellant_dict.get('AppellantId', '') or '')}</td><td id=\"midpadding\">{row['AppealStage']}</td><td id=\"midpadding\">{format_date(row['DateOfApplication'])}</td><td id=\"midpadding\">{row['TypeOfCostAward']}</td><td id=\"midpadding\">{row['ApplyingParty']}</td><td id=\"midpadding\">{row['PayingParty']}</td><td id=\"midpadding\">{row['MindedToAward']}</td><td id=\"midpadding\">{row['ObjectionToMindedToAward']}</td><td id=\"midpadding\">{row['CostsAwardDecision']}</td><td id=\"midpadding\">{format_date(row['DateOfDecision'])}</td><td id=\"midpadding\">{row['CostsAmount']}</td></tr>"
#     MaintainCostAwards_Code += line + '\n'
# html_template = html_template.replace(f"{{{{MaintainCostAwardsPlaceHolder}}}}", MaintainCostAwards_Code)

# # DocumentTracking Details
# DocumentTracking_Code = ''
# for index, row in enumerate(documents_details, start=1):
#     line = f"<tr><td id=\"midpadding\">{row['ReceivedDocumentId']}</td><td id=\"midpadding\">{format_date(row['DateRequested'])}</td><td id=\"midpadding\">{format_date(row['DateRequired'])}</td><td id=\"midpadding\">{format_date(row['DateReceived'])}</td><td id=\"midpadding\">{format_date(row['RepresentativeDate'])}</td><td id=\"midpadding\">{format_date(row['POUDate'])}</td><td id=\"midpadding\">{row['NoLongerRequired']}</td></tr>"
#     DocumentTracking_Code += line + '\n'
# html_template = html_template.replace(f"{{{{DocumentTrackingPlaceHolder}}}}", DocumentTracking_Code)


# # newmatter Details
# newmatter_Code = ''
# for index, row in enumerate(newmatter_details, start=1):
#     line = f"<tr><td id=\"midpadding\">{row['NewMatterDescription']}</td><td id=\"midpadding\">{row['AppealNewMatterNotes']}</td><td id=\"midpadding\">{row['DateReceived']}</td><td id=\"midpadding\">{row['DateReferredToHO']}</td><td id=\"midpadding\">{row['HODecision']}</td><td id=\"midpadding\">{row['DateHODecision']}</td></tr>"
#     newmatter_Code += line + '\n'
# html_template = html_template.replace(f"{{{{NewMattersPlaceHolder}}}}", newmatter_Code)



# # human right Details
# humanrigh_Code = ''
# for index, row in enumerate(humanright_details, start=1):
#     line = f"<tr><td id=\"midpadding\">{row['HumanRightDescription']}</td><td id=\"midpadding\">""</td></tr>"
#     humanrigh_Code += line + '\n'
# html_template = html_template.replace(f"{{{{HumanRightsPlaceHolder}}}}", humanrigh_Code)


# # Appeal Categories Details
# AppealCategories_Code = ''
# for index, row in enumerate(appealcategory_details, start=1):
#     line = f"<tr><td id=\"midpadding\">{row['CategoryDescription']}</td><td id=\"midpadding\">{row['Flag']}</td><td id=\"midpadding\">""</td></tr>"
#     AppealCategories_Code += line + '\n'
# html_template = html_template.replace(f"{{{{AppealCategoriesPlaceHolder}}}}", AppealCategories_Code)


# # Status Details
# Status_Code = ''
# for index, row in enumerate(status_details, start=1):
#     line = f"<tr><td id=\"midpadding\">{row['CaseStatus']}</td><td id=\"midpadding\">{format_date(row['KeyDate'])}</td><td id=\"midpadding\">{row['InterpreterRequired']}</td><td id=\"midpadding\">{format_date(row['DecisionDate'])}</td><td id=\"midpadding\">{row['Outcome']}</td><td id=\"midpadding\">{format_date(row['Promulgated'])}</td></tr>"
#     Status_Code += line + '\n'
# html_template = html_template.replace(f"{{{{StatusPlaceHolder}}}}", Status_Code)


# # History Details
# History_Code = ''
# for index, row in enumerate(history, start=1):
#     line = f"<tr><td id=\"midpadding\">{format_date(row['HistDate'])}</td><td id=\"midpadding\">{row['HistType']}</td><td id=\"midpadding\">{row['UserName']}</td><td id=\"midpadding\">{row['HistoryComment']}</td></tr>"
#     History_Code += line + '\n'
# html_template = html_template.replace(f"{{{{HistoryPlaceHolder}}}}", History_Code)

# # bfdiary Details
# bfdiary_Code = ''
# for index, row in enumerate(bfdiary_details, start=1):
#     line = f"<tr><td id=\"midpadding\">{format_date(row['EntryDate'])}</td><td id=\"midpadding\">{row['BFTypeDescription']}</td><td id=\"midpadding\">{row['Entry']}</td><td id=\"midpadding\">{format_date(row['DateCompleted'])}</td></tr>"
#     bfdiary_Code += line + '\n'
# html_template = html_template.replace(f"{{{{bfdiaryPlaceHolder}}}}", bfdiary_Code)

# #dependents_detail
# dependents_detail_Code = ''
# for index, row in enumerate(dependent_details, start=1):
#     line = f"<tr><td id=\"midpadding\">{row['AppellantName']}</td><td id=\"midpadding\">{row['CaseAppellantRelationship']}</td></tr>"
#     dependents_detail_Code += line + '\n'
# html_template = html_template.replace(f"{{{{DependentsPlaceHolder}}}}", dependents_detail_Code)

# dependent_details_Code = ''
# if dependent_details:
    
    
#     for index, row in enumerate(dependent_details, start=1):
#         line = Dependentsdetailstemplate.replace("{{AppellantName}}", row['AppellantName']) \
#                                         .replace("{{AppellantForenames}}", row['AppellantForenames']) \
#                                         .replace("{{AppellantTitle}}", row['AppellantTitle']) \
#                                         .replace("{{CaseAppellantRelationship}}", row['CaseAppellantRelationship']) \
#                                         .replace("{{AppellantAddress1}}", row['AppellantAddress1']) \
#                                         .replace("{{AppellantAddress2}}", row['AppellantAddress2']) \
#                                         .replace("{{AppellantAddress3}}", row['AppellantAddress3']) \
#                                         .replace("{{AppellantAddress4}}", row['AppellantAddress4']) \
#                                         .replace("{{AppellantAddress5}}", row['AppellantAddress5']) \
#                                         .replace("{{AppellantPostcode}}", row['AppellantPostcode']) \
#                                         .replace("{{AppellantTelephone}}", row['AppellantTelephone'])
#         dependent_details_Code += line + '\n'
#         html_template = html_template.replace(f"{{{{Dependents}}}}", "Dependent Details Exists")
# else:
#     dependent_details_Code = Dependentsdetailstemplate.replace("{{AppellantName}}", "") \
#                                                       .replace("{{AppellantForenames}}", "") \
#                                                       .replace("{{AppellantTitle}}", "") \
#                                                       .replace("{{CaseAppellantRelationship}}", "") \
#                                                       .replace("{{AppellantAddress1}}", "") \
#                                                       .replace("{{AppellantAddress2}}", "") \
#                                                       .replace("{{AppellantAddress3}}", "") \
#                                                       .replace("{{AppellantAddress4}}", "") \
#                                                       .replace("{{AppellantAddress5}}", "") \
#                                                       .replace("{{AppellantPostcode}}", "") \
#                                                       .replace("{{AppellantTelephone}}", "")
#     html_template = html_template.replace(f"{{{{Dependents}}}}", "")
# html_template = html_template.replace(f"{{{{DependentsdetailsPlaceHolder}}}}", dependent_details_Code)



# displayHTML(html_template)

# COMMAND ----------

# df_history = spark.read.table("hive_metastore.ariadm_arm_appeals.bronze_appealcase_history_users")
# df_appealcase_details = spark.read.table("hive_metastore.ariadm_arm_appeals.bronze_appealcase_cr_cs_ca_fl_cres_mr_res_lang")
# df_appellant_details = spark.read.table("hive_metastore.ariadm_arm_appeals.bronze_appealcase_ca_apt_country_detc")

# # ARIADM/ARM/APPEALS/HTML/appeal_case_IA_00006_2015_Jessop.html
# # display(df_history.filter(col('CaseNo').like('%AH/0000%')))

# #  html_content, status = generate_html_content(
# #             'BZ/00001',
# #             appellant_name,
# #             appealcase_details_bc.value,
# #             history_bc.value
# #         )

# # BZ/00001 

# # 'BZ/00000'    '

# display(df_appellant_details.filter(col('CaseNo').like('BZ%')))

# COMMAND ----------

# common_case_nos = df_appealcase_details.select("CaseNo").join(df_history.select("CaseNo"), "CaseNo")
# display(common_case_nos.distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate HTML
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create gold_td_html_generation_status & Processing TribunalDecision HTML's
# MAGIC This section is to prallel process the HTML and create atracker table with log information.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate JSON

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create gold_td_Json_generation_status & Processing TribunalDecision Json's
# MAGIC This section is to prallel process the json and create atracker table with log information.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate a360 files

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create gold_a360_generation_status & Processing TribunalDecision A360's
# MAGIC This section is to prallel process the json and create atracker table with log information.

# COMMAND ----------

# DBTITLE 1,Sample for  reference
#Sample
# {"operation": "create_record","relation_id":"152820","record_metadata":{"publisher":"IADEMO","record_class":"IADEMO","region":"GBR","recordDate":"2023-04-12T00:00:00Z","event_date":"2023-04-12T00:00:00Z","client_identifier":"HU/02287/2021","bf_001":"Orgest","bf_002":"Hoxha","bf_003":"A1234567/001","bf_004":"1990-06-09T00:00:00Z","bf_005":"ABC/12345","bf_010":"2024-01-01T00:00:00Z"}}

# {"operation": "upload_new_file","relation_id":"152820","file_metadata":{"publisher":"IADEMO","dz_file_name":"HU_02287_2021.json","file_tag":"json"}}

# {"operation": "upload_new_file","relation_id":"152820","file_metadata":{"publisher":"IADEMO","dz_file_name":"HU022872021.pdf","file_tag":"pdf"}}
 

# COMMAND ----------

# DBTITLE 1,Generating Tribunal decision Profiles in A360 Outputs
# # Function to format dates
# def format_date_zulu(date_value):
#     if isinstance(date_value, str):  # If the date is already a string, return as is
#         return date_value
#     elif isinstance(date_value, (datetime,)):
#         return datetime.strftime(date_value, "%Y-%m-%dT%H:%M:%SZ")
#     return None  # Return None if the date is invalid or not provided

# # Function to generate .a360 file for tribunal decisions
# def generate_a360_file_for_tribunaldecision(CaseNo, Forenames, Name, metadata_list):
#     try:
#         # Find the metadata for the case
#         row_dict = next((row for row in metadata_list if row['client_identifier'] == CaseNo and row['bf_001'] == Forenames and row['bf_002'] == Name), None)
#         if not row_dict:
#             print(f"No details found for Case No: {CaseNo}")
#             return None, f"No details for Case No: {CaseNo}"

#         # Create metadata, HTML, and JSON strings
#         metadata_data = {
#             "operation": "create_record",
#             "relation_id": row_dict['client_identifier'],
#             "record_metadata": {
#                 "publisher": row_dict['publisher'],
#                 "record_class": row_dict['record_class'],
#                 "region": row_dict['region'],
#                 "recordDate": format_date_zulu(row_dict['recordDate']),
#                 "event_date": format_date_zulu(row_dict['event_date']),
#                 "client_identifier": row_dict['client_identifier'],
#                 "bf_001": row_dict['bf_001'],
#                 "bf_002": row_dict['bf_002'],
#                 "bf_003": format_date_zulu(row_dict['bf_003']),
#                 "bf_004": format_date_zulu(row_dict['bf_004']),
#                 "bf_005": row_dict['bf_005']
#             }
#         }

#         html_data = {
#             "operation": "upload_new_file",
#             "relation_id": row_dict['client_identifier'],
#             "file_metadata": {
#                 "publisher": row_dict['publisher'],
#                 "dz_file_name": f"tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.html",
#                 "file_tag": "html"
#             }
#         }

#         json_data = {
#             "operation": "upload_new_file",
#             "relation_id": row_dict['client_identifier'],
#             "file_metadata": {
#                 "publisher": row_dict['publisher'],
#                 "dz_file_name": f"tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.json",
#                 "file_tag": "json"
#             }
#         }

#         # Convert dictionaries to JSON strings
#         metadata_data_str = json.dumps(metadata_data, separators=(',', ':'))
#         html_data_str = json.dumps(html_data, separators=(',', ':'))
#         json_data_str = json.dumps(json_data, separators=(',', ':'))

#         # Combine the data
#         all_data_str = f"{metadata_data_str}\n{html_data_str}\n{json_data_str}"

#         # Define the file path for each tribunal decision
#         target_path = f"ARIADM/ARM/TD/A360/tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.a360"
        
#         # Upload the content to Azure Blob Storage
#         blob_client = container_client.get_blob_client(target_path)
#         blob_client.upload_blob(all_data_str, overwrite=True)

#         return target_path, "Success"
#     except Exception as e:
#         print(f"Error writing file for Case No: {CaseNo}: {str(e)}")
#         return None, f"Error writing file: {str(e)}"

# # Define a function to run the A360 file generation for each partition
# def process_partition_a360(partition, metadata_bc,blob_service_client, container_name):
#     results = []
#     for row in partition:
#         CaseNo, Forenames, Name = row['CaseNo'], row['Forenames'], row['Name']

#         file_name, status = generate_a360_file_for_tribunaldecision(CaseNo, Forenames, Name, metadata_bc.value)
        
#         if file_name is None:
#             results.append((CaseNo, Forenames, Name, "", status))
#         else:
#             results.append((CaseNo, Forenames, Name, file_name, status))
#     return results

# # Delta Live Table
# @dlt.table(
#     name="gold_td_a360_generation_status",
#     comment="Delta Live Table for Gold Tribunal Decision .a360 File Generation Status.",
#     path=f"{gold_mnt}/gold_td_a360_generation_status"
# )
# def gold_td_a360_generation_status():
#     # df_td_filtered = dlt.read("stg_td_filtered")
#     df_td_metadata = dlt.read("silver_archive_metadata")

#     if not initial_Load:
#         print("Running non-initial load")
#         # df_td_filtered = spark.read.table("hive_metastore.ariadm_arm_iris_td.stg_td_filtered")
#         df_td_metadata = spark.read.table("hive_metastore.ariadm_arm_td.silver_archive_metadata")
    
#     # Fetch the list of CaseNo, Forenames, and Name from the table (as Spark DataFrame)
#     CaseNo_df = df_td_metadata.select(col('client_identifier').alias('CaseNo'), 
#                                       col('bf_001').alias('Forenames'), 
#                                       col('bf_002').alias('Name')).distinct()

#     # Repartition by CaseNo for optimized parallel processing
#     num_partitions = 64  # Assuming an 8-worker cluster
#     repartitioned_df = CaseNo_df.repartition(num_partitions, "CaseNo")

#     # Broadcast metadata to all workers (using the Spark DataFrame instead of the collected list)
#     metadata_bc = spark.sparkContext.broadcast(df_td_metadata.collect())

#     # Run the A360 file generation on each partition
#     result_rdd = repartitioned_df.rdd.mapPartitions(lambda partition: process_partition_a360(partition, metadata_bc,blob_service_client, container_name))

#     # Collect results
#     results = result_rdd.collect()

#     # Create DataFrame for output
#     schema = StructType([
#         StructField("CaseNo", StringType(), True),
#         StructField("Forenames", StringType(), True),
#         StructField("Name", StringType(), True),
#         StructField("GeneratedFilePath", StringType(), True),
#         StructField("Status", StringType(), True)
#     ])
#     a360_result_df = spark.createDataFrame(results, schema=schema)

#     # Log the A360 paths in the Delta Live Table
#     return a360_result_df.select("CaseNo", "Forenames", "Name", "GeneratedFilePath", "Status")


# COMMAND ----------

# display(spark.read.format("binaryFile").load(f"{gold_mnt}/HTML").count())
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/JSON").count())
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/A360").count())

# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_appeals.gold_appeal_html_generation_status
# where status like  '%Error%'
# -- # -- Error writing file: Comment


# -- # BZ/00001     

# COMMAND ----------

# from pyspark.sql.functions import col

# files_df = dbutils.fs.ls("/mnt/ingest00landingsboxlanding")
# filtered_files_df = spark.createDataFrame(files_df).filter(col("path").contains("Review"))
# display(filtered_files_df)

# COMMAND ----------

# %sql

# select * from 
# hive_metastore.ariadm_arm_appeals.bronze_appealcase_ca_apt_country_detc
# where CaseNo in ('IM/00023/2003')

# COMMAND ----------

# %sql
# select CaseNo,  count(*) from hive_metastore.ariadm_arm_appeals.bronze_appealcase_ca_apt_country_detc 
# where CaseAppellantRelationship is null
# group by  CaseNo
# having count(*) >1


# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_appeals.bronze_appealcase_ca_apt_country_detc 


# COMMAND ----------

# DBTITLE 1,Create ReviewSpecficDirection
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import date_format, current_timestamp
# import re

# # Initialize Spark session
# # spark = SparkSession.builder.appName("SingleFileParquetWriter").getOrCreate()

# # Define schema for an empty DataFrame
# review_specific_direction_schema = StructType([
#     StructField("ReviewSpecificDirectionId", IntegerType(), False),
#     StructField("CaseNo", StringType(), False),
#     StructField("StatusId", IntegerType(), False),
#     StructField("SpecificDirection", StringType(), True),
#     StructField("DateRequiredIND", TimestampType(), True),
#     StructField("DateRequiredAppellantRep", TimestampType(), True),
#     StructField("DateReceivedIND", TimestampType(), True),
#     StructField("DateReceivedAppellantRep", TimestampType(), True)
# ])

# # Create an empty DataFrame with the defined schema
# review_specific_direction_df = spark.createDataFrame([], review_specific_direction_schema)

# # Generate timestamp for unique file naming
# datesnap = spark.sql("select date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]

# # Temporary output path (to generate a single .parquet file within a folder)
# temp_output_path = f"/mnt/ingest00landingsboxlanding/ReviewSpecificDirection/temp_{datesnap}"
# review_specific_direction_df.coalesce(1).write.format("parquet").mode("overwrite").save(temp_output_path)

# # Get the single .parquet file generated in the temporary folder
# files = dbutils.fs.ls(temp_output_path)
# parquet_file = [file.path for file in files if re.match(r".*\.parquet$", file.path)][0]

# # Final output path for the single .parquet file
# final_output_path = f"/mnt/ingest00landingsboxlanding/ReviewSpecificDirection//full/SQLServer_Sales_IRIS_dbo_ReviewSpecificDirection_{datesnap}.parquet"

# # Move the single .parquet file to the desired location
# dbutils.fs.mv(parquet_file, final_output_path)

# # Clean up the temporary folder
# dbutils.fs.rm(temp_output_path, True)

# # Read and display schema to confirm the file output
# df = spark.read.format("parquet").load(final_output_path)
# df.printSchema()
# display(df)


# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_appeals.stg_appeals_filtered

# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_appeals.gold_appeal_html_generation_status

# COMMAND ----------

# display(spark.read.format("binaryFile").load(f"{gold_mnt}/HTML").count())
# # display(spark.read.format("binaryFile").load(f"{gold_mnt}/JSON").count())
# # display(spark.read.format("binaryFile").load(f"{gold_mnt}/A360").count())
