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
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-139">ARIADM-139</a>/NSA/Nov-2024</td>
# MAGIC          <td>Appeals : Compete Bronze to silver Notebook</td>
# MAGIC       </tr>
# MAGIC        <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-140">ARIADM-140</a>/NSA/Nov-2024</td>
# MAGIC          <td>Appeals : First Iteration: Appeals: Create Gold Output files - HTML, JSON,A360</td>
# MAGIC       </tr>
# MAGIC        <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-265">ARIADM-265</a>/NSA/DEC-2024</td>
# MAGIC          <td>Appeals : Second Iteration: Appeals: Create Gold Output files - HTML</td>
# MAGIC       </tr>
# MAGIC        <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-361">ARIADM-361</a>/NSA/DEC-2024</td>
# MAGIC          <td>Update New Bronze Table with Linked Cost Award</td>
# MAGIC       </tr>
# MAGIC        <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-363">ARIADM-363</a>/NSA/DEC-2024</td>
# MAGIC          <td>Update HTML Mapping Excluding Status detail</td>
# MAGIC       </tr>
# MAGIC        <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-371">ARIADM-271</a>/NSA/DEC-2024</td>
# MAGIC          <td>Mapping Hearing Points tables to Appellant for ARM Appeals</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-362">ARIADM-362</a>/NSA/04-Feb-2024</td>
# MAGIC          <td>Update HTML Status Details Implementation</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-263">ARIADM-263</a>/NSA/17-Feb-2024</td>
# MAGIC          <td>Create Function for Gold Output Files</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-365">ARIADM-365</a>/NSA/17-Feb-2024</td>
# MAGIC          <td>Optimisation for the curation and generation of Gold output</td>
# MAGIC       </tr>
# MAGIC        <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-453">ARIADM-453</a>/NSA/01-Mar-2024</td>
# MAGIC          <td>Appeals StatusDetils: 35 (Migration) need to be included </td>
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

# COMMAND ----------

# from SharedFunctionsLib.custom_functions import *

# COMMAND ----------

pip install azure-storage-blob


# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Variables

# COMMAND ----------

# MAGIC %md
# MAGIC Please note that running the DLT pipeline with the parameter `initial_load = true` will ensure the creation of the corresponding Hive tables. However, during this stage, none of the gold outputs (HTML, JSON, and A360) are processed. To generate the gold outputs, a secondary run with `initial_load = true` is required.

# COMMAND ----------


read_hive = True

# Setting variables for use in subsequent cells
raw_mnt = "/mnt/ingest00rawsboxraw/ARIADM/ARM/APPEALS"
landing_mnt = "/mnt/ingest00landingsboxlanding/"
bronze_mnt = "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/APPEALS"
silver_mnt = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/APPEALS"
gold_mnt = "/mnt/ingest00curatedsboxgold/ARIADM/ARM/APPEALS"
html_mnt = "/mnt/ingest00landingsboxhtml-template"

gold_outputs = "ARIADM/ARM/APPEALS"
hive_schema = "ariadm_arm_appeals"
key_vault = "ingest00-keyvault-sbox"



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
     return read_latest_parquet("ARIAPou", "tv_ARIAPou", "ARIA_ARM_APPEALS") 
 
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
            )
            .join(
                dlt.read("raw_pou").alias("p"),
                col("cr.RespondentId") == col("p.PouId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_embassy").alias("e"),
                col("cr.RespondentId") == col("e.EmbassyId"),
                "left_outer"
            )
            .select(
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
                
                # Court fields
                col("c.CourtName"),
                col("c.DoNotUse").alias("DoNotUseCourt"),
                
                # Hearing Centre fields
                col("hc.Description").alias("HearingCentreDesc"),

                # ListSitting fields
                col("ls.Chairman"),
                col("ls.Position")
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
            .join(
                dlt.read("raw_caseappellant").alias("ca"),
                col("l.LinkNo") == col("ca.CaseNo"),
                "left_outer"
            )
            .join(
                dlt.read("raw_appellant").alias("a"),
                col("ca.AppellantId") == col("a.AppellantId"),
                "left_outer"
            )
            .filter(col("ca.AppellantId").isNull())
            .select(
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
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation M7: bronze_appealcase_status_sc_ra_cs 

# COMMAND ----------

# spark.sql("""
# CREATE TABLE hive_metastore.ariadm_arm_appeals.m7_backup
# AS
# SELECT *
# FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs
# """)

# COMMAND ----------

# %sql
# select distinct IRISStatusOfCase from hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs

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
            .join(
                dlt.read("raw_adjudicator").alias("a"),
                col("s.AdjudicatorId") == col("a.AdjudicatorId"),
                "left_outer"
            ) .join(
                dlt.read("raw_adjudicator").alias("dAdj"),
                col("s.DeterminationBy") == col("dAdj.AdjudicatorId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_stmcases").alias("stm"),
                col("s.StatusId") == col("stm.NewStatusId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_listtype").alias("lt"),
                col("s.ListTypeId") == col("lt.ListTypeId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_hearingtype").alias("ht"),
                col("s.HearingTypeId") == col("ht.HearingTypeId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_adjudicator").alias("a1"),
                col("stm.Judiciary1Id") == col("a1.AdjudicatorId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_adjudicator").alias("a2"),
                col("stm.Judiciary2Id") == col("a2.AdjudicatorId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_adjudicator").alias("a3"),
                col("stm.Judiciary3Id") == col("a3.AdjudicatorId"),
                "left_outer"
            )
            .select(
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
    # appeal_grounds = dlt.read("raw_appealgrounds").alias("ag")
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
        # .join(appeal_grounds, col("ac.CaseNo") == col("ag.CaseNo"), "left_outer") 
        .join(appeal_type, col("ac.AppealTypeId") == col("at.AppealTypeId"), "left_outer")
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
# MAGIC ### Transformation: bronze_review_specific_direction

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
        dlt.read("raw_costaward").alias("ca")
            # .join(
            #     dlt.read("raw_link").alias("1"),
            #     col("ca.CaseNo") == col("1.CaseNo"),
            #     "left_outer" 
            # )
            .join(
                dlt.read("raw_caseappellant").alias("cap"),
                col("ca.CaseNo") == col("cap.CaseNo"),
                "left_outer"  
            )    
            .join(
                dlt.read("raw_appellant").alias("a"),
                col("cap.AppellantId") == col("a.AppellantId"),
                "left_outer"  
            )
            .join(
                dlt.read("raw_casestatus").alias("cs"),
                col("ca.AppealStage") == col("cs.CaseStatusId"),
                "left_outer"
            )
            .select(
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

# COMMAND ----------

# DBTITLE 1,Cost award linked
@dlt.table(
    name="bronze_cost_award_linked",
    comment="Delta Live Table for retrieving details from the CostAward_linked table.",
    path=f"{bronze_mnt}/bronze_cost_award_linked"
)
def bronze_cost_award_linked():
    return (
        dlt.read("raw_costaward").alias("ca") 
            .join(
                dlt.read("raw_link").alias("l"),
                col("ca.CaseNo") == col("l.CaseNo"),
                "left_outer" 
            ) 
            .join(
                dlt.read("raw_caseappellant").alias("cap"),
                col("ca.CaseNo") == col("cap.CaseNo"),
                "left_outer"  
            )
            .join(
                dlt.read("raw_appellant").alias("a"),
                col("cap.AppellantId") == col("a.AppellantId"),
                "left_outer"  
            )
            .join(
                dlt.read("raw_casestatus").alias("cs"),
                col("ca.AppealStage") == col("cs.CaseStatusId"),
                "left_outer"
            )
            .filter(col("l.LinkNo").isNotNull())
            .select(
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
    )

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
    return (
        dlt.read("raw_costorder").alias("co")
            .join(
                dlt.read("raw_caserep").alias("cr"),
                col("co.CaseNo") == col("cr.CaseNo"),
                "left_outer"
            )
            .join(
                dlt.read("raw_representative").alias("r"),
                col("cr.RepresentativeId") == col("r.RepresentativeId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_decisiontype").alias("dt"),
                col("co.OutcomeOfAppealWhereDecisionMade") == col("dt.DecisionTypeId"),
                "left_outer"
            )
            .select(
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
    )



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
    return (
        dlt.read("raw_hearingpointschangereason").alias("hpcr")
            .join(
                dlt.read("raw_status").alias("s"),
                col("s.HearingPointsChangeReasonId") == col("hpcr.HearingPointsChangeReasonId"),
                "left_outer"
            )
            .select(
                col("s.CaseNo"),
                col("s.StatusId"),
                col("hpcr.HearingPointsChangeReasonId"),
                col("hpcr.Description"),
                col("hpcr.DoNotUse")
            )
    )

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
    return (
        dlt.read("raw_hearingpointshistory").alias("hph")
            .join(
                dlt.read("raw_status").alias("s"),
                col("hph.StatusId") == col("s.StatusId"),
                "left_outer"
            )
            .select(
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
    )

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
    
    return (
        appeal_case.alias("ac")
        .join(appeal_type.alias("at"), col("ac.AppealTypeId") == col("at.AppealTypeId"), "left_outer")
        .join(appeal_type_category.alias("atc"), col("at.AppealTypeId") == col("atc.AppealTypeId"), "left_outer")
        .select(
            col("ac.CaseNo"),
            col("atc.AppealTypeCategoryId"),
            col("atc.AppealTypeId"),
            col("atc.CategoryId"),
            col("atc.FeeExempt")
        )
    )

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
    
    return (
        appeal_grounds.alias("ag")
        .join(appeal_type.alias("at"), col("ag.AppealTypeId") == col("at.AppealTypeId"), "left_outer")
        .select(
            col("ag.CaseNo"),
            col("ag.AppealTypeId"),
            col("at.Description").alias("AppealTypeDescription")
        )
    )

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
    
    return (
        case_adjudicator.alias("ca")
        .join(adjudicator.alias("adj"), (col("ca.AdjudicatorId") == col("adj.Adjudicatorid")) & (col("adj.DoNotList") == 0), "inner")
        .select(
            col("ca.CaseNo"),
            col("ca.Required"),
            col("adj.Surname").alias("JudgeSurname"),
            col("adj.Forenames").alias("JudgeForenames"),
            col("adj.Title").alias("JudgeTitle")
        )
    )

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
    
    return (
        case_adjudicator.alias("ca")
        .join(adjudicator.alias("adj"), (col("ca.AdjudicatorId") == col("adj.Adjudicatorid")) & (col("adj.DoNotList") == 0), "inner")
        .select(
            col("ca.CaseNo"),
            col("ca.Required"),
            col("adj.Surname").alias("JudgeSurname"),
            col("adj.Forenames").alias("JudgeForenames"),
            col("adj.Title").alias("JudgeTitle")
        )
    )

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Segmentation tables

# COMMAND ----------

# DBTITLE 1,Transformation: CaseStatusCategory
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
            (col("ac.CasePrefix").isin('DA', 'DC', 'EA', 'HU', 'PA', 'RP') | 
            (col("ac.CasePrefix").isin('LP', 'LR', 'LD', 'LH', 'LE', 'IA') & col("ac.HOANRef").isNull()) | 
            (col("ac.CasePrefix").isin('LP', 'LR', 'LD', 'LH', 'LE', 'IA') & col("ac.HOANRef").isNotNull() & col("us.CaseStatus").isNotNull() & (add_years(col("us.DecisionDate"), 5) < add_years(col("t.DecisionDate"), 2)))) & 
            (
                ((col("t.CaseStatus") == 10) & col("t.Outcome").isin('13', '80', '122', '25', '120', '2', '105', '119')) | 
                ((col("t.CaseStatus") == 46) & col("t.Outcome").isin('31', '2', '50')) | 
                ((col("t.CaseStatus") == 26) & col("t.Outcome").isin('80', '13', '25', '1', '2')) | 
                (col("t.CaseStatus").isin('37', '38') & col("t.Outcome").isin('1', '2', '80', '13', '25', '72', '14', '125')) | 
                ((col("t.CaseStatus") == 39) & col("t.Outcome").isin('30', '31', '25', '14', '80')) | 
                ((col("t.CaseStatus") == 51) & col("t.Outcome").isin('94', '93')) | 
                ((col("t.CaseStatus") == 52) & col("t.Outcome").isin('91', '95') & (col("st.CaseStatus").isNull() | ~col("st.CaseStatus").isin('37', '38', '39', '17', '40', '41', '42', '43', '44', '45', '53', '27', '28', '29', '34', '32', '33'))) | 
                ((col("t.CaseStatus") == 36) & (col("t.Outcome") == 25) & (col("st.CaseStatus").isNull() | ~col("st.CaseStatus").isin('40', '41', '42', '43', '44', '45', '53', '27', '28', '29', '34', '32', '33')))
            ) & 
            (add_months(col("t.DecisionDate"), 6) >= current_date()), "FT Retained - CCD"
        ).when(
                (col("ac.CasePrefix").isin('DA', 'DC', 'EA', 'HU', 'PA', 'RP') & col("us.CaseStatus").isNull()) | 
                (col("ac.CasePrefix").isin('LP', 'LR', 'LD', 'LH', 'LE', 'IA') & col("ac.HOANRef").isNull()) | 
                (col("ac.CasePrefix").isin('LP', 'LR', 'LD', 'LH', 'LE', 'IA') & col("ac.HOANRef").isNotNull() & col("us.CaseStatus").isNotNull() & (add_years(col("us.DecisionDate"), 5) < add_years(col("t.DecisionDate"), 2))) & 
                (
                    ((col("t.CaseStatus").isin(52, 36)) & (col("t.Outcome") == 0) & (col("st.DecisionDate").isNotNull())) | 
                    ((col("t.CaseStatus") == 36) & col("t.Outcome").isin('1', '2', '50', '108')) | 
                    ((col("t.CaseStatus") == 52) & col("t.Outcome").isin('91', '95') & col("st.CaseStatus").isin('37', '38', '39', '17'))
                ) & 
                (add_months(col("t.DecisionDate"), 6) >= current_date()), "FT Retained - CCD"
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
                    col("t.Outcome").isin('1', '2', '80', '13', '25', '72', '14', '125')
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
                    col("t.Outcome").isin('1', '2', '80', '13', '25', '72', '14',)
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
# MAGIC ### Transformation: stg_firstTier_filtered 
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
    stg_uppertribunaloverdue_filtered = dlt.read("stg_uppertribunaloverdue_filtered")



    # Using unionAll to combine all cases from the tables into one DataFrame
    combined_cases = (
        stg_firsttier_filtered
        .unionByName(stg_skeleton_filtered)
        .unionByName(stg_uppertribunalretained_filtered)
        .unionByName(stg_filepreservedcases_filtered)
        .unionByName(stg_uppertribunaloverdue_filtered)
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
    comment="Delta Live silver Table for Appeals case details.",
    path=f"{silver_mnt}/silver_appealcase_detail"
)
def silver_appealcase_detail():
    appeals_df = dlt.read("bronze_appealcase_cr_cs_ca_fl_cres_mr_res_lang").alias("ap")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

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
        when(col("ap.CRRespondent") == 1, col("POUShortName")).otherwise("").alias("POUShortName"),
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
# MAGIC ### Tarnsformation : silver_caseapplicant_detail

# COMMAND ----------

# DBTITLE 1,silver_applicant_detail
@dlt.table(
    name="silver_applicant_detail",
    comment="Delta Live silver Table for casenapplicant detail.",
    path=f"{silver_mnt}/silver_applicant_detail" 
)
def silver_applicant_detail():
    appeals_df = dlt.read("bronze_appealcase_ca_apt_country_detc").alias("ca")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

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
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

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
# MAGIC ### Tarnsformation : silver_list_detail

# COMMAND ----------

@dlt.table(
    name="silver_list_detail",
    comment="Delta Live silver Table for list detail.",
    path=f"{silver_mnt}/silver_list_detail"
)
def silver_list_detail():
    appeals_df = dlt.read("bronze_appealcase_cl_ht_list_lt_hc_c_ls_adj").alias("ca")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

    joined_df = appeals_df.join(flt_df, col("ca.CaseNo") == col("flt.CaseNo"), "inner")\
                          .withColumn("TimeEstimate_hh_mm", 
                                      expr("floor(TimeEstimate / 60) || ':' || lpad(cast(TimeEstimate % 60 as string), 2, '0')"))\
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
                              "ca.ListStartTime",
                              "ca.ListTypeDesc",
                              "ca.ListType",
                              "ca.DoNotUseListType",
                              "ca.CourtName",
                              "ca.DoNotUseCourt",
                              "ca.HearingCentreDesc",
                              "ca.Chairman",
                              "ca.Position"
                          )

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
    comment="Delta Live silver Table for history detail.",
    path=f"{silver_mnt}/silver_history_detail"
)
def silver_history_detail():
    appeals_df = dlt.read("bronze_appealcase_history_users").alias("hu")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

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
# MAGIC ### Tarnsformation : silver_status_detail

# COMMAND ----------

# query = "SELECT * FROM hive_metastore.ariadm_arm_appeals.silver_status_detail WHERE CaseStatus = 35"
# result_df = spark.sql(query)
# display(result_df)

# COMMAND ----------

@dlt.table(
    name="silver_status_detail",
    comment="Delta Live silver Table for status detail.",
    path=f"{silver_mnt}/silver_status_detail"
)
def silver_status_detail():
    appeals_df = dlt.read("bronze_appealcase_status_sc_ra_cs").alias("st")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

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
                              "st.Allegation",
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
                              "st.Process",
                              "st.COAReferenceNumber",
                              "st.HighCourtReference",
                              "st.OutOfTime",
                              when(col("st.ReconsiderationHearing") == True, "checked").otherwise("disabled").alias("ReconsiderationHearing"),
                              when(col("st.DecisionSentToHO") == 1, "YES").otherwise('NO').alias("DecisionSentToHO"),
                              "st.DecisionSentToHODate",
                              when(col("st.MethodOfTyping") == 1, "IA Typed")
                              .when(col("st.MethodOfTyping") == 2, "Self Type")
                              .when(col("st.MethodOfTyping") == 3, "3rd Party")
                              .alias("MethodOfTyping"),
                              "st.CourtSelection",
                              "st.DecidingCentre",
                              "st.Tier",
                              "st.RemittalOutcome",
                              "st.UpperTribunalAppellant",
                              "st.ListRequirementTypeId",
                              "st.UpperTribunalHearingDirectionId",
                              "st.ApplicationType",
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
                              "st.ExtemporeMethodOfTyping",
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

@dlt.table(
    name="silver_case_detail",
    comment="Delta Live silver Table for case detail.", 
    path=f"{silver_mnt}/silver_case_detail"
)
def silver_case_detail():
    case_df = dlt.read("bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at").alias("case")
    flt_df = dlt.read("stg_appeals_filtered").alias('flt')

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
# MAGIC ### Tarnsformation : silver_reviewspecificdirection_detail

# COMMAND ----------

@dlt.table(
    name="silver_reviewspecificdirection_detail",
    comment="Delta Live silver Table for review-specific direction details.",
    path=f"{silver_mnt}/Silver_reviewspecificdirection_detail"
)
def Silver_reviewspecificdirection_detail():
    review_specific_direction_df = dlt.read("bronze_review_specific_direction").alias("rsd")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = review_specific_direction_df.join(flt_df, col("rsd.CaseNo") == col("flt.CaseNo"), "inner").select("rsd.*")
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_costaward_detail

# COMMAND ----------

# DBTITLE 1,silver_linkedcostaward_detail
@dlt.table(
    name="silver_linkedcostaward_detail",
    comment="Delta Live silver Table for cost award detail.",
    path=f"{silver_mnt}/silver_linkedcostaward_detail"
)
def silver_linkedcostaward_detail():
    costaward_df = dlt.read("bronze_cost_award_linked").alias("ca")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

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
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

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
# MAGIC ### Tarnsformation : silver_costorder_detail 

# COMMAND ----------

@dlt.table(
    name="silver_costorder_detail",
    comment="Delta Live silver Table for cost order detail.",
    path=f"{silver_mnt}/silver_costorder_detail"
)
def silver_costorder_detail():
    costorder_df = dlt.read("bronze_costorder").alias("co")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")



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
# MAGIC ### Tarnsformation : silver_hearingpointschange_detail

# COMMAND ----------

@dlt.table(
    name="silver_hearingpointschange_detail",
    comment="Delta Live silver Table for hearing points change reason detail.",
    path=f"{silver_mnt}/silver_hearingpointschange_detail"
)
def silver_hearingpointschange_detail():
    hearingpointschange_df = dlt.read("bronze_hearing_points_change_reason").alias("hpc")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

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
# MAGIC ### Tarnsformation :silver_hearingpointshistory_detail

# COMMAND ----------

@dlt.table(
    name="silver_hearingpointshistory_detail",
    comment="Delta Live silver Table for hearing points history detail.",
    path=f"{silver_mnt}/silver_hearing_points_history_detail"
)
def silver_hearing_points_history_detail():
    hearingpointshistory_df = dlt.read("bronze_hearing_points_history").alias("hph")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = hearingpointshistory_df.join(flt_df, col("hph.CaseNo") == col("flt.CaseNo"), "inner").select("hph.*")
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation : silver_appealtypecategory_detail

# COMMAND ----------

@dlt.table(
    name="silver_appealtypecategory_detail",
    comment="Delta Live silver Table for appeal type category detail.",
    path=f"{silver_mnt}/silver_appealtypecategory_detail"
)
def silver_appealtypecategory_detail():
    appealtypecategory_df = dlt.read("bronze_appeal_type_category").alias("atc")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = appealtypecategory_df.join(flt_df, col("atc.CaseNo") == col("flt.CaseNo"), "inner").select("atc.*")
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation: silver_appealgrounds_detail

# COMMAND ----------

@dlt.table(
    name="silver_appealgrounds_detail",
    comment="Delta Live silver Table for appeal ground  detail.",
    path=f"{silver_mnt}/silver_appeal_grounds_detail"
)
def silver_appeal_grounds_detail():
    appealtypecategory_df = dlt.read("bronze_appeal_grounds").alias("agt")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = appealtypecategory_df.join(flt_df, col("agt.CaseNo") == col("flt.CaseNo"), "inner").select("agt.*")
    return joined_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation: silver_required_incompatible_adjudicator

# COMMAND ----------

@dlt.table(
    name="silver_required_incompatible_adjudicator",
    comment="Delta Live silver Table for appeal ground  detail.",
    path=f"{silver_mnt}/silver_required_incompatible_adjudicator"
)
def silver_required_incompatible_adjudicator():
    appealtypecategory_df = dlt.read("bronze_required_incompatible_adjudicator").alias("adj")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = appealtypecategory_df.join(flt_df, col("adj.CaseNo") == col("flt.CaseNo"), "inner").select("adj.*")
    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tarnsformation: silver_case_adjudicator

# COMMAND ----------

@dlt.table(
    name="silver_case_adjudicator",
    comment="Delta Live silver Table for appeal ground  detail.",
    path=f"{silver_mnt}/silver_case_adjudicator"
)
def silver_case_adjudicator():
    appealtypecategory_df = dlt.read("bronze_case_adjudicator").alias("adj")
    flt_df = dlt.read("stg_appeals_filtered").alias("flt")

    joined_df = appealtypecategory_df.join(flt_df, col("adj.CaseNo") == col("flt.CaseNo"), "inner").select("adj.*")
    return joined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_archive_metadata
# MAGIC
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
            .join(dlt.read("silver_applicant_detail").alias('ca'), col("ac.CaseNo") == col("ca.CaseNo"), "inner")\
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
secret = dbutils.secrets.get(key_vault, "curated-connection-string-sbox")

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

# DBTITLE 1,Function: Generate a360 Metadata
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
                "entitlement_tag": row.entitlement_tag,
                "bf_001": row.bf_001 or "",
                "bf_002": row.bf_002 or "",
                "bf_003": row.bf_003 or "",
                "bf_004": str(row.bf_004) or "",
                "bf_005": row.bf_005 or "",
                "bf_006": row.bf_006 or ""
            }
        }

        html_data = {
            "operation": "upload_new_file",
            "relation_id": row.client_identifier,
            "file_metadata": {
                "publisher": row.publisher,
                "dz_file_name": f"appeals_{row.client_identifier.replace('/', '_')}.html",
                "file_tag": "html"
            }
        }

        json_data = {
            "operation": "upload_new_file",
            "relation_id": row.client_identifier,
            "file_metadata": {
                "publisher": row.publisher,
                "dz_file_name": f"appeals_{row.client_identifier.replace('/', '_')}.json",
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
        return f"Error generating A360 for client_identifier {row.client_identifier}: {e}"

# Register UDF
generate_a360_udf = udf(generate_a360, StringType())

# COMMAND ----------

# DBTITLE 1,Function: Format Dates for UploadBlob Storage
    
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
    (f"{html_mnt}/appeals/appeals-no-js-v5-template.html", "html_template"),
    (f"{html_mnt}/appeals/dependentsdetails/Dependentsdetailstemplate.html", "Dependentsdetailstemplate"),
    (f"{html_mnt}/appeals/paymentdetails/PaymentDetailstemplate.html", "PaymentDetailstemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailPreliminaryIssueTemplate.html", "StatusDetailPreliminaryIssueTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailCaseManagementReviewTemplate.html", "StatusDetailCaseManagementReviewTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailAppellateCourtTemplate.html", "StatusDetailAppellateCourtTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailHighCourtReviewTemplate.html", "StatusDetailHighCourtReviewTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailHighCourtReviewFilterTemplate.html", "StatusDetailHighCourtReviewFilterTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailImmigrationJudgeHearingTemplate.html", "StatusDetailImmigrationJudgeHearingTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailPanelHearingLegalTemplate.html", "StatusDetailPanelHearingLegalTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailPermissiontoAppealTemplate.html", "StatusDetailPermissiontoAppealTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailReviewofCostOrderTemplate.html", "StatusDetailReviewOfCostOrderTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailFirstTierHearingTemplate.html", "StatusDetailFirstTierHearingTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailFirstTierPaperTemplate.html", "StatusDetailFirstTierPaperTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailFirstTierPermissionApplicationTemplate.html", "StatusDetailFirstTierPermissionApplicationTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailUpperTribunalPermissionApplicationTemplate.html", "StatusDetailUpperTribunalPermissionApplicationTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailUpperTribunalOralPermissionApplicationTemplate.html", "StatusDetailUpperTribunalOralPermissionApplicationTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailUpperTribunalHearingTemplate.html", "StatusDetailUpperTribunalHearingTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailUpperTribunalHearingContinuanceTemplate.html", "StatusDetailUpperTribunalHearingContinuanceTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailUpperTribunalOralPermissionHearingTemplate.html", "StatusDetailUpperTribunalOralPermissionHearingTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailPTADirecttoAppellateCourtTemplate.html", "StatusDetailPTADirecttoAppellateCourtTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailSetAsideApplicationTemplate.html", "StatusDetailSetAsideApplicationTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailJudicialReviewPermissionApplicationTemplate.html", "StatusDetailJudicialReviewPermissionApplicationTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailJudicialReviewHearingTemplate.html", "StatusDetailJudicialReviewHearingTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailOnHoldChargebackTakenTemplate.html", "StatusDetailOnHoldChargebackTakenTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailClosedFeeNotPaidTemplate.html", "StatusDetailClosedFeeNotPaidTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailCaseClosedFeeOutstandingTemplate.html", "StatusDetailCaseClosedFeeOutstandingTemplate"),
    (f"{html_mnt}/appeals/statusdetail/StatusDetailMigrationTemplate.html", "StatusDetailMigrationTemplate"),
    (f"{html_mnt}/appeals/statusdetail/DefaultStatusDetail.html", "DefaultStatusDetail")  

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
            "AppellantBirthDate", "dateCorrectFeeReceived", "DateCorrectFeeDeemedReceived"
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
                for i, status in enumerate(row.TempCaseStatusDetails or [])
            ),
            "{{HistoryPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{format_date(history.HistDate)}</td><td id=\"midpadding\">{history.HistTypeDescription}</td><td id=\"midpadding\">{history.UserName}</td><td id=\"midpadding\">{history.HistoryComment}</td></tr>"
                for i, history in enumerate(row.HistoryDetails or [])
            ),
            "{{bfdiaryPlaceHolder}}": "\n".join(
                f"<tr><td id=\"midpadding\">{format_date(bfdiary.EntryDate)}</td><td id=\"midpadding\">{bfdiary.BFTypeDescription}</td><td id=\"midpadding\">{bfdiary.Entry}</td><td id=\"midpadding\">{format_date(bfdiary.DateCompleted)}</td></tr>"
                for i, bfdiary in enumerate(row.BFDairyDetails or [])
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
            for index, SDP in enumerate(row.TempCaseStatusDetails, start=1):
                
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
                                        .replace("{{MiscDate2}}", format_date_iso(SDP.MiscDate2 or '')) \
                                        .replace("{{VideoLink}}", str(SDP.VideoLink or '')) \
                                        .replace("{{RemittalOutcome}}", str(SDP.RemittalOutcome or '')) \
                                        .replace("{{UpperTribunalAppellant}}", str(SDP.UpperTribunalAppellant or '')) \
                                        .replace("{{DecisionSentToHO}}", str(SDP.DecisionSentToHO or '')) \
                                        .replace("{{DecisionSentToHODate}}", format_date_iso(SDP.DecisionSentToHODate or '')) \
                                        .replace("{{InitialHearingPoints}}", format_date_iso(SDP.InitialHearingPoints or '')) \
                                        .replace("{{FinalHearingPoints}}", format_date_iso(SDP.FinalHearingPoints or '')) \
                                        .replace("{{HearingPointsChangeReasondesc}}", str(SDP.HearingPointsChangeReasondesc or '')) \
                                        .replace("{{CostOrderAppliedFor}}", str(SDP.CostOrderAppliedFor or '')) \
                                        .replace("{{HearingPointsChangeReasonId}}", format_date_iso(SDP.HearingPointsChangeReasonId or '')) \
                                        .replace("{{DecisionDate}}", format_date_iso(SDP.DecisionDate or '')) \
                                        .replace("{{DecisionByTCW}}", str(SDP.DecisionByTCW or '')) \
                                        .replace("{{Allegation}}", str(SDP.Allegation or '')) \
                                        .replace("{{DecidingCentre}}", format_date_iso(SDP.DecidingCentre or '')) \
                                        .replace("{{Process}}", format_date_iso(SDP.Process or '')) \
                                        .replace("{{Tier}}", format_date_iso(SDP.Tier or '')) \
                                        .replace("{{NoCertAwardDate}}", format_date_iso(SDP.NoCertAwardDate or '')) \
                                        .replace("{{WrittenOffDate}}", format_date_iso(SDP.WrittenOffDate or '')) \
                                        .replace("{{WrittenOffFileDate}}", format_date_iso(SDP.WrittenOffFileDate or '')) \
                                        .replace("{{ReferredEnforceDate}}", format_date_iso(SDP.ReferredEnforceDate or '')) \
                                        .replace("{{DeterminationByJudgeSurname}}", format_date_iso(SDP.DeterminationByJudgeSurname or '')) \
                                        .replace("{{DeterminationByJudgeForenames}}", format_date_iso(SDP.DeterminationByJudgeForenames or '')) \
                                        .replace("{{DeterminationByJudgeTitle}}", format_date_iso(SDP.DeterminationByJudgeTitle or '')) \
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
                                        .replace("{{adjournDateReceived}}", format_date_iso(SDP.adjournDateReceived or '')) \
                                        .replace("{{adjournmiscdate2}}", format_date_iso(SDP.adjournmiscdate2 or '')) \
                                        .replace("{{adjournParty}}", str(SDP.adjournParty or '')) \
                                        .replace("{{adjournInTime}}", str(SDP.adjournInTime or '')) \
                                        .replace("{{adjournLetter1Date}}", format_date_iso(SDP.adjournLetter1Date or '')) \
                                        .replace("{{adjournLetter2Date}}", format_date_iso(SDP.adjournLetter2Date or '')) \
                                        .replace("{{adjournAdjudicatorSurname}}", str(SDP.adjournAdjudicatorSurname or '')) \
                                        .replace("{{adjournAdjudicatorForenames}}", str(SDP.adjournAdjudicatorForenames or '')) \
                                        .replace("{{adjournAdjudicatorTitle}}", str(SDP.adjournAdjudicatorTitle or '')) \
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
                                        .replace("{{RequiredIncompatiblejudicialofficersPlaceHolder}}", str("\n".join(
                                                f"<tr><td id=\"midpadding\">{judge.JudgeSurname}, {judge.JudgeForenames} {judge.JudgeTitle}</td><td id=\"midpadding\" style=\"text-align:center\">{'✓' if judge.Required else ''}</td></tr>"
                                                for i, judge in enumerate(SDP.CaseAdjudicatorsDetails or [])
                                            ) or '<tr><td id="midpadding"></td><td id="midpadding"></td></tr>')) \
                                        .replace("{{SpecificdirectionsPlaceHolder}}", str("\n".join(
                                                f"<tr><td id=\"midpadding\">{rspecd.reviewspecificdirection}</td><td id=\"midpadding\">{rspecd.DateRequiredIND}</td><td id=\"midpadding\">{rspecd.DateRequiredAppellantRep}</td><td id=\"midpadding\">{rspecd.DateReceivedIND}</td><td id=\"midpadding\">{rspecd.DateReceivedAppellantRep}</td></tr>"
                                                for i, rspecd in enumerate(SDP.ReviewSpecficDirectionDetails or [])
                                            ) or '<tr><td id="midpadding"></td><td id="midpadding"></td></tr>')) \
                                        .replace("{{StandarddirectionsPlacHolder}}", str("\n".join(
                                                f"<tr><td id=\"midpadding\">{rstd.ReviewStandardDirectionId}</td><td id=\"midpadding\">{format_date(rstd.DateRequiredIND)}</td><td id=\"midpadding\">{format_date(rstd.DateRequiredAppellantRep)}</td><td id=\"midpadding\">{format_date(rstd.DateReceivedIND)}</td><td id=\"midpadding\">{format_date(rstd.DateReceivedAppellantRep)}</td></tr>"
                                                for i, rstd in enumerate(SDP.ReviewStandardDirectionDirectionDetails or [])
                                            ) or '<tr><td id="midpadding"></td><td id="midpadding"></td></tr>')) \
                                        .replace("{{AssignedjudicialofficersPlaceHolder}}", str("\n".join(
                                                f"<tr><td id=\"midpadding\">{adjd.JudgeFT}</td><td id=\"midpadding\">{adjd.CourtClerkUsher}</td><td id=\"midpadding\"></td><td id=\"midpadding\"></td></tr>"
                                                for i, adjd in enumerate(SDP.CaseStatusAdjudicatorDetails or [])
                                            ) or '<tr><td id="midpadding"></td><td id="midpadding"></td><td id="midpadding"></td><td id="midpadding"></td></tr>'))        
                                                                                

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
    (10, "Preliminary Issue", "StatusDetailPreliminaryIssueTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailPreliminaryIssueTemplate.html"),
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
    (26, "Case Management Review", "StatusDetailCaseManagementReviewTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailCaseManagementReviewTemplate.html"),
    (27, "Court of Appeal", "StatusDetailAppellateCourtTemplate",f"{html_mnt}/appeals/statusdetail/StatusDetailAppellateCourtTemplate.html"),
    (28, "High Court Review", "StatusDetailHighCourtReviewTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailHighCourtReviewTemplate.html"),
    (29, "High Court Review (Filter)", "StatusDetailHighCourtReviewFilterTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailHighCourtReviewFilterTemplate.html"),
    (30, "Immigration Judge – Hearing", "StatusDetailImmigrationJudgeHearingTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailImmigrationJudgeHearingTemplate.html"),
    (31, "Immigration Judge – Paper", "StatusDetailImmigrationJudgeHearingTemplate",f"{html_mnt}/appeals/statusdetail/StatusDetailImmigrationJudgeHearingTemplate.html"),
    (32, "Panel Hearing (Legal)", "StatusDetailPanelHearingLegalTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailPanelHearingLegalTemplate.html"),
    (33, "Panel Hearing (Legal/Non Legal)", "StatusDetailPanelHearingLegalTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailPanelHearingLegalTemplate.html"),
    (34, "Permission to Appeal", "StatusDetailPermissiontoAppealTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailPermissiontoAppealTemplate.html"),
    (35, "Migration", "StatusDetailMigrationTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailMigrationTemplate.html"),
    (36, "Review of Cost Order", "StatusDetailReviewOfCostOrderTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailReviewofCostOrderTemplate.html"),
    (37, "First Tier – Hearing", "StatusDetailFirstTierHearingTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailFirstTierHearingTemplate.html"),
    (38, "First Tier – Paper", "StatusDetailFirstTierPaperTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailFirstTierPaperTemplate.html"),
    (39, "First Tier Permission Application", "StatusDetailFirstTierPermissionApplicationTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailFirstTierPermissionApplicationTemplate.html"),
    (40, "Upper Tribunal Permission Application", "StatusDetailUpperTribunalPermissionApplicationTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailUpperTribunalPermissionApplicationTemplate.html"),
    (41, "Upper Tribunal Oral Permission Application", "StatusDetailUpperTribunalOralPermissionApplicationTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailUpperTribunalOralPermissionApplicationTemplate.html"),
    (42, "Upper Tribunal Hearing", "StatusDetailUpperTribunalHearingTemplate",f"{html_mnt}/appeals/statusdetail/StatusDetailUpperTribunalHearingTemplate.html"),
    (43, "Upper Tribunal Hearing – Continuance", "StatusDetailUpperTribunalHearingContinuanceTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailUpperTribunalHearingContinuanceTemplate.html"),
    (44, "Upper Tribunal Oral Permission Hearing", "StatusDetailUpperTribunalOralPermissionHearingTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailUpperTribunalOralPermissionHearingTemplate.html"),
    (45, "PTA Direct to Appellate Court", "StatusDetailPTADirecttoAppellateCourtTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailPTADirecttoAppellateCourtTemplate.html"),
    (46, "Set Aside Application", "StatusDetailSetAsideApplicationTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailSetAsideApplicationTemplate.html"),
    (47, "Judicial Review Permission Application", "StatusDetailJudicialReviewPermissionApplicationTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailJudicialReviewPermissionApplicationTemplate.html"),
    (48, "Judicial Review Hearing", "StatusDetailJudicialReviewHearingTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailJudicialReviewHearingTemplate.html"),
    (49, "Judicial Review Oral Permission Hearing", "StatusDetailJudicialReviewHearingTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailJudicialReviewHearingTemplate.html"),
    (50, "On Hold – Chargeback Taken", "StatusDetailOnHoldChargebackTakenTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailOnHoldChargebackTakenTemplate.html"),
    (51, "Closed – Fee Not Paid", "StatusDetailClosedFeeNotPaidTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailClosedFeeNotPaidTemplate.html"),
    (52, "Case closed fee outstanding", "StatusDetailCaseClosedFeeOutstandingTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailCaseClosedFeeOutstandingTemplate.html"),
    (53, "Upper Trib Case On Hold – Fee Not Paid", "StatusDetailOnHoldChargebackTakenTemplate", f"{html_mnt}/appeals/statusdetail/StatusDetailOnHoldChargebackTakenTemplate.html"),
    (54, "Ork", None, None)
]



columns = ["id", "description", "HTMLName", "path"]
lookup_df = spark.createDataFrame(data, columns).filter(col("path").isNotNull())
casestatus_array = lookup_df.select(col("id")).distinct().rdd.flatMap(lambda x: x).collect()
lookup_list = lookup_df.collect()
# display(lookup_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Create/Update to blob  storage

# COMMAND ----------

# DBTITLE 1,Transformation: stg_statichtml_data
@dlt.table(
    name="stg_statichtml_data",
    comment="Delta Live Silver Table for Archive Metadata data.",
    path=f"{gold_mnt}/stg_statichtml_data"
)
def stg_statichtml_data():
    # df_transaction_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_transaction_detail")
    # df_history_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_history_detail")
    # df_status_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_status_detail")
    # df_link_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_link_detail")

    df_transaction_details = dlt.read("silver_transaction_detail")
    df_history_details =  dlt.read("silver_history_detail")
    df_status_details =  dlt.read("silver_status_detail")
    df_link_details =  dlt.read("silver_link_detail")

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
    df_stg_static_html_data = df_transaction_details_derived.join(df_latest_history_details, "CaseNo", "left") \
                                                            .join(df_latest_status_details, "CaseNo", "left") \
                                                            .join(df_link_details_derived, "CaseNo", "left")

    # display(df_stg_static_html_data)

    return df_stg_static_html_data

# COMMAND ----------

# DBTITLE 1,Transformation: stg_statusdetail_data
@dlt.table(
    name="stg_statusdetail_data",
    comment="Delta Live Silver Table for Archive Metadata data.",
    path=f"{gold_mnt}/stg_statusdetail_data"
)
def stg_statusdetail_data():
    # df_list_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_list_detail")
    # df_status_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_status_detail")
    # df_hearingpointschange_details = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_hearingpointschange_detail")

    df_list_details = dlt.read("silver_list_detail")
    df_status_details = dlt.read("silver_status_detail")
    df_hearingpointschange_details = dlt.read("silver_hearingpointschange_detail")

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
    adjourned_withdrawal_df = df_status_details.filter(
        col("StatusId").isin(
            df_status_details.filter(col("CaseStatus") == 17)
            .select("AdjournmentParentStatusId")
            .rdd.flatMap(lambda x: x)
            .collect()
        )
    ).select("*")

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
        "HearingPointsChangeReasondesc") \
        .join(adjourned_withdrawal_df.alias("adj"), 
            
            ((col("status.StatusId") == col("adj.StatusId"))
            & (col("status.CaseNo") == col("adj.CaseNo"))
            & (col("status.CaseStatus") == col("adj.CaseStatus"))),
            
            "left") \
        .withColumn("adjourned_withdrawal_enabled", when(col("adj.StatusId").isNotNull(), lit(True)).otherwise(lit(False))) \
        .withColumn("adjournDecisionTypeDescription",  when(col("adj.StatusId").isNotNull(),col("adj.DecisionTypeDescription")).otherwise(lit(None))) \
        .withColumn("adjournDateReceived", when(col("adj.StatusId").isNotNull(),col("adj.DateReceived")).otherwise(lit(None))) \
        .withColumn("adjournmiscdate1", when(col("adj.StatusId").isNotNull(),col("adj.miscdate1")).otherwise(lit(None))) \
        .withColumn("adjournmiscdate2", when(col("adj.StatusId").isNotNull(),col("adj.miscdate2")).otherwise(lit(None))) \
        .withColumn("adjournParty", when(col("adj.StatusId").isNotNull(),col("adj.Party")).otherwise(lit(None))) \
        .withColumn("adjournInTime", when(col("adj.StatusId").isNotNull(),col("adj.InTime")).otherwise(lit(None))) \
        .withColumn("adjournLetter1Date", when(col("adj.StatusId").isNotNull(),col("adj.Letter1Date")).otherwise(lit(None))) \
        .withColumn("adjournLetter2Date", when(col("adj.StatusId").isNotNull(),col("adj.Letter2Date")).otherwise(lit(None))) \
        .withColumn("adjournAdjudicatorSurname", when(col("adj.StatusId").isNotNull(),col("adj.StatusDetailAdjudicatorSurname")).otherwise(lit(None))) \
        .withColumn("adjournAdjudicatorForenames", when(col("adj.StatusId").isNotNull(),col("adj.StatusDetailAdjudicatorForenames")).otherwise(lit(None))) \
        .withColumn("adjournAdjudicatorTitle", when(col("adj.StatusId").isNotNull(),col("adj.StatusDetailAdjudicatorTitle")).otherwise(lit(None))) \
        .withColumn("adjournNotes1", when(col("adj.StatusId").isNotNull(),col("adj.Notes1")).otherwise(lit(None))) \
        .withColumn("adjournDecisionDate", when(col("adj.StatusId").isNotNull(),col("adj.DecisionDate")).otherwise(lit(None))) \
        .withColumn("adjournDecisionTypeDescription", when(col("adj.StatusId").isNotNull(),col("adj.DecisionTypeDescription")).otherwise(lit(None))) \
        .withColumn("adjournPromulgated", when(col("adj.StatusId").isNotNull(),col("adj.Promulgated")).otherwise(lit(None))) \
        .withColumn("adjournUKAITNo", when(col("adj.StatusId").isNotNull(),col("adj.UKAITNo")).otherwise(lit(None))) \
        .withColumn("AdjudicatorSurname", when(col("status.KeyDate").isNull(), col("status.StatusDetailAdjudicatorSurname")).otherwise(col("list.ListAdjudicatorSurname"))) \
        .withColumn("AdjudicatorForenames", when(col("status.KeyDate").isNull(), col("status.StatusDetailAdjudicatorForenames")).otherwise(col("list.ListAdjudicatorForenames"))) \
        .withColumn("AdjudicatorTitle", when(col("status.KeyDate").isNull(), col("status.StatusDetailAdjudicatorTitle")).otherwise(col("list.ListAdjudicatorTitle"))) \
        .withColumn("AdjudicatorId", when(col("status.KeyDate").isNull(), col("status.StatusDetailAdjudicatorId")).otherwise(col("list.ListAdjudicatorId"))) \
        .withColumn("AdjudicatorNote", when(col("status.KeyDate").isNull(), col("status.StatusDetailAdjudicatorNote")).otherwise(col("list.ListAdjudicatorNote")))

    # Filter out only CaseStatus that are relevant for appeals
    join_df = status_refined_df.filter((col("status.CaseStatus").cast("integer")).isin(casestatus_array)) \
        .join(df_case_adjudicator.alias('cadj'), 'CaseNo', 'left') \
        .join(df_reviewspecificdirection.alias('rsd'), 'CaseNo', 'left') \
        .join(df_reviewstandarddirection.alias('rsdd'), 'CaseNo', 'left') \
        .withColumn("JudgeFT", when(col("Position") != 3, concat(col("AdjudicatorSurname"), lit(", "), col("AdjudicatorForenames"), lit(" ("), col("AdjudicatorTitle"), lit(")"))).otherwise(lit(None))) \
        .withColumn("CourtClerkUsher", when(col("Position") == 3, concat(col("AdjudicatorSurname"), lit(", "), col("AdjudicatorForenames"), lit(" ("), col("AdjudicatorTitle"), lit(")"))).otherwise(lit(None)))
        

    # df_agg01 = join_df.groupBy("status.CaseNo", "status.CaseStatus", "status.StatusId").agg(
    #     collect_list(struct("AdjudicatorSurname", "AdjudicatorForenames", "AdjudicatorTitle", 'status.KeyDate', 'AdjudicatorId'
    #     )).alias("CaseStatusAdjudicatorDetails")
    # )

    df_agg01 = join_df.groupBy("status.CaseNo", "status.CaseStatus", "status.StatusId").agg(
        collect_list(struct("AdjudicatorSurname", "AdjudicatorForenames", "AdjudicatorTitle", "status.KeyDate", "AdjudicatorId","JudgeFT","CourtClerkUsher","Position")).alias("CaseStatusAdjudicatorDetails"),
        max("status.KeyDate").alias("LatestKeyDate"),
        max_by("AdjudicatorSurname", "status.KeyDate").alias("LatestAdjudicatorSurname"),
        max_by("AdjudicatorForenames", "status.KeyDate").alias("LatestAdjudicatorForenames"),
        max_by("AdjudicatorTitle", "status.KeyDate").alias("LatestAdjudicatorTitle"),
        max_by("AdjudicatorId", "status.KeyDate").alias("LatestAdjudicatorId")
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
            'adjournAdjudicatorSurname', 'adjournAdjudicatorForenames', 'adjournAdjudicatorTitle', 'adjournNotes1', 
            'adjournDecisionDate', 'adjournPromulgated', 'HearingCentreDesc', 'CourtName', 'ListName', 'ListTypeDesc', 
            'HearingTypeDesc', 'ListStartTime', 'StartTime', 'TimeEstimate',  'status.LanguageDescription','cadj.CaseAdjudicatorsDetails','rsd.ReviewSpecficDirectionDetails','rsdd.ReviewStandardDirectionDirectionDetails').distinct()


        
    df_final = df_agg2.alias("casestatus").join(df_agg01.alias("adjj"), ((col("casestatus.StatusId") == col("adjj.StatusId"))
            & (col("casestatus.CaseNo") == col("adjj.CaseNo"))
            & (col("casestatus.CaseStatus") == col("adjj.CaseStatus"))), 'left')\
                .join(lookup_df.alias("lookup"), col("casestatus.CaseStatus") == col("lookup.id")) \
            .groupBy("casestatus.CaseNo").agg(collect_list(struct( "casestatus.CaseStatus", "casestatus.StatusId", "CaseStatusAdjudicatorDetails",'casestatus.CaseStatusDescription',  'casestatus.InterpreterRequired',  'casestatus.MiscDate2', 'casestatus.VideoLink', 'casestatus.RemittalOutcome', 'casestatus.UpperTribunalAppellant', 'casestatus.DecisionSentToHO', 
            'casestatus.InitialHearingPoints', 'casestatus.FinalHearingPoints', 'HearingPointsChangeReasondesc', 'casestatus.CostOrderAppliedFor', 'casestatus.DecisionDate', 
            'casestatus.DeterminationByJudgeSurname', 'casestatus.DeterminationByJudgeForenames', 'casestatus.DeterminationByJudgeTitle', 'casestatus.MethodOfTyping', 
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
            'adjournAdjudicatorSurname', 'adjournAdjudicatorForenames', 'adjournAdjudicatorTitle', 'adjournNotes1', 
            'adjournDecisionDate', 'adjournPromulgated', 'HearingCentreDesc', 'CourtName', 'ListName', 'ListTypeDesc', 
            'HearingTypeDesc', 'ListStartTime', 'StartTime', 'TimeEstimate',  'casestatus.LanguageDescription','casestatus.CaseAdjudicatorsDetails','casestatus.ReviewSpecficDirectionDetails','casestatus.ReviewStandardDirectionDirectionDetails','lookup.HTMLName','LatestKeyDate','LatestAdjudicatorSurname','LatestAdjudicatorForenames','LatestAdjudicatorId','LatestAdjudicatorTitle')).alias("TempCaseStatusDetails"))
    return df_final

# COMMAND ----------

gold_outputs

# COMMAND ----------

# DBTITLE 1,Transformation: stg_appeals_unified
@dlt.table(
    name="stg_appeals_unified",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{gold_mnt}/stg_appeals_unified"
)
def stg_appeals_unified():

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
            struct( 'CategoryDescription', 'Flag')
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
        collect_list(struct('HistoryId', 'CaseNo', 'HistDate', 'fileLocation', 'lastDocument', 'HistType', 'HistoryComment', 'StatusId', 'UserName', 'UserType', 'Fullname', 'Extension', 'DoNotUse', 'HistTypeDescription')).alias("HistoryDetails")
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
        collect_list(struct('Outcome', 'CaseStatus', 'StatusId', 'TimeEstimate', 'ListNumber', 'HearingDuration', 'StartTime', 'HearingTypeDesc', 'HearingTypeEst', 'DoNotUse', 'ListAdjudicatorId', 'ListAdjudicatorSurname', 'ListAdjudicatorForenames', 'ListAdjudicatorNote', 'ListAdjudicatorTitle', 'ListName', 'ListStartTime', 'ListTypeDesc', 'ListType', 'DoNotUseListType', 'CourtName', 'DoNotUseCourt', 'HearingCentreDesc')).alias("ListDetails")
    )

    df_dfdairy = dlt.read("silver_dfdairy_detail").groupBy("CaseNo").agg(
        collect_list(struct('CaseNo', 'Entry', 'EntryDate', 'DateCompleted', 'Reason', 'BFTypeDescription', 'DoNotUse')).alias("BFDairyDetails")
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

   
    # # HTML extra requirement- status details data
    # df_with_statusdetail_data = df_combined.join(spark.read.table("hive_metastore.ariadm_arm_appeals.stg_statusdetail_data"), "CaseNo", "left").join(spark.read.table("hive_metastore.ariadm_arm_appeals.stg_statichtml_data"), "CaseNo", "left")

    #HTML extra requirement- status details data
    df_with_statusdetail_data = df_combined.join(dlt.read("stg_statusdetail_data"), "CaseNo", "left").join(dlt.read("stg_statichtml_data"), "CaseNo", "left")

    # #HTML extra requirement- status details data
    # df_with_statusdetail_data = df_combined.join(dlt.read("stg_statusdetail_data"), "CaseNo", "left").join(dlt.read("stg_statichtml_data"), "CaseNo", "left")

    df_with_html_json_content = df_with_statusdetail_data.withColumn("HTMLContent", generate_html_udf(struct(*df_with_statusdetail_data.columns))) \
        .withColumn("JSONcollection", to_json(struct(*df_combined.columns)))

    #A360 extra requirement for batch and metadata
    # Read the metadata table from the Hive metastore
    # df_joh_metadata = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_archive_metadata")
    df_joh_metadata = dlt.read("silver_archive_metadata")


    # Select distinct client identifiers with HTML and JOSN content and order them
    metadata_df = df_joh_metadata.alias('a').join(df_with_html_json_content.alias('b'), col('b.CaseNo') == col('a.client_identifier'), 'left').filter((~col("HTMLContent").like("Error%")) & (~col("JSONcollection").like("Error%"))).select("client_identifier").distinct().orderBy("client_identifier")

    # Define a window specification to assign row numbers
    window_spec = Window.orderBy("client_identifier")
    df_batch = metadata_df.withColumn("row_num", row_number().over(window_spec)) \
                        .withColumn("A360BatchId", floor((col("row_num") - 1) / 250) + 1)

    # Join the batch information with the original metadata
    df_metadata = df_joh_metadata.join(df_batch, "client_identifier", "left")

    # Repartition the DataFrame to optimize parallelism
    # repartitioned_df = df_metadata.repartition(64, col("client_identifier"))


    # Generate A360 content and associated file names
    df_with_a360 = df_metadata.withColumn(
        "A360Content", generate_a360_udf(struct(*df_joh_metadata.columns))
    ).withColumn(
        "A360FileName", when(col("A360BatchId").isNotNull(), concat(lit(f"{gold_outputs}/A360/appeals_"), col("A360BatchId"), lit(".a360"))).otherwise(lit(None))
    ).withColumn(
        "UploadStatus", upload_udf(col("A360FileName"), col("A360Content"))
    ).select(col("client_identifier").alias("CaseNo"), "A360BatchId", "A360FileName", "A360Content")



    df_with_statusdetail_a360_data =  df_with_html_json_content.join(df_with_a360, "CaseNo", "left")

    df_unified = df_with_statusdetail_a360_data.withColumn(
        "A360FileName", when(col("A360BatchId").isNotNull(), concat(lit(f"{gold_outputs}/A360/appeals_"), col("A360BatchId"), lit(".a360"))).otherwise(lit(None))
    ).withColumn(
        "HTMLFileName", concat(lit(f"{gold_outputs}/HTML/appeals_"), regexp_replace(col("CaseNo"), "/", "_"), lit(".html"))
    ).withColumn(
        "JSONFileName", concat(lit(f"{gold_outputs}/JSON/appeals_"), regexp_replace(col("CaseNo"), "/", "_"), lit(".json"))
    )

    return df_unified

# COMMAND ----------

# DBTITLE 1,Transformation gold_appeals_with_json
@dlt.table(
    name="gold_appeals_with_json",
    comment="Delta Live Gold Table with JSON content.",
    path=f"{gold_mnt}/gold_appeals_with_json"
)
def gold_appeals_with_json():
    """
    Delta Live Table for creating and uploading JSON content for Appeals.
    """
    # Load source data
    df_unified = dlt.read("stg_appeals_unified")
    

    # Optionally load data from Hive if needed
    if read_hive:
        df_unified = spark.read.table(f"hive_metastore.{hive_schema}.stg_appeals_unified")

    # Repartition to optimize parallelism
    repartitioned_df = df_unified.repartition(64)

    df_with_upload_status = repartitioned_df.filter(~col("JSONcollection").like("Error%")).withColumn(
            "UploadStatus", upload_udf(col("JSONFileName"), col("JSONcollection"))
        )
    
    # Optionally load data from Hive
    if read_hive:
        display(df_with_upload_status.select("CaseNo","A360BatchId", "JSONcollection","JSONFileName","UploadStatus"))

    # Return the DataFrame for DLT table creation
    return df_with_upload_status.select("CaseNo","A360BatchId", "JSONcollection","JSONFileName","UploadStatus")


# COMMAND ----------

# DBTITLE 1,Transformation gold_appeals_with_html
checks = {}
checks["html_content_no_error"] = "(HTMLContent NOT LIKE 'Error%')"

@dlt.table(
    name="gold_appeals_with_html",
    comment="Delta Live Gold Table with HTML content and uploads.",
    path=f"{gold_mnt}/gold_appeals_with_html"
)
@dlt.expect_all_or_fail(checks)
def gold_appeals_with_html():
    # Load source data
    df_combined = dlt.read("stg_appeals_unified")

    # Optional: Load from Hive if not an initial load
    if read_hive:
        df_combined = spark.read.table(f"hive_metastore.{hive_schema}.stg_appeals_unified")

    # Repartition to optimize parallelism
    repartitioned_df = df_combined.repartition(64)

    # Trigger upload logic for each row
    df_with_upload_status = repartitioned_df.filter(~col("HTMLContent").like("Error%")).withColumn(
        "UploadStatus", upload_udf(col("HTMLFileName"), col("HTMLContent"))
    )

    # Optionally load data from Hive
    if read_hive:
        display(df_with_upload_status.select("CaseNo","A360BatchId", "HTMLContent", "HTMLFileName", "UploadStatus"))


    # Return the DataFrame for DLT table creation, including the upload status
    return df_with_upload_status.select("CaseNo","A360BatchId", "HTMLContent", "HTMLFileName", "UploadStatus")

# COMMAND ----------

# DBTITLE 1,Transformation gold_appeals_with_a360
checks = {}
checks["A360Content_no_error"] = "(consolidate_A360Content NOT LIKE 'Error%')"

@dlt.table(
    name="gold_appeals_with_a360",
    comment="Delta Live Gold Table with A360 content.",
    path=f"{gold_mnt}/gold_appeals_with_a360"
)
@dlt.expect_all_or_fail(checks)
def gold_appeals_with_a360():
    df_a360 = dlt.read("stg_appeals_unified")

    # Optionally load data from Hive
    if read_hive:
        df_a360 = spark.read.table(f"hive_metastore.{hive_schema}.stg_appeals_unified")

    # Group by 'A360FileName' with Batching and consolidate the 'sets' texts, separated by newline
    df_agg = df_a360.groupBy("A360FileName", "A360BatchId") \
            .agg(concat_ws("\n", collect_list("A360Content")).alias("consolidate_A360Content")) \
            .select(col("A360FileName"), col("consolidate_A360Content"), col("A360BatchId"))

    # Repartition the DataFrame to optimize parallelism
    repartitioned_df = df_agg.repartition(64)

    # Remove existing files
    dbutils.fs.rm(f"{gold_outputs}/A360", True)

    # Generate A360 content
    df_with_a360 = repartitioned_df.withColumn(
        "UploadStatus", upload_udf(col("A360FileName"), col("consolidate_A360Content"))
    )

    # Optionally load data from Hive
    if read_hive:
        display(df_with_a360)
   
    return df_with_a360.select("A360BatchId", "consolidate_A360Content", "A360FileName", "UploadStatus")

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Success Message
dbutils.notebook.exit("Notebook completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix

# COMMAND ----------

# DBTITLE 1,Check Error Records in Appeals Data
# df_unified = spark.read.table("hive_metastore.ariadm_arm_appeals.stg_appeals_unified")
# display(df_unified.filter(col("A360FileName").isNull() | 
#                           col("A360Content").like("Error%") | 
#                           col("HTMLContent").like("Error%") | 
#                           col("JSONcollection").like("Error%"))
#        .select("CaseNo", "A360FileName", "A360Content", "HTMLFileName", "HTMLContent", "JSONFileName", "JSONcollection"))

# COMMAND ----------

# DBTITLE 1,Check for Creation errors
# df_json = spark.read.table("hive_metastore.ariadm_arm_appeals.gold_appeals_with_json").alias("json")
# df_html = spark.read.table("hive_metastore.ariadm_arm_appeals.gold_appeals_with_html").alias("html")
# df_a360 = spark.read.table("hive_metastore.ariadm_arm_appeals.gold_appeals_with_a360").alias("a360")

# df_joined = df_json.join(df_html, "CaseNo", "inner").join(df_a360, df_a360.A360BatchId == df_json.A360BatchId, "inner")
# df_filtered = df_joined.filter((col("json.uploadstatus") != "success") | 
#                                 (col("html.uploadstatus") != "success") | 
#                                 (col("a360.uploadstatus") != "success"))
# display(df_filtered)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from hive_metastore.ariadm_arm_appeals.gold_appeals_with_a360

# COMMAND ----------

# DBTITLE 1,debug: statusdetail_data

# # Read unique CaseNo tables
# # M1
# df_appealcase = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealcase_detail")
# # M2
# df_applicant = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_applicant_detail")
# # M10
# df_case_detail = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_case_detail")
# #M22
# df_hearingpointschange = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_hearingpointschange_detail")


# # Read duplicate CaseNo tables and aggregate them
# df_appealcategory = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealcategory_detail").groupBy("CaseNo").agg(
#     collect_list(
#         struct( 'CategoryDescription', 'Flag')
#     ).alias("AppealCategoryDetails")
# )

# df_appealgrounds = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealgrounds_detail").groupBy("CaseNo").agg(
#     collect_list(struct( 'AppealTypeId', 'AppealTypeDescription')).alias("AppealGroundsDetails")
# )

# df_appealtypecategory = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealtypecategory_detail").groupBy("CaseNo").agg(
#     collect_list(struct('AppealTypeCategoryId', 'AppealTypeId', 'CategoryId', 'FeeExempt')).alias("AppealTypeCategories")
# )

# df_case_adjudicator = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_case_adjudicator").groupBy("CaseNo").agg(
#     collect_list(struct( 'Required', 'JudgeSurname', 'JudgeForenames', 'JudgeTitle')).alias("CaseAdjudicators")
# )

# df_costaward = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_costaward_detail").groupBy("CaseNo").agg(
#     collect_list(struct('CostAwardId', 'Name', 'Forenames', 'Title', 'DateOfApplication', 'TypeOfCostAward', 'ApplyingParty', 'PayingParty', 'MindedToAward', 'ObjectionToMindedToAward', 'CostsAwardDecision', 'DateOfDecision', 'CostsAmount', 'OutcomeOfAppeal', 'AppealStage', 'AppealStageDescription')).alias("CostAwardDetails")
# )

# df_dependent = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_dependent_detail").groupBy("CaseNo").agg(
#     collect_list(struct('AppellantId', 'CaseAppellantRelationship', 'PortReference', 'AppellantName', 'AppellantForenames', 'AppellantTitle', 'AppellantBirthDate', 'AppellantAddress1', 'AppellantAddress2', 'AppellantAddress3', 'AppellantAddress4', 'AppellantAddress5', 'AppellantPostcode', 'AppellantTelephone', 'AppellantFax', 'Detained', 'AppellantEmail', 'FCONumber', 'PrisonRef', 'DetentionCentre', 'CentreTitle', 'DetentionCentreType', 'DCAddress1', 'DCAddress2', 'DCAddress3', 'DCAddress4', 'DCAddress5', 'DCPostcode', 'DCFax', 'DCSdx', 'Country', 'DependentNationality', 'Code', 'DoNotUseCountry', 'CountrySdx', 'DoNotUseNationality')).alias("DependentDetails")
# )

# df_documents = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_documents_detail").groupBy("CaseNo").agg(
#     collect_list(struct( 'ReceivedDocumentId', 'DateRequested', 'DateRequired', 'DateReceived', 'NoLongerRequired', 'RepresentativeDate', 'POUDate', 'DocumentDescription', 'DoNotUse', 'Auditable')).alias("DocumentDetails")
# )

# df_hearingpointshistory = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_hearingpointshistory_detail").groupBy("CaseNo").agg(
#     collect_list(struct('CaseNo', 'StatusId', 'HearingPointsHistoryId', 'HistDate', 'HistType', 'UserId', 'DefaultPoints', 'InitialPoints', 'FinalPoints')).alias("HearingPointHistory")
# )

# df_history = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_history_detail").groupBy("CaseNo").agg(
#     collect_list(struct('HistoryId', 'CaseNo', 'HistDate', 'fileLocation', 'lastDocument', 'HistType', 'HistoryComment', 'StatusId', 'UserName', 'UserType', 'Fullname', 'Extension', 'DoNotUse', 'HistTypeDescription')).alias("HistoryDetails")
# )

# df_humanright = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_humanright_detail").groupBy("CaseNo").agg(
#     collect_list(struct('HumanRightId', 'HumanRightDescription', 'DoNotShow', 'Priority')).alias("HumanRightDetails")
# )

# df_newmatter = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_newmatter_detail").groupBy("CaseNo").agg(
#     collect_list(struct(
#         'AppealNewMatterId', 'NewMatterId', 'AppealNewMatterNotes', 'DateReceived', 
#         'DateReferredToHO', 'HODecision', 'DateHODecision', 'NewMatterDescription', 
#         'NotesRequired', 'DoNotUse'
#     )).alias("NewMatterDetails")
# )

# df_link = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_link_detail").groupBy("CaseNo").agg(
#     collect_list(struct( 'LinkNo', 'LinkDetailComment', 'LinkName', 'LinkForeNames', 'LinkTitle')).alias("LinkedCaseDetails")
# )

# df_linkedcostaward = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_linkedcostaward_detail").groupBy("CaseNo").agg(
#     collect_list(struct('CostAwardId', 'CaseNo', 'LinkNo', 'Name', 'Forenames', 'Title', 'DateOfApplication', 'TypeOfCostAward', 'ApplyingParty', 'PayingParty', 'MindedToAward', 'ObjectionToMindedToAward', 'CostsAwardDecision', 'DateOfDecision', 'CostsAmount', 'OutcomeOfAppeal', 'AppealStage', 'AppealStageDescription')).alias("LinkedCostAwardDetails")
# )

# df_costorder = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_costorder_detail").groupBy("CaseNo").agg(
#     collect_list(
#         struct('CostOrderID', 'DateOfApplication', 'OutcomeOfAppealWhereDecisionMade', 
#             'DateOfDecision', 'ApplyingRepresentativeId', 'ApplyingRepresentativeName', 
#             'OutcomeOfAppealWhereDecisionMadeDescription', 'AppealStageWhenApplicationMade', 
#             'AppealStageWhenDecisionMade', 'CostOrderDecision')
#     ).alias("CostOrderDetails")
# )


# df_list_detail = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_list_detail").groupBy("CaseNo").agg(
#     collect_list(struct('Outcome', 'CaseStatus', 'StatusId', 'TimeEstimate', 'ListNumber', 'HearingDuration', 'StartTime', 'HearingTypeDesc', 'HearingTypeEst', 'DoNotUse', 'ListAdjudicatorId', 'ListAdjudicatorSurname', 'ListAdjudicatorForenames', 'ListAdjudicatorNote', 'ListAdjudicatorTitle', 'ListName', 'ListStartTime', 'ListTypeDesc', 'ListType', 'DoNotUseListType', 'CourtName', 'DoNotUseCourt', 'HearingCentreDesc')).alias("Lists")
# )

# df_dfdairy = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_dfdairy_detail").groupBy("CaseNo").agg(
#     collect_list(struct('CaseNo', 'Entry', 'EntryDate', 'DateCompleted', 'Reason', 'BFTypeDescription', 'DoNotUse')).alias("BFDairyDetails")
# )

# df_required_incompatible_adjudicator = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_required_incompatible_adjudicator").groupBy("CaseNo").agg(
#     collect_list(struct('Required', 'JudgeSurname', 'JudgeForenames', 'JudgeTitle')).alias("RequiredIncompatibleAdjudicators")
# )

# df_status = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_status_detail").groupBy("CaseNo").agg(
#     collect_list(struct('StatusId', 'CaseNo', 'CaseStatus', 'DateReceived', 'StatusDetailAdjudicatorId', 'Allegation', 'KeyDate', 'MiscDate1', 'Notes1', 'Party', 'InTime', 'MiscDate2', 'MiscDate3', 'Notes2', 'DecisionDate', 'Outcome', 'Promulgated', 'InterpreterRequired', 'AdminCourtReference', 'UKAITNo', 'FC', 'VideoLink', 'Process', 'COAReferenceNumber', 'HighCourtReference', 'OutOfTime', 'ReconsiderationHearing', 'DecisionSentToHO', 'DecisionSentToHODate', 'MethodOfTyping', 'CourtSelection', 'DecidingCentre', 'Tier', 'RemittalOutcome', 'UpperTribunalAppellant', 'ListRequirementTypeId', 'UpperTribunalHearingDirectionId', 'ApplicationType', 'NoCertAwardDate', 'CertRevokedDate', 'WrittenOffFileDate', 'ReferredEnforceDate', 'Letter1Date', 'Letter2Date', 'Letter3Date', 'ReferredFinanceDate', 'WrittenOffDate', 'CourtActionAuthDate', 'BalancePaidDate', 'WrittenReasonsRequestedDate', 'TypistSentDate', 'TypistReceivedDate', 'WrittenReasonsSentDate', 'ExtemporeMethodOfTyping', 'Extempore', 'DecisionByTCW', 'InitialHearingPoints', 'FinalHearingPoints', 'HearingPointsChangeReasonId', 'OtherCondition', 'OutcomeReasons', 'AdditionalLanguageId', 'CostOrderAppliedFor', 'HearingCourt', 'CaseStatusDescription', 'DoNotUseCaseStatus', 'CaseStatusHearingPoints', 'ContactStatus', 'SCCourtName', 'SCAddress1', 'SCAddress2', 'SCAddress3', 'SCAddress4', 'SCAddress5', 'SCPostcode', 'SCTelephone', 'SCForenames', 'SCTitle', 'ReasonAdjourn', 'DoNotUseReason', 'LanguageDescription', 'DoNotUseLanguage', 'DecisionTypeDescription', 'DeterminationRequired', 'DoNotUse', 'State', 'BailRefusal', 'BailHOConsent', 'StatusDetailAdjudicatorSurname', 'StatusDetailAdjudicatorForenames', 'StatusDetailAdjudicatorTitle', 'StatusDetailAdjudicatorNote', 'DeterminationByJudgeSurname', 'DeterminationByJudgeForenames', 'DeterminationByJudgeTitle', 'CurrentStatus', 'AdjournmentParentStatusId')).alias("StatusDetails")
# )

# df_statusdecisiontype = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_statusdecisiontype_detail").groupBy("CaseNo").agg(
#     collect_list(struct('DecisionTypeDescription', 'DeterminationRequired', 'DoNotUse', 'State', 'BailRefusal', 'BailHOConsent')).alias("StatusDecisionTypes")
# )

# df_transaction = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_transaction_detail").groupBy("CaseNo").agg(
#     collect_list(struct('TransactionId', 'TransactionTypeId', 'TransactionMethodId', 'TransactionDate', 'Amount', 'ClearedDate', 'TransactionStatusId', 'OriginalPaymentReference', 'PaymentReference', 'AggregatedPaymentURN', 'PayerForename', 'PayerSurname', 'LiberataNotifiedDate', 'LiberataNotifiedAggregatedPaymentDate', 'BarclaycardTransactionId', 'Last4DigitsCard', 'TransactionNotes', 'ExpectedDate', 'ReferringTransactionId', 'CreateUserId', 'LastEditUserId', 'TransactionDescription', 'InterfaceDescription', 'AllowIfNew', 'DoNotUse', 'SumFeeAdjustment', 'SumPayAdjustment', 'SumTotalFee', 'SumTotalPay', 'SumBalance', 'GridFeeColumn', 'GridPayColumn', 'IsReversal', 'TransactionStatusDesc', 'TransactionStatusIntDesc', 'DoNotUseTransactionStatus', 'TransactionMethodDesc', 'TransactionMethodIntDesc', 'DoNotUseTransactionMethod', 'AmountDue', 'AmountPaid', 'FirstTierFee', 'TotalFeeAdjustments', 'TotalFeeDue', 'TotalPaymentsReceived', 'TotalPaymentAdjustments', 'BalanceDue')).alias("TransactionDetails")
# )

# df_standarddirection = spark.read.table("hive_metastore.ariadm_arm_appeals.sliver_direction_detail").groupBy("CaseNo").agg(
#     collect_list(struct(
#         'ReviewStandardDirectionId', 'CaseNo', 'StatusId', 'StandardDirectionId', 
#         'DateRequiredIND', 'DateRequiredAppellantRep', 'DateReceivedIND', 
#         'DateReceivedAppellantRep', 'Description', 'DoNotUse'
#     )).alias("StandardDirectionDetails")
# )

# df_reviewspecificdirection = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_reviewspecificdirection_detail").groupBy("CaseNo").agg(
#     collect_list(struct(
#         'ReviewSpecificDirectionId', 'CaseNo', 'StatusId', 'SpecificDirection', 
#         'DateRequiredIND', 'DateRequiredAppellantRep', 'DateReceivedIND', 'DateReceivedAppellantRep'
#     )).alias("ReviewSpecficDirection")
# )



# # Join all tables
# df_combined = (
#     df_appealcase
#     .join(df_applicant, "CaseNo", "left")
#     .join(df_dependent, "CaseNo", "left")
#     .join(df_list_detail, "CaseNo", "left")
#     .join(df_dfdairy, "CaseNo", "left")
#     .join(df_history, "CaseNo", "left")
#     .join(df_link, "CaseNo", "left")
#     .join(df_status, "CaseNo", "left")
#     .join(df_appealcategory, "CaseNo", "left")
#     .join(df_case_detail, "CaseNo", "left")
#     .join(df_transaction, "CaseNo", "left")
#     .join(df_humanright, "CaseNo", "left")
#     .join(df_newmatter, "CaseNo", "left")
#     .join(df_documents, "CaseNo", "left")
#     .join(df_standarddirection, "CaseNo", "left")
#     .join(df_reviewspecificdirection, "CaseNo", "left")
#     .join(df_costaward, "CaseNo", "left")
#     .join(df_linkedcostaward, "CaseNo", "left")
#     .join(df_costorder, "CaseNo", "left")
#     .join(df_hearingpointschange, "CaseNo", "left")
#     .join(df_hearingpointshistory, "CaseNo", "left")
#     .join(df_appealtypecategory, "CaseNo", "left")
#     .join(df_appealgrounds, "CaseNo", "left")
#     .join(df_required_incompatible_adjudicator, "CaseNo", "left")
#     .join(df_case_adjudicator, "CaseNo", "left")
#     .join(df_statusdecisiontype, "CaseNo", "left")
#    # .join(df_stg_static_html_data, "CaseNo", "left")
    
# )

# #status details data
# df_with_statusdetail_data = df_combined.join(spark.read.table("hive_metastore.ariadm_arm_appeals.stg_statusdetail_data"), "CaseNo", "left").join(spark.read.table("hive_metastore.ariadm_arm_appeals.stg_statichtml_data"), "CaseNo", "left")


# display(df_with_statusdetail_data)


# COMMAND ----------

# DBTITLE 1,Debug: outputs
# df_unified = df_with_statusdetail_data.withColumn("JSONcollection", to_json(struct(*df_combined.columns))) \
#                         .withColumn("HTMLFileName", concat(lit(f"{gold_outputs}/HTML/appeals_"), regexp_replace(col("CaseNo"), "/", "_"), lit(".html"))) \
#                         .withColumn("HTMLContent", generate_html_udf(struct(*df_with_statusdetail_data.columns))) \
#                         .withColumn("JSONFileName", concat(lit(f"{gold_outputs}/JSON/appeals_"), regexp_replace(col("CaseNo"), "/", "_"), lit(".json")))

# display(df_unified.filter(col("TempCaseStatusDetails").isNotNull()).select(size("TempCaseStatusDetails").alias("tcount"), "CaseNo", "HTMLFileName", "HTMLContent", "JSONFileName", "JSONcollection").filter(col("tcount") > 5))



# # Error generating HTML for CaseNo OC/00002/2014: name 'TransactionDetails' is not defined

# # # check for errors
# # display(df_unified.filter(~col("HTMLContent").like("<!DOCTYPE html>%")).select("CaseNo", "HTMLFileName", "HTMLContent", "JSONFileName", "JSONcollection"))

# # Error generating HTML for CaseNo NS/00003/2007: [ATTRIBUTE_NOT_SUPPORTED] Attribute `AppealCaseNote` is not supported.
# # Error generating HTML for CaseNo RD/00008/2007: [ATTRIBUTE_NOT_SUPPORTED] Attribute `ReviewStandardDirectionId` is not supported.

# COMMAND ----------

# DBTITLE 1,debug: error
# # # # display(df_unified.filter(col("CaseNo") == "OC/00013/2005").select("TempCaseStatusDetails","HTMLContent"))

# display(df_unified.filter(col("HTMLContent").contains("Error generating HTML")).select("TempCaseStatusDetails","HTMLContent"))

# # # # Error generating HTML for CaseNo HR/00028/2007: name 'rspecd' is not defined





# COMMAND ----------

# DBTITLE 1,debug: display outputs
# html_content_value = df_unified.filter(col("CaseNo") == "RD/00032/2005").select("HTMLContent").first()["HTMLContent"]
# displayHTML(html_content_value)

# # OC/00014/2005
# # OA/00004/2015

# COMMAND ----------

# case_no = 'VA/00010/2005' 
# case_no = 'VA/00010/2005'
# case_no = 'AA/00001/2014' 
# case_no = 'AA/00001/2023' # HistoryComment
# case_no = 'AA/00007/2014' # fileLocationNote
# case_no = 'AA/00001/2012' # payment details
case_no = 'IM/00023/2003' # dependents
# case_no = 'AA/00017/2011' # Linked Files
# case_no = 'TH/00137/2003' ## StatusDetailFirstTierHearingTemplate
# case_no = 'AA/00011/2012' # bf dairy
# case_no = 'XX/00004/2005' # cost order
# case_no = 'AA/00049/2014' # new matter, Appeal Categories, Human Rights, Representative Details
# case_no = 'OA/00018/2014' # Representative
# case_no = 'HR/00040/2008' # cost award
# case_no = 'AS/00006/2009' # documents
# case_no = 'AA/00047/2011' # payments
# case_no = 'VA/00043/2014'
# case_no = 'OA/00002/2012' # payments
# case_no = 'HR/00010/2008' # respondant
# case_no = 'NS/00003/2008' # Incamera
# case_no = 'AA/00038/2011' # CertOfFeeSatisfaction # currentstatus
# case_no = 'AA/00001/2017' # connectedFiles
# case_no = 'OC/00001/2019' #Trans. out of fast track (date) Removal date :	,Deportation date :	,Provisional Destruction :
                          # Appeal received by
# case_no = 'NS/00003/2008' # Incamera, Publically funded ,Out of time issue
# case_no = 'RD/00011/2007' # Secure Court Req, Publically funded, 
# case_no = 'AA/00001/2011' # parties
# case_no = 'AA/00001/2023' #Additional Grounds 
# case_no = 'TH/00010/2003' # Non-suspensive Certification
# case_no = 'HR/00040/2008'
# case_no = 'AA/00006/2012' # CaseStatus(multiple) 37
# case_no = 'TH/00137/2003' # CaseStatus 37
# case_no = 'OC/00015/2011'  # CaseStatus 37 with CaseStatus 17
# case_no = 'OC/00014/2005' # CaseStatus 10 and 30 multiple


# COMMAND ----------

# df = spark.sql("SELECT * FROM hive_metastore.ariadm_arm_appeals.gold_appeals_with_html")
# # display(df)
# display(df.filter(col("HTMLContent").contains("Error generating HTML")))

# COMMAND ----------

# DBTITLE 1,Display HTML Content
# # case_no = 'IM/00048/2003'
# df = spark.sql("SELECT * FROM hive_metastore.ariadm_arm_appeals.gold_appeals_with_html")

# display(df)
# # Filter for the specific case and extract the JSON collection
# filtered_row = df.filter(col("CaseNo") == case_no).select("HTMLContent").first()

# displayHTML(filtered_row["HTMLContent"])

# COMMAND ----------

# %sql
# drop schema ariadm_arm_appeals cascade

# COMMAND ----------

# DBTITLE 1,Display JSON Content

# df = spark.sql("SELECT * FROM hive_metastore.ariadm_arm_appeals.gold_appeals_with_json")
# display(df)
# # Filter for the specific case and extract the JSON collection
# filtered_row = df.filter(col("CaseNo") == "OC/00014/2005").select("JSONCollection").first()

# json_data = filtered_row["JSONCollection"]

# formated_json = json.dumps(json.loads(json_data), indent=4)
    
# print(formated_json)



# COMMAND ----------

# DBTITLE 1,Display A360 Content

# df = spark.sql("SELECT * FROM hive_metastore.ariadm_arm_appeals.gold_appeals_with_a360")

# display(df)
# # Filter for the specific case and extract the JSON collection
# filtered_row = df.filter(col("A360BatchId") == 1).select("consolidate_A360Content").first()

# A360Content_data = filtered_row["consolidate_A360Content"]

# print("Number of lines " + str(len(A360Content_data.split('\n'))))
# print(A360Content_data)

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

# display(spark.read.format("binaryFile").load(f"{gold_mnt}/HTML").count())
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/JSON").count())
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/A360").count())

# COMMAND ----------

# # Read unique CaseNo tables
# # M1
# df_appealcase = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealcase_detail")
# # M2
# df_applicant = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_applicant_detail")
# # M10
# df_case_detail = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_case_detail")
# #M22
# # df_hearingpointschange = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_hearingpointschange_detail")

# df_hearingpointschange = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_hearingpointschange_detail").groupBy("CaseNo").agg(
# collect_list(
#     struct(
#         'StatusId',
#         'HearingPointsChangeReasonId',
#         'Description',
#         'HearingPointsChangeDoNotUse'
#     )
# ).alias("hearingpointschangedetail")
# )



# # Read duplicate CaseNo tables and aggregate them
# df_appealcategory = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealcategory_detail").groupBy("CaseNo").agg(
#     collect_list(
#         struct( 'CategoryDescription', 'Flag')
#     ).alias("AppealCategoryDetails")
# )

# df_appealgrounds = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealgrounds_detail").groupBy("CaseNo").agg(
#     collect_list(struct( 'AppealTypeId', 'AppealTypeDescription')).alias("AppealGroundsDetails")
# )

# df_appealtypecategory = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealtypecategory_detail").groupBy("CaseNo").agg(
#     collect_list(struct('AppealTypeCategoryId', 'AppealTypeId', 'CategoryId', 'FeeExempt')).alias("AppealTypeCategorieDetails")
# )

# df_case_adjudicator = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_case_adjudicator").groupBy("CaseNo").agg(
#     collect_list(struct( 'Required', 'JudgeSurname', 'JudgeForenames', 'JudgeTitle')).alias("CaseAdjudicatorDetails")
# )

# df_costaward = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_costaward_detail").groupBy("CaseNo").agg(
#     collect_list(struct('CostAwardId', 'Name', 'Forenames', 'Title', 'DateOfApplication', 'TypeOfCostAward', 'ApplyingParty', 'PayingParty', 'MindedToAward', 'ObjectionToMindedToAward', 'CostsAwardDecision', 'DateOfDecision', 'CostsAmount', 'OutcomeOfAppeal', 'AppealStage', 'AppealStageDescription')).alias("CostAwardDetails")
# )

# df_dependent = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_dependent_detail").groupBy("CaseNo").agg(
#     collect_list(struct('AppellantId', 'CaseAppellantRelationship', 'PortReference', 'AppellantName', 'AppellantForenames', 'AppellantTitle', 'AppellantBirthDate', 'AppellantAddress1', 'AppellantAddress2', 'AppellantAddress3', 'AppellantAddress4', 'AppellantAddress5', 'AppellantPostcode', 'AppellantTelephone', 'AppellantFax', 'Detained', 'AppellantEmail', 'FCONumber', 'PrisonRef', 'DetentionCentre', 'CentreTitle', 'DetentionCentreType', 'DCAddress1', 'DCAddress2', 'DCAddress3', 'DCAddress4', 'DCAddress5', 'DCPostcode', 'DCFax', 'DCSdx', 'Country', 'DependentNationality', 'Code', 'DoNotUseCountry', 'CountrySdx', 'DoNotUseNationality')).alias("DependentDetails")
# )

# df_documents = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_documents_detail").groupBy("CaseNo").agg(
#     collect_list(struct( 'ReceivedDocumentId', 'DateRequested', 'DateRequired', 'DateReceived', 'NoLongerRequired', 'RepresentativeDate', 'POUDate', 'DocumentDescription', 'DoNotUse', 'Auditable')).alias("DocumentDetails")
# )

# df_hearingpointshistory = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_hearingpointshistory_detail").groupBy("CaseNo").agg(
#     collect_list(struct('CaseNo', 'StatusId', 'HearingPointsHistoryId', 'HistDate', 'HistType', 'UserId', 'DefaultPoints', 'InitialPoints', 'FinalPoints')).alias("HearingPointHistoryDetails")
# )

# df_history = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_history_detail").groupBy("CaseNo").agg(
#     collect_list(struct('HistoryId', 'CaseNo', 'HistDate', 'fileLocation', 'lastDocument', 'HistType', 'HistoryComment', 'StatusId', 'UserName', 'UserType', 'Fullname', 'Extension', 'DoNotUse', 'HistTypeDescription')).alias("HistoryDetails")
# )

# df_humanright = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_humanright_detail").groupBy("CaseNo").agg(
#     collect_list(struct('HumanRightId', 'HumanRightDescription', 'DoNotShow', 'Priority')).alias("HumanRightDetails")
# )

# df_newmatter = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_newmatter_detail").groupBy("CaseNo").agg(
#     collect_list(struct(
#         'AppealNewMatterId', 'NewMatterId', 'AppealNewMatterNotes', 'DateReceived', 
#         'DateReferredToHO', 'HODecision', 'DateHODecision', 'NewMatterDescription', 
#         'NotesRequired', 'DoNotUse'
#     )).alias("NewMatterDetails")
# )

# df_link = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_link_detail").groupBy("CaseNo").agg(
#     collect_list(struct( 'LinkNo', 'LinkDetailComment', 'LinkName', 'LinkForeNames', 'LinkTitle')).alias("LinkedCaseDetails")
# )

# df_linkedcostaward = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_linkedcostaward_detail").groupBy("CaseNo").agg(
#     collect_list(struct('CostAwardId', 'CaseNo', 'LinkNo', 'Name', 'Forenames', 'Title', 'DateOfApplication', 'TypeOfCostAward', 'ApplyingParty', 'PayingParty', 'MindedToAward', 'ObjectionToMindedToAward', 'CostsAwardDecision', 'DateOfDecision', 'CostsAmount', 'OutcomeOfAppeal', 'AppealStage', 'AppealStageDescription')).alias("LinkedCostAwardDetails")
# )

# df_costorder = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_costorder_detail").groupBy("CaseNo").agg(
#     collect_list(
#         struct('CostOrderID', 'DateOfApplication', 'OutcomeOfAppealWhereDecisionMade', 
#             'DateOfDecision', 'ApplyingRepresentativeId', 'ApplyingRepresentativeName', 
#             'OutcomeOfAppealWhereDecisionMadeDescription', 'AppealStageWhenApplicationMade', 
#             'AppealStageWhenDecisionMade', 'CostOrderDecision')
#     ).alias("CostOrderDetails")
# )


# df_list_detail = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_list_detail").groupBy("CaseNo").agg(
#     collect_list(struct('Outcome', 'CaseStatus', 'StatusId', 'TimeEstimate', 'ListNumber', 'HearingDuration', 'StartTime', 'HearingTypeDesc', 'HearingTypeEst', 'DoNotUse', 'ListAdjudicatorId', 'ListAdjudicatorSurname', 'ListAdjudicatorForenames', 'ListAdjudicatorNote', 'ListAdjudicatorTitle', 'ListName', 'ListStartTime', 'ListTypeDesc', 'ListType', 'DoNotUseListType', 'CourtName', 'DoNotUseCourt', 'HearingCentreDesc')).alias("ListDetails")
# )

# df_dfdairy = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_dfdairy_detail").groupBy("CaseNo").agg(
#     collect_list(struct('CaseNo', 'Entry', 'EntryDate', 'DateCompleted', 'Reason', 'BFTypeDescription', 'DoNotUse')).alias("BFDairyDetails")
# )

# df_required_incompatible_adjudicator = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_required_incompatible_adjudicator").groupBy("CaseNo").agg(
#     collect_list(struct('Required', 'JudgeSurname', 'JudgeForenames', 'JudgeTitle')).alias("RequiredIncompatibleAdjudicatorDetails")
# )

# df_status = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_status_detail").groupBy("CaseNo").agg(
#     collect_list(struct('StatusId', 'CaseNo', 'CaseStatus', 'DateReceived', 'StatusDetailAdjudicatorId', 'Allegation', 'KeyDate', 'MiscDate1', 'Notes1', 'Party', 'InTime', 'MiscDate2', 'MiscDate3', 'Notes2', 'DecisionDate', 'Outcome', 'Promulgated', 'InterpreterRequired', 'AdminCourtReference', 'UKAITNo', 'FC', 'VideoLink', 'Process', 'COAReferenceNumber', 'HighCourtReference', 'OutOfTime', 'ReconsiderationHearing', 'DecisionSentToHO', 'DecisionSentToHODate', 'MethodOfTyping', 'CourtSelection', 'DecidingCentre', 'Tier', 'RemittalOutcome', 'UpperTribunalAppellant', 'ListRequirementTypeId', 'UpperTribunalHearingDirectionId', 'ApplicationType', 'NoCertAwardDate', 'CertRevokedDate', 'WrittenOffFileDate', 'ReferredEnforceDate', 'Letter1Date', 'Letter2Date', 'Letter3Date', 'ReferredFinanceDate', 'WrittenOffDate', 'CourtActionAuthDate', 'BalancePaidDate', 'WrittenReasonsRequestedDate', 'TypistSentDate', 'TypistReceivedDate', 'WrittenReasonsSentDate', 'ExtemporeMethodOfTyping', 'Extempore', 'DecisionByTCW', 'InitialHearingPoints', 'FinalHearingPoints', 'HearingPointsChangeReasonId', 'OtherCondition', 'OutcomeReasons', 'AdditionalLanguageId', 'CostOrderAppliedFor', 'HearingCourt', 'CaseStatusDescription', 'DoNotUseCaseStatus', 'CaseStatusHearingPoints', 'ContactStatus', 'SCCourtName', 'SCAddress1', 'SCAddress2', 'SCAddress3', 'SCAddress4', 'SCAddress5', 'SCPostcode', 'SCTelephone', 'SCForenames', 'SCTitle', 'ReasonAdjourn', 'DoNotUseReason', 'LanguageDescription', 'DoNotUseLanguage', 'DecisionTypeDescription', 'DeterminationRequired', 'DoNotUse', 'State', 'BailRefusal', 'BailHOConsent', 'StatusDetailAdjudicatorSurname', 'StatusDetailAdjudicatorForenames', 'StatusDetailAdjudicatorTitle', 'StatusDetailAdjudicatorNote', 'DeterminationByJudgeSurname', 'DeterminationByJudgeForenames', 'DeterminationByJudgeTitle', 'CurrentStatus', 'AdjournmentParentStatusId')).alias("StatusDetails")
# )

# df_statusdecisiontype = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_statusdecisiontype_detail").groupBy("CaseNo").agg(
#     collect_list(struct('DecisionTypeDescription', 'DeterminationRequired', 'DoNotUse', 'State', 'BailRefusal', 'BailHOConsent')).alias("StatusDecisionTypeDetails")
# )

# df_transaction = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_transaction_detail").groupBy("CaseNo").agg(
#     collect_list(struct('TransactionId', 'TransactionTypeId', 'TransactionMethodId', 'TransactionDate', 'Amount', 'ClearedDate', 'TransactionStatusId', 'OriginalPaymentReference', 'PaymentReference', 'AggregatedPaymentURN', 'PayerForename', 'PayerSurname', 'LiberataNotifiedDate', 'LiberataNotifiedAggregatedPaymentDate', 'BarclaycardTransactionId', 'Last4DigitsCard', 'TransactionNotes', 'ExpectedDate', 'ReferringTransactionId', 'CreateUserId', 'LastEditUserId', 'TransactionDescription', 'InterfaceDescription', 'AllowIfNew', 'DoNotUse', 'SumFeeAdjustment', 'SumPayAdjustment', 'SumTotalFee', 'SumTotalPay', 'SumBalance', 'GridFeeColumn', 'GridPayColumn', 'IsReversal', 'TransactionStatusDesc', 'TransactionStatusIntDesc', 'DoNotUseTransactionStatus', 'TransactionMethodDesc', 'TransactionMethodIntDesc', 'DoNotUseTransactionMethod', 'AmountDue', 'AmountPaid', 'FirstTierFee', 'TotalFeeAdjustments', 'TotalFeeDue', 'TotalPaymentsReceived', 'TotalPaymentAdjustments', 'BalanceDue')).alias("TransactionDetails")
# )

# df_standarddirection = spark.read.table("hive_metastore.ariadm_arm_appeals.sliver_direction_detail").groupBy("CaseNo").agg(
#     collect_list(struct(
#         'ReviewStandardDirectionId', 'CaseNo', 'StatusId', 'StandardDirectionId', 
#         'DateRequiredIND', 'DateRequiredAppellantRep', 'DateReceivedIND', 
#         'DateReceivedAppellantRep', 'Description', 'DoNotUse'
#     )).alias("StandardDirectionDetails")
# )

# df_reviewspecificdirection = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_reviewspecificdirection_detail").groupBy("CaseNo").agg(
#     collect_list(struct(
#         'ReviewSpecificDirectionId', 'CaseNo', 'StatusId', 'SpecificDirection', 
#         'DateRequiredIND', 'DateRequiredAppellantRep', 'DateReceivedIND', 'DateReceivedAppellantRep'
#     )).alias("ReviewSpecficDirectionDetails")
# )


# # Join all tables
# df_combined = (
#     df_appealcase
#     .join(df_applicant, "CaseNo", "left")
#     .join(df_dependent, "CaseNo", "left")
#     .join(df_list_detail, "CaseNo", "left")
#     .join(df_dfdairy, "CaseNo", "left")
#     .join(df_history, "CaseNo", "left")
#     .join(df_link, "CaseNo", "left")
#     .join(df_status, "CaseNo", "left")
#     .join(df_appealcategory, "CaseNo", "left")
#     .join(df_case_detail, "CaseNo", "left")
#     .join(df_transaction, "CaseNo", "left")
#     .join(df_humanright, "CaseNo", "left")
#     .join(df_newmatter, "CaseNo", "left")
#     .join(df_documents, "CaseNo", "left")
#     .join(df_standarddirection, "CaseNo", "left")
#     .join(df_reviewspecificdirection, "CaseNo", "left")
#     .join(df_costaward, "CaseNo", "left")
#     .join(df_linkedcostaward, "CaseNo", "left")
#     .join(df_costorder, "CaseNo", "left")
#     .join(df_hearingpointschange, "CaseNo", "left")
#     .join(df_hearingpointshistory, "CaseNo", "left")
#     .join(df_appealtypecategory, "CaseNo", "left")
#     .join(df_appealgrounds, "CaseNo", "left")
#     .join(df_required_incompatible_adjudicator, "CaseNo", "left")
#     .join(df_case_adjudicator, "CaseNo", "left")
#     .join(df_statusdecisiontype, "CaseNo", "left") 
    
# )

# # HTML extra requirement- status details data
# df_with_statusdetail_data = df_combined.join(spark.read.table("hive_metastore.ariadm_arm_appeals.stg_statusdetail_data"), "CaseNo", "left").join(spark.read.table("hive_metastore.ariadm_arm_appeals.stg_statichtml_data"), "CaseNo", "left")

# # #HTML extra requirement- status details data
# # df_with_statusdetail_data = df_combined.join(dlt.read("stg_statusdetail_data"), "CaseNo", "left").join(dlt.read("stg_statichtml_data"), "CaseNo", "left")

# df_with_html_json_content = df_with_statusdetail_data.withColumn("HTMLContent", generate_html_udf(struct(*df_with_statusdetail_data.columns))) \
#     .withColumn("JSONcollection", to_json(struct(*df_combined.columns)))

# #A360 extra requirement for batch and metadata
# # Read the metadata table from the Hive metastore
# df_joh_metadata = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_archive_metadata")

# # Select distinct client identifiers with HTML and JOSN content and order them
# metadata_df = df_joh_metadata.alias('a').join(df_with_html_json_content.alias('b'), col('b.CaseNo') == col('a.client_identifier'), 'left').filter((~col("HTMLContent").like("Error%")) & (~col("JSONcollection").like("Error%"))).select("client_identifier").distinct().orderBy("client_identifier")

# # Define a window specification to assign row numbers
# window_spec = Window.orderBy("client_identifier")
# df_batch = metadata_df.withColumn("row_num", row_number().over(window_spec)) \
#                     .withColumn("A360BatchId", floor((col("row_num") - 1) / 250) + 1)

# # Join the batch information with the original metadata
# df_metadata = df_joh_metadata.join(df_batch, "client_identifier", "left")

# # Repartition the DataFrame to optimize parallelism
# repartitioned_df = df_metadata.repartition(64, col("client_identifier"))


# # Generate A360 content and associated file names
# df_with_a360 = repartitioned_df.withColumn(
#     "A360Content", generate_a360_udf(struct(*df_joh_metadata.columns))
# ).withColumn(
#     "A360FileName", when(col("A360BatchId").isNotNull(), concat(lit(f"{gold_outputs}/A360/appeals_"), col("A360BatchId"), lit(".a360"))).otherwise(lit(None))
# ).withColumn(
#     "UploadStatus", upload_udf(col("A360FileName"), col("A360Content"))
# ).select(col("client_identifier").alias("CaseNo"), "A360BatchId", "A360FileName", "A360Content")



# df_with_statusdetail_a360_data =  df_with_html_json_content.join(df_with_a360, "CaseNo", "left")

# df_unified = df_with_statusdetail_a360_data.withColumn(
#     "A360FileName", when(col("A360BatchId").isNotNull(), concat(lit(f"{gold_outputs}/A360/appeals_"), col("A360BatchId"), lit(".a360"))).otherwise(lit(None))
# ).withColumn(
#     "HTMLFileName", concat(lit(f"{gold_outputs}/HTML/appeals_"), regexp_replace(col("CaseNo"), "/", "_"), lit(".html"))
# ).withColumn(
#     "JSONFileName", concat(lit(f"{gold_outputs}/JSON/appeals_"), regexp_replace(col("CaseNo"), "/", "_"), lit(".json"))
# )

# display(df_unified.select("CaseNo", "A360FileName","A360Content", "HTMLFileName","HTMLContent", "JSONFileName","JSONcollection"))

# COMMAND ----------

# display(df_unified.filter(col("A360FileName").isNull()).select("CaseNo", "A360FileName","A360Content", "HTMLFileName","HTMLContent", "JSONFileName","JSONcollection"))

# COMMAND ----------

# %sql
# select CaseNo, HTMLContent from hive_metastore.ariadm_arm_appeals.stg_appeals_unified
# where HTMLContent like 'Error%'
# -- Error generating HTML for CaseNo RD/00014/2006: [ATTRIBUTE_NOT_SUPPORTED] Attribute `IRISStatusOfCase` is not supported.

# COMMAND ----------

# %sql
# select distinct VisitVisaType, InCamera from hive_metastore.ariadm_arm_appeals.silver_appealcase_detail

# COMMAND ----------

# %sql
# select distinct Detained from hive_metastore.ariadm_arm_appeals.silver_applicant_detail

# COMMAND ----------

# @dlt.table(
# name="raw_feescale",
# comment="Delta Live Table ARIA AppealTypeCategory.",
# path=f"{raw_mnt}/raw_feescale"
# )
# def raw_feescale():
#     return read_latest_parquet("FeeScale", "tv_FeeScale", "ARIA_ARM_APPEALS") 


# @dlt.table(
# name="raw_feetype",
# comment="Delta Live Table ARIA AppealTypeCategory.",
# path=f"{raw_mnt}/raw_feetype"
# )
# def raw_feetype():
#     return read_latest_parquet("FeeType", "tv_FeeType", "ARIA_ARM_APPEALS") 

# # transactionmethod, transactionstatus, paymentremissionreason, transactiontype, feesatisfaction, feescale, feetype

# COMMAND ----------

# tableNames = [
#     "CaseFeeSummary",
#     "Transaction",
#     "TransactionMethod",
#     "PaymentRemissionReason",
#     "TransactionType",
#     "FeeSatisfaction",
#     "FeeScale",
#     "FeeType"
# ]

# for tableName in tableNames:
#     df = spark.sql(f"""select * from tv_{tableName}""")
#     df.write.mode("overwrite").format("csv").option("header", "true").save(f"/mnt/ingest00rawsboxraw/ARIADM/CSVEXPORT/{tableName}.csv")

# COMMAND ----------

# tableNames = [
#     "CaseFeeSummary",
#     "Transaction",
#     "TransactionMethod",
#     "PaymentRemissionReason",
#     "TransactionType",
#     "FeeSatisfaction",
#     "FeeScale",
#     "FeeType"
# ]

# output_path = "/mnt/ingest00rawsboxraw/ARIADM/CSVEXPORT/"

# for tableName in tableNames:
#     df = spark.sql(f"SELECT * FROM tv_{tableName}")
    
#     temp_path = f"{output_path}{tableName}_temp"
#     final_path = f"{output_path}{tableName}.csv"

#     # Write CSV as a single file
#     df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(temp_path)
    
#     # Move and rename the single CSV file
#     files = dbutils.fs.ls(temp_path)
#     csv_file = [f.path for f in files if f.path.endswith(".csv")][0]  # Get the CSV file
    
#     dbutils.fs.mv(csv_file, final_path, True)  # Move and rename
#     dbutils.fs.rm(temp_path, True)  # Clean up temp directory

