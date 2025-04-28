# Databricks notebook source
# # Use the dbutils.library.install function
# # Use pip to install the library
# %pip install /Workspace/Repos/ara.islam1@hmcts.net/ARIAMigration-Databrick/Databricks/SharedFunctionsLib/dist/ARIAFUNCITONS-0.0.1-py3-none-any.whl


# dbutils.library.restartPython()  # Restart the Python process to pick up the new library

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Bail Cases
# MAGIC
# MAGIC <table style = 'float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Name: </b></td>
# MAGIC          <td>ARIADM_ARM_Bails</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Description: </b></td>
# MAGIC          <td>Notebook to generate a set of HTML, JSON, and A360 files, for Bail caes.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>First Created: </b></td>
# MAGIC          <td>Sep-2024 </td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <th style='text-align: left; '><b>Changelog(JIRA ref/initials./date):</b></th>
# MAGIC          <th>Comments </th>
# MAGIC       </tr>
# MAGIC       </tr>
# MAGIC         <td style='text-align: left;'>
# MAGIC         </b>Create Bronze tables</b>
# MAGIC         </td>
# MAGIC         <td>
# MAGIC         Jira Ticket ARIADM-128</td>
# MAGIC         </td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC       <td>Create Silver tables
# MAGIC       <td> Ticket no
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC       <td>Create Gold Output
# MAGIC       <td> Ticket No
# MAGIC     
# MAGIC    </tbody>
# MAGIC </table>
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Import Packages

# COMMAND ----------


import dlt
import json
from pyspark.sql.functions import when, col,coalesce, current_timestamp, lit, date_format,desc, first,concat_ws,count,collect_list,struct,expr,concat,regexp_replace,trim,udf,row_number,floor,col,date_format,count,explode
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pyspark.sql import DataFrame
import logging
from pyspark.sql.window import Window


# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,  # Agree with Naveen DEBUG, WARNING, or ERROR
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()  # Outputs logs to the console
    ]
)

# Create a logger instance
logger = logging.getLogger(__name__)

# COMMAND ----------

def check_for_duplicates(df:DataFrame, col:str="CaseNo"):
    """
    Checks for duplicate records in the specified column of the DataFrame. Raises a ValueError if duplicates are found.

    :param df: The DataFrame to check, typically read from a Hive table.
    :param col: The column name to check for duplicates.
    :raises ValueError: If duplicates are found in the specified column.
    """
    duplicates_count = df.groupBy(F.col(col)).count().filter(F.col("count")>1).count()
    if duplicates_count > 0:
        raise ValueError(f"Duplicate records found for {col} in the dataset")

# COMMAND ----------

# Helper to format dates in ISO format (YYYY-MM-DD)
def format_date_iso(date_value):
    try:
        if isinstance(date_value, str):
            date_value = datetime.strptime(date_value, "%Y-%m-%d")
        return date_value.strftime("%Y-%m-%d")
    except Exception:
        return ""

# COMMAND ----------


# from pyspark.sql.functions import current_timestamp, lit

# Function to recursively list all files in the ADLS directory
def deep_ls(path: str, depth: int = 0, max_depth: int = 10) -> list:
    """
    Recursively list all files and directories in ADLS directory.
    Returns a list of all paths found.
    """
    output = set()  # Using a set to avoid duplicates
    if depth > max_depth:
        return output

    try:
        children = dbutils.fs.ls(path)
        for child in children:
            if child.path.endswith(".parquet"):
                output.add(child.path.strip())  # Add only .parquet files to the set

            if child.isDir:
                # Recursively explore directories
                output.update(deep_ls(child.path, depth=depth + 1, max_depth=max_depth))

    except Exception as e:
        print(f"Error accessing {path}: {e}")

    return list(output)  # Convert the set back to a list before returning

# Function to extract timestamp from the file path
def extract_timestamp(file_path):
    """
    Extracts timestamp from the parquet file name based on an assumed naming convention.
    """
    # Split the path and get the filename part
    filename = file_path.split('/')[-1]
    # Extract the timestamp part from the filename
    timestamp_str = filename.split('_')[-1].replace('.parquet', '')
    return timestamp_str

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
    
    # Ensure that files were found
    if not all_files:
        print(f"No .parquet files found in {folder_path}")
        return None
    
    # Find the latest .parquet file
    latest_file = max(all_files, key=extract_timestamp)
    
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

## Function to formate the date values
def format_date(date_value):
    if date_value:
        return datetime.strftime(date_value, "%Y-%m-%d")
    return ""  # Return empty string if date_value is None

# COMMAND ----------

# Setting variables for use in subsequent cells
raw_mnt = "/mnt/ingest00rawsboxraw/ARIADM/ARM/BAILS"
external_mnt = "/mnt/ingest00externalsboxexternal-csv"
landing_mnt = "/mnt/ingest00landingsboxlanding/"
bronze_mnt = "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/BAILS"
silver_mnt = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/BAILS"
gold_mnt = "/mnt/ingest00curatedsboxgold/ARIADM/ARM/BAILS"
gold_html_outputs = 'ARIADM/ARM/BAILS/HTML/'
gold_json_outputs = 'ARIADM/ARM/BAILS/JSON/'
gold_a360_outputs = 'ARIADM/ARM/BAILS/A360/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the Storage Client

# COMMAND ----------

secret = secret = dbutils.secrets.get("ingest00-keyvault-sbox", "curatedsbox-connection-string-sbox")
 
 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
 
# Set up the BlobServiceClient with your connection string
connection_string = f"BlobEndpoint=https://ingest00curatedsbox.blob.core.windows.net/;QueueEndpoint=https://ingest00curatedsbox.queue.core.windows.net/;FileEndpoint=https://ingest00curatedsbox.file.core.windows.net/;TableEndpoint=https://ingest00curatedsbox.table.core.windows.net/;SharedAccessSignature={secret}"
 
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
 
# Specify the container name
container_name = "gold"
container_client = blob_service_client.get_container_client(container_name)

audit_container_client = blob_service_client.get_container_client("silver")

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC # Code Architecture 

# COMMAND ----------

# MAGIC %md
# MAGIC ![ARCHIVE_code_archi.jpg](./ARCHIVE_code_archi.jpg "ARCHIVE_code_archi.jpg")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Creating temp views of the raw tables

# COMMAND ----------

# load in all the raw tables

@dlt.table(name="raw_appeal_cases", comment="Raw Appeal Cases",path=f"{raw_mnt}/raw_appeal_cases")
def bail_raw_appeal_cases():
    return read_latest_parquet("AppealCase","tv_AppealCase","ARIA_ARM_BAIL")

@dlt.table(name="raw_case_respondents", comment="Raw Case Respondents",path=f"{raw_mnt}/raw_case_respondents")
def bail_raw_case_respondents():
    return read_latest_parquet("CaseRespondent","tv_CaseRespondent","ARIA_ARM_BAIL")

@dlt.table(name="raw_respondent", comment="Raw Respondents",path=f"{raw_mnt}/raw_respondents")
def bail_raw_respondent():
    return read_latest_parquet("Respondent","tv_Respondent","ARIA_ARM_BAIL")

@dlt.table(name="raw_main_respondent", comment="Raw Main Respondent",path=f"{raw_mnt}/raw_main_respondent")
def bail_raw_main_respondent():
    return read_latest_parquet("MainRespondent","tv_MainRespondent","ARIA_ARM_BAIL")

@dlt.table(name="raw_pou", comment="Raw Pou",path=f"{raw_mnt}/raw_pou")
def bail_raw_pou():
    return read_latest_parquet("Pou","tv_Pou","ARIA_ARM_BAIL")

@dlt.table(name="raw_file_location", comment="Raw File Location",path=f"{raw_mnt}/raw_file_location")
def bail_raw_file_location():
    return read_latest_parquet("FileLocation","tv_FileLocation","ARIA_ARM_BAIL")

@dlt.table(name="raw_case_rep", comment="Raw Case Rep",path=f"{raw_mnt}/raw_case_rep")
def bail_raw_case_rep():
    return read_latest_parquet("CaseRep","tv_CaseRep","ARIA_ARM_BAIL")

@dlt.table(name="raw_representative", comment="Raw Representative",path=f"{raw_mnt}/raw_Representative")
def bail_raw_Representative():
    return read_latest_parquet("Representative","tv_Representative","ARIA_ARM_BAIL")

@dlt.table(name="raw_language", comment="Raw Language",path=f"{raw_mnt}/raw_language")
def bail_raw_language():
    return read_latest_parquet("Language","tv_Language","ARIA_ARM_BAIL")

@dlt.table(name="raw_cost_award", comment="Raw Cost Award",path=f"{raw_mnt}/raw_cost_award")
def bail_raw_cost_award():
    return read_latest_parquet("CostAward","tv_CostAward","ARIA_ARM_BAIL") 

@dlt.table(name='raw_case_list', comment='Raw Case List',path=f"{raw_mnt}/raw_case_list")
def bail_case_list():
    return read_latest_parquet("CaseList","tv_CaseList","ARIA_ARM_BAIL")

@dlt.table(name='raw_hearing_type', comment='Raw Hearing Type',path=f"{raw_mnt}/raw_hearing_type")
def bail_hearing_type():
    return read_latest_parquet("HearingType","tv_HearingType","ARIA_ARM_BAIL")

@dlt.table(name='raw_list',comment='Raw List',path=f"{raw_mnt}/raw_list")
def bail_list():
    return read_latest_parquet("List","tv_List","ARIA_ARM_BAIL")

@dlt.table(name='raw_list_type',comment='Raw List Type',path=f"{raw_mnt}/raw_list_type")
def bail_list_type():
    return read_latest_parquet("ListType","tv_ListType","ARIA_ARM_BAIL")

@dlt.table(name='raw_court',comment='Raw Bail Court',path=f"{raw_mnt}/raw_court")
def bail_court():
    return read_latest_parquet("Court","tv_Court","ARIA_ARM_BAIL")

@dlt.table(name='raw_hearing_centre',comment='Raw  Hearing Centre',path=f"{raw_mnt}/raw_hearing_centre")
def bail_hearing_centre():
    return read_latest_parquet("HearingCentre","tv_HearingCentre","ARIA_ARM_BAIL")

@dlt.table(name='raw_list_sitting',comment='Raw List Sitting',path=f"{raw_mnt}/raw_list_sitting")
def bail_list_sitting():
    return read_latest_parquet("ListSitting","tv_ListSitting","ARIA_ARM_BAIL")

@dlt.table(name='raw_adjudicator',comment='Raw Adjudicator',path=f"{raw_mnt}/raw_adjudicator")
def bail_adjudicator():
    return read_latest_parquet("Adjudicator","tv_Adjudicator","ARIA_ARM_BAIL")

@dlt.table(name='raw_appellant',comment='Raw Bail Appellant',path=f"{raw_mnt}/raw_appellant")
def bail_appellant():
    return read_latest_parquet("Appellant","tv_Appellant","ARIA_ARM_BAIL")

@dlt.table(name='raw_case_appellant',comment='Raw Bail Case Appellant',path=f"{raw_mnt}/raw_case_appellant")
def bail_case_appellant():
    return read_latest_parquet("CaseAppellant","tv_CaseAppellant","ARIA_ARM_BAIL")

@dlt.table(name='raw_detention_centre',comment='Raw Nail Detention Centre',path=f"{raw_mnt}/raw_detention_centre")
def bail_detention_centre():
    return read_latest_parquet("DetentionCentre","tv_DetentionCentre","ARIA_ARM_BAIL")

@dlt.table(name='raw_country',comment='Raw Bail Country',path=f"{raw_mnt}/raw_country")
def bail_country():
    return read_latest_parquet("Country","tv_Country","ARIA_ARM_BAIL")

@dlt.table(name='raw_bf_diary',comment='Raw Bail BF Diary',path=f"{raw_mnt}/raw_bf_diary")
def bail_bf_diary():
    return read_latest_parquet("BFDiary","tv_BFDiary","ARIA_ARM_BAIL")

@dlt.table(name='raw_bf_type',comment='Raw Bail BF Type',path=f"{raw_mnt}/raw_bf_type")
def bail_bf_type():
    return read_latest_parquet("BFType","tv_BFType","ARIA_ARM_BAIL")

@dlt.table(name='raw_history',comment='Raw Bail History',path=f"{raw_mnt}/raw_history")
def bail_history():
    return read_latest_parquet("History","tv_History","ARIA_ARM_BAIL")

@dlt.table(name='raw_users',comment='Raw Bail Users',path=f"{raw_mnt}/raw_users")
def bail_users():
    return read_latest_parquet("Users","tv_Users","ARIA_ARM_BAIL")

@dlt.table(name='raw_link',comment='Raw Bail Link',path=f"{raw_mnt}/raw_link")
def bail_link():
    return read_latest_parquet("Link","tv_Link","ARIA_ARM_BAIL")

@dlt.table(name='raw_link_detail',comment='Raw Bail Link Detail',path=f"{raw_mnt}/raw_link_detail")
def bail_link_detail():
    return read_latest_parquet("LinkDetail","tv_LinkDetail","ARIA_ARM_BAIL")

@dlt.table(name='raw_status',comment='Raw Bail Status',path=f"{raw_mnt}/raw_status")
def bail_status():
    return read_latest_parquet("Status","tv_Status","ARIA_ARM_BAIL")

@dlt.table(name='raw_case_status',comment='Raw Bail Case Status',path=f"{raw_mnt}/raw_case_status")
def bail_case_status():
    return read_latest_parquet("CaseStatus","tv_CaseStatus","ARIA_ARM_BAIL")

@dlt.table(name='raw_status_contact',comment='Raw Bail Status Contact',path=f"{raw_mnt}/raw_status_contact")
def bail_status_contact():
    return read_latest_parquet("StatusContact","tv_StatusContact","ARIA_ARM_BAIL")

@dlt.table(name='raw_reason_adjourn',comment='Raw Bail Reason Adjourn',path=f"{raw_mnt}/raw_reason_adjourn")
def bail_reason_adjourn():
    return read_latest_parquet("ReasonAdjourn","tv_ReasonAdjourn","ARIA_ARM_BAIL")

@dlt.table(name='raw_appeal_category',comment='Raw Bail Appeal Category',path=f"{raw_mnt}/raw_appeal_category")
def bail_appeal_category():
    return read_latest_parquet("AppealCategory","tv_AppealCategory","ARIA_ARM_BAIL")

@dlt.table(name='raw_category',comment='Raw Bail Category',path=f"{raw_mnt}/raw_category")
def bail_category():
    return read_latest_parquet("Category","tv_Category","ARIA_ARM_BAIL")

@dlt.table(name="raw_case_surety",comment="Raw Bail Surety",path=f"{raw_mnt}/raw_case_surety")
def bail_case_surety():
    return read_latest_parquet("CaseSurety","tv_CaseSurety","ARIA_ARM_BAIL")

@dlt.table(name="raw_port",comment="Raw Bail Port",path=f"{raw_mnt}/raw_port")
def bail_port():
    return read_latest_parquet("Port","tv_Port","ARIA_ARM_BAIL")

@dlt.table(name="raw_decisiontype",comment="Raw Bail Decision Type",path=f"{raw_mnt}/raw_decisiontype")
def bail_decisiontype():
    return read_latest_parquet("DecisionType","tv_DecisionType","ARIA_ARM_BAIL")

@dlt.table(name="raw_case_adjudicator",comment="Raw Bail Case Adjudicator",path=f"{raw_mnt}/raw_case_adjudicator")
def bail_case_adjudictor():
    return read_latest_parquet("CaseAdjudicator","tv_CaseAdjudicator","ARIA_ARM_BAIL")

@dlt.table(name="raw_embassy",comment="Raw Bail Embassy",path=f"{raw_mnt}/raw_embassy")
def bail_embassy():
    return read_latest_parquet("Embassy","tv_Embassy","ARIA_ARM_BAIL")

@dlt.table(name="raw_decision_type",comment="Raw Bail Decision Type",path=f"{raw_mnt}/raw_decision_type")
def bail_decision_type():
    return read_latest_parquet("DecisionType","tv_DecisionType","ARIA_ARM_BAIL")






# COMMAND ----------

@dlt.table(name="raw_stm_cases", comment="Raw Bail STM Cases",path=f"{raw_mnt}/raw_stm_cases")
def raw_stm_cases():
    return read_latest_parquet("STMCases","tv_stm_cases","ARIA_ARM_BAIL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Log function

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from delta.tables import DeltaTable



# COMMAND ----------

audit_schema = StructType([
    StructField("Runid", StringType(), True),
    StructField("Unique_identifier_desc", StringType(), True),
    StructField("Unique_identifier", StringType(), True),
    StructField("Table_name", StringType(), True),
    StructField("Stage_name", StringType(), True),
    StructField("Record_count", IntegerType(), True),
    StructField("Run_dt",TimestampType(), True),
    StructField("Batch_id", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("File_name", StringType(), True),
    StructField("Status", StringType(), True)
])

# Define Delta Table Path in Azure Storage
audit_delta_path = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/AUDIT/BAILS/bl_cr_audit_table"


if not DeltaTable.isDeltaTable(spark, audit_delta_path):
    print(f"🛑 Delta table '{audit_delta_path}' does not exist. Creating an empty Delta table...")

    # Create an empty DataFrame
    empty_df = spark.createDataFrame([], audit_schema)

    # Write the empty DataFrame in Delta format to create the table
    empty_df.write.format("delta").mode("overwrite").save(audit_delta_path)

    print("✅ Empty Delta table successfully created in Azure Storage.")
else:
    print(f"⚡ Delta table '{audit_delta_path}' already exists.")


# COMMAND ----------

def create_audit_df(df: DataFrame, unique_identifier_desc: str,table_name: str, stage_name: str, description: str, additional_columns: list = None) -> None:
    """
    Creates an audit DataFrame and writes it to Delta format.

    :param df: Input DataFrame from which unique identifiers are extracted.
    :param unique_identifier_desc: Column name that acts as a unique identifier.
    :param table_name: Name of the source table.
    :param stage_name: Name of the data processing stage.
    :param description: Description of the table.
    :param additional_columns: List of additional columns to include in the audit DataFrame.
    """

    dt_desc = datetime.utcnow()

    additional_columns = additional_columns or []  # Default to an empty list if None   
    additional_columns = [col(c) for c in additional_columns if c is not None]  # Filter out None values

    audit_df = df.select(col(unique_identifier_desc).alias("unique_identifier"),*additional_columns)\
    .withColumn("Runid", lit(run_id_value))\
        .withColumn("Unique_identifier_desc", lit(unique_identifier_desc))\
            .withColumn("Stage_name", lit(stage_name))\
                .withColumn("Table_name", lit(table_name))\
                    .withColumn("Run_dt", lit(dt_desc).cast(TimestampType()))\
                        .withColumn("Description", lit(description))

    list_cols = audit_df.columns

    final_audit_df = audit_df.groupBy(*list_cols).agg(count("*").cast(IntegerType()).alias("Record_count"))

    final_audit_df.write.format("delta").mode("append").option("mergeSchema","true").save(audit_delta_path)



# COMMAND ----------

audit = spark.read.format("delta").load(audit_delta_path)

audit.display()

# COMMAND ----------


import uuid


def datetime_uuid():
    dt_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return str(uuid.uuid5(uuid.NAMESPACE_DNS,dt_str))

run_id_value = datetime_uuid()


# COMMAND ----------

# MAGIC %md 
# MAGIC # Creating Bronze Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## M1: bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC         -- AppealCase Fields  
# MAGIC         ac.CaseNo,  
# MAGIC         ac.HORef,  
# MAGIC         ac.BailType,
# MAGIC         ac.CourtPreference   
# MAGIC         ac.DateOfIssue,  
# MAGIC         ac.DateOfNextListedHearing,  
# MAGIC         ac.DateReceived,  
# MAGIC         ac.DateServed,  
# MAGIC         Ac.Notes AS AppealCaseNote,  
# MAGIC         ac.InCamera,  
# MAGIC         ac.ProvisionalDestructionDate,
# MAGIC         ac.HOInterpreter,  
# MAGIC         ac.Interpreter,  
# MAGIC         ac.CountryId,
# MAGIC         c1.Country AS CountryOfTravelOrigin,
# MAGIC         ac.PortId,
# MAGIC         po.PortName AS PortOfEntry,
# MAGIC         ac.NationalityId,
# MAGIC         n.Nationality AS Nationality,
# MAGIC         ac.LanguageId AS InterpreterRequirementsLanguage,
# MAGIC         l.Description as Language,
# MAGIC         ac.CentreId,
# MAGIC         hc.Description AS DedicatedHearingCentre,
# MAGIC         ac.AppealCategories,
# MAGIC         ac.PubliclyFunded, 
# MAGIC         -- Case Respondent Fields  
# MAGIC         cr.Respondent AS CaseRespondent,  
# MAGIC         cr.Reference AS CaseRespondentReference,  
# MAGIC         cr.Contact AS CaseRespondentContact,  
# MAGIC         -- Respondent Fields  
# MAGIC         r.Address1 AS RespondentAddress1,  
# MAGIC         r.Address2 AS RespondentAddress2,  
# MAGIC         r.Address3 AS RespondentAddress3, 
# MAGIC         r.Address4 AS RespondentAddress4,  
# MAGIC         r.Address5 AS RespondentAddress5,  
# MAGIC         r.Email AS RespondentEmail,  
# MAGIC         r.Fax AS RespondentFax,  
# MAGIC         r.ShortName AS RespondentShortName,  
# MAGIC         r.Telephone AS RespondentTelephone ,  
# MAGIC         r.Postcode AS RespondentPostcode, 
# MAGIC         --POU  
# MAGIC         p.ShortName AS PouShortName, 
# MAGIC         p.Address1 AS PouAddress1, 
# MAGIC         p.Address2 AS PouAddress2, 
# MAGIC         p.Address3 AS PouAddress3, 
# MAGIC         p.Address4 AS PouAddress4, 
# MAGIC         p.Address5 AS PouAddress5, 
# MAGIC         p.Postcode AS PouPostcode, 
# MAGIC         p.Telephone AS PouTelephone, 
# MAGIC         p.Fax AS PouFax, 
# MAGIC         p.Email AS PouEMail, 
# MAGIC         -- Embassy
# MAGIC         e.Location AS EmbassyLocation,
# MAGIC         e.Embassy,
# MAGIC         e.Surname,
# MAGIC         e.Forename,
# MAGIC         e.Title,
# MAGIC         e.OfficialTitle,
# MAGIC         e.Address1 AS EmbassyAddress1,
# MAGIC         e.Address2 AS EmbassyAddress2,
# MAGIC         e.Address3 AS EmbassyAddress3,
# MAGIC         e.Address4 AS EmbassyAddress4,
# MAGIC         e.Address5 AS EmbassyAddress5,
# MAGIC         e.Postcode AS EmbassyPostcode,
# MAGIC         e.Telephone AS EmbassyTelephone,
# MAGIC         e.Fax AS EmbassyFax,
# MAGIC         e.Email AS EmbassyEmail,
# MAGIC         -- MainRespondent Fields  
# MAGIC         mr.Name AS MainRespondentName,   
# MAGIC         -- File Location Fields  
# MAGIC         fl.Note AS FileLocationNote,  
# MAGIC         fl.TransferDate AS FileLocationTransferDate,  
# MAGIC         -- CaseRepresentative Feilds 
# MAGIC         crep.Name AS CaseRepName,  
# MAGIC         crep.Address1 AS CaseRepAddress1,  
# MAGIC         crep.Address2 AS CaseRepAddress2,  
# MAGIC         crep.Address3 AS CaseRepAddress3,  
# MAGIC         crep.Address4 AS CaseRepAddress4,  
# MAGIC         crep.Address5 AS CaseRepAddress5,  
# MAGIC         crep.Postcode AS CaseRepPostcode,
# MAGIC         crep.TelephonePrime AS CaseRepPhone,
# MAGIC         crep.Email AS CaseRepEmail,  
# MAGIC         crep.Fax AS CaseRepFax,
# MAGIC         crep.DxNo1 AS CaseRepDxNo1,
# MAGIC         crep.DxNo2 AS CaseRepDxNo2,  
# MAGIC         crep.LSCCommission AS CaseRepLSCCommission,  
# MAGIC         crep.RepresentativeRef AS FileSpecificReference,
# MAGIC         crep.Contact AS FileSpecifcContact,
# MAGIC         crep.Telephone AS FileSpecificPhone,
# MAGIC         crep.FileSpecificFax,
# MAGIC         crep.FileSpecificEmail, 
# MAGIC         -- Representative Fields  
# MAGIC         rep.Address1 AS RepAddress1,  
# MAGIC         rep.Address2 AS RepAddress2,  
# MAGIC         rep.Address3 AS RepAddress3,  
# MAGIC         rep.Address4 AS RepAddress4,  
# MAGIC         rep.Address5 AS RepAddress5,  
# MAGIC         rep.Name AS RepName,  
# MAGIC         rep.DxNo1 AS RepDxNo1,  
# MAGIC         rep.DxNo2 AS RepDxNo2,  
# MAGIC         rep.Postcode AS RepPostcode,  
# MAGIC         rep.Telephone AS RepTelephone,  
# MAGIC         rep.Fax AS RepFax,  
# MAGIC         rep.Email AS RepEmail,  
# MAGIC         -- Cost Award Fields  
# MAGIC         ca.DateOfApplication,  
# MAGIC         ca.TypeOfCostAward, 
# MAGIC         ca.ApplyingParty,  
# MAGIC         ca.PayingPArty,  
# MAGIC         ca.MindedToAward,  
# MAGIC         ca.ObjectionToMindedToAward,  
# MAGIC         ca.CostsAwardDecision,  
# MAGIC         ca.CostsAmount,  
# MAGIC         ca.AppealStage  
# MAGIC
# MAGIC
# MAGIC         FROM [ARIAREPORTS].[dbo].[AppealCase] ac
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CaseRespondent] cr
# MAGIC         ON ac.CaseNo = cr.CaseNo
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Respondent] r
# MAGIC         ON cr.RespondentId = r.RespondentId
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Pou] p
# MAGIC         ON cr.RespondentId = p.PouId
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[MainRespondent] mr
# MAGIC         ON cr.MainRespondentId = mr.MainRespondentId
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[FileLocation] fl
# MAGIC         ON ac.CaseNo = fl.CaseNo
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CaseRep] crep
# MAGIC         ON ac.CaseNo = crep.CaseNo
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Representative] rep
# MAGIC         ON crep.RepresentativeId = rep.RepresentativeId
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Language] l
# MAGIC         ON ac.LanguageId = l.LanguageId
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CostAward] ca
# MAGIC         ON ac.CaseNo = ca.CaseNo
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Country] c1
# MAGIC         ON ac.CountryId = c1.CountryId
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Country] n
# MAGIC         ON ac.NationalityId = n.CountryId
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Port] po
# MAGIC         ON ac.PortId = po.PortId
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingCentre] hc
# MAGIC         ON ac.CentreId = hc.CentreId
# MAGIC         LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Embassy] e
# MAGIC         ON cr.RespondentId = e.EmbassyId
# MAGIC  

# COMMAND ----------

@dlt.table(
    name='bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang',
    comment='ARIA Migration Archive Bails cases bronze table',
    partition_cols=["CaseNo"],
    path=f"{bronze_mnt}/bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang"
)
def bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang():

    df = (dlt.read("raw_appeal_cases").alias("ac")
    .join(dlt.read("raw_case_respondents").alias("cr"), col("ac.CaseNo") == col("cr.CaseNo"), 'left_outer')
    .join(dlt.read("raw_respondent").alias("r"), col("cr.RespondentId") == col("r.RespondentId"), 'left_outer')
    .join(dlt.read("raw_pou").alias("p"), col("cr.RespondentId") == col("p.PouId"), 'left_outer')
    .join(dlt.read("raw_main_respondent").alias("mr"), col("cr.MainrespondentId") == col("mr.MainRespondentId"), 'left_outer')
    .join(dlt.read("raw_file_location").alias("fl"), col("ac.CaseNo") == col("fl.CaseNo"), "left_outer")
    .join(dlt.read("raw_case_rep").alias("crep"), col("ac.CaseNo") == col("crep.CaseNo"), "left_outer")
    .join(dlt.read("raw_representative").alias("rep"), col("crep.RepresentativeId") == col("rep.RepresentativeId"), "left_outer")
    .join(dlt.read("raw_language").alias("l"), col("ac.LanguageId") == col("l.LanguageId"), "left_outer")
    .join(dlt.read("raw_cost_award").alias("ca"), col("ac.CaseNo") == col("ca.CaseNo"), "left_outer")
    .join(dlt.read("raw_country").alias("cl"), col("ac.CountryId") == col("cl.CountryId"), "left_outer")
    .join(dlt.read("raw_country").alias("n"), col("ac.NationalityId") == col("n.CountryId"), "left_outer")
    .join(dlt.read("raw_port").alias("po"),col("ac.PortId") == col("po.PortId"), "left_outer")
    .join(dlt.read("raw_hearing_centre").alias("hc"), col("ac.CentreId") == col("hc.CentreId"), "left_outer")
    .join(dlt.read("raw_embassy").alias("e"), col("cr.RespondentId") == col("e.EmbassyId"), "left_outer")
    .select(
        # AppealCase Fields
        col("ac.CaseNo"),
        col("ac.HORef"),
        col("ac.BailType"),
        col("ac.CourtPreference"),
        col("ac.DateOfIssue"),
        col("ac.DateOfNextListedHearing"),
        col("ac.DateReceived"),
        col("ac.DateServed"),
        col("ac.Notes").alias("AppealCaseNote"),
        col("ac.InCamera"),
        col("ac.ProvisionalDestructionDate"),
        col("ac.HOInterpreter"),
        col("ac.Interpreter"),
        col("ac.CountryId"),
        col("cl.Country").alias("CountryOfTravelOrigin"),
        col("po.PortId"),
        col("po.PortName").alias("PortOfEntry"),
        col("ac.NationalityId"),
        col("n.Nationality").alias("Nationality"),
        col("ac.LanguageId").alias("InterpreterRequirementsLanguage"),
        col("l.Description").alias("Language"),
        col("ac.CentreId"),
        col("hc.Description").alias("DedicatedHearingCentre"),
        col("ac.AppealCategories"),
        col("ac.PubliclyFunded"),
        # Case Respondent Fields
        col("cr.Respondent").alias("CaseRespondent"),
        col("cr.Reference").alias("CaseRespondentReference"),
        col("cr.Contact").alias("CaseRespondentContact"),
        # Respondent Fields
        col("r.Address1").alias("RespondentAddress1"),
        col("r.Address2").alias("RespondentAddress2"),
        col("r.Address3").alias("RespondentAddress3"),
        col("r.Address4").alias("RespondentAddress4"),
        col("r.Address5").alias("RespondentAddress5"),
        col("r.Email").alias("RespondentEmail"),
        col("r.Fax").alias("RespondentFax"),
        col("r.ShortName").alias("RespondentShortName"),
        col("r.Telephone").alias("RespondentTelephone"),
        col("r.Postcode").alias("RespondentPostcode"),
        # POU Fields
        col("p.ShortName").alias("PouShortName"),
        col("p.Address1").alias("PouAddress1"),
        col("p.Address2").alias("PouAddress2"),
        col("p.Address3").alias("PouAddress3"),
        col("p.Address4").alias("PouAddress4"),
        col("p.Address5").alias("PouAddress5"),
        col("p.Postcode").alias("PouPostcode"),
        col("p.Telephone").alias("PouTelephone"),
        col("p.Fax").alias("PouFax"),
        col("p.Email").alias("PouEmail"),
        # Embassy
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
        # Main Respondent Fields
        col("mr.Name").alias("MainRespondentName"),
        # File Location Fields
        col("fl.Note").alias("FileLocationNote"),
        col("fl.TransferDate").alias("FileLocationTransferDate"),
        # Case Representative Fields
        col("crep.Name").alias("CaseRepName"),
        col("crep.Address1").alias("CaseRepAddress1"),
        col("crep.Address2").alias("CaseRepAddress2"),
        col("crep.Address3").alias("CaseRepAddress3"),
        col("crep.Address4").alias("CaseRepAddress4"),
        col("crep.Address5").alias("CaseRepAddress5"),
        col("crep.Postcode").alias("CaseRepPostcode"),
        col("crep.TelephonePrime").alias("CaseRepPhone"),
        col("crep.Email").alias("CaseRepEmail"),
        col("crep.Fax").alias("CaseRepFax"),
        col("crep.DxNo1").alias("CaseRepDxNo1"),
        col("crep.DxNo2").alias("CaseRepDxNo2"),
        col("crep.LSCCommission").alias("CaseRepLSCCommission"),
        col("crep.Contact").alias("FileSpecifcContact"),
        col("crep.Telephone").alias("FileSpecificPhone"),
        col("crep.RepresentativeRef").alias("FileSpecificReference"),
        col("crep.FileSpecificFax"),
        col("crep.FileSpecificEmail"),
        # Representative Fields
        col("rep.Address1").alias("RepAddress1"),
        col("rep.Address2").alias("RepAddress2"),
        col("rep.Address3").alias("RepAddress3"),
        col("rep.Address4").alias("RepAddress4"),
        col("rep.Address5").alias("RepAddress5"),
        col("rep.Name").alias("RepName"),
        col("rep.DxNo1").alias("RepDxNo1"),
        col("rep.DxNo2").alias("RepDxNo2"),
        col("rep.Postcode").alias("RepPostcode"),
        col("rep.Telephone").alias("RepTelephone"),
        col("rep.Fax").alias("RepFax"),
        col("rep.Email").alias("RepEmail"),
        # Cost Award Fields
        col("ca.DateOfApplication"),
        col("ca.TypeOfCostAward"),
        col("ca.ApplyingParty"),
        col("ca.PayingParty"),
        col("ca.MindedToAward"),
        col("ca.ObjectionToMindedToAward"),
        col("ca.CostsAwardDecision"),
        col("ca.CostsAmount"),
        col("Ca.DateOfDecision"),
        col("ca.AppealStage")
    )
    )

    ## Create and save audit log for this table
    table_name = "bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang"
    stage_name = "bronze_stage"

    description = "The bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang table consolidates key details of appeal cases, including case numbers, court preferences, hearing dates, language requirements, and respondents, alongside associated embassy, representative, and cost award information. It joins multiple tables to provide a comprehensive dataset for tracking legal proceedings and related entities"

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)



    return df
                        


# COMMAND ----------

# MAGIC %md
# MAGIC ## M2: bronze_bail_ac_ca_apt_country_detc
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     -- CaseAppellant Fields
# MAGIC     ca.AppellantId,
# MAGIC     ca.CaseNo,
# MAGIC     ca.Relationship,
# MAGIC     -- Appellant Fields
# MAGIC     a.PortReference,
# MAGIC     a.Name AS AppellantName,
# MAGIC     a.Forenames AS AppellantForenames,
# MAGIC     a.Title AS AppellantTitle,
# MAGIC     a.BirthDate AS AppellantBirthDate,f
# MAGIC     a.Address1 AS AppellantAddress1,
# MAGIC     a.Address2 AS AppellantAddress2,
# MAGIC     a.Address3 AS AppellantAddress3,
# MAGIC     a.Address4 AS AppellantAddress4,
# MAGIC     a.Address5 AS AppellantAddress5,
# MAGIC     a.Postcode AS AppellantPostcode,
# MAGIC     a.Telephone AS AppellantTelephone,
# MAGIC     a.Fax AS AppellantFax,
# MAGIC     a.PrisonRef AS AppellantPrisonRef,
# MAGIC     a.Detained AS AppellantDetained,
# MAGIC     a.Email AS AppellantEmail,
# MAGIC     -- DetentionCentre Fields
# MAGIC     dc.Centre AS DetentionCentre,
# MAGIC     dc.Address1 AS DetentionCentreAddress1,
# MAGIC     dc.Address2 AS DetentionCentreAddress2,
# MAGIC     dc.Address3 AS DetentionCentreAddress3,
# MAGIC     dc.Address4 AS DetentionCentreAddress4,
# MAGIC     dc.Address5 AS DetentionCentreAddress5,
# MAGIC     dc.Postcode AS DetentionCentrePoscode,
# MAGIC     dc.Fax AS DetentionCentreFax,
# MAGIC     -- Country Fields
# MAGIC     c.Country,
# MAGIC FROM [ARIAREPORTS].[dbo].[CaseAppellant] ca
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Appellant] a
# MAGIC ON ca.AppellantId = a.AppellantId
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[DetentionCentre] dc
# MAGIC ON a.DetentionCentreId = dc.DetentionCentreId
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Country] c
# MAGIC ON a.AppellantCountryId = c.CountryId

# COMMAND ----------

@dlt.table(
    name='bronze_bail_ac_ca_apt_country_detc',
    comment='ARIA Migration Archive Bails cases bronze table',
    partition_cols=["CaseNo"],
    path=f"{bronze_mnt}/bronze_bail_ac_ca_apt_country_detc")
def bronze_bail_ac_ca_apt_country_detc():
    df =  (
        dlt.read("raw_case_appellant").alias("ca")
        .join(dlt.read("raw_appellant").alias("a"), col("ca.AppellantId") == col("a.AppellantId"), "left_outer")
        .join(dlt.read("raw_detention_centre").alias("dc"), col("a.DetentionCentreId") == col("dc.DetentionCentreId"), "left_outer")
        .join(dlt.read("raw_country").alias("c"), col("a.AppellantCountryId") == col("c.CountryId"), "left_outer")
        .select(
            # CaseAppellant Fields
            col("ca.AppellantId"),
            col("ca.CaseNo"),
            col("ca.Relationship"),
            # Appellant Fields
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
            col("a.PrisonRef").alias("AppellantPrisonRef"),
            col("a.Detained").alias("AppellantDetained"),
            col("a.Email").alias("AppellantEmail"),
            # DetentionCentre Fields
            col("dc.Centre").alias("DetentionCentre"),
            col("dc.Address1").alias("DetentionCentreAddress1"),
            col("dc.Address2").alias("DetentionCentreAddress2"),
            col("dc.Address3").alias("DetentionCentreAddress3"),
            col("dc.Address4").alias("DetentionCentreAddress4"),
            col("dc.Address5").alias("DetentionCentreAddress5"),
            col("dc.Postcode").alias("DetentionCentrePostcode"),
            col("dc.Fax").alias("DetentionCentreFax"),
            # Country Fields
            col("c.DoNotUseNationality")
    )
    )

    ## Create and save audit log for this table
    table_name = "bronze_bail_ac_ca_apt_country_detc"
    stage_name = "bronze_stage"

    description = "The bronze_bail_ac_ca_apt_country_detc table consolidates key details related to appellants in bail cases. It joins data from multiple sources, including case appellant records, appellant personal details, detention centre information, and country data. The table provides a comprehensive view of the appellant’s identity, detention status, and nationality, linking them to their respective case numbers. Key attributes include appellant name, birth date, address, telephone, email, detention centre details, and country information, ensuring a structured dataset for tracking individuals involved in bail proceedings."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## M3: bronze_ bail_ac _cl_ht_list_lt_hc_c_ls_adj
# MAGIC
# MAGIC -- Data Mapping
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC
# MAGIC     -- Status
# MAGIC     s.CaseNo,
# MAGIC     s.StatusId,
# MAGIC     s.Outcome,
# MAGIC     -- CaseList
# MAGIC     cl.TimeEstimate AS CaseListTimeEstimate,
# MAGIC     cl.ListNumber AS CaseListNumber,
# MAGIC     cl.HearingDuration AS CaseListHearingDuration,
# MAGIC     cl.StartTime AS CaseListStartTime,
# MAGIC     --HearingType
# MAGIC     ht.Description AS HearingTypeDesc,
# MAGIC     ht.TimeEstimate AS HearingTypeEst,
# MAGIC     ht.DoNotUse,
# MAGIC     --List
# MAGIC     l.ListName,
# MAGIC     l.StartDate AS HearingDate,
# MAGIC     l.StartTime AS ListStartTime,
# MAGIC     l.NumReqSeniorImmigrationJudge AS UpperTribJudge,
# MAGIC     l.NumReqDesignatedImmigrationJudge AS DesJudgeFirstTier,
# MAGIC     l.NumReqImmigrationJudge AS JudgeFirstTier,
# MAGIC     l.NumReqNonLegalMember AS NonLegalMember,
# MAGIC     --ListType
# MAGIC     lt.Description AS ListTypeDesc,
# MAGIC     lt.ListType,
# MAGIC     lt.DoNotUse AS DoNotUseListType,
# MAGIC     --Court
# MAGIC     c.CourtName,
# MAGIC     c.DoNotUse AS DoNotUseCourt,
# MAGIC     --HearingCentre
# MAGIC     hc.Description AS HearingCentreDesc,
# MAGIC     -- ListSitting
# MAGIC     ls.Chairman,
# MAGIC     Ls.Position,
# MAGIC     -- Adjudicator
# MAGIC     a.Surname AS AdjudicatorSurname,
# MAGIC     a.Forenames AS AdjudicatorForenames,
# MAGIC     a.Title AS AdjudicatorTitle,
# MAGIC     a.Notes AS AdjudicatorNote,
# MAGIC     --DecisionType 
# MAGIC     dt.Description AS OutcomeDescription, 
# MAGIC     --AppealCase 
# MAGIC     ac.Notes
# MAGIC     FROM [ARIAREPORTS].[dbo].[Status] s
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CaseList] cl
# MAGIC     ON s.StatusId = cl.StatusId
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingType] ht
# MAGIC     ON cl.HearingTypeId = ht.HearingTypeId
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[List] l
# MAGIC     ON cl.ListId = l.ListId
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[ListType] lt
# MAGIC     ON l.ListTypeId = lt.ListTypeId
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Court] c
# MAGIC     ON l.CourtId = c.CourtId
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingCentre] hc
# MAGIC     ON l.CentreId = hc.CentreId
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[ListSitting] ls
# MAGIC     ON l.ListId = ls.ListId
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Adjudicator] a
# MAGIC     ON ls.AdjudicatorId = a.AdjudicatorId
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[DecisionType] dt
# MAGIC     ON s.Outcome = dt.DecisionTypeId
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[AppealCase] ac
# MAGIC     ON s.CaseNo = ac.CaseNo

# COMMAND ----------

@dlt.table(
    name="bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj",
    comment="ARIA Migration Archive Bails cases bronze table",
    partition_cols=["CaseNo"],
    path=f"{bronze_mnt}/bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj"
)
def bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj():
    df =  (
        dlt.read("raw_status").alias("s")
        .join(dlt.read("raw_case_list").alias("cl"), col("s.StatusId") == col("cl.StatusId"))
        .join(dlt.read("raw_hearing_type").alias("ht"), col("cl.HearingTypeId") == col("ht.HearingTypeId"), "left_outer")
        .join(dlt.read("raw_list").alias("l"), col("cl.ListId") == col("l.ListId"), "left_outer")
        .join(dlt.read("raw_list_type").alias("lt"), col("l.ListTypeId") == col("lt.ListTypeId"), "left_outer")
        .join(dlt.read("raw_court").alias("c"), col("l.CourtId") == col("c.CourtId"), "left_outer")
        .join(dlt.read("raw_hearing_centre").alias("hc"), col("l.CentreId") == col("hc.CentreId"), "left_outer")
        .join(dlt.read("raw_list_sitting").alias("ls"), col("l.ListId") == col("ls.ListId"), "left_outer")
        .join(dlt.read("raw_adjudicator").alias("adj"), col("ls.AdjudicatorId") == col("adj.AdjudicatorId"), "left_outer")
        .join(dlt.read("raw_decision_type").alias("dt"), col("s.outcome") == col("dt.DecisionTypeId"), "left_outer")
        .join(dlt.read("raw_appeal_cases").alias("ac"), col("s.CaseNo") == col("ac.CaseNo"))
        .select(
            # Status
            col("s.CaseNo"),
            col("s.StatusId"),
            col("s.Outcome"),
            # CaseList
            col("cl.TimeEstimate").alias("CaseListTimeEstimate"),
            col("cl.StartTime").alias("CaseListStartTime"),
            # HearingType
            col("ht.Description").alias("HearingTypeDesc"),
            # List
            col("l.ListName"),
            col("l.StartDate").alias("HearingDate"),
            col("l.StartTime").alias("ListStartTime"),
            col("l.NumReqSeniorImmigrationJudge").alias("UpperTribJudge"),
            col("l.NumReqDesignatedImmigrationJudge").alias("DesJudgeFirstTier"),
            col("l.NumReqImmigrationJudge").alias("JudgeFirstTier"),
            col("l.NumReqNonLegalMember").alias("NonLegalMember"),
            # ListType
            col("lt.Description").alias("ListTypeDesc"),
            col("lt.ListType"),
            # Court
            col("c.CourtName"),
            # HearingCenter
            col("hc.Description").alias("HearingCentreDesc"),
            # ListSitting
            col("ls.Chairman"),
            col("ls.Position"),
            # Adjudicator
            col("adj.Surname").alias("AdjudicatorSurname"),
            col("adj.Forenames").alias("AdjudicatorForenames"),
            col("adj.Title").alias("AdjudicatorTitle"),
            # Decision Type
            col("dt.Description").alias("OutcomeDescription"),
            # AppealCase
            col("ac.Notes")

        )
        )
    
    ## Create and save audit log for this table
    table_name = "bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj"
    stage_name = "bronze_stage"

    description = "The bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj table consolidates information on case statuses, hearings, listings, courts, adjudicators, and appeal outcomes in bail cases. It integrates data from multiple sources, including case lists, hearing types, list types, courts, hearing centres, list sittings, adjudicators, and decision types, providing a structured view of legal proceedings. Key attributes include case status, hearing date, list type, court name, hearing centre, adjudicator details, and appeal case notes, enabling efficient tracking of hearing schedules and case outcomes"

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df





# COMMAND ----------

# from pyspark.sql.functions import concat_ws, col, first, countDistinct

# df_named = m3_df.withColumn(
#     "FullName",
#     concat_ws(" ", col("AdjudicatorTitle"), col("AdjudicatorForenames"), col("AdjudicatorSurname"))
# )

# columns_to_group_by = [col(c) for c in m3_df.columns if c not in ["FullName", "AdjudicatorTitle", "AdjudicatorForenames", "AdjudicatorSurname", "Chairman", "Position"]]

# pivoted_df = df_named.groupBy(*columns_to_group_by) \
#     .pivot("Position") \
#     .agg(first("FullName")).withColumnRenamed("3", "CourtClerkUsher")

# pivoted_df.display()
# rows = pivoted_df.collect()


# COMMAND ----------

# rows[303]

# COMMAND ----------

# labels = []

# label_fields = ["UpperTribJudge", "DesJudgeFirstTier", "JudgeFirstTier", "NonLegalMember"]
# row = rows[303]
# caseno = row["CaseNo"]

# for field in label_fields:
#     count = row[field] if row[field] is not None else 0

#     for i in range(count):
#         labels.append(f"{field}_{i+1}")

# positions = sorted([col for col in pivoted_df.columns if col.isdigit()], key=int)

# label_col_map = dict(zip(positions, labels))

# display(caseno, label_col_map)

# for i in label_col_map.keys():
#     labels_dict = label_col_map.get(i, 'N/A')
#     print(f"Position {i}: {labels_dict}")
#     print(row[i])

# COMMAND ----------

# from pyspark.sql.functions import col
# m3_df = spark.read.table("hive_metastore.aria_bails.bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj")

# m3_df.filter(col("Caseno")=="ZZ/00001     ").display()


# COMMAND ----------

# UpperTribJudge = first_row["UpperTribJudge"]
# DesJudgeFirstTier = first_row["DesJudgeFirstTier"]
# JudgeFirstTier = first_row["JudgeFirstTier"]

# print(f"UpperTribJudge: {UpperTribJudge}", f"DesJudgeFirstTier: {DesJudgeFirstTier}", f"JudgeFirstTier: {JudgeFirstTier}")



# COMMAND ----------

# MAGIC %md
# MAGIC ## M4: bronze_bail_ac_bfdiary_bftype
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     bfd.CaseNo,
# MAGIC     bfd.BFDate,
# MAGIC     bfd.Entry,
# MAGIC     bfd.DateCompleted,
# MAGIC     -- BF Type Fields
# MAGIC     bft.Description AS BFTypeDescription,
# MAGIC
# MAGIC FROM [ARIAREPORTS].[dbo].[BFDiary] bfd
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[BFType] bft
# MAGIC     ON bfd.BFTypeId = bft.BFTypeId;
# MAGIC

# COMMAND ----------

@dlt.table(
    name="bronze_bail_ac_bfdiary_bftype", 
    comment="ARIA Migration Archive Bails cases bronze table", 
    partition_cols=["CaseNo"],
    path=f"{bronze_mnt}/bronze_bail_ac_bfdiary_bftype")
def bronze_bail_ac_bfdiary_bftype():
    df = (
        dlt.read("raw_bf_diary").alias("bfd")
        .join(dlt.read("raw_bf_type").alias("bft"), col("bfd.BFTypeId") == col("bft.BFTypeId"), "left_outer")
        .select(
            col("bfd.CaseNo"),
            col("bfd.BFDate"),
            col("bfd.Entry") ,
            col("bfd.DateCompleted"),
            # -- bF Type Fields
            col("bft.Description").alias("BFTypeDescription")
        )
        )
    ## Create and save audit log for this table
    table_name = "bronze_bail_ac_bfdiary_bftype"
    stage_name = "bronze_stage"

    description = "The bronze_bail_ac_bfdiary_bftype table consolidates information on BF (Bring Forward) diary entries related to bail cases. It links BF diary records with their corresponding BF types, providing a structured overview of case follow-ups and deadlines. Key attributes include case number, BF date, entry details, completion date, and BF type description, enabling efficient tracking of pending and completed case actions."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## M5: bronze_ bail_ac _history_users

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT
# MAGIC
# MAGIC     h.CaseNo,
# MAGIC     h.HistoryId,
# MAGIC     h.HistDate,
# MAGIC     h.HistType,
# MAGIC     h.Comment AS HistoryComment,
# MAGIC     h.DeletedBy,
# MAGIC     u.Fullname AS UserFullName,
# MAGIC FROM [ARIAREPORTS].[dbo].[History] h
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Users] u
# MAGIC ON h.UserId = u.UserId

# COMMAND ----------

@dlt.table(
    name="bronze_bail_ac_history_users", 
    comment="ARIA Migration Archive Bails cases bronze table", 
    partition_cols=["CaseNo"],
    path=f"{bronze_mnt}/bronze_bail_ac_history_users")
def bronze_bail_ac_history_users():
    df = (
        dlt.read("raw_history").alias("h")
        .join(dlt.read("raw_users").alias("u"), col("h.UserId") == col("u.UserId"), "left_outer")
        .select(
            # History table fields
            col("h.CaseNo"),
            col("h.HistoryId"),
            col("h.HistDate"),
            col("h.HistType"),
            col("h.Comment").alias("HistoryComment"),
            col("h.DeletedBy"),
            # Users table fields
            col("u.Fullname").alias("UserFullname")
        )
    )

    ## Create and save audit log for this table
    table_name = "bronze_bail_ac_history_users"
    stage_name = "bronze_stage"

    description = "The bronze_bail_ac_history_users table captures a detailed history of actions taken on bail cases, linking historical records with user information. It integrates data from the history and users tables, providing insights into case modifications, timestamps, action types, and user activity. Key attributes include case number, history ID, action date, history type, comments, deleted by information, and the full name of the user responsible for the action, ensuring a transparent audit trail of case updates"

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## M6: bronze_ bail_ac _link_linkdetail

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC   SELECT
# MAGIC   
# MAGIC   l.LinkNo,
# MAGIC
# MAGIC   a.Name,
# MAGIC
# MAGIC   a.Fornames,
# MAGIC
# MAGIC   a.Title
# MAGIC
# MAGIC   l.CaseNo,
# MAGIC
# MAGIC   ld.Comment AS LinkDetailComment
# MAGIC
# MAGIC   FROM [ARIAREPORTS].[dbo].[Link] l
# MAGIC
# MAGIC   LEFT OUTER JOIN [ARIAREPORTS].[dbo].[LinkDetail] ld
# MAGIC   ON l.LinkNo = ld.LinkNo
# MAGIC
# MAGIC   LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CaseAppellant] ca
# MAGIC   ON l.CaseNo = ca.CaseNo
# MAGIC
# MAGIC   LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Appellant] a
# MAGIC   ON ca.AppellantId = a.AppellantId

# COMMAND ----------

@dlt.table(
  name="bronze_bail_ac_link_linkdetail", 
  comment="ARIA Migration Archive Bails cases bronze table", 
  partition_cols=["CaseNo"],
  path=f"{bronze_mnt}/bronze_bail_ac_link_linkdetail")
def bronze_bail_ac_link_linkdetail():
    df = (
        dlt.read("raw_link").alias("l")
        .join(dlt.read("raw_link_detail").alias("ld"), col("l.LinkNo") == col("ld.LinkNo"), "left_outer")
        .join(dlt.read("raw_case_appellant").alias("ca"), col("l.CaseNo") == col("ca.CaseNo"), "left_outer")
        .join(dlt.read("raw_appellant").alias("a"), col("ca.AppellantId") == col("a.AppellantId"), "left_outer")
        .select(
          col("l.LinkNo"),
          col("a.Name"),
          col("a.Forenames"),
          col("a.Title"),
          col("l.CaseNo"),
          col("ld.Comment").alias("LinkDetailComment")
          )
        )
    ## Create and save audit log for this table
    table_name = "bronze_bail_ac_link_linkdetail"
    stage_name = "bronze_stage"

    description = "The bronze_bail_ac_link_linkdetail table captures case linkages and related appellant details within bail cases. It integrates data from case links, link details, and appellant records, providing insights into case associations and relevant individuals. Key attributes include link number, case number, appellant’s name, title, and any associated comments from link details, facilitating the tracking of case relationships and appellant connections"

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## M7: bronze_bail_status_sc_ra_cs

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT
# MAGIC
# MAGIC     --Status Fields
# MAGIC     s.StatusId,
# MAGIC     s.CaseNo,
# MAGIC     s.CaseStatus,
# MAGIC     s.DateReceived,
# MAGIC     s.Notes1 AS StatusNotes1,
# MAGIC     -- Date Fields varied on case status
# MAGIC     s.Keydate,
# MAGIC     s.MiscDate1,
# MAGIC     s.MiscDate2,
# MAGIC     s.MiscDate3,
# MAGIC     -----------------
# MAGIC     s.Recognizance AS TotalAmountOfFinancialCondition,
# MAGIC     s.Security AS TotalSecurity,
# MAGIC     s.Notes2 AS StatusNotes2,
# MAGIC     s.DecisionDate,
# MAGIC     s.Outcome,
# MAGIC     dt.Description AS OutcomeDescription,
# MAGIC     s.Promulgated AS StatusPromulgated,
# MAGIC     s.Party AS StatusParty,
# MAGIC     s.ResidenceOrder,
# MAGIC     s.ReportingOrder,
# MAGIC     s.BailedTimePlace,
# MAGIC     s.BaileddateHearing,
# MAGIC     s.InterpreterRequired,
# MAGIC     s.BailConditions,
# MAGIC     s.LivesAndSleepsAt,
# MAGIC     s.AppearBefore,
# MAGIC     s.ReportTo,
# MAGIC     s.AdjournmentParentStatusId,
# MAGIC     s.ListedCentre,
# MAGIC     s.DecisionSentToHO,
# MAGIC     s.DecisionSentToHODate,
# MAGIC     s.VideoLink,
# MAGIC     s.WorkAndStudyRestriction,
# MAGIC     s.Tagging AS StatusBailConditionTagging,
# MAGIC     s.OtherCondition,
# MAGIC     s.OutcomeReasons,
# MAGIC     s.FC,
# MAGIC     --Case Status Fields
# MAGIC     cs.Description AS CaseStatusDescription,
# MAGIC     --Status Contact Fields
# MAGIC     sc.Contact AS ContactStatus,
# MAGIC     sc.CourtName AS SCCourtName,
# MAGIC     sc.Address1 AS SCAddress1,
# MAGIC     sc.Address2 AS SCAddress2,
# MAGIC     sc.Address3 AS SCAddress3,
# MAGIC     sc.Address4 AS SCAddress4,
# MAGIC     sc.Address5 AS SCAddress5,
# MAGIC     sc.Postcode AS SCPostcode,
# MAGIC     sc.Telephone AS SCTelephone,
# MAGIC     --Language Fields
# MAGIC     l.Description AS LanguageDescription,
# MAGIC From [ARIAREPORTS].[dbo].[Status] s
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CaseStatus] cs
# MAGIC ON s.CaseStatus = cs.CaseStatusId
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[StatusContact] sc
# MAGIC ON s.StatusId = sc.StatusId
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[ReasonAdjourn] ra
# MAGIC ON s.ReasonAdjournId = ra.ReasonAdjournId
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Language] l
# MAGIC ON s.AdditionalLanguageId = l.LanguageId
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[DecisionType] dt
# MAGIC ON s.Outcome = dt.DecisionTypeId

# COMMAND ----------

@dlt.table(
    name="bronze_bail_status_sc_ra_cs",
    comment="ARIA Migration Archive Bails Status cases bronze table",
    partition_cols=["CaseNo"],
    path=f"{bronze_mnt}/bronze_bail_status_sc_ra_cs"
)
def bronze_bail_status_sc_ra_cs():
    df = (
        dlt.read("raw_status").alias("s")
        .join(
            dlt.read("raw_case_status").alias("cs"),
            col("s.CaseStatus") == col("cs.CaseStatusId"),
            "left_outer"
        )
        .join(
            dlt.read("raw_status_contact").alias("sc"),
            col("s.StatusId") == col("sc.StatusId"),
            "left_outer"
        )
        .join(
            dlt.read("raw_reason_adjourn").alias("ra"),
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
            dlt.read("raw_stm_cases").alias("stm"),
            col("s.StatusId") == col("stm.NewStatusId"),
            "left_outer"
        )
        .join(
            dlt.read("raw_list_type").alias("lt"),
            col("s.ListTypeId") == col("lt.ListTypeId"),
            "left_outer"
        )
        .join(
            dlt.read("raw_hearing_type").alias("ht"),
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
            # -------------------------
            # STATUS fields (s)
            # -------------------------
            col("s.StatusId"),
            col("s.CaseNo"),
            col("s.CaseStatus"),
            col("s.DateReceived"),
            col("s.Notes1").alias("StatusNotes1"),
            col("s.Keydate"),
            col("s.MiscDate1"),
            col("s.MiscDate2"),
            col("s.MiscDate3"),
            col("s.Recognizance").alias("TotalAmountOfFinancialCondition"),
            col("s.Security").alias("TotalSecurity"),
            col("s.Notes2").alias("StatusNotes2"),
            col("s.DecisionDate"),
            col("s.Outcome"),
            col("dt.Description").alias("OutcomeDescription"),
            col("s.Promulgated").alias("StatusPromulgated"),
            col("s.Party").alias("StatusParty"),
            col("s.ResidenceOrder"),
            col("s.ReportingOrder"),
            col("s.BailedTimePlace"),
            col("s.BaileddateHearing"),
            col("s.InterpreterRequired"),
            col("s.BailConditions"),
            col("s.LivesAndSleepsAt"),
            col("s.AppearBefore"),
            col("s.ReportTo"),
            col("s.AdjournmentParentStatusId"),
            col("s.ListedCentre").alias("HearingCentre"),
            col("s.DecisionSentToHO"),
            col("s.DecisionSentToHODate"),
            col("s.VideoLink"),
            col("s.WorkAndStudyRestriction"),
            col("s.Tagging").alias("StatusBailConditionTagging"),
            col("s.OtherCondition"),
            col("s.OutcomeReasons"),
            col("s.FC"),

            # -------------------------
            # CASE STATUS fields (cs)
            # -------------------------
            col("cs.Description").alias("CaseStatusDescription"),

            # -------------------------
            # STATUS CONTACT fields (sc)
            # -------------------------
            col("sc.Contact").alias("ContactStatus"),
            col("sc.CourtName").alias("SCCourtName"),
            col("sc.Address1").alias("SCAddress1"),
            col("sc.Address2").alias("SCAddress2"),
            col("sc.Address3").alias("SCAddress3"),
            col("sc.Address4").alias("SCAddress4"),
            col("sc.Address5").alias("SCAddress5"),
            col("sc.Postcode").alias("SCPostcode"),
            col("sc.Telephone").alias("SCTelephone"),

            # -------------------------
            # LANGUAGE fields (l)
            # -------------------------
            col("l.Description").alias("LanguageDescription"),

            # -------------------------
            # STM, List Type & Hearing Type fields
            # -------------------------
            col("s.ListTypeId"),
            col("lt.Description").alias("ListType"),
            col("s.HearingTypeId"),
            col("ht.Description").alias("HearingType"),

            # -------------------------
            # Judiciary fields (Adjudicator)
            # -------------------------
            col("stm.Judiciary1Id"),
            concat_ws(" ", col("a1.Surname"), col("a1.Forenames"), col("a1.Title")).alias("Judiciary1Name"),
            col("stm.Judiciary2Id"),
            concat_ws(" ", col("a2.Surname"), col("a2.Forenames"), col("a2.Title")).alias("Judiciary2Name"),
            col("stm.Judiciary3Id"),
            concat_ws(" ", col("a3.Surname"), col("a3.Forenames"), col("a3.Title")).alias("Judiciary3Name"),
        )
    )

    ## Create and save audit log for this table
    table_name = "bronze_bail_status_sc_ra_cs"
    stage_name = "bronze_stage"

    description = "The bronze_bail_status_sc_ra_cs table consolidates information related to case statuses, contacts, adjournment reasons, language preferences, list types, hearing types, and judiciary assignments."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df, unique_identifier_desc, table_name, stage_name, description)

    return df


    ## Create and save audit log for this table
    table_name = "bronze_bail_status_sc_ra_cs"
    stage_name = "bronze_stage"

    description = "The bronze_bail_status_sc_ra_cs table consolidates information related to case statuses, status contacts, adjournment reasons, and language preferences in bail cases. It integrates data from multiple sources, including case status records, decision outcomes, adjournment reasons, and interpreter requirements, providing a structured view of the legal progression of bail cases. Key attributes include case number, case status description, outcome details, financial conditions, bail conditions, hearing dates, reporting orders, status contacts (including court and address details), and language requirements. This table ensures comprehensive tracking of case progress, decisions, and associated conditions."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## M8: bronze_ bail_ac _appealcatagory_catagory

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT
# MAGIC
# MAGIC     ap.CaseNo,
# MAGIC     c.Description AS CategoryDescription,
# MAGIC     c.Flag,
# MAGIC     c.Priority
# MAGIC FROM [ARIAREPORTS].[dbo].[AppealCategory] ap
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Category] c
# MAGIC ON ap.CategoryId = c.CategoryId

# COMMAND ----------

@dlt.table(
    name="bronze_bail_ac_appealcategory_category",
    comment="ARIA Migration Archive Bails Appeal Category cases bronze table",
    partition_cols=["CaseNo"],
    path=f"{bronze_mnt}/bronze_bail_ac_appealcategory_category"
)
def bronze_bail_ac_appealcategory_category():
    df = (
        dlt.read("raw_appeal_category").alias("ap")
        .join(dlt.read("raw_category").alias("c"), col("ap.CategoryId") == col("c.CategoryId"), "left_outer")
        .select(
            # AppealCategory fields
            col("ap.CaseNo"),
            # Category fields
            col("c.Description").alias("CategoryDescription"),
            col("c.Flag"),
            col("c.Priority"),
        )
    )

    ## Create and save audit log for this table
    table_name = "bronze_bail_ac_appealcategory_category"
    stage_name = "bronze_stage"

    description = "The bronze_bail_ac_appealcategory_category table records appeal categories and their associated details for bail cases. It integrates data from appeal category records and general category descriptions, allowing for a structured view of case classifications. Key attributes include case number, category description, and category flag, enabling efficient tracking of case types and appeal classifications for auditing purposes."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df.orderBy("Priority")


# COMMAND ----------

# MAGIC %md
# MAGIC ## CaseSurety Query

# COMMAND ----------

@dlt.table(
    name="bronze_case_surety_query",
    comment="ARIA Migration Archive Case Surety cases bronze table",
    path=f"{bronze_mnt}/bronze_case_surety_query"
)
def bronze_case_surety_query():
    df = (
        dlt.read("raw_case_surety").alias("cs")
        .select(
            # CaseSurety fields
            col("SuretyId"),
            col("CaseNo"),
            col("Name").alias("CaseSuretyName"),
            col("Forenames").alias("CaseSuretyForenames"),
            col("Title").alias("CaseSuretyTitle"),
            col("Address1").alias("CaseSuretyAddress1"),
            col("Address2").alias("CaseSuretyAddress2"),
            col("Address3").alias("CaseSuretyAddress3"),
            col("Address4").alias("CaseSuretyAddress4"),
            col("Address5").alias("CaseSuretyAddress5"),
            col("Postcode").alias("CaseSuretyPostcode"),
            col("Recognizance").alias("AmountOfFinancialCondition"),
            col("Security").alias("AmountOfTotalSecurity"),
            col("DateLodged").alias("CaseSuretyDateLodged"),
            col("Location"),
            col("Solicitor"),
            col("Email").alias("CaseSuretyEmail"),
            col("Telephone").alias("CaseSuretyTelephone")
        )
    )

    ## Create and save audit log for this table
    table_name = "bronze_case_surety_query"
    stage_name = "bronze_stage"

    description = "The bronze_case_surety_query table captures surety details related to bail cases, providing insights into individuals or entities offering financial guarantees for an appellant's release. It includes surety identity, contact details, financial commitments (recognizance and security amounts), lodging dates, solicitor involvement, and communication details. This table ensures comprehensive tracking of surety obligations and case associations, supporting case management and auditing processes."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df




# COMMAND ----------

# MAGIC %md
# MAGIC ## Judicial Requirement table

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT
# MAGIC
# MAGIC     ca.CaseNo,
# MAGIC     ca.Required,
# MAGIC     adj.Surname As JudgeSurname,
# MAGIC     adj.Forenames AS JudgeForenames,
# MAGIC     adj,Title AS JudgeTitle
# MAGIC
# MAGIC FROM dbo.CaseAdjudicator ca
# MAGIC INNER JOIN dbo.Adjudicator adj ON ca.Adjudicator = adj.AdjudicatorId
# MAGIC

# COMMAND ----------

@dlt.table(
    name="judicial_requirement",
    comment="ARIA Migration Archive Judicial Requirements cases table",
    path=f"{bronze_mnt}/judicial_requirement"
)

def judicial_requirement():
    df = (
        dlt.read("raw_case_adjudicator").alias("ca")
        .join(dlt.read("raw_adjudicator").alias("adj"), col("ca.AdjudicatorId") == col("adj.AdjudicatorId"), "inner")
        .select(
                col("ca.CaseNo"),
                col("ca.required"),
                col("adj.Surname").alias("JudgeSurname"),
                col("adj.Forenames").alias("JudgeForenames"),
                col("adj.Title").alias("JudgeTitle")
        )
    )
    ## Create and save audit log for this table
    table_name = "judicial_requirement"
    stage_name = "bronze_stage"

    description = "The judicial_requirement table tracks judicial assignments and requirements for bail cases. It links case adjudicators to their assigned judges, providing a structured view of judicial oversight in legal proceedings. Key attributes include case number, adjudicator requirements, and judge details (surname, forenames, and title), ensuring visibility into judicial involvement and case management."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linked Cases Cost Award

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT
# MAGIC
# MAGIC     ca.CostAwardId,
# MAGIC     l.LinkNo,
# MAGIC     ca.CaseNo,
# MAGIC     a.Name,
# MAGIC     a.Forenames,
# MAGIC     a.Title,
# MAGIC     ca.DateOfApplication,
# MAGIC     ca.TypeOfCostAward,
# MAGIC     ca.ApplyingParty,
# MAGIC     ca.PayingParty,
# MAGIC     ca.MindedToAward,
# MAGIC     ca.ObjectionToMindedToAward,
# MAGIC     ca.CostsAwardDecision,
# MAGIC     ca.DateOfDecision,
# MAGIC     ca.CostsAmount,
# MAGIC     ca.OutcomeOfAppeal,
# MAGIC     ca.AppealStage,
# MAGIC     cs.Description AS AppealStageDescription
# MAGIC
# MAGIC     FROM [ARIAREPORTS].[dbo].[CostAward] ca
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Link] l
# MAGIC     ON ca.CaseNo = l.CaseNo
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CaseAppellant] cap
# MAGIC     ON ca.CaseNo = cap.CaseNo
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Appellant] a
# MAGIC     ON cap.AppellantId = a.AppellantId
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CaseStatus] cs
# MAGIC     ON ca.AppealStage = cs.CaseStatusId

# COMMAND ----------

@dlt.table(
    name="linked_cases_cost_award",
    comment="Linked Cases Cost Award cases table",
    path=f"{bronze_mnt}/linked_cases_cost_award"
)
def linked_cases_cost_award():
    df =  (
        dlt.read("raw_cost_award").alias("ca")
        .join(dlt.read("raw_link").alias("l"), col("ca.CaseNo") == col("l.CaseNo"), "left_outer")
        .join(dlt.read("raw_case_appellant").alias("cap"), col("ca.CaseNo") == col("cap.CaseNo"), "left_outer")
        .join(dlt.read("raw_appellant").alias("a"), col("cap.AppellantId") == col("a.AppellantId"), "left_outer")
        .join(dlt.read("raw_case_status").alias("cs"), col("ca.AppealStage") == col("cs.CaseStatusId"), "left_outer")
        .select(
            col("ca.CostAwardId").alias("CostAwardId"),
            col("l.LinkNo").alias("LinkNo"),
            col("ca.CaseNo").alias("CaseNo"),
            col("a.Name").alias("Name"),
            col("a.Forenames").alias("Forenames"),
            col("a.Title").alias("Title"),
            col("ca.DateOfApplication").alias("DateOfApplication"),
            col("ca.TypeOfCostAward").alias("TypeOfCostAward"),
            col("ca.ApplyingParty").alias("ApplyingParty"),
            col("ca.PayingParty").alias("PayingParty"),
            col("ca.MindedToAward").alias("MindedToAward"),
            col("ca.ObjectionToMindedToAward").alias("ObjectionToMindedToAward"),
            col("ca.CostsAwardDecision").alias("CostsAwardDecision"),
            col("ca.DateOfDecision").alias("DateOfDecision"),
            col("ca.CostsAmount").alias("CostsAmount"),
            col("ca.OutcomeOfAppeal").alias("OutcomeOfAppeal"),
            col("ca.AppealStage").alias("AppealStage"),
            col("cs.Description").alias("AppealStageDescription")
        )
    )

    ## Create and save audit log for this table
    table_name = "linked_cases_cost_award"
    stage_name = "bronze_stage"

    description = "The linked_cases_cost_award table consolidates cost awards and their associations with linked cases, appellants, and appeal stages. It provides a structured overview of financial awards in legal proceedings, linking them to case numbers, appellants, appeal stages, and case statuses. Key attributes include cost award details (application date, type, applying and paying parties, decision outcomes, and amounts), linked case numbers, appellant details (name, forenames, title), and appeal stage descriptions, ensuring a comprehensive record of financial decisions within bail cases."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Segmentation tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normal Bails

# COMMAND ----------

from pyspark.sql import functions as F

@dlt.table(
    name="silver_normal_bail",
    comment="Silver Normal Bail cases table",
    path=f"{silver_mnt}/silver_normal_bail"
)
def silver_normal_bail():
    # Read the necessary raw data
    appeal_case = dlt.read("raw_appeal_cases").alias("ac")
    status = dlt.read("raw_status").alias("t")
    file_location = dlt.read("raw_file_location").alias("fl")
    history = dlt.read("raw_history").alias("h")

    # Create a subquery to get the max StatusId for each CaseNo
    max_status_subquery = (
        status
        .withColumn("status_value", F.when(F.isnull(F.col("CaseStatus")), -1).otherwise(F.col("CaseStatus"))) # new column with the status value if null setting it to -1
        .filter(F.col("status_value") != 17) # filter out status value of 17
        .groupBy("CaseNo")  # group by case No
        .agg(F.max("StatusId").alias("max_ID"))
    )
    max_status_subquery = max_status_subquery.select("CaseNo", "max_ID").alias("s")

    # Join the AppealCase to sub Query then status table then file location then history
    result = (
        appeal_case
        .join(max_status_subquery, F.col("ac.CaseNo") == F.col("s.CaseNo"), "left_outer")  
        .join(status, (F.col("t.CaseNo") == F.col("s.CaseNo")) & 
                (F.col("s.max_ID") == F.col("t.StatusId")), "left_outer")  
        .join(file_location, F.col("fl.CaseNo") == F.col("ac.CaseNo"), "left_outer")
        .join(history, F.col("h.CaseNo") == F.col("ac.CaseNo"), "left_outer")
    )
    result_filtered = result.filter(
        (F.col("ac.CaseType") == 2) &
        (F.col("fl.DeptId") != 519) &
        (
            (~F.col("fl.Note").like("%destroyed%")) &
            (~F.col("fl.Note").like("%detroyed%")) &
            (~F.col("fl.Note").like("%destoyed%")) & 
            (~F.col("fl.Note").like("%distroyed%")) |
            (F.col("fl.Note").isNull())
        ))

    result_with_case = result_filtered.withColumn(
        "case_result",
        F.when(F.col("h.Comment").like("%indefinite retention%"), 'Legal Hold')
        .when(F.col("h.Comment").like("%indefinate retention%"), 'Legal Hold')
        .when(F.date_add(F.col("t.DecisionDate"), 2 * 365) < F.current_date(), 'Destroy')
        .otherwise('Archive')
    )
    final_result = result_with_case.filter(F.col("case_result") == 'Archive')

    final_grouped_result = final_result.groupBy(
        "ac.CaseNo"
    ).agg(
        F.count(F.lit(1)).alias("case_count")
        
    )
    final_normal_bail = final_grouped_result.orderBy("CaseNo", ascending=False).cache()


    df = final_normal_bail.select("ac.CaseNo",
                                    lit("Normal Bail").alias("BaseBailType"))
    

    ## Create and save audit log for this table
    table_name = "silver_normal_bail"
    stage_name = "segmentation_stage"

    description = "The silver_normal_bail table processes and filters normal bail cases from raw datasets, refining the data for analytical and reporting purposes. It identifies archivable cases by determining the most recent status record for each case, filtering out specific file locations, and applying conditions based on case history comments and decision dates. The table ensures that only archivable normal bail cases are retained, providing structured insights into case numbers and bail type classifications."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Legal hold normal bail

# COMMAND ----------


@dlt.table(
    name="silver_legal_hold_normal_bail",
    comment="Silver table for legal hold normal bail cases",
    path=f"{silver_mnt}/silver_legal_hold_normal_bail"
)
def silver_legal_hold_normal_bail():
    # Read the necessary raw data
    appeal_case = dlt.read("raw_appeal_cases").alias("ac")
    file_location = dlt.read("raw_file_location").alias("fl")
    history = dlt.read("raw_history").alias("h")

    # Filter and join the data according to the provided SQL logic
    result = (
        appeal_case.alias("ac")
        .join(file_location.alias("fl"), F.col("ac.CaseNo") == F.col("fl.CaseNo"), "left_outer")
        .join(history.alias("h"), F.col("h.CaseNo") == F.col("ac.CaseNo"), "left_outer")
        .filter(
            (col("ac.CaseType") == '2') &
            (col("fl.DeptId") != 519) &
            (
                col("h.Comment").like('%indefinite retention%') |
                col("h.Comment").like('%indefinate retention%')
            )
        )
    )

    # Select CaseNo, group by, and aggregate the results
    final_result = (
        result.select(F.col("ac.CaseNo"))
        .groupBy(F.col("ac.CaseNo"))
        .agg(F.count("*").alias("count"))  
        .orderBy(F.col("ac.CaseNo"))
    )

    df = final_result.select("CaseNo",
                               lit("BailLegalHold").alias("BaseBailType"))  
    

    ## Create and save audit log for this table
    table_name = "silver_legal_hold_normal_bail"
    stage_name = "segmentation_stage"

    description = "The silver_legal_hold_normal_bail table identifies normal bail cases under legal hold by analyzing case history and file location data. It filters cases where indefinite retention is indicated in the history comments while excluding specific file locations. The table ensures that only cases meeting the legal hold criteria are retained, providing a structured dataset with case numbers and a classification of 'BailLegalHold' for tracking and auditing purposes."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Scottish Bails holding funds

# COMMAND ----------

@dlt.table(name="silver_scottish_bails_funds",
           comment="Silver table for Scottish Bails Funds cases",
           path=f"{silver_mnt}/silver_scottish_bails_funds")
def silver_scottish_bails_funds():
    df = spark.read.format("csv").option("header", "true").load(f"{external_mnt}/Scottish__Bailsfile.csv").select(
        col("Caseno/ Bail Ref no").alias("CaseNo"),
        lit("ScottishBailsFunds").alias("BaseBailType")
        )
    
    ## Create and save audit log for this table
    table_name = "silver_scottish_bails_funds"
    stage_name = "segmentation_stage"

    description = "The silver_scottish_bails_funds table processes Scottish Bails Funds cases from an external CSV file. It extracts case numbers (or bail reference numbers) and categorizes them under the ScottishBailsFunds bail type. This table ensures structured integration of external Scottish bail data into the silver layer for further analysis and reporting."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combined Segmentaiton query

# COMMAND ----------

@dlt.table(name="silver_bail_combined_segmentation_nb_lhnb_sbhf",
           comment="Silver table for combined segmentation Normal bails, legal hold normal bail cases and Scottish Bails Holding Funds",
           path=f"{silver_mnt}/silver_bail_combined_segmentation_nb_lhnb_sbhf")
def silver_bail_combined_segmentation_nb_lhnb_sbhf():
    nb = dlt.read("silver_normal_bail").select("CaseNo", "BaseBailType")
    lhnb = dlt.read("silver_legal_hold_normal_bail").select("CaseNo", "BaseBailType")
    sbhf = dlt.read("silver_scottish_bails_funds").select("CaseNo", "BaseBailType")
    df =  nb.union(lhnb).union(sbhf).select("CaseNo","BaseBailType")


    ## Create and save audit log for this table
    table_name = "silver_bail_combined_segmentation_nb_lhnb_sbhf"
    stage_name = "segmentation_stage"

    description = "The silver_bail_combined_segmentation_nb_lhnb_sbhf table consolidates normal bail cases, legal hold normal bail cases, and Scottish Bails Holding Funds into a single dataset. It integrates data from the silver_normal_bail, silver_legal_hold_normal_bail, and silver_scottish_bails_funds tables, ensuring a unified view of bail case classifications. Key attributes include case numbers and corresponding bail types, providing a structured dataset for further analysis and decision-making."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## M1: silver_bail_m1_case_details

# COMMAND ----------

@dlt.table(name="silver_bail_m1_case_details",
           comment="ARIA Migration Archive Bails m1 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m1")
def silver_m1():
    m1_df = dlt.read("bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang").alias("m1")
    
    segmentation_df = dlt.read("silver_bail_combined_segmentation_nb_lhnb_sbhf").alias("bs")
    
    joined_df = m1_df.join(segmentation_df.alias("bs"), col("m1.CaseNo") == col("bs.CaseNo"), "inner")

    selected_columns = [col(c) for c in m1_df.columns if c!= "CaseNo"]
    
    df = joined_df.select("m1.CaseNo", *selected_columns,
                        when(col("BailType") == 1,"Bail")
                        .when(col("BailType") == 2,"Scottish Bail")
                        .otherwise("Other").alias("BailTypeDesc"),
                        when(col("CourtPreference") == 1,"All Male,")
                        .when(col("CourtPreference") == 2,"All Female,")
                        .otherwise("Other").alias("CourtPreferenceDesc"),
                        when(col("Interpreter") == 1,"Yes")
                        .when(col("Interpreter") == 2,"No").otherwise("Unknown").alias("InterpreterDesc"),
                        when(col("TypeOfCostAward") == 1,"Fee Costs")
                        .when(col("TypeOfCostAward") == 2,"Wasted Costs")
                        .when(col("TypeOfCostAward") == 3,"Unreasonable Behaviour")
                        .when(col("TypeOfCostAward") == 4,"General Costs")
                        .otherwise("Unknown").alias("TypeOfCostAwardDesc"),
                        when(col("ApplyingParty") == 1,"Appellant")
                        .when(col("ApplyingParty") == 2,"Respondent")
                        .otherwise("Unknown").alias("ApplyingPartyDesc"),
                        when(col("PayingParty") == 1,"Appellant")
                        .when(col("PayingParty") == 2,"Respondent")
                        .when(col("PayingParty") == 3,"Surety/cautioner")
                        .when(col("PayingParty") == 4,"Interested Party")
                        .when(col("PayingParty") == 5,"Appellant Rep")
                        .when(col("PayingParty") == 6,"Respondent Rep")
                        .otherwise("Unknown").alias("PayingPartyDesc"),
                        when(col("CostsAwardDecision") == 1,"Granted")
                        .when(col("CostsAwardDecision") == 2,"Refused")
                        .when(col("CostsAwardDecision") == 3,"Interim field")
                        .otherwise("Unknown").alias("CostsAwardDecisionDesc"),
                        "BaseBailType"
                            
    )
    ## Create and save audit log for this table
    table_name = "silver_bail_m1_case_details"
    stage_name = "silver_stage"

    description = "The silver_bail_m1_case_details table refines and enriches bail case details by integrating segmentation data and bronze-level case records. It links bail cases with their segmentation type (Normal Bail, Legal Hold, or Scottish Bail) while applying meaningful descriptions to key categorical fields, such as Bail Type, Court Preference, Interpreter Requirement, Cost Award Types, and Paying Party Details. The result is a well-structured dataset that enables clear classification and analysis of bail cases based on their characteristics and financial/legal implications."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## M2: silver_bail_m2_case_appellant

# COMMAND ----------

@dlt.table(name="silver_bail_m2_case_appellant",
           comment="ARIA Migration Archive Bails m2 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m2_case_appellant")
def silver_m2():
    m2_df = dlt.read("bronze_bail_ac_ca_apt_country_detc").alias("m2")
    segmentation_df = dlt.read("silver_bail_combined_segmentation_nb_lhnb_sbhf").alias("bs")

    joined_df = m2_df.join(segmentation_df.alias("bs"), col("m2.CaseNo") == col("bs.CaseNo"), "inner")

    selected_columns = [col(c) for c in m2_df.columns if c != "CaseNo"]

    df = joined_df.select("m2.CaseNo", *selected_columns,
                        when(col("AppellantDetained") == 1,"HMP")
                        .when(col("AppellantDetained") == 2,"IRC")
                        .when(col("AppellantDetained") == 3,"No")
                        .when(col("AppellantDetained") == 4,"Other")
                        .otherwise("Unknown").alias("AppellantDetainedDesc"),"BaseBailType")
    
    ## Create and save audit log for this table
    table_name = "silver_bail_m2_case_appellant"
    stage_name = "silver_stage"

    description = "The silver_bail_m2_case_appellant table refines and enriches appellant-related details in bail cases by integrating segmentation data and bronze-level case appellant records. It links appellants to their bail case type while applying meaningful descriptions to the detention status of appellants. The result is a structured dataset that facilitates better analysis of appellant demographics, detention conditions, and case classifications within the bail system."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## M3: silver_bail_m3_hearing_details
# MAGIC

# COMMAND ----------


m3_grouped_cols = [
"CaseNo", 
"StatusId", 
"Outcome", 
"CaseListTimeEstimate", 
"CaseListStartTime", 
"HearingTypeDesc", 
"ListName", 
"HearingDate", 
"ListStartTime", 
"ListTypeDesc", 
"ListType", 
"CourtName", 
"HearingCentreDesc"
]



@dlt.table(name="silver_bail_m3_hearing_details", 
           comment="ARIA Migration Archive Bails m3 silver table", 
           partition_cols=["CaseNo"], 
           path=f"{silver_mnt}/silver_bail_m3")

def silver_m3():
    # 1. Read from the existing Hive table
    m3_df = dlt.read("bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj").alias("m3")
    

    segmentation_df = dlt.read("silver_bail_combined_segmentation_nb_lhnb_sbhf").alias("bs")
    joined_df = m3_df.join(segmentation_df.alias("bs"), on="CaseNo", how="inner")

    df = joined_df.drop("BaseBailType")


    ## Create and save audit log for this table
    table_name = "silver_bail_m3_hearing_details"
    stage_name = "silver_stage"

    description = "The silver_bail_m3_hearing_details table refines and consolidates hearing-related details for bail cases by grouping and aggregating key court and hearing attributes. It integrates bronze-level hearing records and bail segmentation data, providing structured insights into case status, hearing schedules, court locations, and adjudicator assignments. The table distinguishes between the presiding judge (JudgeFT) and court clerk/usher (CourtClerkUsher) based on their role in the proceedings. This dataset enables efficient tracking of hearing events, adjudicator assignments, and court proceedings within bail case management."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df



# COMMAND ----------

# m3_df = spark.read.table("hive_metastore.aria_bails.bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj")

# m3_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## M4: silver_bail_m4_bf_diary

# COMMAND ----------

@dlt.table(name="silver_bail_m4_bf_diary",
           comment="ARIA Migration Archive Bails m4 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m4_bf_diary")
def silver_m4():
    m4_df = dlt.read("bronze_bail_ac_bfdiary_bftype").alias("m4")
    segmentation_df = dlt.read("silver_bail_combined_segmentation_nb_lhnb_sbhf").alias("bs")
    joined_df = m4_df.join(segmentation_df.alias("bs"), col("m4.CaseNo") == col("bs.CaseNo"), "inner")
    selected_columns = [col(c) for c in m4_df.columns if c != "CaseNo"]
    df = joined_df.select("m4.CaseNo", *selected_columns)



    ## Create and save audit log for this table
    table_name = "silver_bail_m4_bf_diary"
    stage_name = "silver_stage"

    description = "The silver_bail_m4_bf_diary table refines and consolidates BF (Bring Forward) diary records related to bail cases. It integrates bronze-level BF diary data with bail segmentation classifications, ensuring that each case is categorized under Normal Bail, Legal Hold, or Scottish Bail Funds. This table provides structured insights into BF dates, diary entries, completion statuses, and BF types, enabling effective tracking of pending actions and case follow-ups within the bail process."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## M5: silver_bail_m5_history

# COMMAND ----------

@dlt.table(name="silver_bail_m5_history",
           comment="ARIA Migration Archive Bails m5 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m5_history")
def silver_m5():
    m5_df = dlt.read("bronze_bail_ac_history_users").alias("m5")
    segmentation_df = dlt.read("silver_bail_combined_segmentation_nb_lhnb_sbhf").alias("bs")
    joined_df = m5_df.join(segmentation_df.alias("bs"), col("m5.CaseNo") == col("bs.CaseNo"), "inner")
    selected_columns = [col(c) for c in m5_df.columns if c != "CaseNo"]
    df = joined_df.select("m5.CaseNo", *selected_columns)

    ## Create and save audit log for this table
    table_name = "silver_bail_m5_history"
    stage_name = "silver_stage"

    description = "The silver_bail_m5_history table consolidates historical actions and user activity related to bail cases. It integrates bronze-level case history records with bail segmentation classifications, ensuring that each case is categorized under Normal Bail and Legal Hold. Key attributes include historical changes, timestamps, action types, user activity, and related case updates, providing a transparent and structured audit trail for case modifications and decision-making processes."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## M6: silver_bail_m6_link

# COMMAND ----------

@dlt.table(name="silver_bail_m6_link",
           comment="ARIA Migration Archive Bails m6 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m6_link")
def silver_m6():
    m6_df = dlt.read("bronze_bail_ac_link_linkdetail").alias("m6")
    segmentation_df = dlt.read("silver_bail_combined_segmentation_nb_lhnb_sbhf").alias("bs")
    joined_df = m6_df.join(segmentation_df.alias("bs"), col("m6.CaseNo") == col("bs.CaseNo"), "inner")
    selected_columns = [col(c) for c in m6_df.columns if c != "CaseNo"]

    df = joined_df.select("m6.CaseNo", "LinkNo", "LinkDetailComment", concat_ws(" ",
        col("Title"),col("Forenames"),col("Name")).alias("FullName")
    )


    ## Create and save audit log for this table
    table_name = "silver_bail_m6_link"
    stage_name = "silver_stage"

    description = "The silver_bail_m6_link table consolidates case linkages and related details for bail cases. It integrates bronze-level case link data with bail segmentation classifications, ensuring that each case is categorized under Normal Bail or Legal Hold. Key attributes include case number, link number, link detail comments, and a concatenated full name (title, forenames, and name), providing a structured overview of case relationships and associated appellants for efficient case tracking and analysis."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## M7: silver_bail_m7_status

# COMMAND ----------

@dlt.table(name="silver_bail_m7_status",
           comment="ARIA Migration Archive Bails m7 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m7_status")
def silver_m7():
    m7_df = dlt.read("bronze_bail_status_sc_ra_cs").alias("m7")

    m7_ref_df = m7_df.select("*",
                        when(col("BailConditions") == 1,"Yes")
                        .when(col("BailConditions") == 2,"No")
                        .otherwise("Unknown").alias("BailConditionsDesc"),
                        when(col("InterpreterRequired") == 0 ,"Zero")
                        .when(col("InterpreterRequired") == 1 ,"One")
                        .when(col("InterpreterRequired") == 2 ,"Two")
                        .alias("InterpreterRequiredDesc"),
                        when(col("ResidenceOrder") == 1,"Yes")
                        .when(col("ResidenceOrder") == 2,"No")
                        .otherwise("Unknown").alias("ResidenceOrderDesc"),
                        when(col("ReportingOrder") == 1,"Yes")
                        .when(col("ReportingOrder") == 2,"No")
                        .otherwise("Unknown").alias("ReportingOrderDesc"),
                        when(col("BailedTimePlace") == 1,"Yes")
                        .when(col("BailedTimePlace") == 2,"No")
                        .otherwise("Unknown").alias("BailedTimePlaceDesc"),
                        when(col("BaileddateHearing") == 1,"Yes")
                        .when(col("BaileddateHearing") == 2,"No")
                        .otherwise("Unknown").alias("BaileddateHearingDesc"),
                        when(col("StatusParty") == 1,"Appellant")
                        .when(col("StatusParty") == 2,"Respondent")
                        .otherwise("Unknown").alias("StatusPartyDesc"),

    )

    segmentation_df = dlt.read("silver_bail_combined_segmentation_nb_lhnb_sbhf").alias("bs")
    joined_df = m7_ref_df.join(segmentation_df.alias("bs"), col("m7.CaseNo") == col("bs.CaseNo"), "inner")
    selected_columns = [col(c) for c in m7_ref_df.columns if c != "CaseNo"]
    df = joined_df.select("m7.CaseNo", *selected_columns)


    ## Create and save audit log for this table
    table_name = "silver_bail_m7_status"
    stage_name = "silver_stage"

    description = "The silver_bail_m7_status table refines bail status details by integrating bronze-level status records with bail segmentation classifications, ensuring each case is categorized under Normal Bail or Legal Hold. It enhances categorical fields by providing descriptive labels for bail conditions, interpreter requirements, residence orders, reporting orders, and status parties. This structured dataset facilitates clearer analysis of case statuses, bail conditions, and legal requirements for improved decision-making and case management."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## M8: silver_bail_m8

# COMMAND ----------

@dlt.table(name="silver_bail_m8",
           comment="ARIA Migration Archive Bails m8 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m8")
def silver_m8():
    m8_df = dlt.read("bronze_bail_ac_appealcategory_category").alias("m8")
    segmentation_df = dlt.read("silver_bail_combined_segmentation_nb_lhnb_sbhf").alias("bs")
    joined_df = m8_df.join(segmentation_df.alias("bs"), col("m8.CaseNo") == col("bs.CaseNo"), "inner")
    selected_columns = [col(c) for c in m8_df.columns if c != "CaseNo"]
    df = joined_df.select("m8.CaseNo", *selected_columns)


    ## Create and save audit log for this table
    table_name = "silver_bail_m8"
    stage_name = "silver_stage"

    description = "The silver_bail_m8 table refines appeal category details by integrating bronze-level appeal category records with bail segmentation classifications, ensuring each case is categorized under Normal Bail or Legal Hold. It provides structured insights into case numbers, appeal categories, and associated classifications, enabling clearer tracking and analysis of case types and their legal classifications within the bail system."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Meta Data table

# COMMAND ----------

@dlt.table(
  name="silver_bail_meta_data",
  comment="ARIA Migration Archive Bails meta data table",
  path=f"{silver_mnt}/silver_bail_meta_data"
)
def silver_meta_data():
  m1_df = dlt.read("silver_bail_m1_case_details").alias("m1")
  m2_df = dlt.read("silver_bail_m2_case_appellant").alias("m2")
  base_df = (
        m1_df.join(m2_df, on="CaseNo", how="left")
             .select(
                 F.col("CaseNo").alias("client_identifier"),
                 date_format(
                   coalesce(F.col("DateOfDecision"),current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_date"),
                 date_format(
                   coalesce(F.col("DateOfDecision"),current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("recordDate"),
                 F.lit("GBR").alias("region"),
                 F.lit("ARIA").alias("publisher"),
                 F.when(F.col("m1.BaseBailType") == "ScottishBailsFunds", "ARIASB")
                  .otherwise("ARIAB")
                  .alias("record_class"),
                 F.lit("IA_Tribunal").alias("entitlement_tag"),
                 F.col("HoRef").alias("bf_001"),
                 F.col("Forename").alias("bf_002"),
                 F.col("Surname").alias("bf_003"),
                 date_format(coalesce(F.col("AppellantBirthDate"),current_timestamp()), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("bf_004"),
                 F.col("PortReference").alias("bf_005"),
                 F.col("RepPostcode").alias("bf_006"),
                 F.when(F.col("m1.BaseBailType") == "BailLegalHold", "Yes").otherwise("No").alias("bf_007")
             )
    )
    
    
  # Join the batchid mapping back onto the base DataFrame
  final_df = base_df
    


  ## Create and save audit log for this table
  table_name = "silver_bail_meta_data"
  stage_name = "silver_stage"

  description = "The silver_bail_meta_data table consolidates key metadata from bail case details and appellant records, creating a structured dataset for tracking and categorizing bail cases. It assigns client identifiers, event dates, region, publisher, and record classifications while standardizing date formats and ensuring each case is assigned to a batch ID for processing. The table provides essential attributes to cteate the manifest file."

  unique_identifier_desc = "client_identifier"

  create_audit_df(final_df,unique_identifier_desc,table_name,stage_name,description)

  return final_df

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Output Code

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code start: import template

# COMMAND ----------

# MAGIC %md
# MAGIC ### Case Status Mappings

# COMMAND ----------

#
# Case status Mapping
case_status_mappings = {
    11: {  # Scottish Payment Liability
        "{{ScottishPaymentLiabilityStatusOfBail}}": "CaseStatusDescription",
        "{{ScottishPaymentLiabilityDateOfOrder}}": "DateReceived",
        "{{ScottishPaymentLiabilityDateOfHearing}}": "Keydate",
        "{{ScottishPaymentLiabilityFC}}": "FC",
        "{{ScottishPaymentLiabilityInterpreterRequired}}": "InterpreterRequiredDesc",
        "{{ScottishPaymentLiabilityDetailsOfOrder}}": "StatusNotes1",
        "{{ScottishPaymentLiabilityDatePaymentInstructions}}": "MiscDate1",
        "{{ScottishPaymentLiabilityDateChequesIssued}}": "MiscDate2",
        "{{ScottishPaymentLiabilityVideoLink}}": "VideoLink",
        "{{ScottishPaymentLiabilityDateOfDecision}}": "DecisionDate",
        "{{ScottishPaymentLiabilityOutcome}}": "OutcomeDescription",
        "{{livesAndSleepsAt}}": "LivesAndSleepsAt",
        "{{appearBefore}}":"AppearBefore",
        "{{reportTo}}": "ReportTo",
        "{{tagging}}":"StatusBailConditionTagging",
        "{{workAndStudyRestriction}}": "WorkAndStudyRestriction",
        "{{other}}": "OtherCondition",
        "{{outcomeReasons}}": "OutcomeReasons",
        "{{adjDateOfApplication}}": "DateReceived",
        "{{adjDateOfHearing}}": "MiscDate1",
        "{{adjPartyMakingApp}}":"StatusPartyDesc",
        "{{adjDirections}}":"StatusNotes1",
        "{{adjDateOfDecision}}":"DecisionDate",
        "{{adjOutcome}}":"OutcomeDescription",
        "{{adjdatePartiesNotified}}":"DecisionSentToHODate",
        ## hearing tab
        "{{HearingCentre}}":"HearingTypeDesc",
        "{{Court}}":"CourtName",
        "{{HearingDate}}":"HearingDate",
        "{{ListName}}":"ListName",
        "{{ListType}}":"ListType",
        "{{HearingType}}":"HearingTypeDesc",
        "{{ListStartTime}}":"ListStartTime",
        "{{JudgeFT}}":"JudgeFT",
        "{{CourtClerkUsher}}":"CourtClerkUsher",
        "{{StartTime}}":"CaseListStartTime",
        "{{EstDuration}}":"CaseListTimeEstimate",
        "{{Outcome}}": "Outcome",
        "{{PrimaryLanguage}}": "Language",
        "{{outcome}}": "OutcomeDescription"




    },
    4: {  # Bail Application
        "{{BailApplicationStatusOfBail}}": "CaseStatusDescription",
        "{{BailApplicationDateOfApplication}}": "DateReceived",
        "{{BailApplicationDateOfHearing}}": "DecisionDate",
        "{{BailApplicationFC}}": "FC",
        "{{BailApplicationInterpreterRequired}}": "InterpreterRequiredDesc",
        "{{BailApplicationDateOfOrder}}": "MiscDate2",
        "{{BailApplicationTotalAmountOfFinancialCondition}}": "TotalAmountOfFinancialCondition",
        "{{BailApplicationBailCondition}}": "BailConditionsDesc",
        "{{BailApplicationTotalSecurity}}": "TotalSecurity",
        "{{BailApplicationDateDischarged}}": "MiscDate1",
        "{{BailApplicationRemovalDate}}": "MiscDate3",
        "{{BailApplicationVideoLink}}": "VideoLink",
        "{{BailApplicationResidenceOrderMade}}": "ResidenceOrderDesc",
        "{{BailApplicationReportingOrderMade}}": "ReportingOrderDesc",
        "{{BailApplicationBailedTimePlace}}": "BailedTimePlaceDesc",
        "{{BailApplicationBailedDateOfHearing}}": "BaileddateHearing",
        "{{BailApplicationDateOfDecision}}": "DecisionDate",
        "{{BailApplicationOutcome}}": "OutcomeDescription",
        "{{BailApplicationHOConsentDate}}": "DecisionSentToHODate",
        "{{livesAndSleepsAt}}": "LivesAndSleepsAt",
        "{{appearBefore}}":"AppearBefore",
        "{{reportTo}}": "ReportTo",
        "{{tagging}}":"StatusBailConditionTagging",
        "{{workAndStudyRestriction}}": "WorkAndStudyRestriction",
        "{{other}}": "OtherCondition",
        "{{outcomeReasons}}": "OutcomeReasons",
        ## adjournment
        "{{adjDateOfApplication}}": "DateReceived",
        "{{adjDateOfHearing}}": "MiscDate1",
        "{{adjPartyMakingApp}}":"StatusParty",
        "{{adjDirections}}":"StatusNotes1",
        "{{adjDateOfDecision}}":"DecisionDate",
        "{{adjOutcome}}":"OutcomeDescription",
        "{{adjdatePartiesNotified}}":"DecisionSentToHODate",
                ## hearing tab
        "{{HearingCentre}}":"HearingTypeDesc",
        "{{Court}}":"CourtName",
        "{{HearingDate}}":"HearingDate",
        "{{ListName}}":"ListName",
        "{{ListType}}":"ListType",
        "{{HearingType}}":"HearingTypeDesc",
        "{{ListStartTime}}":"ListStartTime",
        "{{JudgeFT}}":"JudgeFT",
        "{{CourtClerkUsher}}":"CourtClerkUsher",
        "{{StartTime}}":"CaseListStartTime",
        "{{EstDuration}}":"CaseListTimeEstimate",
        "{{Outcome}}": "Outcome",
        "{{PrimaryLanguage}}": "Language",
        "{{outcome}}": "OutcomeDescription"
    },
    6: {  # Payment Liability
        "{{PaymentLiabilityStatusOfBail}}": "CaseStatusDescription",
        "{{PaymentLiabilityDateOfOrder}}": "DateReceived",
        "{{PaymentLiabilityDateOfHearing}}": "DecisionDate",
        "{{PaymentLiabilityFC}}": "FC",
        "{{PaymentLiabilityInterpreterRequired}}": "InterpreterRequiredDesc",
        "{{PaymentLiabilityDetailsOfOrder}}": "StatusNotes1",
        "{{PaymentLiabilityInstalmentDetails}}": "TotalSecurity",
        "{{PaymentLiabilityTotalAmount}}": "TotalSecurity",
        "{{PaymentLiabilityVideoLink}}": "VideoLink",
        "{{PaymentLiabilityContact}}": "ContactStatus",
        "{{PaymentLiabilityCollectionOffice}}": "SCCourtName",
        "{{PaymentLiabilityPhone}}": "SCTelephone",
        "{{PaymentLiabilityAddressLine1}}": "SCAddress1",
        "{{PaymentLiabilityAddressLine2}}": "SCAddress2",
        "{{PaymentLiabilityAddressLine3}}": "SCAddress3",
        "{{PaymentLiabilityAddressLine4}}": "SCAddress4",
        "{{PaymentLiabilityAddressLine5}}": "SCAddress5",
        "{{PaymentLiabilityPostcode}}": "SCPostcode",
        "{{PaymentLiabilityNotes}}": "Notes2",
        "{{PaymentLiabilityDateOfDecision}}": "DecisionDate",
        "{{PaymentLiabilityOutcome}}": "OutcomeDescription",
        "{{livesAndSleepsAt}}": "LivesAndSleepsAt",
        "{{appearBefore}}":"AppearBefore",
        "{{reportTo}}": "ReportTo",
        "{{tagging}}":"StatusBailConditionTagging",
        "{{workAndStudyRestriction}}": "WorkAndStudyRestriction",
        "{{other}}": "OtherCondition",
        "{{outcomeReasons}}": "OutcomeReasons",
        ## adjournment
        "{{adjDateOfApplication}}": "DateReceived",
        "{{adjDateOfHearing}}": "MiscDate1",
        "{{adjPartyMakingApp}}":"StatusParty",
        "{{adjDirections}}":"StatusNotes1",
        "{{adjDateOfDecision}}":"DecisionDate",
        "{{adjOutcome}}":"OutcomeDescription",
        "{{adjdatePartiesNotified}}":"DecisionSentToHODate",
                ## hearing tab
        "{{HearingCentre}}":"HearingTypeDesc",
        "{{Court}}":"CourtName",
        "{{HearingDate}}":"HearingDate",
        "{{ListName}}":"ListName",
        "{{ListType}}":"ListType",
        "{{HearingType}}":"HearingTypeDesc",
        "{{ListStartTime}}":"ListStartTime",
        "{{JudgeFT}}":"JudgeFT",
        "{{CourtClerkUsher}}":"CourtClerkUsher",
        "{{StartTime}}":"CaseListStartTime",
        "{{EstDuration}}":"CaseListTimeEstimate",
        "{{Outcome}}": "Outcome",
        "{{PrimaryLanguage}}": "Language",
        "{{outcome}}": "OutcomeDescription"
    },
    8: {  # Lodgement
        "{{LodgementStatusOfBail}}": "CaseStatusDescription",
        "{{LodgementDateCautionLodged}}": "DateReceived",
        "{{LodgementAmountOfLodged}}": "TotalAmountOfFinancialCondition",
        "{{LodgementWhomToBeRepaid}}": "StatusNotes1",
        "{{livesAndSleepsAt}}": "LivesAndSleepsAt",
        "{{appearBefore}}":"AppearBefore",
        "{{reportTo}}": "ReportTo",
        "{{tagging}}":"StatusBailConditionTagging",
        "{{workAndStudyRestriction}}": "WorkAndStudyRestriction",
        "{{other}}": "OtherCondition",
        "{{outcomeReasons}}": "OutcomeReasons",
                ## hearing tab
        "{{HearingCentre}}":"HearingTypeDesc",
        "{{Court}}":"CourtName",
        "{{HearingDate}}":"HearingDate",
        "{{ListName}}":"ListName",
        "{{ListType}}":"ListType",
        "{{HearingType}}":"HearingTypeDesc",
        "{{ListStartTime}}":"ListStartTime",
        "{{JudgeFT}}":"JudgeFT",
        "{{CourtClerkUsher}}":"CourtClerkUsher",
        "{{StartTime}}":"CaseListStartTime",
        "{{EstDuration}}":"CaseListTimeEstimate",
        "{{Outcome}}": "Outcome",
        "{{PrimaryLanguage}}": "Language",
        "{{outcome}}": "OutcomeDescription",
        ## adjournment
        "{{adjDateOfApplication}}": "DateReceived",
        "{{adjDateOfHearing}}": "MiscDate1",
        "{{adjPartyMakingApp}}":"StatusParty",
        "{{adjDirections}}":"StatusNotes1",
        "{{adjDateOfDecision}}":"DecisionDate",
        "{{adjOutcome}}":"OutcomeDescription",
        "{{adjdatePartiesNotified}}":"DecisionSentToHODate",
    },
    18: {  # Bail Renewal
        "{{BailRenewalStatusOfBail}}": "CaseStatusDescription",
        "{{BailRenewalDateOfApplication}}": "DateReceived",
        "{{BailRenewalDateOfHearing}}": "Keydate",
        "{{BailRenewalInterpreterRequired}}": "InterpreterRequiredDesc",
        "{{BailRenewalDateOfOrder}}": "MiscDate2",
        "{{BailRenewalTotalAmountOfFinancialCondition}}": "TotalAmountOfFinancialCondition",
        "{{BailRenewalBailCondition}}": "BailConditionsDesc",
        "{{BailRenewalTotalSecurity}}": "TotalSecurity",
        "{{BailRenewalDateDischarged}}": "MiscDate1",
        "{{BailRenewalVideoLink}}": "VideoLink",
        "{{BailRenewalDateOfDecision}}": "DecisionDate",
        "{{BailRenewalOutcome}}": "OutcomeDescription",
        "{{BailRenewalHOConsentDate}}": "DecisionSentToHODate",
        "{{livesAndSleepsAt}}": "LivesAndSleepsAt",
        "{{appearBefore}}":"AppearBefore",
        "{{reportTo}}": "ReportTo",
        "{{tagging}}":"StatusBailConditionTagging",
        "{{workAndStudyRestriction}}": "WorkAndStudyRestriction",
        "{{other}}": "OtherCondition",
        "{{outcomeReasons}}": "OutcomeReasons",
        ## hearing tab
        "{{HearingCentre}}":"HearingTypeDesc",
        "{{Court}}":"CourtName",
        "{{HearingDate}}":"HearingDate",
        "{{ListName}}":"ListName",
        "{{ListType}}":"ListType",
        "{{HearingType}}":"HearingTypeDesc",
        "{{ListStartTime}}":"ListStartTime",
        "{{JudgeFT}}":"JudgeFT",
        "{{CourtClerkUsher}}":"CourtClerkUsher",
        "{{StartTime}}":"CaseListStartTime",
        "{{EstDuration}}":"CaseListTimeEstimate",
        "{{Outcome}}": "Outcome",
        "{{PrimaryLanguage}}": "Language",
        "{{outcome}}": "OutcomeDescription",
        ## adjournment
        "{{adjDateOfApplication}}": "DateReceived",
        "{{adjDateOfHearing}}": "MiscDate1",
        "{{adjPartyMakingApp}}":"StatusParty",
        "{{adjDirections}}":"StatusNotes1",
        "{{adjDateOfDecision}}":"DecisionDate",
        "{{adjOutcome}}":"OutcomeDescription",
        "{{adjdatePartiesNotified}}":"DecisionSentToHODate",
    },
    19: {  # Bail Variation
        "{{BailVariationStatusOfBail}}": "CaseStatusDescription",
        "{{BailVariationDateOfApplication}}": "DateReceived",
        "{{BailVariationPartyMakingApplication}}": "Party",
        "{{BailVariationDateOfHearing}}": "Keydate",
        "{{BailVariationInterpreterRequired}}": "InterpreterRequiredDesc",
        "{{BailVariationDateOfOrder}}": "MiscDate1",
        "{{BailVariationTotalAmountOfFinancialCondition}}": "TotalAmountOfFinancialCondition",
        "{{BailVariationBailCondition}}": "BailConditionsDesc",
        "{{BailVariationTotalSecurity}}": "TotalSecurity",
        "{{BailVariationDateDischarged}}": "MiscDate2",
        "{{BailVariationVideoLink}}": "VideoLink",
        "{{BailVariationDateOfDecision}}": "DecisionDate",
        "{{BailVariationOutcome}}": "OutcomeDescription",
        "{{livesAndSleepsAt}}": "LivesAndSleepsAt",
        "{{appearBefore}}":"AppearBefore",
        "{{reportTo}}": "ReportTo",
        "{{tagging}}":"StatusBailConditionTagging",
        "{{workAndStudyRestriction}}": "WorkAndStudyRestriction",
        "{{other}}": "OtherCondition",
        "{{outcomeReasons}}": "OutcomeReasons",
        ## hearing tab
        "{{HearingCentre}}":"HearingTypeDesc",
        "{{Court}}":"CourtName",
        "{{HearingDate}}":"HearingDate",
        "{{ListName}}":"ListName",
        "{{ListType}}":"ListType",
        "{{HearingType}}":"HearingTypeDesc",
        "{{ListStartTime}}":"ListStartTime",
        "{{JudgeFT}}":"JudgeFT",
        "{{CourtClerkUsher}}":"CourtClerkUsher",
        "{{StartTime}}":"CaseListStartTime",
        "{{EstDuration}}":"CaseListTimeEstimate",
        "{{Outcome}}": "Outcome",
        "{{PrimaryLanguage}}": "Language",
        "{{outcome}}": "OutcomeDescription",
        ## adjournment
        "{{adjDateOfApplication}}": "DateReceived",
        "{{adjDateOfHearing}}": "MiscDate1",
        "{{adjPartyMakingApp}}":"StatusParty",
        "{{adjDirections}}":"StatusNotes1",
        "{{adjDateOfDecision}}":"DecisionDate",
        "{{adjOutcome}}":"OutcomeDescription",
        "{{adjdatePartiesNotified}}":"DecisionSentToHODate",
    },

    35: {
        "{{MigrationDateOfHearing}}": "Keydate",
        "{{MigrationListType}}": "ListType",
        "{{MigrationHearingType}}": "HearingType",
        "{{MigrationJudiciary1}}": "Judiciary1Name",
        "{{MigrationJudiciary2}}": "Judiciary2Name",
        "{{MigrationJudiciary3}}": "Judiciary3Name",
        "{{MigrationPartyMakingApplication}}": "StatusParty",
        "{{MigrationDateOfDecision}}": "DecisionDate",
        "{{MigrationOutcome}}": "OutcomeDescription",
        "{{MigrationDateOfPromulgation}}": "StatusPromulgated",
        "{{livesAndSleepsAt}}": "LivesAndSleepsAt",
        "{{appearBefore}}":"AppearBefore",
        "{{reportTo}}": "ReportTo",
        "{{tagging}}":"StatusBailConditionTagging",
        "{{workAndStudyRestriction}}": "WorkAndStudyRestriction",
        "{{other}}": "OtherCondition",
        "{{outcomeReasons}}": "OutcomeReasons",
        ## hearing tab
        "{{HearingCentre}}":"HearingTypeDesc",
        "{{Court}}":"CourtName",
        "{{HearingDate}}":"HearingDate",
        "{{ListName}}":"ListName",
        "{{ListType}}":"ListType",
        "{{HearingType}}":"HearingTypeDesc",
        "{{ListStartTime}}":"ListStartTime",
        "{{JudgeFT}}":"JudgeFT",
        "{{CourtClerkUsher}}":"CourtClerkUsher",
        "{{StartTime}}":"CaseListStartTime",
        "{{EstDuration}}":"CaseListTimeEstimate",
        "{{Outcome}}": "Outcome",
        "{{PrimaryLanguage}}": "Language",
        "{{outcome}}": "OutcomeDescription",
        ## adjournment
        "{{adjDateOfApplication}}": "DateReceived",
        "{{adjDateOfHearing}}": "MiscDate1",
        "{{adjPartyMakingApp}}":"StatusParty",
        "{{adjDirections}}":"StatusNotes1",
        "{{adjDateOfDecision}}":"DecisionDate",
        "{{adjOutcome}}":"OutcomeDescription",
        "{{adjdatePartiesNotified}}":"DecisionSentToHODate",


    }
}

date_fields = {
    "DateReceived", "Keydate", "MiscDate1", "MiscDate2", "MiscDate3",
    "DecisionDate", "DateOfOrder", "DatePaymentInstructions", 
    "DateChequesIssued", "DateCautionLodged", "HOConsentDate","DateReceived","MiscDate1","DecisionDate","DecisionSentToHODate"
}




# COMMAND ----------

# MAGIC %md
# MAGIC ## HTML Templates
# MAGIC

# COMMAND ----------

fcs_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/fcs_template.html"
with open (fcs_template_path, "r") as f:
    fcs_template = f.read()

# COMMAND ----------

# File paths ofr status HTML templates
bail_Application_path = "/dbfs/mnt/ingest00landingsboxhtml-template/bailsv3/bail_applicaiton.html"
bail_Variation_path = "/dbfs/mnt/ingest00landingsboxhtml-template/bailsv3/bail _variation.html"
bail_Renewal_path = "/dbfs/mnt/ingest00landingsboxhtml-template/bailsv3/bail_renewal.html"
bail_Payment_path = "/dbfs/mnt/ingest00landingsboxhtml-template/bailsv3/bail_payment_liability.html"
bail_Scottish_Payment_Liability_path = "/dbfs/mnt/ingest00landingsboxhtml-template/bailsv3/bail_scottish_payment_liability.html"
bail_Lodgement_path = "/dbfs/mnt/ingest00landingsboxhtml-template/bailsv3/bail_lodgement.html"
migration_path = "/dbfs/mnt/ingest00landingsboxhtml-template/bailsv3/bail_migration.html"

# Path of the  bail HTML template
bails_html_path = "/dbfs/mnt/ingest00landingsboxhtml-template/bailsv3/bails-no-js-v3.html"


# read in the different templates
with open(bails_html_path, "r") as f:
    bails_html_dyn = f.read()

with open(bail_Application_path, "r") as f:
    bail_Application = f.read()

with open(bail_Payment_path, "r") as f:
    bail_Payment = f.read()

with open(bail_Variation_path, "r") as f:
    bail_Variation = f.read()

with open(bail_Renewal_path, "r") as f:
    bail_Renewal = f.read()

with open(bail_Scottish_Payment_Liability_path, "r") as f:
    bail_Scottish_Payment_Liability = f.read()

with open(bail_Lodgement_path, "r") as f:
    bail_Lodgement = f.read()

with open(migration_path, "r") as f:
    migration = f.read()


# displayHTML(bails_html_dyn)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join M1 and M2 to Master table

# COMMAND ----------



stg_m1_m2_struct = struct(
    col("HORef"),
    col("BailType"),
    col("CourtPreference"),
    col("DateOfIssue"),
    col("DateOfNextListedHearing"),
    col("DateReceived"),
    col("DateServed"),
    col("AppealCaseNote"),
    col("InCamera"),
    col("ProvisionalDestructionDate"),
    col("HOInterpreter"),
    col("Interpreter"),
    col("CountryId"),
    col("CountryOfTravelOrigin"),
    col("PortId"),
    col("PortOfEntry"),
    col("NationalityId"),
    col("Nationality"),
    col("InterpreterRequirementsLanguage"),
    col("Language"),
    col("CentreId"),
    col("DedicatedHearingCentre"),
    col("AppealCategories"),
    col("PubliclyFunded"),
    col("CaseRespondent"),
    col("CaseRespondentReference"),
    col("CaseRespondentContact"),
    col("RespondentAddress1"),
    col("RespondentAddress2"),
    col("RespondentAddress3"),
    col("RespondentAddress4"),
    col("RespondentAddress5"),
    col("RespondentEmail"),
    col("RespondentFax"),
    col("RespondentShortName"),
    col("RespondentTelephone"),
    col("RespondentPostcode"),
    col("PouShortName"),
    col("PouAddress1"),
    col("PouAddress2"),
    col("PouAddress3"),
    col("PouAddress4"),
    col("PouAddress5"),
    col("PouPostcode"),
    col("PouTelephone"),
    col("PouFax"),
    col("PouEmail"),
    col("EmbassyLocation"),
    col("Embassy"),
    col("Surname"),
    col("Forename"),
    col("Title"),
    col("OfficialTitle"),
    col("EmbassyAddress1"),
    col("EmbassyAddress2"),
    col("EmbassyAddress3"),
    col("EmbassyAddress4"),
    col("EmbassyAddress5"),
    col("EmbassyPostcode"),
    col("EmbassyTelephone"),
    col("EmbassyFax"),
    col("EmbassyEmail"),
    col("MainRespondentName"),
    col("FileLocationNote"),
    col("FileLocationTransferDate"),
    col("CaseRepName"),
    col("CaseRepAddress1"),
    col("CaseRepAddress2"),
    col("CaseRepAddress3"),
    col("CaseRepAddress4"),
    col("CaseRepAddress5"),
    col("CaseRepPostcode"),
    col("CaseRepPhone"),
    col("CaseRepEmail"),
    col("CaseRepFax"),
    col("CaseRepDxNo1"),
    col("CaseRepDxNo2"),
    col("CaseRepLSCCommission"),
    col("FileSpecifcContact"),
    col("FileSpecificPhone"),
    col("FileSpecificReference"),
    col("FileSpecificFax"),
    col("FileSpecificEmail"),
    col("RepAddress1"),
    col("RepAddress2"),
    col("RepAddress3"),
    col("RepAddress4"),
    col("RepAddress5"),
    col("RepName"),
    col("RepDxNo1"),
    col("RepDxNo2"),
    col("RepPostcode"),
    col("RepTelephone"),
    col("RepFax"),
    col("RepEmail"),
    col("DateOfApplication"),
    col("TypeOfCostAward"),
    col("ApplyingParty"),
    col("PayingParty"),
    col("MindedToAward"),
    col("ObjectionToMindedToAward"),
    col("CostsAwardDecision"),
    col("CostsAmount"),
    col("DateOfDecision"),
    col("AppealStage"),
    col("BailTypeDesc"),
    col("CourtPreferenceDesc"),
    col("InterpreterDesc"),
    col("TypeOfCostAwardDesc"),
    col("ApplyingPartyDesc"),
    col("PayingPartyDesc"),
    col("CostsAwardDecisionDesc"),
    col("BaseBailType"),
    col("AppellantId"),
    col("Relationship"),
    col("PortReference"),
    col("AppellantName"),
    col("AppellantForenames"),
    col("AppellantTitle"),
    col("AppellantBirthDate"),
    col("AppellantAddress1"),
    col("AppellantAddress2"),
    col("AppellantAddress3"),
    col("AppellantAddress4"),
    col("AppellantAddress5"),
    col("AppellantPostcode"),
    col("AppellantTelephone"),
    col("AppellantFax"),
    col("AppellantPrisonRef"),
    col("AppellantDetained"),
    col("AppellantEmail"),
    col("DetentionCentre"),
    col("DetentionCentreAddress1"),
    col("DetentionCentreAddress2"),
    col("DetentionCentreAddress3"),
    col("DetentionCentreAddress4"),
    col("DetentionCentreAddress5"),
    col("DetentionCentrePostcode"),
    col("DetentionCentreFax"),
    col("DoNotUseNationality"),
    col("AppellantDetainedDesc")
)


# COMMAND ----------


@dlt.table(name="stg_m1_m2")
def stg_m1_m2():
    # Read tables
    m1 = dlt.read("silver_bail_m1_case_details").alias("m1")

    m2 = dlt.read("silver_bail_m2_case_appellant").filter(col("Relationship").isNull()).alias("m2")

    # find all columns in m2 not in m1
    m2_new_columns = [col_name for col_name in m2.columns if col_name not in m1.columns]


    selected_columns = [col("m1.*")] + [col(f"m2.{c}") for c in m2_new_columns]

    # Join M1 and M2 tables
    m1_m2 = m1.join(
        m2,col("m1.CaseNo") == col("m2.CaseNo")
    ).select(*selected_columns)

    m1_m2_final = m1_m2.groupBy("m1.CaseNo").agg(collect_list(stg_m1_m2_struct).alias("Case_detail"))
                                                 
    return m1_m2_final



# COMMAND ----------

# MAGIC %md
# MAGIC ### Join M3 and M7 to Master table

# COMMAND ----------

# # # # read in the silver m3 filtered table

# m3_df = spark.read.table("hive_metastore.aria_bails.bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj")

# columns_to_group_by = [col(c) for c in m3_df.columns if c not in ["FullName", "AdjudicatorTitle", "AdjudicatorForenames", "AdjudicatorSurname", "Chairman", "Position"]]

# df_named = m3_df.withColumn(
#     "FullName",
#     concat_ws(" ", col("AdjudicatorTitle"), col("AdjudicatorForenames"), col("AdjudicatorSurname"))
# )
# pivoted_df = df_named.groupBy(*columns_to_group_by) \
#     .pivot("Position",["3","10","11","12"]) \
#     .agg(first("FullName"))


# for c in pivoted_df.columns:
#     if c == "null":
#         new_col = "NoPosition"
#     elif c.isdigit():
#         if c == "3":
#             new_col = "CourtClerkUsher"
#         else:
#             new_col = f"Position{c}"
#     else:
#         new_col = c
#     pivoted_df = pivoted_df.withColumnRenamed(c, new_col)




# pivoted_df.display()




# m3 = pivoted_df.alias("m3")

# m7 = spark.read.table("hive_metastore.aria_bails.silver_bail_m7_status").alias("m7")

# m3_new_columns = [col_name for col_name in m3.columns if col_name not in m7.columns]

# status_tab = m7.alias("m7").join(
#     m3.select("CaseNo", "StatusId", *m3_new_columns).alias("m3"),
#     on= ["CaseNo","StatusId"] ,how=
#     "left"
# )

# status_tab_struct = struct(*[col(c) for c in status_tab.columns])

# m7_m3_statuses = (
#         status_tab
#         .groupBy(col("CaseNo"))
#         .agg(
#             collect_list(
#                 # Collect each record's columns as a struct
#                 status_tab_struct
#             ).alias("all_status_objects")
#         )
#     )

# m7_m3_statuses.display()

# COMMAND ----------

# stg_m3_m7_df = spark.read.table("hive_metastore.aria_bails.stg_m3_m7")

# stg_m3_m7_df.display()

# COMMAND ----------

@dlt.table(name="stg_m3_m7")
def stg_m3_m7():


                                                                                                                                    
    # read in all tables


    m3 = dlt.read("silver_bail_m3_hearing_details")

    columns_to_group_by = [col(c) for c in m3.columns if c not in ["FullName", "AdjudicatorTitle", "AdjudicatorForenames", "AdjudicatorSurname", "Chairman", "Position"]]

    df_named = m3.withColumn(
        "FullName",
        concat_ws(" ", col("AdjudicatorTitle"), col("AdjudicatorForenames"), col("AdjudicatorSurname"))
    )
    pivoted_df = df_named.groupBy(*columns_to_group_by) \
        .pivot("Position",["3","10","11","12"]) \
        .agg(first("FullName")).withColumnRenamed("3", "CourtClerkUsher").withColumnRenamed("null", "NoPossition")


    for c in pivoted_df.columns:
        if c == "null":
            new_col = "NoPosition"
        elif c.isdigit():
            if c == "3":
                new_col = "CourtClerkUsher"
            else:
                new_col = f"Position{c}"
        else:
            new_col = c
        pivoted_df = pivoted_df.withColumnRenamed(c, new_col)




    m7 = dlt.read("silver_bail_m7_status")


    # Get all columns in m3 not in m7
    m3_new_columns = [col_name for col_name in pivoted_df.columns if col_name not in m7.columns]

    status_tab = m7.alias("m7").join(
        pivoted_df.select("CaseNo", "StatusId", *m3_new_columns).alias("m3"),
        on= ["CaseNo","StatusId"] ,how=
        "left"
    )


    # create a nested list for the stausus table (m7_m3 tables)

    status_tab_struct = struct(*[col(c) for c in status_tab.columns])
    m7_m3_statuses = (
        status_tab
        .groupBy(col("CaseNo"))
        .agg(
            collect_list(
                # Collect each record's columns as a struct
                status_tab_struct
            ).alias("all_status_objects")
        )
    )
    return m7_m3_statuses

# COMMAND ----------

# from pyspark.sql.window import Window
# from pyspark.sql.functions import col, row_number, explode

# exploded = m7_m3_statuses.select(col("CaseNo"), explode("all_status_objects").alias("status"))

# window_spec = Window.partitionBy("CaseNo").orderBy(col("status.StatusId").desc())

# ordered_rank = exploded.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank").select(col("CaseNo"), col("status.StatusId").alias("MaxCaseStatusDescription"))

# w_non_null_lang = Window.partitionBy("CaseNo").orderBy(col("status.LanguageDescription").desc())

# top_non_null_lang = exploded.filter(col("status.LanguageDescription").isNotNull()).withColumn("rank", row_number().over(w_non_null_lang)).filter(col("rank") == 1).drop("rank").select(col("CaseNo"), col("status.LanguageDescription").alias("SecondaryLanguage"))

# m3_m7_final = m7_m3_statuses.join(ordered_rank, on="CaseNo", how="left").join(top_non_null_lang, on="CaseNo", how="left")
# m3_m7_final


# COMMAND ----------

@dlt.table(name="stg_m7_m3_statuses", comment="This table will be joined to the m3_m7 table to add information like the max statusid and secondary language")
def final_m7_m3_statuses():

    m7_m3_statuses = dlt.read("stg_m3_m7")

    exploded = m7_m3_statuses.select(col("CaseNo"), explode("all_status_objects").alias("status"))

    window_spec = Window.partitionBy("CaseNo").orderBy(col("status.StatusId").desc())

    ordered_rank = exploded.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank").select(col("CaseNo"), col("status.StatusId").alias("MaxCaseStatusDescription"))

    w_non_null_lang = Window.partitionBy("CaseNo").orderBy(col("status.LanguageDescription").desc())

    top_non_null_lang = exploded.filter(col("status.LanguageDescription").isNotNull()).withColumn("rank", row_number().over(w_non_null_lang)).filter(col("rank") == 1).drop("rank").select(col("CaseNo"), col("status.LanguageDescription").alias("SecondaryLanguage"))

    m3_m7_final = m7_m3_statuses.join(ordered_rank, on="CaseNo", how="left").join(top_non_null_lang, on="CaseNo", how="left")
    m3_m7_final


    return m3_m7_final

# COMMAND ----------

# @dlt.table(name="stg_m7_m3_statuses", comment="Final Bail Status Table")
# def final_m7_m3_statuses():

#     final_m7_m3_df = dlt.read("stg_statuses")

#     m7_m3_statuses = dlt.read("stg_m3_m7")



#     final_m7_m3_statuses = m7_m3_statuses.join(final_m7_m3_df, "CaseNo", "left_outer")

#     return final_m7_m3_statuses

# COMMAND ----------

@dlt.table(name="stg_m1_m2_m3_m7")
def m1_m2_m3_m7():

    m1_m2 = dlt.read("stg_m1_m2")
    final_m7_m3_statuses = dlt.read("stg_m7_m3_statuses")




    # Join status stab to main table m1_m2 table
    m1_m2_m3_m7_df = m1_m2.join(final_m7_m3_statuses, "CaseNo", "left_outer")

    return m1_m2_m3_m7_df




# COMMAND ----------

# MAGIC %md
# MAGIC #### Status Mapping dictionary

# COMMAND ----------


# Dictionary to map status codes to html templates
template_for_status = { 4: bail_Application,
                       6: bail_Payment,
                       8: bail_Lodgement,
                       11: bail_Scottish_Payment_Liability,
                       18: bail_Renewal,
                       19: bail_Variation,
                       35: migration

}



# COMMAND ----------

# MAGIC %md
# MAGIC ### Join M5 to master table

# COMMAND ----------

@dlt.table(name="stg_m1_m2_m3_m5_m7", comment="Silver Bail M1 M2 Table")
def stg_m1_m2_m3_m5_m7():


    # History table
    # read in all tables

    m5 = dlt.read("silver_bail_m5_history")

    m1_m2_m3_m7_df = dlt.read("stg_m1_m2_m3_m7")

    # History 

    m5_history = m5.groupBy(col("CaseNo")).agg(
        collect_list(
        struct(
            col("HistoryId"),
            col("HistDate"),
            col("HistType"),
            col("HistoryComment"),
            col("UserFullname"),
            col("DeletedBy")
    )
        )
        .alias("m5_history_details"))



    last_document_expr = expr("""
        IF(
            size(
                array_sort(
                    filter(m5_history_details, x -> x.HistType = 16),
                    (a, b) -> CASE 
                                WHEN a.HistDate > b.HistDate THEN -1 
                                WHEN a.HistDate < b.HistDate THEN 1 
                                ELSE 0 
                            END
                )
            ) > 0,
            array_sort(
                filter(m5_history_details, x -> x.HistType = 16),
                (a, b) -> CASE 
                            WHEN a.HistDate > b.HistDate THEN -1 
                            WHEN a.HistDate < b.HistDate THEN 1 
                            ELSE 0 
                        END
            )[0].HistoryComment,
            null
        )
    """)

    # Expression to extract file_location for HistType 6, explicitly sorting by HistDate descending
    file_location_expr = expr("""
        IF(
            size(
                array_sort(
                    filter(m5_history_details, x -> x.HistType = 6),
                    (a, b) -> CASE 
                                WHEN a.HistDate > b.HistDate THEN -1 
                                WHEN a.HistDate < b.HistDate THEN 1 
                                ELSE 0 
                            END
                )
            ) > 0,
            array_sort(
                filter(m5_history_details, x -> x.HistType = 6),
                (a, b) -> CASE 
                            WHEN a.HistDate > b.HistDate THEN -1 
                            WHEN a.HistDate < b.HistDate THEN 1 
                            ELSE 0 
                        END
            )[0].HistoryComment,
            null
        )
    """)

    # Add the new columns to m5_history
    m5_history_enriched = m5_history \
        .withColumn("last_document", last_document_expr) \
        .withColumn("file_location", file_location_expr)

    m1_m2_m3_m5_m7_df = m1_m2_m3_m7_df.join(m5_history_enriched, "CaseNo", "left")

    return m1_m2_m3_m5_m7_df



# COMMAND ----------

# MAGIC %md
# MAGIC ### Join M4 to master table

# COMMAND ----------

@dlt.table(name="stg_m1_m2_m3_m4_m5_m7", comment="Silver Bail M4 Table")
def stg_m1_m2_m3_m4_m5_m7_df():

    m1_m2_m3_m5_m7_df = dlt.read("stg_m1_m2_m3_m5_m7")

    # BF diary M4 Tables

    # read in all tables

    m4 = dlt.read("silver_bail_m4_bf_diary")


    m4_bfdiary_df = m4.groupBy(col("CaseNo")).agg(
        collect_list(
            struct(
                col("BFDate"),
                col("BFTypeDescription"),
                col("Entry"),
                col("DateCompleted")
                )).alias("bfdiary_details"))

    m1_m2_m3_m4_m5_m7_df = m1_m2_m3_m5_m7_df.join(m4_bfdiary_df, "CaseNo", "left")

    return m1_m2_m3_m4_m5_m7_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Join M8 to Master table

# COMMAND ----------

@dlt.table(name="stg_m1_m2_m3_m4_m5_m7_m8")
def stg_m1_m2_m3_m4_m5_m6_m7_m8_df():

    m1_m2_m3_m4_m5_m7_df = dlt.read("stg_m1_m2_m3_m4_m5_m7")

    # Appeal Category

    # read in all tables

    m8 = dlt.read("silver_bail_m8")


    m8_appeal_category_df = m8.groupBy(col("CaseNo")).agg(
        collect_list(
            struct(
                col("CategoryDescription"),
                col("Flag")
                )).alias("appeal_category_details"))

    m1_m2_m3_m4_m5_m7_m8_df = m1_m2_m3_m4_m5_m7_df.join(m8_appeal_category_df, "CaseNo", "left")

    return m1_m2_m3_m4_m5_m7_m8_df





# COMMAND ----------

# MAGIC %md
# MAGIC ### Join M6 to master table

# COMMAND ----------

@dlt.table(name="stg_m1_m2_m3_m4_m5_m6_m7_m8")
def stg_m1_m2_m3_m4_m5_m6_m7_m8_df():
    

    m1_m2_m3_m4_m5_m7_m8_df = dlt.read("stg_m1_m2_m3_m4_m5_m7_m8")


    # # Linked Files

    # read in all tables

    m6 = dlt.read("silver_bail_m6_link")


    m6_linked_files_df = m6.groupBy(col("CaseNo")).agg(
        collect_list(
            struct(
                col("LinkDetailComment"),
                col("LinkNo"),
                col("FullName"),
            )).alias("linked_files_details"))

    m1_m2_m3_m4_m5_m6_m7_m8_df = m1_m2_m3_m4_m5_m7_m8_df.join(m6_linked_files_df, "CaseNo", "left")


    return m1_m2_m3_m4_m5_m6_m7_m8_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Casesurety to master table

# COMMAND ----------

case_surety_replacement = {
    "{{SponsorName}}": "CaseSuretyName",
    "{{SponsorForename}}": "CaseSuretyForenames",
    "{{SponsorTitle}}": "CaseSuretyTitle",
    "{{SponsorAddress1}}": "CaseSuretyAddress1",
    "{{SponsorAddress2}}": "CaseSuretyAddress2",
    "{{SponsorAddress3}}": "CaseSuretyAddress3",
    "{{SponsorAddress4}}": "CaseSuretyAddress4",
    "{{SponsorAddress5}}": "CaseSuretyAddress5",
    "{{SponsorPostcode}}": "CaseSuretyPostcode",
    "{{SponsorPhone}}": "CaseSuretyTelephone",
    "{{SponsorEmail}}": "CaseSuretyEmail",
    "{{AmountOfFinancialCondition}}": "AmountOfFinancialCondition",
    "{{SponsorSolicitor}}": "Solicitor",
    "{{SponserDateLodged}}": "CaseSuretyDateLodged",
    "{{SponsorLocation}}": "Location",
    "{{AmountOfSecurity}}": "AmountOfTotalSecurity"
}


@dlt.table(name="stg_m1_m2_m3_m4_m5_m6_m7_m8_cs")
def stg_m1_m2_m3_m4_m5_m6_m7_m8_cs_df():

    m1_m2_m3_m4_m5_m6_m7_m8_df = dlt.read("stg_m1_m2_m3_m4_m5_m6_m7_m8")

    # read in all tables
    case_surety = dlt.read("bronze_case_surety_query")

    # Group and aggregate financial condition details
    financial_condition_df = case_surety.groupBy(col("CaseNo")).agg(
        collect_list(
            struct(
                col("SuretyId"),
                col("AmountOfFinancialCondition"),
                col("AmountOfTotalSecurity"),
                col("CaseSuretyName"),
                col("CaseSuretyForenames"),
                col("CaseSuretyTitle"),
                col("CaseSuretyAddress1"),
                col("CaseSuretyAddress2"),
                col("CaseSuretyAddress3"),
                col("CaseSuretyAddress4"),
                col("CaseSuretyAddress5"),
                col("CaseSuretyPostcode"),
                col("CaseSuretyDateLodged"),
                col("Location"),
                col("Solicitor"),
                col("CaseSuretyEmail"),
                col("CaseSuretyTelephone")
            )
        ).alias("financial_condition_details")
    )

    # Join DataFrames
    m1_m2_m3_m4_m5_m6_m7_m8_cs_df = m1_m2_m3_m4_m5_m6_m7_m8_df.join(
        financial_condition_df, "CaseNo", "left"
    )

    return m1_m2_m3_m4_m5_m6_m7_m8_cs_df



# COMMAND ----------

# MAGIC %md
# MAGIC ### Respondent Mapping dict

# COMMAND ----------

respondent_mapping = {
    1: {
        "{{RespondentName}}": "CaseRespondent",
        "{{CaseRespondentAddress1}}": "RespondentAddress1",
        "{{CaseRespondentAddress2}}": "RespondentAddress2",
        "{{CaseRespondentAddress3}}": "RespondentAddress3",
        "{{CaseRespondentAddress4}}": "RespondentAddress4",
        "{{CaseRespondentAddress5}}": "RespondentAddress5",
        "{{CaseRespondentPostcode}}": "RespondentPostcode",
        "{{CaseRespondentTelephone}}": "RespondentTelephone",
        "{{CaseRespondentFAX}}": "RespondentFax",
        "{{CaseRespondentEmail}}": "RespondentEmail",
        "{{CaseRespondentRef}}": "CaseRespondentReference",
        "{{CaseRespondentContact}}": "CaseRespondentContact",
        "{{POU}}":None,
        "{{RRrespondent}}":"MainRespondentName"
    },

    2: {
        "{{RespondentName}}": "CaseRespondent",
        "{{CaseRespondentAddress1}}": "EmbassyAddress1",
        "{{CaseRespondentAddress2}}": "EmbassyAddress2",
        "{{CaseRespondentAddress3}}": "EmbassyAddress3",
        "{{CaseRespondentAddress4}}": "EmbassyAddress4",
        "{{CaseRespondentAddress5}}": "EmbassyAddress5",
        "{{CaseRespondentPostcode}}": "EmbassyPostcode",
        "{{CaseRespondentTelephone}}": "EmbassyTelephone",
        "{{CaseRespondentFAX}}": "EmbassyFax",
        "{{CaseRespondentEmail}}": "EmbassyEmail",
        "{{CaseRespondentRef}}": "CaseRespondentReference",
        "{{CaseRespondentContact}}": "CaseRespondentContact",
        "{{POU}}":None,
        "{{RRrespondent}}":None
    },

    3: {
        "{{RespondentName}}": "CaseRespondent",
        "{{CaseRespondentAddress1}}": "PouAddress1",
        "{{CaseRespondentAddress2}}": "PouAddress2",
        "{{CaseRespondentAddress3}}": "PouAddress3",
        "{{CaseRespondentAddress4}}": "PouAddress4",
        "{{CaseRespondentAddress5}}": "PouAddress5",
        "{{CaseRespondentPostcode}}": "PouPostcode",
        "{{CaseRespondentTelephone}}": "PouTelephone",
        "{{CaseRespondentFAX}}": "PouFax",
        "{{CaseRespondentEmail}}": "PouEmail",
        "{{CaseRespondentRef}}": "CaseRespondentReference",
        "{{CaseRespondentContact}}": "CaseRespondentContact",
        "{{POU}}":"PouShortName",
        "{{RRrespondent}}":None
    }
}





        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Linked cases to master table

# COMMAND ----------

@dlt.table(name="final_staging_bails")
def final_staging_bails():


    m1_m2_m3_m4_m5_m6_m7_m8_cs_df = dlt.read("stg_m1_m2_m3_m4_m5_m6_m7_m8_cs")

    # read in all tables
    linked_cases = dlt.read("linked_cases_cost_award")

    linked_cases_df = linked_cases.groupBy(col("CaseNo")).agg(
        collect_list(
            struct(
                col("CostAwardId"),
                col("LinkNo"),
                col("CaseNo"),
                col("Name"),
                col("Forenames"),
                col("Title"),
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
                col("AppealStage"),
                col("AppealStageDescription")
            )
        ).alias("linked_cases_aggregated")
    )

    m1_m2_m3_m4_m5_m6_m7_m8_cs_lc_df = m1_m2_m3_m4_m5_m6_m7_m8_cs_df.join(linked_cases_df, "CaseNo", "left")

    return m1_m2_m3_m4_m5_m6_m7_m8_cs_lc_df


# COMMAND ----------

# MAGIC %md
# MAGIC dont need this

# COMMAND ----------

# html = bails_html_dyn

# flag_list = []
# flag_dict = {}

# row = df.limit(1).collect()[0]
# print(row.CaseNo)
# if row.Case_detail is not None:
#     for casedetail in row.Case_detail:
#         if casedetail.AppellantDetainedDesc == "HMP" or casedetail.AppellantDetainedDesc == "IRC" or casedetail.AppellantDetainedDesc == "Others":
#             flag_list.append("DET")
#         if casedetail.InCamera == 1:
#             flag_list.append("CAM")
#         else:
#             pass

# if row.all_status_objects is not None:
#     for statusdetails in row.all_status_objects:
#         if statusdetails.CaseStatusDescription == "35":
#             flag_list.append("MIG")
#         else:
#             pass

# if row.appeal_category_details is not None:
#     for appealcategorydetails in row.appeal_category_details:
#         flag_list.append(appealcategorydetails.Flag)

# for i in range(1, 4):
#     flag_dict[f"flag_{i}"] = flag_list[i] if flag_list[i] is not None else ""

# for i,(key,flag) in enumerate(flag_dict.items(),start=1):
#     # print(i,flag)
#     placeholder = f"{{{{flag{i}Placeholder}}}}"
#     html = html.replace(placeholder, flag)





# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions Diagram

# COMMAND ----------

# MAGIC %md
# MAGIC ![functions.jpg](./functions.jpg "functions.jpg"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### code checking duplicates

# COMMAND ----------

# tables = spark.catalog.listTables("hive_metastore.aria_bails")
# for table in tables:
#     # if table.name.startswith("bronze_") or table.name.startswith("silver_"):
#     if table.name.startswith("silver_"):
#         table_name = f"hive_metastore.aria_bails.{table.name}"
#         df = spark.read.table(table_name)
#         if "CaseNo" in df.columns:
#             duplicate_cases = df.groupBy("CaseNo").count().filter("count > 1")
#             if duplicate_cases.count() > 0:
#                 print(f"Table {table_name} has duplicate CaseNo")
#             else:
#                 print(f"Table {table_name} has unique CaseNo")
#         table_name = f"hive_metastore.aria_bails.{table.name}"
#         print(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## HTML Combined Code 

# COMMAND ----------

# MAGIC %md
# MAGIC dont need this

# COMMAND ----------



# # display(m1_m2)

# def create_html(rows_iter):
#     """
#     For a given a single row, this funciton will return the final HTML string
#     """
#     results = []
#     for row in rows_iter:
#         try:

#             # initialise html template
#             html = bails_html_dyn
            
#             # maintain cost award tab
#             main_cost_award_code = f"<tr><td id='midpadding'>{row['CaseNo']}</td><td id='midpadding'>{row['AppellantName']}</td><td id='midpadding'>{row['AppealStage']}</td><td id='midpadding'>{row['DateOfApplication']}</td><td id='midpadding'>{row['TypeOfCostAward']}</td><td id='midpadding'>{row['ApplyingParty']}</td><td id='midpadding'>{row['PayingParty']}</td><td id='midpadding'>{row['MindedToAward']}</td><td id='midpadding'>{row['ObjectionToMindedToAward']}</td><td id='midpadding'>{row['CostsAwardDecision']}</td><td id='midpadding'></td><td id='midpadding'>{row['CostsAmount']}</td></tr>"

#             # Linked Cases

#             linked_cases_code = ""

#             for linked_cases_row in row["linked_cases_aggregated"] or []:
#                 linked_cases_code += f"<tr><td id='midpadding'>{linked_cases_row['CaseNo']}</td><td id='midpadding'>{linked_cases_row['Forenames']}</td><td id='midpadding'>{linked_cases_row['AppealStage']}</td><td id='midpadding'>{linked_cases_row['DateOfApplication']}</td><td id='midpadding'>{linked_cases_row['TypeOfCostAward']}</td><td id='midpadding'>{linked_cases_row['ApplyingParty']}</td><td id='midpadding'>{linked_cases_row['PayingParty']}</td><td id='midpadding'>{linked_cases_row['MindedToAward']}</td><td id='midpadding'>{linked_cases_row['ObjectionToMindedToAward']}</td><td id='midpadding'>{linked_cases_row['CostsAwardDecision']}</td><td id='midpadding'></td><td id='midpadding'>{linked_cases_row['CostsAmount']}</td></tr>"
#             html = html.replace("{{linked_cases_replacement}}", linked_cases_code)


#             m1_replacement = {
#                 "{{ bailCaseNo }}":row["CaseNo"] ,
#                 "{{ hoRef }}": row["HORef"] ,
#                 "{{ lastName }}": row["AppellantName"],
#                 "{{ firstName }}" : row["AppellantForenames"],
#                 "{{ birthDate }}": format_date(row["AppellantBirthDate"]),
#                 "{{ portRef }}": row["PortReference"],
#                 "{{AppellantTitle}}": row["AppellantTitle"],
#                 ## Main section
#                 "{{BailType}}": row["BailTypeDesc"],
#                 "{{AppealCategoriesField}}": row["AppealCategories"],
#                 "{{Nationality}}":row["Nationality"],
#                 "{{TravelOrigin}}":row["CountryOfTravelOrigin"],
#                 "{{Port}}":row["PortOfEntry"],
#                 "{{DateOfReceipt}}":format_date(row["DateReceived"]),
#                 "{{DedicatedHearingCentre}}":row["DedicatedHearingCentre"],
#                 "{{DateNoticeServed}}":format_date(row["DateServed"]) ,
#                 "{{CurrentStatus}}": row["MaxCaseStatusDescription"],
#                 "{{ConnectedFiles}}":"",
#                 "{{DateOfIssue}}":format_date(row["DateOfIssue"]),
#                 # "{{NextHearingDate}}":row["DateOfNextListedHearing"],
#                 "{{LastDocument}}": row["last_document"],
#                 "{{FileLocation}}": row["file_location"],
#                 "{{BFEntry}}":"",
#                 "{{ProvisionalDestructionDate}}":format_date(row["ProvisionalDestructionDate"]),

#                 # Parties Tab - Applicant Section
#                 "{{Centre}}": row["DetentionCentre"],
#                 "{{AddressLine1}}": row["DetentionCentreAddress1"],
#                 "{{AddressLine2}}": row["DetentionCentreAddress2"],
#                 "{{AddressLine3}}": row["DetentionCentreAddress3"],
#                 "{{AddressLine4}}": row["DetentionCentreAddress4"],
#                 "{{AddressLine5}}": row["DetentionCentreAddress5"],
#                 "{{Postcode}}": row["DetentionCentrePostcode"],
#                 "{{Country}}": row["CountryOfTravelOrigin"],
#                 "{{phone}}": row["AppellantTelephone"],
#                 "{{email}}": row["AppellantEmail"],
#                 "{{PrisonRef}}": row["AppellantPrisonRef"],
                
                
#                 # Respondent Section
#                 "{{Detained}}":row["AppellantDetainedDesc"],
#                 "{{RespondentName}}":row["MainRespondentName"],
#                 "{{repName}}":row["CaseRepName"],
#                 "{{InterpreterRequirementsLanguage}}" : row["InterpreterRequirementsLanguage"],
#                 "{{HOInterpreter}}" : row["HOInterpreter"],
#                 "{{CourtPreference}}" : row["CourtPreferenceDesc"],
#                 "{{language}}": row["Language"],
#                 "{{required}}": 1 if row["InterpreterRequirementsLanguage"] is not None else 0,

#                 # Misc Tab
#                 "{{Notes}}" : row["AppealCaseNote"],

#                 # Maintain cost awards Tab

#                 # Representative Tab
#                 "{{RepName}}":row["CaseRepName"],
#                 "{{CaseRepAddress1}}": row["CaseRepAddress1"],
#                 "{{CaseRepAddress2}}": row["CaseRepAddress2"],
#                 "{{CaseRepAddress3}}": row["CaseRepAddress3"],
#                 "{{CaseRepAddress4}}": row["CaseRepAddress4"],
#                 "{{CaseRepAddress5}}": row["CaseRepAddress5"],
#                 "{{CaseRepPostcode}}": row["CaseRepPostcode"],
#                 "{{CaseRepTelephone}}": row["CaseRepPhone"],
#                 "{{CaseRepFAX}}": row["CaseRepFax"],
#                 "{{CaseRepEmail}}": row["CaseRepEmail"],
#                 "{{RepDxNo1}}": row["RepDxNo1"],
#                 "{{RepDxNo2}}": row["RepDxNo2"],
#                 "{{RepLAARefNo}}": "",
#                 "{{RepLAACommission}}":row["CaseRepLSCCommission"],
#                 #File specific contact





#                 # Status Tab - Additional Language
#                 "{{PrimaryLanguage}}":row["Language"],
#                 "{{SecondaryLanguage}}":row["SecondaryLanguage"],

#                 # Parties Tab
#                 # "{{Detained}}": row[""]
#                 "{{Centre}}":row["DetentionCentre"],



#                 # Financial Condition supporter
#                 # which case surty do we use

                
#                 } 
            

#             # BF diary 
#             bf_diary_code = ""
#             for bfdiary in row["bfdiary_details"] or []:
#                 bf_line = f"<tr><td id=\"midpadding\">{bfdiary['BFDate']}</td><td id=\"midpadding\">{bfdiary['BFTypeDescription']}</td><td id=\"midpadding\">{bfdiary['Entry']}</td><td id=\"midpadding\">{bfdiary['DateCompleted']}</td></tr>"
#                 bf_diary_code += bf_line + "\n"
            
#             # History 
#             history_code = ''
#             for history in row["m5_history_details"] or []:
#                 history_line = f"<tr><td id='midpadding'>{history['HistDate']}</td><td id='midpadding'>{history['HistType']}</td><td id='midpadding'>{history['UserFullname']}</td><td id='midpadding'>{history['HistoryComment']}</td></tr>"
#                 history_code += history_line + "\n"

#             # # Linked Files
#             linked_files_code = ''
#             for likedfile in row["linked_files_details"] or []:
#                 linked_files_line = f"<tr><td id='midpadding'></td><td id='midpadding'>{likedfile['LinkNo']}</td><td id='midpadding'>{likedfile['FullName']}</td><td id='midpadding'>{likedfile['LinkDetailComment']}</td></tr>"
#                 linked_files_code += linked_files_line + "\n"

#             # main typing - has no mapping

#             # Appeal Category
#             appeal_category_code = ""
#             for appeal_category in row["appeal_category_details"] or []:
#                 appeal_line = f"<tr><td id='midpadding'>{appeal_category['CategoryDescription']}</td><td id='midpadding'>{appeal_category['Flag']}</td><td id='midpadding'></td></tr>"
#                 appeal_category_code += appeal_line + " \n"

#             # Case Respondent
#             if row["CaseRespondent"] in respondent_mapping:
#                 current_respondent_mapping = respondent_mapping[row["CaseRespondent"]]

#                 for resp_placeholder, resp_field_name in current_respondent_mapping.items():
#                     if resp_field_name:
#                         value = row[resp_field_name]
#                     else:
#                         value = ""
#                     html = html.replace(resp_placeholder,str(value))
#             else:
#                 # logger.warn(f'Mapping not found for CaseRespondent: {row["CaseRespondent"]}, CaseNo: {row["m7.CaseNo"]}')
#                 current_respondent_mapping = {
#                     "{{RespondentName}}": "",
#                     "{{CaseRespondentAddress1}}": "",
#                     "{{CaseRespondentAddress2}}": "",
#                     "{{CaseRespondentAddress3}}": "",
#                     "{{CaseRespondentAddress4}}": "",
#                     "{{CaseRespondentAddress5}}": "",
#                     "{{CaseRespondentPostcode}}": "",
#                     "{{CaseRespondentTelephone}}": "",
#                     "{{CaseRespondentFAX}}": "",
#                     "{{CaseRespondentEmail}}": "",
#                     "{{CaseRespondentRef}}": "",
#                     "{{CaseRespondentContact}}": "",
#                     "{{POU}}":"",
#                     "{{RRrespondent}}":""
#                 }
#                 for resp_placeholder, resp_value in current_respondent_mapping.items():
#                     html = html.replace(resp_placeholder,str(resp_value))





#             # status
#             code = ""



#             for index,status in enumerate(row["all_status_objects"] or [],start=1):
#                 ## get the case status in the list
#                 case_status = int(status["CaseStatus"]) if status["CaseStatus"] is not None else 0

#                 ## set the margin and id counter
#                 if index == 1:
#                     margin = "10px"
#                 else:
#                     margin = "600px"

#                 counter = 30+index

#                 if case_status in case_status_mappings:
#                     template = template_for_status[case_status]
#                     template = template.replace("{{margin_placeholder}}",str(margin))
#                     template = template.replace("{{index}}",str(counter))
#                     status_mapping = case_status_mappings[case_status]



#                     for placeholder,field_name in status_mapping.items():
#                         if field_name in date_fields:
#                             raw_value = status[field_name] if field_name in status else None
#                             value = format_date(raw_value)
#                         else:
#                             value = status[field_name] if field_name in status else None
#                         template = template.replace(placeholder,str(value))
#                     code += template + "\n"
                    
                        
#                 else:
#                     # logger.info(f"Mapping not found for CaseStatus: {case_status}, CaseNo: {row['m7.CaseNo']}")
#                     continue

#             html = html.replace("{{statusplaceholder}}",code)
            
            
#             # Financial supporter

#             sponsor_name = "Financial Condiiton Suportor details entered" if row["financial_condition_details"] else "Financial Condiiton Suportor details not entered"

#             case_surety_replacement = {
#             "{{SponsorName}}":"CaseSuretyName",
#             "{{SponsorForename}}":"CaseSuretyForenames",
#             "{{SponsorTitle}}":"CaseSuretyTitle",
#             "{{SponsorAddress1}}":"CaseSuretyAddress1",
#             "{{SponsorAddress2}}":"CaseSuretyAddress2",
#             "{{SponsorAddress3}}":"CaseSuretyAddress3",
#             "{{SponsorAddress4}}":"CaseSuretyAddress4",
#             "{{SponsorAddress5}}":"CaseSuretyAddress5",
#             "{{SponsorPostcode}}":"CaseSuretyPostcode",
#             "{{SponsorPhone}}":"CaseSuretyTelephone",
#             "{{SponsorEmail}}":"CaseSuretyEmail",
#             "{{AmountOfFinancialCondition}}":"AmountOfFinancialCondition",
#             "{{SponsorSolicitor}}":"Solicitor",
#             "{{SponserDateLodged}}":"CaseSuretyDateLodged",
#             "{{SponsorLocation}}":"Location",
#             "{{AmountOfSecurity}}": "AmountOfTotalSecurity"
            

#         }

#             financial_condition_code = ""
#             details = row["financial_condition_details"] or []

#             # Iterate over each record in financial_condition_details array
#             for index, casesurety in enumerate(details, start=10):
#                 current_code = fcs_template  # Use the defined HTML template
#                 current_code = current_code.replace("{{Index}}", str(index))

#                 # Loop over each placeholder in the dictionary and replace with corresponding values
#                 for placeholder, col_name in case_surety_replacement.items():
#                     # Check if the field exists in the current struct; fallback to empty string if not
#                     value = casesurety[col_name] if col_name in casesurety and casesurety[col_name] is not None else ""
#                     current_code = current_code.replace(placeholder, str(value))

#                 financial_condition_code += current_code + "\n"

#             # Replace the placeholder in the HTML template with the generated code
#             html = html.replace('{{financial_condition_code}}',financial_condition_code)
#             # is there a financial condition suporter
#             html = html.replace("{{sponsorName}}",str(sponsor_name))
#             # add multiple lines of code for bf diary
#             html = html.replace("{{bfdiaryPlaceholder}}",bf_diary_code)
#             # add multiple lines of code for history
#             html = html.replace("{{HistoryPlaceholder}}",history_code)
#             # add multiple lines of code for linked details
#             html = html.replace("{{LinkedFilesPlaceholder}}",linked_files_code)
#             # add multiple lines of maintain cost awards
#             html = html.replace("{{MaintainCostAward}}",main_cost_award_code)
#             # add multiple line for appeal
#             html = html.replace("{{AppealPlaceholder}}",appeal_category_code)
#             for key, value in m1_replacement.items():
#                 html = html.replace(str(key), str(value))

#             results.append((row["CaseNo"], html))

            
#         except Exception as e:
#             logger.error(f"Error processing row: {row['CaseNo']}. Error: {e}")

        
#          # Create schema for the output DataFrame
#     schema = StructType([
#         StructField("CaseNo", StringType(), True),
#         StructField("HtmlContent", StringType(), True)
#     ])

#     # Convert results into a DataFrame
#     return spark.createDataFrame(results, schema)





# COMMAND ----------

# final_staging_bails_df = spark.read.table("hive_metastore.aria_bails.final_staging_bails")
# rows = final_staging_bails_df.collect()

# COMMAND ----------

# final_staging_bails_df.display()

# COMMAND ----------

# final_staging_bails_df.filter(col())

# COMMAND ----------

# row = rows[391]


# COMMAND ----------

# final_staging_bails_df.display()



# COMMAND ----------

# first_row.all_status_objects

# COMMAND ----------

# labels = []

# label_fields = ["UpperTribJudge", "DesJudgeFirstTier", "JudgeFirstTier", "NonLegalMember"]
# row = rows[303]
# caseno = row["CaseNo"]

# for field in label_fields:
#     count = row[field] if row[field] is not None else 0

#     for i in range(count):
#         labels.append(f"{field}_{i+1}")

# positions = sorted([col for col in pivoted_df.columns if col.isdigit()], key=int)

# label_col_map = dict(zip(positions, labels))

# display(caseno, label_col_map)

# for i in label_col_map.keys():
#     labels_dict = label_col_map.get(i, 'N/A')
#     print(f"Position {i}: {labels_dict}")
#     print(row[i])

# COMMAND ----------

# if first_row.all_status_objects is not None:
#     for index,status in enumerate(first_row.all_status_objects,start=1):
#         print(f"Status {index}: {status}")


# COMMAND ----------


# html = bails_html_dyn
# ### status
# code = ""


# if row.all_status_objects is not None:
#     for index,status in enumerate(row.all_status_objects,start=1):
#         ## get the case status in the list
#         case_status = int(status["CaseStatus"]) if status["CaseStatus"] is not None else 0

#         ## set the margin and id counter
#         if index == 1:
#             margin = "10px"
#         else:
#             margin = "600px"

#         counter = 30+index

#         if case_status in case_status_mappings:
#             template = template_for_status[case_status]
#             template = template.replace("{{margin_placeholder}}",str(margin))
#             template = template.replace("{{index}}",str(counter))
#             status_mapping = case_status_mappings[case_status]



#             for placeholder,field_name in status_mapping.items():
#                 if field_name in date_fields:
#                     raw_value = status[field_name] if field_name in status else None
#                     value = format_date_iso(raw_value)
#                 else:
#                     value = status[field_name] if field_name in status else None
#                 template = template.replace(placeholder,str(value))
            

#             labels = []

#             label_fields = ["UpperTribJudge", "DesJudgeFirstTier", "JudgeFirstTier", "NonLegalMember"]

#             for label in label_fields:
#                 count = status[label] if status[label] is not None else 0
#                 for i in range(count):
#                     labels.append(f"{label}_{i+1}")

#             possitions = ["Position10","Position11","Position12"]

#             possitions_labelled = dict(zip(possitions,labels))

#             possitions_keys = list(possitions_labelled.keys())
#             possitions_values = list(possitions_labelled.values())


#             for i in range(1, 4):
#                 label_placeholder = f"{{{{Label{i}}}}}"
#                 value_placeholder = f"{{{{Label{i}value}}}}"

#                 if i <= len(possitions_labelled):
#                     label_value = possitions_values[i-1]
#                     name_col = possitions_keys[i-1]
#                     name_value = status[name_col] if status[name_col] is not None else ""
#                     name_value = name_value if name_value else ""
#                 else:
#                     label_value = ""
#                     name_value = ""

#                 template = template.replace(label_placeholder, str(label_value))
#                 template = template.replace(value_placeholder, str(name_value))



                    
#             if status["CourtClerkUsher"]:
#                 template = template.replace("{{courtclerkusherplaceholder}}",status["CourtClerkUsher"])

#             else:
#                 template = template.replace("{{courtclerkusherplaceholder}}",'N/A')




#             code += template + "\n"
            
                
#         else:
#             # logger.info(f"Mapping not found for CaseStatus: {case_status}, CaseNo: {row['m7.CaseNo']}")
#             continue

        
        
# if code:
#     html = html.replace("{{statusplaceholder}}",code)
# else:
#     html = html.replace("{{statusplaceholder}}","")
    
# displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UDF Create HTML

# COMMAND ----------

def create_html_column(row, html_template=bails_html_dyn):
    """
    For a given a single row, this function returns the final HTML string.
    """
    try:
        # Initialize HTML template
        html = html_template  # Use input template instead of global

        # First replacements dictionary
        replacements = {
            "{{ bailCaseNo }}": str(row.CaseNo),
            "{{LastDocument}}": row.last_document,
            "{{FileLocation}}": row.file_location,
            "{{CurrentStatus}}": row.MaxCaseStatusDescription,
        }
        for key, value in replacements.items():
            html = html.replace(key, str(value) if value is not None else "")


        # Replace placeholders with actual values
        for cd_row in row.Case_detail:
            # Second replacements dictionary
            m1_replacement = {
            "{{ hoRef }}": cd_row.HORef,
            "{{ lastName }}": cd_row.AppellantName,
            "{{ firstName }}": cd_row.AppellantForenames,
            "{{ birthDate }}": format_date_iso(cd_row.AppellantBirthDate),
            "{{ portRef }}": cd_row.PortReference,
            "{{AppellantTitle}}": cd_row.AppellantTitle,
            "{{BailType}}": cd_row.BailTypeDesc,
            "{{AppealCategoriesField}}": str(cd_row.AppealCategories),
            "{{Nationality}}": cd_row.Nationality,
            "{{TravelOrigin}}": cd_row.CountryOfTravelOrigin,
            "{{Port}}": cd_row.PortOfEntry,
            "{{DateOfReceipt}}": format_date_iso(cd_row.DateReceived),
            "{{DedicatedHearingCentre}}": cd_row.DedicatedHearingCentre,
            "{{DateNoticeServed}}": format_date_iso(cd_row.DateServed),
            # "{{CurrentStatus}}": cd_row.MaxCaseStatusDescription,
            "{{ConnectedFiles}}": "",
            "{{DateOfIssue}}": format_date_iso(cd_row.DateOfIssue),
            # "{{LastDocument}}": cd_row.last_document,
            # "{{FileLocation}}": cd_row.file_location,
            "{{BFEntry}}": "",
            "{{ProvisionalDestructionDate}}": format_date_iso(cd_row.ProvisionalDestructionDate),

            # Parties Tab - Applicant Section
            "{{Centre}}": cd_row.DetentionCentre,
            "{{AddressLine1}}": cd_row.DetentionCentreAddress1,
            "{{AddressLine2}}": cd_row.DetentionCentreAddress2,
            "{{AddressLine3}}": cd_row.DetentionCentreAddress3,
            "{{AddressLine4}}": cd_row.DetentionCentreAddress4,
            "{{AddressLine5}}": cd_row.DetentionCentreAddress5,
            "{{Postcode}}": cd_row.DetentionCentrePostcode,
            "{{Country}}": cd_row.CountryOfTravelOrigin,
            "{{phone}}": cd_row.AppellantTelephone,
            "{{email}}": cd_row.AppellantEmail,
            "{{PrisonRef}}": cd_row.AppellantPrisonRef,

            # Respondent Section
            "{{Detained}}": cd_row.AppellantDetainedDesc,
            "{{RespondentName}}": cd_row.MainRespondentName,
            "{{repName}}": cd_row.CaseRepName,
            "{{InterpreterRequirementsLanguage}}": cd_row.InterpreterRequirementsLanguage,
            "{{HOInterpreter}}": cd_row.HOInterpreter,
            "{{CourtPreference}}": cd_row.CourtPreferenceDesc,
            "{{language}}": cd_row.Language,
            "{{required}}": "1" if cd_row.InterpreterRequirementsLanguage is not None else "0",

            # Misc Tab
            "{{Notes}}": cd_row.AppealCaseNote,

            # Representative Tab
            "{{RepName}}": cd_row.CaseRepName,
            "{{CaseRepAddress1}}": cd_row.CaseRepAddress1,
            "{{CaseRepAddress2}}": cd_row.CaseRepAddress2,
            "{{CaseRepAddress3}}": cd_row.CaseRepAddress3,
            "{{CaseRepAddress4}}": cd_row.CaseRepAddress4,
            "{{CaseRepAddress5}}": cd_row.CaseRepAddress5,
            "{{CaseRepPostcode}}": cd_row.CaseRepPostcode,
            "{{CaseRepTelephone}}": cd_row.CaseRepPhone,
            "{{CaseRepFAX}}": cd_row.CaseRepFax,
            "{{CaseRepEmail}}": cd_row.CaseRepEmail,
            "{{RepDxNo1}}": cd_row.RepDxNo1,
            "{{RepDxNo2}}": cd_row.RepDxNo2,
            "{{RepLAARefNo}}": "",
            "{{RepLAACommission}}": cd_row.CaseRepLSCCommission
        }
            for key, value in m1_replacement.items():
                html = html.replace(key, str(value) if value is not None else "")

        ### maintain cost award tab


        main_cost_award_code = f"<tr><td id='midpadding'>{row.CaseNo}</td><td id='midpadding'>{cd_row.AppellantName}</td><td id='midpadding'>{cd_row.AppealStage}</td><td id='midpadding'>{cd_row.DateOfApplication}</td><td id='midpadding'>{cd_row.TypeOfCostAward}</td><td id='midpadding'>{cd_row.ApplyingParty}</td><td id='midpadding'>{cd_row.PayingParty}</td><td id='midpadding'>{cd_row.MindedToAward}</td><td id='midpadding'>{cd_row.ObjectionToMindedToAward}</td><td id='midpadding'>{cd_row.CostsAwardDecision}</td><td id='midpadding'></td><td id='midpadding'>{cd_row.CostsAmount}</td></tr>"

        # add multiple lines of maintain cost awards
        html = html.replace("{{MaintainCostAward}}",main_cost_award_code)

        ### Linked Cases

        linked_cases_code = ""

        if row.linked_cases_aggregated is not None:
            for linked_cases_row in row.linked_cases_aggregated:
                linked_cases_code += f"<tr><td id='midpadding'>{linked_cases_row.CaseNo}</td><td id='midpadding'>{linked_cases_row.Forenames}</td><td id='midpadding'>{linked_cases_row.AppealStage}</td><td id='midpadding'>{linked_cases_row.DateOfApplication}</td><td id='midpadding'>{linked_cases_row.TypeOfCostAward}</td><td id='midpadding'>{linked_cases_row.ApplyingParty}</td><td id='midpadding'>{linked_cases_row.PayingParty}</td><td id='midpadding'>{linked_cases_row.MindedToAward}</td><td id='midpadding'>{linked_cases_row.ObjectionToMindedToAward}</td><td id='midpadding'>{linked_cases_row.CostsAwardDecision}</td><td id='midpadding'></td><td id='midpadding'>{linked_cases_row.CostsAmount}</td></tr>"
            html = html.replace("{{linked_cases_replacement}}", linked_cases_code)

        else:
            html = html.replace("{{linked_cases_replacement}}", "")

        ### BF diary 
        bf_diary_code = ""
        if row.bfdiary_details is not None:
            for bfdiary in row.bfdiary_details or []:
                bf_line = f"<tr><td id=\"midpadding\">{bfdiary.BFDate}</td><td id=\"midpadding\">{bfdiary.BFTypeDescription}</td><td id=\"midpadding\">{bfdiary.Entry}</td><td id=\"midpadding\">{bfdiary.DateCompleted}</td></tr>"
                bf_diary_code += bf_line + "\n"
            html = html.replace("{{bfdiaryPlaceholder}}", bf_diary_code)
        else:
            html = html.replace("{{bfdiaryPlaceholder}}", "")
        
        ### History 
        history_code = ''
        if row.m5_history_details is not None:
            for history in row.m5_history_details:
                history_line = f"<tr><td id='midpadding'>{history.HistDate}</td><td id='midpadding'>{history.HistType}</td><td id='midpadding'>{history.UserFullname}</td><td id='midpadding'>{history.HistoryComment}</td></tr>"
                history_code += history_line + "\n"
            html = html.replace("{{HistoryPlaceholder}}", history_code)
        else:
            html = html.replace("{{HistoryPlaceholder}}", "")

        ### Linked Files
        linked_files_code = ''
        if row.linked_files_details is not None:
            for likedfile in row.linked_files_details:
                linked_files_line = f"<tr><td id='midpadding'></td><td id='midpadding'>{likedfile.LinkNo}</td><td id='midpadding'>{likedfile.FullName}</td><td id='midpadding'>{likedfile.LinkDetailComment}</td></tr>"
                linked_files_code += linked_files_line + "\n"
            html = html.replace("{{LinkedFilesPlaceholder}}", linked_files_code)
        else:
            html = html.replace("{{LinkedFilesPlaceholder}}", "")


        # main typing - has no mapping

        ### Appeal Category
        appeal_category_code = ""
        if row.appeal_category_details is not None:
            for appeal_category in row.appeal_category_details:
                appeal_line = f"<tr><td id='midpadding'>{appeal_category.CategoryDescription}</td><td id='midpadding'>{appeal_category.Flag}</td><td id='midpadding'></td></tr>"
                appeal_category_code += appeal_line + " \n"
            html = html.replace("{{AppealPlaceholder}}", appeal_category_code)
        else:
            html = html.replace("{{AppealPlaceholder}}", "")


        ### Case Respondent

        for cd_row in row.Case_detail:
            if cd_row.CaseRespondent in respondent_mapping:
                current_respondent_mapping = respondent_mapping[cd_row.CaseRespondent]

                for resp_placeholder, resp_field_name in current_respondent_mapping.items():
                    if resp_field_name:
                        value = cd_row[resp_field_name]
                    else:
                        value = ""
                    html = html.replace(resp_placeholder,str(value))
            else:
                # logger.warn(f'Mapping not found for CaseRespondent: {row["CaseRespondent"]}, CaseNo: {row["m7.CaseNo"]}')
                current_respondent_mapping = {
                    "{{RespondentName}}": "",
                    "{{CaseRespondentAddress1}}": "",
                    "{{CaseRespondentAddress2}}": "",
                    "{{CaseRespondentAddress3}}": "",
                    "{{CaseRespondentAddress4}}": "",
                    "{{CaseRespondentAddress5}}": "",
                    "{{CaseRespondentPostcode}}": "",
                    "{{CaseRespondentTelephone}}": "",
                    "{{CaseRespondentFAX}}": "",
                    "{{CaseRespondentEmail}}": "",
                    "{{CaseRespondentRef}}": "",
                    "{{CaseRespondentContact}}": "",
                    "{{POU}}":"",
                    "{{RRrespondent}}":""
                }
                for resp_placeholder, resp_value in current_respondent_mapping.items():
                    html = html.replace(resp_placeholder,str(resp_value))

        ### status
        code = ""

        if row.all_status_objects is not None:
            for index,status in enumerate(row.all_status_objects,start=1):
                ## get the case status in the list
                case_status = int(status["CaseStatus"]) if status["CaseStatus"] is not None else 0

                ## set the margin and id counter
                if index == 1:
                    margin = "10px"
                else:
                    margin = "600px"

                counter = 30+index

                if case_status in case_status_mappings:
                    template = template_for_status[case_status]
                    template = template.replace("{{margin_placeholder}}",str(margin))
                    template = template.replace("{{index}}",str(counter))
                    status_mapping = case_status_mappings[case_status]



                    for placeholder,field_name in status_mapping.items():
                        if field_name in date_fields:
                            raw_value = status[field_name] if field_name in status else None
                            value = format_date_iso(raw_value)
                        else:
                            value = status[field_name] if field_name in status else None
                        template = template.replace(placeholder,str(value))
                    

                    labels = []

                    label_fields = ["UpperTribJudge", "DesJudgeFirstTier", "JudgeFirstTier", "NonLegalMember"]

                    for label in label_fields:
                        count = status[label] if status[label] is not None else 0
                        for i in range(count):
                            labels.append(f"{label}_{i+1}")

                    possitions = ["Position10","Position11","Position12"]

                    possitions_labelled = dict(zip(possitions,labels))

                    possitions_keys = list(possitions_labelled.keys())
                    possitions_values = list(possitions_labelled.values())


                    for i in range(1, 4):
                        label_placeholder = f"{{{{Label{i}}}}}"
                        value_placeholder = f"{{{{Label{i}value}}}}"

                        if i <= len(possitions_labelled):
                            label_value = possitions_values[i-1]
                            name_col = possitions_keys[i-1]
                            name_value = status[name_col] if status[name_col] is not None else ""
                            name_value = name_value if name_value else ""
                        else:
                            label_value = ""
                            name_value = ""

                        template = template.replace(label_placeholder, str(label_value))
                        template = template.replace(value_placeholder, str(name_value))



                            
                    if status["CourtClerkUsher"]:
                        template = template.replace("{{courtclerkusherplaceholder}}",status["CourtClerkUsher"])

                    else:
                        template = template.replace("{{courtclerkusherplaceholder}}",'N/A')




                    code += template + "\n"
                    
                        
                else:
                    # logger.info(f"Mapping not found for CaseStatus: {case_status}, CaseNo: {row['m7.CaseNo']}")
                    continue

        if code:
            html = html.replace("{{statusplaceholder}}",code)
        else:
            html = html.replace("{{statusplaceholder}}","")

        ## Add in Flag logic
        flag_list = []
        flag_dict = {}

        if row.Case_detail is not None:
            for casedetail in row.Case_detail:
                if casedetail.AppellantDetainedDesc == "HMP" or casedetail.AppellantDetainedDesc == "IRC" or casedetail.AppellantDetainedDesc == "Others":
                    flag_1 = "DET"
                    html = html.replace("{{flag1Placeholder}}", str(flag_1))
                if casedetail.InCamera == 1:
                    flag_2 = "CAM"
                    html = html.replace("{{flag2Placeholder}}", str(flag_2))
                else:
                    flag_1 = ""
                    flag_2 = ""
                    html = html.replace("{{flag1Placeholder}}", str(flag_1))
                    html = html.replace("{{flag2Placeholder}}", str(flag_2))

        if row.appeal_category_details is not None:
            for appealcategorydetails in row.appeal_category_details:
                flag_list.append(appealcategorydetails.Flag)

        flag_3 = " ".join(flag_list[:3])
        html = html.replace("{{flag3Placeholder}}", str(flag_3))

        # Financial supporter

        sponsor_name = "Financial Condiiton Suportor details entered" if row.financial_condition_details else "Financial Condiiton Suportor details not entered"

        html = html.replace("{{sponsorName}}",str(sponsor_name))

        case_surety_replacement = {
        "{{SponsorName}}":"CaseSuretyName",
        "{{SponsorForename}}":"CaseSuretyForenames",
        "{{SponsorTitle}}":"CaseSuretyTitle",
        "{{SponsorAddress1}}":"CaseSuretyAddress1",
        "{{SponsorAddress2}}":"CaseSuretyAddress2",
        "{{SponsorAddress3}}":"CaseSuretyAddress3",
        "{{SponsorAddress4}}":"CaseSuretyAddress4",
        "{{SponsorAddress5}}":"CaseSuretyAddress5",
        "{{SponsorPostcode}}":"CaseSuretyPostcode",
        "{{SponsorPhone}}":"CaseSuretyTelephone",
        "{{SponsorEmail}}":"CaseSuretyEmail",
        "{{AmountOfFinancialCondition}}":"AmountOfFinancialCondition",
        "{{SponsorSolicitor}}":"Solicitor",
        "{{SponserDateLodged}}":"CaseSuretyDateLodged",
        "{{SponsorLocation}}":"Location",
        "{{AmountOfSecurity}}": "AmountOfTotalSecurity"
        

    }

        financial_condition_code = ""

        if row.financial_condition_details is not None:
            details = row.financial_condition_details

            # Iterate over each record in financial_condition_details array
            for index, casesurety in enumerate(details, start=10):
                current_code = fcs_template  # Use the defined HTML template
                current_code = current_code.replace("{{Index}}", str(index))

                # Loop over each placeholder in the dictionary and replace with corresponding values
                for placeholder, col_name in case_surety_replacement.items():
                    # Check if the field exists in the current struct; fallback to empty string if not
                    value = casesurety[col_name] if col_name in casesurety and casesurety[col_name] is not None else ""
                    current_code = current_code.replace(placeholder, str(value))

                financial_condition_code += current_code + "\n"
            html = html.replace("{{financial_condition_code}}",financial_condition_code)
        else:
            html = html.replace("{{financial_condition_code}}","")

        html = html.replace("{{SecondaryLanguage}}",str(row.SecondaryLanguage))


        return html
    
    except Exception as e:
        return f"Failure Error: {e}"
    


# Register the UDF
create_html_udf = udf(create_html_column, StringType())

# COMMAND ----------


# # Pass columns as a struct
# html_mc = final_staging_bails_df.withColumn("content", create_html_udf(struct(*final_staging_bails_df.columns)))

# html_mc.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up 'Write' Funciton

# COMMAND ----------

secret = secret = dbutils.secrets.get("ingest00-keyvault-sbox", "curatedsbox-connection-string-sbox")
 
 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
 
# Set up the BlobServiceClient with your connection string
connection_string = f"BlobEndpoint=https://ingest00curatedsbox.blob.core.windows.net/;QueueEndpoint=https://ingest00curatedsbox.queue.core.windows.net/;FileEndpoint=https://ingest00curatedsbox.file.core.windows.net/;TableEndpoint=https://ingest00curatedsbox.table.core.windows.net/;SharedAccessSignature={secret}"
 
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
 
# Specify the container name
container_name = "gold"
container_client = blob_service_client.get_container_client(container_name)

# COMMAND ----------

def upload_to_blob(file_name, file_content):
    try:
        # blob_client = container_client.get_blob_client(f"{gold_outputs}/HTML/{file_name}")
        blob_client = container_client.get_blob_client(f"{file_name}")
        blob_client.upload_blob(file_content, overwrite=True)
        return "success"
    except Exception as e:
        return f"error: {str(e)}"
 


upload_to_blob_udf = udf(upload_to_blob, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create HTML Content

# COMMAND ----------


@dlt.table(
    name="create_bails_html_content",
    comment="create the HTML content for bails and add a fail name",
    path=f"{silver_mnt}/bail_html_content")
# 
def create_bails_html_content():
    df = dlt.read("final_staging_bails")

    results_df = df.withColumn("HTMLContent", create_html_udf(struct(*df.columns))).withColumn("HTML_File_path", concat(lit(f"{gold_html_outputs}bails_"), regexp_replace(trim(col("CaseNo")), "/", "_"), lit(f".html")))

    results_df = results_df.withColumn("HTML_status",when(col("HTMLContent").contains("Failure Error:"), "Failure on Create Content")
    .otherwise("Successful creating HTML Content") )


    ## Create and save audit log for this table
    df = results_df.withColumn("File_name", col("HTML_File_path"))
    df = df.withColumnRenamed("HTML_Status","Status")
    table_name = "create_bails_html_content"
    stage_name = "staging_stage"

    description = "The create_bails_html_content table generates HTML content for bail cases, transforming structured case data into formatted HTML outputs. It reads from final_staging_bails, processes the data using the create_html function, and assigns a unique file name for each case, ensuring proper formatting by replacing special characters in case numbers. This table supports automated document generation for bail case summaries, facilitating downstream reporting, storage, and retrieval."

    unique_identifier_desc = "CaseNo"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description,["File_name","Status"])


    return results_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## END

# COMMAND ----------

# MAGIC %md
# MAGIC ## JSON Combined Dev

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create JSON Content

# COMMAND ----------

from pyspark.sql.functions import to_json,struct


@dlt.table(
    name="create_bails_json_content",
    comment="create the JSON content for bails and add a fail name",
    path=f"{silver_mnt}/bail_json_content",
)
def create_bails_json_content():
    try:
        m1_m2_m3_m4_m5_m6_m7_m8_cs_lc_df = dlt.read("final_staging_bails")

        df_with_json = m1_m2_m3_m4_m5_m6_m7_m8_cs_lc_df.withColumn("JSONContent", to_json(struct("*"))).withColumn("JSON_File_path", concat(lit(f"{gold_json_outputs}bails_"), regexp_replace(trim(col("CaseNo")), "/", "_"), lit(f".json")))

        df_with_json = df_with_json.withColumn("JSON_status",when(col("JSONContent").contains("Error"), "Failure on Create JSON Content").otherwise("Successful creating JSON Content"))

        # df_with_json.display()

        df = df_with_json.withColumn("File_name",col("JSON_File_path")).withColumnRenamed("JSON_Status","Status")

        ## Create and save audit log for this table
        table_name = "create_bails_json_content"
        stage_name = "gold_stage"

        description = "The create_bails_json_content table generates JSON content for bail cases, transforming structured case data into serialized JSON format. It reads from final_staging_bails, converts all fields into a nested JSON structure, and assigns a unique file name for each case by replacing special characters in case numbers. This table facilitates automated JSON document generation, enabling structured data storage, retrieval, and downstream processing for bail case management and reporting."

        unique_identifier_desc = "CaseNo"

        create_audit_df(df,unique_identifier_desc,table_name,stage_name,description,["File_name","Status"])


        return df_with_json
    except Exception as e:
        return f"Failure error: {str(e)}"



# COMMAND ----------

# MAGIC %md
# MAGIC ## A360 Code

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create A360 Content

# COMMAND ----------

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
                "bf_003": row.bf_003 or "",
                "bf_004": str(row.bf_004) or "",
                "bf_005": row.bf_005 or ""
            }
        }
 
        html_data = {
            "operation": "upload_new_file",
            "relation_id": row.client_identifier,
            "file_metadata": {
                "publisher": row.publisher,
                "dz_file_name": row.HTMLFileName,
                "file_tag": "html"
            }
        }
 
        json_data = {
            "operation": "upload_new_file",
            "relation_id": row.client_identifier,
            "file_metadata": {
                "publisher": row.publisher,
                "dz_file_name": row.JSONFileName,
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
        return f"Failure: Error generating A360 for client_identifier {row.client_identifier}: {e}"
 
# Register UDF
generate_a360_udf = udf(generate_a360, StringType())
 



# COMMAND ----------

@dlt.table(name="create_bails_a360_content", 
           comment="A360 content for bails",
           path=f"{gold_mnt}/Data/create_bails_a360_content")

def gold_bails_with_a360():
    """
    This function generates A360 content batchs them in 250 unique_ids in a batch and uploads to blob storage
    """
    ## Load in the metadata table adding in the file names and construct an a360 content string for each unique id
    metadata_df = (
        dlt.read("silver_bail_meta_data").withColumn(
            "JSONFileName",
            concat(
                lit("bails_"),
                regexp_replace(trim(col("client_identifier")), "/", "_"),
                lit(".json")
            )
        ).withColumn(
            "HTMLFileName",
            concat(
                lit("bails_"),
                regexp_replace(trim(col("client_identifier")), "/", "_"),
                lit(".html")
            )
        )
    )
    metadata_df = metadata_df.withColumn("A360Content", generate_a360_udf(struct(*metadata_df.columns)))

    metadata_df = metadata_df.withColumn("A360_Status",when(col("A360Content").contains("Failure"), "Failure on Creating A360 Content").otherwise("Successful creating A360 Content"))

    df = metadata_df.withColumnRenamed("A360_Status", "Status")

    ## Create and save audit log for this table
    table_name = "create_bails_a360_content"
    stage_name = "gold_stage"

    description = "The create_bails_json_content table generates JSON content for bail cases, transforming structured case data into serialized JSON format. It reads from final_staging_bails, converts all fields into a nested JSON structure, and assigns a unique file name for each case by replacing special characters in case numbers. This table facilitates automated JSON document generation, enabling structured data storage, retrieval, and downstream processing for bail case management and reporting."

    unique_identifier_desc = "client_identifier"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description,["Status"])


    return metadata_df

# COMMAND ----------

# MAGIC %md
# MAGIC # Final Unified Table

# COMMAND ----------


@dlt.table(name="gold_bails_HTML_JSON_a360", comment="A360 content for bails", path=f"{gold_mnt}/Data/gold_bails_HTML_JSON_a360")
@dlt.expect_or_drop("No errors in HTML content", "NOT (lower(HTMLContent) LIKE '%failure%')")
@dlt.expect_or_drop("No errors in JSON content", "NOT (lower(JSONContent) LIKE '%failure%')")
@dlt.expect_or_drop("No errors in A360 content", "NOT (lower(A360Content) LIKE '%failure%')")


def gold_bails_HTML_JSON_with_a360():
    a360_df = dlt.read("create_bails_a360_content").alias("a360")
    html_df = dlt.read("create_bails_html_content").alias("html")
    json_df = dlt.read("create_bails_json_content").alias("json")

    # Rename CaseNo to match client_identifier for correct joins
    json_df = json_df.withColumnRenamed("CaseNo", "client_identifier")
    html_df = html_df.withColumnRenamed("CaseNo", "client_identifier")

    columns_list = [c for c in json_df.columns if c not in html_df.columns]

    unified_df = (
        html_df.join(json_df.select("client_identifier", *columns_list), on="client_identifier", how="inner").join(a360_df, on="client_identifier", how="inner")
    )

    distinct_ids = unified_df.select("client_identifier").distinct().orderBy("client_identifier")
    window_spec = Window.orderBy("client_identifier")
    distinct_ids_with_window = distinct_ids.withColumn("row_num", row_number().over(window_spec))
    distinct_ids_with_window = distinct_ids_with_window.withColumn("batchid2", floor((col("row_num")-1) / 250) +1 ).drop("row_num")

    unified_df = unified_df.join(distinct_ids_with_window, on="client_identifier", how="left")

    df = unified_df.withColumn("batchid", col("batchid2"))

    ## Create and save audit log for this table
    table_name = "gold_bails_HTML_JSON_a360"
    stage_name = "gold_stage"

    description = "The gold_bails_HTML_JSON_a360 table consolidates structured bail case content, integrating HTML, JSON, and A360 formats for A360 processing. It ensures data integrity by enforcing quality checks to exclude records containing errors in any content format and organizes cases into batches for streamlined processing."

    unique_identifier_desc = "client_identifier"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description,["batchid"])



    return unified_df

# COMMAND ----------

# MAGIC %md
# MAGIC # Save HMTL

# COMMAND ----------

@dlt.table(
    name="save_html_to_blob",
    comment="upload HTML content to blob storage",
    path=f"{silver_mnt}/save_html_to_blob",
)
def save_html_to_blob():
    results_df = dlt.read("gold_bails_HTML_JSON_a360")

    repartioned_df = results_df.repartition(64)

    df_html_with_status = repartioned_df.withColumn("Status", upload_to_blob_udf(col("HTML_File_path"),col("HTMLContent")))

    # df_html_with_status.display()

    ## Create and save audit log for this table
    df = df_html_with_status.withColumn("File_name", col("HTMLFileName"))
    table_name = "save_html_to_blob"
    stage_name = "gold_stage"

    description = "The save_html_to_blob table is responsible for uploading generated HTML content for bail cases to blob storage. It reads from create_bails_html_content, repartitions the data for optimized parallel processing, and applies the upload_to_blob_udf function to transfer each HTML file to blob storage. A new column, HTMLTransferStatus, is added to track the status of each upload, ensuring visibility into successful and failed transfers. This table facilitates automated storage and retrieval of bail case HTML documents for further processing and access."

    unique_identifier_desc = "client_identifier"

    create_audit_df(df,unique_identifier_desc,table_name,stage_name,description,["File_name","Status"])


    return df_html_with_status.withColumnRenamed("Status", "HTMLSaveStatus").select("HTMLFileName","HTMLContent","HTMLSaveStatus")


# COMMAND ----------

# MAGIC %md
# MAGIC # Save JSON

# COMMAND ----------

# df = spark.read.table("hive_metastore.aria_bails.gold_bails_HTML_JSON_a360")

# df.display()

# COMMAND ----------

@dlt.table(
  name="save_json_to_blob",
  comment="upload JSON content to blob storage",
  path=f"{silver_mnt}/save_json_to_blob",
)
def save_json_to_blob():
  df_with_json = dlt.read("gold_bails_HTML_JSON_a360")
  json_repartioned_df = df_with_json.repartition(64)

  df_json_with_status = json_repartioned_df.withColumn("Status", upload_to_blob_udf(col("JSON_File_path"), col("JSONContent")))

  ## Create and save audit log for this table
  df = df_json_with_status
  table_name = "save_json_to_blob"
  stage_name = "gold_stage"

  description = "The save_json_to_blob table is responsible for uploading generated JSON content for bail cases to blob storage. It reads from create_bails_json_content, repartitions the data for optimized parallel processing, and applies the upload_to_blob_udf function to transfer each JSON file to blob storage. A new column, Status, is added to track the upload status of each JSON file, ensuring visibility into successful and failed transfers. This table facilitates automated storage and retrieval of structured JSON case data for further processing and access."

  unique_identifier_desc = "client_identifier"

  create_audit_df(df, unique_identifier_desc, table_name, stage_name, description, ["JSONFileName", "Status"])

  return df_json_with_status.withColumnRenamed("Status", "JSONSaveStatus").select("JSONFileName", "JSONContent", "JSONSaveStatus")

# COMMAND ----------

# MAGIC %md
# MAGIC # Batch and Save A360
# MAGIC
# MAGIC

# COMMAND ----------

@dlt.table(name="gold_bails_a360", 
           comment="A360 content for bails",
           path=f"{gold_mnt}/Data/batched_A360_save_status")

def gold_bails_with_a360():
    """
    This function generates A360 content in batches of 250 unique_ids and uploads to blob storage
    """

    metadata_df = dlt.read("gold_bails_HTML_JSON_a360")

    ## Group by batchid and concat all the A360 content into a single string
    batched_a360_df = metadata_df.groupBy("batchid2").agg(concat_ws("\n", collect_list(col("A360Content"))).alias("A360Content")).withColumn("File_name", concat(lit(f"{gold_a360_outputs}bails_"), col("batchid2"), lit(".a360")))

    ## Publish these a360 files to the blob storage
    a360_result_df = batched_a360_df.withColumn("Status", upload_to_blob_udf(col("File_name"), col("A360Content")))

    a360_result_df = a360_result_df.withColumnRenamed("batchid2", "a360batchid")
    a360_result_df = a360_result_df.withColumn("batchid", col("a360batchid").cast("string"))


    ## Create and save audit log for this table
    table_name = "gold_bails_with_a360"
    stage_name = "gold_stage"

    description = "The gold_bails_a360 table takes A360 content for bail cases, batches them into groups of 250 unique IDs, and uploads them to blob storage. It constructs JSON and HTML file names for each case, generates A360-formatted content using the generate_a360_udf function, and consolidates records into batched files. The batched content is then uploaded to blob storage, with a status column tracking the success or failure of each upload. This table facilitates efficient processing, storage, and retrieval of A360-formatted bail case data for further use in legal and administrative workflows."

    unique_identifier_desc = "batchid"

    create_audit_df(a360_result_df, unique_identifier_desc, table_name, stage_name, description, ["File_name", "Status"])

    return a360_result_df

# COMMAND ----------


