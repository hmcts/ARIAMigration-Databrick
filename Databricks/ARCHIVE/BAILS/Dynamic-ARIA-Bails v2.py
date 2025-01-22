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
from pyspark.sql.functions import when, col,coalesce, current_timestamp, lit, date_format,desc, first,concat_ws,count,collect_list,struct,expr
# from pyspark.sql.functions import *
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pyspark.sql import DataFrame
import logging

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
landing_mnt = "/mnt/ingest00landingsboxlanding/"
bronze_mnt = "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/BAILS"
silver_mnt = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/BAILS"
gold_mnt = "/mnt/ingest00curatedsboxgold/ARIADM/ARM/BAILS"

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
    return (
        dlt.read("raw_appeal_cases").alias("ac")
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
            col("crep.RepresentativeRef").alias("CaseRepRepresentativeRef"),
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
    return (
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## M3: bronze_ bail_ac _cl_ht_list_lt_hc_c_ls_adj
# MAGIC
# MAGIC -- Data Mapping
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC     -- Status
# MAGIC     s.CaseNo,
# MAGIC     s.StatusId,
# MAGIC     s.Outcome,
# MAGIC     
# MAGIC     -- CaseList
# MAGIC     cl.TimeEstimate AS CaseListTimeEstimate,
# MAGIC     cl.StartTime AS CaseListStartTime,
# MAGIC     
# MAGIC     -- HearingType
# MAGIC     ht.Description AS HearingTypeDesc,
# MAGIC
# MAGIC     
# MAGIC     -- List
# MAGIC     l.ListName,
# MAGIC     l.StartTime AS ListStartTime,
# MAGIC     
# MAGIC     -- ListType
# MAGIC     lt.Description AS ListTypeDesc,
# MAGIC     lt.ListType,
# MAGIC     
# MAGIC     -- Court
# MAGIC     c.CourtName,
# MAGIC     
# MAGIC     -- HearingCentre
# MAGIC     hc.Description AS HearingCentreDesc,
# MAGIC     
# MAGIC     -- ListSitting
# MAGIC     ls.Chairman,
# MAGIC     
# MAGIC     -- Adjudicator
# MAGIC     a.Surname AS AdjudicatorSurname,
# MAGIC     a.Forenames AS AdjudicatorForenames,
# MAGIC     a.Title AS AdjudicatorTitle
# MAGIC
# MAGIC     --DecisionType
# MAGIC     dt.Description AS OutcomeDescription,
# MAGIC     --AppealCase
# MAGIC     ac.Notes
# MAGIC
# MAGIC FROM [ARIAREPORTS].[dbo].[Status] s
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CaseList] cl ON s.StatusId = cl.StatusId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingType] ht ON cl.HearingTypeId = ht.HearingTypeId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[List] l ON cl.ListId = l.ListId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[ListType] lt ON l.ListTypeId = lt.ListTypeId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Court] c ON l.CourtId = c.CourtId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingCentre] hc ON l.CentreId = hc.CentreId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[ListSitting] ls ON l.ListId = ls.ListId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Adjudicator] a ON ls.AdjudicatorId = a.AdjudicatorId;
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[DecisionType] dt
# MAGIC ON s.Outcome = dt.DecisionTypeId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[AppealCase] ac
# MAGIC ON s.CaseNo = ac.CaseNo
# MAGIC

# COMMAND ----------

@dlt.table(
    name="bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj",
    comment="ARIA Migration Archive Bails cases bronze table",
    partition_cols=["CaseNo"],
    path=f"{bronze_mnt}/bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj"
)
def bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj():
    return (
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
            # ListType
            col("lt.Description").alias("ListTypeDesc"),
            col("lt.ListType"),
            # Court
            col("c.CourtName"),
            # HearingCenter
            col("hc.Description").alias("HearingCentreDesc"),
            # ListSitting
            col("ls.Chairman"),
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
    return (
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
    return (
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
    return (
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
    return (
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
        .select(
            # -------------------------
            # STATUS fields (s)
            # -------------------------
            col("s.StatusId"),
            col("s.CaseNo"),
            col("s.CaseStatus"),
            col("s.DateReceived"),
            col("s.Notes1").alias("StatusNotes1"),

            # Date fields (varied on case status)
            col("s.Keydate"),
            col("s.MiscDate1"),
            col("s.MiscDate2"),
            col("s.MiscDate3"),

            col("s.Recognizance").alias("TotalAmountOfFinancialCondition"),
            col("s.Security").alias("TotalSecurity"),
            col("s.Notes2").alias("StatusNotes2"),
            col("s.DecisionDate"),
            col("s.Outcome"),
            col("dt.Description").alias("OutcomeDescription"),  # Matches the SQL
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
            col("s.ListedCentre"),
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
        )
    )


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
    return (
        dlt.read("raw_appeal_category").alias("ap")
        .join(dlt.read("raw_category").alias("c"), col("ap.CategoryId") == col("c.CategoryId"), "left_outer")
        .select(
            # AppealCategory fields
            col("ap.CaseNo"),
            # Category fields
            col("c.Description").alias("CategoryDescription"),
            col("c.Flag"),
        )
    )


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
    return (
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
    return (
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
    return (
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


    return final_normal_bail.select("ac.CaseNo")


# COMMAND ----------

# from pyspark.sql import functions as F

# # Define the function if you want to use it later
# # def silver_normal_bail():

# # Read the necessary raw data
# appeal_case = spark.read.table("hive_metastore.aria_bails.raw_appeal_cases").alias("ac")
# status = spark.read.table("hive_metastore.aria_bails.raw_status").alias("t")
# file_location = spark.read.table("hive_metastore.aria_bails.raw_file_location").alias("fl")
# history = spark.read.table("hive_metastore.aria_bails.raw_history").alias("h")

# # Create a subquery to get the max StatusId for each CaseNo
# max_status_subquery = (
#     status
#     .withColumn("status_value", F.when(F.isnull(F.col("CaseStatus")), -1).otherwise(F.col("CaseStatus"))) # new column with the status value if null setting it to -1
#     .filter(F.col("status_value") != 17) # filter out status value of 17
#     .groupBy("CaseNo")  # group by case No
#     .agg(F.max("StatusId").alias("max_ID"))
# )
# max_status_subquery = max_status_subquery.select("CaseNo", "max_ID").alias("s")

# # Join the AppealCase to sub Query then status table then file location then history
# result = (
#     appeal_case
#     .join(max_status_subquery, F.col("ac.CaseNo") == F.col("s.CaseNo"), "left_outer")  
#     .join(status, (F.col("t.CaseNo") == F.col("s.CaseNo")) & 
#                (F.col("s.max_ID") == F.col("t.StatusId")), "left_outer")  
#     .join(file_location, F.col("fl.CaseNo") == F.col("ac.CaseNo"), "left_outer")
#     .join(history, F.col("h.CaseNo") == F.col("ac.CaseNo"), "left_outer")
# )
# result_filtered = result.filter(
#     (F.col("ac.CaseType") == 2) &
#     (F.col("fl.DeptId") != 519) &
#     (
#         (~F.col("fl.Note").like("%destroyed%")) &
#         (~F.col("fl.Note").like("%detroyed%")) &
#         (~F.col("fl.Note").like("%destoyed%")) & 
#         (~F.col("fl.Note").like("%distroyed%")) |
#         (F.col("fl.Note").isNull())
#     ))

# result_with_case = result_filtered.withColumn(
#     "case_result",
#     F.when(F.col("h.Comment").like("%indefinite retention%"), 'Legal Hold')
#      .when(F.col("h.Comment").like("%indefinate retention%"), 'Legal Hold')
#      .when(F.date_add(F.col("t.DecisionDate"), 2 * 365) < F.current_date(), 'Destroy')
#      .otherwise('Archive')
# )
# final_result = result_with_case.filter(F.col("case_result") == 'Archive')

# final_grouped_result = final_result.groupBy(
#     "ac.CaseNo",
# #     "cst.Description",  
# #     "dt.Description",  
#     "t.DecisionDate",
#     "fl.Note"
# ).agg(
#     F.count(F.lit(1)).alias("case_count")  # Aggregate count of cases
# )

# display(final_grouped_result)



# # Show the results for debugging
# # result.select("ac.CaseNo").groupBy("ac.CaseNo").count().alias("count").filter(F.col("count") > 1).show()


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

    return final_result.select("CaseNo")  


# COMMAND ----------

# MAGIC %md
# MAGIC ## Scottish Bails holding funds

# COMMAND ----------

# import from csv

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
    m1_df = dlt.read("bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang")
    return m1_df.select("*",
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
                        .otherwise("Unknown").alias("CostsAwardDecisionDesc")
                            
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## M2: silver_bail_m2_case_appellant

# COMMAND ----------

@dlt.table(name="silver_bail_m2_case_appellant",
           comment="ARIA Migration Archive Bails m2 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m2_case_appellant")
def silver_m2():
    m2_df = dlt.read("bronze_bail_ac_ca_apt_country_detc")
    return m2_df.select("*",
                        when(col("AppellantDetained") == 1,"HMP")
                        .when(col("AppellantDetained") == 2,"IRC")
                        .when(col("AppellantDetained") == 3,"No")
                        .when(col("AppellantDetained") == 4,"Other")
                        .otherwise("Unknown").alias("AppellantDetainedDesc"))

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
    m3_df = dlt.read("bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj")
    return (
    m3_df.groupBy(m3_grouped_cols).agg(
    concat_ws(" ",
    first(when(col("Chairman") == True, col("AdjudicatorTitle"))),
     first(when(col("Chairman") == True, col("AdjudicatorForenames"))),
     first(when(col("Chairman") == True, col("AdjudicatorSurname")))).alias("JudgeFT"),
    
    concat_ws(" ",
     first(when(col("Chairman") == False, col("AdjudicatorTitle"))),
     first(when(col("Chairman") == False, col("AdjudicatorForenames"))),
     first(when(col("Chairman") == False, col("AdjudicatorSurname")))).alias("CourtClerkUsher"),
     
     
     )
    )



# COMMAND ----------

# MAGIC %md
# MAGIC ## M4: silver_bail_m4_bf_diary

# COMMAND ----------

@dlt.table(name="silver_bail_m4_bf_diary",
           comment="ARIA Migration Archive Bails m4 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m4_bf_diary")
def silver_m4():
    m4_df = dlt.read("bronze_bail_ac_bfdiary_bftype")
    return m4_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## M5: silver_bail_m5_history

# COMMAND ----------

@dlt.table(name="silver_bail_m5_history",
           comment="ARIA Migration Archive Bails m5 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m5_history")
def silver_m5():
    m5_df = dlt.read("bronze_bail_ac_history_users")
    return m5_df


# COMMAND ----------

# MAGIC %md
# MAGIC ## M6: silver_bail_m6_link

# COMMAND ----------

@dlt.table(name="silver_bail_m6_link",
           comment="ARIA Migration Archive Bails m6 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m6_link")
def silver_m6():
    m6_df = dlt.read("bronze_bail_ac_link_linkdetail")

    return m6_df.select("CaseNo", "LinkNo", "LinkDetailComment", concat_ws(" ",
        col("Title"),col("Forenames"),col("Name")).alias("FullName")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## M7: silver_bail_m7_status

# COMMAND ----------

@dlt.table(name="silver_bail_m7_status",
           comment="ARIA Migration Archive Bails m7 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m7_status")
def silver_m7():
    m7_df = dlt.read("bronze_bail_status_sc_ra_cs")
    return m7_df.select("*",
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


# COMMAND ----------

# MAGIC %md
# MAGIC ## M8: silver_bail_m8

# COMMAND ----------

@dlt.table(name="silver_bail_m8",
           comment="ARIA Migration Archive Bails m8 silver table",
           partition_cols=["CaseNo"],
           path=f"{silver_mnt}/silver_bail_m8")
def silver_m8():
    m8_df = dlt.read("bronze_bail_ac_appealcategory_category")
    return m8_df

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Output Code

# COMMAND ----------

# # load bails html file

# bails_html_path = "/dbfs/mnt/ingest00landingsboxhtml-template/bail-no-js.html"

# with open(bails_html_path, "r") as f:
#     html_template = "".join(l for l in f)

# # displayHTML(html=html_template)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code start: import template

# COMMAND ----------



# COMMAND ----------


# Read in the normal bails cases
normal_bails = spark.read.table("hive_metastore.aria_bails.silver_normal_bail")

# Read in the HTML template for bails
bails_html_path = "/dbfs/mnt/ingest00landingsboxhtml-template/bails-no-js-v2.html"

with open(bails_html_path, "r") as f:
    html_template = f.read()

# displayHTML(html=html_template)

# Get the Normal Bail CaseNo
rows = normal_bails.select("CaseNo").collect()
caseno_list = [row[0] for row in rows]

number_to_test = 2

#development using 2 test case no
test_case_no = caseno_list[0:number_to_test]
print(test_case_no)

# filter m1 for the test case no
m1 = spark.read.table("hive_metastore.aria_bails.silver_bail_m1_case_details").filter(col("CaseNo").isin(test_case_no)).alias("m1")

m2 = spark.read.table("hive_metastore.aria_bails.silver_bail_m2_case_appellant").filter(col("Relationship").isNull()).filter(col("CaseNo").isin(test_case_no)).alias("m2")

# Join M1 and M2 tables
m1_m2 = m1.join(
    m2,col("m1.CaseNo") == col("m2.CaseNo")
)
                                                                                                                                    
# read in all tables
case_surety = spark.read.table("hive_metastore.aria_bails.bronze_case_surety_query").filter(col("CaseNo").isin(test_case_no))
m3 = spark.read.table("hive_metastore.aria_bails.silver_bail_m3_hearing_details").filter(col("CaseNo").isin(test_case_no))
m4 = spark.read.table("hive_metastore.aria_bails.silver_bail_m4_bf_diary").filter(col("CaseNo").isin(test_case_no))
m5 = spark.read.table("hive_metastore.aria_bails.silver_bail_m5_history").filter(col("CaseNo").isin(test_case_no))
m6 = spark.read.table("hive_metastore.aria_bails.silver_bail_m6_link").filter(col("CaseNo").isin(test_case_no))
m7 = spark.read.table("hive_metastore.aria_bails.silver_bail_m7_status").filter(col("CaseNo").isin(test_case_no))
m8 = spark.read.table("hive_metastore.aria_bails.silver_bail_m8").filter(col("CaseNo").isin(test_case_no))

# Get all columns in m3 not in m7
m3_new_columns = [col_name for col_name in m3.columns if col_name not in m7.columns]

status_tab = m7.alias("m7").join(
    m3.select("CaseNo", "StatusId", *m3_new_columns).alias("m3"),
    (col("m7.CaseNo") == col("m3.CaseNo")) & (col("m7.StatusId") == col("m3.StatusId")),
    "left"
)

# Case status Mapping
case_status_mappings = {
    11: {  # Scottish Payment Liability
        "{{ScottishPaymentLiabilityStatusOfBail}}": "CaseStatusDescription",
        "{{ScottishPaymentLiabilityDateOfOrder}}": "DateReceived",
        "{{ScottishPaymentLiabilityDateOfHearing}}": "Keydate",
        "{{ScottishPaymentLiabilityFC}}": "FC",
        "{{ScottishPaymentLiabilityInterpreterRequired}}": "InterpreterRequired",
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
    4: {  # Bail Application
        "{{BailApplicationStatusOfBail}}": "CaseStatusDescription",
        "{{BailApplicationDateOfApplication}}": "DateReceived",
        "{{BailApplicationDateOfHearing}}": "DecisionDate",
        "{{BailApplicationFC}}": "FC",
        "{{BailApplicationInterpreterRequired}}": "InterpreterRequired",
        "{{BailApplicationDateOfOrder}}": "MiscDate2",
        "{{BailApplicationTotalAmountOfFinancialCondition}}": "TotalAmountOfFinancialCondition",
        "{{BailApplicationBailCondition}}": "BailConditions",
        "{{BailApplicationTotalSecurity}}": "TotalSecurity",
        "{{BailApplicationDateDischarged}}": "MiscDate1",
        "{{BailApplicationRemovalDate}}": "MiscDate3",
        "{{BailApplicationVideoLink}}": "VideoLink",
        "{{BailApplicationResidenceOrderMade}}": "ResidenceOrder",
        "{{BailApplicationReportingOrderMade}}": "ReportingOrder",
        "{{BailApplicationBailedTimePlace}}": "BailedTimePlace",
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
        "{{PaymentLiabilityInterpreterRequired}}": "InterpreterRequired",
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
        "{{BailRenewalInterpreterRequired}}": "InterpreterRequired",
        "{{BailRenewalDateOfOrder}}": "MiscDate2",
        "{{BailRenewalTotalAmountOfFinancialCondition}}": "TotalAmountOfFinancialCondition",
        "{{BailRenewalBailCondition}}": "BailConditions",
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
        "{{BailVariationInterpreterRequired}}": "InterpreterRequired",
        "{{BailVariationDateOfOrder}}": "MiscDate1",
        "{{BailVariationTotalAmountOfFinancialCondition}}": "TotalAmountOfFinancialCondition",
        "{{BailVariationBailCondition}}": "BailConditions",
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
    }
}

date_fields = {
    "DateReceived", "Keydate", "MiscDate1", "MiscDate2", "MiscDate3",
    "DecisionDate", "DateOfOrder", "DatePaymentInstructions", 
    "DateChequesIssued", "DateCautionLodged", "HOConsentDate","DateReceived","MiscDate1","DecisionDate","DecisionSentToHODate"
}


display(m1_m2)


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
bail_Application_path = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/Status-template-1.html"
bail_Variation_path = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/Status-template-2.html"
bail_Renewal_path = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/Status-template-3.html"
bail_Payment_path = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/Status-template-4.html"
bail_Scottish_Payment_Liability_path = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/Status-template-5.html"
bail_Lodgement_path = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/Status-template-6.html"

# Path of the  bail HTML template
bails_html_path = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/template_bail_v0.3.html"


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


# displayHTML(bails_html_dyn)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ** DEV: Status tab

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join M3 and M7 to Master table

# COMMAND ----------


# create a nested list for the stausus table (m7_m3 tables)

status_tab_struct = struct(
            col("m7.StatusId"),
            col("m7.CaseNo"),
            col("CaseStatus"),
            col("DateReceived"),
            col("StatusNotes1"),
            col("Keydate"),
            col("MiscDate1"),
            col("MiscDate2"),
            col("MiscDate3"),
            col("TotalAmountOfFinancialCondition"),
            col("TotalSecurity"),
            col("StatusNotes2"),
            col("DecisionDate"),
            col("Outcome"),
            col("OutcomeDescription"),
            col("StatusPromulgated"),
            col("StatusParty"),
            col("ResidenceOrder"),
            col("ReportingOrder"),
            col("BailedTimePlace"),
            col("BaileddateHearing"),
            col("InterpreterRequired"),
            col("BailConditions"),
            col("LivesAndSleepsAt"),
            col("AppearBefore"),
            col("ReportTo"),
            col("AdjournmentParentStatusId"),
            col("ListedCentre"),
            col("DecisionSentToHO"),
            col("DecisionSentToHODate"),
            col("VideoLink"),
            col("WorkAndStudyRestriction"),
            col("StatusBailConditionTagging"),
            col("OtherCondition"),
            col("OutcomeReasons"),
            col("FC"),
            col("CaseStatusDescription"),
            col("ContactStatus"),
            col("SCCourtName"),
            col("SCAddress1"),
            col("SCAddress2"),
            col("SCAddress3"),
            col("SCAddress4"),
            col("SCAddress5"),
            col("SCPostcode"),
            col("SCTelephone"),
            col("LanguageDescription"),
            col("CaseListTimeEstimate"),
            col("CaseListStartTime"),
            col("HearingTypeDesc"),
            col("ListName"),
            col("HearingDate"),
            col("ListStartTime"),
            col("ListTypeDesc"),
            col("ListType"),
            col("CourtName"),
            col("HearingCentreDesc"),
            col("JudgeFT"),
            col("CourtClerkUsher")
        )
m7_m3_statuses = (
    status_tab
    .groupBy(col("m7.CaseNo"))
    .agg(
        collect_list(
            # Collect each record's columns as a struct
            status_tab_struct
        ).alias("all_status_objects")
    )
)

# COMMAND ----------

# Logic to add the max status for each caseno


# Create a SQL-compatible named_struct that matches the schema of all_status_objects
status_tab_struct_sql = """
    named_struct(
        'StatusId', 0,
        'CaseNo', '',
        'CaseStatus', '',
        'DateReceived', cast(null as timestamp),
        'StatusNotes1', '',
        'Keydate', cast(null as timestamp),
        'MiscDate1', cast(null as timestamp),
        'MiscDate2', cast(null as timestamp),
        'MiscDate3', cast(null as timestamp),
        'TotalAmountOfFinancialCondition', cast(0.0 as decimal(19,4)),
        'TotalSecurity', cast(0.0 as decimal(19,4)),
        'StatusNotes2', '',
        'DecisionDate', cast(null as timestamp),
        'Outcome', 0,
        'OutcomeDescription', '',
        'StatusPromulgated', cast(null as timestamp),
        'StatusParty', 0,
        'ResidenceOrder', 0,
        'ReportingOrder', 0,
        'BailedTimePlace', 0,
        'BaileddateHearing', 0,
        'InterpreterRequired', 0,
        'BailConditions', 0,
        'LivesAndSleepsAt', '',
        'AppearBefore', '',
        'ReportTo', '',
        'AdjournmentParentStatusId', 0,
        'ListedCentre', '',
        'DecisionSentToHO', 0,
        'DecisionSentToHODate', cast(null as timestamp),
        'VideoLink', false,
        'WorkAndStudyRestriction', '',
        'StatusBailConditionTagging', '',
        'OtherCondition', '',
        'OutcomeReasons', '',
        'FC', false,
        'CaseStatusDescription', '',
        'ContactStatus', '',
        'SCCourtName', '',
        'SCAddress1', '',
        'SCAddress2', '',
        'SCAddress3', '',
        'SCAddress4', '',
        'SCAddress5', '',
        'SCPostcode', '',
        'SCTelephone', '',
        'LanguageDescription', '',
        'CaseListTimeEstimate', 0,
        'CaseListStartTime', cast(null as timestamp),
        'HearingTypeDesc', '',
        'ListName', '',
        'HearingDate', cast(null as timestamp),
        'ListStartTime', cast(null as timestamp),
        'ListTypeDesc', '',
        'ListType', 0,
        'CourtName', '',
        'HearingCentreDesc', '',
        'JudgeFT', '',
        'CourtClerkUsher', ''
    )
"""

final_m7_m3_df = m7_m3_statuses.select(
    col("m7.CaseNo"),
    expr(f"""
        aggregate(
            all_status_objects,
            {status_tab_struct_sql},
            (acc, x) -> 
                CASE 
                    WHEN x.StatusId > acc.StatusId THEN x 
                    ELSE acc 
                END
        ).CaseStatusDescription
    """).alias("MaxCaseStatusDescription"),
    expr(f"""
        aggregate(
            all_status_objects,
            {status_tab_struct_sql},
            (acc, x) -> 
                CASE 
                    WHEN x.LanguageDescription is not null THEN x 
                    ELSE acc 
                END
        ).LanguageDescription
    """).alias("SecondaryLanguage")
)
display(final_m7_m3_df)
# Show the result
# display(max_status_df)


# COMMAND ----------

final_m7_m3_statuses = m7_m3_statuses.join(final_m7_m3_df, "CaseNo", "left_outer")

display(final_m7_m3_statuses)

# COMMAND ----------




# Join status stab to main table m1_m2 table
m1_m2_m3_m7_df = m1_m2.join(final_m7_m3_statuses, "CaseNo", "left_outer")
display(m1_m2_m3_m7_df)


# Dictionary to map status codes to html templates
template_for_status = { 4: bail_Application,
                       6: bail_Payment,
                       8: bail_Lodgement,
                       11: bail_Scottish_Payment_Liability,
                       18: bail_Renewal,
                       19: bail_Variation

}



for row in m1_m2_m3_m7_df.collect():
    case_number = row["CaseNo"]

    code = ""
    temp_template = bails_html_dyn



    for status in row["all_status_objects"] or []:
        case_status = int(status["CaseStatus"]) if status["CaseStatus"] is not None else 0

        if case_status in case_status_mappings:
            print("mapping found")
            print(case_status)
            template = template_for_status[case_status]
            template = template.replace("{{index}}",str(case_status))
            status_mapping = case_status_mappings[case_status]



            for placeholder,field_name in status_mapping.items():
                if field_name in date_fields:
                    raw_value = status[field_name] if field_name in status else None
                    value = format_date(raw_value)
                else:
                    value = status[field_name] if field_name in status else None
                template = template.replace(placeholder,str(value))
            code += template + "\n"
            
                
        else:
            (print("mapping not found"))

    temp_template = temp_template.replace("{{statusplaceholder}}",code)
    # displayHTML(temp_template)

# COMMAND ----------

# hearing detail





# COMMAND ----------

# MAGIC %md
# MAGIC ### Join M5 to master table

# COMMAND ----------

# History table

# History 
m5_filtered = m5.filter(F.col("CaseNo") == case_number)
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

display(m5_history_enriched)

m1_m2_m3_m5_m7_df = m1_m2_m3_m7_df.join(m5_history_enriched, "CaseNo", "left")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Join M4 to master table

# COMMAND ----------

    # BF diary M4 Tables

m4_bfdiary_df = m4.groupBy(col("CaseNo")).agg(
    collect_list(
        struct(
            col("BFDate"),
            col("BFTypeDescription"),
            col("Entry"),
            col("DateCompleted")
            )).alias("bfdiary_details"))

m1_m2_m3_m4_m5_m7_df = m1_m2_m3_m5_m7_df.join(m4_bfdiary_df, "CaseNo", "left")

display(m1_m2_m3_m4_m5_m7_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Join M8 to Master table

# COMMAND ----------

    # Appeal Category
m8_filtered = m8.filter(F.col("CaseNo") == case_number)

m8_appeal_category_df = m8.groupBy(col("CaseNo")).agg(
    collect_list(
        struct(
            col("CategoryDescription"),
            col("Flag")
            )).alias("appeal_category_details"))

m1_m2_m3_m4_m5_m7_m8_df = m1_m2_m3_m4_m5_m7_df.join(m8_appeal_category_df, "CaseNo", "left")

display(m1_m2_m3_m4_m5_m7_m8_df)





# COMMAND ----------

# MAGIC %md
# MAGIC ### Join M6 to master table

# COMMAND ----------

# # Linked Files
m6_filtered = m6.filter(F.col("CaseNo") == case_number)

m6_linked_files_df = m6.groupBy(col("CaseNo")).agg(
    collect_list(
        struct(
            col("LinkDetailComment"),
            col("LinkNo"),
            col("FullName"),
        )).alias("linked_files_details"))

m1_m2_m3_m4_m5_m6_m7_m8_df = m1_m2_m3_m4_m5_m7_m8_df.join(m6_linked_files_df, "CaseNo", "left")


display(m1_m2_m3_m4_m5_m6_m7_m8_df)


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

display(m1_m2_m3_m4_m5_m6_m7_m8_cs_df)




# COMMAND ----------

# MAGIC %md
# MAGIC ## HTML Combined Code 

# COMMAND ----------



# display(m1_m2)
for row in m1_m2_m3_m4_m5_m6_m7_m8_cs_df.collect():
    case_number = row["CaseNo"]
    # initialise html template
    html = bails_html_dyn
    
    # maintain cost award tab
    main_cost_award_code = f"<tr><td id='midpadding'>{row['CaseNo']}</td><td id='midpadding'>{row['AppellantName']}</td><td id='midpadding'>{row['AppealStage']}</td><td id='midpadding'>{row['DateOfApplication']}</td><td id='midpadding'>{row['TypeOfCostAward']}</td><td id='midpadding'>{row['ApplyingParty']}</td><td id='midpadding'>{row['PayingParty']}</td><td id='midpadding'>{row['MindedToAward']}</td><td id='midpadding'>{row['ObjectionToMindedToAward']}</td><td id='midpadding'>{row['CostsAwardDecision']}</td><td id='midpadding'></td><td id='midpadding'>{row['CostsAmount']}</td></tr>"


    m1_replacement = {
        "{{ bailCaseNo }}":row["CaseNo"] ,
        "{{ hoRef }}": row["HORef"] ,
        "{{ lastName }}": row["AppellantName"],
        "{{ firstName }}" : row["AppellantForenames"],
        "{{ birthDate }}": format_date(row["AppellantBirthDate"]),
        "{{ portRef }}": row["PortReference"],
        "{{AppellantTitle}}": row["AppellantTitle"],
        ## Main section
        "{{BailType}}": row["BailType"],
        "{{AppealCategoriesField}}": row["AppealCategories"],
        "{{Nationality}}":row["Nationality"],
        "{{TravelOrigin}}":row["CountryOfTravelOrigin"],
        "{{Port}}":row["PortOfEntry"],
        "{{DateOfReceipt}}":format_date(row["DateReceived"]),
        "{{DedicatedHearingCentre}}":row["DedicatedHearingCentre"],
        "{{DateNoticeServed}}":format_date(row["DateServed"]) ,
        "{{CurrentStatus}}": row["MaxCaseStatusDescription"],
        "{{ConnectedFiles}}":"",
        "{{DateOfIssue}}":format_date(row["DateOfIssue"]),
        # "{{NextHearingDate}}":row["DateOfNextListedHearing"],
        "{{LastDocument}}": row["last_document"],
        "{{FileLocation}}": row["file_location"],
        "{{BFEntry}}":"",
        "{{ProvisionalDestructionDate}}":format_date(row["ProvisionalDestructionDate"]),

        # Parties Tab - Applicant Section
        "{{Centre}}": row["DetentionCentre"],
        "{{AddressLine1}}": row["DetentionCentreAddress1"],
        "{{AddressLine2}}": row["DetentionCentreAddress2"],
        "{{AddressLine3}}": row["DetentionCentreAddress3"],
        "{{AddressLine4}}": row["DetentionCentreAddress4"],
        "{{AddressLine5}}": row["DetentionCentreAddress5"],
        "{{Postcode}}": row["DetentionCentrePostcode"],
        "{{Country}}": row["CountryOfTravelOrigin"],
        "{{phone}}": row["AppellantTelephone"],
        "{{email}}": row["AppellantEmail"],
        "{{PrisonRef}}": row["AppellantPrisonRef"],
        
        
        # Respondent Section
        "{{Detained}}":row["AppellantDetained"],
        "{{RespondentName}}":row["MainRespondentName"],
        "{{repName}}":row["CaseRepName"],
        "{{InterpreterRequirementsLanguage}}" : row["InterpreterRequirementsLanguage"],
        "{{HOInterpreter}}" : row["HOInterpreter"],
        "{{CourtPreference}}" : row["CourtPreference"],
        "{{language}}": row["Language"],
        "{{required}}": 1 if row["InterpreterRequirementsLanguage"] is not None else 0,

        # Misc Tab
        "{{Notes}}" : row["AppealCaseNote"],

        # Maintain cost awards Tab

        # Representative Tab
        "{{RepName}}":row["CaseRepName"],
        "{{CaseRepAddress1}}": row["CaseRepAddress1"],
        "{{CaseRepAddress2}}": row["CaseRepAddress2"],
        "{{CaseRepAddress3}}": row["CaseRepAddress3"],
        "{{CaseRepAddress4}}": row["CaseRepAddress4"],
        "{{CaseRepAddress5}}": row["CaseRepAddress5"],
        "{{CaseRepPostcode}}": row["CaseRepPostcode"],
        "{{CaseRepTelephone}}": row["CaseRepPhone"],
        "{{CaseRepFAX}}": row["CaseRepFax"],
        "{{CaseRepEmail}}": row["CaseRepEmail"],
        "{{RepDxNo1}}": row["RepDxNo1"],
        "{{RepDxNo2}}": row["RepDxNo2"],
        # "{{RepLAARefNo}}": row["CaseRepLSCCommission"],
        "{{RepLAACommission}}":row["CaseRepLSCCommission"],
        #File specific contact



        # Respondent Tab
        "{{RespondentName}}":row["CaseRespondent"],
        "{{CaseRespondentAddress1}}": row["RespondentAddress1"],
        "{{CaseRespondentAddress2}}": row["RespondentAddress2"],
        "{{CaseRespondentAddress3}}": row["RespondentAddress3"],
        "{{CaseRespondentAddress4}}": row["RespondentAddress4"],
        "{{CaseRespondentAddress5}}": row["RespondentAddress5"],
        "{{CaseRespondentPostcode}}": row["RespondentPostcode"],
        "{{CaseRespondentTelephone}}": row["RespondentTelephone"],
        "{{CaseRespondentFAX}}": row["RespondentFax"],
        "{{CaseRespondentEmail}}": row["RespondentEmail"],
        "{{CaseRespondentRef}}":row["CaseRespondentReference"],
        "{{CaseRespondentContact}}":row["CaseRespondentContact"],



        # Status Tab - Additional Language
        "{{PrimaryLanguage}}":row["Language"],
        "{{SecondaryLanguage}}":row["SecondaryLanguage"],

        # Parties Tab
        # "{{Detained}}": row[""]
        "{{Centre}}":row["DetentionCentre"],



        # Financial Condition supporter
        # which case surty do we use


        # status - Hearing details tab
        # need logic to filter which hearing details to use using latest date

        
        } 
    # BF diary 
    bf_diary_code = ""
    for bfdiary in row["bfdiary_details"]:
        bf_line = f"<tr><td id=\"midpadding\">{bfdiary['BFDate']}</td><td id=\"midpadding\">{bfdiary['BFTypeDescription']}</td><td id=\"midpadding\">{bfdiary['Entry']}</td><td id=\"midpadding\">{bfdiary['DateCompleted']}</td></tr>"
        bf_diary_code += bf_line + "\n"
    
    # History 
    history_code = ''
    for history in row["m5_history_details"] or []:
        history_line = f"<tr><td id='midpadding'>{history['HistDate']}</td><td id='midpadding'>{history['HistType']}</td><td id='midpadding'>{history['UserFullname']}</td><td id='midpadding'>{history['HistoryComment']}</td></tr>"
        history_code += history_line + "\n"

    # # Linked Files
    linked_files_code = ''
    for likedfile in row["linked_files_details"] or []:
        linked_files_line = f"<tr><td id='midpadding'></td><td id='midpadding'>{likedfile['CaseNo']}</td><td id='midpadding'>{likedfile['FullName']}</td><td id='midpadding'>{likedfile['LinkDetailComment']}</td></tr>"
        linked_files_code += linked_files_line + "\n"

    # # main typing - has no mapping

    # Appeal Category
    appeal_category_code = ""
    for appeal_category in row["appeal_category_details"] or []:
        appeal_line = f"<tr><td id='midpadding'>{appeal_category['CategoryDescription']}</td><td id='midpadding'>{appeal_category['Flag']}</td><td id='midpadding'></td></tr>"
        appeal_category_code += appeal_line + " \n"


    # status

    code = ""



    for status in row["all_status_objects"] or []:
        case_status = int(status["CaseStatus"]) if status["CaseStatus"] is not None else 0

        if case_status in case_status_mappings:
            template = template_for_status[case_status]
            template = template.replace("{{index}}",str(case_status))
            status_mapping = case_status_mappings[case_status]



            for placeholder,field_name in status_mapping.items():
                if field_name in date_fields:
                    raw_value = status[field_name] if field_name in status else None
                    value = format_date(raw_value)
                else:
                    value = status[field_name] if field_name in status else None
                template = template.replace(placeholder,str(value))
            code += template + "\n"
            
                
        else:
            logger.warn(f"Mapping not found for CaseStatus: {case_status}, CaseNo: {row['m7.CaseNo']}")
            continue

    html = html.replace("{{statusplaceholder}}",code)
     
    
    # Financial supporter

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
    details = row["financial_condition_details"] or []

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

    # Replace the placeholder in the HTML template with the generated code
    html = html.replace('{{financial_condition_code}}',financial_condition_code)
    # # is there a financial condition suporter
    # html = html.replace("{{sponsorName}}",str(sponsor_name))
    # add multiple lines of code for bf diary
    html = html.replace("{{bfdiaryPlaceholder}}",bf_diary_code)
    # add multiple lines of code for history
    html = html.replace("{{HistoryPlaceholder}}",history_code)
    # add multiple lines of code for linked details
    html = html.replace("{{LinkedFilesPlaceholder}}",linked_files_code)
    # add multiple lines of maintain cost awards
    html = html.replace("{{MaintainCostAward}}",main_cost_award_code)
    # add multiple line for appeal
    html = html.replace("{{AppealPlaceholder}}",appeal_category_code)
    for key, value in m1_replacement.items():
        html = html.replace(str(key), str(value))
    # displayHTML(html)





# COMMAND ----------



# # display(m1_m2)
# for row in m1_m2.collect():
#     case_number = row["CaseNo"]
    
#     # maintain cost award tab
#     main_cost_award_code = f"<tr><td id='midpadding'>{row['CaseNo']}</td><td id='midpadding'>{row['AppellantName']}</td><td id='midpadding'>{row['AppealStage']}</td><td id='midpadding'>{row['DateOfApplication']}</td><td id='midpadding'>{row['TypeOfCostAward']}</td><td id='midpadding'>{row['ApplyingParty']}</td><td id='midpadding'>{row['PayingParty']}</td><td id='midpadding'>{row['MindedToAward']}</td><td id='midpadding'>{row['ObjectionToMindedToAward']}</td><td id='midpadding'>{row['CostsAwardDecision']}</td><td id='midpadding'></td><td id='midpadding'>{row['CostsAmount']}</td></tr>"


#     m1_replacement = {
#         "{{ bailCaseNo }}":row["CaseNo"] ,
#         "{{ hoRef }}": row["HORef"] ,
#         "{{ lastName }}": row["AppellantName"],
#         "{{ firstName }}" : row["AppellantForenames"],
#         "{{ birthDate }}": format_date(row["AppellantBirthDate"]),
#         "{{ portRef }}": row["PortReference"],
#         "{{AppellantTitle}}": row["AppellantTitle"],
#         ## Main section
#         "{{BailType}}": row["BailType"],
#         "{{AppealCategoriesField}}": row["AppealCategories"],
#         "{{Nationality}}":row["Nationality"],
#         "{{TravelOrigin}}":row["CountryOfTravelOrigin"],
#         "{{Port}}":row["PortOfEntry"],
#         "{{DateOfReceipt}}":format_date(row["DateReceived"]),
#         "{{DedicatedHearingCentre}}":row["DedicatedHearingCentre"],
#         "{{DateNoticeServed}}":format_date(row["DateServed"]) ,
#         # "{{CurrentStatus}}":"", Comes from M7 table
#         "{{ConnectedFiles}}":"",
#         "{{DateOfIssue}}":format_date(row["DateOfIssue"]),
#         # "{{NextHearingDate}}":row["DateOfNextListedHearing"],
#         # "{{lastDocument}}": LastDocument Field is populated by the latest Comment from the History table where HistType = 16
#         "{{BFEntry}}":"",
#         "{{ProvisionalDestructionDate}}":format_date(row["ProvisionalDestructionDate"]),

#         # Parties Tab - Applicant Section
#         "{{Centre}}": row["DetentionCentre"],
#         "{{AddressLine1}}": row["DetentionCentreAddress1"],
#         "{{AddressLine2}}": row["DetentionCentreAddress2"],
#         "{{AddressLine3}}": row["DetentionCentreAddress3"],
#         "{{AddressLine4}}": row["DetentionCentreAddress4"],
#         "{{AddressLine5}}": row["DetentionCentreAddress5"],
#         "{{Postcode}}": row["DetentionCentrePostcode"],
#         "{{Country}}": row["CountryOfTravelOrigin"],
#         "{{phone}}": row["AppellantTelephone"],
#         "{{email}}": row["AppellantEmail"],
#         "{{PrisonRef}}": row["AppellantPrisonRef"],
        
        
#         # Respondent Section
#         "{{Detained}}":row["AppellantDetained"],
#         "{{RespondentName}}":row["MainRespondentName"],
#         "{{repName}}":row["CaseRepName"],
#         "{{InterpreterRequirementsLanguage}}" : row["InterpreterRequirementsLanguage"],
#         "{{HOInterpreter}}" : row["HOInterpreter"],
#         "{{CourtPreference}}" : row["CourtPreference"],
#         "{{language}}": row["Language"],
#         "{{required}}": 1 if row["InterpreterRequirementsLanguage"] is not None else 0,

#         # Misc Tab
#         "{{Notes}}" : row["AppealCaseNote"],

#         # Maintain cost awards Tab

#         # Representative Tab
#         "{{RepName}}":row["CaseRepName"],
#         "{{CaseRepAddress1}}": row["CaseRepAddress1"],
#         "{{CaseRepAddress2}}": row["CaseRepAddress2"],
#         "{{CaseRepAddress3}}": row["CaseRepAddress3"],
#         "{{CaseRepAddress4}}": row["CaseRepAddress4"],
#         "{{CaseRepAddress5}}": row["CaseRepAddress5"],
#         "{{CaseRepPostcode}}": row["CaseRepPostcode"],
#         "{{CaseRepTelephone}}": row["CaseRepPhone"],
#         "{{CaseRepFAX}}": row["CaseRepFax"],
#         "{{CaseRepEmail}}": row["CaseRepEmail"],
#         "{{RepDxNo1}}": row["RepDxNo1"],
#         "{{RepDxNo2}}": row["RepDxNo2"],
#         # "{{RepLAARefNo}}": row["CaseRepLSCCommission"],
#         "{{RepLAACommission}}":row["CaseRepLSCCommission"],
#         #File specific contact



#         # Respondent Tab
#         "{{RespondentName}}":row["CaseRespondent"],
#         "{{CaseRespondentAddress1}}": row["RespondentAddress1"],
#         "{{CaseRespondentAddress2}}": row["RespondentAddress2"],
#         "{{CaseRespondentAddress3}}": row["RespondentAddress3"],
#         "{{CaseRespondentAddress4}}": row["RespondentAddress4"],
#         "{{CaseRespondentAddress5}}": row["RespondentAddress5"],
#         "{{CaseRespondentPostcode}}": row["RespondentPostcode"],
#         "{{CaseRespondentTelephone}}": row["RespondentTelephone"],
#         "{{CaseRespondentFAX}}": row["RespondentFax"],
#         "{{CaseRespondentEmail}}": row["RespondentEmail"],
#         "{{CaseRespondentRef}}":row["CaseRespondentReference"],
#         "{{CaseRespondentContact}}":row["CaseRespondentContact"],



#         # Status Tab - Additional Language
#         "{{PrimaryLanguage}}":row["Language"],

#         # Parties Tab
#         # "{{Detained}}": row[""]
#         "{{Centre}}":row["DetentionCentre"],



#         # Financial Condition supporter
#         # which case surty do we use


#         # status - Hearing details tab
#         # need logic to filter which hearing details to use using latest date

        
#         } 
#     # BF diary 
#     m4_filtered = m4.filter(F.col("CaseNo") == case_number)
#     bf_diary_code = ""
#     for index,row in enumerate(m4_filtered.collect(),start=1):
#         bf_line = f"<tr><td id=\"midpadding\">{row['BFDate']}</td><td id=\"midpadding\">{row['BFTypeDescription']}</td><td id=\"midpadding\">{row['Entry']}</td><td id=\"midpadding\">{row['DateCompleted']}</td></tr>"
#         bf_diary_code += bf_line + "\n"
    
#     # History 
#     m5_filtered = m5.filter(F.col("CaseNo") == case_number)
    
#     # Last Document filters on Hist type 16
#     last_doc = m5_filtered.filter(F.col("HistType")==16).orderBy(desc("HistDate")).limit(1).select("HistoryComment").first()
#     if last_doc is None:
#         last_document = None
#     else:
#         last_document = last_doc["HistoryComment"]

#     # File Location Code filters on Hist Type 6
#     file_loc = m5_filtered.filter(F.col("HistType")==6).orderBy(desc("HistDate")).limit(1).select("HistoryComment").first()

#     if file_loc is None:
#         file_location = None
#     else:
#         file_location = file_loc["HistoryComment"]
    
#     history_code = ''
#     for index, row in enumerate(m5_filtered.collect(),start=1):
#         history_line = f"<tr><td id='midpadding'>{row['HistDate']}</td><td id='midpadding'>{row['HistType']}</td><td id='midpadding'>{row['UserFullname']}</td><td id='midpadding'>{row['HistoryComment']}</td></tr>"
#         history_code += history_line + "\n"

#     # # Linked Files
#     m6_filtered = m6.filter(F.col("CaseNo") == case_number)
#     linked_files_code = ''
#     for index, row in enumerate(m6_filtered.collect(),start=1):
#         linked_files_line = f"<tr><td id='midpadding'></td><td id='midpadding'>{row['CaseNo']}</td><td id='midpadding'></td><td id='midpadding'>{row['LinkDetailComment']}</td></tr>"
#         linked_files_code += linked_files_line + "\n"

#     # main typing - has no mapping

#     # Appeal Category
#     m8_filtered = m8.filter(F.col("CaseNo") == case_number)
#     appeal_category_code = ""
#     for index, row in enumerate(m8_filtered.collect(),start=1):
#         appeal_line = f"<tr><td id='midpadding'>{row['CategoryDescription']}</td><td id='midpadding'>{row['Flag']}</td><td id='midpadding'></td></tr>"
#         appeal_category_code += appeal_line + "\n"


#     # status
#     m7_filtered = m7.filter(F.col("CaseNo") == case_number)
#     max_status = m7_filtered.agg(F.max("StatusId").alias("MaxStatusId")).collect()[0][0]
#     max_case_status = m7_filtered.filter(F.col("StatusId") == max_status ).select(F.col("CaseStatusDescription")).collect()[0][0]


#     bail_entry = m7_filtered.collect()

    

#    # initialise html template
#     html = html_template
#     for entry in bail_entry:
#         # Get the case status
#         case_status = int(entry['CaseStatus'])

#         # Check if the status has a defined mapping
#         if case_status in case_status_mappings:
#             # Get the specific mappings for the case status
#             status_mappings = case_status_mappings[case_status]

#             # Replace placeholders in the current mapping
#             for place_holder, field in status_mappings.items():
#                 if field in date_fields:
#                     value = format_date(entry[field]) if field in entry else ""
#                 else:
#                     value = str(entry[field]) if field in entry else ""
#                 html = html.replace(place_holder, value)
     

#     # Replace all other placeholders with empty strings
#     all_placeholders = [key for mappings in case_status_mappings.values() for key in mappings]
#     for place_holder in all_placeholders:
#         if place_holder not in status_mappings:
#             html = html.replace(place_holder, "")

#     # Hearing Details tab   
#     m3_filtered = m3.filter(F.col("CaseNo") == case_number)
    
#     m3_grouped_cols = [
#     "CaseNo", 
#     "StatusId", 
#     "Outcome", 
#     "CaseListTimeEstimate", 
#     "CaseListStartTime", 
#     "HearingTypeDesc", 
#     "ListName", 
#     "HearingDate", 
#     "ListStartTime", 
#     "ListTypeDesc", 
#     "ListType", 
#     "CourtName", 
#     "HearingCentreDesc"
# ]

#     final_m3 = m3.groupBy(m3_grouped_cols).agg(
#     F.concat_ws(" ",
#     first(when(col("Chairman") == True, col("AdjudicatorTitle"))),
#      first(when(col("Chairman") == True, col("AdjudicatorForenames"))),
#      first(when(col("Chairman") == True, col("AdjudicatorSurname")))).alias("JudgeFT"),
    
#     F.concat_ws(" ",
#      first(when(col("Chairman") == False, col("AdjudicatorTitle"))),
#      first(when(col("Chairman") == False, col("AdjudicatorForenames"))),
#      first(when(col("Chairman") == False, col("AdjudicatorSurname")))).alias("CourtClerkUsher"),
     
     
#      )
    
#     hearing_code = ""

#     # Iterate over the filtered DataFrame for each case_number
#     for index, hearing_row in enumerate(final_m3.collect(), start=1):
#         current_code = hearing_status_code
#         current_code = current_code.replace("{{Index}}", str(index))
        
#         for key, col_name in historydetail_replacements.items():
#             value = hearing_row[col_name]  # maybe hearing_row.get(col_name, None)
#             current_code = current_code.replace(key, str(value) if value is not None else "")

#         hearing_code += current_code + "\n"

#     # Replace placeholder in the HTML with the generated hearing_code
#     html = html.replace('{{HearingdetailsPlaceholder}}', hearing_code)
    
#     case_surety_replacement = {
#     "{{SponsorName}}":"CaseSuretyName",
#     "{{SponsorForename}}":"CaseSuretyForenames",
#     "{{SponsorTitle}}":"CaseSuretyTitle",
#     "{{SponsorAddress1}}":"CaseSuretyAddress1",
#     "{{SponsorAddress2}}":"CaseSuretyAddress2",
#     "{{SponsorAddress3}}":"CaseSuretyAddress3",
#     "{{SponsorAddress4}}":"CaseSuretyAddress4",
#     "{{SponsorAddress5}}":"CaseSuretyAddress5",
#     "{{SponsorPostcode}}":"CaseSuretyPostcode",
#     "{{SponsorPhone}}":"CaseSuretyTelephone",
#     "{{SponsorEmail}}":"CaseSuretyEmail",
#     "{{AmountOfFinancialCondition}}":"AmountOfFinancialCondition",
#     "{{SponsorSolicitor}}":"Solicitor",
#     "{{SponserDateLodged}}":"CaseSuretyDateLodged",
#     "{{SponsorLocation}}":"Location",
#     "{{AmountOfSecurity}}": "AmountOfTotalSecurity"
    

# }

#     financial_condition = case_surety.filter(F.col("CaseNo") == case_number)
#     sponsor_name = "Financial Condiiton Suportor details entered" if financial_condition.select("CaseSuretyName").count() >0 else None
#     financial_condition_code = ""

#     for index,row in enumerate(financial_condition.collect(),start=3):
#         current_code = template
#         current_code = current_code.replace("{{Index}}",str(index))
#         for key,col_name in case_surety_replacement.items():
#             value = row[col_name]
#             current_code = current_code.replace(key, str(value) if value is not None else "")
#         financial_condition_code += current_code + "\n"


#     html = html.replace('{{financial_condition_code}}',financial_condition_code)

#     # secondary language
#     secondary_language = m7_filtered.groupBy(F.col("CaseNo")).agg(F.first("LanguageDescription",True).alias("LanguageDescription")).first()["LanguageDescription"]

#     html = html.replace("{{SecondaryLanguage}}",str(secondary_language) if secondary_language is not None else "")


# #     # adjournment hearing tab
# #     adj_statusId = m7_filtered(F.col("CaseNo") == case_number).agg(F.first(F.col("AdjournmentParentStatusId"), True).alias("AdjournmentParentStatusId")).first()["AdjournmentParentStatusId"]

# #     adjourment_detail = m7_filtered(F.col("StatusId")==value_row)

# #     for inner_adj_row in adjourment_detail.collect():
# #         adjourment_detail_mapping = {
# #             "{{adjDateOfApplication}}":format_date(inner_adj_row["DateReceived"]),
# #             "{{adjDateOfHearing}}":format_date(inner_adj_row["MiscDate1"]),
# #             "{{adjPartyMakingApp}}":inner_adj_row["StatusParty"],
# #             "{{adjDirections}}":inner_adj_row["StatusNotes1"],
# #             "{{adjDateOfDecision}}":format_date(inner_adj_row["DecisionDate"]),
# #             "{{adjOutcome}}":inner_adj_row["OutcomeDescription"],
# #             "{{adjdatePartiesNotified}}":format_date(inner_adj_row["DecisionSentToHODate"])
# # }
# #         for key, value in adjourment_detail_mapping.items():
# #             html = html.replace(key, str(value))
    

#     # is there a financial condition suporter
#     html = html.replace("{{sponsorName}}",str(sponsor_name))
#     # add file locaiton
#     html = html.replace("{{FileLocation}}",str(file_location))
#     # add last document
#     html = html.replace("{{LastDocument}}",str(last_document))
#     # add latest case status
#     html = html.replace("{{CurrentStatus}}",max_case_status)
#     # add multiple lines of code for bf diary
#     html = html.replace("{{bfdiaryPlaceholder}}",bf_diary_code)
#     # add multiple lines of code for history
#     html = html.replace("{{HistoryPlaceholder}}",history_code)
#     # add multiple lines of code for linked details
#     html = html.replace("{{LinkedFilesPlaceholder}}",linked_files_code)
#     # add multiple lines of maintain cost awards
#     html = html.replace("{{MaintainCostAward}}",main_cost_award_code)
#     # add multiple line for appeal
#     html = html.replace("{{AppealPlaceholder}}",appeal_category_code)
#     for key, value in m1_replacement.items():
#         html = html.replace(str(key), str(value))
#     # displayHTML(html)





# COMMAND ----------


