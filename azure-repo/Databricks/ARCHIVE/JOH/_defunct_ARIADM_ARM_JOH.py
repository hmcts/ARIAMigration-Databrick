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

import dlt
import json
from pyspark.sql.functions import when, col,coalesce, current_timestamp, lit, date_format
from pyspark.sql.functions import *
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("pipelines.tableManagedByMultiplePipelinesCheck.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Variables

# COMMAND ----------

# MAGIC %md
# MAGIC Please note that running the DLT pipeline with the parameter `read_hive = true` will ensure the creation of the corresponding Hive tables. However, during this stage, none of the gold outputs (HTML, JSON, and A360) are processed. To generate the gold outputs, a secondary run with `read_hive = true` is required.

# COMMAND ----------

read_hive = False

# Setting variables for use in subsequent cells
raw_mnt = "/mnt/ingest00rawsboxraw/ARIADM/ARM/JOH"
landing_mnt = "/mnt/ingest00landingsboxlanding"
bronze_mnt = "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/JOH"
silver_mnt = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/JOH"
gold_mnt = "/mnt/ingest00curatedsboxgold/ARIADM/ARM/JOH"
gold_outputs = "ARIADM/ARM/JOH"
hive_schema = "ariadm_arm_joh"

# COMMAND ----------

# dbutils.widgets.text("initial_Load", "True")
# dbutils.widgets.text("raw_mnt", "/mnt/ingest00rawsboxraw/ARIADM/ARM/JOH")
# dbutils.widgets.text("landing_mnt", "/mnt/ingest00landingsboxlanding")
# dbutils.widgets.text("bronze_mnt", "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/JOH")
# dbutils.widgets.text("silver_mnt", "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/JOH")
# dbutils.widgets.text("gold_mnt", "/mnt/ingest00curatedsboxgold/ARIADM/ARM/JOH")
# dbutils.widgets.text("gold_outputs", "ARIADM/ARM/JOH")  # Path for gold output files in gold container
# dbutils.widgets.text("hive_schema", "ariadm_arm_joh")  # SchemaName

# initial_Load = dbutils.widgets.get("initial_Load") 
# raw_mnt = dbutils.widgets.get("raw_mnt")
# landing_mnt = dbutils.widgets.get("landing_mnt")
# bronze_mnt = dbutils.widgets.get("bronze_mnt")
# silver_mnt = dbutils.widgets.get("silver_mnt")
# gold_mnt = dbutils.widgets.get("gold_mnt")
# gold_outputs = dbutils.widgets.get("gold_outputs")
# hive_schema = dbutils.widgets.get("hive_schema")


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
    name="raw_adjudicatorrole",
    comment="Delta Live Table ARIA AdjudicatorRole.",
    path=f"{raw_mnt}/Raw_AdjudicatorRole"
)
def Raw_AdjudicatorRole():
    return read_latest_parquet("AdjudicatorRole", "tv_AdjudicatorRole", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_adjudicator",
    comment="Delta Live Table ARIA Adjudicator.",
    path=f"{raw_mnt}/Raw_Adjudicator"
)
def Raw_Adjudicator():
    return read_latest_parquet("Adjudicator", "tv_Adjudicator", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_employmentterm",
    comment="Delta Live Table ARIA EmploymentTerm.",
    path=f"{raw_mnt}/Raw_EmploymentTerm"
)
def Raw_EmploymentTerm():
     return read_latest_parquet("ARIAEmploymentTerm", "tv_EmploymentTerm", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_donotusereason",
    comment="Delta Live Table ARIA DoNotUseReason.",
    path=f"{raw_mnt}/Raw_DoNotUseReason"
)
def Raw_DoNotUseReason():
    return read_latest_parquet("DoNotUseReason", "tv_DoNotUseReason", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_johistory",
    comment="Delta Live Table ARIA JoHistory.",
    path=f"{raw_mnt}/Raw_JoHistory"
)
def Raw_JoHistory():
    return read_latest_parquet("JoHistory", "tv_JoHistory", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_othercentre",
    comment="Delta Live Table ARIA OtherCentre.",
    path=f"{raw_mnt}/Raw_OtherCentre"
)
def Raw_OtherCentre():
    return read_latest_parquet("OtherCentre", "tv_OtherCentre", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_hearingcentre",
    comment="Delta Live Table ARIA HearingCentre.",
    path=f"{raw_mnt}/Raw_HearingCentre"
)
def Raw_HearingCentre():
    return read_latest_parquet("ARIAHearingCentre", "tv_HearingCentre", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_users",
    comment="Delta Live Table ARIA Users.",
    path=f"{raw_mnt}/Raw_Users"
)
def Raw_Users():
    return read_latest_parquet("Users", "tv_Users", "ARIA_ARM_JOH_ARA")


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Bronze DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_adjudicator_et_hc_dnur

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC SELECT adj.AdjudicatorId, 
# MAGIC   adj.Surname, 
# MAGIC        adj.Forenames, 
# MAGIC        adj.Title, 
# MAGIC        adj.DateOfBirth, 
# MAGIC        adj.CorrespondenceAddress, 
# MAGIC        adj.ContactTelephone, 
# MAGIC        adj.ContactDetails, 
# MAGIC        adj.AvailableAtShortNotice, 
# MAGIC        hc.Description as DesignatedCentre, 
# MAGIC        et.Description as EmploymentTerms, 
# MAGIC        adj.FullTime, 
# MAGIC        adj.IdentityNumber, 
# MAGIC        adj.DateOfRetirement, 
# MAGIC        adj.ContractEndDate, 
# MAGIC        adj.ContractRenewalDate, 
# MAGIC        dnur.DoNotUse, 
# MAGIC        dnur.Description as DoNotUseReason, 
# MAGIC        adj.JudicialStatus, 
# MAGIC        adj.Address1, 
# MAGIC        adj.Address2, 
# MAGIC        adj.Address3, 
# MAGIC        adj.Address4, 
# MAGIC        adj.Address5, 
# MAGIC        adj.Postcode, 
# MAGIC        adj.Telephone, 
# MAGIC        adj.Mobile, 
# MAGIC        adj.Email, 
# MAGIC        adj.BusinessAddress1, 
# MAGIC        adj.BusinessAddress2, 
# MAGIC        adj.BusinessAddress3, 
# MAGIC        adj.BusinessAddress4, 
# MAGIC        adj.BusinessAddress5, 
# MAGIC        adj.BusinessPostcode, 
# MAGIC        adj.BusinessTelephone, 
# MAGIC        adj.BusinessFax, 
# MAGIC        adj.BusinessEmail, 
# MAGIC        adj.JudicialInstructions, 
# MAGIC        adj.JudicialInstructionsDate, 
# MAGIC        adj.Notes 
# MAGIC FROM [ARIAREPORTS].[dbo].[Adjudicator] adj 
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingCentre] hc 
# MAGIC         ON adj.CentreId = hc.CentreId 
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[EmploymentTerm] et 
# MAGIC         ON adj.EmploymentTerms = et.EmploymentTermId 
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[DoNotUseReason] dnur 
# MAGIC         ON adj.DoNotUseReason = dnur.DoNotUseReasonId 
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col

@dlt.table(
    name="bronze_adjudicator_et_hc_dnur",
    comment="Delta Live Table combining Adjudicator data with Hearing Centre, Employment Terms, and Do Not Use Reason.",
    path=f"{bronze_mnt}/bronze_adjudicator_et_hc_dnur"
)
def bronze_adjudicator_et_hc_dnur():
    return (
        dlt.read("raw_adjudicator").alias("adj")
            .join(
                dlt.read("raw_hearingcentre").alias("hc"),
                col("adj.CentreId") == col("hc.CentreId"),
                "left_outer",
            )
            .join(
                dlt.read("raw_employmentterm").alias("et"),
                col("adj.EmploymentTerms") == col("et.EmploymentTermId"),
                "left_outer",
            )
            .join(
                dlt.read("raw_donotusereason").alias("dnur"),
                col("adj.DoNotUseReason") == col("dnur.DoNotUseReasonId"),
                "left_outer",
            )
        .select(
            col("adj.AdjudicatorId"),
            col("adj.Surname"),
            col("adj.Forenames"),
            col("adj.Title"),
            col("adj.DateOfBirth"),
            col("adj.CorrespondenceAddress"),
            col("adj.ContactTelephone"),
            col("adj.ContactDetails"),
            col("adj.AvailableAtShortNotice"),
            col("hc.Description").alias("DesignatedCentre"),
            col("et.Description").alias("EmploymentTerm"),
            col("adj.FullTime"),
            col("adj.IdentityNumber"),
            col("adj.DateOfRetirement"),
            col("adj.ContractEndDate"),
            col("adj.ContractRenewalDate"),
            col("dnur.DoNotUse"),
            col("dnur.Description").alias("DoNotUseReason"),
            col("adj.JudicialStatus"),
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

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Transformation  bronze_johistory_users 

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC SELECT jh.AdjudicatorId, 
# MAGIC        jh.HistDate, 
# MAGIC        jh.HistType, 
# MAGIC        u.FullName as UserName, 
# MAGIC        jh.Comment 
# MAGIC FROM [ARIAREPORTS].[dbo].[JoHistory] jh 
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Users] u 
# MAGIC         ON jh.UserId = u.UserId 
# MAGIC ```

# COMMAND ----------

@dlt.table(
    name="bronze_johistory_users",
    comment="Delta Live Table combining JoHistory data with Users information.",
    path=f"{bronze_mnt}/bronze_johistory_users" 
)
def bronze_johistory_users():
    return (
        dlt.read("raw_johistory").alias("joh")
            .join(dlt.read("raw_users").alias("u"), col("joh.UserId") == col("u.UserId"), "left_outer")
            .select(
                col("joh.AdjudicatorId"),
                col("joh.HistDate"),
                col("joh.HistType"),
                col("u.FullName").alias("UserName"),
                col("joh.Comment"),
                col("joh.AdtclmnFirstCreatedDatetime"),
                col("joh.AdtclmnModifiedDatetime"),
                col("joh.SourceFileName"),
                col("joh.InsertedByProcessName")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_othercentre_hearingcentre 

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC SELECT oc.AdjudicatorId, 
# MAGIC        hc.Description As OtherCentres 
# MAGIC FROM [ARIAREPORTS].[dbo].[OtherCentre] oc 
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingCentre] hc 
# MAGIC         ON oc.CentreId = hc.CentreId 
# MAGIC ```

# COMMAND ----------

# DLT Table 1: bronze_othercentre_hearingcentre
@dlt.table(
    name="bronze_othercentre_hearingcentre",
    comment="Delta Live Table combining OtherCentre data with HearingCentre information.",
    path=f"{bronze_mnt}/bronze_othercentre_hearingcentre" 
)
def bronze_othercentre_hearingcentre():
    return (
        dlt.read("raw_othercentre").alias("oc")
        .join(
            dlt.read("raw_hearingcentre").alias("hc"),
            col("hc.CentreId") == col("oc.CentreId"),
            "left_outer",
        )
        .select(
            col("oc.AdjudicatorId"),
            col("hc.Description").alias("OtherCentres"),
            col("oc.AdtclmnFirstCreatedDatetime"),
            col("oc.AdtclmnModifiedDatetime"),
            col("oc.SourceFileName"),
            col("oc.InsertedByProcessName")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_adjudicatorrole

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC SELECT AdjudicatorId, 
# MAGIC        Role, 
# MAGIC        DateOfAppointment, 
# MAGIC        EndDateOfAppointment 
# MAGIC FROM [ARIAREPORTS].[dbo].[AdjudicatorRole] 
# MAGIC ```

# COMMAND ----------


# DLT Table 2: bronze_adjudicator_role
@dlt.table(
    name="bronze_adjudicator_role",
    comment="Delta Live Table for Adjudicator Role data.",
    path=f"{bronze_mnt}/bronze_adjudicator_role" 
)
def bronze_adjudicator_role():
    return  (
        dlt.read("raw_adjudicatorrole").alias("adjr")
        .select(
            col("adjr.AdjudicatorId"),
            col("adjr.Role"),
            col("adjr.DateOfAppointment"),
            col("adjr.EndDateOfAppointment"),
            col("adjr.AdtclmnFirstCreatedDatetime"),
            col("adjr.AdtclmnModifiedDatetime"),
            col("adjr.SourceFileName"),
            col("adjr.InsertedByProcessName")
        )
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
    return (
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
    return (
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
    return (
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
    return (dlt.read("bronze_othercentre_hearingcentre").alias("hc").join(dlt.read("stg_joh_filtered").alias('flt'), col("hc.AdjudicatorId") == col("flt.AdjudicatorId"), "inner").select("hc.*"))

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
    return (
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
    return (
        dlt.read("silver_adjudicator_detail").alias("adj").join(dlt.read("stg_joh_filtered").alias('flt'), col("adj.AdjudicatorId") == col("flt.AdjudicatorId"), "inner").select(
            col('adj.AdjudicatorId').alias('client_identifier'),
            date_format(coalesce(col('adj.DateOfRetirement'), col('adj.ContractEndDate'), col('adj.AdtclmnFirstCreatedDatetime')), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_date"),
            date_format(col('adj.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("recordDate"),
            lit("GBR").alias("region"),
            lit("ARIA").alias("publisher"),
            lit("ARIA Judicial Records").alias("record_class"),
            lit('IA_Judicial_Office').alias("entitlement_tag"),
            col('adj.Title').alias('bf_001'),
            col('adj.Forenames').alias('bf_002'),
            col('adj.Surname').alias('bf_003'),
            col('adj.DateOfBirth').alias('bf_004'),
            col('adj.DesignatedCentre').alias('bf_005')
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Outputs and Tracking DLT Table Creation

# COMMAND ----------

secret = dbutils.secrets.get("ingest00-keyvault-sbox", "ingest00-adls-ingest00curatedsbox-connection-string-sbox")

# COMMAND ----------

# DBTITLE 1,Azure Blob Storage Connection Setup in Python
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

# Set up the BlobServiceClient with your connection string
connection_string = f"BlobEndpoint=https://ingest00curatedsbox.blob.core.windows.net/;QueueEndpoint=https://ingest00curatedsbox.queue.core.windows.net/;FileEndpoint=https://ingest00curatedsbox.file.core.windows.net/;TableEndpoint=https://ingest00curatedsbox.table.core.windows.net/;SharedAccessSignature={secret}"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Specify the container name
container_name = "gold"
container_client = blob_service_client.get_container_client(container_name)


# COMMAND ----------

# DBTITLE 1,Spark SQL Shuffle Partitions
spark.conf.set("spark.sql.shuffle.partitions", 32)  # Set this to 32 for your 8-worker cluster


# COMMAND ----------

# Modify the UDF to accept a row object
def generate_html(row):
    try:
        # Load template
        html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
        with open(html_template_path, "r") as f:
            html_template = f.read()

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
        return f"Error generating HTML for AdjudicatorId {row.AdjudicatorId}: {e}"

# Register UDF
generate_html_udf = udf(generate_html, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC #### In Dev
# MAGIC
# MAGIC <!-- This section is currently in development and may not be ready for production use. -->

# COMMAND ----------

# display(spark.read.table(f"hive_metastore.ariadm_arm_joh.stg_judicial_officer_unified"))

# COMMAND ----------

@dlt.table(
    name="stg_judicial_officer_unified",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{gold_mnt}/stg_judicial_officer_unified"
)
def stg_judicial_officer_unified():

    df_judicial_officer_details = dlt.read("silver_adjudicator_detail")
    df_other_centres = dlt.read("silver_othercentre_detail")
    df_roles = dlt.read("silver_appointment_detail")
    df_history = dlt.read("silver_history_detail")

    # Aggregate Other Centres
    grouped_centres = df_other_centres.groupBy("AdjudicatorId").agg(
        collect_list("OtherCentres").alias("OtherCentres")
    )

    # Aggregate Roles
    grouped_roles = df_roles.groupBy("AdjudicatorId").agg(
        collect_list(
            struct("Role", "DateOfAppointment", "EndDateOfAppointment")
        ).alias("Roles")
    )

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

    df_unified = df_combined.withColumn("HTMLContent", generate_html_udf(struct(*df_combined.columns))) \
                            .withColumn("JSONcollection", to_json(struct(*df_combined.columns))) \
                            .withColumn("HTMLFileName", concat(lit("judicial_officer_"), col("AdjudicatorId"), lit(".html"))) \
                            .withColumn("JSONFileName", concat(lit("judicial_officer_"), col("AdjudicatorId"), lit(".json")))

    return df_unified

# COMMAND ----------

 if read_hive:
     print("Loading data from Hive")

# COMMAND ----------

# DBTITLE 1,Generate HTML in Parallel
# Helper to format dates in ISO format (YYYY-MM-DD)
def format_date_iso(date_value):
    try:
        if isinstance(date_value, str):
            date_value = datetime.strptime(date_value, "%Y-%m-%d")
        return date_value.strftime("%Y-%m-%d")
    except Exception as e:
        return ""

# Helper to format dates in dd/MM/YYYY format
def format_date(date_value):
    try:
        if isinstance(date_value, str):
            date_value = datetime.strptime(date_value, "%Y-%m-%d")
        return date_value.strftime("%d/%m/%Y")
    except Exception as e:
        return ""
    


# Upload HTML to Blob Storage
def upload_html_partition(partition):
    for row in partition:
        try:
            blob_client = container_client.get_blob_client(
                f"{gold_outputs}/HTML/{row['HTMLFileName']}"
            )
            blob_client.upload_blob(row["HTMLContent"], overwrite=True)
        except Exception as e:
            print(f"Error uploading HTML for Adjudicator ID {row['AdjudicatorId']}: {str(e)}")

@dlt.table(
    name="gold_judicial_officer_with_html",
    comment="Delta Live Gold Table for with HTML content.",
    path=f"{gold_mnt}/gold_judicial_officer_with_html"
)
def gold_judicial_officer_with_html():
    # Load source data
    df_combined = dlt.read("stg_judicial_officer_unified")

    # Optional: Load from Hive if not an initial load
    if read_hive:
        df_combined = spark.read.table(f"hive_metastore.{hive_schema}.stg_judicial_officer_unified")

    # Generate HTML content
    # df_with_html = df_combined.withColumn(
    #     "HTMLContent", generate_html_udf(struct(*df_combined.columns))
    # )

    # Upload HTML files to Azure Blob Storage
    df_combined.select("AdjudicatorId", "HTMLContent","HTMLFileName").foreachPartition(upload_html_partition)

    # Return the DataFrame for DLT table creation
    return df_combined.select("AdjudicatorId", "HTMLContent","HTMLFileName")

# COMMAND ----------

# DBTITLE 1,temp
# # import dlt
# # from pyspark.sql.functions import col, collect_list, struct, lit, udf
# # from pyspark.sql.types import StringType
# # from datetime import datetime

#  # Helper to format dates
# def format_date(date_value, output_format="%Y-%m-%d"):
#     try:
#         if isinstance(date_value, str):
#             date_value = datetime.strptime(date_value, "%Y-%m-%d")
#         return date_value.strftime(output_format)
#     except Exception:
#         return ""
    

# # Modify the UDF to accept a row object
# def generate_html(row):
#     try:
#         # Load template
#         html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
#         with open(html_template_path, "r") as f:
#             html_template = f.read()

#         # Replace placeholders in the template with row data
#         replacements = {
#             "{{AdjudicatorId}}": str(row.AdjudicatorId),
#             "{{Surname}}": row.Surname or "",
#             "{{Forenames}}": row.Forenames or "",
#             "{{Title}}": row.Title or "",
#             "{{DateOfBirth}}": format_date_iso(row.DateOfBirth),
#             "{{CorrespondenceAddress}}": row.CorrespondenceAddress or "",
#             "{{Telephone}}": row.ContactTelephone or "",
#             "{{ContactDetails}}": row.ContactDetails or "",
#             "{{DesignatedCentre}}": row.DesignatedCentre or "",
#             "{{EmploymentTerm}}": row.EmploymentTerm or "",
#             "{{FullTime}}": row.FullTime or "",
#             "{{IdentityNumber}}": row.IdentityNumber or "",
#             "{{DateOfRetirement}}": format_date_iso(row.DateOfRetirement),
#             "{{ContractEndDate}}": format_date_iso(row.ContractEndDate),
#             "{{ContractRenewalDate}}": format_date_iso(row.ContractRenewalDate),
#             "{{DoNotUseReason}}": row.DoNotUseReason or "",
#             "{{JudicialStatus}}": row.JudicialStatus or "",
#             "{{Address1}}": row.Address1 or "",
#             "{{Address2}}": row.Address2 or "",
#             "{{Address3}}": row.Address3 or "",
#             "{{Address4}}": row.Address4 or "",
#             "{{Address5}}": row.Address5 or "",
#             "{{Postcode}}": row.Postcode or "",
#             "{{Mobile}}": row.Mobile or "",
#             "{{Email}}": row.Email or "",
#             "{{BusinessAddress1}}": row.BusinessAddress1 or "",
#             "{{BusinessAddress2}}": row.BusinessAddress2 or "",
#             "{{BusinessAddress3}}": row.BusinessAddress3 or "",
#             "{{BusinessAddress4}}": row.BusinessAddress4 or "",
#             "{{BusinessAddress5}}": row.BusinessAddress5 or "",
#             "{{BusinessPostcode}}": row.BusinessPostcode or "",
#             "{{BusinessTelephone}}": row.BusinessTelephone or "",
#             "{{BusinessFax}}": row.BusinessFax or "",
#             "{{BusinessEmail}}": row.BusinessEmail or "",
#             "{{JudicialInstructions}}": row.JudicialInstructions or "",
#             "{{JudicialInstructionsDate}}": format_date_iso(row.JudicialInstructionsDate),
#             "{{Notes}}": row.Notes or "",
#             "{{OtherCentre}}": "\n".join(
#                 f"<tr><td id=\"midpadding\">{i+1}</td><td id=\"midpadding\">{centre}</td></tr>"
#                 for i, centre in enumerate(row.OtherCentres or [])
#             ),
#             "{{AppointmentPlaceHolder}}": "\n".join(
#                 f"<tr><td id=\"midpadding\">{i+1}</td><td id=\"midpadding\">{role.Role}</td><td id=\"midpadding\">{format_date(role.DateOfAppointment)}</td><td id=\"midpadding\">{format_date(role.EndDateOfAppointment)}</td></tr>"
#                 for i, role in enumerate(row.Roles or [])
#             ),
#             "{{HistoryPlaceHolder}}": "\n".join(
#                 f"<tr><td id=\"midpadding\">{format_date(hist.HistDate)}</td><td id=\"midpadding\">{hist.HistType}</td><td id=\"midpadding\">{hist.UserName}</td><td id=\"midpadding\">{hist.Comment}</td></tr>"
#                 for hist in (row.History or [])
#             ),
#         }

    
        
#         for key, value in replacements.items():
#             html_template = html_template.replace(key, value)
        
#         return html_template
#     except Exception as e:
#         return f"Error generating HTML for AdjudicatorId {row.AdjudicatorId}: {e}"

# # Register UDF
# generate_html_udf = udf(generate_html, StringType())


# def upload_html(row):
#     try:
#         blob_client = container_client.get_blob_client(f"{gold_outputs}/HTML/judicial_officer_{row['AdjudicatorId']}.html")
#         blob_client.upload_blob(row['HTMLContent'], overwrite=True)
#     except Exception as e:
#         print(f"Error uploading HTML for Adjudicator ID {row['AdjudicatorId']}: {str(e)}")


# @dlt.table(
#     name="gold_judicial_officer_with_html",
#     comment="Delta Live Gold Table for with HTML content.",
#     path=f"{gold_mnt}/gold_judicial_officer_with_html"
# )
# def gold_judicial_officer_with_html():

#     df_combined = dlt.read("stg_judicial_officer_unified")

#     # Load the necessary dataframes from Hive metastore if not initial load
#     if read_hive == True:
#         df_combined = spark.read.table(f"hive_metastore.{hive_schema}.stg_judicial_officer_unified")


#     # Apply the UDF to the combined DataFrame
#     # Add HTMLContent column
#     df_with_html = df_combined.withColumn("HTMLContent", generate_html_udf(struct(*df_combined.columns)))
#     df_with_html.select("AdjudicatorId", "HTMLContent").foreach(upload_html)

#     return df_with_html

# COMMAND ----------

# DBTITLE 1,Generate JSON in Parallel
# from pyspark.sql.functions import to_json, struct

def upload_json_partition(partition):
    """
    Upload JSON files for each row in a partition to Azure Blob Storage.
    """
    for row in partition:
        try:
            blob_client = container_client.get_blob_client(
                f"{gold_outputs}/JSON/{row['JSONFileName']}"
            )
            blob_client.upload_blob(row['JSONcollection'], overwrite=True)
        except Exception as e:
            print(f"Error uploading JSON for Adjudicator ID {row['AdjudicatorId']}: {str(e)}")

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

    # Optionally load data from Hive if needed
    if read_hive:
        df_combined = spark.read.table(f"hive_metastore.{hive_schema}.stg_judicial_officer_unified")

    # Generate JSON content from all columns in the DataFrame
    # df_with_json = df_combined.withColumn(
    #     "JSONcollection", to_json(struct(*df_combined.columns))
    # )

    # Upload JSON files to Azure Blob Storage
    df_combined.select("AdjudicatorId", "JSONcollection","JSONFileName").foreachPartition(upload_json_partition)

    # Return the DataFrame for DLT table creation
    return df_combined.select("AdjudicatorId", "JSONcollection","JSONFileName")


# COMMAND ----------

# DBTITLE 1,temp
# def upload_json(row):
#     try:
#         blob_client = container_client.get_blob_client(f"{gold_outputs}/JSON/judicial_officer_{row['AdjudicatorId']}.json")
#         blob_client.upload_blob(row['JSONcollection'], overwrite=True)
#     except Exception as e:
#         print(f"Error uploading JSON for Adjudicator ID {row['AdjudicatorId']}: {str(e)}")


# @dlt.table(
#     name="gold_judicial_officer_with_json",
#     comment="Delta Live Gold Table for with HTML content.",
#     path=f"{gold_mnt}/gold_judicial_officer_with_json"
# )
# def gold_judicial_officer_with_json():

#     df_combined = dlt.read("stg_judicial_officer_unified")

#     # Load the necessary dataframes from Hive metastore if not initial load
#     if read_hive == True:
#         df_combined = spark.read.table(f"hive_metastore.{hive_schema}.stg_judicial_officer_unified")

#     # # Aggregate Other Centres
#     # grouped_centres = df_other_centres.groupBy("AdjudicatorId").agg(
#     #     collect_list("OtherCentres").alias("OtherCentres")
#     # )

#     # # Aggregate Roles
#     # grouped_roles = df_roles.groupBy("AdjudicatorId").agg(
#     #     collect_list(
#     #         struct("Role", "DateOfAppointment", "EndDateOfAppointment")
#     #     ).alias("Roles")
#     # )

#     # # Aggregate History
#     # grouped_history = df_history.groupBy("AdjudicatorId").agg(
#     #     collect_list(
#     #         struct("HistDate", "HistType", "UserName", "Comment")
#     #     ).alias("History")
#     # )

#     # # Join all aggregated data with JudicialOfficerDetails
#     # df_combined = (
#     #     df_judicial_officer_details
#     #     .join(grouped_centres, "AdjudicatorId", "left")
#     #     .join(grouped_roles, "AdjudicatorId", "left")
#     #     .join(grouped_history, "AdjudicatorId", "left")
#     # )


#     df_with_json = df_combined.withColumn("JSONcollection", to_json(struct(*df_combined.columns)))
#     df_with_json.select("AdjudicatorId", "JSONcollection").foreach(upload_json)

#     return df_with_json

# COMMAND ----------

# DBTITLE 1,Generate A360 in Parallel
def upload_a360_partition(partition):
    for row in partition:
        try:
            blob_client = container_client.get_blob_client(
                f"{gold_outputs}/A360/{row['A360FileName']}"
            )
            blob_client.upload_blob(row['A360Content'], overwrite=True)
        except Exception as e:
            print(f"Error uploading A360 for client_identifier {row['client_identifier']}: {str(e)}")

def generate_a360(row):
    try:
        metadata_data = {
            "operation": "create_record",
            "relation_id": row.client_identifier,
            "record_metadata": {
                "publisher": "ARIA",
                "record_class": "ARIA Judicial Records",
                "region": "GBR",
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
                "publisher": "ARIA",
                "dz_file_name": f"judicial_officer_{row.client_identifier}.html",
                "file_tag": "html"
            }
        }

        json_data = {
            "operation": "upload_new_file",
            "relation_id": row.client_identifier,
            "file_metadata": {
                "publisher": "ARIA",
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
        return f"Error generating A360 for client_identifier {row.client_identifier}: {e}"

# Register UDF
generate_a360_udf = udf(generate_a360, StringType())

@dlt.table(
    name="gold_judicial_officer_with_a360",
    comment="Delta Live Gold Table with A360 content.",
    path=f"{gold_mnt}/gold_judicial_officer_with_a360"
)
def gold_judicial_officer_with_a360():
    df_joh_metadata = dlt.read("silver_archive_metadata")
    # df_joh_metadata = spark.read.table(f"hive_metastore.{hive_schema}.silver_archive_metadata")

    # Optionally load data from Hive
    if read_hive:
        df_joh_metadata = spark.read.table(f"hive_metastore.{hive_schema}.silver_archive_metadata")

    # Generate A360 content
    df_with_a360 = df_joh_metadata.withColumn(
        "A360Content", generate_a360_udf(struct(*df_joh_metadata.columns))
    ).withColumn("A360FileName", concat(lit("judicial_officer_"), col("client_identifier"), lit(".a360")))

    # Upload A360 files
    df_with_a360.select("client_identifier", "A360Content","A360FileName").foreachPartition(upload_a360_partition)

    return df_with_a360.select("client_identifier", "A360Content","A360FileName")


# COMMAND ----------

# DBTITLE 1,temp
# def upload_a360(row):
#     try:
#         blob_client = container_client.get_blob_client(f"{gold_outputs}/A360/judicial_officer_{row['client_identifier']}.a360")
#         blob_client.upload_blob(row['A360Content'], overwrite=True)
#     except Exception as e:
#         print(f"Error uploading A360 for Adjudicator ID {row['client_identifier']}: {str(e)}")

# # Function to format dates
# def format_date_zulu(date_value):
#     if isinstance(date_value, str):  # If the date is already a string, return as is
#         return date_value
#     elif isinstance(date_value, (datetime,)):
#         return datetime.strftime(date_value, "%Y-%m-%dT%H:%M:%SZ")
#     return None  # Return None if the date is invalid or not provided

# def generate_a360(row):
#     try:
#         metadata_data = {
#             "operation": "create_record",
#             "relation_id": row.client_identifier,
#             "record_metadata": {
#                 "publisher": "ARIA",
#                 "record_class": "ARIA Judicial Records",
#                 "region": "GBR",
#                 "recordDate": str(row.recordDate),
#                 "event_date": str(row.event_date),
#                 "client_identifier": row.client_identifier,
#                 "bf_001": row.bf_001 or "",
#                 "bf_002": row.bf_002 or "",
#                 "bf_003": row.bf_003 or "",
#                 "bf_004": str(row.bf_004) or "",
#                 "bf_005": row.bf_005 or ""
#             }
#         }

#         html_data = {
#             "operation": "upload_new_file",
#             "relation_id": row.client_identifier,
#             "file_metadata": {
#                 "publisher": "ARIA",
#                 "dz_file_name": f"judicial_officer_{row.client_identifier}.html",
#                 "file_tag": "html"
#             }
#         }

#         json_data = {
#             "operation": "upload_new_file",
#             "relation_id": row.client_identifier,
#             "file_metadata": {
#                 "publisher": "ARIA",
#                 "dz_file_name": f"judicial_officer_{row.client_identifier}.json",
#                 "file_tag": "json"
#             }
#         }

#         # Convert dictionaries to JSON strings
#         metadata_data_str = json.dumps(metadata_data, separators=(',', ':'))
#         html_data_str = json.dumps(html_data, separators=(',', ':'))
#         json_data_str = json.dumps(json_data, separators=(',', ':'))

#         # Combine the data
#         all_data_str = f"{metadata_data_str}\n{html_data_str}\n{json_data_str}"

#         return all_data_str
#     except Exception as e:
#         return f"Error generating A360 for client_identifier {row.client_identifier}: {e}"

# # Register UDF
# generate_a360_udf = udf(generate_a360, StringType())


# @dlt.table(
#     name="gold_judicial_officer_with_a360",
#     comment="Delta Live Gold Table for with HTML content.",
#     path=f"{gold_mnt}/gold_judicial_officer_with_a360"
# )
# def gold_judicial_officer_with_a360():

#     df_joh_metadata = dlt.read("silver_archive_metadata")

#     # Load the necessary dataframes from Hive metastore if not initial load
#     if read_hive == True:
#        df_joh_metadata = spark.read.table(f"hive_metastore.{hive_schema}.silver_archive_metadata")

#     # Apply the UDF to the combined DataFrame
#     df_with_a360 = df_joh_metadata.withColumn("A360Content", generate_a360_udf(struct(*df_joh_metadata.columns)))

#     df_with_a360.select("client_identifier", "A360Content").foreach(upload_a360)

#     return df_with_a360

# COMMAND ----------

# df_joh_metadata = spark.read.table(f"hive_metastore.{hive_schema}.silver_archive_metadata")

# # Function to format dates
# def format_date_zulu(date_value):
#     if isinstance(date_value, str):  # If the date is already a string, return as is
#         return date_value
#     elif isinstance(date_value, (datetime,)):
#         return datetime.strftime(date_value, "%Y-%m-%dT%H:%M:%SZ")
#     return None  # Return None if the date is invalid or not provided

# def generate_a360(row):
#     try:
#         metadata_data = {
#             "operation": "create_record",
#             "relation_id": row.client_identifier,
#             "record_metadata": {
#                 "publisher": "ARIA",
#                 "record_class": "ARIA Judicial Records",
#                 "region": "GBR",
#                 "recordDate": str(row.recordDate),
#                 "event_date": str(row.event_date),
#                 "client_identifier": row.client_identifier,
#                 "bf_001": row.bf_001 or "",
#                 "bf_002": row.bf_002 or "",
#                 "bf_003": row.bf_003 or "",
#                 "bf_004": str(row.bf_004) or "",
#                 "bf_005": row.bf_005 or ""
#             }
#         }

#         html_data = {
#             "operation": "upload_new_file",
#             "relation_id": row.client_identifier,
#             "file_metadata": {
#                 "publisher": "ARIA",
#                 "dz_file_name": f"judicial_officer_{row.client_identifier}.html",
#                 "file_tag": "html"
#             }
#         }

#         json_data = {
#             "operation": "upload_new_file",
#             "relation_id": row.client_identifier,
#             "file_metadata": {
#                 "publisher": "ARIA",
#                 "dz_file_name": f"judicial_officer_{row.client_identifier}.json",
#                 "file_tag": "json"
#             }
#         }

#         # Convert dictionaries to JSON strings
#         metadata_data_str = json.dumps(metadata_data, separators=(',', ':'))
#         html_data_str = json.dumps(html_data, separators=(',', ':'))
#         json_data_str = json.dumps(json_data, separators=(',', ':'))

#         # Combine the data
#         all_data_str = f"{metadata_data_str}\n{html_data_str}\n{json_data_str}"

#         return all_data_str
#     except Exception as e:
#         return f"Error generating A360 for client_identifier {row.client_identifier}: {e}"

# # Register UDF
# generate_a360_udf = udf(generate_a360, StringType())

# # Apply the UDF to the combined DataFrame
# df_with_a360 = df_joh_metadata.withColumn("A360Content", generate_a360_udf(struct(*df_joh_metadata.columns)))

# display(df_with_a360)

# COMMAND ----------

# def upload_json(row):
#     try:
#         blob_client = container_client.get_blob_client(f"{gold_outputs}/HTML/judicial_officer_{row['AdjudicatorId']}.json")
#         blob_client.upload_blob(row['HTMLContent'], overwrite=True)
#     except Exception as e:
#         print(f"Error uploading HTML for Adjudicator ID {row['AdjudicatorId']}: {str(e)}")

# @dlt.table(
#     name="judicial_officer_with_jsom",
#     comment="Delta Live Gold Table for with HTML content.",
#     path=f"{gold_mnt}/judicial_officer_with_jsom"
# )
# def judicial_officer_with_jsom():

#     df_judicial_officer_details = spark.read.table(f"hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
#     df_other_centres = spark.read.table(f"hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
#     df_roles = spark.read.table(f"hive_metastore.ariadm_arm_joh.silver_appointment_detail")
#     df_history = spark.read.table(f"hive_metastore.ariadm_arm_joh.silver_history_detail")

#     # Aggregate Other Centres
#     grouped_centres = df_other_centres.groupBy("AdjudicatorId").agg(
#         collect_list("OtherCentres").alias("OtherCentres")
#     )

#     # Aggregate Roles
#     grouped_roles = df_roles.groupBy("AdjudicatorId").agg(
#         collect_list(
#             struct("Role", "DateOfAppointment", "EndDateOfAppointment")
#         ).alias("Roles")
#     )

#     # Aggregate History
#     grouped_history = df_history.groupBy("AdjudicatorId").agg(
#         collect_list(
#             struct("HistDate", "HistType", "UserName", "Comment")
#         ).alias("History")
#     )

#     # Join all aggregated data with JudicialOfficerDetails
#     df_combined = (
#         df_judicial_officer_details
#         .join(grouped_centres, "AdjudicatorId", "left")
#         .join(grouped_roles, "AdjudicatorId", "left")
#         .join(grouped_history, "AdjudicatorId", "left")
#     )

#     # output_path = "{gold_outputs}/JSON/"  
#     output_path = f"{gold_mnt}/JSON/" 

#     # Repartition to optimize for JSON generation
#     df_combined = df_combined.repartition("AdjudicatorId")

#     # Write JSON files partitioned by AdjudicatorId
#     df_combined.write.partitionBy("AdjudicatorId").json(output_path + f"judicial_officer_{AdjudicatorId}.json", mode="overwrite")

#     df_with_json = df_combined.withColumn("jsoncollection", to_json(struct([df_combined[x] for x in df_combined.columns])))
   
#     return  df_with_json

# COMMAND ----------

# DBTITLE 1,Aggregating DataFrames
# from pyspark.sql.functions import collect_list, struct, lit, col

# df_stg_joh_filtered = spark.read.table(f"hive_metastore.ariadm_arm_joh.stg_joh_filtered")
# df_judicial_officer_details = spark.read.table(f"hive_metastore.ariadm_arm_joh.silver_adjudicator_details")
# df_other_centres = spark.read.table(f"hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
# df_roles = spark.read.table(f"hive_metastore.ariadm_arm_joh.silver_appointment_detail")
# df_history = spark.read.table(f"hive_metastore.ariadm_arm_joh.silver_history_detail")

# # Aggregate Other Centres
# grouped_centres = df_other_centres.groupBy("AdjudicatorId").agg(
#     collect_list("OtherCentres").alias("OtherCentres")
# )

# # Aggregate Roles
# grouped_roles = df_roles.groupBy("AdjudicatorId").agg(
#     collect_list(
#         struct("Role", "DateOfAppointment", "EndDateOfAppointment")
#     ).alias("Roles")
# )

# # Aggregate History
# grouped_history = df_history.groupBy("AdjudicatorId").agg(
#     collect_list(
#         struct("HistDate", "HistType", "UserName", "Comment")
#     ).alias("History")
# )

# # Join all aggregated data with JudicialOfficerDetails
# df_combined = (
#     df_judicial_officer_details
#     .join(grouped_centres, "AdjudicatorId", "left")
#     .join(grouped_roles, "AdjudicatorId", "left")
#     .join(grouped_history, "AdjudicatorId", "left")
# )



# COMMAND ----------

# DBTITLE 1,Generate HTML in Parallel
# from pyspark.sql import functions
# from pyspark.sql.types import StringType
# from datetime import datetime

# # Helper to format dates in ISO format (YYYY-MM-DD)
# def format_date_iso(date_value):
#     try:
#         if isinstance(date_value, str):
#             date_value = datetime.strptime(date_value, "%Y-%m-%d")
#         return date_value.strftime("%Y-%m-%d")
#     except Exception as e:
#         return ""

# # Helper to format dates in dd/MM/YYYY format
# def format_date(date_value):
#     try:
#         if isinstance(date_value, str):
#             date_value = datetime.strptime(date_value, "%Y-%m-%d")
#         return date_value.strftime("%d/%m/%Y")
#     except Exception as e:
#         return ""

# # Modify the UDF to accept a row object
# def generate_html(row):
#     try:
#         # Load template
#         html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
#         with open(html_template_path, "r") as f:
#             html_template = f.read()

#         # Replace placeholders in the template with row data
#         replacements = {
#             "{{AdjudicatorId}}": str(row.AdjudicatorId),
#             "{{Surname}}": row.Surname or "",
#             "{{Forenames}}": row.Forenames or "",
#             "{{Title}}": row.Title or "",
#             "{{DateOfBirth}}": format_date_iso(row.DateOfBirth),
#             "{{CorrespondenceAddress}}": row.CorrespondenceAddress or "",
#             "{{Telephone}}": row.ContactTelephone or "",
#             "{{ContactDetails}}": row.ContactDetails or "",
#             "{{DesignatedCentre}}": row.DesignatedCentre or "",
#             "{{EmploymentTerm}}": row.EmploymentTerm or "",
#             "{{FullTime}}": row.FullTime or "",
#             "{{IdentityNumber}}": row.IdentityNumber or "",
#             "{{DateOfRetirement}}": format_date_iso(row.DateOfRetirement),
#             "{{ContractEndDate}}": format_date_iso(row.ContractEndDate),
#             "{{ContractRenewalDate}}": format_date_iso(row.ContractRenewalDate),
#             "{{DoNotUseReason}}": row.DoNotUseReason or "",
#             "{{JudicialStatus}}": row.JudicialStatus or "",
#             "{{Address1}}": row.Address1 or "",
#             "{{Address2}}": row.Address2 or "",
#             "{{Address3}}": row.Address3 or "",
#             "{{Address4}}": row.Address4 or "",
#             "{{Address5}}": row.Address5 or "",
#             "{{Postcode}}": row.Postcode or "",
#             "{{Mobile}}": row.Mobile or "",
#             "{{Email}}": row.Email or "",
#             "{{BusinessAddress1}}": row.BusinessAddress1 or "",
#             "{{BusinessAddress2}}": row.BusinessAddress2 or "",
#             "{{BusinessAddress3}}": row.BusinessAddress3 or "",
#             "{{BusinessAddress4}}": row.BusinessAddress4 or "",
#             "{{BusinessAddress5}}": row.BusinessAddress5 or "",
#             "{{BusinessPostcode}}": row.BusinessPostcode or "",
#             "{{BusinessTelephone}}": row.BusinessTelephone or "",
#             "{{BusinessFax}}": row.BusinessFax or "",
#             "{{BusinessEmail}}": row.BusinessEmail or "",
#             "{{JudicialInstructions}}": row.JudicialInstructions or "",
#             "{{JudicialInstructionsDate}}": format_date_iso(row.JudicialInstructionsDate),
#             "{{Notes}}": row.Notes or "",
#             "{{OtherCentre}}": "\n".join(
#                 f"<tr><td id=\"midpadding\">{i+1}</td><td id=\"midpadding\">{centre}</td></tr>"
#                 for i, centre in enumerate(row.OtherCentres or [])
#             ),
#             "{{AppointmentPlaceHolder}}": "\n".join(
#                 f"<tr><td id=\"midpadding\">{i+1}</td><td id=\"midpadding\">{role.Role}</td><td id=\"midpadding\">{format_date(role.DateOfAppointment)}</td><td id=\"midpadding\">{format_date(role.EndDateOfAppointment)}</td></tr>"
#                 for i, role in enumerate(row.Roles or [])
#             ),
#             "{{HistoryPlaceHolder}}": "\n".join(
#                 f"<tr><td id=\"midpadding\">{format_date(hist.HistDate)}</td><td id=\"midpadding\">{hist.HistType}</td><td id=\"midpadding\">{hist.UserName}</td><td id=\"midpadding\">{hist.Comment}</td></tr>"
#                 for hist in (row.History or [])
#             ),
#         }

    
        
#         for key, value in replacements.items():
#             html_template = html_template.replace(key, value)
        
#         return html_template
#     except Exception as e:
#         return f"Error generating HTML for AdjudicatorId {row.AdjudicatorId}: {e}"

# # Register UDF
# generate_html_udf = udf(generate_html, StringType())

# # Apply the UDF to the combined DataFrame
# df_with_html = df_combined.withColumn("HTMLContent", generate_html_udf(struct(*df_combined.columns)))

# display(df_with_html)

# COMMAND ----------

# DBTITLE 1,display HTML Content
# html_content = df_with_html.select("HTMLContent").first()["HTMLContent"]
# displayHTML(html_content)

# COMMAND ----------

# DBTITLE 1,Write HTML
# def upload_html(row):
#     try:
#         blob_client = container_client.get_blob_client(f"{gold_outputs}/HTML/judicial_officer_{row['AdjudicatorId']}.html")
#         blob_client.upload_blob(row['HTMLContent'], overwrite=True)
#     except Exception as e:
#         print(f"Error uploading HTML for Adjudicator ID {row['AdjudicatorId']}: {str(e)}")

# df_with_html.select("AdjudicatorId", "HTMLContent").foreach(upload_html)

# COMMAND ----------

# DBTITLE 1,Write Json
# output_path = "{gold_outputs}/JSON/"  # Replace with your desired path

# # Repartition to optimize for JSON generation
# df_combined = df_combined.repartition("AdjudicatorId")

# # Write JSON files partitioned by AdjudicatorId
# df_combined.write.partitionBy("AdjudicatorId").json(output_path + "judicial_officer_{AdjudicatorId}.json", mode="overwrite")

# COMMAND ----------

# type(json_object)

# COMMAND ----------

# import json
# from pyspark.sql.functions import to_json

# df_with_json = df_combined.withColumn("jsoncollection", to_json(struct(*df_combined.columns)))
# json_content = df_with_json.select("jsoncollection").first()["jsoncollection"]
# json_object = json.loads(json_content)
# display(json_object)

# COMMAND ----------

# DBTITLE 1,Generate A360 in Parallel
# df_joh_metadata = spark.read.table(f"hive_metastore.{hive_schema}.silver_archive_metadata")

# # Function to format dates
# def format_date_zulu(date_value):
#     if isinstance(date_value, str):  # If the date is already a string, return as is
#         return date_value
#     elif isinstance(date_value, (datetime,)):
#         return datetime.strftime(date_value, "%Y-%m-%dT%H:%M:%SZ")
#     return None  # Return None if the date is invalid or not provided

# def generate_a360(row):
#     try:
#         metadata_data = {
#             "operation": "create_record",
#             "relation_id": row.client_identifier,
#             "record_metadata": {
#                 "publisher": "ARIA",
#                 "record_class": "ARIA Judicial Records",
#                 "region": "GBR",
#                 "recordDate": str(row.recordDate),
#                 "event_date": str(row.event_date),
#                 "client_identifier": row.client_identifier,
#                 "bf_001": row.bf_001 or "",
#                 "bf_002": row.bf_002 or "",
#                 "bf_003": row.bf_003 or "",
#                 "bf_004": str(row.bf_004) or "",
#                 "bf_005": row.bf_005 or ""
#             }
#         }

#         html_data = {
#             "operation": "upload_new_file",
#             "relation_id": row.client_identifier,
#             "file_metadata": {
#                 "publisher": "ARIA",
#                 "dz_file_name": f"judicial_officer_{row.client_identifier}.html",
#                 "file_tag": "html"
#             }
#         }

#         json_data = {
#             "operation": "upload_new_file",
#             "relation_id": row.client_identifier,
#             "file_metadata": {
#                 "publisher": "ARIA",
#                 "dz_file_name": f"judicial_officer_{row.client_identifier}.json",
#                 "file_tag": "json"
#             }
#         }

#         # Convert dictionaries to JSON strings
#         metadata_data_str = json.dumps(metadata_data, separators=(',', ':'))
#         html_data_str = json.dumps(html_data, separators=(',', ':'))
#         json_data_str = json.dumps(json_data, separators=(',', ':'))

#         # Combine the data
#         all_data_str = f"{metadata_data_str}\n{html_data_str}\n{json_data_str}"

#         return all_data_str
#     except Exception as e:
#         return f"Error generating A360 for client_identifier {row.client_identifier}: {e}"

# # Register UDF
# generate_a360_udf = udf(generate_a360, StringType())

# # Apply the UDF to the combined DataFrame
# df_with_a360 = df_joh_metadata.withColumn("A360Content", generate_a360_udf(struct(*df_joh_metadata.columns)))

# display(df_with_a360)

# COMMAND ----------

# DBTITLE 1,display A360 Content
# A360_content = df_with_a360.select("A360Content").first()["A360Content"]
# print(A360_content)

# COMMAND ----------

# DBTITLE 1,Write A360
# def upload_A360(row):
#     try:
#         blob_client = container_client.get_blob_client(f"{gold_outputs}/A360/judicial_officer_{row['client_identifier']}.A360")
#         blob_client.upload_blob(row['A360Content'], overwrite=True)
#     except Exception as e:
#         print(f"Error uploading HTML for Adjudicator ID {row['client_identifier']}: {str(e)}")

# df_with_a360.select("client_identifier", "A360Content").foreach(upload_A360)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate HTML

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create gold_adjudicator_html_generation_status & Processing Adjudicator HTML's
# MAGIC This section is to prallel process the HTML and create atracker table with log information.

# COMMAND ----------

# DBTITLE 1,Adhoc debug code- Generating HTML Content with Date Formatting

# # from datetime import datetime

# # # Helper to format dates in ISO format (YYYY-MM-DD)
# def format_date_iso(date_value):
#     try:
#         # Attempt to parse as datetime if it's a string
#         if isinstance(date_value, str):
#             date_value = datetime.strptime(date_value, "%Y-%m-%d")
#         return date_value.strftime("%Y-%m-%d")
    
#     except (ValueError, TypeError):  # If parsing fails, attempt with other formats
#         try:
#             # Try parsing the string as dd/MM/YYYY format
#             if isinstance(date_value, str):
#                 date_value = datetime.strptime(date_value, "%d/%m/%Y")
#             return date_value.strftime("%Y-%m-%d")
#         except (ValueError, TypeError):  # If both parsing attempts fail
#             return ""  # Return empty string if parsing fails

# # Helper to format dates in dd/MM/YYYY format
# def format_date(date_value):
#     try:
#         # Attempt to parse as datetime if it's a string
#         if isinstance(date_value, str):
#             date_value = datetime.strptime(date_value, "%Y-%m-%d")
#         return date_value.strftime("%d/%m/%Y")
    
#     except (ValueError, TypeError):  # If parsing fails, attempt with other formats
#         try:
#             # Try parsing the string as dd/MM/YYYY format
#             if isinstance(date_value, str):
#                 date_value = datetime.strptime(date_value, "%d/%m/%Y")
#             return date_value.strftime("%d/%m/%Y")
#         except (ValueError, TypeError):  # If both parsing attempts fail
#             return ""  # Return empty string if parsing fails




# # Helper function to find data from a list by AdjudicatorId
# def find_data_in_list(data_list, adjudicator_id):
#     for row in data_list:
#         if row['AdjudicatorId'] == adjudicator_id:
#             return row
#     return None

# # Define the function to generate HTML content for a given adjudicator
# def generate_html_content(adjudicator_id, judicial_officer_details_list, other_centres_list, roles_list, history_list):
#     try:
#         # Step 1: Find judicial officer details
#         judicial_officer_details = find_data_in_list(judicial_officer_details_list, adjudicator_id)
#         if not judicial_officer_details:
#             print(f"No details found for Adjudicator ID: {adjudicator_id}")
#             return None, "No details found"

#         # Step 2: Find other centres
#         other_centres = [centre for centre in other_centres_list if centre['AdjudicatorId'] == adjudicator_id]

#         # Step 3: Find roles
#         roles = [role for role in roles_list if role['AdjudicatorId'] == adjudicator_id]

#         # Step 4: Find history
#         history = [hist for hist in history_list if hist['AdjudicatorId'] == adjudicator_id]

#         # Step 4: Read the HTML template
#         html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
#         with open(html_template_path, "r") as f:
#             html_template = "".join([l for l in f])

#         # Step 6: Convert the Spark Row object to a dictionary
#         row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

#         # print(type(row_dict.get('DateOfBirth'))) # <class 'datetime.datetime'>

#         # Create a dictionary with the replacements, ensuring all values are strings
#         replacements = {
#             "{{Surname}}": str(row_dict.get('Surname', '') or ''),
#             "{{Title}}": str(row_dict.get('Title', '') or ''),
#             "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
#             "{{DateOfBirth}}": format_date_iso(row_dict.get('DateOfBirth')),
#             "{{CorrespondenceAddress}}": str(row_dict.get('CorrespondenceAddress', '') or ''),
#             "{{Telephone}}": str(row_dict.get('ContactTelephone', '') or ''),
#             "{{ContactDetails}}": str(row_dict.get('ContactDetails', '') or ''),
#             "{{DesignatedCentre}}": str(row_dict.get('DesignatedCentre', '') or ''),
#             "{{EmploymentTerm}}": str(row_dict.get('EmploymentTerm', '') or ''),
#             "{{FullTime}}": str(row_dict.get('FullTime', '') or ''),
#             "{{IdentityNumber}}": str(row_dict.get('IdentityNumber', '') or ''),
#             "{{DateOfRetirement}}": format_date_iso(row_dict.get('DateOfRetirement')),
#             "{{ContractEndDate}}": format_date_iso(row_dict.get('ContractEndDate')),
#             "{{ContractRenewalDate}}": format_date_iso(row_dict.get('ContractRenewalDate')),
#             "{{DoNotUseReason}}": str(row_dict.get('DoNotUseReason', '') or ''),
#             "{{JudicialStatus}}": str(row_dict.get('JudicialStatus', '') or ''),
#             "{{Address1}}": str(row_dict.get('Address1', '') or ''),
#             "{{Address2}}": str(row_dict.get('Address2', '') or ''),
#             "{{Address3}}": str(row_dict.get('Address3', '') or ''),
#             "{{Address4}}": str(row_dict.get('Address4', '') or ''),
#             "{{Address5}}": str(row_dict.get('Address5', '') or ''),
#             "{{Postcode}}": str(row_dict.get('Postcode', '') or ''),
#             "{{Mobile}}": str(row_dict.get('Mobile', '') or ''),
#             "{{Email}}": str(row_dict.get('Email', '') or ''),
#             "{{BusinessAddress1}}": str(row_dict.get('BusinessAddress1', '') or ''),
#             "{{BusinessAddress2}}": str(row_dict.get('BusinessAddress2', '') or ''),
#             "{{BusinessAddress3}}": str(row_dict.get('BusinessAddress3', '') or ''),
#             "{{BusinessAddress4}}": str(row_dict.get('BusinessAddress4', '') or ''),
#             "{{BusinessAddress5}}": str(row_dict.get('BusinessAddress5', '') or ''),
#             "{{BusinessPostcode}}": str(row_dict.get('BusinessPostcode', '') or ''),
#             "{{BusinessTelephone}}": str(row_dict.get('BusinessTelephone', '') or ''),
#             "{{BusinessFax}}": str(row_dict.get('BusinessFax', '') or ''),
#             "{{BusinessEmail}}": str(row_dict.get('BusinessEmail', '') or ''),
#             "{{JudicialInstructions}}": str(row_dict.get('JudicialInstructions', '') or ''),
#             "{{JudicialInstructionsDate}}": format_date_iso(row_dict.get('JudicialInstructionsDate')),
#             "{{Notes}}": str(row_dict.get('Notes', '') or ''),
#         }

#         # Step 7: Replace placeholders using the replacements dictionary
#         for key, value in replacements.items():
#             html_template = html_template.replace(key, value)

#         # Step 8: Handle multiple rows for Other Centres
#         other_centres_code = ""
#         for i, centre in enumerate(other_centres, start=1):
#             line = f"<tr><td id=\"midpadding\">{i}</td><td id=\"midpadding\">{centre['OtherCentres']}</td></tr>"
#             other_centres_code += line + '\n'
#         html_template = html_template.replace("{{OtherCentre}}", other_centres_code)

#         # Handle roles
#         roles_code = ""
#         for i, role in enumerate(roles, start=1):
#             line = f"<tr><td id=\"midpadding\">{i}</td><td id=\"midpadding\">{role['Role']}</td><td id=\"midpadding\">{format_date(role['DateOfAppointment'])}</td><td id=\"midpadding\">{format_date(role['EndDateOfAppointment'])}</td></tr>"
#             roles_code += line + '\n'
            
#         html_template = html_template.replace("{{AppointmentPlaceHolder}}", roles_code)

#         # History Details
#         History_Code = ''
#         for index, row in enumerate(history, start=1):
#             # print(type(row['HistDate']))
#             line = f"<tr><td id=\"midpadding\">{format_date(row['HistDate'])}</td><td id=\"midpadding\">{row['HistType']}</td><td id=\"midpadding\">{row['UserName']}</td><td id=\"midpadding\">{row['Comment']}</td></tr>"
#             History_Code += line + '\n'
#         html_template = html_template.replace(f"{{{{HistoryPlaceHolder}}}}", History_Code)

#         # Step 9: Return the transformed HTML content
#         return html_template, "Success"

#     except Exception as e:
#         print(f"Error writing file for Adjudicator ID: {adjudicator_id}: {str(e)}")
#         return None, f"Error writing file: {str(e)}"

# # adjudicator_id = 5210
# # df_stg_joh_filtered = spark.read.table(f"hive_metastore.ariadm_arm_joh_test.stg_joh_filtered")
# # df_judicial_officer_details = spark.read.table(f"hive_metastore.ariadm_arm_joh_test.silver_adjudicator_detail")
# # df_other_centres = spark.read.table(f"hive_metastore.ariadm_arm_joh_test.silver_othercentre_detail")
# # df_roles = spark.read.table(f"hive_metastore.ariadm_arm_joh_test.silver_appointment_detail")
# # df_history = spark.read.table(f"hive_metastore.ariadm_arm_joh_test.silver_history_detail")

# adjudicator_id = 1660
# df_stg_joh_filtered = spark.read.table(f"hive_metastore.ariadm_arm_joh.stg_joh_filtered")
# df_judicial_officer_details = spark.read.table(f"hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
# df_other_centres = spark.read.table(f"hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
# df_roles = spark.read.table(f"hive_metastore.ariadm_arm_joh.silver_appointment_detail")
# df_history = spark.read.table(f"hive_metastore.ariadm_arm_joh.silver_history_detail")

# # Broadcast the dataframes to all workers
# judicial_officer_details_bc = spark.sparkContext.broadcast(df_judicial_officer_details.collect())
# other_centres_bc = spark.sparkContext.broadcast(df_other_centres.collect())
# roles_bc = spark.sparkContext.broadcast(df_roles.collect())
# history_bc = spark.sparkContext.broadcast(df_history.collect())

# html_content, status = generate_html_content(
#             adjudicator_id,
#             judicial_officer_details_bc.value,
#             other_centres_bc.value,
#             roles_bc.value,
#             history_bc.value
#         )

# displayHTML(html_content)

# COMMAND ----------

# DBTITLE 1,Generating Judicial Officer Profiles in HMTL Outputs

# # Date formatting helper
# # def format_date_iso(date_value):
# #     if date_value:
# #         return datetime.strftime(date_value, "%Y-%m-%d")
# #     return ""

# # def format_date(date_value):
# #     if date_value:
# #         return datetime.strftime(date_value, "%d/%m/%Y")
# #     return ""

# # Helper to format dates in ISO format (YYYY-MM-DD)
# def format_date_iso(date_value):
#     try:
#         if isinstance(date_value, str):
#             date_value = datetime.strptime(date_value, "%Y-%m-%d")
#         return date_value.strftime("%Y-%m-%d")
#     except Exception as e:
#         return ""

# # Helper function to find data from a list by AdjudicatorId
# def find_data_in_list(data_list, adjudicator_id):
#     for row in data_list:
#         if row['AdjudicatorId'] == adjudicator_id:
#             return row
#     return None

# # Define the function to generate HTML content for a given adjudicator
# def generate_html_content(adjudicator_id, judicial_officer_details_list, other_centres_list, roles_list, history_list):
#     try:
#         # Step 1: Find judicial officer details
#         judicial_officer_details = find_data_in_list(judicial_officer_details_list, adjudicator_id)
#         if not judicial_officer_details:
#             print(f"No details found for Adjudicator ID: {adjudicator_id}")
#             return None, "No details found"

#         # Step 2: Find other centres
#         other_centres = [centre for centre in other_centres_list if centre['AdjudicatorId'] == adjudicator_id]

#         # Step 3: Find roles
#         roles = [role for role in roles_list if role['AdjudicatorId'] == adjudicator_id]

#         # Step 4: Find history
#         history = [hist for hist in history_list if hist['AdjudicatorId'] == adjudicator_id]

#         # Step 4: Read the HTML template
#         html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
#         with open(html_template_path, "r") as f:
#             html_template = "".join([l for l in f])

#         # Step 6: Convert the Spark Row object to a dictionary
#         row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

#         # Create a dictionary with the replacements, ensuring all values are strings
#         replacements = {
#             "{{Surname}}": str(row_dict.get('Surname', '') or ''),
#             "{{Title}}": str(row_dict.get('Title', '') or ''),
#             "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
#             "{{DateOfBirth}}": format_date_iso(row_dict.get('DateOfBirth')),
#             "{{CorrespondenceAddress}}": str(row_dict.get('CorrespondenceAddress', '') or ''),
#             "{{Telephone}}": str(row_dict.get('ContactTelephone', '') or ''),
#             "{{ContactDetails}}": str(row_dict.get('ContactDetails', '') or ''),
#             "{{DesignatedCentre}}": str(row_dict.get('DesignatedCentre', '') or ''),
#             "{{EmploymentTerm}}": str(row_dict.get('EmploymentTerm', '') or ''),
#             "{{FullTime}}": str(row_dict.get('FullTime', '') or ''),
#             "{{IdentityNumber}}": str(row_dict.get('IdentityNumber', '') or ''),
#             "{{DateOfRetirement}}": format_date_iso(row_dict.get('DateOfRetirement')),
#             "{{ContractEndDate}}": format_date_iso(row_dict.get('ContractEndDate')),
#             "{{ContractRenewalDate}}": format_date_iso(row_dict.get('ContractRenewalDate')),
#             "{{DoNotUseReason}}": str(row_dict.get('DoNotUseReason', '') or ''),
#             "{{JudicialStatus}}": str(row_dict.get('JudicialStatus', '') or ''),
#             "{{Address1}}": str(row_dict.get('Address1', '') or ''),
#             "{{Address2}}": str(row_dict.get('Address2', '') or ''),
#             "{{Address3}}": str(row_dict.get('Address3', '') or ''),
#             "{{Address4}}": str(row_dict.get('Address4', '') or ''),
#             "{{Address5}}": str(row_dict.get('Address5', '') or ''),
#             "{{Postcode}}": str(row_dict.get('Postcode', '') or ''),
#             "{{Mobile}}": str(row_dict.get('Mobile', '') or ''),
#             "{{Email}}": str(row_dict.get('Email', '') or ''),
#             "{{BusinessAddress1}}": str(row_dict.get('BusinessAddress1', '') or ''),
#             "{{BusinessAddress2}}": str(row_dict.get('BusinessAddress2', '') or ''),
#             "{{BusinessAddress3}}": str(row_dict.get('BusinessAddress3', '') or ''),
#             "{{BusinessAddress4}}": str(row_dict.get('BusinessAddress4', '') or ''),
#             "{{BusinessAddress5}}": str(row_dict.get('BusinessAddress5', '') or ''),
#             "{{BusinessPostcode}}": str(row_dict.get('BusinessPostcode', '') or ''),
#             "{{BusinessTelephone}}": str(row_dict.get('BusinessTelephone', '') or ''),
#             "{{BusinessFax}}": str(row_dict.get('BusinessFax', '') or ''),
#             "{{BusinessEmail}}": str(row_dict.get('BusinessEmail', '') or ''),
#             "{{JudicialInstructions}}": str(row_dict.get('JudicialInstructions', '') or ''),
#             "{{JudicialInstructionsDate}}": format_date_iso(row_dict.get('JudicialInstructionsDate')),
#             "{{Notes}}": str(row_dict.get('Notes', '') or ''),
#         }

#         # Step 7: Replace placeholders using the replacements dictionary
#         for key, value in replacements.items():
#             html_template = html_template.replace(key, value)

#         # Step 8: Handle multiple rows for Other Centres
#         other_centres_code = ""
#         for i, centre in enumerate(other_centres, start=1):
#             line = f"<tr><td id=\"midpadding\">{i}</td><td id=\"midpadding\">{centre['OtherCentres']}</td></tr>"
#             other_centres_code += line + '\n'
#         html_template = html_template.replace("{{OtherCentre}}", other_centres_code)

#         # Handle roles
#         roles_code = ""
#         for i, role in enumerate(roles, start=1):
#             line = f"<tr><td id=\"midpadding\">{i}</td><td id=\"midpadding\">{role['Role']}</td><td id=\"midpadding\">{format_date_iso(role['DateOfAppointment'])}</td><td id=\"midpadding\">{format_date_iso(role['EndDateOfAppointment'])}</td></tr>"
#             roles_code += line + '\n'
            
#         html_template = html_template.replace("{{AppointmentPlaceHolder}}", roles_code)

#         # History Details
#         History_Code = ''
#         for index, row in enumerate(history, start=1):
#             line = f"<tr><td id=\"midpadding\">{format_date_iso(row['HistDate'])}</td><td id=\"midpadding\">{row['HistType']}</td><td id=\"midpadding\">{row['UserName']}</td><td id=\"midpadding\">{row['Comment']}</td></tr>"
#             History_Code += line + '\n'
#         html_template = html_template.replace(f"{{{{HistoryPlaceHolder}}}}", History_Code)

#         # Step 9: Return the transformed HTML content
#         return html_template, "Success"

#     except Exception as e:
#         print(f"Error writing file for Adjudicator ID: {adjudicator_id}: {str(e)}")
#         return None, f"Error writing file: {str(e)}"

# # Define a function to run the HTML generation for each partition
# def process_html_partition(partition, judicial_officer_details_bc, other_centres_bc, roles_bc, history_bc):
#     results = []
#     for row in partition:
#         adjudicator_id = row['AdjudicatorId']
#         html_content, status = generate_html_content(
#             adjudicator_id,
#             judicial_officer_details_bc.value,
#             other_centres_bc.value,
#             roles_bc.value,
#             history_bc.value
#         )
        
#         # if html_content is None:
#         #     continue
        
#         # Define the target path for each Adjudicator's HTML file
#         target_path = f"{gold_outputs}/HTML/judicial_officer_{adjudicator_id}.html"
        
#         # Upload the HTML content to Azure Blob Storage
#         blob_client = container_client.get_blob_client(target_path)
#         # blob_client.upload_blob(html_content, overwrite=True)
#         if status == "Success":
#             try:
#                 blob_client.upload_blob(html_content, overwrite=True)
#             except Exception as e:
#                 print(f"Error uploading HTML for Adjudicator ID {adjudicator_id}: {str(e)}")
#                 continue  # Skip to the next Adjudicator

#         # Append the result
#         results.append((adjudicator_id, target_path, status))
#     return results



# @dlt.table(
#     name="gold_joh_html_generation_status",
#     comment="Delta Live Table for Adjudicator HTML Generation Status.",
#     path=f"{gold_mnt}/gold_joh_html_generation_status"
# )
# def gold_joh_html_generation_status():
#     df_stg_joh_filtered = dlt.read("stg_joh_filtered")
#     df_judicial_officer_details = dlt.read("silver_adjudicator_detail")
#     df_other_centres = dlt.read("silver_othercentre_detail")
#     df_roles = dlt.read("silver_appointment_detail")
#     df_history = dlt.read("silver_history_detail")

#     # Load the necessary dataframes from Hive metastore if not initial load
#     if read_hive == True:
#         df_stg_joh_filtered = spark.read.table(f"hive_metastore.{hive_schema}.stg_joh_filtered")
#         df_judicial_officer_details = spark.read.table(f"hive_metastore.{hive_schema}.silver_adjudicator_detail")
#         df_other_centres = spark.read.table(f"hive_metastore.{hive_schema}.silver_othercentre_detail")
#         df_roles = spark.read.table(f"hive_metastore.{hive_schema}.silver_appointment_detail")
#         df_history = spark.read.table(f"hive_metastore.{hive_schema}.silver_history_detail")

#     # Broadcast the dataframes to all workers
#     judicial_officer_details_bc = spark.sparkContext.broadcast(df_judicial_officer_details.collect())
#     other_centres_bc = spark.sparkContext.broadcast(df_other_centres.collect())
#     roles_bc = spark.sparkContext.broadcast(df_roles.collect())
#     history_bc = spark.sparkContext.broadcast(df_history.collect())

#     # Repartition the DataFrame by AdjudicatorId to optimize parallel processing
#     num_partitions = 32  # Setting this to optimize for your 8-worker cluster
#     repartitioned_df = df_stg_joh_filtered.repartition(num_partitions, "AdjudicatorId")

#     # Run the HTML generation on each partition
#     result_rdd = repartitioned_df.rdd.mapPartitions(lambda partition: process_html_partition(partition, judicial_officer_details_bc, other_centres_bc, roles_bc, history_bc))

#     # Collect results
#     results = result_rdd.collect()

#     # Create DataFrame for output
#     schema = StructType([
#         StructField("AdjudicatorId", LongType(), True),
#         StructField("HTML_Path", StringType(), True),
#         StructField("Status", StringType(), True)
#     ])
#     html_result_df = spark.createDataFrame(results, schema=schema)

#     # Log the HTML paths in the Delta Live Table
#     return html_result_df.select("AdjudicatorId", "HTML_Path", "Status")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate JSON

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create gold_adjudicator_Json_generation_status & Processing Adjudicator Json's
# MAGIC This section is to prallel process the json and create atracker table with log information.

# COMMAND ----------

# DBTITLE 1,Generating Judicial Officer Profiles in JSON Outputs
# # from pyspark.sql.functions import col
# # from pyspark.sql.types import LongType, StringType, StructType, StructField
# # from datetime import datetime
# # import json

# # # Step 5: Date formatting helper
# # def format_date_iso(date_value):
# #     if date_value:
# #         return datetime.strftime(date_value, "%Y-%m-%d")
# #     return ""

# # def format_date(date_value):
# #     if date_value:
# #         return datetime.strftime(date_value, "%d/%m/%Y")
# #     return ""

# # # Helper function to find data from a list by AdjudicatorId
# # def find_data_in_list(data_list, adjudicator_id):
# #     for row in data_list:
# #         if row['AdjudicatorId'] == adjudicator_id:
# #             return row
# #     return None

# # Date formatting helper
# def format_date_iso(date_value):
#     if date_value:
#         return datetime.strftime(date_value, "%Y-%m-%dT%H:%M:%S")
#     return ""

# # Define the function to generate JSON content for a given adjudicator
# def generate_json_content(adjudicator_id, judicial_officer_details_list, other_centres_list, roles_list, history_list):
#     try:
#         # Step 1: Find judicial officer details
#         judicial_officer_details = find_data_in_list(judicial_officer_details_list, adjudicator_id)
#         if not judicial_officer_details:
#             print(f"No details found for Adjudicator ID: {adjudicator_id}")
#             return None, "No details found"

#         # Step 2: Find other centres
#         other_centres = [centre for centre in other_centres_list if centre['AdjudicatorId'] == adjudicator_id]

#         # Step 3: Find roles
#         roles = [role for role in roles_list if role['AdjudicatorId'] == adjudicator_id]

#         # Step 4: Find history
#         history = [hist for hist in history_list if hist['AdjudicatorId'] == adjudicator_id]

#         # Step 6: Convert the Spark Row object to a dictionary
#         row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

#         # Create a dictionary with the data
#         json_data = {
#             "Surname": row_dict.get('Surname', ''),
#             "Title": row_dict.get('Title', ''),
#             "Forenames": row_dict.get('Forenames', ''),
#             "DateOfBirth": str(row_dict.get('DateOfBirth')),
#             "CorrespondenceAddress": row_dict.get('CorrespondenceAddress', ''),
#             "Telephone": row_dict.get('ContactTelephone', ''),
#             "ContactDetails": row_dict.get('ContactDetails', ''),
#             "DesignatedCentre": row_dict.get('DesignatedCentre', ''),
#             "EmploymentTerm": row_dict.get('EmploymentTerm', ''),
#             "FullTime": row_dict.get('FullTime', ''),
#             "IdentityNumber": row_dict.get('IdentityNumber', ''),
#             "DateOfRetirement": str(row_dict.get('DateOfRetirement')),
#             "ContractEndDate": str(row_dict.get('ContractEndDate')),
#             "ContractRenewalDate": str(row_dict.get('ContractRenewalDate')),
#             "DoNotUseReason": row_dict.get('DoNotUseReason', ''),
#             "JudicialStatus": row_dict.get('JudicialStatus', ''),
#             "Address1": row_dict.get('Address1', ''),
#             "Address2": row_dict.get('Address2', ''),
#             "Address3": row_dict.get('Address3', ''),
#             "Address4": row_dict.get('Address4', ''),
#             "Address5": row_dict.get('Address5', ''),
#             "Postcode": row_dict.get('Postcode', ''),
#             "Mobile": row_dict.get('Mobile', ''),
#             "Email": row_dict.get('Email', ''),
#             "BusinessAddress1": row_dict.get('BusinessAddress1', ''),
#             "BusinessAddress2": row_dict.get('BusinessAddress2', ''),
#             "BusinessAddress3": row_dict.get('BusinessAddress3', ''),
#             "BusinessAddress4": row_dict.get('BusinessAddress4', ''),
#             "BusinessAddress5": row_dict.get('BusinessAddress5', ''),
#             "BusinessPostcode": row_dict.get('BusinessPostcode', ''),
#             "BusinessTelephone": row_dict.get('BusinessTelephone', ''),
#             "BusinessFax": row_dict.get('BusinessFax', ''),
#             "BusinessEmail": row_dict.get('BusinessEmail', ''),
#             "JudicialInstructions": row_dict.get('JudicialInstructions', ''),
#             "JudicialInstructionsDate": str(row_dict.get('JudicialInstructionsDate')),
#             "Notes": row_dict.get('Notes', ''),
#             "OtherCentres": [centre['OtherCentres'] for centre in other_centres],
#             "Roles": [{"Role": role['Role'], "DateOfAppointment": str(role['DateOfAppointment']), "EndDateOfAppointment": str(role['EndDateOfAppointment'])} for role in roles],
#             "History": [{"HistDate": str(hist['HistDate']), "HistType": hist['HistType'], "UserName": hist['UserName'], "Comment": hist['Comment']} for hist in history]
#         }

#         # Convert the dictionary to a JSON string
#         json_content = json.dumps(json_data, indent=4)

#         # Step 9: Return the transformed JSON content
#         return json_content, "Success"

#     except Exception as e:
#         print(f"Error writing file for Adjudicator ID: {adjudicator_id}: {str(e)}")
#         return None, f"Error writing file: {str(e)}"

# # Define a function to run the JSON generation for each partition
# def process_json_partition(partition, judicial_officer_details_bc, other_centres_bc, roles_bc, history_bc):
#     results = []
#     for row in partition:
#         adjudicator_id = row['AdjudicatorId']
#         json_content, status = generate_json_content(
#             adjudicator_id,
#             judicial_officer_details_bc.value,
#             other_centres_bc.value,
#             roles_bc.value,
#             history_bc.value
#         )
        
#         if json_content is None:
#             continue
        
#         # Define the target path for each Adjudicator's JSON file
#         target_path = f"{gold_outputs}/JSON/judicial_officer_{adjudicator_id}.json"
        
#         # Upload the JSON content to Azure Blob Storage
#         blob_client = container_client.get_blob_client(target_path)
#         # blob_client.upload_blob(json_content, overwrite=True)

#         try:
#             blob_client.upload_blob(json_content, overwrite=True)
#         except Exception as e:
#             print(f"Error uploading HTML for Adjudicator ID {adjudicator_id}: {str(e)}")
#             continue  # Skip to the next Adjudicator

        
#         # Append the result
#         results.append((adjudicator_id, target_path, status))
#     return results

# @dlt.table(
#     name="gold_joh_json_generation_status",
#     comment="Delta Live Table for Adjudicator JSON Generation Status.",
#     path=f"{gold_mnt}/gold_joh_json_generation_status"
# )
# def gold_joh_json_generation_status():
#     df_stg_joh_filtered = dlt.read("stg_joh_filtered")
#     df_judicial_officer_details = dlt.read("silver_adjudicator_detail")
#     df_other_centres = dlt.read("silver_othercentre_detail")
#     df_roles = dlt.read("silver_appointment_detail")
#     df_history = dlt.read("silver_history_detail")

#     # Load the necessary dataframes from Hive metastore if not initial load
#     if read_hive == True:
#         df_stg_joh_filtered = spark.read.table(f"hive_metastore.{hive_schema}.stg_joh_filtered")
#         df_judicial_officer_details = spark.read.table(f"hive_metastore.{hive_schema}.silver_adjudicator_detail")
#         df_other_centres = spark.read.table(f"hive_metastore.{hive_schema}.silver_othercentre_detail")
#         df_roles = spark.read.table(f"hive_metastore.{hive_schema}.silver_appointment_detail")
#         df_history = spark.read.table(f"hive_metastore.{hive_schema}.silver_history_detail")

#     # Broadcast the dataframes to all workers
#     judicial_officer_details_bc = spark.sparkContext.broadcast(df_judicial_officer_details.collect())
#     other_centres_bc = spark.sparkContext.broadcast(df_other_centres.collect())
#     roles_bc = spark.sparkContext.broadcast(df_roles.collect())
#     history_bc = spark.sparkContext.broadcast(df_history.collect())

#     # Repartition the DataFrame by AdjudicatorId to optimize parallel processing
#     num_partitions = 32  # Setting this to optimize for your 8-worker cluster
#     repartitioned_df = df_stg_joh_filtered.repartition(32, "AdjudicatorId")

#     # Run the JSON generation on each partition
#     result_rdd = repartitioned_df.rdd.mapPartitions(lambda partition: process_json_partition(partition, judicial_officer_details_bc, other_centres_bc, roles_bc, history_bc))

#     # Collect results
#     results = result_rdd.collect()

#     # Create DataFrame for output
#     schema = StructType([
#         StructField("AdjudicatorId", LongType(), True),
#         StructField("JSON_Path", StringType(), True),
#         StructField("Status", StringType(), True)
#     ])
#     json_result_df = spark.createDataFrame(results, schema=schema)

#     # Log the JSON paths in the Delta Live Table
#     return json_result_df.select("AdjudicatorId", "JSON_Path", "Status")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate a360 files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create gold_adjudicator_a360_generation_status & Processing Adjudicator a360's
# MAGIC This section is to prallel process the a360 and create atracker table with log information.

# COMMAND ----------

# DBTITLE 1,Generating metadata in A360 Outputs
# # Function to format dates
# def format_date_zulu(date_value):
#     if isinstance(date_value, str):  # If the date is already a string, return as is
#         return date_value
#     elif isinstance(date_value, (datetime,)):
#         return datetime.strftime(date_value, "%Y-%m-%dT%H:%M:%SZ")
#     return None  # Return None if the date is invalid or not provided

# # Function to generate an .a360 file for a given adjudicator
# def generate_a360_file_for_adjudicator(adjudicator_id, joh_metadata_list, container_client):
#     try:
#         # Find judicial officer metadata
#         joh_metadata = next((row for row in joh_metadata_list if row['client_identifier'] == adjudicator_id), None)
#         if not joh_metadata:
#             print(f"No details found for Adjudicator ID: {adjudicator_id}")
#             return None, f"No details for Adjudicator ID: {adjudicator_id}"

#         # Create metadata, HTML, and JSON strings
#         metadata_data = {
#             "operation": "create_record",
#             "relation_id": joh_metadata.client_identifier,
#             "record_metadata": {
#                 "publisher": joh_metadata.publisher,
#                 "record_class": joh_metadata.record_class,
#                 "region": joh_metadata.region,
#                 "recordDate": format_date_zulu(joh_metadata.recordDate),
#                 "event_date": format_date_zulu(joh_metadata.event_date),
#                 "client_identifier": joh_metadata.client_identifier,
#                 "bf_001": joh_metadata.bf_001,
#                 "bf_002": joh_metadata.bf_002,
#                 "bf_003": joh_metadata.bf_003,
#                 "bf_004": format_date_zulu(joh_metadata.bf_004),
#                 "bf_005": joh_metadata.bf_005
#             }
#         }

#         html_data = {
#             "operation": "upload_new_file",
#             "relation_id": joh_metadata.client_identifier,
#             "file_metadata": {
#                 "publisher": joh_metadata.publisher,
#                 "dz_file_name": f"judicial_officer_{adjudicator_id}.html",
#                 "file_tag": "html"
#             }
#         }

#         json_data = {
#             "operation": "upload_new_file",
#             "relation_id": joh_metadata.client_identifier,
#             "file_metadata": {
#                 "publisher": joh_metadata.publisher,
#                 "dz_file_name": f"judicial_officer_{adjudicator_id}.json",
#                 "file_tag": "json"
#             }
#         }

#         # Convert dictionaries to JSON strings
#         metadata_data_str = json.dumps(metadata_data, separators=(',', ':'))
#         html_data_str = json.dumps(html_data, separators=(',', ':'))
#         json_data_str = json.dumps(json_data, separators=(',', ':'))

#         # Combine the data
#         all_data_str = f"{metadata_data_str}\n{html_data_str}\n{json_data_str}"

#         # Define the target path for each Adjudicator's A360 file
#         target_path = f"{gold_outputs}/A360/judicial_officer_{adjudicator_id}.a360"
        
#         # Upload the A360 content to Azure Blob Storage
#         # Not standard content so, write the text here
#         blob_client = container_client.get_blob_client(target_path)
#         blob_client.upload_blob(all_data_str, overwrite=True)


#         # print(f"A360 file created for Adjudicator with ID: {adjudicator_id} at {target_path}")
#         return target_path, "Success"
#     except Exception as e:
#         print(f"Error writing file for Adjudicator ID: {adjudicator_id}: {str(e)}")
#         return None, f"Error writing file: {str(e)}"

# # Define a function to run the A360 file generation for each partition
# def process_partition_a360(partition, joh_metadata_bc, container_client):
#     results = []
#     for row in partition:
#         adjudicator_id = row['AdjudicatorId']
#         file_name, status = generate_a360_file_for_adjudicator(
#             adjudicator_id,
#             joh_metadata_bc.value,
#             container_client
#         )
        
#         if file_name is None:
#             results.append((adjudicator_id, "", status))
#         else:
#             results.append((adjudicator_id, file_name, status))
#     return results


# @dlt.table(
#     name="gold_joh_a360_generation_status",
#     comment="Delta Live Table for Silver Adjudicator .a360 File Generation Status.",
#     path=f"{gold_mnt}/gold_joh_a360_generation_status"
# )
# def gold_joh_a360_generation_status():

#     df_stg_joh_filtered = dlt.read("stg_joh_filtered")
#     df_joh_metadata = dlt.read("silver_archive_metadata")

#     # Load necessary data from Hive tables if not initial load
#     if read_hive == True:
#         print("Running non initial load")
#         df_stg_joh_filtered = spark.read.table(f"hive_metastore.{hive_schema}.stg_joh_filtered")
#         df_joh_metadata = spark.read.table(f"hive_metastore.{hive_schema}.silver_archive_metadata")

#     # Broadcast the dataframes to all workers
#     joh_metadata_bc = spark.sparkContext.broadcast(df_joh_metadata.collect())

#     # Repartition the DataFrame by AdjudicatorId to optimize parallel processing
#     num_partitions = 32  # Setting this to optimize for your 8-worker cluster
#     repartitioned_df = df_stg_joh_filtered.repartition(num_partitions, "AdjudicatorId")

#     # # Define the container client for Azure Blob Storage
#     # container_client = BlobServiceClient.from_connection_string("your_connection_string").get_container_client("your_container_name")

#     # Run the A360 file generation on each partition
#     result_rdd = repartitioned_df.rdd.mapPartitions(lambda partition: process_partition_a360(partition, joh_metadata_bc, container_client))

#     # Collect results
#     results = result_rdd.collect()

#     # Create DataFrame for output
#     schema = StructType([
#         StructField("AdjudicatorId", LongType(), True),
#         StructField("GeneratedFilePath", StringType(), True),
#         StructField("Status", StringType(), True)
#     ])
#     a360_result_df = spark.createDataFrame(results, schema=schema)

#     # Log the A360 paths in the Delta Live Table
#     return a360_result_df.select("AdjudicatorId", "GeneratedFilePath", "Status")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix

# COMMAND ----------

# DBTITLE 1,Sample A360
#Sample frin td
# {"operation": "create_record","relation_id":"152820","record_metadata":{"publisher":"IADEMO","record_class":"IADEMO","region":"GBR","recordDate":"2023-04-12T00:00:00Z","event_date":"2023-04-12T00:00:00Z","client_identifier":"HU/02287/2021","bf_001":"Orgest","bf_002":"Hoxha","bf_003":"A1234567/001","bf_004":"1990-06-09T00:00:00Z","bf_005":"ABC/12345","bf_010":"2024-01-01T00:00:00Z"}}

# {"operation": "upload_new_file","relation_id":"152820","file_metadata":{"publisher":"IADEMO","dz_file_name":"HU_02287_2021.json","file_tag":"json"}}

# {"operation": "upload_new_file","relation_id":"152820","file_metadata":{"publisher":"IADEMO","dz_file_name":"HU022872021.pdf","file_tag":"pdf"}}
 

# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_joh_test.gold_joh_html_generation_status

# COMMAND ----------

# %sql
# drop schema ariadm_arm_joh cascade

# COMMAND ----------

# display(spark.read.format("binaryFile").load(f"{gold_mnt}/HTML").count())
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/JSON").count())
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/A360").count())
