# Databricks notebook source
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
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..','..')))

import dlt
import json
# from pyspark.sql.functions import when, col,coalesce, current_timestamp, lit, date_format
from pyspark.sql.functions import *
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions to Read Latest Landing Files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Variables

# COMMAND ----------

# MAGIC %md
# MAGIC Please note that running the DLT pipeline with the parameter `read_hive = true` will ensure the creation of the corresponding Hive tables. However, during this stage, none of the gold outputs (HTML, JSON, and A360) are processed. To generate the gold outputs, a secondary run with `read_hive = true` is required.

# COMMAND ----------

read_hive = True
# True

raw_mnt = "/mnt/ingest00rawsboxraw/ARIADM/ARM/TD"
landing_mnt = "/mnt/ingest00landingsboxlanding/"
bronze_mnt = "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/TD"
silver_mnt = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/TD"
gold_mnt = "/mnt/ingest00curatedsboxgold/ARIADM/ARM/TD"
file_path = "/mnt/ingest00landingsboxlanding/IRIS-TD-CSV/Example IRIS tribunal decisions data file.csv"
gold_outputs = "ARIADM/ARM/TD"
hive_schema = "ariadm_arm_td"
key_vault = "ingest00-keyvault-sbox"


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
checks["PrimaryKeyCheckforNULLS"] = "(CaseNo IS NOT NULL and Forenames IS NOT NULL and Name IS NOT NULL)"

@dlt.table(
    name="bronze_ac_ca_ant_fl_dt_hc",
    comment="Delta Live Table combining Appeal Case data with Case Appellant, Appellant, File Location, Department, and Hearing Centre.",
    path=f"{bronze_mnt}/bronze_ac_ca_ant_fl_dt_hc"
)
@dlt.expect_all_or_fail(checks)
def bronze_ac_ca_ant_fl_dt_hc():
    return (
        dlt.read("raw_appealcase").alias("ac")
            .join(
                dlt.read("raw_caseappellant").alias("ca"),
                col("ac.CaseNo") == col("ca.CaseNo"),
                "left_outer"
            )
            .join(
                dlt.read("raw_appellant").alias("a"),
                col("ca.AppellantId") == col("a.AppellantId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_filelocation").alias("fl"),
                col("ac.CaseNo") == col("fl.CaseNo"),
                "left_outer"
            )
            .join(
                dlt.read("raw_department").alias("d"),
                col("fl.DeptId") == col("d.DeptId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_hearingcentre").alias("hc"),
                col("d.CentreId") == col("hc.CentreId"),
                "left_outer"
            ).filter(col("ca.RelationShip").isNotNull())
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
                col("ac.AdtclmnFirstCreatedDatetime"),
                col("ac.AdtclmnModifiedDatetime"),
                col("ac.SourceFileName"),
                col("ac.InsertedByProcessName")
            )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_iris_extract 

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
        col('AdtclmnFirstCreatedDatetime'),
        col('AdtclmnModifiedDatetime'),
        col('SourceFileName'),
        col('InsertedByProcessName')
    )

    return df_iris

# COMMAND ----------

# RUN_ID = str(uuid.uuid4())  # Generates a unique RunID for each run


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
    path=f"{bronze_mnt}/stg_td_filtered"
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
    
    return joined_df.unionByName(iris_df)

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

@dlt.table(
    name="silver_archive_metadata",
    comment="Delta Live Silver Table for Archive Metadata data.",
    path=f"{silver_mnt}/silver_archive_metadata"
)
def silver_archive_metadata():
    td_df = dlt.read("bronze_ac_ca_ant_fl_dt_hc").alias("td").join(dlt.read("stg_td_filtered").alias('flt'), col("td.CaseNo") == col("flt.CaseNo"), "inner").select(
        col('td.CaseNo').alias('client_identifier'),
        date_format(col('td.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_date"),
        date_format(col('td.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("recordDate"),
        lit("GBR").alias("region"),
        lit("ARIA").alias("publisher"),
        lit("ARIA Tribunal Decision").alias("record_class"),
        lit('IA_Tribunal').alias("entitlement_tag"),
        col('td.Forenames').alias('bf_001'),
        col('td.Name').alias('bf_002'),
        col('td.BirthDate').alias('bf_003'),
        col('td.HORef').alias('bf_004'),
        col('td.PortReference').alias('bf_005')
    )
    iris_df = dlt.read("bronze_iris_extract").alias("iris").select(
        col('iris.CaseNo').alias('client_identifier'),
        date_format(col('iris.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_date"),
        date_format(col('iris.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("recordDate"),
        lit("GBR").alias("region"),
        lit("ARIA").alias("publisher"),
        lit("ARIA Tribunal Decision").alias("record_class"),
        lit('IA_Tribunal').alias("entitlement_tag"),
        col('iris.Forenames').alias('bf_001'),
        col('iris.Name').alias('bf_002'),
        col('iris.BirthDate').alias('bf_003'),
        col('iris.HORef').alias('bf_004'),
        col('iris.PortReference').alias('bf_005')
    )
    return td_df.unionByName(iris_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Outputs and Tracking DLT Table Creation

# COMMAND ----------

# DBTITLE 1,Secret Retrieval for Database Connection
secret = dbutils.secrets.get(key_vault, "curatedsbox-connection-string-sbox")

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
html_template_list = spark.read.text("/mnt/ingest00landingsboxhtml-template/TD-Details-no-js-v1.html").collect()
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
        return f"Error"

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
        return f"error: {str(e)}"

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
                "bf_005": row.bf_005 or ""
            }
        }

        html_data = {
            "operation": "upload_new_file",
            "relation_id": row.client_identifier,
            "file_metadata": {
                "publisher": row.publisher,
                "dz_file_name": f"tribunal_decision_{row.client_identifier.replace('/', '_')}_{row.bf_001}_{row.bf_002}.html",
                "file_tag": "html"
            }
        }

        json_data = {
            "operation": "upload_new_file",
            "relation_id": row.client_identifier,
            "file_metadata": {
                "publisher": row.publisher,
                "dz_file_name": f"tribunal_decision_{row.client_identifier.replace('/', '_')}_{row.bf_001}_{row.bf_002}.json",
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

# DBTITLE 1,Transformation: stg_td_iris_unified
@dlt.table(
    name="stg_td_iris_unified",
    comment="Delta Live unified stage Gold Table for gold outputs.",
    path=f"{gold_mnt}/stg_td_iris_unified"
)
def stg_td_iris_unified():

    df_tribunaldecision_detail = dlt.read("silver_tribunaldecision_detail")
    df_td_metadata = dlt.read("silver_archive_metadata")

     # Optional: Load from Hive if not an initial load
    if read_hive:
        df_combined = spark.read.table(f"hive_metastore.{hive_schema}.stg_td_iris_unified")
        df_tribunaldecision_detail = spark.read.table(f"hive_metastore.{hive_schema}.silver_tribunaldecision_detail")
        df_td_metadata = spark.read.table(f"hive_metastore.{hive_schema}.silver_archive_metadata")
    

    # Apply the UDF to the combined DataFrame
    df_with_html_json = df_tribunaldecision_detail.withColumn("HTMLContent", generate_html_udf(struct(*df_tribunaldecision_detail.columns))) \
                                            .withColumn("JSONcollection", to_json(struct(*df_tribunaldecision_detail.columns))) \
                                            .withColumn("HTMLFileName", concat(lit(f"{gold_outputs}/HTML/tribunal_decision_"), regexp_replace(col("CaseNo"), "/", "_"), lit("_"), col("Forenames"), lit("_"), col("Name"), lit(".html"))) \
                                            .withColumn("JSONFileName", concat(lit(f"{gold_outputs}/JSON/tribunal_decision_"), regexp_replace(col("CaseNo"), "/", "_"), lit("_"), col("Forenames"), lit("_"), col("Name"), lit(".json")))

    # Select distinct client identifiers with HTML and JSON content and order them
    metadata_df = df_td_metadata.alias('a').join(df_with_html_json.alias('b'), ((col('b.CaseNo') == col('a.client_identifier')) & (col('b.Forenames') == col('a.bf_001')) & (col('b.Name') == col('a.bf_002'))), 'left').filter((~col("HTMLContent").like("Error%")) & (~col("JSONcollection").like("Error%"))).select("client_identifier", "bf_001", "bf_002").distinct().orderBy("client_identifier", "bf_001", "bf_002")

    # Define a window specification to assign row numbers  
    window_spec = Window.orderBy("client_identifier","bf_001","bf_002")
    df_batch = metadata_df.withColumn("row_num", row_number().over(window_spec)) \
                        .withColumn("A360BatchId", floor((col("row_num") - 1) / 250) + 1)

    # Join the batch information with the original metadata
    df_metadata = df_td_metadata.join(df_batch, ["client_identifier", "bf_001", "bf_002"], "left")

    # Repartition the DataFrame to optimize parallelism
    # repartitioned_df = df_metadata.repartition(64, col("client_identifier"))

    # Generate A360 content and associated file names
    df_with_a360 = df_metadata.withColumn(
        "A360Content", generate_a360_udf(struct(*df_td_metadata.columns))
    ).withColumn(
        "A360FileName", when(col("A360BatchId").isNotNull(), concat(lit(f"{gold_outputs}/A360/judicial_officer_"), col("A360BatchId"), lit(".a360"))).otherwise(lit(None))
    ).select(col("client_identifier").alias("CaseNo"),col("bf_001").alias("Forenames"),col("bf_002").alias("Name"), "A360BatchId", "A360FileName", "A360Content")

    df_unified =  df_with_html_json.join(df_with_a360, ["CaseNo","Forenames","Name"], "left")

      # Optionally load data from Hive
    if read_hive:
        display(df_unified)

    return df_unified

# COMMAND ----------

num_cores = spark.sparkContext.defaultParallelism  # Get available cores
optimal_partitions = 16 * 2  # 2x cores for parallelism
# repartitioned_df = df_combined.repartition(optimal_partitions)
optimal_partitions

# COMMAND ----------

# DBTITLE 1,Transformation gold_td_iris_with_html
checks = {}
checks["html_content_no_error"] = "(HTMLContent NOT LIKE 'Error%')"
checks["html_content_no_error"] = "(HTMLContent IS NOT NULL)"
checks["UploadStatus_no_error"] = "(UploadStatus NOT LIKE 'Error%')"

@dlt.table(
    name="gold_td_iris_with_html",
    comment="Delta Live Gold Table with HTML content.",
    path=f"{gold_mnt}/gold_td_iris_with_html"
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
        "UploadStatus", upload_udf(col("HTMLFileName"), col("HTMLContent"))
    )
    
    # # Upload HTML files to Azure Blob Storage
    # df_combined.select("CaseNo","Forenames","Name", "HTMLContent","HTMLFileName").repartition(64).foreach(upload_html)

    # Optionally load data from Hive
    if read_hive:
        display(df_with_upload_status.select("CaseNo","Forenames","Name", "A360BatchId","HTMLContent","HTMLFileName","UploadStatus"))

    # Return the DataFrame for DLT table creation
    return df_with_upload_status.select("CaseNo","Forenames","Name", "A360BatchId","HTMLContent","HTMLFileName","UploadStatus")

# COMMAND ----------

# DBTITLE 1,Transformation gold_td_iris_with_json
checks = {}
checks["json_content_no_error"] = "(JSONcollection IS NOT NULL)"
checks["UploadStatus_no_error"] = "(UploadStatus NOT LIKE 'Error%')"



@dlt.table(
    name="gold_td_iris_with_json",
    comment="Delta Live Gold Table with JSON content.",
    path=f"{gold_mnt}/gold_td_iris_with_json"
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
        "UploadStatus", upload_udf(col("JSONFileName"), col("JSONcollection"))
    )

    # Optionally load data from Hive
    if read_hive:
        display(df_with_upload_status)

    # Return the DataFrame for DLT table creation
    return df_with_upload_status.select("CaseNo","Forenames","Name","A360BatchId","JSONcollection","JSONFileName","UploadStatus")


# COMMAND ----------

# DBTITLE 1,Transformation gold_td_iris_with_a360
checks = {}
checks["A360Content_no_error"] = "(consolidate_A360Content NOT LIKE 'Error%')"
checks["A360_content_no_error"] = "(consolidate_A360Content IS NOT NULL)"
checks["UploadStatus_no_error"] = "(UploadStatus NOT LIKE 'Error%')"

@dlt.table(
    name="gold_td_iris_with_a360",
    comment="Delta Live Gold Table with A360 content.",
    path=f"{gold_mnt}/gold_td_iris_with_a360"
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
    df_agg = df_a360.groupBy("A360FileName", "A360BatchId") \
            .agg(concat_ws("\n", collect_list("A360Content")).alias("consolidate_A360Content")) \
            .select(col("A360FileName"), col("consolidate_A360Content"), col("A360BatchId"))

    # Repartition the DataFrame to optimize parallelism
    repartitioned_df = df_agg.repartition(optimal_partitions)

    # Remove existing files
    dbutils.fs.rm(f"{gold_outputs}/A360", True)

    # Generate A360 content
    df_with_a360 = repartitioned_df.withColumn(
        "UploadStatus", upload_udf(col("A360FileName"), col("consolidate_A360Content"))
    )

    # # Optionally load data from Hive
    if read_hive:
        display(df_with_a360)
   
    return df_with_a360.select("A360BatchId", "consolidate_A360Content", "A360FileName", "UploadStatus")


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
# select * from hive_metastore.ariadm_arm_td.gold_td_iris_with_a360 where UploadStatus != 'success'  and A360Content like '%ERROR%'

# COMMAND ----------

# %sql
# drop schema hive_metastore.ariadm_arm_td cascade
