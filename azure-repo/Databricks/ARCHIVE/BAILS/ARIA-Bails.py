# Databricks notebook source
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
# MAGIC     
# MAGIC    </tbody>
# MAGIC </table>
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# run custom functions
import sys
import os
# Append the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..','..')))

import dlt
import json
from pyspark.sql.functions import when, col,coalesce, current_timestamp, lit, date_format
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime





# COMMAND ----------

from SharedFunctionsLib.custom_functions import *

# COMMAND ----------

# Setting variables for use in subsequent cells
raw_mnt = "/mnt/ingest00rawsboxraw/ARIADM/ARM/JOH"
landing_mnt = "/mnt/ingest00landingsboxlanding/"
bronze_mnt = "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/JOH"
silver_mnt = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/JOH"
gold_mnt = "/mnt/ingest00curatedsboxgold/ARIADM/ARM/JOH"

# COMMAND ----------

# load in all the raw tables

@dlt.table(name="bail_raw_appeal_cases", comment="Raw Bail Appeal Cases",path=f"{raw_mnt}/raw_appeal_cases")
def bail_raw_appeal_cases():
    return read_latest_parquet("AppealCase","tv_AppealCase","ARIA_ARM_BAIL")

@dlt.table(name="bail_raw_case_respondents", comment="Raw Bail Case Respondents",path=f"{raw_mnt}/raw_case_respondents")
def bail_raw_case_respondents():
    return read_latest_parquet("CaseRespondent","tv_CaseRespondent","ARIA_ARM_BAIL")

@dlt.table(name="bail_raw_respondent", comment="Raw Bail Respondents",path=f"{raw_mnt}/raw_respondents")
def bail_raw_respondent():
    return read_latest_parquet("Respondent","tv_Respondent","ARIA_ARM_BAIL")

@dlt.table(name="bail_raw_main_respondent", comment="Raw Bail Main Respondent",path=f"{raw_mnt}/raw_main_respondent")
def bail_raw_main_respondent():
    return read_latest_parquet("MainRespondent","tv_MainRespondent","ARIA_ARM_BAIL")

@dlt.table(name="bail_raw_pou", comment="Raw Bail Pou",path=f"{raw_mnt}/raw_pou")
def bail_raw_pou():
    return read_latest_parquet("Pou","tv_Pou","ARIA_ARM_BAIL")

@dlt.table(name="bail_raw_file_location", comment="Raw Bail File Location",path=f"{raw_mnt}/raw_file_location")
def bail_raw_file_location():
    return read_latest_parquet("FileLocation","tv_FileLocation","ARIA_ARM_BAIL")

@dlt.table(name="bail_raw_case_rep", comment="Raw Bail Case Rep",path=f"{raw_mnt}/raw_case_rep")
def bail_raw_case_rep():
    return read_latest_parquet("CaseRep","tv_CaseRep","ARIA_ARM_BAIL")

@dlt.table(name="bail_raw_Representative", comment="Raw Bail Representative",path=f"{raw_mnt}/raw_Representative")
def bail_raw_Representative():
    return read_latest_parquet("Representative","tv_Representative","ARIA_ARM_BAIL")

@dlt.table(name="bail_raw_language", comment="Raw Bail Language",path=f"{raw_mnt}/raw_language")
def bail_raw_language():
    return read_latest_parquet("Language","tv_Language","ARIA_ARM_BAIL")

@dlt.table(name="bail_raw_cost_award", comment="Raw Bail Cost Award",path=f"{raw_mnt}/raw_cost_award")
def bail_raw_cost_award():
    return read_latest_parquet("CostAward","tv_CostAward","ARIA_ARM_BAIL") 
    





# COMMAND ----------


