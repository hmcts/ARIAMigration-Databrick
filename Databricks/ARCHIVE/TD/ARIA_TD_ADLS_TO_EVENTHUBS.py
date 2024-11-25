# Databricks notebook source
# MAGIC %md
# MAGIC # Tribunal Decision Archive from Gold to Event Hubs
# MAGIC <table style='float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>Name: </b></td>
# MAGIC          <td>ARIA_TD_ADLS_TO_EVENTHUBS</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>Description: </b></td>
# MAGIC          <td>Notebook to transfer a set of HTML, JSON, and A360 files, each representing data on Tribunal Decisions stored in ARIA, to Event Hubs.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>First Created: </b></td>
# MAGIC          <td>Oct-2024</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <th style='text-align: left;'><b>Changelog (JIRA ref / initials / date):</b></th>
# MAGIC          <th>Comments</th>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><a href="https://tools.hmcts.net/jira/browse/ARIADM-xxx">ARIADM-xxx</a> / NSA / OCT-2024</td>
# MAGIC          <td>Tribunal Decision and IRIS: Transfer data from Gold ADLS to Event Hubs</td>
# MAGIC       </tr>
# MAGIC    </tbody>
# MAGIC </table>
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import Binaries
from confluent_kafka import Producer
import json
# from  itertools import islice
# import numpy as np
from pyspark.sql.functions import col, decode, split, element_at
import logging
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext
