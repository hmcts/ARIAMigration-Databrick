#!/bin/bash

# Run a Python script inside the init script
/databricks/python3/bin/python3 <<EOF
from pyspark.sql import SparkSession

# Define function to get dbutils
def det_dbutils():
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(SparkSession.builder.getOrCreate())
    except ImportError:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]

# Initialize dbutils
dbutils = get_dbutils()

secret_scope = "ingest00-keyvault-sbox"
client_id = dbutils.secrets.get(scope=secret_scope, key="SERVICE-PRINCIPLE-CLIENT-ID")
client_secret = dbutils.secrets.get(scope=secret_scope, key="SERVICE-PRINCIPLE-CLIENT-SECRET")
tenant_id = dbutils.secrets.get(scope=secret_scope, key="SERVICE-PRINCIPLE-TENANT-ID")
storage_account = "ingest02rawsbox"  

# Get Spark session
spark = SparkSession.builder.getOrCreate()

# Set Spark configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
EOF
