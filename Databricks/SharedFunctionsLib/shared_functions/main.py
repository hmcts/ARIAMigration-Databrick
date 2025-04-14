#def main():
#    print("Hello from shared_functions.main!")

import os
from pyspark.sql import SparkSession
def main():
    print("Setting Spark configs...")
    spark = SparkSession.builder.getOrCreate()
    # Read values from environment variables
    client_id = os.environ["CLIENT_ID"]
    client_secret = os.environ["CLIENT_SECRET"]
    tenant_url = os.environ["TENANT_ID"]  # full URL like https://login.microsoftonline.com/<tenant>/oauth2/token
    #raw_storage_account_key = os.environ["RawStorageAccountKey"]
    storage_account = "ingest02rawsbox"
    # Set Spark configs before any access to ADLS
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", tenant_url)
    print("Configs set. About to access ADLS")

    print("=== Finished setting configs ===")
    # Optional: list configs to confirm
    print("CLIENT_ID:", spark.conf.get(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net"))
    print("TENANT_URL:", spark.conf.get(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net"))
    print("=== Attempting to access ADLS ===")
    try:
        spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        ).listStatus(
            spark._jvm.org.apache.hadoop.fs.Path("abfss://raw@ingest02rawsbox.dfs.core.windows.net/")
        )
        print("=== ADLS access successful ===")
    except Exception as e:
        print("=== ADLS access failed ===")
        print(str(e))