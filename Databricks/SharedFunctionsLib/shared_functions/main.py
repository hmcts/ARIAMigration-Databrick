import argparse
from pyspark.sql import SparkSession
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant-id")
    args = parser.parse_args()
    tenant_id = args.tenant_id
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set(
        "fs.azure.account.oauth2.client.endpoint.ingest02rawsbox.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    )