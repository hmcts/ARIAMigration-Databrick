# Databricks notebook source
dbutils.fs.unmount('/mnt/dropzone')

# COMMAND ----------

# Storage account and container details
storage_account_name = "a360c2x2555dz"
container_name = "dropzone"
folder_name = "ARIAJR"
sas_token = "xxxxx"

# Define the Azure Blob Storage URL
source_url = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"
# source_url = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"

# Mount point path in Databricks
mount_point = f"/mnt/{container_name}"

# Mounting the container
try:
    dbutils.fs.mount(
        source=source_url,
        mount_point=mount_point,
        extra_configs={f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
    )
    print(f"Container '{container_name}' mounted successfully at '{mount_point}'")
except Exception as e:
    print(f"Error mounting container: {e}")

# Access the specific folder
folder_path = f"{mount_point}/{folder_name}"

# List files in the folder to verify
try:
    display(dbutils.fs.ls(folder_path))
    print(f"Folder '{folder_name}' accessed successfully at '{folder_path}'")
except Exception as e:
    print(f"Error accessing folder '{folder_name}': {e}")


# COMMAND ----------

dbutils.fs.ls('/mnt/dropzone/ARIAJR')

# COMMAND ----------

df = spark.read.text("dbfs:/mnt/dropzone/ARIAJR/response/5b6345f883e63fc04ae489d6cf098c46_78bff80d-590f-7194-814d-2184b577fd36_1_cr.rsp")
display(df)
