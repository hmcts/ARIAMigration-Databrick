# Pre-requisites. You need to login to az cli locally with your user having:
# - read permissions on the ingest meta002 vault and also read permissions.
# - read permissions on the ingest curated storage account.
# - be on F5 VPN to access the curated storage account.
# To get the count of records to be deleted, run:
# python3 dm_store_meta_cleardown.py --lz-key 01 --env stg
# To perform the actual delete, run:
# python3 dm_store_meta_cleardown.py --lz-key 01 --env stg --delete-run

import argparse
import logging
import adlfs
import pandas as pd
import psycopg2
import pyarrow.dataset as ds
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient
from deltalake import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lz-key", default="00")
    parser.add_argument("--env", default="sbox")
    parser.add_argument("--delete-run", action="store_true")
    return parser.parse_args()

SECRET_KEYS = [
    "DM-STORE-HOST",
    "DM-STORE-PORT",
    "DM-STORE-DB",
    "DM-STORE-USER",
    "DM-STORE-PASS",
]


def get_secrets(vault_url: str, credential: AzureCliCredential) -> dict[str, str]:
    client = SecretClient(vault_url=vault_url, credential=credential)
    secrets = {}
    for key in SECRET_KEYS:
        logger.info(f"Retrieving secret: {key}")
        secrets[key] = client.get_secret(key).value
        logger.info(f"Successfully retrieved: {key}")
    return secrets


def connect(secrets: dict[str, str]) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=secrets["DM-STORE-HOST"],
        port=int(secrets["DM-STORE-PORT"]),
        dbname=secrets["DM-STORE-DB"],
        user=secrets["DM-STORE-USER"],
        password=secrets["DM-STORE-PASS"],
        connect_timeout=10,
    )
    logger.info("Database connection established")
    return conn


def read_delta(storage_account: str, delta_path: str, credential: AzureCliCredential) -> pd.DataFrame:
    storage_options = {
        "account_name": storage_account,
        "use_azure_cli": "true",
    }
    logger.info(f"Reading delta table from: {delta_path}")
    dt = DeltaTable(delta_path, storage_options=storage_options)

    active_files = [f.removeprefix("az://") for f in dt.file_uris()]
    logger.info(f"Found {len(active_files)} active parquet files in transaction log")

    fs = adlfs.AzureBlobFileSystem(account_name=storage_account, credential=credential)
    dataset = ds.dataset(active_files, filesystem=fs, format="parquet")
    df = dataset.to_table().to_pandas()
    logger.info(f"Read {len(df):,} rows from ack_audit")
    return df


def main():
    args = parse_args()
    credential = AzureCliCredential()

    ack_audit_path = "az://silver/ARIADM/ACTIVE/CCD/AUDIT/APPEALS/CDAM/ack_audit"
    audit_results = read_delta(f"ingest{args.lz_key}curated{args.env}", ack_audit_path, credential)

    uuids = audit_results["document_url"].str.split("/").str[-1].tolist()
    logger.info(f"Extracted {len(uuids):,} UUIDs from document_url")
    logger.info(uuids)

    if args.delete_run:
        query = """
            DELETE FROM documentmetadata
            WHERE name = 'case_id'
            AND documentmetadata_id IN %s
        """
    else:
        query = """
            SELECT COUNT(*) FROM documentmetadata
            WHERE name = 'case_id'
            AND documentmetadata_id IN %s
        """

    logger.info("About to run query:")
    logger.info(query)

    keyvault_url = f"https://ingest{args.lz_key}-meta002-{args.env}.vault.azure.net/"
    secrets = get_secrets(keyvault_url, credential)
    conn = connect(secrets)
    try:
        with conn.cursor() as cur:
            cur.execute(query, (tuple(uuids),))
            if args.delete_run:
                logger.info(f"Deleted {cur.rowcount:,} rows from documentmetadata")
            else:
                count = cur.fetchone()[0]
                logger.info(f"Count of records for deletion = {count:,}")
        conn.commit()
    finally:
        conn.close()
        logger.info("Connection closed")


if __name__ == "__main__":
    main()
