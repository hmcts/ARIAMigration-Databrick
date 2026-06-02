# Pre-requisites. You need to login to az cli locally with your user having:
# - read permissions on the ingest meta002 vault and also read permissions.
# - read permissions on the ingest curated storage account.
# - be on F5 VPN to access the curated storage account.

import argparse
import logging
import pandas as pd
import psycopg2
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


def read_delta(storage_account, delta_path, credential: AzureCliCredential) -> pd.DataFrame:
    token = credential.get_token("https://storage.azure.com/.default").token
    storage_options = {
        "azure_storage_account_name": storage_account,
        "azure_storage_token": token,
    }
    logger.info(f"Reading Delta table from: {delta_path}")
    dt = DeltaTable(delta_path, storage_options=storage_options)
    df = dt.to_pandas()
    logger.info(f"Read {len(df):,} rows from ack_audit")
    return df


def main():
    args = parse_args()
    credential = AzureCliCredential()

    ack_audit_path = "az://silver/ARIADM/ACTIVE/CCD/AUDIT/APPEALS/CDAM/ack_audit"
    audit_results = read_delta(f"ingest{args.lz_key}curated{args.env}", ack_audit_path, credential)

    audit_results.show()

    keyvault_url = f"https://ingest{args.lz_key}-meta002-{args.env}.vault.azure.net/"
    secrets = get_secrets(keyvault_url, credential)
    conn = connect(secrets)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            logger.info(f"Connected to: {version[0]}")
    finally:
        conn.close()
        logger.info("Connection closed")


if __name__ == "__main__":
    main()
