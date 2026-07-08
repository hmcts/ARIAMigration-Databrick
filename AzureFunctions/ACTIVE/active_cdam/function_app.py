import asyncio
import azure.functions as func
import logging
import json
import os

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob.aio import BlobServiceClient
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.identity.aio import DefaultAzureCredential
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets.aio import SecretClient
from typing import List

try:
    from .cdamFunctions import process_event
except Exception:
    # Fallback for running the script directly during local debugging.
    from cdamFunctions import process_event

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

ENV = os.environ["ENVIRONMENT"] or "sbox"
LZ_KEY = os.environ["LZ_KEY"]
ARIA_NAME = "active"

eventhub_name = f"evh-active-cdam-pub-{ENV}-{LZ_KEY}-uks-dlrm-01"
eventhub_connection = "sboxdlrmeventhubns_RootManageSharedAccessKey_EVENTHUB"

kv_url = f"https://ingest{LZ_KEY}-meta002-{ENV}.vault.azure.net"
results_eh_name = f"evh-active-cdam-res-{ENV}-{LZ_KEY}-uks-dlrm-01"
idempotency_account_url = f"https://ingest{LZ_KEY}xcutting{ENV}.blob.core.windows.net"
idempotency_container_name = "af-idempotency"

app = func.FunctionApp()


@app.function_name("eventhub_trigger")
@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name=eventhub_name,
    consumer_group='$Default',
    connection=eventhub_connection,
    starting_position="@latest",
    cardinality='many',
    max_batch_size=1,
    data_type='binary'
)
async def eventhub_trigger_active(azeventhub: List[func.EventHubEvent]):
    logger.info(f"Processing a batch of {len(azeventhub)} events")

    try:
        # Retrieve credentials
        credential = DefaultAzureCredential()
        logger.info("Connected to Azure Credentials")

        kv_client = SecretClient(vault_url=kv_url, credential=credential)
        logger.info(f"Connected to KeyVault: {kv_url}")

        results_eh_key = await kv_client.get_secret(f"{results_eh_name}-key")
        result_eh_secret_key = results_eh_key.value
        logger.info("Acquired KV secret for Results Event Hub")

        storage_sp_tenant_id = await kv_client.get_secret("SERVICE-PRINCIPLE-TENANT-ID")
        storage_sp_client_id = await kv_client.get_secret("SERVICE-PRINCIPLE-CLIENT-ID")
        storage_sp_client_secret = await kv_client.get_secret("SERVICE-PRINCIPLE-CLIENT-SECRET")
        storage_credential = ClientSecretCredential(
            tenant_id=storage_sp_tenant_id.value,
            client_id=storage_sp_client_id.value,
            client_secret=storage_sp_client_secret.value
        )
        logger.info("Acquired KV service principal credentials for accessing ARIA DLRM storage accounts.")

        idempotency_blob_service = BlobServiceClient(account_url=idempotency_account_url, credential=credential)
        idempotency_container = idempotency_blob_service.get_container_client(idempotency_container_name)

        res_eh_producer = EventHubProducerClient.from_connection_string(conn_str=result_eh_secret_key)

        async with res_eh_producer:
            logger.info(f"Creating batch for {len(azeventhub)} events")
            event_data_batch = await res_eh_producer.create_batch()
            try:
                for event in azeventhub:
                    try:
                        logger.info(f"Event received with partition key: {event.partition_key}")

                        # Parse the payload
                        caseNo = event.partition_key
                        payload_str = event.get_body().decode('utf-8')
                        payload = json.loads(payload_str)
                        run_id = payload.get("RunID", None)
                        file_name = payload.get("FileName")
                        file_url = payload.get("FileURL")
                        file_content_type = payload.get("FileContentType", "text/html")  # Default to HTML content type.

                        idempotency_blob_path = f"active/cdam/idempotency/{caseNo.replace('/', '_')}.flag"
                        idempotency_blob = idempotency_container.get_blob_client(idempotency_blob_path)

                        try:
                            # upload_blob is an atomic operation so awaiting the result ensures only 1 event runs for the state/caseNo.
                            await idempotency_blob.upload_blob(b"", overwrite=False)
                            logger.info(f"[IDEMPOTENCY][CDAM] Case note upload for {caseNo} is being processed.")
                        except ResourceExistsError:
                            logger.warning(f"[IDEMPOTENCY][CDAM] Skipping in progress case {caseNo}.")
                            continue

                        result = await asyncio.to_thread(process_event, ENV, caseNo, run_id, file_name, file_url, file_content_type, storage_credential)

                        # Mark processed if success
                        if result.get("Status") == "SUCCESS":
                            logger.info(f"CDAM upload document for: {caseNo} is successful.")
                        else:
                            try:
                                await idempotency_blob.delete_blob()
                                logger.info(f"[IDEMPOTENCY][CDAM] Deleting idempotency blob: {caseNo}.")
                            except Exception as delete_error:
                                logger.warning(f"[IDEMPOTENCY][CDAM] Failed to delete blob for {caseNo}: {delete_error}")

                        result_json = json.dumps(result)

                        try:
                            event_data_batch.add(EventData(result_json))
                        except ValueError:
                            # If the batch is full, send it and create a new one
                            await res_eh_producer.send_batch(event_data_batch)
                            logger.info("Sent a batch of events to Results Event Hub")
                            event_data_batch = None  # Force cleardown on successful send to prevent sending duplicate events
                            event_data_batch = await res_eh_producer.create_batch()
                            event_data_batch.add(EventData(result_json))

                    except Exception as e:
                        logger.error(f"Error processing event for caseNo {caseNo}: {e}")

                # Send any remaining events in the batch
                if event_data_batch and len(event_data_batch) > 0:
                    await res_eh_producer.send_batch(event_data_batch)
                    logger.info("Sent the final batch of events to Results Event Hub")

            except Exception as e:
                logger.error(f"Error in event hub processing batch: {e}")
            finally:
                # Clean up all clients
                await kv_client.close()
                await credential.close()
    except Exception as e:
        logger.error(f"An error has occurred before processing the batch. {e}")
        raise e
