import asyncio
import azure.functions as func
import logging
import json
import os

from tenacity import AsyncRetrying, retry_if_result, stop_after_attempt, wait_exponential

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob.aio import BlobServiceClient
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient
from typing import List

try:
    from .cl_ccdFunctions import process_event
except Exception:
    # Fallback for running the script directly during local debugging.
    from cl_ccdFunctions import process_event

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

ENV = os.environ["ENVIRONMENT"]
LZ_KEY = os.environ["LZ_KEY"]
PR_REFERENCE = os.environ.get("PR_REFERENCE", "pr-" + os.environ.get("PR_NUMBER", "1")) if ENV == "sbox" else None
ARIA_NAME = "active"

eventhub_name = f"evh-active-caselink-pub-{ENV}-{LZ_KEY}-uks-dlrm-01"
eventhub_connection = "sboxdlrmeventhubns_RootManageSharedAccessKey_EVENTHUB"

app = func.FunctionApp()


def _log_retry(retry_state):
    result = retry_state.outcome.result() if retry_state.outcome else {}
    error = result.get("Error", "") if isinstance(result, dict) else ""
    logger.warning(
        f"Retrying process_event — attempt {retry_state.attempt_number} failed "
        f"(sleeping {retry_state.next_action.sleep:.0f}s): {error}"
    )


def _is_retryable(result):
    RETRYABLE_STATUS_CODES = {408, 409, 429, 500, 502, 503, 504}
    if not (isinstance(result, dict) and result.get("Status") == "ERROR"):
        return False
    return result.get("StatusCode") in RETRYABLE_STATUS_CODES


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

    # Retrieve credentials
    credential = DefaultAzureCredential()
    logger.info("Connected to Azure Credentials")

    kv_url = f"https://ingest{LZ_KEY}-meta002-{ENV}.vault.azure.net"
    kv_client = SecretClient(vault_url=kv_url, credential=credential)
    logger.info(f"Connected to KeyVault: {kv_url}")

    results_eh_name = f"evh-active-caselink-res-{ENV}-{LZ_KEY}-uks-dlrm-01"
    results_eh_key = await kv_client.get_secret(f"{results_eh_name}-key")
    result_eh_secret_key = results_eh_key.value
    logger.info("Acquired KV secret for Results Event Hub")

    idempotency_account_url = f"https://ingest{LZ_KEY}xcutting{ENV}.blob.core.windows.net"
    idempotency_container_name = "af-idempotency"
    idempotency_blob_service = BlobServiceClient(account_url=idempotency_account_url, credential=credential)
    idempotency_container = idempotency_blob_service.get_container_client(idempotency_container_name)

    res_eh_producer = EventHubProducerClient.from_connection_string(conn_str=result_eh_secret_key)

    async with res_eh_producer:
        event_data_batch = await res_eh_producer.create_batch()
        try:
            for event in azeventhub:
                try:
                    logger.info(f"Event received with partition key: {event.partition_key}")

                    # Parse the payload
                    ccdReference = event.partition_key
                    payload_str = event.get_body().decode('utf-8')
                    payload = json.loads(payload_str)
                    run_id = payload.get("RunID", None)
                    data = payload.get("CaseLinkPayload", None)
                    overwrite = payload.get("Overwrite", None)

                    idempotency_blob_path = f"active/caselink/idempotency/{ccdReference}.flag"
                    idempotency_blob = idempotency_container.get_blob_client(idempotency_blob_path)

                    try:
                        # upload_blob is an atomic operation so awaiting the result ensures only 1 event runs for the state/caseNo.
                        await idempotency_blob.upload_blob(b"", overwrite=False)
                        logger.info(f"[IDEMPOTENCY][CASELINK] Case linking for {ccdReference} is being processed.")
                    except ResourceExistsError:
                        logger.warning(f"[IDEMPOTENCY][CASELINK] Skipping in progress case {ccdReference}.")
                        continue

                    async def _process():
                        return await asyncio.to_thread(process_event, ENV, ccdReference, run_id, data, PR_REFERENCE, overwrite)

                    result = await AsyncRetrying(
                        retry=retry_if_result(_is_retryable),
                        stop=stop_after_attempt(3),
                        wait=wait_exponential(min=30, max=60),
                        before_sleep=_log_retry,
                        retry_error_callback=lambda retry_state: retry_state.outcome.result(),
                    )(_process)

                    # Skip if marked for SKIPPED
                    if result.get("Status") == "SKIPPED":
                        logger.info(f"Case linking skipped for {ccdReference} as same links already exist in CCD")
                        continue

                    # Mark processed if success
                    if result.get("Status") == "SUCCESS":
                        logger.info(f"Case linking processed from: {ccdReference} to: {', '.join(str(obj['id']) for obj in data if 'id' in obj)}")
                    else:
                        try:
                            await idempotency_blob.delete_blob()
                            logger.info(f"[IDEMPOTENCY][CASELINK] Deleting idempotency blob: {ccdReference}.")
                        except Exception as delete_error:
                            logger.warning(f"[IDEMPOTENCY][CASELINK] Failed to delete blob for {ccdReference}: {delete_error}")

                    result.pop("StatusCode", None)
                    result_json = json.dumps(result)

                    try:
                        event_data_batch.add(EventData(result_json))
                    except ValueError:
                        # If the batch is full, send it and create a new one
                        await res_eh_producer.send_batch(event_data_batch)
                        logger.info("Sent a batch of events to Results Event Hub")
                        event_data_batch = await res_eh_producer.create_batch()
                        event_data_batch.add(EventData(result_json))

                except Exception as e:
                    logger.error(f"Error processing event for caseNo {ccdReference}: {e}")

            # Send any remaining events in the batch
            if len(event_data_batch) > 0:
                await res_eh_producer.send_batch(event_data_batch)
                logger.info("Sent the final batch of events to Results Event Hub")

        except Exception as e:
            logger.error(f"Error in event hub processing batch: {e}")
        finally:
            # Clean up all clients
            await kv_client.close()
            await credential.close()
