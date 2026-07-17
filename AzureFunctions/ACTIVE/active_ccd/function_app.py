import asyncio
import azure.functions as func
import logging
import json
import os

from contextlib import AsyncExitStack

from tenacity import AsyncRetrying, retry_if_result, stop_after_attempt, wait_exponential

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob.aio import BlobServiceClient
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient
from datetime import datetime, timezone
from typing import List

try:
    # When running as a function app the module will be a package. Use a
    # relative import where possible.
    from .ccdFunctions import process_case
except Exception:
    # Fallback for running the script directly during local debugging.
    from ccdFunctions import process_case

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

ENV = os.environ["ENVIRONMENT"]
LZ_KEY = os.environ["LZ_KEY"]
PR_REFERENCE = os.environ.get("PR_REFERENCE", "pr-" + os.environ.get("PR_NUMBER", "1")) if ENV == "sbox" else None
ARIA_NAME = "active"

eventhub_name = f"evh-active-pub-{ENV}-{LZ_KEY}-uks-dlrm-01"
eventhub_connection = "sboxdlrmeventhubns_RootManageSharedAccessKey_EVENTHUB"

kv_url = f"https://ingest{LZ_KEY}-meta002-{ENV}.vault.azure.net"
results_eh_name = f"evh-active-res-{ENV}-{LZ_KEY}-uks-dlrm-01"
idempotency_account_url = f"https://ingest{LZ_KEY}xcutting{ENV}.blob.core.windows.net"
idempotency_container_name = "af-idempotency"

app = func.FunctionApp()


def _log_retry(retry_state):
    result = retry_state.outcome.result() if retry_state.outcome else {}
    error = result.get("Error", "") if isinstance(result, dict) else ""
    logger.warning(
        f"Retrying process_case — attempt {retry_state.attempt_number} failed "
        f"(sleeping {retry_state.next_action.sleep:.0f}s): {error}"
    )


def _is_retryable(result):
    RETRYABLE_STATUS_CODES = {408, 409, 429, 500, 502, 503, 504}
    TRANSIENT_ERROR_TYPES = {
        "ConnectionError", "ConnectTimeout", "ReadTimeout", "Timeout",
        "ChunkedEncodingError", "SSLError", "ProxyError", "EOFError",
    }
    if not (isinstance(result, dict) and result.get("Status") == "ERROR"):
        return False
    if result.get("StatusCode") in RETRYABLE_STATUS_CODES:
        return True
    return result.get("StatusCode") is None and result.get("ErrorType") in TRANSIENT_ERROR_TYPES


@app.function_name("eventhub_trigger")
@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name=eventhub_name,
    consumer_group='$Default',
    connection=eventhub_connection,
    cardinality='many',
    data_type='binary'
)
async def eventhub_trigger_active(azeventhub: List[func.EventHubEvent]):
    # Metadata is common to the whole batch under cardinality=many, so it can be read from any single event
    metadata = azeventhub[0].metadata or {} if azeventhub else {}
    partition_id = metadata.get("PartitionContext", {}).get("PartitionId")
    sequence_numbers = metadata.get("SequenceNumberArray") or []
    min_sequence = min(sequence_numbers) if sequence_numbers else None
    max_sequence = max(sequence_numbers) if sequence_numbers else None
    logger.info(
        f"Processing a batch of {len(azeventhub)} events - "
        f"Partition: {partition_id}, Sequence numbers: {min_sequence}-{max_sequence}"
    )

    try:
        async with AsyncExitStack() as stack:
            # Retrieve credentials
            credential = await stack.enter_async_context(DefaultAzureCredential())
            logger.info('Connected to Azure Credentials')

            kv_client = await stack.enter_async_context(SecretClient(vault_url=kv_url, credential=credential))
            logger.info(f'Connected to KeyVault: {kv_url}')

            results_eh_key = await kv_client.get_secret(f"{results_eh_name}-key")
            result_eh_secret_key = results_eh_key.value
            logger.info('Acquired KV secret for Results Event Hub')

            # Initialise the idempotent client outside of the loop / context manager
            idempotency_blob_service = await stack.enter_async_context(
                BlobServiceClient(account_url=idempotency_account_url, credential=credential)
            )
            idempotency_container = idempotency_blob_service.get_container_client(idempotency_container_name)

            res_eh_producer = await stack.enter_async_context(
                EventHubProducerClient.from_connection_string(conn_str=result_eh_secret_key)
            )
            logger.info(
                f"Creating batch for {len(azeventhub)} events - "
                f"Partition: {partition_id}, Sequence numbers: {min_sequence}-{max_sequence}"
            )
            event_data_batch = await res_eh_producer.create_batch()
            try:
                for event in azeventhub:
                    try:
                        logger.info(f'Event received with partition key: {event.partition_key}')

                        # Parse the payload
                        start_datetime = datetime.now(timezone.utc).isoformat()
                        caseNo = event.partition_key
                        payload_str = event.get_body().decode('utf-8')
                        payload = json.loads(payload_str)
                        run_id = payload.get("RunID", None)
                        state = payload.get("State", None)
                        data = payload.get("Content", None)

                        # Build idempotency blob reference
                        idempotency_blob_path = f"active/{state}/idempotency/{caseNo}.flag"
                        idempotency_blob = idempotency_container.get_blob_client(idempotency_blob_path)

                        try:
                            await idempotency_blob.upload_blob(b"", overwrite=False)
                            logger.info(f"[IDEMPOTENCY] Marked for processing: {caseNo}")
                        except ResourceExistsError:
                            logger.warning(f"[IDEMPOTENCY] Skipping in progress case {caseNo}.")
                            continue

                        # Process the file
                        async def _process():
                            return await asyncio.to_thread(process_case, ENV, caseNo, data, run_id, state, PR_REFERENCE)

                        result = await AsyncRetrying(
                            retry=retry_if_result(_is_retryable),
                            stop=stop_after_attempt(3),
                            wait=wait_exponential(multiplier=30, min=30, max=60),
                            before_sleep=_log_retry,
                            retry_error_callback=lambda retry_state: retry_state.outcome.result(),
                        )(_process)
                        result["StartDateTime"] = start_datetime

                        # Mark processed if success
                        if result.get("Status") != "SUCCESS":
                            try:
                                await idempotency_blob.delete_blob()
                                logger.info(f"[IDEMPOTENCY] Removing idempotency blob for failed processing of: {caseNo}")
                            except Exception as delete_error:
                                logger.warning(f"[IDEMPOTENCY] Failed to delete blob for {caseNo}: {delete_error}")

                        result.pop("StatusCode", None)
                        result.pop("ErrorType", None)
                        result_json = json.dumps(result)
                        try:
                            event_data_batch.add(EventData(result_json))
                        except ValueError:
                            # If the batch is full, send it and create a new one
                            await res_eh_producer.send_batch(event_data_batch)
                            logger.info('Sent a batch of events to Results Event Hub')
                            event_data_batch = None  # Force cleardown on successful send to prevent sending duplicate events
                            event_data_batch = await res_eh_producer.create_batch()
                            event_data_batch.add(EventData(result_json))

                    except Exception as e:
                        logger.error(f'Error processing event for caseNo {caseNo}: {e}')

                # Send any remaining events in the batch
                if event_data_batch and len(event_data_batch) > 0:
                    await res_eh_producer.send_batch(event_data_batch)
                    logger.info('Sent the final batch of events to Results Event Hub')

            except Exception as e:
                logger.error(f'Error in event hub processing batch: {e}')
                raise e
    except Exception as e:
        logger.error(f"An error has occurred before processing the batch. {e}")
        raise e
