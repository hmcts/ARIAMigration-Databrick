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

kv_url = f"https://ingest{LZ_KEY}-meta002-{ENV}.vault.azure.net"
results_eh_name = f"evh-active-caselink-res-{ENV}-{LZ_KEY}-uks-dlrm-01"
idempotency_account_url = f"https://ingest{LZ_KEY}xcutting{ENV}.blob.core.windows.net"
idempotency_container_name = "af-idempotency"

app = func.FunctionApp()


def _log_retry(retry_state):
    if retry_state.outcome.failed:
        error = retry_state.outcome.exception()
    else:
        result = retry_state.outcome.result()
        error = result.get("Error", "") if isinstance(result, dict) else result
    logger.warning(
        f"Attempt {retry_state.attempt_number} failed due to: {error}."
        f"(sleeping {retry_state.next_action.sleep}s)"
    )


async def _process_case_link(env, ccd_reference, run_id, data, pr_reference, overwrite):
    return await asyncio.to_thread(process_event, env, ccd_reference, run_id, data, pr_reference, overwrite)


async def _publish_result(producer, payload_json):
    batch = await producer.create_batch()
    batch.add(EventData(payload_json))
    await producer.send_batch(batch)


def _is_retryable(result):
    RETRYABLE_STATUS_CODES = {408, 409, 429, 500, 502, 503, 504}
    TRANSIENT_ERROR_TYPES = {
        "ConnectionError", "ConnectTimeout", "ReadTimeout", "Timeout",
        "ChunkedEncodingError", "SSLError", "ProxyError",
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
            logger.info("Connected to Azure Credentials")

            kv_client = await stack.enter_async_context(SecretClient(vault_url=kv_url, credential=credential))
            logger.info(f"Connected to KeyVault: {kv_url}")

            results_eh_key = await kv_client.get_secret(f"{results_eh_name}-key")
            result_eh_secret_key = results_eh_key.value
            logger.info("Acquired KV secret for Results Event Hub")

            idempotency_blob_service = await stack.enter_async_context(
                BlobServiceClient(account_url=idempotency_account_url, credential=credential)
            )
            idempotency_container = idempotency_blob_service.get_container_client(idempotency_container_name)

            res_eh_producer = await stack.enter_async_context(
                EventHubProducerClient.from_connection_string(conn_str=result_eh_secret_key)
            )
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

                        start_datetime = datetime.now(timezone.utc).isoformat()

                        try:
                            result = await AsyncRetrying(
                                retry=retry_if_result(_is_retryable),
                                stop=stop_after_attempt(3),
                                wait=wait_exponential(multiplier=30, min=30, max=60),
                                before_sleep=_log_retry,
                                retry_error_callback=lambda retry_state: retry_state.outcome.result(),
                            )(_process_case_link, ENV, ccdReference, run_id, data, PR_REFERENCE, overwrite)
                        except Exception as processing_error:
                            logger.error(f"Unhandled exception while processing case {ccdReference}: {processing_error}")
                            result = {
                                "RunID": run_id,
                                "CCDCaseReferenceNumber": ccdReference,
                                "CaseLinkCount": 0,
                                "StartDateTime": start_datetime,
                                "Status": "ERROR",
                                "StatusCode": getattr(processing_error, "status_code", None),
                                "ErrorType": type(processing_error).__name__,
                                "Error": f"Unhandled exception while processing case: {processing_error}",
                                "EndDateTime": datetime.now(timezone.utc).isoformat(),
                            }

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
                        result.pop("ErrorType", None)
                        result_json = json.dumps(result)

                        # Send immediately so a computed result is never lost
                        # sitting in an in-memory batch that fails to flush.
                        await AsyncRetrying(
                            stop=stop_after_attempt(3),
                            wait=wait_exponential(multiplier=10, min=10, max=20),
                            before_sleep=_log_retry,
                            reraise=True,
                        )(_publish_result, res_eh_producer, result_json)
                        logger.info(f"Sent result for case {ccdReference} to Results Event Hub")

                    except Exception as e:
                        logger.error(f"Error processing event for caseNo {ccdReference}: {e}")

            except Exception as e:
                logger.error(f"Error in event hub processing batch: {e}")
                raise e
    except Exception as e:
        logger.error(f"An error has occurred before processing the batch. {e}")
        raise e
