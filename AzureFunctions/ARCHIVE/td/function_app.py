import azure.functions as func
import logging
import json
from azure.storage.blob.aio import BlobServiceClient, ContainerClient, BlobClient
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from typing import List
import asyncio
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
import datetime
import os
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type


# Retrieve environment variables
env: str = os.environ["ENVIRONMENT"]
lz_key = os.environ["LZ_KEY"]

ARIA_SEGMENT = "td"
ARM_SEGMENT = "TDDEV" if env == "sbox" else "TD"
eventhub_name = f"evh-{ARIA_SEGMENT}-pub-{lz_key}-uks-dlrm-01"
eventhub_connection = "sboxdlrmeventhubns_RootManageSharedAccessKey_EVENTHUB"

_credential = DefaultAzureCredential()
_kv_client = SecretClient(
    vault_url=f"https://ingest{lz_key}-meta002-{env}.vault.azure.net",
    credential=_credential
)

idempotency_account_url = f"https://ingest{lz_key}xcutting{env}.blob.core.windows.net"
idempotency_base = f"ARCHIVE/ARIA{ARM_SEGMENT}/processed"

account_url = "https://a360c2x2555dz.blob.core.windows.net"
container_name = "dropzone"
sub_dir = f"ARIA{ARM_SEGMENT}/submission"

app = func.FunctionApp()


@app.function_name("eventhub_trigger")
@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name=eventhub_name,
    consumer_group='$Default',
    connection=eventhub_connection,
    cardinality='many',
    data_type='binary'
)
async def eventhub_trigger_td(azeventhub: List[func.EventHubEvent]):
    producer_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(3)

    metadata = azeventhub[0].metadata or {} if azeventhub else {}
    partition_id = metadata.get("PartitionContext", {}).get("PartitionId")
    sequence_numbers = [event.sequence_number for event in azeventhub] if azeventhub else []
    min_sequence = min(sequence_numbers) if sequence_numbers else None
    max_sequence = max(sequence_numbers) if sequence_numbers else None
    logging.info(
        f"Processing a batch of {len(azeventhub)} events - "
        f"Partition: {partition_id}, Sequence numbers: {min_sequence}-{max_sequence}"
    )

    ev_dl_secret, ev_ack_secret, container_secret, source_container_secret = await asyncio.gather(
        _kv_client.get_secret(f"evh-{ARIA_SEGMENT}-dl-{lz_key}-uks-dlrm-01-key"),
        _kv_client.get_secret(f"evh-{ARIA_SEGMENT}-ack-{lz_key}-uks-dlrm-01-key"),
        _kv_client.get_secret(f"ARIA{ARM_SEGMENT}-SAS-TOKEN"),
        _kv_client.get_secret(f"CURATED-AZUREFUNCTION-{env}-SAS-TOKEN"),
    )
    ev_dl_key = ev_dl_secret.value
    ev_ack_key = ev_ack_secret.value
    container_secret = container_secret.value
    source_container_secret = source_container_secret.value

    container_url = f"{account_url}/{container_name}?{container_secret}"

    try:
        container_service_client = ContainerClient.from_container_url(container_url)
        logging.info(f"Connected to ARM Container Client on url {container_url} with sub directory {sub_dir}")
    except Exception as e:
        logging.error(f"Failed to connect to ARM Container Client {e}")
        raise e

    try:
        async with EventHubProducerClient.from_connection_string(ev_dl_key) as dl_producer_client, \
                   EventHubProducerClient.from_connection_string(ev_ack_key) as ack_producer_client, \
                   BlobServiceClient(idempotency_account_url, _credential) as idempotency_blob_service:

            idempotency_container = idempotency_blob_service.get_container_client("af-idempotency")

            async def bounded_process(event):
                async with semaphore:
                    try:
                        async with asyncio.timeout(360):
                            await process_messages(
                                event,
                                container_service_client,
                                sub_dir,
                                dl_producer_client,
                                ack_producer_client,
                                source_container_secret,
                                idempotency_container,
                                producer_lock,
                                semaphore
                            )
                    except TimeoutError:
                        key = event.partition_key
                        msg = f"Task timed out after 6 minutes for key '{key}'."
                        logging.error(msg)
                        raise

            logging.info(
                f"Processing messages - Partition: {partition_id}, Sequence numbers: {min_sequence}-{max_sequence}"
            )
            await asyncio.gather(*[bounded_process(event) for event in azeventhub])
            logging.info(
                f"Finished processing messages - Partition: {partition_id}, Sequence numbers: {min_sequence}-{max_sequence}"
            )
    except Exception as e:
        logging.error(f"Exception occurred when processing messages {e}")
        raise e
    finally:
        await container_service_client.close()


@retry(
    wait=wait_exponential(multiplier=1, min=5, max=10),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(Exception),
    reraise=True,
    before_sleep=lambda r: logging.warning(
        f"Retrying upload attempt {r.attempt_number} due to: {r.outcome.exception()}"
    ),
)
async def upload_blob_with_retry(blob_client, message, capture_response):
    async with asyncio.timeout(90):
        await blob_client.upload_blob(message, overwrite=True, raw_response_hook=capture_response)


@retry(
    wait=wait_exponential(multiplier=1, min=5, max=10),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(Exception),
    reraise=True,
    before_sleep=lambda r: logging.warning(
        f"Retrying download attempt {r.attempt_number} due to: {r.outcome.exception()}"
    ),
)
async def download_blob_with_retry(blob_url):
    async with BlobClient.from_blob_url(blob_url) as source_blob_client:
        async with asyncio.timeout(90):
            stream = await source_blob_client.download_blob()
            return await stream.readall()


async def process_messages(event, container_service_client, subdirectory, dl_producer_client, ack_producer_client, source_container_secret, idempotency_container, producer_lock, semaphore):
    results: dict = {
        "filename": None,
        "http_response": None,
        "timestamp": None,
        "http_message": None
    }

    def capture_response(response):
        http_response = response.http_response
        results["http_response"] = http_response.status_code
        results["http_message"] = getattr(http_response, "reason", "No reason provided")

    key = None
    message = None

    try:
        # Decode incoming event
        message = event.get_body().decode('utf-8').strip()
        key = event.partition_key
        logging.info(f"Processing message for {key} file")

        if not key:
            logging.error('Key was empty')
            await send_to_dead_letter(dl_producer_client, message, key, producer_lock)
            return

        # Attempt to parse JSON payload
        blob_url = None
        file_name = key
        try:
            payload = json.loads(message)
            blob_url = payload.get("blob_url")
            file_name = payload.get("file_name") or key
            logging.info(f"Parsed JSON message for file {file_name}")
        except json.JSONDecodeError:
            # Handle plain URL message
            blob_url = message
            logging.info(f"Message was plain blob URL for file {file_name}")

        results["filename"] = file_name

        if not blob_url:
            logging.error("Missing blob_url in the event message")
            await send_to_dead_letter(dl_producer_client, message, key, producer_lock)
            return

        idempotency_blob = idempotency_container.get_blob_client(f"{idempotency_base}/{file_name}.flag")

        try:
            try:
                # Create flag immediately (first writer wins)
                try:
                    await idempotency_blob.upload_blob(b"processed", overwrite=False)
                    logging.info(f"[IDEMPOTENCY] Flag created for file: {file_name}")
                except ResourceExistsError:
                    props = await idempotency_blob.get_blob_properties()

                    if (props.metadata or {}).get("is_complete") == "True":
                        logging.warning(f"[IDEMPOTENCY] File already processed, skipping: {file_name}")
                        return

                    # Giving 180s grace for checking if an original run has been lost, for the duplicate message to be reprocessed.
                    remaining = 180 - (datetime.datetime.now(datetime.timezone.utc) - props.last_modified).total_seconds()
                    if remaining > 0:
                        logging.warning(f"[IDEMPOTENCY] File in-progress for {file_name}")
                        semaphore.release()  # don't block waiting for in-progress event
                        try:
                            await asyncio.sleep(remaining)
                        finally:
                            await semaphore.acquire()
                        props = await idempotency_blob.get_blob_properties()
                        if (props.metadata or {}).get("is_complete") == "True":
                            logging.info(f"[IDEMPOTENCY] In-progress file has now completed processing, skipping: {file_name}")
                            return

                    logging.info(f"[IDEMPOTENCY] Flag is not complete and is past expiry for: {file_name}. Refreshing idempotency lock")
                    await idempotency_blob.upload_blob(b"processed", overwrite=True)

            except Exception as e:
                logging.warning(f"[IDEMPOTENCY] Check failed, proceeding anyway: {e}", exc_info=True)

            # Append SAS token if missing
            if "?" in blob_url:
                source_blob_url_with_sas = blob_url
            else:
                source_blob_url_with_sas = f"{blob_url}?{source_container_secret}"

            logging.info(f"Final download URL (with SAS if added): {source_blob_url_with_sas}")

            # Download file from source
            file_content = await download_blob_with_retry(source_blob_url_with_sas)

            # Upload to target
            full_blob_name = f"{subdirectory}/{file_name}"
            blob_client = container_service_client.get_blob_client(blob=full_blob_name)
            logging.info(f'Uploading to target blob: {full_blob_name}')

            await upload_blob_with_retry(blob_client, file_content, capture_response)

            # In case raw_response_hook did not trigger correctly.
            if results["http_response"] is None:
                logging.warning(f"No response captured for {file_name}, verifying blob uploaded.")
                await blob_client.get_blob_properties()
                results["http_response"] = 201
                results["http_message"] = "Created with missed response hook"
                logging.info(f"Blob exists for {file_name}. Responses set.")

            logging.info(f"CaseNo = {results['filename']}, http_response = {results['http_response']}, http_message = {results['http_message']}")

            results["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            logging.info("Uploaded blob successfully: %s", key)

            await send_to_eventhub(ack_producer_client, json.dumps(results), key, producer_lock)
        except BaseException:
            logging.warning(f"[IDEMPOTENCY] Deleting flag for file: {file_name} after processing failure")
            try:
                await idempotency_blob.delete_blob()
            except ResourceNotFoundError:
                pass
            raise

        # Ack sent successfully
        try:
            await idempotency_blob.set_blob_metadata({"is_complete": "True"})
            logging.info(f"[IDEMPOTENCY] Marked complete for file: {file_name}")
        except Exception as e:
            logging.error(f"[IDEMPOTENCY] Ack sent but failed to mark flag complete for {file_name}: {e}.")

    except BaseException as e:
        logging.error(f"Failed to process event with key '{key}': {e}", exc_info=True)
        results["http_message"] = str(e)
        logging.error(f"CaseNo = {results['filename']}, http_response = {results['http_response']}, http_message = {results['http_message']}")

        await send_to_dead_letter(dl_producer_client, message, key, producer_lock)
        # Raise exception, whole batch will be re-tried but successful runs will be skipped by idempotency.
        raise


@retry(
    wait=wait_exponential(min=15, max=30),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(Exception),
    reraise=True,
    before_sleep=lambda r: logging.warning(
        f"Retrying EventHub send attempt {r.attempt_number} due to: {r.outcome.exception()}"
    ),
)
async def send_to_eventhub(producer_client: EventHubProducerClient, message: str, partition_key: str | None, producer_lock: asyncio.Lock):
    logging.info(f"Creating ack batch for {partition_key}.")
    async with producer_lock:
        async with asyncio.timeout(60):
            event_data_batch = await producer_client.create_batch(partition_key=partition_key)
            event_data_batch.add(EventData(message))
            logging.info(f"Sending ack for {partition_key}.")
            await producer_client.send_batch(event_data_batch, timeout=60)
    logging.info(f"Message added to Event Hub with partition key: {partition_key}")


async def send_to_dead_letter(dl_producer_client, message, key, producer_lock):
    if message is not None:
        try:
            await send_to_eventhub(dl_producer_client, message, key, producer_lock)
            logging.info(f"{key}: Sent to dead letter queue")
        except Exception as e:
            logging.error(f"Failed to send {key} to dead-letter EventHub: {e}")
    else:
        logging.error("Cannot send to dead-letter queue because message is None.")
