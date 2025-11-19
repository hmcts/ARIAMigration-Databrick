import azure.functions as func
import logging
import json
import datetime
import os
import asyncio
from azure.keyvault.secrets._models import KeyVaultSecret
from azure.storage.blob.aio import BlobServiceClient, ContainerClient, BlobClient
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from typing import List
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

env: str = os.environ["ENVIRONMENT"]
lz_key = os.environ["LZ_KEY"]

ARIA_SEGMENT = "bl"
ARM_SEGMENT = "BDEV" if env == "sbox" else "B"

eventhub_name = f"evh-{ARIA_SEGMENT}-pub-{lz_key}-uks-dlrm-01"
eventhub_connection = "sboxdlrmeventhubns_RootManageSharedAccessKey_EVENTHUB"

app = func.FunctionApp()


@app.function_name("eventhub_trigger")
@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name=eventhub_name,
    consumer_group='$Default',
    connection=eventhub_connection,
    starting_position="-1",
    cardinality='many',
    max_batch_size=500,
    data_type='binary'
)
async def eventhub_trigger_bails(azeventhub: List[func.EventHubEvent]):
    logging.info(f"Processing a batch of {len(azeventhub)} events")

    # Retrieve credentials
    credential = DefaultAzureCredential()
    logging.info('Connected to Azure Credentials')
    kv_client = SecretClient(
        vault_url=f"https://ingest{lz_key}-meta002-{env}.vault.azure.net", credential=credential
    )
    logging.info('Connected to KeyVault')

    try:
        # Retrieve Event Hub secrets
        ev_dl_key = (await kv_client.get_secret(f"evh-{ARIA_SEGMENT}-dl-{lz_key}-uks-dlrm-01-key")).value
        ev_ack_key = (await kv_client.get_secret(f"evh-{ARIA_SEGMENT}-ack-{lz_key}-uks-dlrm-01-key")).value
        logging.info('Acquired KV secrets for DL and ACK')

        # Blob Storage credentials
        account_url = f"https://ingest{lz_key}curated{env}.blob.core.windows.net"
        # account_url = "https://a360c2x2555dz.blob.core.windows.net"
        container_name = "dropzone"
        # container_secret = (await kv_client.get_secret(f"ARIA{ARM_SEGMENT}-SAS-TOKEN")).value
        container_secret = (await kv_client.get_secret(f"CURATED-AZUREFUNCTION-{env}-SAS-TOKEN")).value
        source_container_secret = (await kv_client.get_secret(f"CURATED-AZUREFUNCTION-{env}-SAS-TOKEN")).value #AM 030625: added to test sas token value vs. cnxn string manipulation
        logging.info('Assigned container secret value')

        container_url = f"{account_url}/{container_name}?{container_secret}"
        logging.info(f'Created container URL: {container_url}')

        sub_dir = f"ARIA{ARM_SEGMENT}/submission"
        logging.info(f'Created sub_dir: {sub_dir}')

        try:
            container_service_client = ContainerClient.from_container_url(container_url)
            logging.info('Created container service client')
        except Exception as e:
            logging.error(f"Failed to connect to ARM Container Client {e}")

        try:
            async with EventHubProducerClient.from_connection_string(ev_dl_key) as dl_producer_client, \
                       EventHubProducerClient.from_connection_string(ev_ack_key) as ack_producer_client:

                logging.info('Processing messages')
                tasks = [
                    process_messages(
                        event,
                        container_service_client,
                        sub_dir,
                        dl_producer_client,
                        ack_producer_client,
                        source_container_secret
                    )
                    for event in azeventhub
                ]
                await asyncio.gather(*tasks)
                logging.info('Finished processing messages')
        finally:
            await container_service_client.close()

    finally:
        # Explicitly close SecretClient to avoid session leaks
        await kv_client.close()
        await credential.close()


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(Exception),
    reraise=True,
    before_sleep=lambda r: logging.warning(
        f"Retrying upload attempt {r.attempt_number} due to: {r.outcome.exception()}"
    ),
)
async def upload_blob_with_retry(blob_client, message, capture_response):
    logging.info(f'Uploading blob')
    await blob_client.upload_blob(message, overwrite=True, raw_response_hook=capture_response)


async def process_messages(event, container_service_client, subdirectory, dl_producer_client, ack_producer_client, source_container_secret):
    ## set up results logging
    results: dict[str, any] = {
        "filename": None,
        "http_response": None,
        "timestamp": None,
        "http_message": None
    }

    ## call back function to capture responses
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
            raise ValueError("Key not found in the message")

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

        if not blob_url:
            raise ValueError("Missing blob_url in the event message")

        # -----------------------------------------------
        #  IDEMPOTENCY CHECK  (ADDED)
        # -----------------------------------------------
        idempotency_base = f"ARIA{ARM_SEGMENT}/idempotency/processed"

        idempotency_blob = container_service_client.get_blob_client(
            blob=f"{idempotency_base}/{file_name}.flag"
        )

        if await idempotency_blob.exists():
            logging.warning(f"[IDEMPOTENCY] Skipping duplicate message for file: {file_name}")
            results["filename"] = file_name
            results["http_message"] = "Duplicate skipped by idempotency"
            await send_to_eventhub(ack_producer_client, json.dumps(results), key)
            return results

        # Create flag immediately (first writer wins)
        try:
            await idempotency_blob.upload_blob(b"processed", overwrite=False)
            logging.info(f"[IDEMPOTENCY] Flag created for file: {file_name}")
        except Exception as ex:
            logging.warning(f"[IDEMPOTENCY] Another instance already created the flag, skipping: {file_name}")
            results["filename"] = file_name
            results["http_message"] = "Duplicate skipped due to race-condition flag"
            await send_to_eventhub(ack_producer_client, json.dumps(results), key)
            return results
        # -----------------------------------------------


        # ✅ Print blob_url for debugging
        logging.info(f"blob_url received: {blob_url}")

        # Append SAS token if missing
        if "?" in blob_url:
            source_blob_url_with_sas = blob_url
        else:
            source_blob_url_with_sas = f"{blob_url}?{source_container_secret}"

        logging.info(f"Final download URL (with SAS if added): {source_blob_url_with_sas}")

        # Download file from source
        source_blob_client = BlobClient.from_blob_url(source_blob_url_with_sas)
        stream = await source_blob_client.download_blob()
        file_content = await stream.readall()
        await source_blob_client.close()

        # Upload to target
        full_blob_name = f"{subdirectory}/{file_name}"
        results["filename"] = file_name
        blob_client = container_service_client.get_blob_client(blob=full_blob_name)
        logging.info(f'Uploading to target blob: {full_blob_name}')

        await upload_blob_with_retry(blob_client, file_content, capture_response)

        results["timestamp"] = datetime.datetime.utcnow().isoformat()
        logging.info("Uploaded blob successfully: %s", key)

    except Exception as e:
        logging.error(f"Failed to process event with key '{key}': {e}")
        results["http_message"] = str(e)

        # Send failed message to dead-letter event hub
        if message is not None and key is not None:
            await send_to_eventhub(dl_producer_client, message, key)
        else:
            logging.error("Cannot send to dead-letter queue because message or key is None.")

    # Always send acknowledgment
    await send_to_eventhub(ack_producer_client, json.dumps(results), key)
    logging.info(f"{key}: Ack stored successfully")
    return results


async def send_to_eventhub(producer_client: EventHubProducerClient, message: str, partition_key: str):
    """Sends messages to an Event Hub."""
    try:
        event_data_batch = await producer_client.create_batch(partition_key=partition_key)
        event_data_batch.add(EventData(message))
        await producer_client.send_batch(event_data_batch)
        logging.info(f"Message added to Event Hub with partition key: {partition_key}")
    except Exception as e:
        logging.error(f"Failed to upload {partition_key} to EventHub: {e}")
