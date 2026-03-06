import asyncio
import azure.functions as func
import logging
import json
import os

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
PR_NUMBER = os.environ.get("PR_NUMBER") if ENV == "sbox" else None
ARIA_NAME = "active"

eventhub_name = f"evh-active-caselink-pub-{ENV}-{LZ_KEY}-uks-dlrm-01"
eventhub_connection = "sboxdlrmeventhubns_RootManageSharedAccessKey_EVENTHUB"

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
                    start_datetime = payload.get("StartDateTime")

                    # Process the file
                    result = await asyncio.to_thread(process_event, ENV, ccdReference, run_id, data, PR_NUMBER)
                    result["StartDateTime"] = start_datetime

                    # Mark processed if success
                    if result.get("Status") == "Success":
                        logger.info(f"Case linking processed from: {ccdReference} to: {", ".join(str(obj["id"]) for obj in data if "id" in obj)}")

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
