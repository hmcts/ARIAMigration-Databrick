import azure.functions as func
import logging
import json
from azure.keyvault.secrets._models import KeyVaultSecret
from azure.storage.blob.aio import BlobServiceClient, ContainerClient
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from typing import List
import asyncio
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient
import datetime
import os
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from ccdFunctions import process_case



ENV: str = os.environ["ENVIRONMENT"]
LZ_KEY = os.environ["LZ_KEY"]


ARIA_NAME = "active"

eventhub_name = f"af-{ARIA_NAME}-ccd-{ENV}{LZ_KEY}-uks-dlrm-01"

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
    max_batch_size=2000,
    data_type='binary'
)
async def eventhub_trigger_active(azeventhub: List[func.EventHubEvent]):
    logging.info(f"Processing a batch of {len(azeventhub)} events")

    # Retrieve credentials
    credential = DefaultAzureCredential()
    logging.info('Connected to Azure Credentials')

    kv_url = f"https://ingest{LZ_KEY}-meta002-{ENV}.vault.azure.net"
    kv_client = SecretClient(vault_url=kv_url, credential=credential)
    logging.info(f'Connected to KeyVault: {kv_url}')

    results_eh_name = f"evh-active-res-{ENV}-{LZ_KEY}-uks-dlrm-01"

    results_eh_key = ( await kv_client.get_secret(f"{results_eh_name}_key").value )
    logging.info('Acquired KV secret for Results Event Hub')


    res_eh_producer = EventHubProducerClient.from_connection_string(
        conn_str=results_eh_key)
    
    async with res_eh_producer:
        event_data_batch = await res_eh_producer.create_batch()

        for event in azeventhub:
            try:
                logging.info(f'Event received with partition key: {event.partition_key}')

                caseNo = event.partition_key
                payload_str = event.get_body().decode('utf-8')
                payload = json.loads(payload_str)
                run_id = payload.get("RunID", None)
                state = payload.get("State", None)
                data = payload.get("Content", None)


                result = process_case(env=ENV,caseNo=caseNo, payloadData=data,state=state,runId=run_id)

                logging.info(f'Processing result for caseNo {caseNo}')

                result_json = json.dumps(result)
                try:
                    event_data_batch.add(EventData(result_json))
                except ValueError:
                    # If the batch is full, send it and create a new one
                    await res_eh_producer.send_batch(event_data_batch)
                    logging.info(f'Sent a batch of events to Results Event Hub')
                    event_data_batch = await res_eh_producer.create_batch()
                    event_data_batch.add(EventData(result_json))
            except Exception as e:
                logging.error(f'Error processing event for caseNo {caseNo}: {e}')
        
        # Send any remaining events in the batch
        if len(event_data_batch) > 0:
            await res_eh_producer.send_batch(event_data_batch)
            logging.info(f'Sent the final batch of events to Results Event Hub')




