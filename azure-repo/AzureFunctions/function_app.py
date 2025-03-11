import azure.functions as func
import logging
import json
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from azure.eventhub import EventHubProducerClient,EventData
import os
from typing import List
import time
import asyncio
from tenacity import retry,stop_after_attempt,wait_exponential


app = func.FunctionApp()

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="evh-joh-pub-dev-uks-dlrm-01",consumer_group='preview_data_consumer_group',
                               connection="sboxdlrmeventhubns_RootManageSharedAccessKey_EVENTHUB",
                               starting_position = "-1",cardinality='many',max_batch_size=500,data_type='binary') 
async def eventhub_trigger(azeventhub: List[func.EventHubEvent]):

    logging.info(f"Processing a batch of {len(azeventhub)} events")

    # connect to the blob storage
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['AzureWebJobsStorage'])
    container_name = "event-hub-messages"
    producer_client = EventHubProducerClient.from_connection_string(os.environ['DeadLetterEventHubConnectionString'])

    # ensure the container exists
    try:
        await blob_service_client.create_container(container_name)
        logging.info("Created a container: %s",container_name)
    except ResourceExistsError:
        pass

    tasks = []

    for event in azeventhub:
        task = process_messages(event,blob_service_client,container_name,producer_client)

        tasks.append(task)

    await asyncio.gather(*tasks)


async def process_messages(event,blob_service_client,container_name,producer_client):        
        # set the key and message to none at the start of each event
        key = None
        message = None
        retries = 5
        retry_delay = 5

        status = []
        file_name = []
        for attempt in range(retries):
            try:
                message = event.get_body().decode('utf-8')
                key = event.partition_key
                
                logging.info(f"Processing message for {key} file")
                if not key:
                    raise ValueError("Key not found in the message")
                
                #log the file name to collect it status
                file_name.append(key)
                
                #upload message to blob with partition key as file name

                blob_client = blob_service_client.get_blob_client(container=container_name,blob=key)

                if not blob_client.exists():
                    blob_client.upload_blob(message,overwrite=True)
                    logging.info("Uploaded blob:%s",key)
                else:
                    logging.info("Blob already exists: %s", key)

                break # exit the loop if retry loop is successful

            except Exception as e:
                logging.error(f"Failed to process event with key '{key}': {e}")
                if attempt +1 == retries:
                    logging.error(f"Failed to send message {key} to client storage on attempt {attempt+1}/{retries}: {e}")
                    if message is not None and key is not None:
                        send_to_deadletter(producer_client,message,key)
                    else:
                        logging.error("Cannot send to dead-letter queue because message or key is None.")
                    if attempt +1 < retries:
                        await asyncio.sleep(retry_delay)



    
async def send_to_deadletter(Producer_client:EventHubProducerClient,message: str, partition_key: str):

    try:
        event_data_batch = await Producer_client.create_batch(partition_key=partition_key)
        event_data_batch.add(EventData(message))
        await Producer_client.send_batch(event_data_batch)
        logging.info(f"Message added to dead letter EvenHub with partition key: {partition_key}")
    except Exception as e:
        logging.error(f"failed to upload {partition_key} to dead letter EventHhub: {e}")


    