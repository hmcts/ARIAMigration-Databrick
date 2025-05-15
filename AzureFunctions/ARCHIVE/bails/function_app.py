import azure.functions as func
import logging
import json
from azure.keyvault.secrets._models import KeyVaultSecret
from azure.storage.blob.aio import BlobServiceClient,ContainerClient
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from typing import List
import asyncio
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import datetime
import os



app = func.FunctionApp()

@app.function_name("eventhub_trigger")
@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name="evh-bl-pub-dev-uks-dlrm-01",
    consumer_group='$Default',
    connection="sboxdlrmeventhubns_RootManageSharedAccessKey_EVENTHUB",
    starting_position="-1",
    cardinality='many',
    max_batch_size=500,
    data_type='binary'
)
async def eventhub_trigger_bails(azeventhub: List[func.EventHubEvent]):
    logging.info(f"Processing a batch of {len(azeventhub)} events")

    # Retrieve credentials
    credential = DefaultAzureCredential()
    kv_client = SecretClient(vault_url="https://ingest00-meta002-sbox.vault.azure.net", credential=credential)

    try:
        # Retrieve Event Hub secrets
        ev_dl_key = kv_client.get_secret("evh-bl-dl-dev-uks-dlrm-01-key").value
        ev_ack_key = kv_client.get_secret("evh-bl-ack-dev-uks-dlrm-01-key").value

        # Blob Storage credentials

        # account_url = "https://ingest00curatedsbox.blob.core.windows.net"
        account_url = "https://a360c2x2555dz.blob.core.windows.net"
        container_name = "dropzone"

        container_secret = kv_client.get_secret("ARIAB-SAS-TOKEN").value
        # container_secret = kv_client.get_secret("CURATED-SAS-TOKEN").value
        container_url = f"{account_url}/{container_name}?{container_secret}"

        sub_dir = "ARIAB/submission"

        # Use async with to properly close clients
        async with ContainerClient.from_container_url(container_url) as container_service_client, \
                   EventHubProducerClient.from_connection_string(ev_dl_key) as dl_producer_client, \
                   EventHubProducerClient.from_connection_string(ev_ack_key) as ack_producer_client:

            tasks = [
                process_messages(event, container_service_client, sub_dir, dl_producer_client, ack_producer_client)
                for event in azeventhub
            ]
            await asyncio.gather(*tasks)
    
    finally:
        # Explicitly close SecretClient to avoid session leaks
        kv_client.close()


async def process_messages(event,container_service_client,subdirectory,dl_producer_client,ack_producer_client):
        ## set up results logging
        results: dict[str, any] = {
            "filename" : None,
            "http_response" :None,
            "timestamp": None,
            "http_message" : None
        }
 
        ## call back function to capture responses
        def capture_response(response):
            http_response = response.http_response
            results["http_response"] = http_response.status_code
            results["http_message"] = getattr(http_response,"reason", "No reason provided")
 
 
 
 
        # set the key and message to none at the start of each event
        key = None
        message = None
        retries = 5
        retry_delay = 5
 
 
        for attempt in range(retries):
            try:
                message = event.get_body().decode('utf-8')
                key = event.partition_key
                
                logging.info(f"Processing message for {key} file")
                if not key:
                    raise ValueError("Key not found in the message")
                
                full_blob_name = f"{subdirectory}/{key}"
                results["filename"] = key
 
                
                #upload message to blob with partition key as file name
 
                blob_client = container_service_client.get_blob_client(blob=full_blob_name)
 
 
                await blob_client.upload_blob(message,overwrite=True,raw_response_hook=capture_response)
                results["timestamp"] = datetime.datetime.utcnow().isoformat()
                logging.info("Uploaded blob:%s",key)
 
 
                break # exit the loop if retry loop is successful
 
            except Exception as e:
                logging.error(f"Failed to process event with key '{key}': {e}")
                if attempt +1 == retries:
                    logging.error(f"Failed to send message {key} to client storage on attempt {attempt+1}/{retries}: {e}")
                    # results["http_response"] = "Failed to Send"
                    results["http_message"] = str(e)
                    if message is not None and key is not None:
                         await send_to_eventhub(dl_producer_client,message,key)
                    else:
                        logging.error("Cannot send to dead-letter queue because message or key is None.")
                    if attempt +1 < retries:
                        await asyncio.sleep(retry_delay)
        
        await send_to_eventhub(ack_producer_client,json.dumps(results),key)
        logging.info(f"{key}: Ack stored succesfully")
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



#### OLD CODE ###
### Credentials Imports ###

# credential = DefaultAzureCredential()
# kv_client = SecretClient(vault_url="https://ingest00-meta002-sbox.vault.azure.net", credential=credential)



# ### Bails Eventhubs Secrets
# bail_ev_pub_key: KeyVaultSecret = kv_client.get_secret("evh-bl-pub-dev-uks-dlrm-01-key")
# bail_ev_dl_key: KeyVaultSecret = kv_client.get_secret("evh-bl-dl-dev-uks-dlrm-01-key")
# bail_ev_ack_key: KeyVaultSecret = kv_client.get_secret("evh-bl-ack-dev-uks-dlrm-01-key")



# ### ARM Account details
# # account_url: str = "https://a360c2x2555dz.blob.core.windows.net"
# account_url: str = "https://ingest00curatedsbox.blob.core.windows.net"


# container_name: str = "dropzone"
# ## ARM SAS Tokens

# # bails_container_secret: KeyVaultSecret = kv_client.get_secret("ARIAB-SAS-TOKEN")
# bails_container_secret: KeyVaultSecret = kv_client.get_secret("CURATED-SAS-TOKEN")


# async def process_messages(event,container_service_client,subdirectory,dl_producer_client,ack_producer_client): 
#         ## set up results logging
#         results: dict[str, any] = {
#             "filename" : None,
#             "http_response" :None,
#             "timestamp": None,
#             "http_message" : None
#         }

#         ## call back function to capture responses
#         def capture_response(response):
#             http_response = response.http_response
#             results["http_response"] = http_response.status_code
#             results["http_message"] = getattr(http_response,"reason", "No reason provided")




#         # set the key and message to none at the start of each event
#         key = None
#         message = None
#         retries = 5
#         retry_delay = 5


#         for attempt in range(retries):
#             try:
#                 message = event.get_body().decode('utf-8')
#                 key = event.partition_key
                
#                 logging.info(f"Processing message for {key} file")
#                 if not key:
#                     raise ValueError("Key not found in the message")
                
#                 full_blob_name = f"{subdirectory}/{key}"
#                 results["filename"] = key

                
#                 #upload message to blob with partition key as file name

#                 blob_client = container_service_client.get_blob_client(blob=full_blob_name)


#                 await blob_client.upload_blob(message,overwrite=True,raw_response_hook=capture_response)
#                 results["timestamp"] = datetime.datetime.utcnow().isoformat()
#                 logging.info("Uploaded blob:%s",key)


#                 break # exit the loop if retry loop is successful

#             except Exception as e:
#                 logging.error(f"Failed to process event with key '{key}': {e}")
#                 if attempt +1 == retries:
#                     logging.error(f"Failed to send message {key} to client storage on attempt {attempt+1}/{retries}: {e}")
#                     # results["http_response"] = "Failed to Send"
#                     results["http_message"] = str(e)
#                     if message is not None and key is not None:
#                          await send_to_eventhub(dl_producer_client,message,key)
#                     else:
#                         logging.error("Cannot send to dead-letter queue because message or key is None.")
#                     if attempt +1 < retries:
#                         await asyncio.sleep(retry_delay)
        
#         await send_to_eventhub(ack_producer_client,json.dumps(results),key)
#         logging.info(f"{key}: Ack stored succesfully")
#         return results



    
# async def send_to_eventhub(Producer_client:EventHubProducerClient,message: str, partition_key: str):

#     try:
#         event_data_batch = await Producer_client.create_batch(partition_key=partition_key)
#         event_data_batch.add(EventData(message))
#         await Producer_client.send_batch(event_data_batch)
#         logging.info(f"Message added to {Producer_client} with partition key: {partition_key}")
#     except Exception as e:
#         logging.error(f"failed to upload {partition_key} to {Producer_client} EventHhub: {e}")


# ####### Bail Function ######

# @app.event_hub_message_trigger(arg_name="bailsazeventhub", event_hub_name="evh-bl-pub-dev-uks-dlrm-01",consumer_group='$Default',
#                                connection= "sboxdlrmeventhubns_RootManageSharedAccessKey_EVENTHUB",
#                                starting_position = "-1",cardinality='many',max_batch_size=500,data_type='binary') 
# async def eventhub_trigger_bails(bailsazeventhub: List[func.EventHubEvent]):

#     logging.info(f"Processing a batch of {len(bailsazeventhub)} events")

#     subdir: str = "ARIAB/submission"

#     # connect to the blob storage
#     container_service_client = ContainerClient(account_url, container_name, bails_container_secret.value)
#     async with EventHubProducerClient.from_connection_string(bail_ev_dl_key.value) as dl_producer_client, \
#     EventHubProducerClient.from_connection_string(bail_ev_ack_key.value) as ack_producer_client:
        

#         bail_tasks = []

#         bail_tasks = [process_messages(event,container_service_client,subdir,dl_producer_client,ack_producer_client) for event in bailsazeventhub]

#         await asyncio.gather(*bail_tasks)


# @app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="myeventhub",
#                                connection="AzureWebJobsStorage") 
# def eventhub_trigger(azeventhub: func.EventHubEvent):
#     logging.info('Python EventHub trigger processed an event: %s',
#                 azeventhub.get_body().decode('utf-8'))
