import asyncio
import azure.functions as func
import logging
import json
import os

from azure.keyvault.secrets._models import KeyVaultSecret
from azure.storage.blob.aio import BlobServiceClient, ContainerClient
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient
from datetime import datetime, timezone, timedelta
from typing import List
# from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
try:
    # When running as a function app the module will be a package. Use a
    # relative import where possible.
    from .ccdFunctions import process_case, validate_case
    from .tokenManager import IDAMTokenManager, S2S_Manager
except Exception:
    # Fallback for running the script directly during local debugging.
    from ccdFunctions import process_case, validate_case
    from tokenManager import IDAMTokenManager, S2S_Manager

ENV = os.environ["ENVIRONMENT"]
LZ_KEY = os.environ["LZ_KEY"]
PR_NUMBER = os.environ["PR_NUMBER"]
ARIA_NAME = "active"

eventhub_name = f"evh-active-pub-{ENV}-{LZ_KEY}-uks-dlrm-01"
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
    logging.info(f"Processing a batch of {len(azeventhub)} events")

    # Retrieve credentials
    credential = DefaultAzureCredential()
    logging.info('Connected to Azure Credentials')

    kv_url = f"https://ingest{LZ_KEY}-meta002-{ENV}.vault.azure.net"
    kv_client = SecretClient(vault_url=kv_url, credential=credential)
    logging.info(f'Connected to KeyVault: {kv_url}')

    results_eh_name = f"evh-active-res-{ENV}-{LZ_KEY}-uks-dlrm-01"
    results_eh_key = await kv_client.get_secret(f"{results_eh_name}-key")
    result_eh_secret_key = results_eh_key.value
    logging.info('Acquired KV secret for Results Event Hub')

    # Initialise the idempotent client outside of the loop / context manager
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
                    logging.info(f'Event received with partition key: {event.partition_key}')

                    # Parse the payload
                    start_datetime = datetime.now(timezone.utc).isoformat()
                    caseNo = event.partition_key
                    payload_str = event.get_body().decode('utf-8')
                    payload = json.loads(payload_str)
                    run_id = payload.get("RunID", None)
                    state = payload.get("State", None)
                    data = payload.get("Content", None)

                    ## Build idempotency blob reference
                    idempotency_blob_path = f"active/{state}/processed/{caseNo}.flag"
                    idempotency_blob = idempotency_container.get_blob_client(idempotency_blob_path)

                    if await idempotency_blob.exists():
                        logging.warning(f"[IDEMPOTENCY] Skipping duplicate message for {state}/{caseNo}")
                        continue

                    ##Retrieve tokens for validation
                    idam_manager = IDAMTokenManager(ENV, 18000)
                    idam_token, uid = idam_manager.get_token()
                    s2s_manager = S2S_Manager(ENV, 18000)
                    s2s_token = s2s_manager.get_token()

                    validate_response = await asyncio.to_thread(
                    validate_case,
                    ccd_base_url = f"https://ccd-service-{ENV}.platform.hmcts.net",
                    event_token = run_id,
                    payloadData = data,
                    jid = "IA",
                    ctid = "Asylum",
                    idam_token = idam_token,
                    uid = uid,
                    s2s_token = s2s_token
                    )

                    # If validation is a pass or fail return the idempotent value
                    await idempotency_blob.upload_blob(b"", overwrite=True)

                    if validate_response is not None and validate_response.status_code == 200:
                        result = await asyncio.to_thread(process_case, ENV, caseNo, data, run_id, state, PR_NUMBER)
                        result["StartDateTime"] = start_datetime
                    else: 
                        logging.warning(f"Validation failed for {caseNo}, skipping processing")

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
                
        except Exception as e:
            logging.error(f'Error in event hub processing batch: {e}')
        finally:
            # Clean up all clients
            await idempotency_blob_service.close()
            await kv_client.close()
            await credential.close()




# async def eventhub_trigger_active(azeventhub: List[func.EventHubEvent]):
#     logging.info(f"Processing a batch of {len(azeventhub)} events")

#     # Retrieve credentials
#     credential = DefaultAzureCredential()
#     logging.info('Connected to Azure Credentials')

#     kv_url = f"https://ingest{LZ_KEY}-meta002-{ENV}.vault.azure.net"
#     kv_client = SecretClient(vault_url=kv_url, credential=credential)
#     logging.info(f'Connected to KeyVault: {kv_url}')

#     results_eh_name = f"evh-active-res-{ENV}-{LZ_KEY}-uks-dlrm-01"
#     results_eh_key = await kv_client.get_secret(f"{results_eh_name}-key")
#     result_eh_secret_key = results_eh_key.value
#     logging.info('Acquired KV secret for Results Event Hub')

#     # Initialise the idempotent client outside of the loop / context manager
#     idempotency_account_url = f"https://ingest{LZ_KEY}xcutting{ENV}.blob.core.windows.net"
#     idempotency_container_name = "af-idempotency"
#     idempotency_blob_service = BlobServiceClient(account_url=idempotency_account_url, credential=credential)
#     idempotency_container = idempotency_blob_service.get_container_client(idempotency_container_name)
    
#     res_eh_producer = EventHubProducerClient.from_connection_string(conn_str=result_eh_secret_key)
    
#     async with res_eh_producer:
#         event_data_batch = await res_eh_producer.create_batch()
#         try:
#             for event in azeventhub:
#                 try:
#                     logging.info(f'Event received with partition key: {event.partition_key}')

#                     # Parse the payload
#                     start_datetime = datetime.now(timezone.utc).isoformat()
#                     caseNo = event.partition_key
#                     payload_str = event.get_body().decode('utf-8')
#                     payload = json.loads(payload_str)
#                     run_id = payload.get("RunID", None)
#                     state = payload.get("State", None)
#                     data = payload.get("Content", None)

#                     ## Build idempotency blob reference
#                     idempotency_blob_path = f"active/{state}/processed/{caseNo}.flag"
#                     idempotency_blob = idempotency_container.get_blob_client(idempotency_blob_path)

#                     if await idempotency_blob.exists():
#                         logging.warning(f"[IDEMPOTENCY] Skipping duplicate message for {state}/{caseNo}")
#                         continue

#                     ##Retrieve tokens for validation
#                     idam_manager = IDAMTokenManager(ENV)
#                     idam_token, uid = idam_manager.get_token()
#                     s2s_manager = S2S_Manager(ENV)
#                     s2s_token = s2s_manager.get_token()

#                     validate_response = await asyncio.to_thread(
#                     validate_case,
#                     ccd_base_url = f"https://ccd-service-{ENV}.platform.hmcts.net",
#                     event_token = run_id,
#                     payloadData = data,
#                     jid = "IA",
#                     ctid = "Asylum",
#                     idam_token = idam_token,
#                     uid = uid,
#                     s2s_token = s2s_token
#                     )

#                     if validate_response is None or validate_response.status_code != 200:
#                         logging.warning(f"Validation failed for {caseNo}, skipping processing")
#                         continue

#                     # Process the file
#                     result = await asyncio.to_thread(
#                         process_case, ENV, caseNo, data, run_id, state, PR_NUMBER
#                     )
#                     result["StartDateTime"] = start_datetime

#                     # Mark processed if success
#                     if result.get("Status") == "Success":
#                         try:
#                             await idempotency_blob.upload_blob(b"", overwrite=True)
#                             logging.info(f"[IDEMPOTENCY] Marked processed: {caseNo}")
#                         except Exception as upload_error:
#                             logging.error(
#                                 f"[IDEMPOTENCY] Failed to mark processed for {caseNo}: {upload_error}"
#                             )

#                     result_json = json.dumps(result)

#                     try:
#                         event_data_batch.add(EventData(result_json))
#                     except ValueError:
#                         # If the batch is full, send it and create a new one
#                         await res_eh_producer.send_batch(event_data_batch)
#                         logging.info(f'Sent a batch of events to Results Event Hub')
#                         event_data_batch = await res_eh_producer.create_batch()
#                         event_data_batch.add(EventData(result_json))
                        
#                 except Exception as e:
#                     logging.error(f'Error processing event for caseNo {caseNo}: {e}')
            
#             # Send any remaining events in the batch
#             if len(event_data_batch) > 0:
#                 await res_eh_producer.send_batch(event_data_batch)
#                 logging.info(f'Sent the final batch of events to Results Event Hub')
                
#         except Exception as e:
#             logging.error(f'Error in event hub processing batch: {e}')
#         finally:
#             # Clean up all clients
#             await idempotency_blob_service.close()
#             await kv_client.close()
#             await credential.close()