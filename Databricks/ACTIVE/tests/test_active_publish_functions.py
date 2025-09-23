def process_partition(partition, broadcast_conf=None, env = 'sbox', lz_key = '00'):
    import logging
    from confluent_kafka import Producer
    from datetime import datetime
    from types import SimpleNamespace

    # Initialize logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('KafkaProducer')
    
    failure_list = []
    success_list = []
    results = []

    # Initialize producer
    if broadcast_conf is None:
        broadcast_conf = SimpleNamespace(value={"bootstrap.servers": "localhost:9092"})

    producer = Producer(**broadcast_conf.value)

    for row in partition:
        if row.file_path is None or row.content_str is None:
            logger.warning(f"Skipping row with missing file_path/content_str: {row}")
            continue

        ## Use current row for callback
        current_state_row = row.state
        current_file_path = row.file_path

        def delivery_report(err, msg):
            key_str = msg.key().decode('utf-8') if msg.key() is not None else "Unknown"
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            
            if err is not None:
                err_msg = str(err)
                logger.error(f"Message delivery failed for key {key_str}: {err}")
                failure_list.append((key_str, current_state_row, "failure", err_msg, timestamp))
            else:
                success_list.append((key_str, current_state_row, "success", "", timestamp))

        try:
            # Handle different content_str types
            if isinstance(row.content_str, str):
                value = row.content_str.encode('utf-8')
            elif isinstance(row.content_str, bytearray):
                value = bytes(row.content_str)
            elif isinstance(row.content_str, bytes):
                value = row.content_str
            else:
                logger.error(f"Unsupported type for content_str: {type(row.content_str)}")
                failure_list.append((current_file_path, current_state_row, "failure", "Unsupported content type", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
                continue

            # Produce message to Kafka
            producer.produce(
                topic=f'evh-active-pub-{env}-{lz_key}-uks-dlrm-01',
                key=current_file_path.encode('utf-8'),
                value=value,
                callback=delivery_report
            )

        except BufferError:
            logger.error("Producer buffer full. Polling for events.")
            producer.poll(1)
            # Retry the message production
            try:
                producer.produce(
                    topic=f'evh-active-pub-{env}-{lz_key}-uks-dlrm-01',
                    key=current_file_path.encode('utf-8'),
                    value=value,
                    callback=delivery_report
                )
            except Exception as retry_e:
                logger.error(f"Failed to produce message after buffer retry: {retry_e}")
                failure_list.append((current_file_path, current_state_row, "failure", f"Buffer error retry failed: {str(retry_e)}", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
                
        except Exception as e:
            logger.error(f"Unexpected error during production: {e}")
            failure_list.append((current_file_path, current_state_row, "failure", str(e), datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))

    # Flush producer and handle any remaining messages
    try:
        producer.flush()
        logger.info("Producer flushed successfully.")
    except Exception as e:
        logger.error(f"Unexpected error during flush: {e}")

    # Combine all results
    results.extend(success_list)
    results.extend(failure_list)

    return results

# # ---------------------------
# # Read From Storage
# # ---------------------------

def read_from_storage(storage_client, container, blob_name):
    blob = storage_client.get_blob_client(container=container, blob=blob_name)
    return blob.download_blob().readall()

# # ---------------------------
# # Validate States
# # ---------------------------

DEFAULT_STATES = [
    "paymentPending",
    "appealSubmitted",
    "awaitingRespondentEvidence(a)",
    "awaitingRespondentEvidence(b)",
    "caseUnderReview",
    "reasonsForAppealSubmitted",
    "listing",
    "PrepareForHearing",
    "Decision",
    "FTPA Submitted (a)",
    "FTPA Submitted (b)",
    "Decided (b)",
    "Decided (a)",
    "FTPA Decided",
    "Ended",
    "Remitted"
]

def validate_state(state, states=DEFAULT_STATES):
    if state not in states:
        raise ValueError(f"Invalid state: {state}")
    return True

