# functions.py

from datetime import datetime
import functools
import time
import logging
from confluent_kafka import Producer

DEFAULT_STATES = [
    "paymentPending",
    "appealSubmitted",
    "awaitingRespondentEvidence(a)",
    "awaitingRespondentEvidence(b)",
    "caseUnderReview",
    "reasonForAppealSubmitted",
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

# ---------------------------
# Retry Decorator
# ---------------------------

def validate_state(state, states=DEFAULT_STATES):
    if state not in states:
        raise ValueError(f"Invalid state: {state}")
    return True

def retry_on_exception(max_retries=3, delay=1, backoff=2, exceptions=(Exception,)):
    """
    Decorator to retry a function on exceptions with exponential backoff.
    
    Parameters:
        max_retries (int): Maximum number of retry attempts
        delay (float): Initial delay between retries (seconds)
        backoff (float): Multiplier for exponential backoff
        exceptions (tuple): Exceptions that trigger retry
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay
            while attempts <= max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempts += 1
                    if attempts > max_retries:
                        logging.error(f"Max retries reached for {func.__name__}. Last error: {e}")
                        raise
                    logging.warning(f"Retry {attempts}/{max_retries} for {func.__name__} due to: {e}. Retrying in {current_delay}s...")
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator

# ---------------------------
# Kafka Message Producer Wrapper
# ---------------------------
@retry_on_exception(max_retries=3, delay=1, backoff=2, exceptions=(BufferError, Exception))
def produce_message(producer, topic, key, value, callback):
    producer.produce(topic=topic, key=key, value=value, callback=callback)

# ---------------------------
# process_partition
# ---------------------------
def process_partition(partition, producer, topic, logger):
    failure_list = []
    success_list = []
    results = []

    for row in partition:
        if row.file_path is None or row.content_str is None:
            logger.warning(f"Skipping row with missing file_path/content_str: {row}")
            failure_list.append((row.file_path, row.state, "failure", "Missing content", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
            continue

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
            # Convert content to bytes
            if isinstance(row.content_str, str):
                value = row.content_str.encode('utf-8')
            elif isinstance(row.content_str, (bytes, bytearray)):
                value = bytes(row.content_str)
            else:
                logger.error(f"Unsupported type for content_str: {type(row.content_str)}")
                failure_list.append((current_file_path, current_state_row, "failure", "Unsupported content type", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
                continue

            # Produce message with retry
            produce_message(producer, topic, key=current_file_path.encode('utf-8'), value=value, callback=delivery_report)

        except Exception as e:
            logger.error(f"Unexpected error during production: {e}")
            failure_list.append((current_file_path, current_state_row, "failure", str(e), datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))

    # Flush producer and handle any remaining messages
    try:
        producer.flush()
        logger.info("Producer flushed successfully.")
    except Exception as e:
        logger.error(f"Unexpected error during flush: {e}")

    # Combine results
    results.extend(success_list)
    results.extend(failure_list)

    return results


# -----------------------
# Storage Client Read
# -----------------------
def read_from_storage(storage_client, container, blob_name):
    """
    Reads a blob from a given storage client.
    """
    blob = storage_client.get_blob_client(container=container, blob=blob_name)
    return blob.download_blob().readall()

if __name__ == "__main__":
    pass