import unittest
import logging
from Test_Active_Publish_Functions import validate_state, process_partition, read_from_storage, retry_on_exception

# -----------------------
# Helpers for Testing
# -----------------------
class MockRow:
    def __init__(self, file_path, state, content_str):
        self.file_path = file_path
        self.state = state
        self.content_str = content_str

class MockProducer:
    def __init__(self, fail=False):
        self.fail = fail
        self.messages = []

    def produce(self, topic, key, value, callback=None):
        if self.fail:
            if callback:
                callback(Exception("Kafka error"), None)
            raise Exception("Kafka error")
        self.messages.append((topic, key, value))
        if callback:
            class DummyMsg:
                def key(self): return key
            callback(None, DummyMsg())

    def flush(self):
        return 0

class MockLogger:
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.infos = []

    def error(self, msg):
        self.errors.append(msg)

    def warning(self, msg):
        self.warnings.append(msg)

    def info(self, msg):
        self.infos.append(msg)

class MockBlob:
    def download_blob(self): return self
    def readall(self): return b"test data"

class MockStorageClient:
    def get_blob_client(self, container, blob): return MockBlob()

class RetryMockProducer:
    def __init__(self, fail_times=2):
        self.fail_times = fail_times
        self.attempts = 0
        self.messages = []

    def produce(self, topic, key, value, callback=None):
        self.attempts += 1
        if self.attempts <= self.fail_times:
            raise BufferError("Simulated buffer full")
        # Success callback
        if callback:
            class DummyMsg:
                def key(self): return key
            callback(None, DummyMsg())
        self.messages.append((topic, key, value))


# -----------------------
# Unit Tests
# -----------------------
class TestFunctions(unittest.TestCase):

    # --- validate_state ---
    def test_valid_state(self):
        DEFAULT_STATES = [ "paymentPending", "appealSubmitted", "awaitingRespondentEvidence(a)", "awaitingRespondentEvidence(b)", "caseUnderReview", "reasonForAppealSubmitted", "listing", "PrepareForHearing", "Decision", "FTPA Submitted (a)", "FTPA Submitted (b)", "Decided (b)", "Decided (a)", "FTPA Decided", "Ended", "Remitted" ]

        for state in DEFAULT_STATES:
            self.assertTrue(validate_state(state))

    # --- process_partition ---
    # All inputs are valid, return success from the result tuple
    def test_process_partition_success(self):
        row = MockRow("file1", "paymentPending", "content")
        producer = MockProducer()
        logger = MockLogger()
        result = process_partition([row], producer, "test-topic", logger)
        self.assertEqual(result[0][2], "success")
        self.assertEqual(len(producer.messages), 1)

    # If filepath or content is null, return failure from the result tuple
    def test_process_partition_failure_missing(self):
        row = MockRow(None, "appealSubmitted", None)
        producer = MockProducer()
        logger = MockLogger()
        result = process_partition([row], producer, "test-topic", logger)
        self.assertEqual(result[0][2], "failure")

    # If kafka fails (fail=True), return failure from the result tuple and that the logger produces errors
    def test_process_partition_kafka_error(self):
        row = MockRow("file1", "paymentPending", "content")
        producer = MockProducer(fail=True)
        logger = MockLogger()
        result = process_partition([row], producer, "test-topic", logger)
        self.assertEqual(result[0][2], "failure")
        self.assertTrue(logger.errors)
    
    #
    def test_kafka_retry_success(self):
        producer = RetryMockProducer(fail_times=2)
        results = []

        def dummy_callback(err, msg):
            results.append("success")

        # Wrap producer.produce with retry decorator
        @retry_on_exception(max_retries=5, delay=0.1)
        def produce_with_retry():
            producer.produce("topic", b"key", b"value", callback=dummy_callback)

        produce_with_retry()

        self.assertEqual(len(results), 1)
        self.assertEqual(producer.attempts, 3)

    # --- read_from_storage ---
    # return sample data assuming connection has been established with Mock Azure Storage Account
    def test_read_from_storage(self):
        client = MockStorageClient()
        data = read_from_storage(client, "container", "blob.txt")
        self.assertEqual(data, b"test data")

if __name__ == "__main__":
    unittest.main(argv=[''], verbosity=2, exit=False)
