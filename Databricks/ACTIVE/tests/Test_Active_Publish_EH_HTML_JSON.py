import pytest
from unittest.mock import Mock, patch
from test_active_publish_functions import process_partition, read_from_storage, validate_state

# --- process_partition with successful retry ---
def test_buffer_error_retry():
    """Test that BufferError triggers retry logic and succeeds on retry"""

    # Create a mock row
    mock_row = Mock()
    mock_row.file_path = "test_file.json"
    mock_row.content_str = "test content"
    mock_row.state = "test_state"

    # Mock Kafka message for callback
    mock_msg = Mock()
    mock_msg.key.return_value = b"test_file.json"

    with patch("confluent_kafka.Producer") as MockProducer:
        mock_producer = Mock()
        MockProducer.return_value = mock_producer

        # Define produce side effect
        def produce_side_effect(*args, **kwargs):
            if produce_side_effect.counter == 0:
                produce_side_effect.counter += 1
                raise BufferError()  # first call fails
            else:
                # simulate successful delivery via callback
                if 'callback' in kwargs and kwargs['callback']:
                    kwargs['callback'](None, mock_msg)

        produce_side_effect.counter = 0
        mock_producer.produce.side_effect = produce_side_effect
        mock_producer.poll = Mock()
        mock_producer.flush = Mock()

        # Run function
        results = process_partition([mock_row])

        # Assertions
        assert mock_producer.produce.call_count == 2         #Producer should be called twice
        mock_producer.poll.assert_called_once_with(1)        #poll(1) called once after first failure (retry)
        assert any(r[2] == "success" for r in results)       #Contain at least 1 success in row
        assert not any(r[2] == "failure" for r in results)   #Contain zero failure in row

# --- process_partition with failed retry ---
def test_buffer_error_retry_fails():
    """Test when retry also fails"""
    
    # Create mock row
    mock_row = Mock()
    mock_row.file_path = "test_file.json"
    mock_row.content_str = "test content"
    mock_row.state = "test_state"
    
    with patch('confluent_kafka.Producer') as MockProducer:
        
        mock_producer = Mock()
        MockProducer.return_value = mock_producer
        
        # Both calls fail
        mock_producer.produce.side_effect = [BufferError(), BufferError()]
        
        # Call your actual function
        results = process_partition([mock_row])
        
        # Verify retry was attempted
        assert mock_producer.produce.call_count == 2                        # Producer should be called twice
        mock_producer.poll.assert_called_once_with(1)                       # poll(1) called once after first failure (retry)
        assert any("Buffer error retry failed" in r[3] for r in results)    # assert buffer error from failure_list was produced at least once (failure before retry)

# --- read_from_storage ---
def test_read_from_storage():
    # Create a mock blob with a readall method
    mock_blob = Mock()
    mock_blob.download_blob.return_value = mock_blob
    mock_blob.readall.return_value = b"test data"

    # Create a mock storage client that returns the mock blob
    mock_storage_client = Mock()
    mock_storage_client.get_blob_client.return_value = mock_blob

    # Call the function
    data = read_from_storage(mock_storage_client, "container", "blob.txt")

    # Assert the result
    assert data == b"test data"

    # Optional: check that get_blob_client was called with correct args
    mock_storage_client.get_blob_client.assert_called_once_with(container="container", blob="blob.txt")


# --- validate_state ---
def test_valid_state():
    default_states = [ "paymentPending", "appealSubmitted", "awaitingRespondentEvidence(a)", "awaitingRespondentEvidence(b)", "caseUnderReview", "reasonsForAppealSubmitted", "listing", "PrepareForHearing", "Decision", "FTPA Submitted (a)", "FTPA Submitted (b)", "Decided (b)", "Decided (a)", "FTPA Decided", "Ended", "Remitted" ]

    for state in default_states:
        assert validate_state(state) is True

