import asyncio
import contextlib
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

from azure.core.exceptions import ResourceExistsError

# function_app.py reads os.environ at module level and calls FunctionApp(); patch both before import.
_mock_app = MagicMock()
_mock_app.function_name.return_value = lambda f: f
_mock_app.event_hub_message_trigger.return_value = lambda f: f

with patch.dict(os.environ, {"ENVIRONMENT": "sbox", "LZ_KEY": "testlz"}), \
        patch("azure.functions.FunctionApp", MagicMock(return_value=_mock_app)):
    from AzureFunctions.ACTIVE.active_cdam.function_app import eventhub_trigger_active, _is_retryable
    import AzureFunctions.ACTIVE.active_cdam.function_app as app_module


MODULE = "AzureFunctions.ACTIVE.active_cdam.function_app"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_mock_event(case_no="1234567890123456", run_id="run-001",
                    file_name="test.html",
                    file_url="https://storage.blob.core.windows.net/c/test.html",
                    file_content_type="text/html"):
    event = MagicMock()
    event.partition_key = case_no
    payload = {
        "RunID": run_id,
        "FileName": file_name,
        "FileURL": file_url,
        "FileContentType": file_content_type,
    }
    event.get_body.return_value = json.dumps(payload).encode("utf-8")
    event.metadata = {
        "PartitionContext": {"PartitionId": "0"},
        "SequenceNumberArray": [1],
    }
    return event


def setup_mocks(batch_len=1):
    """Return mocked Azure client objects used inside eventhub_trigger_active."""
    mock_secret = MagicMock()
    mock_secret.value = "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=abc="

    mock_kv_client = AsyncMock()
    mock_kv_client.__aenter__ = AsyncMock(return_value=mock_kv_client)
    mock_kv_client.__aexit__ = AsyncMock(return_value=False)
    mock_kv_client.get_secret.return_value = mock_secret

    mock_batch = MagicMock()
    mock_batch.__len__.return_value = batch_len

    mock_producer = AsyncMock()
    mock_producer.create_batch.return_value = mock_batch
    mock_producer.__aenter__ = AsyncMock(return_value=mock_producer)
    mock_producer.__aexit__ = AsyncMock(return_value=False)

    mock_credential = AsyncMock()
    mock_credential.__aenter__ = AsyncMock(return_value=mock_credential)
    mock_credential.__aexit__ = AsyncMock(return_value=False)
    mock_storage_credential = MagicMock()

    mock_idempotency_blob = AsyncMock()

    mock_idempotency_container = MagicMock()
    mock_idempotency_container.get_blob_client.return_value = mock_idempotency_blob

    mock_blob_svc = MagicMock()
    mock_blob_svc.close = AsyncMock()
    mock_blob_svc.__aenter__ = AsyncMock(return_value=mock_blob_svc)
    mock_blob_svc.__aexit__ = AsyncMock(return_value=False)
    mock_blob_svc.get_container_client.return_value = mock_idempotency_container

    return {
        "kv_client": mock_kv_client,
        "producer": mock_producer,
        "batch": mock_batch,
        "credential": mock_credential,
        "storage_credential": mock_storage_credential,
        "secret": mock_secret,
        "idempotency_blob": mock_idempotency_blob,
        "blob_svc": mock_blob_svc,
    }


PROCESS_SUCCESS_RESULT = {
    "Status": "SUCCESS",
    "CaseNo": "1234567890123456",
    "RunID": "run-001",
    "Error": None,
}

PROCESS_ERROR_RESULT = {
    "Status": "ERROR",
    "CaseNo": "1234567890123456",
    "RunID": "run-001",
    "Error": "Something went wrong",
}


def run(coro):
    return asyncio.run(coro)


def patched(mocks, to_thread_mock=None):
    patches = [
        patch(f"{MODULE}.DefaultAzureCredential", return_value=mocks["credential"]),
        patch(f"{MODULE}.SecretClient", return_value=mocks["kv_client"]),
        patch(f"{MODULE}.EventHubProducerClient"),
        patch(f"{MODULE}.BlobServiceClient"),
        patch(f"{MODULE}.ClientSecretCredential", return_value=mocks["storage_credential"]),
    ]
    if to_thread_mock is not None:
        patches.append(patch(f"{MODULE}.process_event", new=to_thread_mock))
    return patches


@contextlib.contextmanager
def apply_patches(patch_list, mocks):
    with contextlib.ExitStack() as stack:
        active = [stack.enter_context(p) for p in patch_list]
        # active[2] = EventHubProducerClient, active[3] = BlobServiceClient
        active[2].from_connection_string.return_value = mocks["producer"]
        active[3].return_value = mocks["blob_svc"]
        yield active


# ---------------------------------------------------------------------------
# Idempotency: blob uploaded before processing
# ---------------------------------------------------------------------------

def test_idempotency_blob_uploaded_atomically_before_processing():
    """upload_blob(b"", overwrite=False) is called before process_event runs."""
    mocks = setup_mocks(batch_len=1)
    events = [make_mock_event()]
    to_thread = MagicMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    mocks["idempotency_blob"].upload_blob.assert_awaited_once_with(b"", overwrite=False)


def test_idempotency_blob_path_contains_case_number_and_idempotency():
    """The blob path contains the case number and the 'idempotency' segment."""
    mocks = setup_mocks(batch_len=1)
    case_no = "9999000011112222"
    events = [make_mock_event(case_no=case_no)]
    to_thread = MagicMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    called_path = mocks["blob_svc"].get_container_client.return_value.get_blob_client.call_args[0][0]
    assert case_no in called_path
    assert "idempotency" in called_path


def test_idempotency_container_is_af_idempotency():
    """The BlobServiceClient is asked for the 'af-idempotency' container."""
    mocks = setup_mocks(batch_len=1)
    to_thread = MagicMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    mocks["blob_svc"].get_container_client.assert_called_once_with("af-idempotency")


# ---------------------------------------------------------------------------
# Idempotency: duplicate event skipped
# ---------------------------------------------------------------------------

def test_in_progress_event_skipped_when_idempotency_blob_exists():
    """When upload raises ResourceExistsError the event is skipped — process_event never called."""
    mocks = setup_mocks(batch_len=0)
    mocks["idempotency_blob"].upload_blob.side_effect = ResourceExistsError()
    events = [make_mock_event()]
    to_thread = MagicMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    to_thread.assert_not_called()
    mocks["batch"].add.assert_not_called()
    mocks["producer"].send_batch.assert_not_called()


def test_duplicate_skip_continues_to_next_event():
    """After skipping a duplicate, later events in the same batch are still processed."""
    mocks = setup_mocks(batch_len=1)

    blob_dup = AsyncMock()
    blob_dup.upload_blob.side_effect = ResourceExistsError()
    blob_new = AsyncMock()

    mocks["blob_svc"].get_container_client.return_value.get_blob_client.side_effect = [
        blob_dup, blob_new
    ]

    events = [make_mock_event(case_no="1111111111111111"), make_mock_event(case_no="2222222222222222")]
    to_thread = MagicMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    to_thread.assert_called_once()


# ---------------------------------------------------------------------------
# Idempotency: blob deleted on failure, kept on success
# ---------------------------------------------------------------------------

def test_idempotency_blob_not_deleted_on_success():
    """SUCCESS result: idempotency blob is kept to prevent future duplicate runs."""
    mocks = setup_mocks(batch_len=1)
    to_thread = MagicMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    mocks["idempotency_blob"].delete_blob.assert_not_called()


def test_idempotency_blob_deleted_on_error_result():
    """ERROR result: idempotency blob is deleted so the event can be retried."""
    mocks = setup_mocks(batch_len=1)
    to_thread = MagicMock(return_value=dict(PROCESS_ERROR_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    mocks["idempotency_blob"].delete_blob.assert_awaited_once()


def test_idempotency_blob_not_touched_when_no_events():
    """With an empty batch the idempotency blob is never touched."""
    mocks = setup_mocks(batch_len=0)

    with apply_patches(patched(mocks), mocks):
        run(eventhub_trigger_active([]))

    mocks["idempotency_blob"].upload_blob.assert_not_called()
    mocks["idempotency_blob"].delete_blob.assert_not_called()


# ---------------------------------------------------------------------------
# Normal processing
# ---------------------------------------------------------------------------

def test_single_event_success_sends_final_batch():
    """Happy path: one event processed, result batch sent."""
    mocks = setup_mocks(batch_len=1)
    to_thread = MagicMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    mocks["producer"].send_batch.assert_awaited_once_with(mocks["batch"])


def test_process_event_called_with_correct_args():
    """asyncio.to_thread receives process_event and the right positional arguments."""
    mocks = setup_mocks(batch_len=1)
    case_no = "9876543210987654"
    run_id = "run-007"
    file_name = "doc.pdf"
    file_url = "https://storage.blob.core.windows.net/c/doc.pdf"
    file_content_type = "application/pdf"
    events = [make_mock_event(case_no=case_no, run_id=run_id, file_name=file_name,
                              file_url=file_url, file_content_type=file_content_type)]
    to_thread = MagicMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    to_thread.assert_called_once_with(
        app_module.ENV,
        case_no,
        run_id,
        file_name,
        file_url,
        file_content_type,
        mocks["storage_credential"],
    )


def test_keyvault_secret_fetched_with_correct_name():
    """The correct Results Event Hub secret name is requested from Key Vault."""
    mocks = setup_mocks(batch_len=0)

    with apply_patches(patched(mocks), mocks):
        run(eventhub_trigger_active([]))

    expected = f"evh-active-cdam-res-{app_module.ENV}-{app_module.LZ_KEY}-uks-dlrm-01-key"
    mocks["kv_client"].get_secret.assert_any_await(expected)


def test_producer_created_from_kv_secret_value():
    """EventHubProducerClient is created from the connection string retrieved from Key Vault."""
    mocks = setup_mocks(batch_len=0)

    with apply_patches(patched(mocks), mocks) as active:
        eh_cls_mock = active[2]
        run(eventhub_trigger_active([]))

    eh_cls_mock.from_connection_string.assert_called_once_with(conn_str=mocks["secret"].value)


def test_error_result_is_still_added_to_batch():
    """ERROR results are sent to the results Event Hub (so downstream can handle them)."""
    mocks = setup_mocks(batch_len=1)
    to_thread = MagicMock(return_value=dict(PROCESS_ERROR_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    mocks["batch"].add.assert_called_once()
    mocks["producer"].send_batch.assert_awaited_once()


def test_empty_event_list_does_not_send_batch():
    """With no events, send_batch is never called."""
    mocks = setup_mocks(batch_len=0)

    with apply_patches(patched(mocks), mocks):
        run(eventhub_trigger_active([]))

    mocks["producer"].send_batch.assert_not_called()


def test_multiple_events_all_results_added_to_batch():
    """Three events → three add() calls and one final send_batch."""
    mocks = setup_mocks(batch_len=3)
    events = [make_mock_event(case_no=f"REF{i:016d}") for i in range(3)]
    to_thread = MagicMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    assert mocks["batch"].add.call_count == 3
    mocks["producer"].send_batch.assert_awaited_once_with(mocks["batch"])
    mocks["idempotency_blob"].delete_blob.assert_not_called()


def test_batch_overflow_flushes_old_batch_and_creates_new_one():
    """When add() raises ValueError, the current batch is sent and a new batch is opened."""
    second_batch = MagicMock()
    second_batch.__len__.return_value = 1

    mocks = setup_mocks(batch_len=1)
    mocks["batch"].add.side_effect = ValueError("Batch is full")
    mocks["producer"].create_batch = AsyncMock(side_effect=[mocks["batch"], second_batch])

    to_thread = MagicMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    assert mocks["producer"].send_batch.await_count == 2
    mocks["producer"].send_batch.assert_any_await(mocks["batch"])
    mocks["producer"].send_batch.assert_any_await(second_batch)


def test_individual_event_error_does_not_stop_other_events():
    """An exception for one event is caught; remaining events still process."""
    mocks = setup_mocks(batch_len=1)
    call_count = 0

    def side_effect(*_):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("First event failed")
        return dict(PROCESS_SUCCESS_RESULT)

    events = [make_mock_event(case_no="FAIL000000000001"), make_mock_event(case_no="OK00000000000002")]

    with apply_patches(patched(mocks, to_thread_mock=side_effect), mocks):
        run(eventhub_trigger_active(events))

    assert mocks["batch"].add.call_count == 1
    mocks["producer"].send_batch.assert_awaited_once_with(mocks["batch"])


def test_cleanup_always_called_on_success():
    """kv_client and credential are exited (closed) via the AsyncExitStack."""
    mocks = setup_mocks(batch_len=1)
    to_thread = MagicMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    mocks["kv_client"].__aexit__.assert_awaited_once()
    mocks["credential"].__aexit__.assert_awaited_once()


def test_cleanup_called_even_when_event_processing_raises():
    """Cleanup runs even when an event raises an exception."""
    mocks = setup_mocks(batch_len=0)
    to_thread = MagicMock(side_effect=Exception("processing failed"))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    mocks["kv_client"].__aexit__.assert_awaited_once()
    mocks["credential"].__aexit__.assert_awaited_once()


def test_error_result_sent_even_when_delete_blob_raises():
    """When delete_blob raises on ERROR, the result is still added to the batch and sent."""
    mocks = setup_mocks(batch_len=1)
    mocks["idempotency_blob"].delete_blob.side_effect = Exception("Storage error")
    to_thread = MagicMock(return_value=dict(PROCESS_ERROR_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    mocks["idempotency_blob"].delete_blob.assert_awaited_once()
    mocks["batch"].add.assert_called_once()
    mocks["producer"].send_batch.assert_awaited_once()


# ---------------------------------------------------------------------------
# _is_retryable unit tests
# ---------------------------------------------------------------------------

def test_cdam_is_retryable_true_for_retryable_status_codes():
    for code in [408, 409, 429, 500, 502, 503, 504]:
        assert _is_retryable({"Status": "ERROR", "StatusCode": code}) is True, f"Expected retryable for {code}"


def test_cdam_is_retryable_false_for_non_retryable_status_codes():
    for code in [200, 201, 400, 401, 403, 404, 422]:
        assert _is_retryable({"Status": "ERROR", "StatusCode": code}) is False, f"Expected non-retryable for {code}"


def test_cdam_is_retryable_false_when_status_is_success():
    assert _is_retryable({"Status": "SUCCESS", "StatusCode": 500}) is False


def test_cdam_is_retryable_false_when_status_code_is_none():
    assert _is_retryable({"Status": "ERROR", "StatusCode": None}) is False


def test_cdam_is_retryable_false_for_non_dict_result():
    assert _is_retryable("error string") is False
    assert _is_retryable(None) is False


def test_cdam_is_retryable_true_for_transient_error_types_with_missing_status_code():
    for error_type in ("ConnectionError", "ConnectTimeout", "ReadTimeout", "Timeout",
                        "ChunkedEncodingError", "SSLError", "ProxyError", "EOFError",
                        "ServiceRequestError", "ServiceRequestTimeoutError",
                        "ServiceResponseError", "ServiceResponseTimeoutError"):
        result = {"Status": "ERROR", "StatusCode": None, "ErrorType": error_type}
        assert _is_retryable(result) is True, f"Expected retryable for ErrorType {error_type}"


def test_cdam_is_retryable_false_for_non_transient_error_type_with_missing_status_code():
    result = {"Status": "ERROR", "StatusCode": None, "ErrorType": "ResourceNotFoundError"}
    assert _is_retryable(result) is False


def test_cdam_is_retryable_false_when_error_type_present_but_status_code_is_set():
    result = {"Status": "ERROR", "StatusCode": 422, "ErrorType": "ConnectionError"}
    assert _is_retryable(result) is False


# ---------------------------------------------------------------------------
# Retry integration tests
# ---------------------------------------------------------------------------

def test_cdam_non_retryable_error_not_retried():
    """process_event is NOT retried for a non-retryable status code (422)."""
    from tenacity import wait_none
    mocks = setup_mocks(batch_len=1)
    to_thread = MagicMock(return_value={"Status": "ERROR", "StatusCode": 422, "Error": "Unprocessable"})

    extra = [patch(f"{MODULE}.wait_exponential", return_value=wait_none())]
    with apply_patches(patched(mocks, to_thread_mock=to_thread) + extra, mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    assert to_thread.call_count == 1


def test_cdam_retryable_error_is_retried_3_times():
    """process_event is retried up to 3 times for a retryable status code (500)."""
    from tenacity import wait_none
    mocks = setup_mocks(batch_len=1)
    to_thread = MagicMock(return_value={"Status": "ERROR", "StatusCode": 500, "Error": "Server Error"})

    extra = [patch(f"{MODULE}.wait_exponential", return_value=wait_none())]
    with apply_patches(patched(mocks, to_thread_mock=to_thread) + extra, mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    assert to_thread.call_count == 3


def test_cdam_retryable_error_stops_retrying_on_success():
    """After a retryable error, success on the second attempt stops further retries."""
    from tenacity import wait_none
    mocks = setup_mocks(batch_len=1)
    to_thread = MagicMock(side_effect=[
        {"Status": "ERROR", "StatusCode": 502, "Error": "Bad Gateway"},
        dict(PROCESS_SUCCESS_RESULT),
    ])

    extra = [patch(f"{MODULE}.wait_exponential", return_value=wait_none())]
    with apply_patches(patched(mocks, to_thread_mock=to_thread) + extra, mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    assert to_thread.call_count == 2
    mocks["idempotency_blob"].delete_blob.assert_not_called()


def test_cdam_transient_network_error_is_retried_3_times():
    """A transient ErrorType with no StatusCode (e.g. blob download connection error)
    is retried the same as a retryable status code."""
    from tenacity import wait_none
    mocks = setup_mocks(batch_len=1)
    to_thread = MagicMock(return_value={
        "Status": "ERROR", "StatusCode": None, "ErrorType": "ServiceRequestError",
        "Error": "storage account unreachable",
    })

    extra = [patch(f"{MODULE}.wait_exponential", return_value=wait_none())]
    with apply_patches(patched(mocks, to_thread_mock=to_thread) + extra, mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    assert to_thread.call_count == 3


def test_cdam_error_type_stripped_from_event_hub_payload():
    """ErrorType is an internal routing field and must not appear in the published event."""
    mocks = setup_mocks(batch_len=1)
    to_thread = MagicMock(return_value={
        "Status": "ERROR", "StatusCode": 400, "ErrorType": "InvalidSchema",
        "CaseNo": "1234567890123456", "Error": "bad request",
    })

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active([make_mock_event()]))

    mocks["batch"].add.assert_called_once()
    published_payload = mocks["batch"].add.call_args[0][0].body_as_json()
    assert "ErrorType" not in published_payload
