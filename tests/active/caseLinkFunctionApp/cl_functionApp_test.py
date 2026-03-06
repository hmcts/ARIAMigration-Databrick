import asyncio
import json
import os
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from AzureFunctions.ACTIVE.active_caselink_ccd.function_app import eventhub_trigger_active
import AzureFunctions.ACTIVE.active_caselink_ccd.function_app as app_module

# These must be set before cl_function_app is imported, as it reads them at module level.
os.environ.setdefault("ENVIRONMENT", "sbox")
os.environ.setdefault("LZ_KEY", "testlz")
os.environ.setdefault("PR_NUMBER", "9999")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_mock_event(body_data, partition_key="1234567890123456"):
    """Build a minimal mock Azure EventHubEvent."""
    event = MagicMock()
    event.partition_key = partition_key
    event.get_body.return_value = json.dumps(body_data).encode("utf-8")
    return event


def setup_mocks(batch_len=1):
    """Return mocked Azure client objects used inside eventhub_trigger_active."""
    mock_secret = MagicMock()
    mock_secret.value = "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=abc="

    mock_kv_client = AsyncMock()
    mock_kv_client.get_secret.return_value = mock_secret

    mock_batch = MagicMock()
    mock_batch.__len__.return_value = batch_len

    mock_producer = AsyncMock()
    mock_producer.create_batch.return_value = mock_batch
    mock_producer.__aenter__ = AsyncMock(return_value=mock_producer)
    mock_producer.__aexit__ = AsyncMock(return_value=False)

    mock_credential = AsyncMock()

    return {
        "kv_client": mock_kv_client,
        "producer": mock_producer,
        "batch": mock_batch,
        "credential": mock_credential,
        "secret": mock_secret,
    }


PROCESS_SUCCESS_RESULT = {
    "Status": "Success",
    "CCDCaseReferenceNumber": "1234567890123456",
    "RunID": "run-001",
    "CaseLinkCount": 2,
    "Error": None,
}

PROCESS_ERROR_RESULT = {
    "Status": "ERROR",
    "CCDCaseReferenceNumber": "1234567890123456",
    "RunID": "run-001",
    "CaseLinkCount": 0,
    "Error": "Something went wrong",
}


def run(coro):
    """Run a coroutine synchronously in tests (pytest-asyncio not required)."""
    return asyncio.run(coro)


def patched(mocks, to_thread_mock=None, extra_patches=None):
    """
    Return a list of patch context managers targeting the Azure clients used
    by eventhub_trigger_active, plus an optional asyncio.to_thread override.
    """
    patches = [
        patch(
            "AzureFunctions.ACTIVE.active_ccd.case_link.cl_function_app.DefaultAzureCredential",
            return_value=mocks["credential"],
        ),
        patch(
            "AzureFunctions.ACTIVE.active_ccd.case_link.cl_function_app.SecretClient",
            return_value=mocks["kv_client"],
        ),
        patch(
            "AzureFunctions.ACTIVE.active_ccd.case_link.cl_function_app.EventHubProducerClient",
        ),
    ]
    if to_thread_mock is not None:
        patches.append(patch("asyncio.to_thread", new=to_thread_mock))
    if extra_patches:
        patches.extend(extra_patches)
    return patches


def apply_patches(patch_list, mocks):
    """Apply all patches and wire the producer mock. Returns a context-manager stack."""
    import contextlib

    @contextlib.contextmanager
    def _stack():
        with contextlib.ExitStack() as stack:
            active = [stack.enter_context(p) for p in patch_list]
            # active[2] is the EventHubProducerClient patch
            active[2].from_connection_string.return_value = mocks["producer"]
            yield active

    return _stack()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_single_event_success_sends_final_batch():
    """Happy path: one event is processed and the result batch is sent."""
    mocks = setup_mocks(batch_len=1)
    payload = {"RunID": "run-001", "Content": {"caseLinks": []}}
    events = [make_mock_event(payload)]

    to_thread = AsyncMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    mocks["producer"].send_batch.assert_awaited_once_with(mocks["batch"])


def test_cleanup_always_called_on_success():
    """kv_client.close and credential.close are awaited after successful processing."""
    mocks = setup_mocks(batch_len=1)
    events = [make_mock_event({"RunID": "r1", "Content": {}})]

    to_thread = AsyncMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    mocks["kv_client"].close.assert_awaited_once()
    mocks["credential"].close.assert_awaited_once()


def test_cleanup_called_even_when_individual_event_errors():
    """Cleanup runs even when an event raises an exception in the inner try/except."""
    mocks = setup_mocks(batch_len=0)
    events = [make_mock_event({"RunID": "r1", "Content": {}})]

    to_thread = AsyncMock(side_effect=Exception("processing failed"))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    mocks["kv_client"].close.assert_awaited_once()
    mocks["credential"].close.assert_awaited_once()


def test_keyvault_secret_fetched_with_correct_name():
    """The correct Results Event Hub secret name is requested from Key Vault."""
    mocks = setup_mocks(batch_len=0)
    events = []

    with apply_patches(patched(mocks), mocks):
        run(eventhub_trigger_active(events))

    # From module: f"evh-active-res-{ENV}-{LZ_KEY}-uks-dlrm-01-key"
    expected = f"evh-active-res-{app_module.ENV}-{app_module.LZ_KEY}-uks-dlrm-01-key"
    mocks["kv_client"].get_secret.assert_awaited_once_with(expected)


def test_producer_created_from_kv_secret_value():
    """EventHubProducerClient is created using the connection string from Key Vault."""
    mocks = setup_mocks(batch_len=0)
    events = []

    with apply_patches(patched(mocks), mocks) as active:
        eh_cls_mock = active[2]
        run(eventhub_trigger_active(events))

    eh_cls_mock.from_connection_string.assert_called_once_with(
        conn_str=mocks["secret"].value
    )


def test_process_event_called_with_correct_args():
    """asyncio.to_thread receives process_event and the right positional arguments."""
    mocks = setup_mocks(batch_len=1)
    payload = {"RunID": "run-007", "Content": {"key": "value"}}
    partition_key = "9876543210987654"
    events = [make_mock_event(payload, partition_key=partition_key)]

    to_thread = AsyncMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    to_thread.assert_awaited_once_with(
        app_module.process_event,
        app_module.ENV,
        partition_key,
        payload["Content"],
        payload["RunID"],
        app_module.PR_NUMBER,
    )


def test_start_datetime_injected_into_result():
    """StartDateTime is added to the result dict before it is serialised to JSON."""
    mocks = setup_mocks(batch_len=1)
    events = [make_mock_event({"RunID": "run-dt", "Content": {}})]

    to_thread = AsyncMock(return_value={"Status": "Success", "Error": None})
    serialized_strings = []

    original_event_data = app_module.EventData

    def capture_event_data(body):
        serialized_strings.append(body)
        return original_event_data(body)

    extra = [
        patch(
            "AzureFunctions.ACTIVE.active_ccd.case_link.cl_function_app.EventData",
            side_effect=capture_event_data,
        )
    ]

    with apply_patches(patched(mocks, to_thread_mock=to_thread, extra_patches=extra), mocks):
        run(eventhub_trigger_active(events))

    assert len(serialized_strings) == 1
    result = json.loads(serialized_strings[0])
    assert "StartDateTime" in result


def test_empty_event_list_does_not_send_batch():
    """With no events, the final send_batch is skipped (empty batch)."""
    mocks = setup_mocks(batch_len=0)

    with apply_patches(patched(mocks), mocks):
        run(eventhub_trigger_active([]))

    mocks["producer"].send_batch.assert_not_called()


def test_multiple_events_each_result_added_to_batch():
    """Three events → three add() calls and one final send_batch."""
    mocks = setup_mocks(batch_len=3)
    events = [
        make_mock_event({"RunID": f"run-{i}", "Content": {}}, partition_key=f"REF-{i}")
        for i in range(3)
    ]

    to_thread = AsyncMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    assert mocks["batch"].add.call_count == 3
    mocks["producer"].send_batch.assert_awaited_once_with(mocks["batch"])


def test_batch_overflow_flushes_old_batch_and_creates_new_one():
    """When add() raises ValueError the current batch is sent and a new batch opened."""
    second_batch = MagicMock()
    second_batch.__len__.return_value = 1

    mocks = setup_mocks(batch_len=1)
    # First add raises ValueError (batch full); second add (on new batch) succeeds.
    mocks["batch"].add.side_effect = ValueError("Batch is full")
    mocks["producer"].create_batch = AsyncMock(
        side_effect=[mocks["batch"], second_batch]
    )

    payload = {"RunID": "run-overflow", "Content": {}}
    events = [make_mock_event(payload)]

    to_thread = AsyncMock(return_value=dict(PROCESS_SUCCESS_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    # First: old batch flushed on overflow. Second: new batch flushed at end.
    assert mocks["producer"].send_batch.await_count == 2
    mocks["producer"].send_batch.assert_any_await(mocks["batch"])
    mocks["producer"].send_batch.assert_any_await(second_batch)


def test_individual_event_error_does_not_stop_other_events():
    """An exception for one event is caught; remaining events are still processed."""
    mocks = setup_mocks(batch_len=1)

    call_count = 0

    async def side_effect(*_):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("First event failed")
        return dict(PROCESS_SUCCESS_RESULT)

    events = [
        make_mock_event({"RunID": "run-fail", "Content": {}}, partition_key="REF-FAIL"),
        make_mock_event({"RunID": "run-ok", "Content": {}}, partition_key="REF-OK"),
    ]

    with apply_patches(patched(mocks, to_thread_mock=side_effect), mocks):
        run(eventhub_trigger_active(events))

    # Only the second event's result should have been added.
    assert mocks["batch"].add.call_count == 1
    mocks["producer"].send_batch.assert_awaited_once_with(mocks["batch"])


def test_error_result_still_serialised_and_sent():
    """An error result from process_event is still serialised and added to the batch."""
    mocks = setup_mocks(batch_len=1)
    events = [make_mock_event({"RunID": "run-err", "Content": {}})]

    to_thread = AsyncMock(return_value=dict(PROCESS_ERROR_RESULT))

    with apply_patches(patched(mocks, to_thread_mock=to_thread), mocks):
        run(eventhub_trigger_active(events))

    mocks["batch"].add.assert_called_once()
    mocks["producer"].send_batch.assert_awaited_once()


def test_keyvault_failure_propagates_as_exception():
    """If Key Vault secret retrieval fails, the exception propagates to the caller."""
    mocks = setup_mocks()
    mocks["kv_client"].get_secret.side_effect = Exception("KeyVault unavailable")

    with apply_patches(patched(mocks), mocks):
        with pytest.raises(Exception, match="KeyVault unavailable"):
            run(eventhub_trigger_active([make_mock_event({"RunID": "r1", "Content": {}})]))
