import asyncio
import json
import os
from unittest.mock import patch, MagicMock, ANY, AsyncMock

# ccdFunctions instantiates IDAMTokenManager at module level, which calls Azure
# Key Vault in __init__. Patch the Azure SDK before importing to prevent real
# network calls during module load.
with patch("azure.identity.DefaultAzureCredential"), \
        patch("azure.keyvault.secrets.SecretClient"):
    from AzureFunctions.ACTIVE.active_ccd.ccdFunctions import start_case_creation, validate_case, submit_case, process_case

# function_app.py reads os.environ at module level and calls FunctionApp(); patch both for import.
# The FunctionApp mock must use identity decorators so the original async function is preserved.
_mock_app = MagicMock()
_mock_app.function_name.return_value = lambda f: f
_mock_app.event_hub_message_trigger.return_value = lambda f: f

with patch.dict(os.environ, {"ENVIRONMENT": "sbox", "LZ_KEY": "test0", "PR_NUMBER": "9999"}), \
        patch("azure.functions.FunctionApp", MagicMock(return_value=_mock_app)):
    from AzureFunctions.ACTIVE.active_ccd.function_app import eventhub_trigger_active, _is_retryable

# FUNCTIONS  - this is to be removed once we import the functions at the top of the script (This was only done because we could not merge)


@patch("requests.get")
def test_start_case_success(mock_get):
    ccd_base_url = "https://ccd-api.test.net"
    uid = "12345"
    jid = "IA"
    ctid = "Asylum"
    etid = "createCase"
    idam_token = "idam123"
    s2s_token = "s2s123"

    mock_start_case_response = MagicMock()
    mock_start_case_response.status_code = 200
    mock_get.return_value = mock_start_case_response

    response = start_case_creation(ccd_base_url, uid, jid, ctid, etid, idam_token, s2s_token)

    expected_url = f"{ccd_base_url}/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/event-triggers/{etid}/token"
    # check the correct url is called
    mock_get.assert_called_once_with(
        expected_url,
        headers={
            "Authorization": f"Bearer {idam_token}",
            "ServiceAuthorization": f"{s2s_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
    )
    assert response == mock_start_case_response
    assert response.status_code == 200


@patch("requests.get")
def test_start_case_network_failure(mock_get):
    # Arrange
    ccd_base_url = "https://ccd-api.test.net"
    uid = "12345"
    jid = "IA"
    ctid = "Asylum"
    etid = "createCase"
    idam_token = "idam123"
    s2s_token = "s2s123"

    # simulate a network error
    mock_get.side_effect = Exception("Connection error")

    response = start_case_creation(ccd_base_url, uid, jid, ctid, etid, idam_token, s2s_token)

    # Asserts
    expected_url = f"{ccd_base_url}/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/event-triggers/{etid}/token"
    mock_get.assert_called_once_with(
        expected_url,
        headers={
            "Authorization": f"Bearer {idam_token}",
            "ServiceAuthorization": f"{s2s_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
    )

    # The function should return None on network failure
    assert response is None


@patch("requests.post")
def test_validate_case_success(mock_post):
    mock_validate_case_response = MagicMock()
    mock_validate_case_response.status_code = 201
    mock_post.return_value = mock_validate_case_response

    ccd_base_url = "https://ccd-api.test.net"
    uid = "12345"
    jid = "IA"
    ctid = "Asylum"
    idam_token = "test_idam_token"
    s2s_token = "test_s2s_token"

    event_token = "test_event_token"

    payload_data = {"data": "test_payload"}

    validate_case_response = validate_case(ccd_base_url, event_token, payload_data, jid, ctid, idam_token, uid, s2s_token)

    expected_url = f"{ccd_base_url}/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/validate"
    # assertions
    mock_post.assert_called_once_with(
        expected_url,
        headers={
            "Authorization": f"Bearer {idam_token}",
            "ServiceAuthorization": f"{s2s_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        json=ANY
    )

    assert validate_case_response.status_code == 201


@patch("requests.post")
def test_validate_case_failure(mock_post):

    ccd_base_url = "https://ccd-api.test.net"
    uid = "12345"
    jid = "IA"
    ctid = "Asylum"
    idam_token = "test_idam_token"
    s2s_token = "test_s2s_token"

    event_token = "test_event_token"

    payload_data = {"data": "test_payload"}

    mock_post.side_effect = Exception("Connection error")

    validate_case_response = validate_case(ccd_base_url, event_token, payload_data, jid, ctid, idam_token, uid, s2s_token)

    expected_url = f"{ccd_base_url}/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/validate"
    # assertions
    mock_post.assert_called_once_with(
        expected_url,
        headers={
            "Authorization": f"Bearer {idam_token}",
            "ServiceAuthorization": f"{s2s_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        json=ANY
    )
    validate_case_response is None


@patch("requests.post")
def test_submit_case_success(mock_post):

    mock_submit_response = MagicMock()
    mock_submit_response.status_code = 201
    mock_submit_response.json.return_value = {"id": "1234567891011"}
    mock_post.return_value = mock_submit_response

    ccd_base_url = "https://ccd-api.test.net"
    uid = "12345"
    jid = "IA"
    ctid = "Asylum"
    idam_token = "test_idam_token"
    s2s_token = "test_s2s_token"

    event_token = "test_event_token"

    payload_data = {"data": "test_payload"}

    submit_response = submit_case(ccd_base_url, event_token, payload_data, jid, ctid, idam_token, uid, s2s_token)

    # assertions

    expected_url = f"{ccd_base_url}/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/cases"

    mock_post.assert_called_once_with(
        expected_url,
        headers={
            "Authorization": f"Bearer {idam_token}",
            "ServiceAuthorization": f"{s2s_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        json=ANY  # ✅ same fix as validate_case
    )

    assert submit_response.status_code == 201
    assert submit_response.json()["id"] == "1234567891011"


@patch("requests.post")
def test_submit_case_failure(mock_post):
    mock_post.side_effect = Exception("Failed to make submit API call")

    ccd_base_url = "https://ccd-api.test.net"
    uid = "12345"
    jid = "IA"
    ctid = "Asylum"

    idam_token = "test_idam_token"
    s2s_token = "test_s2s_token"

    event_token = "test_event_token"

    payload_data = {"data": "test_payload"}

    submit_response = submit_case(ccd_base_url, event_token, payload_data, jid, ctid, idam_token, uid, s2s_token)

    expected_url = f"{ccd_base_url}/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/cases"
    # assertions
    mock_post.assert_called_once_with(
        expected_url,
        headers={
            "Authorization": f"Bearer {idam_token}",
            "ServiceAuthorization": f"{s2s_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        json=ANY
    )

    assert submit_response is None


def mock_response(status_code, json_data=None, text=""):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json_data = json_data
    mock.text = text
    mock.headers = {'Content-Type': "application/json"}
    mock.json.return_value = json_data or {}
    return mock


# Succesfully sent a case payload
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.idam_token_mgr")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.s2s_manager")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.submit_case")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.validate_case")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_funciton_success(mock_start_case_creation_response,
                                  mock_validate_case_response,
                                  mock_submit_case_response,
                                  mock_s2s_manager,
                                  mock_idam_mgr):

    mock_idam_mgr.get_token.return_value = ("mock_idam_token", "mock_uid")
    mock_s2s_manager.get_token.return_value = "mock_s2s_token"

    mock_start_case_creation = mock_response(200, {"token": "ABC123"})
    mock_validate_case = mock_response(201)
    mock_validate_case.json.return_value = {"case_data": {"validated": True}}
    mock_submit_case = mock_response(201)
    mock_submit_case.json.return_value = {"id": "1234567891011", "case_data": {"submitted": True}}

    mock_start_case_creation_response.return_value = mock_start_case_creation
    mock_validate_case_response.return_value = mock_validate_case
    mock_submit_case_response.return_value = mock_submit_case

    results = process_case(
        env="sbox",
        caseNo="CASE123",
        payloadData={"key": "value"},
        runId="run123",
        state="paymentPending",
        PR_REFERENCE="2866"
    )

    assert results["Status"] == "SUCCESS"
    assert results["CCDCaseID"] == "1234567891011"
    assert results["Error"] is None
    assert json.loads(results["SuccessResponse"]) == {"id": "1234567891011", "case_data": {"submitted": True}}
    assert json.loads(results["StartResponse"]) == {"token": "ABC123"}

# failed to start case creation


@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.idam_token_mgr")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.s2s_manager")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_case_url_uses_pr_number(mock_start_case_creation, mock_s2s_manager, mock_idam_mgr):
    mock_idam_mgr.get_token.return_value = ("mock_idam_token", "mock_uid")
    mock_s2s_manager.get_token.return_value = "mock_s2s_token"

    # Return 401 so process_case exits before validate_case is reached
    mock_start_case_creation.return_value = mock_response(401, text="Unauthorized")

    pr_number = "9999"
    process_case(
        env="sbox",
        caseNo="CASE123",
        payloadData={"key": "value"},
        runId="run123",
        state="paymentPending",
        PR_REFERENCE=pr_number,
    )

    called_url = mock_start_case_creation.call_args[0][0]
    assert pr_number in called_url
    assert "ynot-pr-1" not in called_url


@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.idam_token_mgr")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.s2s_manager")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_case_start_case_fail(mock_start_case_creation, mock_s2s_manager, mock_idam_mgr):
    mock_idam_mgr.get_token.return_value = ("mock_idam_token", "mock_uid")
    mock_s2s_manager.get_token.return_value = "mock_s2s_token"

    mock_start_case_response = mock_response(401, {"text": "bad response"})
    mock_start_case_creation.return_value = mock_start_case_response

    results = process_case(
        env="sbox",
        caseNo="CASE123",
        payloadData={"key": "value"},
        runId="run123",
        state="paymentPending",
        PR_REFERENCE="2866"
    )

    assert results["Status"] == "ERROR"
    assert results["Error"] is not None


@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.idam_token_mgr")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.s2s_manager")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.validate_case")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_case_validation_fails(mock_start, mock_validate, mock_s2s_manager, mock_idam_mgr):
    mock_idam_mgr.get_token.return_value = ("mock_idam_token", "mock_uid")
    mock_s2s_manager.get_token.return_value = "mock_s2s_token"

    mock_start.return_value = mock_response(200, {"token": "abc123"})
    mock_validate.return_value = mock_response(400, text="Invalid payload")

    result = process_case(
        env="sbox",
        caseNo="CASE888",
        payloadData={},
        runId="run123",
        state="paymentPending",
        PR_REFERENCE="2866"
    )

    assert result["Status"] == "ERROR"
    assert result["Error"] is not None


@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.idam_token_mgr")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.s2s_manager")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.submit_case")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.validate_case")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_case_submission_fails(mock_start, mock_validate, mock_submit, mock_s2s_manager, mock_idam_mgr):
    mock_idam_mgr.get_token.return_value = ("mock_idam_token", "mock_uid")
    mock_s2s_manager.get_token.return_value = "mock_s2s_token"

    mock_start.return_value = mock_response(200, {"token": "abc123"})
    mock_validate.return_value = mock_response(201)
    mock_submit.return_value = mock_response(500, text="Server error")

    result = process_case(
        env="sbox",
        caseNo="CASE777",
        payloadData={},
        runId="run123",
        state="paymentPending",
        PR_REFERENCE="2866"
    )

    assert result["Status"] == "ERROR"
    assert result["Error"] is not None


def _make_event(case_no, run_id, state, content):
    """Build a mock EventHubEvent."""
    event = MagicMock()
    event.partition_key = case_no
    event.get_body.return_value = json.dumps(
        {"RunID": run_id, "State": state, "Content": content}
    ).encode("utf-8")
    return event


def _build_trigger_mocks():
    """Shared async infrastructure mocks for eventhub_trigger_active tests."""
    mock_idempotency_blob = AsyncMock()

    mock_idempotency_container = MagicMock()
    mock_idempotency_container.get_blob_client.return_value = mock_idempotency_blob

    mock_blob_svc = MagicMock()
    mock_blob_svc.close = AsyncMock()
    mock_blob_svc.get_container_client.return_value = mock_idempotency_container

    mock_batch = MagicMock()
    mock_batch.__len__ = MagicMock(return_value=1)

    mock_producer = AsyncMock()
    mock_producer.__aenter__ = AsyncMock(return_value=mock_producer)
    mock_producer.__aexit__ = AsyncMock(return_value=None)
    mock_producer.create_batch.return_value = mock_batch

    mock_secret = MagicMock()
    mock_secret.value = "mock_eh_connection_string"

    mock_kv = AsyncMock()
    mock_kv.get_secret.return_value = mock_secret

    return {
        "blob_svc": mock_blob_svc,
        "idempotency_blob": mock_idempotency_blob,
        "producer": mock_producer,
        "batch": mock_batch,
        "kv": mock_kv,
    }


# eventhub_trigger_active: idempotency and result event hub tests

@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_eventhub_trigger_uploads_idempotency_blob_on_success(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer):
    mocks = _build_trigger_mocks()
    mock_process_case.return_value = {
        "Status": "SUCCESS",
        "CaseNo": "CASE123",
        "CCDCaseID": "9876543210",
        "Error": None,
    }
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE123", "run001", "paymentPending", {"key": "val"})]))

    # Idempotency blob uploaded before processing (overwrite=False prevents duplicates)
    mocks["idempotency_blob"].upload_blob.assert_awaited_once_with(b"", overwrite=False)
    # SUCCESS: blob kept to prevent future duplicates
    mocks["idempotency_blob"].delete_blob.assert_not_called()


@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_eventhub_trigger_deletes_idempotency_blob_on_error(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer):
    mocks = _build_trigger_mocks()
    mock_process_case.return_value = {
        "Status": "ERROR",
        "CaseNo": "CASE456",
        "CCDCaseID": None,
        "Error": "Something went wrong",
    }
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE456", "run002", "paymentPending", {"key": "val"})]))

    # Idempotency blob uploaded before processing
    mocks["idempotency_blob"].upload_blob.assert_awaited_once_with(b"", overwrite=False)
    # ERROR: blob deleted so the event can be retried
    mocks["idempotency_blob"].delete_blob.assert_awaited_once()


@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_eventhub_trigger_skips_event_when_blob_upload_fails(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer):
    """When idempotency blob upload raises an exception, the event is skipped."""
    mocks = _build_trigger_mocks()
    mocks["idempotency_blob"].upload_blob.side_effect = Exception("Storage unavailable")
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE789", "run003", "paymentPending", {"key": "val"})]))

    mock_process_case.assert_not_called()


@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_idempotency_blob_not_deleted_when_process_case_raises(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer):
    """When process_case raises, the exception is caught; delete_blob is not reached."""
    mocks = _build_trigger_mocks()
    mock_process_case.side_effect = Exception("unexpected error")
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE999", "run004", "paymentPending", {"key": "val"})]))

    # Blob was uploaded before processing
    mocks["idempotency_blob"].upload_blob.assert_awaited_once_with(b"", overwrite=False)
    # Exception caught by inner except — delete never reached
    mocks["idempotency_blob"].delete_blob.assert_not_called()


@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_process_case_called_with_correct_args(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer):
    """process_case is called with (ENV, caseNo, data, run_id, state, PR_REFERENCE)."""
    mocks = _build_trigger_mocks()
    mock_process_case.return_value = {
        "Status": "SUCCESS",
        "CaseNo": "CASE123",
        "CCDCaseID": "1111",
        "Error": None,
    }
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE123", "run005", "paymentPending", {"key": "val"})]))

    args = mock_process_case.call_args[0]
    assert args[1] == "CASE123"        # caseNo
    assert args[3] == "run005"         # run_id
    assert args[4] == "paymentPending"  # state


@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_idempotency_blob_path_includes_state_and_case_number(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer):
    """get_blob_client is called with a path containing the state and case number."""
    mocks = _build_trigger_mocks()
    mock_process_case.return_value = {"Status": "SUCCESS", "CaseNo": "CASE111", "CCDCaseID": "1", "Error": None}
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE111", "run006", "paymentPending", {"key": "val"})]))

    called_path = mocks["blob_svc"].get_container_client.return_value.get_blob_client.call_args[0][0]
    assert "CASE111" in called_path
    assert "paymentPending" in called_path
    assert "idempotency" in called_path


@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_error_result_sent_even_when_delete_blob_raises(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer):
    """When delete_blob raises on ERROR, the result is still added to the batch and sent."""
    mocks = _build_trigger_mocks()
    mocks["idempotency_blob"].delete_blob.side_effect = Exception("Storage error")
    mock_process_case.return_value = {
        "Status": "ERROR",
        "CaseNo": "CASE_DEL_FAIL",
        "CCDCaseID": None,
        "Error": "Processing failed",
    }
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE_DEL_FAIL", "run010", "paymentPending", {"key": "val"})]))

    mocks["idempotency_blob"].delete_blob.assert_awaited_once()
    mocks["batch"].add.assert_called_once()
    mocks["producer"].send_batch.assert_awaited_once_with(mocks["batch"])


# ---------------------------------------------------------------------------
# _is_retryable unit tests
# ---------------------------------------------------------------------------

def test_is_retryable_true_for_all_retryable_status_codes():
    for code in [408, 409, 429, 500, 502, 503, 504]:
        assert _is_retryable({"Status": "ERROR", "StatusCode": code}) is True, f"Expected retryable for {code}"


def test_is_retryable_false_for_non_retryable_status_codes():
    for code in [200, 201, 400, 401, 403, 404, 422]:
        assert _is_retryable({"Status": "ERROR", "StatusCode": code}) is False, f"Expected non-retryable for {code}"


def test_is_retryable_false_when_status_is_success():
    assert _is_retryable({"Status": "SUCCESS", "StatusCode": 500}) is False


def test_is_retryable_false_when_status_code_is_none():
    assert _is_retryable({"Status": "ERROR", "StatusCode": None}) is False


def test_is_retryable_false_for_non_dict_result():
    assert _is_retryable("error string") is False
    assert _is_retryable(None) is False


# ---------------------------------------------------------------------------
# StatusCode stripping before event hub publish
# ---------------------------------------------------------------------------

@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_status_code_stripped_from_event_hub_payload(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer):
    """StatusCode is an internal routing field and must not appear in the published event."""
    mocks = _build_trigger_mocks()
    mock_process_case.return_value = {
        "Status": "SUCCESS",
        "StatusCode": 201,
        "CaseNo": "CASE_SC",
        "CCDCaseID": "777",
        "Error": None,
    }
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE_SC", "run_sc", "paymentPending", {"key": "val"})]))

    mocks["batch"].add.assert_called_once()
    published_payload = mocks["batch"].add.call_args[0][0].body_as_json()
    assert "StatusCode" not in published_payload


@patch("AzureFunctions.ACTIVE.active_ccd.function_app.wait_exponential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_non_retryable_error_not_retried(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer, mock_wait_exp):
    """process_case is NOT retried when StatusCode is non-retryable (e.g. 422)."""
    from tenacity import wait_none
    mock_wait_exp.return_value = wait_none()

    mocks = _build_trigger_mocks()
    mock_process_case.return_value = {
        "Status": "ERROR", "StatusCode": 422, "CaseNo": "CASE_NO_RETRY", "Error": "Unprocessable"
    }
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE_NO_RETRY", "run_nr", "paymentPending", {"key": "val"})]))

    assert mock_process_case.call_count == 1


@patch("AzureFunctions.ACTIVE.active_ccd.function_app.wait_exponential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_retryable_error_is_retried_3_times(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer, mock_wait_exp):
    """process_case is retried up to stop_after_attempt(3) when StatusCode is retryable."""
    from tenacity import wait_none
    mock_wait_exp.return_value = wait_none()

    mocks = _build_trigger_mocks()
    mock_process_case.return_value = {
        "Status": "ERROR", "StatusCode": 500, "CaseNo": "CASE_R3", "Error": "Server Error"
    }
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE_R3", "run_r3", "paymentPending", {"key": "val"})]))

    assert mock_process_case.call_count == 3


@patch("AzureFunctions.ACTIVE.active_ccd.function_app.wait_exponential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_retryable_error_result_published_and_blob_deleted_after_all_retries_exhausted(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer, mock_wait_exp):
    """After all 3 retries with a retryable code, the error result is published and blob deleted."""
    from tenacity import wait_none
    mock_wait_exp.return_value = wait_none()

    mocks = _build_trigger_mocks()
    mock_process_case.return_value = {
        "Status": "ERROR", "StatusCode": 503, "CaseNo": "CASE_EX", "Error": "Service Unavailable"
    }
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE_EX", "run_ex", "paymentPending", {"key": "val"})]))

    mocks["idempotency_blob"].delete_blob.assert_awaited_once()
    mocks["batch"].add.assert_called_once()
    published_payload = mocks["batch"].add.call_args[0][0].body_as_json()
    assert "StatusCode" not in published_payload
    assert published_payload["Status"] == "ERROR"


@patch("AzureFunctions.ACTIVE.active_ccd.function_app.wait_exponential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_retryable_error_stops_retrying_on_success(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer, mock_wait_exp):
    """When a retryable error is followed by a success, process_case is called exactly twice."""
    from tenacity import wait_none
    mock_wait_exp.return_value = wait_none()

    mocks = _build_trigger_mocks()
    mock_process_case.side_effect = [
        {"Status": "ERROR", "StatusCode": 502, "CaseNo": "CASE_OK2", "Error": "Bad Gateway"},
        {"Status": "SUCCESS", "StatusCode": 201, "CaseNo": "CASE_OK2", "CCDCaseID": "999", "Error": None},
    ]
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE_OK2", "run_ok2", "paymentPending", {"key": "val"})]))

    assert mock_process_case.call_count == 2
    mocks["idempotency_blob"].delete_blob.assert_not_called()
    mocks["batch"].add.assert_called_once()
