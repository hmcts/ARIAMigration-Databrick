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
    from AzureFunctions.ACTIVE.active_ccd.function_app import eventhub_trigger_active

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
    mock.json.return_values = json_data or {}
    return mock


# Succesfully sent a case payload
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.idam_token_mgr")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.S2S_Manager")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.submit_case")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.validate_case")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_funciton_success(mock_start_case_creation_response,
                                  mock_validate_case_response,
                                  mock_submit_case_response,
                                  mock_s2s_class,
                                  mock_idam_mgr):

    mock_idam_mgr.get_token.return_value = ("mock_idam_token", "mock_uid")
    mock_s2s_inst = MagicMock()
    mock_s2s_inst.get_token.return_value = "mock_s2s_token"
    mock_s2s_class.return_value = mock_s2s_inst

    mock_start_case_creation = mock_response(200, {"token": "ABC123"})
    mock_validate_case = mock_response(201)
    mock_submit_case = mock_response(201)
    mock_submit_case.json = lambda: {"id": "1234567891011"}

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

# failed to start case creation


@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.idam_token_mgr")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.S2S_Manager")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_case_url_uses_pr_number(mock_start_case_creation, mock_s2s_class, mock_idam_mgr):
    mock_idam_mgr.get_token.return_value = ("mock_idam_token", "mock_uid")
    mock_s2s_inst = MagicMock()
    mock_s2s_inst.get_token.return_value = "mock_s2s_token"
    mock_s2s_class.return_value = mock_s2s_inst

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
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.S2S_Manager")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_case_start_case_fail(mock_start_case_creation, mock_s2s_class, mock_idam_mgr):
    mock_idam_mgr.get_token.return_value = ("mock_idam_token", "mock_uid")
    mock_s2s_inst = MagicMock()
    mock_s2s_inst.get_token.return_value = "mock_s2s_token"
    mock_s2s_class.return_value = mock_s2s_inst

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
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.S2S_Manager")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.validate_case")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_case_validation_fails(mock_start, mock_validate, mock_s2s_class, mock_idam_mgr):
    mock_idam_mgr.get_token.return_value = ("mock_idam_token", "mock_uid")
    mock_s2s_inst = MagicMock()
    mock_s2s_inst.get_token.return_value = "mock_s2s_token"
    mock_s2s_class.return_value = mock_s2s_inst

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
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.S2S_Manager")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.submit_case")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.validate_case")
@patch("AzureFunctions.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_case_submission_fails(mock_start, mock_validate, mock_submit, mock_s2s_class, mock_idam_mgr):
    mock_idam_mgr.get_token.return_value = ("mock_idam_token", "mock_uid")
    mock_s2s_inst = MagicMock()
    mock_s2s_inst.get_token.return_value = "mock_s2s_token"
    mock_s2s_class.return_value = mock_s2s_inst

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
    mock_idempotency_blob.exists.return_value = False

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

    mocks["idempotency_blob"].upload_blob.assert_awaited_once_with(b"", overwrite=True)


@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_eventhub_trigger_no_idempotency_upload_on_error(
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

    mocks["idempotency_blob"].upload_blob.assert_not_called()


@patch("AzureFunctions.ACTIVE.active_ccd.function_app.EventHubProducerClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.BlobServiceClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.SecretClient")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.DefaultAzureCredential")
@patch("AzureFunctions.ACTIVE.active_ccd.function_app.process_case")
def test_eventhub_trigger_skips_duplicate_event(
        mock_process_case, mock_credential, mock_secret_client,
        mock_blob_service, mock_eh_producer):
    mocks = _build_trigger_mocks()
    mocks["idempotency_blob"].exists.return_value = True
    mock_credential.return_value = AsyncMock()
    mock_secret_client.return_value = mocks["kv"]
    mock_blob_service.return_value = mocks["blob_svc"]
    mock_eh_producer.from_connection_string.return_value = mocks["producer"]

    asyncio.run(eventhub_trigger_active([_make_event("CASE789", "run003", "paymentPending", {"key": "val"})]))

    mock_process_case.assert_not_called()
