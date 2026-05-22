import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

# Patch Azure SDK clients before the module-level IDAMTokenManager(env="sbox")
# instantiation runs, so that importing cdamFunctions does not hit Key Vault.
with patch("azure.identity.DefaultAzureCredential"), \
     patch("azure.keyvault.secrets.SecretClient") as _mock_kv_cls:
    _mock_kv_cls.return_value.get_secret.return_value.value = "fake-secret"
    from AzureFunctions.ACTIVE.active_cdam.cdamFunctions import (
        upload_document,
        process_event,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def mock_response(status_code, json_data=None, text=""):
    mock = MagicMock()
    mock.status_code = status_code
    mock.text = text
    mock.json.return_value = json_data or {}
    return mock


MODULE = "AzureFunctions.ACTIVE.active_cdam.cdamFunctions"

UPLOAD_COMMON = dict(
    cdam_base_url="http://ccd-case-document-am-api-aat.service.core-compute-aat.internal",
    jid="IA",
    ctid="Asylum",
    cid="1234567890123456",
    file_name="test_document.html",
    doc_binary=b"<html>document</html>",
    content_type="text/html",
    idam_token="idam-token",
    s2s_token="s2s-token",
)

PROCESS_DEFAULTS = dict(
    env="sbox",
    caseNo="1234567890123456",
    runId="run-001",
    file_name="test.html",
    file_url="https://mystorageaccount.blob.core.windows.net/mycontainer/path/to/test.html",
    file_content_type="text/html",
    storage_credential=MagicMock(),
)


# ---------------------------------------------------------------------------
# upload_document
# ---------------------------------------------------------------------------

@patch("requests.post")
def test_upload_document_success_returns_response(mock_post):
    mock_post.return_value = mock_response(201, {"documents": [{"id": "doc-123"}]})

    response = upload_document(**UPLOAD_COMMON)

    assert response.status_code == 201


@patch("requests.post")
def test_upload_document_posts_to_correct_url(mock_post):
    mock_post.return_value = mock_response(201)

    upload_document(**UPLOAD_COMMON)

    expected_url = f"{UPLOAD_COMMON['cdam_base_url']}/cases/documents"
    mock_post.assert_called_once()
    assert mock_post.call_args.args[0] == expected_url


@patch("requests.post")
def test_upload_document_sends_correct_auth_headers(mock_post):
    mock_post.return_value = mock_response(201)

    upload_document(**UPLOAD_COMMON)

    headers = mock_post.call_args.kwargs["headers"]
    assert headers["Authorization"] == f"Bearer {UPLOAD_COMMON['idam_token']}"
    assert headers["ServiceAuthorization"] == UPLOAD_COMMON["s2s_token"]


@patch("requests.post")
def test_upload_document_sends_correct_classification_body(mock_post):
    mock_post.return_value = mock_response(201)

    upload_document(**UPLOAD_COMMON)

    body = mock_post.call_args.kwargs["data"]
    assert body["classification"] == "PUBLIC"
    assert body["caseTypeId"] == UPLOAD_COMMON["ctid"]
    assert body["jurisdictionId"] == UPLOAD_COMMON["jid"]


@patch("requests.post")
def test_upload_document_includes_file_in_request(mock_post):
    mock_post.return_value = mock_response(201)

    upload_document(**UPLOAD_COMMON)

    files = mock_post.call_args.kwargs["files"]
    assert len(files) == 1
    field_name, (name, binary, content_type) = files[0]
    assert field_name == "files"
    assert name == UPLOAD_COMMON["file_name"]
    assert binary == UPLOAD_COMMON["doc_binary"]
    assert content_type == UPLOAD_COMMON["content_type"]


@patch("requests.post")
def test_upload_document_uses_provided_content_type(mock_post):
    mock_post.return_value = mock_response(201)

    upload_document(**{**UPLOAD_COMMON, "content_type": "application/pdf"})

    files = mock_post.call_args.kwargs["files"]
    _field, (_name, _binary, ct) = files[0]
    assert ct == "application/pdf"


@patch("requests.post", side_effect=Exception("Connection refused"))
def test_upload_document_returns_none_on_network_error(mock_post):
    response = upload_document(**UPLOAD_COMMON)

    assert response is None


@patch("requests.post")
def test_upload_document_returns_non_201_response(mock_post):
    mock_post.return_value = mock_response(400, text="Bad Request")

    response = upload_document(**UPLOAD_COMMON)

    assert response.status_code == 400


# ---------------------------------------------------------------------------
# process_event fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def no_retry_sleep():
    with patch("asyncio.sleep", new_callable=AsyncMock):
        yield


@pytest.fixture
def mock_token_managers():
    """Patch the module-level idam_token_mgr and s2s_manager instances."""
    with patch(f"{MODULE}.idam_token_mgr") as mock_idam, \
         patch(f"{MODULE}.s2s_manager") as mock_s2s:

        mock_idam.get_token.return_value = ("mock-idam-token", "uid-001")
        mock_s2s.get_token.return_value = "mock-s2s-token"

        yield mock_idam, mock_s2s


@pytest.fixture
def mock_blob_client():
    """Patch BlobServiceClient to return fake document binary."""
    with patch(f"{MODULE}.BlobServiceClient") as mock_blob_cls:
        mock_blob_instance = MagicMock()
        mock_blob_cls.return_value = mock_blob_instance
        mock_blob_instance.get_blob_client.return_value.readall.return_value = b"<html>doc</html>"
        yield mock_blob_cls


# ---------------------------------------------------------------------------
# process_event — token acquisition
# ---------------------------------------------------------------------------

def test_process_event_idam_token_failure_returns_error():
    with patch(f"{MODULE}.idam_token_mgr") as mock_idam:
        mock_idam.get_token.side_effect = Exception("IDAM unreachable")

        result = asyncio.run(process_event(**PROCESS_DEFAULTS))

    assert result["Status"] == "ERROR"
    assert "IDAM" in result["Error"] or "s2s" in result["Error"]
    assert result["CaseNo"] == PROCESS_DEFAULTS["caseNo"]
    assert result["RunID"] == PROCESS_DEFAULTS["runId"]


def test_process_event_s2s_token_failure_returns_error():
    with patch(f"{MODULE}.idam_token_mgr") as mock_idam, \
         patch(f"{MODULE}.s2s_manager") as mock_s2s:

        mock_idam.get_token.return_value = ("tok", "uid")
        mock_s2s.get_token.side_effect = Exception("S2S unreachable")

        result = asyncio.run(process_event(**PROCESS_DEFAULTS))

    assert result["Status"] == "ERROR"
    assert result["CaseNo"] == PROCESS_DEFAULTS["caseNo"]


# ---------------------------------------------------------------------------
# process_event — invalid environment
# ---------------------------------------------------------------------------

def test_process_event_invalid_env_raises_value_error(mock_token_managers, mock_blob_client):
    with pytest.raises(ValueError, match="Invalid environment"):
        asyncio.run(process_event(
            env="invalid_env",
            caseNo="1234567890123456",
            runId="run-001",
            file_name="doc.html",
            file_url="https://storage.blob.core.windows.net/container/blob",
            file_content_type="text/html",
            storage_credential=MagicMock(),
        ))


# ---------------------------------------------------------------------------
# process_event — blob read failure
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("mock_token_managers")
def test_process_event_blob_read_failure_returns_error():
    with patch(f"{MODULE}.BlobServiceClient") as mock_blob_cls:
        mock_blob_cls.side_effect = Exception("Blob storage unavailable")

        result = asyncio.run(process_event(**PROCESS_DEFAULTS))

    assert result["Status"] == "ERROR"
    assert "blob" in result["Error"].lower() or "Failed" in result["Error"]
    assert result["CaseNo"] == PROCESS_DEFAULTS["caseNo"]


# ---------------------------------------------------------------------------
# process_event — upload failure
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("mock_token_managers")
@patch(f"{MODULE}.upload_document")
def test_process_event_upload_returns_none_returns_error(mock_upload, mock_blob_client):
    mock_upload.return_value = None

    result = asyncio.run(process_event(**PROCESS_DEFAULTS))

    assert result["Status"] == "ERROR"
    assert "No response" in result["Error"]


@pytest.mark.usefixtures("mock_token_managers")
@patch(f"{MODULE}.upload_document")
def test_process_event_upload_non_2xx_returns_error(mock_upload, mock_blob_client):
    mock_upload.return_value = mock_response(500, text="Internal Server Error")

    result = asyncio.run(process_event(**PROCESS_DEFAULTS))

    assert result["Status"] == "ERROR"
    assert "500" in result["Error"]


@pytest.mark.usefixtures("mock_token_managers")
@patch(f"{MODULE}.upload_document")
def test_process_event_upload_400_returns_error(mock_upload, mock_blob_client):
    mock_upload.return_value = mock_response(400, text="Bad Request")

    result = asyncio.run(process_event(**PROCESS_DEFAULTS))

    assert result["Status"] == "ERROR"
    assert "400" in result["Error"]


# ---------------------------------------------------------------------------
# process_event — success
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("mock_token_managers")
@patch(f"{MODULE}.upload_document")
def test_process_event_success_with_201(mock_upload, mock_blob_client):
    doc_href = "http://cdam.example.com/documents/doc-abc"
    mock_upload.return_value = mock_response(
        201,
        json_data={"links": {"self": {"href": doc_href}}},
    )

    result = asyncio.run(process_event(**PROCESS_DEFAULTS))

    assert result["Status"] == "SUCCESS"
    assert result["Error"] is None
    assert result["CaseNo"] == PROCESS_DEFAULTS["caseNo"]
    assert result["RunID"] == PROCESS_DEFAULTS["runId"]


@pytest.mark.usefixtures("mock_token_managers")
@patch(f"{MODULE}.upload_document")
def test_process_event_success_with_200(mock_upload, mock_blob_client):
    doc_href = "http://cdam.example.com/documents/doc-abc"
    mock_upload.return_value = mock_response(
        200,
        json_data={"links": {"self": {"href": doc_href}}},
    )

    result = asyncio.run(process_event(**PROCESS_DEFAULTS))

    assert result["Status"] == "SUCCESS"


@pytest.mark.usefixtures("mock_token_managers")
@patch(f"{MODULE}.upload_document")
def test_process_event_result_contains_cdam_response(mock_upload, mock_blob_client):
    cdam_resp = {"links": {"self": {"href": "http://example.com/doc"}}}
    mock_upload.return_value = mock_response(201, json_data=cdam_resp)

    result = asyncio.run(process_event(**PROCESS_DEFAULTS))

    assert result["CDAMResponse"] == cdam_resp


@pytest.mark.usefixtures("mock_token_managers")
@patch(f"{MODULE}.upload_document")
def test_process_event_passes_idam_token_string_not_tuple(mock_upload, mock_blob_client):
    """idam_token passed to upload_document must be a plain string, not a tuple."""
    mock_upload.return_value = mock_response(
        201,
        json_data={"links": {"self": {"href": "http://example.com/doc"}}},
    )

    asyncio.run(process_event(**PROCESS_DEFAULTS))

    # upload_document is called positionally; idam_token is at index 7 after the
    # new content_type arg was inserted at index 6.
    call_args = mock_upload.call_args.args
    idam_token_arg = call_args[7]
    assert isinstance(idam_token_arg, str), (
        f"Expected idam_token to be a str, got {type(idam_token_arg)}: {idam_token_arg!r}"
    )


@pytest.mark.usefixtures("mock_token_managers")
@patch(f"{MODULE}.upload_document")
def test_process_event_forwards_file_content_type(mock_upload, mock_blob_client):
    """file_content_type from the event payload must be forwarded to upload_document."""
    mock_upload.return_value = mock_response(
        201,
        json_data={"links": {"self": {"href": "http://example.com/doc"}}},
    )

    asyncio.run(process_event(**{**PROCESS_DEFAULTS, "file_content_type": "application/pdf"}))

    call_args = mock_upload.call_args.args
    # content_type is the 7th positional arg (index 6)
    assert call_args[6] == "application/pdf"


@pytest.mark.usefixtures("mock_token_managers")
@patch(f"{MODULE}.upload_document")
def test_process_event_result_has_start_and_end_datetime(mock_upload, mock_blob_client):
    mock_upload.return_value = mock_response(
        201,
        json_data={"links": {"self": {"href": "http://example.com/doc"}}},
    )

    result = asyncio.run(process_event(**PROCESS_DEFAULTS))

    assert "StartDateTime" in result
    assert "EndDateTime" in result
    assert result["StartDateTime"] is not None
    assert result["EndDateTime"] is not None
