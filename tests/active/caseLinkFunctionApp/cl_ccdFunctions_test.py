import json
import pytest
from unittest.mock import patch, MagicMock, ANY

# Patch Azure SDK clients before the module-level IDAMTokenManager(env="sbox")
# instantiation runs, so that importing cl_ccdFunctions does not hit Key Vault.
with patch("azure.identity.DefaultAzureCredential"), \
     patch("azure.keyvault.secrets.SecretClient") as _mock_kv_cls:
    _mock_kv_cls.return_value.get_secret.return_value.value = "fake-secret"
    from AzureFunctions.ACTIVE.active_caselink_ccd.cl_ccdFunctions import (
        get_case_details,
        start_case_event,
        validate_case,
        submit_case_event,
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


COMMON = dict(
    ccd_base_url="https://ccd-api.test.net",
    uid="uid123",
    jid="IA",
    ctid="Asylum",
    cid="1234567890123456",
    etid="createCaseLink",
    idam_token="idam-token",
    s2s_token="s2s-token",
)

EXPECTED_HEADERS = {
    "Authorization": f"Bearer {COMMON['idam_token']}",
    "ServiceAuthorization": COMMON["s2s_token"],
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Keys required by get_case_details (no etid)
CASE_DETAILS_COMMON = {k: COMMON[k] for k in ("ccd_base_url", "uid", "jid", "ctid", "cid", "idam_token", "s2s_token")}

# A non-empty caseLinks list used as a "different from payload" sentinel in tests
EXISTING_LINKS = [{"id": "existing-link"}]


# ---------------------------------------------------------------------------
# get_case_details
# ---------------------------------------------------------------------------

@patch("requests.get")
def test_get_case_details_success(mock_get):
    mock_get.return_value = mock_response(200, {"id": "123", "case_data": {}})

    response = get_case_details(**CASE_DETAILS_COMMON)

    expected_url = (
        f"{COMMON['ccd_base_url']}/caseworkers/{COMMON['uid']}"
        f"/jurisdictions/{COMMON['jid']}/case-types/{COMMON['ctid']}"
        f"/cases/{COMMON['cid']}"
    )
    mock_get.assert_called_once_with(expected_url, headers=EXPECTED_HEADERS)
    assert response.status_code == 200


@patch("requests.get")
def test_get_case_details_network_error(mock_get):
    mock_get.side_effect = Exception("Connection refused")

    response = get_case_details(**CASE_DETAILS_COMMON)

    assert response is None


# ---------------------------------------------------------------------------
# start_case_event
# ---------------------------------------------------------------------------

@patch("requests.get")
def test_start_case_event_success(mock_get):
    mock_get.return_value = mock_response(200, {"token": "event-token-abc"})

    response = start_case_event(**COMMON)

    expected_url = (
        f"{COMMON['ccd_base_url']}/caseworkers/{COMMON['uid']}"
        f"/jurisdictions/{COMMON['jid']}/case-types/{COMMON['ctid']}"
        f"/cases/{COMMON['cid']}/event-triggers/{COMMON['etid']}/token"
    )
    mock_get.assert_called_once_with(expected_url, headers=EXPECTED_HEADERS)
    assert response.status_code == 200


@patch("requests.get")
def test_start_case_event_non_200(mock_get):
    mock_get.return_value = mock_response(403, text="Forbidden")

    response = start_case_event(**COMMON)

    assert response.status_code == 403


@patch("requests.get")
def test_start_case_event_network_error(mock_get):
    mock_get.side_effect = Exception("Connection refused")

    response = start_case_event(**COMMON)

    assert response is None


# ---------------------------------------------------------------------------
# validate_case
# ---------------------------------------------------------------------------

VALIDATE_COMMON = dict(
    ccd_base_url=COMMON["ccd_base_url"],
    uid=COMMON["uid"],
    jid=COMMON["jid"],
    ctid=COMMON["ctid"],
    cid=COMMON["cid"],
    etid=COMMON["etid"],
    event_token="event-token-abc",
    payloadData={"caseLinks": [{"value": {"CaseReference": "9999999999999999"}}]},
    idam_token=COMMON["idam_token"],
    s2s_token=COMMON["s2s_token"],
)


@patch("requests.post")
def test_validate_case_success(mock_post):
    mock_post.return_value = mock_response(200)

    response = validate_case(**VALIDATE_COMMON)

    expected_url = (
        f"{COMMON['ccd_base_url']}/caseworkers/{COMMON['uid']}"
        f"/jurisdictions/{COMMON['jid']}/case-types/{COMMON['ctid']}/validate"
    )
    mock_post.assert_called_once_with(expected_url, headers=EXPECTED_HEADERS, json=ANY)
    assert response.status_code == 200


@patch("requests.post")
def test_validate_case_returns_201(mock_post):
    mock_post.return_value = mock_response(201)

    response = validate_case(**VALIDATE_COMMON)

    assert response.status_code == 201


@patch("requests.post")
def test_validate_case_json_payload_built_correctly(mock_post):
    mock_post.return_value = mock_response(200)

    validate_case(**VALIDATE_COMMON)

    _, kwargs = mock_post.call_args
    body = kwargs["json"]
    assert body["event"]["id"] == COMMON["etid"]
    assert body["event_token"] == VALIDATE_COMMON["event_token"]
    assert body["ignore_warning"] is True
    assert body["data"] == VALIDATE_COMMON["payloadData"]


@patch("requests.post")
def test_validate_case_str_payload_is_parsed(mock_post):
    mock_post.return_value = mock_response(200)

    str_payload = json.dumps({"key": "val"})
    params = {**VALIDATE_COMMON, "payloadData": str_payload}
    validate_case(**params)

    _, kwargs = mock_post.call_args
    assert kwargs["json"]["data"] == {"key": "val"}


@patch("requests.post")
def test_validate_case_network_error(mock_post):
    mock_post.side_effect = Exception("Network error")

    response = validate_case(**VALIDATE_COMMON)

    assert response is None


# ---------------------------------------------------------------------------
# submit_case_event
# ---------------------------------------------------------------------------

SUBMIT_COMMON = dict(
    ccd_base_url=COMMON["ccd_base_url"],
    uid=COMMON["uid"],
    jid=COMMON["jid"],
    ctid=COMMON["ctid"],
    cid=COMMON["cid"],
    etid=COMMON["etid"],
    event_token="event-token-abc",
    payloadData={"caseLinks": [{"value": {"CaseReference": "9999999999999999"}}]},
    idam_token=COMMON["idam_token"],
    s2s_token=COMMON["s2s_token"],
)


@patch("requests.post")
def test_submit_case_event_success(mock_post):
    mock_post.return_value = mock_response(201, {"id": "9876543210987654", "link": 1})

    response = submit_case_event(**SUBMIT_COMMON)

    expected_url = (
        f"{COMMON['ccd_base_url']}/caseworkers/{COMMON['uid']}"
        f"/jurisdictions/{COMMON['jid']}/case-types/{COMMON['ctid']}"
        f"/cases/{COMMON['cid']}/events"
    )
    mock_post.assert_called_once_with(expected_url, headers=EXPECTED_HEADERS, json=ANY)
    assert response.status_code == 201


@patch("requests.post")
def test_submit_case_event_json_payload_built_correctly(mock_post):
    mock_post.return_value = mock_response(201, {"id": "123", "link": 2})

    submit_case_event(**SUBMIT_COMMON)

    _, kwargs = mock_post.call_args
    body = kwargs["json"]
    assert body["event"]["id"] == COMMON["etid"]
    assert body["event_token"] == SUBMIT_COMMON["event_token"]
    assert body["ignore_warning"] is True
    assert body["data"] == SUBMIT_COMMON["payloadData"]


@patch("requests.post")
def test_submit_case_event_str_payload_is_parsed(mock_post):
    mock_post.return_value = mock_response(201, {"id": "123", "link": 1})

    str_payload = json.dumps({"key": "val"})
    params = {**SUBMIT_COMMON, "payloadData": str_payload}
    submit_case_event(**params)

    _, kwargs = mock_post.call_args
    assert kwargs["json"]["data"] == {"key": "val"}


@patch("requests.post")
def test_submit_case_event_network_error(mock_post):
    mock_post.side_effect = Exception("Timeout")

    response = submit_case_event(**SUBMIT_COMMON)

    assert response is None


# ---------------------------------------------------------------------------
# process_event
# ---------------------------------------------------------------------------

PROCESS_DEFAULTS = dict(
    env="sbox",
    ccdReference="1234567890123456",
    caseLinkPayload={
        "link1": {"CaseReference": "111"},
        "link2": {"CaseReference": "222"},
        "link3": {"CaseReference": "333"},
    },
    runId="run-001",
    PR_REFERENCE="1234",
)

MODULE = "AzureFunctions.ACTIVE.active_caselink_ccd.cl_ccdFunctions"
SLEEP_PATH = "AzureFunctions.ACTIVE.active_caselink_ccd.retry_decorator.time.sleep"


@pytest.fixture(autouse=True)
def no_retry_sleep():
    """Suppress retry backoff sleeps in all process_event tests."""
    with patch(SLEEP_PATH):
        yield


@pytest.fixture
def mock_token_managers_cl():
    """Patch the module-level idam_token_mgr instance and S2S_Manager."""
    with patch(f"{MODULE}.idam_token_mgr") as mock_idam, \
         patch(f"{MODULE}.S2S_Manager") as mock_s2s_cls:

        mock_idam.get_token.return_value = ("mock-idam-token", "uid-001")

        mock_s2s_inst = MagicMock()
        mock_s2s_inst.get_token.return_value = "mock-s2s-token"
        mock_s2s_cls.return_value = mock_s2s_inst

        yield


@pytest.mark.usefixtures("mock_token_managers_cl")
@patch(f"{MODULE}.submit_case_event")
@patch(f"{MODULE}.validate_case")
@patch(f"{MODULE}.start_case_event")
@patch(f"{MODULE}.get_case_details")
def test_process_event_success(mock_get_case, mock_start, mock_validate, mock_submit):
    mock_get_case.return_value = mock_response(200, {"case_data": {"caseLinks": EXISTING_LINKS}})
    mock_start.return_value = mock_response(200, {"token": "tok123"})
    mock_validate.return_value = mock_response(200)
    mock_submit.return_value = mock_response(
        201,
        {"id": "9876543210987654", "case_data": {"caseLinks": [{}, {}, {}]}},
    )

    result = process_event(**PROCESS_DEFAULTS)

    assert result["Status"] == "SUCCESS"
    assert result["CaseLinkCount"] == 3
    assert result["Error"] is None
    assert result["CCDCaseReferenceNumber"] == PROCESS_DEFAULTS["ccdReference"]
    assert result["RunID"] == PROCESS_DEFAULTS["runId"]


@pytest.mark.usefixtures("mock_token_managers_cl")
@patch(f"{MODULE}.start_case_event")
@patch(f"{MODULE}.get_case_details")
def test_process_event_start_fails_non_200(mock_get_case, mock_start):
    mock_get_case.return_value = mock_response(200, {"case_data": {"caseLinks": EXISTING_LINKS}})
    mock_start.return_value = mock_response(401, text="Unauthorized")

    result = process_event(**PROCESS_DEFAULTS)

    assert result["Status"] == "ERROR"
    assert "401" in result["Error"]
    assert result["CaseLinkCount"] == 0


@pytest.mark.usefixtures("mock_token_managers_cl")
@patch(f"{MODULE}.start_case_event")
@patch(f"{MODULE}.get_case_details")
def test_process_event_start_returns_none(mock_get_case, mock_start):
    mock_get_case.return_value = mock_response(200, {"case_data": {"caseLinks": EXISTING_LINKS}})
    mock_start.return_value = None

    result = process_event(**PROCESS_DEFAULTS)

    assert result["Status"] == "ERROR"
    assert result["CaseLinkCount"] == 0


@pytest.mark.usefixtures("mock_token_managers_cl")
@patch(f"{MODULE}.validate_case")
@patch(f"{MODULE}.start_case_event")
@patch(f"{MODULE}.get_case_details")
def test_process_event_validation_fails(mock_get_case, mock_start, mock_validate):
    mock_get_case.return_value = mock_response(200, {"case_data": {"caseLinks": EXISTING_LINKS}})
    mock_start.return_value = mock_response(200, {"token": "tok123"})
    mock_validate.return_value = mock_response(400, text="Bad payload")

    result = process_event(**PROCESS_DEFAULTS)

    assert result["Status"] == "ERROR"
    assert result["CaseLinkCount"] == 0
    assert "400" in result["Error"]


@pytest.mark.usefixtures("mock_token_managers_cl")
@patch(f"{MODULE}.submit_case_event")
@patch(f"{MODULE}.validate_case")
@patch(f"{MODULE}.start_case_event")
@patch(f"{MODULE}.get_case_details")
def test_process_event_success_null_case_data(mock_get_case, mock_start, mock_validate, mock_submit):
    """CaseLinkCount should be 0 when case_data is explicitly null in the response."""
    mock_get_case.return_value = mock_response(200, {"case_data": {"caseLinks": EXISTING_LINKS}})
    mock_start.return_value = mock_response(200, {"token": "tok123"})
    mock_validate.return_value = mock_response(200)
    mock_submit.return_value = mock_response(201, {"id": "9876543210987654", "case_data": None})

    result = process_event(**PROCESS_DEFAULTS)

    assert result["Status"] == "SUCCESS"
    assert result["CaseLinkCount"] == 0


@pytest.mark.usefixtures("mock_token_managers_cl")
@patch(f"{MODULE}.submit_case_event")
@patch(f"{MODULE}.validate_case")
@patch(f"{MODULE}.start_case_event")
@patch(f"{MODULE}.get_case_details")
def test_process_event_submission_fails(mock_get_case, mock_start, mock_validate, mock_submit):
    mock_get_case.return_value = mock_response(200, {"case_data": {"caseLinks": EXISTING_LINKS}})
    mock_start.return_value = mock_response(200, {"token": "tok123"})
    mock_validate.return_value = mock_response(200)
    mock_submit.return_value = mock_response(500, text="Server error")

    result = process_event(**PROCESS_DEFAULTS)

    assert result["Status"] == "ERROR"
    assert result["CaseLinkCount"] == 0
    assert "500" in result["Error"]


@pytest.mark.usefixtures("mock_token_managers_cl")
@patch(f"{MODULE}.submit_case_event")
@patch(f"{MODULE}.validate_case")
@patch(f"{MODULE}.start_case_event")
@patch(f"{MODULE}.get_case_details")
def test_process_event_submission_returns_none(mock_get_case, mock_start, mock_validate, mock_submit):
    mock_get_case.return_value = mock_response(200, {"case_data": {"caseLinks": EXISTING_LINKS}})
    mock_start.return_value = mock_response(200, {"token": "tok123"})
    mock_validate.return_value = mock_response(200)
    mock_submit.return_value = None

    result = process_event(**PROCESS_DEFAULTS)

    assert result["Status"] == "ERROR"
    assert result["CaseLinkCount"] == 0


def test_process_event_invalid_env():
    with patch(f"{MODULE}.idam_token_mgr") as mock_idam, \
         patch(f"{MODULE}.S2S_Manager") as mock_s2s_cls:

        mock_idam.get_token.return_value = ("tok", "uid")
        mock_s2s_inst = MagicMock()
        mock_s2s_inst.get_token.return_value = "s2s"
        mock_s2s_cls.return_value = mock_s2s_inst

        with pytest.raises(ValueError, match="Invalid environment"):
            process_event(
                env="invalid_env",
                ccdReference="1234567890123456",
                caseLinkPayload=[],
                runId="run-001",
                PR_REFERENCE="1234",
            )


def test_process_event_idam_token_failure():
    with patch(f"{MODULE}.idam_token_mgr") as mock_idam:
        mock_idam.get_token.side_effect = Exception("IDAM unreachable")

        result = process_event(**PROCESS_DEFAULTS)

        assert result["Status"] == "ERROR"
        assert result["CaseLinkCount"] == 0


def test_process_event_s2s_token_failure():
    with patch(f"{MODULE}.idam_token_mgr") as mock_idam, \
         patch(f"{MODULE}.S2S_Manager") as mock_s2s_cls:

        mock_idam.get_token.return_value = ("tok", "uid")
        mock_s2s_cls.side_effect = Exception("S2S unreachable")

        result = process_event(**PROCESS_DEFAULTS)

        assert result["Status"] == "ERROR"
        assert result["CaseLinkCount"] == 0


@pytest.mark.usefixtures("mock_token_managers_cl")
@patch(f"{MODULE}.submit_case_event")
@patch(f"{MODULE}.validate_case")
@patch(f"{MODULE}.start_case_event")
@patch(f"{MODULE}.get_case_details")
def test_process_event_url_uses_pr_number(mock_get_case, mock_start, mock_validate, mock_submit):
    mock_get_case.return_value = mock_response(200, {"case_data": {"caseLinks": EXISTING_LINKS}})
    mock_start.return_value = mock_response(200, {"token": "tok123"})
    mock_validate.return_value = mock_response(200)
    mock_submit.return_value = mock_response(201, {"id": "9876543210987654", "link": 1})

    process_event(**PROCESS_DEFAULTS)

    called_url = mock_start.call_args[0][0]
    assert PROCESS_DEFAULTS["PR_REFERENCE"] in called_url
    assert "ynot-pr-1" not in called_url


@pytest.mark.usefixtures("mock_token_managers_cl")
@patch(f"{MODULE}.submit_case_event")
@patch(f"{MODULE}.validate_case")
@patch(f"{MODULE}.start_case_event")
@patch(f"{MODULE}.get_case_details")
def test_process_event_no_notification_flag_in_payload(mock_get_case, mock_start, mock_validate, mock_submit):
    mock_get_case.return_value = mock_response(200, {"case_data": {"caseLinks": EXISTING_LINKS}})
    mock_start.return_value = mock_response(200, {"token": "tok123"})
    mock_validate.return_value = mock_response(200)
    mock_submit.return_value = mock_response(201, {"id": "9876543210987654", "link": 1})

    process_event(**PROCESS_DEFAULTS)

    # payloadData is the 8th positional arg to validate_case
    full_case_data = mock_validate.call_args[0][7]
    assert "isNotificationTurnedOff" not in full_case_data


# ---------------------------------------------------------------------------
# process_event — overwrite / SKIP behaviour
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("mock_token_managers_cl")
@patch(f"{MODULE}.start_case_event")
@patch(f"{MODULE}.get_case_details")
def test_process_event_skips_when_case_links_match(mock_get_case, mock_start):
    """When existing caseLinks match the payload and overwrite=False, SKIP is returned."""
    case_links = [{"value": {"CaseReference": "1111111111111111"}}]
    mock_get_case.return_value = mock_response(200, {"case_data": {"caseLinks": case_links}})

    result = process_event(
        env="sbox",
        ccdReference="1234567890123456",
        runId="run-001",
        caseLinkPayload={"caseLinks": case_links},
        PR_REFERENCE="1234",
        overwrite=False,
    )

    assert result["Status"] == "SKIPPED"
    assert result["CaseLinkCount"] == len(case_links)
    mock_start.assert_not_called()


@pytest.mark.usefixtures("mock_token_managers_cl")
@patch(f"{MODULE}.submit_case_event")
@patch(f"{MODULE}.validate_case")
@patch(f"{MODULE}.start_case_event")
@patch(f"{MODULE}.get_case_details")
def test_process_event_proceeds_when_case_links_differ(mock_get_case, mock_start, mock_validate, mock_submit):
    """When existing caseLinks differ from the payload, processing continues normally."""
    existing_links = [{"value": {"CaseReference": "1111111111111111"}}]
    new_links = [{"value": {"CaseReference": "2222222222222222"}}]
    mock_get_case.return_value = mock_response(200, {"case_data": {"caseLinks": existing_links}})
    mock_start.return_value = mock_response(200, {"token": "tok123"})
    mock_validate.return_value = mock_response(200)
    mock_submit.return_value = mock_response(201, {"id": "9876543210987654", "case_data": {"caseLinks": new_links}})

    result = process_event(
        env="sbox",
        ccdReference="1234567890123456",
        runId="run-001",
        caseLinkPayload={"caseLinks": new_links},
        PR_REFERENCE="1234",
        overwrite=False,
    )

    assert result["Status"] == "SUCCESS"
    mock_start.assert_called_once()


@pytest.mark.usefixtures("mock_token_managers_cl")
@patch(f"{MODULE}.submit_case_event")
@patch(f"{MODULE}.validate_case")
@patch(f"{MODULE}.start_case_event")
@patch(f"{MODULE}.get_case_details")
def test_process_event_overwrite_skips_case_details_check(mock_get_case, mock_start, mock_validate, mock_submit):
    """When overwrite=True, get_case_details is not called and processing always proceeds."""
    mock_start.return_value = mock_response(200, {"token": "tok123"})
    mock_validate.return_value = mock_response(200)
    mock_submit.return_value = mock_response(201, {"id": "9876543210987654", "case_data": {"caseLinks": []}})

    result = process_event(**{**PROCESS_DEFAULTS, "overwrite": True})

    mock_get_case.assert_not_called()
    assert result["Status"] == "SUCCESS"
