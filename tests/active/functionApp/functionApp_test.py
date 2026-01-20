import pytest
from unittest.mock import patch, MagicMock, ANY
from datetime import datetime, timedelta, timezone
from AzureFunctionsApp.ACTIVE.active_ccd.ccdFunctions import start_case_creation, validate_case, submit_case, process_case
from AzureFunctionsApp.ACTIVE.active_ccd.tokenManager import IDAMTokenManager, S2S_Manager

#### FUNCTIONS  - this is to be removed once we import the functions at the top of the script (This was only done because we could not merge)#################

############################################################
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
    ### check the correct url is called
    mock_get.assert_called_once_with(
        expected_url,
        headers = {
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
    etid = "createCase"

    idam_token = "test_idam_token"
    s2s_token = "test_s2s_token"

    event_token = "test_event_token"

    payload_data = {"data":"test_payload"}

    validate_case_response = validate_case(ccd_base_url,event_token, payload_data,jid,ctid,idam_token,uid,s2s_token)

    expected_url = f"{ccd_base_url}/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/validate"
    ### assertions
    mock_post.assert_called_once_with(
        expected_url,
        headers = {
            "Authorization": f"Bearer {idam_token}",
            "ServiceAuthorization": f"{s2s_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        json = ANY
    )

    assert validate_case_response.status_code == 201

@patch("requests.post")
def test_validate_case_failure(mock_post):

    ccd_base_url = "https://ccd-api.test.net"
    uid = "12345"
    jid = "IA"
    ctid = "Asylum"
    etid = "createCase"

    idam_token = "test_idam_token"
    s2s_token = "test_s2s_token"

    event_token = "test_event_token"

    payload_data = {"data":"test_payload"}

    mock_post.side_effect = Exception("Connection error")

    validate_case_response = validate_case(ccd_base_url,event_token,payload_data,jid,ctid,idam_token,uid,s2s_token)

    expected_url = f"{ccd_base_url}/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/validate"
    ### assertions
    mock_post.assert_called_once_with(
        expected_url,
        headers = {
            "Authorization": f"Bearer {idam_token}",
            "ServiceAuthorization": f"{s2s_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        json = ANY
    )
    validate_case_response is None


@patch("requests.post")
def test_submit_case_success(mock_post):

    mock_submit_response = MagicMock()
    mock_submit_response.status_code = 201
    mock_submit_response.json.return_value = {"id":"1234567891011"}
    mock_post.return_value = mock_submit_response


    ccd_base_url = "https://ccd-api.test.net"
    uid = "12345"
    jid = "IA"
    ctid = "Asylum"
    etid = "createCase"

    idam_token = "test_idam_token"
    s2s_token = "test_s2s_token"

    event_token = "test_event_token"

    payload_data = {"data":"test_payload"}

    submit_response = submit_case(ccd_base_url,event_token,payload_data,jid,ctid,idam_token,uid,s2s_token)

    ### assertions 

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

    payload_data = {"data":"test_payload"}

    submit_response = submit_case(ccd_base_url,event_token,payload_data,jid,ctid,idam_token,uid,s2s_token)

    expected_url = f"{ccd_base_url}/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/cases"
    #### assertions
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

# def get_idam_token():
#     response = requests.post("https://idam-web-public.aat.platform.hmcts.net/o/token")
#     return response

# @pytest.fixtures
# def mock_dbutils

# @patch('requests.post')
# def test_get_idam_token(self,mock_get):
#     mock_response = MagicMock()
#     mock_response.status_code = 200
#     mock_response.json.return_value = {"access_token": "mocked_token", 'expires_in': 3600}
#     mock_response.post.return_value = mock_response 

def mock_response(status_code,json_data=None, text=""):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json_data = json_data
    mock.text = text
    mock.headers = {'Content-Type':"application/json"}
    mock.json.return_values = json_data or {}
    return mock

### Succesfully sent a case payload ###
@pytest.mark.usefixtures("mock_token_managers")
@patch("AzureFunctionsApp.ACTIVE.active_ccd.ccdFunctions.submit_case")
@patch("AzureFunctionsApp.ACTIVE.active_ccd.ccdFunctions.validate_case")
@patch("AzureFunctionsApp.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_funciton_success(mock_start_case_creation_response,
                                  mock_validate_case_response,
                                  mock_submit_case_response):

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
        PR_NUMBER="2866"
    )

    assert results["Status"] == "Success"
    assert results["CCDCaseID"] == "1234567891011"
    assert results["Error"] is None
    
    ### failed to start case creation ###
@patch("AzureFunctionsApp.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_case_start_case_fail(mock_start_case_creation):
    mock_start_case_response = mock_response(401,{"text":"bad response"})
    mock_start_case_creation.return_value = mock_start_case_response

    results = process_case(
    env="sbox",
    caseNo="CASE123",
    payloadData={"key": "value"},
    runId="run123",
    state="paymentPending",
    PR_NUMBER = "2866"
)
    
    assert results["Status"]=="ERROR"
    assert results["Error"] is not None

@patch("AzureFunctionsApp.ACTIVE.active_ccd.ccdFunctions.validate_case")
@patch("AzureFunctionsApp.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_case_validation_fails(mock_start, mock_validate):
    mock_start.return_value = mock_response(200, {"token": "abc123"})
    mock_validate.return_value = mock_response(400, text="Invalid payload")

    result = process_case(
        env="sbox",
        caseNo="CASE888",
        payloadData={},
        runId="run123",
        state="paymentPending",
        PR_NUMBER = "2866"
    )

    assert result["Status"] == "ERROR"
    assert result["Error"] is not None

@patch("AzureFunctionsApp.ACTIVE.active_ccd.ccdFunctions.submit_case")
@patch("AzureFunctionsApp.ACTIVE.active_ccd.ccdFunctions.validate_case")
@patch("AzureFunctionsApp.ACTIVE.active_ccd.ccdFunctions.start_case_creation")
def test_process_case_submission_fails(mock_start, mock_validate, mock_submit):
    mock_start.return_value = mock_response(200, {"token": "abc123"})
    mock_validate.return_value = mock_response(201)
    mock_submit.return_value = mock_response(500, text="Server error")

    result = process_case(
        env="sbox",
        caseNo="CASE777",
        payloadData={},
        runId="run123",
        state="paymentPending",
        PR_NUMBER = "2866"
    )


    assert result["Status"] == "ERROR"
    assert result["Error"] is not None

