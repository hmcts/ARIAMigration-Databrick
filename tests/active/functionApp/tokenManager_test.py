from unittest.mock import MagicMock,patch, ANY
import pytest

###### IDAM token manager code - to be deleted when you import the module
import AzureFunctions.Active.active_ccd.tokenManager as tokenManager
from datetime import datetime, timezone, timedelta



##### Unit tests
## ------ Initilaise -------
@pytest.fixture
def mock_keyvault_client(monkeypatch):
    """Mock Azure Key Vault SecretClient."""
    mock_secret_client = MagicMock()
    mock_secret_client.get_secret.side_effect = lambda name: MagicMock(value=f"mock_{name}")

    # Patch SecretClient constructor
    monkeypatch.setattr(tokenManager,"SecretClient", MagicMock(return_value=mock_secret_client))
    # Patch DefaultAzureCredential (not used but initialised)
    monkeypatch.setattr(tokenManager,"DefaultAzureCredential", MagicMock(return_value=MagicMock()))
    return mock_secret_client

### test get token
@patch(f"AzureFunctions.Active.active_ccd.tokenManager.IDAMTokenManager._fetch_uid",return_value="test_uid")
@patch(f"AzureFunctions.Active.active_ccd.tokenManager.requests.post")
def test_fetch_token_success(mock_post,mock_fetch_uid,mock_keyvault_client):
   
   mock_fetch_token_response = MagicMock()
   mock_fetch_token_response.status_code = 200
   mock_fetch_token_response.json.return_value = {
      "access_token":"mock_test",
      "expires_in":360
   }

   mock_post.return_value = mock_fetch_token_response



   IDAMTokenMgr = tokenManager.IDAMTokenManager("sbox")

   token,expr_in,uid = IDAMTokenMgr._fetch_token()

   assert token == "mock_test"
   assert isinstance(expr_in,datetime)
   assert uid == "test_uid"

#### test _fetch_uid
@patch("requests.get")
def test_fetch_uid_success(mock_get,mock_keyvault_client):
   
   mock_fetch_uid_response = MagicMock()
   mock_fetch_uid_response.json.return_value = {"uid":"test_uid"}
   mock_get.return_value = mock_fetch_uid_response
   
   mgr = tokenManager.IDAMTokenManager("sbox")
   idam_token = "test_idam"
   uid = mgr._fetch_uid(idam_token)
   ## assertions
   assert uid == "test_uid"
   mock_get.assert_called_once_with(mgr.uid_url, headers={"Authorization": f"Bearer {idam_token}"})


##### Test that when token needs refresh this calls _fetch_token to get a new token 

@patch(f"AzureFunctions.Active.active_ccd.tokenManager.IDAMTokenManager._fetch_token",return_value=("test_idam_token",datetime.now(),"test_uid"))
@patch(f"AzureFunctions.Active.active_ccd.tokenManager.IDAMTokenManager._needs_refresh",return_value=True)
def test_get_token_when_token_expired(mock_need_refresh,mock_fetch_token,mock_keyvault_client):
   
   mgr = tokenManager.IDAMTokenManager("sbox")

   idam_token,uid = mgr.get_token()

   assert idam_token == "test_idam_token"
   assert isinstance(mgr._expiration_time, datetime)
   assert uid == "test_uid"

   
   


   
