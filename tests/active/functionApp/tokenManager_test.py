from unittest.mock import MagicMock,patch, ANY
import pytest
import sys
import __main__ ### remove this and change where this is used to the module path
current_module = sys.modules[__name__]

# sys.modules["azure.identity"] = MagicMock()
# sys.modules["azure.keyvault.secrets"] = MagicMock()
###### IDAM token manager code - to be deleted when you import the module

from datetime import datetime, timezone, timedelta
import threading
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

class IDAMTokenManager:
  def __init__(self,env:str,skew:int=28792):
    self.env = env
    self._token = None
    self._expiration_time = None
    self._uid = None
    self.skew = timedelta(seconds=int(skew))
    self._lock = threading.RLock()

     # ----- Environment config (host + key vault) -----
    if env == "sbox":
        self.idam_host = "https://idam-web-public.aat.platform.hmcts.net"
        key_vault = "ia-aat"
    elif env == "stg":
        self.idam_host = "https://idam-api.aat.platform.hmcts.net"
        key_vault = "ia-aat"
    elif env == "prod":
        # TODO: fill in production host and key vault names.
        self.idam_host = ""
        key_vault = ""
    else:
        # Fail fast if an unknown env is passed.
        raise ValueError(f"Unknown env: {env}")

    # ----- Secrets (fetched once per manager instance) -----
    # Databricks utility to retrieve secrets from Key Vault. ## need to change this to use kv client

    credentials = DefaultAzureCredential()
    kv_client = SecretClient(vault_url=f"https://{key_vault}.vault.azure.net/", credential=credentials)
    self.client_id = kv_client.get_secret("idam-client-id").value
    self.client_secret = kv_client.get_secret("idam-secret").value
    self.username = kv_client.get_secret("system-username").value
    self.password = kv_client.get_secret("system-password").value
    # self.client_id = dbutils.secrets.get(key_vault, "idam-client-id")
    # self.client_secret = dbutils.secrets.get(key_vault, "idam-secret")
    # self.username = dbutils.secrets.get(key_vault, "system-username")
    # self.password = dbutils.secrets.get(key_vault, "system-password")

    # OAuth2 token endpoint (password grant).
    self.token_url = f"{self.idam_host}/o/token"
    self.uid_url = f"{self.idam_host}/o/userinfo"
    # Requested scopes.
    self.scope = "openid profile roles"




  def _fetch_token(self):
    data = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": self.username,
            "password": self.password,
            "scope": self.scope,
        }
    
    headers = {"Content-Type":"application/x-www-form-urlencoded"}

    idam_response = requests.post(self.token_url , headers=headers, data=data)

    if idam_response.status_code != 200:
      raise RuntimeError(f"Token request failed: {idam_response.status_code} {idam_response.text}")


    payload = idam_response.json()

    idam_token = payload.get("access_token")
    expires_in = payload.get("expires_in")
    if not idam_token or not expires_in:
      raise RuntimeError(f"Invalid token response: {payload}")

    now = datetime.now(timezone.utc)

    expiration_time = now + timedelta(seconds=int(expires_in))

    uid = self._fetch_uid(idam_token)

    return idam_token,expiration_time,uid
  

  def _needs_refresh(self):

    if not self._token or not self._expiration_time:
      return True

    return datetime.now(timezone.utc) >= (self._expiration_time - self.skew)


  def get_token(self):
    if not self._needs_refresh():
      return self._token,self._uid
# use locking to only allow one block to refresh and other blocks will have to wait
    with self._lock:
      if self._needs_refresh():
        self._token, self._expiration_time, self._uid = self._fetch_token()
      return self._token, self._uid
    
  def _fetch_uid(self,idam_token):
    uid_headers = {"Authorization": f"Bearer {idam_token}"}
  ## make the get request to get the uid
    try:
        idam_response = requests.get(self.uid_url, headers=uid_headers)
    except Exception as e:        
        print(f"UID request failed: {e}")
  ## safley convert to json
    try:
      payload = idam_response.json()
    except ValueError:
      raise RuntimeError(f"UID endpoint did not return valid JSON: {idam_response.text}")
  ## get the uid from the json
    uid = payload.get("uid")
    if not uid:
        raise RuntimeError(f"UID missing in response: {payload}")
    return uid


    
  def invalidate(self):
    with self._lock:
      self._token = None
      self._expiration_time = None
      self._uid = None




##### Unit tests
## ------ Initilaise -------
@pytest.fixture
def mock_keyvault_client(monkeypatch):
    """Mock Azure Key Vault SecretClient."""
    mock_secret_client = MagicMock()
    mock_secret_client.get_secret.side_effect = lambda name: MagicMock(value=f"mock_{name}")

    # Patch SecretClient constructor
    monkeypatch.setattr(current_module,"SecretClient", MagicMock(return_value=mock_secret_client))
    # Patch DefaultAzureCredential (not used but initialised)
    monkeypatch.setattr(current_module,"DefaultAzureCredential", MagicMock(return_value=MagicMock()))
    return mock_secret_client

### test get token
@patch(f"{__name__}.IDAMTokenManager._fetch_uid",return_value="test_uid")
@patch(f"{__name__}.requests.post")
def test_fetch_token_success(mock_post,mock_fetch_uid,mock_keyvault_client):
   
   mock_fetch_token_response = MagicMock()
   mock_fetch_token_response.status_code = 200
   mock_fetch_token_response.json.return_value = {
      "access_token":"mock_test",
      "expires_in":360
   }

   mock_post.return_value = mock_fetch_token_response



   IDAMTokenMgr = IDAMTokenManager("sbox")

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
   
   mgr = IDAMTokenManager("sbox")
   idam_token = "test_idam"
   uid = mgr._fetch_uid(idam_token)
   ## assertions
   assert uid == "test_uid"
   mock_get.assert_called_once_with(mgr.uid_url, headers={"Authorization": f"Bearer {idam_token}"})


##### Test that when token needs refresh this calls _fetch_token to get a new token 

@patch(f"{__name__}.IDAMTokenManager._fetch_token",return_value=("test_idam_token",datetime.now(),"test_uid"))
@patch(f"{__name__}.IDAMTokenManager._needs_refresh",return_value=True)
def test_get_token_when_token_expired(mock_need_refresh,mock_fetch_token,mock_keyvault_client):
   
   mgr = IDAMTokenManager("sbox")

   idam_token,uid = mgr.get_token()

   assert idam_token == "test_idam_token"
   assert isinstance(mgr._expiration_time, datetime)
   assert uid == "test_uid"

   
   


   
