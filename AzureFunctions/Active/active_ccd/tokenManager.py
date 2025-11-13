# from azure.keyvault import KeyVaultClient
import requests
import pyotp
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


from datetime import datetime, timezone, timedelta
import threading
import requests

class IDAMTokenManager:
  def __init__(self,env:str,skew:int=28792):
    self.env = env
    self._token = None
    self._expiration_time = None
    self._uid = None
    self.skew = timedelta(seconds=int(skew))
    self._lock = threading.RLock()

     # ----- Environment config (host + key vault) -----

    urls = {
        "sbox": {
            "idam_host": "https://idam-web-public.aat.platform.hmcts.net",
            "key_vault": "ia-aat"
        },
        "stg": {
            "idam_host": "https://idam-api.aat.platform.hmcts.net",
            "key_vault": "ia-aat"
        },
        "prod": {
            "idam_host": "",
            "key_vault": ""
        }   
    }

    self.idam_host = urls[self.env]["idam_host"]
    key_vault = urls[self.env]["key_vault"]

    # ----- Secrets (fetched once per manager instance) -----
    # Databricks utility to retrieve secrets from Key Vault. ## need to change this to use kv client
    credentials = DefaultAzureCredential()
    kv_client = SecretClient(vault_url=f"https://{key_vault}.vault.azure.net/", credential=credentials)
    self.client_id = kv_client.get_secret("idam-client-id").value
    self.client_secret = kv_client.get_secret("idam-secret").value
    self.username = kv_client.get_secret("system-username").value
    self.password = kv_client.get_secret("system-password").value
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

import pyotp
class S2S_Manager():
    def __init__(self,env:str,skew:int=21000):
        self.env = env
        self._s2s_token = None
        self.expire_time = None
        self._lock = threading.RLock()
        self._skew = skew


     # ----- Environment config (host + key vault) -----
        urls = {
            "sbox": {
                "s2s_host": "http://rpe-service-auth-provider-aat.service.core-compute-aat.internal",
                "s2s_ip": "http://10.10.143.250/",
                "key_vault": "ia-aat"},
            "stg": {
                "s2s_host": "http://rpe-service-auth-provider-aat.service.core-compute-aat.internal",
                "s2s_ip": None,
                "key_vault": "ia-aat"},
            "prod": {
                "s2s_host": None,
                "s2s_ip": None,
                "key_vault": None}
        }

        self.s2s_host = urls[self.env]["s2s_host"]
        self.s2s_ip = urls[self.env]["s2s_ip"]
        key_vault = urls[self.env]["key_vault"]

    # ----- Secrets (fetched once per manager instance) -----
    # Databricks utility to retrieve secrets from Key Vault. ## need to change this to use kv client

        credentials = DefaultAzureCredential()
        kv_client = SecretClient(vault_url=f"https://{key_vault}.vault.azure.net/", credential=credentials)

        self._s2s_secret = kv_client.get_secret("s2s-secret").value

        self.url = f"{self.s2s_host}/lease"
        self.s2s_microservice = "iac"


    def _fetch_s2s_token(self):
        otp = pyotp.TOTP(self._s2s_secret).now()
        # create payload
        s2s_payload = {
            "microservice": self.s2s_microservice,
            "oneTimePassword": otp
        }    
        # Attempt to make request to s2s endpoint
        try:
            now = datetime.now(timezone.utc)
            s2s_response = requests.post(
            self.url,
            json=s2s_payload,
            headers={
                # "Host": self.s2s_host,
                "Content-Type": "application/json"
            }
    )
        except Exception as e:
            raise EOFError(f"Error reuesting service to service token: {e}")
        ## Ensure you get a 200 response else raise an error
        if s2s_response.status_code != 200:
            raise RuntimeError(f"Error requesting service to service token: {s2s_response.text}")

        ## Extract token from response
        try:
            payload = s2s_response.text.strip()
            # s2s_token = payload.get("token")
        except Exception as e:
            raise RuntimeError(f"Error extracting service to service token: {e}")
        
        self.expire_time = now + timedelta(seconds=int(self._skew))
        self._s2s_token = payload

        return payload


    def get_token(self):
        if self.expire_time and datetime.now(timezone.utc) < self.expire_time:
            return self._s2s_token
        with self._lock:
            self._s2s_token = self._fetch_s2s_token()
        return self._s2s_token



    

if __name__ == "__main__":
    idam_token_mgr = IDAMTokenManager(env="sbox")
    token, uid = idam_token_mgr.get_token()
    print(f"IDAM Token: {token}")
    print(f"UID: {uid}")

    s2s_manager = S2S_Manager("sbox",21)
    s2s_token = s2s_manager.get_token()
    print(f"S2S Token: {s2s_token}")



