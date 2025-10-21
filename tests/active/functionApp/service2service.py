import pyotp
import threading
import requests
from datetime import datetime, timezone, timedelta

class S2S_Manager():
    def __init__(self,env:str,skew:int=21):
        self.env = env
        self._s2s_token = None
        self.expire_time = None
        self._lock = threading.RLock()
        self._skew = skew

     # ----- Environment config (host + key vault) -----
        if self.env =="sbox":
            self.s2s_host = "rpe-service-auth-provider-aat.service.core-compute-aat.internal"
            self.s2s_ip = "https://10.10.143.250/"
            key_vault = "ia-aat"
        elif self.env =="stg":
            self.s2s_host = None
            self.s2s_ip = None
            key_vault = None
        elif self.env =="prod":
            self.s2s_host = None
            self.s2s_ip = None
            key_vault = None

    # ----- Secrets (fetched once per manager instance) -----
    # Databricks utility to retrieve secrets from Key Vault. ## need to change this to use kv client

        # credentials = DefaultAzureCredential()
        # kv_client = SecretClient(vault_url=f"https://{key_vault}.vault.azure.net/", credential=credential)
        # self.s2s_client_id = kv_client.get_secret("s2s-client-id").value
        self._s2s_secret = dbutils.secrets.get(key_vault, "s2s-secret")
        self.url = f"{self.s2s_ip}lease"
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
                "Host": self.s2s_host,
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
