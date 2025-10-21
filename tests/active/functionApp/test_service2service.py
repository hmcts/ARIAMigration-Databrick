import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta
from service2service import S2S_Manager
import service2service

def test_fetch_s2s_token(monkeypatch):
    manager = S2S_Manager("sbox")  # creates self. values from __init__ based on environment provided eg sbox

    # mock generate one time password
    mock_totp_instance = MagicMock()
    mock_totp_instance.now.return_value = "123456"
    monkeypatch.setattr(service2service.pyotp, "TOTP", lambda secret: mock_totp_instance) # replace dbutils.secret with mock

    # mock generate response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "mock_token_string" 
    monkeypatch.setattr(service2service.requests, "post", lambda url, json, headers: mock_response) # replace post request vars with mock

    token = manager._fetch_s2s_token() #generate token using s2s.OTP, s2s.requests

    assert token == "mock_token_string" # token is a value
    assert manager._s2s_token == "mock_token_string" #token is a value
    assert isinstance(manager.expire_time, datetime) #token has not expired
    mock_totp_instance.now.assert_called_once() # has the function been called


@patch.object(S2S_Manager, "_fetch_s2s_token") #return token from previous func
def test_get_token_returns_existing(mock_fetch):
    manager = S2S_Manager("sbox")
    manager._s2s_token = "EXISTING_TOKEN"
    manager.expire_time = datetime.now(timezone.utc) + timedelta(seconds=60)

    token = manager.get_token()

    assert token == "EXISTING_TOKEN"
    mock_fetch.assert_not_called()