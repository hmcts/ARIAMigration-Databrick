import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, call

from AzureFunctions.ACTIVE.active_caselink_ccd.cl_tokenManager import IDAMTokenManager, S2S_Manager

MODULE = "AzureFunctions.ACTIVE.active_caselink_ccd.cl_tokenManager"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_kv_client(overrides=None):
    """Return a mock SecretClient with configurable secret values."""
    secrets = {
        "idam-client-id": "fake-client-id",
        "idam-secret": "fake-client-secret",
        "system-username": "fake-username",
        "system-password": "fake-password",
        "s2s-secret": "JBSWY3DPEHPK3PXP",  # valid base32 for pyotp
    }
    if overrides:
        secrets.update(overrides)

    def get_secret(name):
        secret = MagicMock()
        secret.value = secrets[name]
        return secret

    kv = MagicMock()
    kv.get_secret.side_effect = get_secret
    return kv


def make_idam_manager(env="sbox", skew=28792, **kv_overrides):
    """Instantiate IDAMTokenManager with Azure dependencies mocked."""
    with patch(f"{MODULE}.DefaultAzureCredential"), \
         patch(f"{MODULE}.SecretClient") as mock_kv_cls:
        mock_kv_cls.return_value = make_kv_client(kv_overrides)
        return IDAMTokenManager(env=env, skew=skew)


def make_s2s_manager(env="sbox", skew=21, **kv_overrides):
    """Instantiate S2S_Manager with Azure dependencies mocked."""
    with patch(f"{MODULE}.DefaultAzureCredential"), \
         patch(f"{MODULE}.SecretClient") as mock_kv_cls:
        mock_kv_cls.return_value = make_kv_client(kv_overrides)
        return S2S_Manager(env=env, skew=skew)


def mock_token_response(status_code=200, access_token="tok-abc", expires_in=3600):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {"access_token": access_token, "expires_in": expires_in}
    resp.text = "OK"
    return resp


def mock_uid_response(uid="user-123"):
    resp = MagicMock()
    resp.json.return_value = {"uid": uid}
    return resp


# ---------------------------------------------------------------------------
# IDAMTokenManager.__init__
# ---------------------------------------------------------------------------

def test_idam_init_sbox_sets_correct_idam_host():
    mgr = make_idam_manager(env="sbox")
    assert mgr.idam_host == "https://idam-web-public.aat.platform.hmcts.net"


def test_idam_init_stg_sets_correct_idam_host():
    mgr = make_idam_manager(env="stg")
    assert mgr.idam_host == "https://idam-api.aat.platform.hmcts.net"


def test_idam_init_key_vault_url_contains_ia_aat():
    with patch(f"{MODULE}.DefaultAzureCredential"), \
         patch(f"{MODULE}.SecretClient") as mock_kv_cls:
        mock_kv_cls.return_value = make_kv_client()
        IDAMTokenManager(env="sbox")
    assert "ia-aat" in mock_kv_cls.call_args.kwargs["vault_url"]


def test_idam_init_fetches_all_required_secrets():
    with patch(f"{MODULE}.DefaultAzureCredential"), \
         patch(f"{MODULE}.SecretClient") as mock_kv_cls:
        kv = make_kv_client()
        mock_kv_cls.return_value = kv
        IDAMTokenManager(env="sbox")

    fetched = [c.args[0] for c in kv.get_secret.call_args_list]
    assert "idam-client-id" in fetched
    assert "idam-secret" in fetched
    assert "system-username" in fetched
    assert "system-password" in fetched


def test_idam_init_stores_secrets_on_instance():
    mgr = make_idam_manager(
        **{
            "idam-client-id": "my-client",
            "idam-secret": "my-secret",
            "system-username": "user@test.com",
            "system-password": "p4ssw0rd",
        }
    )
    assert mgr.client_id == "my-client"
    assert mgr.client_secret == "my-secret"
    assert mgr.username == "user@test.com"
    assert mgr.password == "p4ssw0rd"


def test_idam_init_token_state_is_none():
    mgr = make_idam_manager()
    assert mgr._token is None
    assert mgr._expiration_time is None
    assert mgr._uid is None


def test_idam_init_token_url_and_uid_url_are_set():
    mgr = make_idam_manager(env="sbox")
    assert mgr.token_url.endswith("/o/token")
    assert mgr.uid_url.endswith("/o/userinfo")
    assert mgr.idam_host in mgr.token_url


# ---------------------------------------------------------------------------
# IDAMTokenManager._needs_refresh
# ---------------------------------------------------------------------------

def test_needs_refresh_when_token_is_none():
    mgr = make_idam_manager()
    assert mgr._needs_refresh() is True


def test_needs_refresh_when_expiration_is_none():
    mgr = make_idam_manager()
    mgr._token = "tok"
    assert mgr._needs_refresh() is True


def test_needs_refresh_when_token_is_past_expiry():
    mgr = make_idam_manager(skew=0)
    mgr._token = "tok"
    mgr._expiration_time = datetime.now(timezone.utc) - timedelta(seconds=1)
    assert mgr._needs_refresh() is True


def test_needs_refresh_when_within_skew_window():
    mgr = make_idam_manager(skew=3600)
    mgr._token = "tok"
    # Expires in 30 minutes, but skew is 1 hour → still needs refresh
    mgr._expiration_time = datetime.now(timezone.utc) + timedelta(minutes=30)
    assert mgr._needs_refresh() is True


def test_no_refresh_needed_when_token_is_valid():
    mgr = make_idam_manager(skew=10)
    mgr._token = "tok"
    mgr._expiration_time = datetime.now(timezone.utc) + timedelta(hours=8)
    assert mgr._needs_refresh() is False


# ---------------------------------------------------------------------------
# IDAMTokenManager._fetch_uid
# ---------------------------------------------------------------------------

def test_fetch_uid_success():
    mgr = make_idam_manager()
    with patch("requests.get", return_value=mock_uid_response("user-abc")):
        uid = mgr._fetch_uid("some-idam-token")
    assert uid == "user-abc"


def test_fetch_uid_uses_bearer_token_header():
    mgr = make_idam_manager()
    with patch("requests.get", return_value=mock_uid_response()) as mock_get:
        mgr._fetch_uid("my-token")
    _, kwargs = mock_get.call_args
    assert kwargs["headers"]["Authorization"] == "Bearer my-token"


def test_fetch_uid_raises_on_invalid_json():
    mgr = make_idam_manager()
    bad_resp = MagicMock()
    bad_resp.json.side_effect = ValueError("No JSON object")
    bad_resp.text = "Not JSON"
    with patch("requests.get", return_value=bad_resp):
        with pytest.raises(RuntimeError, match="valid JSON"):
            mgr._fetch_uid("some-token")


def test_fetch_uid_raises_when_uid_missing():
    mgr = make_idam_manager()
    resp = MagicMock()
    resp.json.return_value = {"sub": "something-else"}
    with patch("requests.get", return_value=resp):
        with pytest.raises(RuntimeError, match="UID missing"):
            mgr._fetch_uid("some-token")


# ---------------------------------------------------------------------------
# IDAMTokenManager._fetch_token
# ---------------------------------------------------------------------------

def test_fetch_token_success_returns_token_expiry_and_uid():
    mgr = make_idam_manager()
    with patch("requests.post", return_value=mock_token_response(access_token="new-tok", expires_in=3600)), \
         patch("requests.get", return_value=mock_uid_response("uid-999")):
        token, expiry, uid = mgr._fetch_token()

    assert token == "new-tok"
    assert uid == "uid-999"
    assert expiry > datetime.now(timezone.utc)


def test_fetch_token_posts_to_token_url():
    mgr = make_idam_manager()
    with patch("requests.post", return_value=mock_token_response()) as mock_post, \
         patch("requests.get", return_value=mock_uid_response()):
        mgr._fetch_token()

    assert mock_post.call_args.args[0] == mgr.token_url


def test_fetch_token_posts_password_grant_data():
    mgr = make_idam_manager()
    with patch("requests.post", return_value=mock_token_response()) as mock_post, \
         patch("requests.get", return_value=mock_uid_response()):
        mgr._fetch_token()

    data = mock_post.call_args.kwargs["data"]
    assert data["grant_type"] == "password"
    assert data["client_id"] == mgr.client_id
    assert data["client_secret"] == mgr.client_secret
    assert data["username"] == mgr.username
    assert data["password"] == mgr.password


def test_fetch_token_raises_on_non_200():
    mgr = make_idam_manager()
    bad_resp = MagicMock()
    bad_resp.status_code = 401
    bad_resp.text = "Unauthorized"
    with patch("requests.post", return_value=bad_resp):
        with pytest.raises(RuntimeError, match="Token request failed"):
            mgr._fetch_token()


def test_fetch_token_raises_when_access_token_missing():
    mgr = make_idam_manager()
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"expires_in": 3600}  # no access_token
    with patch("requests.post", return_value=resp):
        with pytest.raises(RuntimeError, match="Invalid token response"):
            mgr._fetch_token()


def test_fetch_token_raises_when_expires_in_missing():
    mgr = make_idam_manager()
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"access_token": "tok"}  # no expires_in
    with patch("requests.post", return_value=resp):
        with pytest.raises(RuntimeError, match="Invalid token response"):
            mgr._fetch_token()


# ---------------------------------------------------------------------------
# IDAMTokenManager.get_token
# ---------------------------------------------------------------------------

def test_get_token_returns_cached_when_valid():
    mgr = make_idam_manager(skew=10)
    mgr._token = "cached-tok"
    mgr._uid = "cached-uid"
    mgr._expiration_time = datetime.now(timezone.utc) + timedelta(hours=8)

    token, uid = mgr.get_token()

    assert token == "cached-tok"
    assert uid == "cached-uid"


def test_get_token_refreshes_when_expired():
    mgr = make_idam_manager(skew=0)
    mgr._token = "old-tok"
    mgr._uid = "old-uid"
    mgr._expiration_time = datetime.now(timezone.utc) - timedelta(seconds=1)

    with patch("requests.post", return_value=mock_token_response(access_token="fresh-tok")), \
         patch("requests.get", return_value=mock_uid_response("fresh-uid")):
        token, uid = mgr.get_token()

    assert token == "fresh-tok"
    assert uid == "fresh-uid"


def test_get_token_does_not_call_fetch_when_valid():
    mgr = make_idam_manager(skew=10)
    mgr._token = "valid-tok"
    mgr._uid = "valid-uid"
    mgr._expiration_time = datetime.now(timezone.utc) + timedelta(hours=8)

    with patch.object(mgr, "_fetch_token") as mock_fetch:
        mgr.get_token()

    mock_fetch.assert_not_called()


# ---------------------------------------------------------------------------
# IDAMTokenManager.invalidate
# ---------------------------------------------------------------------------

def test_invalidate_clears_token_uid_and_expiry():
    mgr = make_idam_manager(skew=10)
    mgr._token = "tok"
    mgr._uid = "uid"
    mgr._expiration_time = datetime.now(timezone.utc) + timedelta(hours=1)

    mgr.invalidate()

    assert mgr._token is None
    assert mgr._uid is None
    assert mgr._expiration_time is None


def test_invalidate_causes_next_get_token_to_refresh():
    mgr = make_idam_manager(skew=10)
    mgr._token = "tok"
    mgr._uid = "uid"
    mgr._expiration_time = datetime.now(timezone.utc) + timedelta(hours=8)

    assert mgr._needs_refresh() is False
    mgr.invalidate()
    assert mgr._needs_refresh() is True


# ---------------------------------------------------------------------------
# S2S_Manager.__init__
# ---------------------------------------------------------------------------

def test_s2s_init_sbox_sets_correct_host():
    mgr = make_s2s_manager(env="sbox")
    assert "rpe-service-auth-provider-aat" in mgr.s2s_host


def test_s2s_init_fetches_s2s_secret():
    with patch(f"{MODULE}.DefaultAzureCredential"), \
         patch(f"{MODULE}.SecretClient") as mock_kv_cls:
        kv = make_kv_client()
        mock_kv_cls.return_value = kv
        S2S_Manager(env="sbox", skew=21)

    fetched = [c.args[0] for c in kv.get_secret.call_args_list]
    assert "s2s-secret" in fetched


def test_s2s_init_stores_secret_on_instance():
    mgr = make_s2s_manager(**{"s2s-secret": "MY_SECRET_VALUE"})
    assert mgr._s2s_secret == "MY_SECRET_VALUE"


def test_s2s_init_token_state_is_none():
    mgr = make_s2s_manager()
    assert mgr._s2s_token is None
    assert mgr.expire_time is None


def test_s2s_init_sets_lease_url():
    mgr = make_s2s_manager(env="sbox")
    assert mgr.url == f"{mgr.s2s_host}/lease"


def test_s2s_init_microservice_is_iac():
    mgr = make_s2s_manager()
    assert mgr.s2s_microservice == "iac"


# ---------------------------------------------------------------------------
# S2S_Manager._fetch_s2s_token
# ---------------------------------------------------------------------------

def test_fetch_s2s_token_success_returns_token():
    mgr = make_s2s_manager(skew=21)
    resp = MagicMock()
    resp.status_code = 200
    resp.text = "  s2s-jwt-value  "
    with patch("requests.post", return_value=resp), \
         patch(f"{MODULE}.pyotp.TOTP") as mock_totp:
        mock_totp.return_value.now.return_value = "123456"
        token = mgr._fetch_s2s_token()

    assert token == "s2s-jwt-value"
    assert mgr._s2s_token == "s2s-jwt-value"


def test_fetch_s2s_token_sets_expire_time():
    mgr = make_s2s_manager(skew=21)
    resp = MagicMock()
    resp.status_code = 200
    resp.text = "tok"
    before = datetime.now(timezone.utc)
    with patch("requests.post", return_value=resp), \
         patch(f"{MODULE}.pyotp.TOTP") as mock_totp:
        mock_totp.return_value.now.return_value = "000000"
        mgr._fetch_s2s_token()

    assert mgr.expire_time is not None
    assert mgr.expire_time > before


def test_fetch_s2s_token_posts_correct_microservice_and_otp():
    mgr = make_s2s_manager()
    resp = MagicMock()
    resp.status_code = 200
    resp.text = "tok"
    with patch("requests.post", return_value=resp) as mock_post, \
         patch(f"{MODULE}.pyotp.TOTP") as mock_totp:
        mock_totp.return_value.now.return_value = "777888"
        mgr._fetch_s2s_token()

    body = mock_post.call_args.kwargs["json"]
    assert body["microservice"] == "iac"
    assert body["oneTimePassword"] == "777888"


def test_fetch_s2s_token_uses_s2s_secret_for_otp():
    mgr = make_s2s_manager(**{"s2s-secret": "TESTSECRET123456"})
    mgr._s2s_secret = "TESTSECRET123456"
    resp = MagicMock()
    resp.status_code = 200
    resp.text = "tok"
    with patch("requests.post", return_value=resp), \
         patch(f"{MODULE}.pyotp.TOTP") as mock_totp:
        mock_totp.return_value.now.return_value = "111111"
        mgr._fetch_s2s_token()

    mock_totp.assert_called_once_with("TESTSECRET123456")


def test_fetch_s2s_token_raises_eoferror_on_network_error():
    mgr = make_s2s_manager()
    with patch("requests.post", side_effect=Exception("Connection timeout")), \
         patch(f"{MODULE}.pyotp.TOTP") as mock_totp:
        mock_totp.return_value.now.return_value = "123456"
        with pytest.raises(EOFError, match="Error reuesting service to service token"):
            mgr._fetch_s2s_token()


def test_fetch_s2s_token_raises_runtime_error_on_non_200():
    mgr = make_s2s_manager()
    resp = MagicMock()
    resp.status_code = 403
    resp.text = "Forbidden"
    with patch("requests.post", return_value=resp), \
         patch(f"{MODULE}.pyotp.TOTP") as mock_totp:
        mock_totp.return_value.now.return_value = "123456"
        with pytest.raises(RuntimeError, match="Error requesting service to service token"):
            mgr._fetch_s2s_token()


# ---------------------------------------------------------------------------
# S2S_Manager.get_token
# ---------------------------------------------------------------------------

def test_s2s_get_token_fetches_when_no_cached_token():
    mgr = make_s2s_manager(skew=21)
    resp = MagicMock()
    resp.status_code = 200
    resp.text = "brand-new-s2s"
    with patch("requests.post", return_value=resp), \
         patch(f"{MODULE}.pyotp.TOTP") as mock_totp:
        mock_totp.return_value.now.return_value = "000000"
        token = mgr.get_token()

    assert token == "brand-new-s2s"


def test_s2s_get_token_returns_cached_when_not_expired():
    mgr = make_s2s_manager(skew=21)
    mgr._s2s_token = "cached-s2s"
    mgr.expire_time = datetime.now(timezone.utc) + timedelta(hours=1)

    token = mgr.get_token()

    assert token == "cached-s2s"


def test_s2s_get_token_refreshes_when_expired():
    mgr = make_s2s_manager(skew=21)
    mgr._s2s_token = "old-s2s"
    mgr.expire_time = datetime.now(timezone.utc) - timedelta(seconds=1)

    resp = MagicMock()
    resp.status_code = 200
    resp.text = "refreshed-s2s"
    with patch("requests.post", return_value=resp), \
         patch(f"{MODULE}.pyotp.TOTP") as mock_totp:
        mock_totp.return_value.now.return_value = "999999"
        token = mgr.get_token()

    assert token == "refreshed-s2s"


def test_s2s_get_token_does_not_call_fetch_when_valid():
    mgr = make_s2s_manager(skew=21)
    mgr._s2s_token = "valid-s2s"
    mgr.expire_time = datetime.now(timezone.utc) + timedelta(hours=1)

    with patch.object(mgr, "_fetch_s2s_token") as mock_fetch:
        mgr.get_token()

    mock_fetch.assert_not_called()
