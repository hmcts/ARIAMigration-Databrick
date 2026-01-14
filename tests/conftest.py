import pytest
from unittest.mock import patch, MagicMock
import os

@pytest.fixture
def mock_token_managers():
    """
    Patch IDAMTokenManager and S2S_Manager in ccdFunctions where process_case uses them.
    """
    with patch("AzureFunctions.Active.active_ccd.ccdFunctions.IDAMTokenManager") as mock_idam, \
         patch("AzureFunctions.Active.active_ccd.ccdFunctions.S2S_Manager") as mock_s2s:

        mock_idam_inst = MagicMock()
        mock_idam_inst.get_token.return_value = ("mock_idam_token", "uid123")
        mock_idam.return_value = mock_idam_inst

        mock_s2s_inst = MagicMock()
        mock_s2s_inst.get_token.return_value = "mock_s2s_token"
        mock_s2s.return_value = mock_s2s_inst

        yield

@pytest.fixture(scope="session", autouse=True)
def setup_env_vars():
    """Set up required environment variables for tests"""
    os.environ.setdefault("ENVIRONMENT", "sbox")
    os.environ.setdefault("LZ_KEY", "01")
    os.environ.setdefault("PR_NUMBER", "1234")