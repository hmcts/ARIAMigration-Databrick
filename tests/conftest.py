import pytest
from unittest.mock import patch, MagicMock

@pytest.fixture
def mock_token_managers():
    """
    Patch the token managers in ccdFunctions where process_case is defined.
    """
    with patch("AzureFunctions.Active.active_ccd.ccdFunctions.tokenManager.IDAMTokenManager") as mock_idam, \
         patch("AzureFunctions.Active.active_ccd.ccdFunctions.tokenManager.S2S_Manager") as mock_s2s:

        mock_idam_inst = MagicMock()
        mock_idam_inst.get_token.return_value = ("mock_idam_token", "uid123")
        mock_idam.return_value = mock_idam_inst

        mock_s2s_inst = MagicMock()
        mock_s2s_inst.get_token.return_value = "mock_s2s_token"
        mock_s2s.return_value = mock_s2s_inst

        yield