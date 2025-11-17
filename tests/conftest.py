import pytest
from unittest.mock import patch

@pytest.fixture
def mock_token_managers():
    with patch("AzureFunctions.Active.active_ccd.tokenManager.IDAMTokenManager") as mock_idam, \
         patch("AzureFunctions.Active.active_ccd.tokenManager.S2S_Manager") as mock_s2s:
        
        # Provide mock token values to avoid errors inside process_case
        mock_idam.return_value.get_token.return_value = "mock-idam-token"
        mock_s2s.return_value.get_s2s_token.return_value = "mock-s2s-token"

        yield