import sys
from unittest.mock import MagicMock, patch

# Mock Azure Identity BEFORE any modules are imported
mock_azure_identity = MagicMock()
mock_credential_instance = MagicMock()
mock_azure_identity.DefaultAzureCredential.return_value = mock_credential_instance
sys.modules['azure.identity'] = mock_azure_identity

import pytest

@pytest.fixture(autouse=True)
def mock_token_managers():
    """Mock the token managers that use Azure credentials"""
    with patch('AzureFunctions.Active.active_ccd.tokenManager.IDAMTokenManager') as mock_idam, \
         patch('AzureFunctions.Active.active_ccd.tokenManager.S2S_Manager') as mock_s2s:
        
        # Mock the token manager instances
        mock_idam_instance = MagicMock()
        mock_idam_instance.get_token.return_value = "mock_idam_token"
        mock_idam.return_value = mock_idam_instance
        
        mock_s2s_instance = MagicMock()
        mock_s2s_instance.get_token.return_value = "mock_s2s_token"
        mock_s2s.return_value = mock_s2s_instance
        
        yield mock_idam_instance, mock_s2s_instance