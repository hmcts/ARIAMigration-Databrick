import sys
from unittest.mock import MagicMock, patch

# Mock Azure Identity BEFORE any modules are imported
mock_azure_identity = MagicMock()
mock_credential_instance = MagicMock()
mock_azure_identity.DefaultAzureCredential.return_value = mock_credential_instance
sys.modules['azure.identity'] = mock_azure_identity