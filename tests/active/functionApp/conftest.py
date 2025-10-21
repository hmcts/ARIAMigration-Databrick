import sys
from unittest.mock import MagicMock

# Mock dbutils globally for all tests
mock_dbutils = MagicMock()
mock_dbutils.secrets.get.return_value = "dummy_secret"

# Make it available to service2service module
import service2service
service2service.dbutils = mock_dbutils