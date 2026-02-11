#Setup Test Results Class
from dataclasses import dataclass

@dataclass
class TestResult:
    test_field: str ="" # Field Name
    # test_jira_ticket:str = "" # Jira ticket reference
    status: str ="" # "PASS" or "FAIL"
    message: str = ""  # Test Result Text
    test_from_state: str=""  #State where test Orginated (for traceability/diagnosing test issues)    
    
    # DEFAULT_TEST_FROM_STATE = "unknown"    

    # def __init__(self, test_from_state=""):
    #     self.test_from_state = DEFAULT_TEST_FROM_STATE 