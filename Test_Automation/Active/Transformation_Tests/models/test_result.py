#Setup Test Results Class
from dataclasses import dataclass

@dataclass
class TestResult:
    test_field: str =""
    status: str =""
    message: str = ""
    test_from_state: str=""
    test_name: str=""    
    