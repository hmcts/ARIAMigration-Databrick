from dataclasses import dataclass

@dataclass
class TestResult:
    table_name: str
    test_type: str
    status: str
    message: str