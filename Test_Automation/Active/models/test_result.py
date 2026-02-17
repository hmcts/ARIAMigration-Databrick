from dataclasses import dataclass

@dataclass
class TestResult:
    table_name: str
    test_type: str
    status: str
    message: str

# @dataclass
# class TestResult:
#     test_field: str =""     
#     status: str =""
#     message: str = ""
#     test_from_state: str=""
#     test_name: str=""