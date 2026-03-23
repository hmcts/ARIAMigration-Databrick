import json
import pytest
from unittest.mock import Mock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(status_code, json_data=None, text=""):
    mock_resp = Mock()
    mock_resp.status_code = status_code
    mock_resp.text = text
    mock_resp.json.return_value = json_data or {}
    return mock_resp


VALIDATE_CASE_DATA = {"field1": "value1", "field2": "value2"}
SUBMIT_CASE_DATA = {"field3": "value3", "ccd_id": "abc123"}
SUBMIT_CASE_ID = "99887766554433"

START_TOKEN_DATA = {"token": "event-token-xyz"}
VALIDATE_RESPONSE = _make_response(200, {"data": VALIDATE_CASE_DATA})
SUBMIT_RESPONSE = _make_response(201, {"id": SUBMIT_CASE_ID, "case_data": SUBMIT_CASE_DATA})
START_RESPONSE = _make_response(200, START_TOKEN_DATA)


# ---------------------------------------------------------------------------
# Fixtures / patch targets
# ---------------------------------------------------------------------------

_MODULE = "AzureFunctions.ACTIVE.active_ccd.ccdFunctions"
PATCH_IDAM = f"{_MODULE}.IDAMTokenManager"
PATCH_S2S = f"{_MODULE}.S2S_Manager"
PATCH_START = f"{_MODULE}.start_case_creation"
PATCH_VALIDATE = f"{_MODULE}.validate_case"
PATCH_SUBMIT = f"{_MODULE}.submit_case"
PATCH_IDAM_MGR = f"{_MODULE}.idam_token_mgr"


def _setup_happy_path_mocks(mock_idam_cls, mock_s2s_cls):
    """Configure IDAM and S2S token mocks for the happy path."""
    mock_idam_inst = Mock()
    mock_idam_inst.get_token.return_value = ("idam-token", "uid-123")
    mock_idam_cls.return_value = mock_idam_inst

    mock_s2s_inst = Mock()
    mock_s2s_inst.get_token.return_value = "s2s-token"
    mock_s2s_cls.return_value = mock_s2s_inst


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestProcessCaseSuccess:
    """process_case returns the correct result structure on full success."""

    def test_success_result_contains_new_fields(self):
        """SuccessResponse is present in a successful result."""
        with (
            patch(PATCH_IDAM),
            patch(PATCH_S2S) as mock_s2s_cls,
            patch(PATCH_START, return_value=START_RESPONSE),
            patch(PATCH_VALIDATE, return_value=VALIDATE_RESPONSE),
            patch(PATCH_SUBMIT, return_value=SUBMIT_RESPONSE),
            patch(PATCH_IDAM_MGR) as mock_idam_mgr,
        ):
            mock_idam_mgr.get_token.return_value = ("idam-token", "uid-123")
            mock_s2s_inst = Mock()
            mock_s2s_inst.get_token.return_value = "s2s-token"
            mock_s2s_cls.return_value = mock_s2s_inst

            from AzureFunctions.ACTIVE.active_ccd.ccdFunctions import process_case

            result = process_case(
                env="sbox",
                caseNo="CASE-001",
                payloadData={"appealReferenceNumber": "HU/001/2024"},
                runId="run-1",
                state="appealSubmitted",
                PR_REFERENCE="pr-123",
            )

        assert result["Status"] == "SUCCESS"
        assert result["CCDCaseID"] == SUBMIT_CASE_ID
        assert json.loads(result["SuccessResponse"]) == {"id": SUBMIT_CASE_ID, "case_data": SUBMIT_CASE_DATA}
        assert json.loads(result["StartResponse"]) == START_TOKEN_DATA

    def test_success_result_base_fields_present(self):
        """All pre-existing fields are still returned on success."""
        with (
            patch(PATCH_IDAM),
            patch(PATCH_S2S) as mock_s2s_cls,
            patch(PATCH_START, return_value=START_RESPONSE),
            patch(PATCH_VALIDATE, return_value=VALIDATE_RESPONSE),
            patch(PATCH_SUBMIT, return_value=SUBMIT_RESPONSE),
            patch(PATCH_IDAM_MGR) as mock_idam_mgr,
        ):
            mock_idam_mgr.get_token.return_value = ("idam-token", "uid-123")
            mock_s2s_inst = Mock()
            mock_s2s_inst.get_token.return_value = "s2s-token"
            mock_s2s_cls.return_value = mock_s2s_inst

            from AzureFunctions.ACTIVE.active_ccd.ccdFunctions import process_case

            result = process_case(
                env="sbox",
                caseNo="CASE-001",
                payloadData={"appealReferenceNumber": "HU/001/2024"},
                runId="run-1",
                state="appealSubmitted",
                PR_REFERENCE="pr-123",
            )

        for key in ("RunID", "CaseNo", "State", "Status", "Error", "EndDateTime", "CCDCaseID", "SuccessResponse", "StartResponse"):
            assert key in result, f"Missing key: {key}"
        assert result["Error"] is None


class TestProcessCaseValidationFailure:
    """process_case returns an error result when validation fails."""

    def test_validate_failure_returns_error(self):
        failed_validate = _make_response(422, text="Validation error")

        with (
            patch(PATCH_IDAM),
            patch(PATCH_S2S) as mock_s2s_cls,
            patch(PATCH_START, return_value=START_RESPONSE),
            patch(PATCH_VALIDATE, return_value=failed_validate),
            patch(PATCH_SUBMIT) as mock_submit,
            patch(PATCH_IDAM_MGR) as mock_idam_mgr,
        ):
            mock_idam_mgr.get_token.return_value = ("idam-token", "uid-123")
            mock_s2s_inst = Mock()
            mock_s2s_inst.get_token.return_value = "s2s-token"
            mock_s2s_cls.return_value = mock_s2s_inst

            from AzureFunctions.ACTIVE.active_ccd.ccdFunctions import process_case

            result = process_case(
                env="sbox",
                caseNo="CASE-002",
                payloadData={"appealReferenceNumber": "HU/002/2024"},
                runId="run-2",
                state="appealSubmitted",
                PR_REFERENCE="pr-123",
            )

        assert result["Status"] == "ERROR"
        assert "Case validation failed" in result["Error"]
        assert json.loads(result["StartResponse"]) == START_TOKEN_DATA
        mock_submit.assert_not_called()

    def test_validate_response_none_raises(self):
        # The None guard on validate_case_response occurs after a print statement
        # that accesses .status_code, so a None response currently raises
        # AttributeError before reaching the guard. This test documents that behaviour.
        with (
            patch(PATCH_IDAM),
            patch(PATCH_S2S) as mock_s2s_cls,
            patch(PATCH_START, return_value=START_RESPONSE),
            patch(PATCH_VALIDATE, return_value=None),
            patch(PATCH_SUBMIT),
            patch(PATCH_IDAM_MGR) as mock_idam_mgr,
        ):
            mock_idam_mgr.get_token.return_value = ("idam-token", "uid-123")
            mock_s2s_inst = Mock()
            mock_s2s_inst.get_token.return_value = "s2s-token"
            mock_s2s_cls.return_value = mock_s2s_inst

            from AzureFunctions.ACTIVE.active_ccd.ccdFunctions import process_case

            with pytest.raises(AttributeError):
                process_case(
                    env="sbox",
                    caseNo="CASE-003",
                    payloadData={},
                    runId="run-3",
                    state="appealSubmitted",
                    PR_REFERENCE="pr-123",
                )


class TestProcessCaseSubmitFailure:
    """process_case returns an error result when submission fails."""

    def test_submit_failure_returns_error(self):
        failed_submit = _make_response(500, text="Internal Server Error")

        with (
            patch(PATCH_IDAM),
            patch(PATCH_S2S) as mock_s2s_cls,
            patch(PATCH_START, return_value=START_RESPONSE),
            patch(PATCH_VALIDATE, return_value=VALIDATE_RESPONSE),
            patch(PATCH_SUBMIT, return_value=failed_submit),
            patch(PATCH_IDAM_MGR) as mock_idam_mgr,
        ):
            mock_idam_mgr.get_token.return_value = ("idam-token", "uid-123")
            mock_s2s_inst = Mock()
            mock_s2s_inst.get_token.return_value = "s2s-token"
            mock_s2s_cls.return_value = mock_s2s_inst

            from AzureFunctions.ACTIVE.active_ccd.ccdFunctions import process_case

            result = process_case(
                env="sbox",
                caseNo="CASE-004",
                payloadData={"appealReferenceNumber": "HU/004/2024"},
                runId="run-4",
                state="appealSubmitted",
                PR_REFERENCE="pr-123",
            )

        assert result["Status"] == "ERROR"
        assert "Case submission failed" in result["Error"]
        assert json.loads(result["StartResponse"]) == START_TOKEN_DATA
        assert "SuccessResponse" not in result
        assert "ValidateResponse" not in result


class TestProcessCaseStartFailure:
    """process_case returns an error result when start_case_creation fails."""

    def test_start_failure_returns_error(self):
        failed_start = _make_response(503, text="Service Unavailable")

        with (
            patch(PATCH_IDAM),
            patch(PATCH_S2S) as mock_s2s_cls,
            patch(PATCH_START, return_value=failed_start),
            patch(PATCH_VALIDATE) as mock_validate,
            patch(PATCH_SUBMIT) as mock_submit,
            patch(PATCH_IDAM_MGR) as mock_idam_mgr,
        ):
            mock_idam_mgr.get_token.return_value = ("idam-token", "uid-123")
            mock_s2s_inst = Mock()
            mock_s2s_inst.get_token.return_value = "s2s-token"
            mock_s2s_cls.return_value = mock_s2s_inst

            from AzureFunctions.ACTIVE.active_ccd.ccdFunctions import process_case

            result = process_case(
                env="sbox",
                caseNo="CASE-005",
                payloadData={},
                runId="run-5",
                state="appealSubmitted",
                PR_REFERENCE="pr-123",
            )

        assert result["Status"] == "ERROR"
        assert "Case creation failed" in result["Error"]
        assert "StartResponse" not in result
        mock_validate.assert_not_called()
        mock_submit.assert_not_called()
