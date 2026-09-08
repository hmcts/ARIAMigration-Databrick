import json
import logging
from datetime import datetime, timezone

import requests

# tokenManager lives in the same package. When this module is imported by the
# Functions host the package root will be `AzureFunctions.ACTIVE.active_ccd`.
# Use a robust import that works both when running under the Functions host
# (package import) and when running the module directly (script import).
try:
    # package import when running under Functions host
    from .tokenManager import IDAMTokenManager, S2S_Manager
except Exception:
    # fallback when running as a script in the same folder
    from tokenManager import IDAMTokenManager, S2S_Manager

logger = logging.getLogger(__name__)


def _get_res_body_as_text(response) -> str:
    try:
        return str(response.text)
    except Exception:
        try:
            return (getattr(response, 'content', None) or b'').decode('utf-8', errors='replace')
        except Exception:
            return 'Unable to get response body as text'


def _compact(value) -> str:
    try:
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        text = str(value)
        return text.replace("\r\n", "\\n").replace("\r", "\\n").replace("\n", "\\n")
    except Exception as e:
        print(f"Unable to compact the log. {e}")
        return str(value)


# Instantiate only one IDAMTokenManager instance per ccdFunctions import.
idam_token_mgr = IDAMTokenManager(env="sbox")
s2s_manager = S2S_Manager(env="sbox")


def start_case_creation(ccd_base_url, uid, jid, ctid, etid, idam_token, s2s_token):
    start_case_endpoint = f"/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/event-triggers/{etid}/token"
    start_case_creation_url = f"{ccd_base_url}{start_case_endpoint}"

    print(f"CCD start creation url: {start_case_creation_url}")

    headers = {
        "Authorization": f"Bearer {idam_token}",  # IDAM user JWT
        "ServiceAuthorization": f"{s2s_token}",  # service-to-service JWT
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    try:
        response = requests.get(start_case_creation_url, headers=headers)
        print(f"🔢 Response status: {response.status_code}:{_compact(_get_res_body_as_text(response))}")
        return response
    except Exception as e:
        print(f"❌ Network error while calling {start_case_creation_url}: {e}")
        raise


def validate_case(ccd_base_url, event_token, payloadData, jid, ctid, idam_token, uid, s2s_token):
    validate_case_endpoint = f"/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/validate"
    validate_case_url = f"{ccd_base_url}{validate_case_endpoint}"

    print(f"CCD validate url: {validate_case_url}")

    headers = {
        "Authorization": f"Bearer {idam_token}",        # IDAM user JWT
        "ServiceAuthorization": f"{s2s_token}",  # service-to-service JWT
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    if isinstance(payloadData, str):
        try:
            payloadData = json.loads(payloadData)
        except json.JSONDecodeError as e:
            print(f"❌ Error decoding payloadData JSON string: {e}")

    try:
        json_object = {
            "data": payloadData,
            "event": {"id": "ariaCreateCase"},
            "event_token": event_token,
            "ignore_warning": True,
        }

        caseNo = json_object.get("data", {}).get("appealReferenceNumber", "N/A")
        print(f"🔢 Validate posting payload for {caseNo}: validate_case_url = {validate_case_url} headers = {_compact(headers)} json = {_compact(json_object)}")

        response = requests.post(validate_case_url, headers=headers, json=json_object)

        print(f"🔢 Validate Response for {caseNo}= {response.status_code}: {_compact(_get_res_body_as_text(response))}")
        return response

    except Exception as e:
        print(f"❌ Network error while calling {validate_case_url}: {e}")
        raise


def submit_case(ccd_base_url, event_token, payloadData, jid, ctid, idam_token, uid, s2s_token):
    submit_case_endpoint = f"/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/cases"
    submit_case_url = ccd_base_url + submit_case_endpoint

    print(f"CCD submission url: {submit_case_url}")

    headers = {
        "Authorization": f"Bearer {idam_token}",        # IDAM user JWT
        "ServiceAuthorization": f"{s2s_token}",  # service-to-service JWT
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    if isinstance(payloadData, str):
        try:
            payloadData = json.loads(payloadData)
        except json.JSONDecodeError as e:
            print(f"❌ Error decoding payloadData JSON string: {e}")

    print("🎁 payload type recieved for submission:", type(payloadData))

    try:
        json_object = {
            "data": payloadData,
            "event": {"id": "ariaCreateCase"},
            "event_token": event_token,
            "ignore_warning": True,
        }

        caseNo = json_object.get("data", {}).get("appealReferenceNumber", "N/A")
        print(f"🔢 Submit payload for {caseNo}: submit_case_url = {submit_case_url} headers = {_compact(headers)} json = {_compact(json_object)}")

        response = requests.post(submit_case_url, headers=headers, json=json_object)

        print(f"🔢 Submit Response status for {caseNo}: {response.status_code}:{_compact(_get_res_body_as_text(response))}")
        return response

    except Exception as e:
        print(f"❌ Network error while calling {submit_case_url}: {e}")
        raise


def process_case(env, caseNo, payloadData, runId, state, PR_REFERENCE):
    print(f"Starting processing case for {caseNo}")

    try:
        idam_token, uid = idam_token_mgr.get_token()

    except Exception as e:
        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "State": state,
            "Status": "ERROR",
            "StatusCode": getattr(e, "status_code", None),
            "ErrorType": type(e).__name__,
            "Error": f"failed to gather IDAM token: {e}",
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
        }
        return result

    try:
        s2s_token = s2s_manager.get_token()
    except Exception as e:
        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "State": state,
            "Status": "ERROR",
            "StatusCode": getattr(e, "status_code", None),
            "ErrorType": type(e).__name__,
            "Error": f"failed to gather s2s token: {e}",
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
        }
        return result

    jid = "IA"
    ctid = "Asylum"
    etid = "ariaCreateCase"

    urls = {
        "sbox": f"https://ccd-data-store-api-ia-case-api-{PR_REFERENCE}.preview.platform.hmcts.net",
        "stg": "http://ccd-data-store-api-aat.service.core-compute-aat.internal",
        "prod": None,
    }

    try:
        ccd_base_url = urls[env]
        print(f"URL for {urls}")

    except KeyError:
        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "State": state,
            "Status": "ERROR",
            "StatusCode": None,
            "Error": f"Invalid environment: {env}",
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
        }
        return result

    # start case creation

    print("Starting case creation")
    try:
        start_response = start_case_creation(ccd_base_url, uid, jid, ctid, etid, idam_token, s2s_token)
    except Exception as e:
        print(f"❌ Case creation failed with exception: {e}")
        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "State": state,
            "Status": "ERROR",
            "StatusCode": "N/A",
            "ErrorType": type(e).__name__,
            "Error": f"Case creation failed: {e}",
            "EndDateTime": datetime.now(timezone.utc).isoformat()
        }
        return result

    print(f"Started case creation = {_compact(start_response)}")

    if start_response is None or start_response.status_code != 200:
        status_code = start_response.status_code if start_response is not None else "N/A"
        text = _get_res_body_as_text(start_response) if start_response is not None else "No response from API"

        print(f"Case creation failed: {status_code} - {text}")

        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "State": state,
            "Status": "ERROR",
            "StatusCode": start_response.status_code if start_response is not None else None,
            "ErrorType": None,
            "Error": f"Case creation failed: {status_code} - {text}",
            "EndDateTime": datetime.now(timezone.utc).isoformat()
        }
        return result

    event_token = start_response.json()["token"]
    start_response_data = json.dumps(start_response.json() or {})
    print(f"Case creation started for case {caseNo} with event token {event_token}")

    # validate case
    print("Starting validate case")
    try:
        validate_case_response = validate_case(ccd_base_url, event_token, payloadData, jid, ctid, idam_token, uid, s2s_token)
    except Exception as e:
        print(f"❌ Case validation failed with exception: {e}")
        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "State": state,
            "Status": "ERROR",
            "StatusCode": "N/A",
            "ErrorType": type(e).__name__,
            "Error": f"Case validation failed: {e}",
            "EndDateTime": datetime.now(timezone.utc).isoformat()
        }
        return result

    try:
        print(f"Validation response for case {caseNo}: {_compact(validate_case_response.json())}")
    except Exception:
        try:
            print(_compact(_get_res_body_as_text(validate_case_response)))
        except Exception:
            print(f"Unable to parse validate_case_response for case {caseNo}")

    if validate_case_response is None or validate_case_response.status_code not in {201, 200}:
        error_type = None
        if validate_case_response is not None:
            status_code = validate_case_response.status_code
            text = _get_res_body_as_text(validate_case_response)
        else:
            status_code = "N/A"
            text = "No response from API"

        print(f"Case validation failed: {status_code} - {text}")

        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "State": state,
            "Status": "ERROR",
            "StatusCode": validate_case_response.status_code if validate_case_response is not None else None,
            "ErrorType": error_type,
            "Error": f"Case validation failed: {status_code} - {text}",
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "StartResponse": start_response_data
        }
        return result

    else:
        print(f"Validation passed for case {caseNo}")

    # submit case
    print("Starting submit case")
    try:
        submit_case_response = submit_case(ccd_base_url, event_token, payloadData, jid, ctid, idam_token, uid, s2s_token)
    except Exception as e:
        print(f"❌ Case submission failed with exception: {e}")
        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "State": state,
            "Status": "ERROR",
            "StatusCode": "N/A",
            "ErrorType": type(e).__name__,
            "Error": f"Case submission failed: {e}",
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "StartResponse": start_response_data
        }
        print(f"Case {caseNo} submission failed.")
        return result

    try:
        print(f"Submit response for case {caseNo}: {_compact(submit_case_response.json())}")
    except Exception:
        try:
            print(_compact(_get_res_body_as_text(submit_case_response)))
        except Exception:
            print(f"Unable to parse submit_case_response for case {caseNo}")

    if submit_case_response is None or submit_case_response.status_code not in {201, 200}:
        error_type = None
        if submit_case_response is not None:
            status_code = submit_case_response.status_code
            text = _get_res_body_as_text(submit_case_response)
        else:
            status_code = "N/A"
            text = "No response from API"

        print(f"Case submission failed: {status_code} - {text}")

        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "State": state,
            "Status": "ERROR",
            "StatusCode": submit_case_response.status_code if submit_case_response is not None else None,
            "ErrorType": error_type,
            "Error": f"Case submission failed: {status_code} - {text}",
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "StartResponse": start_response_data
        }
        print(f"Case {caseNo} submission failed.")
        return result
    else:
        try:
            submit_json = submit_case_response.json()
            ccd_case_id = submit_json.get("id")
            success_response = json.dumps(submit_json or {})
        except Exception as parse_error:
            ccd_case_id = f"Unable to parse CCDCaseID: {parse_error}"
            success_response = _get_res_body_as_text(submit_case_response)

        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "State": state,
            "Status": "SUCCESS",
            "StatusCode": submit_case_response.status_code,
            "Error": None,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "CCDCaseID": ccd_case_id,
            "SuccessResponse": success_response,
            "StartResponse": start_response_data
        }
        print(f"✅ Case {caseNo} submitted successfully with CCD Case ID: {result.get('CCDCaseID', 'N/A')}")
        return result
