import json
import logging
import requests
from datetime import datetime, timezone

# tokenManager lives in the same package. When this module is imported by the
# Functions host the package root will be `AzureFunctions.ACTIVE.active_ccd`.
# Use a robust import that works both when running under the Functions host
# (package import) and when running the module directly (script import).
try:
    # package import when running under Functions host
    from .cl_tokenManager import IDAMTokenManager, S2S_Manager
except Exception:
    # fallback when running as a script in the same folder
    from AzureFunctions.ACTIVE.active_caselink_ccd.cl_tokenManager import IDAMTokenManager, S2S_Manager

logger = logging.getLogger(__name__)

# Instantiate only one IDAMTokenManager instance per ccdFunctions import.
idam_token_mgr = IDAMTokenManager(env="sbox")


def start_case_event(ccd_base_url, uid, jid, ctid, cid, etid, idam_token, s2s_token):
    start_event_endpoint = f"/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/cases/{cid}/event-triggers/{etid}/token"
    start_event_url = f"{ccd_base_url}{start_event_endpoint}"

    headers = {
        "Authorization": f"Bearer {idam_token}",  # IDAM user JWT
        "ServiceAuthorization": f"{s2s_token}",  # service-to-service JWT
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(start_event_url, headers=headers)
        print(f"🔢 Response status: {response.status_code}:{response.text}")
        return response
    except Exception as e:
        print(f"❌ Network error while calling {start_event_url}: {e}")
        return None


def validate_case(ccd_base_url, uid, jid, ctid, cid, etid, event_token, payloadData, idam_token, s2s_token):
    validate_case_endpoint = f"/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/validate"
    validate_case_url = f"{ccd_base_url}{validate_case_endpoint}"

    headers = {
        "Authorization": f"Bearer {idam_token}",  # IDAM user JWT
        "ServiceAuthorization": f"{s2s_token}",  # service-to-service JWT
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    if isinstance(payloadData, str):
        try:
            payloadData = json.loads(payloadData)
        except json.JSONDecodeError as e:
            print(f"❌ Error decoding payloadData JSON string: {e}")

    try:
        json_object = {
            "data": payloadData,
            "event": {"id": f"{etid}"},
            "event_token": event_token,
            "ignore_warning": True,
        }

        print(f"🔢 Validate posting payload for {cid}: validate_case_url = {validate_case_url} headers = {headers} json = {json_object}")

        response = requests.post(validate_case_url, headers=headers, json=json_object)

        print(f"🔢 Validate Response for {cid} = {response.status_code}: {response.text}")
        return response

    except Exception as e:
        print(f"❌ Network error while calling {validate_case_url}: {e}")
        return None


def submit_case_event(ccd_base_url, uid, jid, ctid, cid, etid, event_token, payloadData, idam_token, s2s_token):
    submit_event_endpoint = f"/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/cases/{cid}/events"
    submit_event_url = f"{ccd_base_url}{submit_event_endpoint}"

    headers = {
        "Authorization": f"Bearer {idam_token}",  # IDAM user JWT
        "ServiceAuthorization": f"{s2s_token}",  # service-to-service JWT
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    if isinstance(payloadData, str):
        try:
            payloadData = json.loads(payloadData)
        except json.JSONDecodeError as e:
            print(f"❌ Error decoding payloadData JSON string: {e}")

    print("🎁 payload recieved for submission:", type(payloadData))

    try:
        json_object = {
            "data": payloadData,
            "event": {"id": f"{etid}"},
            "event_token": event_token,
            "ignore_warning": True,
        }

        print(f"🔢 Submit payload for {cid}: submit_case_url = {submit_event_url} headers = {headers} json = {json_object}\n")

        response = requests.post(submit_event_url, headers=headers, json=json_object)

        print(f"🔢 Submit Response status for {cid}: {response.status_code}:{response.text}\n")
        return response

    except Exception as e:
        print(f"❌ Network error while calling {submit_event_url}: {e}")
        return None


# caseNo = event.key, payloadData = event.value
def process_event(env, ccdReference, runId, caseLinkPayload, PR_NUMBER):
    print(f"Starting processing case for {ccdReference}")

    startDateTime = datetime.now(timezone.utc).isoformat()

    try:
        idam_token, uid = idam_token_mgr.get_token()
    except Exception as e:
        result = {
            "RunID": runId,
            "CCDCaseReferenceNumber": ccdReference,
            "CaseLinkCount": 0,
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "ERROR",
            "Error": f"failed to gather s2s token: {e}"
        }
        return result

    try:
        s2s_manager = S2S_Manager("sbox", 21)
        s2s_token = s2s_manager.get_token()
    except Exception as e:
        result = {
            "RunID": runId,
            "CCDCaseReferenceNumber": ccdReference,
            "CaseLinkCount": 0,
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "ERROR",
            "Error": f"failed to gather IDAM token: {e}"
        }
        return result

    jid = "IA"
    ctid = "Asylum"
    etid = "createCaseLink"

    urls = {
        "sbox": f"https://ccd-data-store-api-ia-case-api-ynot-pr-1.preview.platform.hmcts.net",
        "stg": "http://ccd-data-store-api-aat.service.core-compute-aat.internal",
        "prod": None,
    }

    try:
        ccd_base_url = urls[env]
        print(f"URL for {urls}")

    except KeyError:
        raise ValueError("Invalid environment")

    # start case creation
    start_response = start_case_event(ccd_base_url, uid, jid, ctid, ccdReference, etid, idam_token, s2s_token)
    print("Starting case event")

    if start_response is None or start_response.status_code != 200:
        if start_response is not None:
            status_code = start_response.status_code
            text = start_response.text
        else:
            status_code = "N/A"
            text = "No response from API"

        print(f"Case event start failed: {status_code} - {text}")

        result = {
            "RunID": runId,
            "CCDCaseReferenceNumber": ccdReference,
            "CaseLinkCount": 0,
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "ERROR",
            "Error": f"Case link event failed: {status_code} - {text}"
        }
        return result

    else:
        event_token = start_response.json()["token"]
        print(f"Case creation started for case {ccdReference} with event token {event_token}")

    # validate case
    validate_case_response = validate_case(ccd_base_url, uid, jid, ctid, ccdReference, etid, event_token, caseLinkPayload, idam_token, s2s_token)

    print(f"Validation response for case {ccdReference}: {validate_case_response.status_code}")
    try:
        print(json.dumps(validate_case_response.json(), indent=2))
    except Exception:
        print(validate_case_response.text)

    if validate_case_response is None or validate_case_response.status_code not in {201, 200}:
        if validate_case_response is not None:
            status_code = validate_case_response.status_code
            text = validate_case_response.text
        else:
            status_code = "N/A"
            text = "No response from API"

        print(f"Case validation failed: {status_code} - {text}")

        result = {
            "RunID": runId,
            "CCDCaseReferenceNumber": ccdReference,
            "CaseLinkCount": 0,
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "ERROR",
            "Error": f"Case link validation failed: {status_code} - {text}",
        }
        return result

    else:
        print(f"Validation passed for case {ccdReference}")

    # submit case
    submit_case_response = submit_case_event(ccd_base_url, uid, jid, ctid, ccdReference, etid, event_token, caseLinkPayload, idam_token, s2s_token)
    print(f"Submit case response = {submit_case_response}")

    if submit_case_response is None or submit_case_response.status_code not in {201, 200}:
        if submit_case_response is not None:
            status_code = submit_case_response.status_code
            text = submit_case_response.text
        else:
            status_code = "N/A"
            text = "No response from API"

        print(f"Case submission failed: {status_code} - {text}")

        result = {
            "RunID": runId,
            "CCDCaseReferenceNumber": ccdReference,
            "CaseLinkCount": 0,
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "ERROR",
            "Error": f"Case link submission failed: {status_code} - {text}",
        }

        return result

    else:
        result = {
            "RunID": runId,
            "CCDCaseReferenceNumber": ccdReference,
            "CaseLinkCount": len(caseLinkPayload),
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "Success",
            "Error": None
        }
        print(submit_case_response.json())
        print(f"✅ Case {ccdReference} submitted successfully with CCD Case ID: {submit_case_response.json()['id']}")
        return result
