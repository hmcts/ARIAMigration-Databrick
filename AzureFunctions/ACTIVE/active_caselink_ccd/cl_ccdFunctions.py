import json
import requests
from datetime import datetime, timezone


def _compact(value) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    text = str(value)
    return text.replace("\r\n", "\\n").replace("\r", "\\n").replace("\n", "\\n")


def _get_res_body_as_text(response) -> str:
    try:
        return str(response.text)
    except Exception:
        try:
            return (getattr(response, 'content', None) or b'').decode('utf-8', errors='replace')
        except Exception:
            return 'Unable to get response body as text'


# tokenManager lives in the same package. When this module is imported by the
# Functions host the package root will be `AzureFunctions.ACTIVE.active_ccd`.
# Use a robust import that works both when running under the Functions host
# (package import) and when running the module directly (script import).
try:
    # package import when running under Functions host
    from .cl_tokenManager import IDAMTokenManager, S2S_Manager
except Exception:
    # fallback when running as a script in the same folder
    from cl_tokenManager import IDAMTokenManager, S2S_Manager

# Instantiate only one IDAMTokenManager instance per ccdFunctions import.
idam_token_mgr = IDAMTokenManager(env="sbox")
s2s_manager = S2S_Manager(env="sbox")


def get_case_details(ccd_base_url, uid, jid, ctid, cid, idam_token, s2s_token):
    get_case_endpoint = f"/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/cases/{cid}"
    get_case_url = f"{ccd_base_url}{get_case_endpoint}"

    headers = {
        "Authorization": f"Bearer {idam_token}",  # IDAM user JWT
        "ServiceAuthorization": f"{s2s_token}",  # service-to-service JWT
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(get_case_url, headers=headers)
        print(f"🔢 Get Case Details Response status: {response.status_code}:{_compact(_get_res_body_as_text(response))}")
        return response
    except Exception as e:
        print(f"❌ Network error while calling {get_case_url}: {e}")
        return None


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
        print(f"🔢 Start Case Event Response status: {response.status_code}:{_compact(_get_res_body_as_text(response))}")
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

        print(f"🔢 Validate posting payload for {cid}: validate_case_url = {validate_case_url} headers = {_compact(headers)} json = {_compact(json_object)}")

        response = requests.post(validate_case_url, headers=headers, json=json_object)

        print(f"🔢 Validate Response for {cid} = {response.status_code}: {_compact(_get_res_body_as_text(response))}")
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

    print(f"🎁 payload recieved for submission: {type(payloadData)}")

    try:
        json_object = {
            "data": payloadData,
            "event": {"id": f"{etid}"},
            "event_token": event_token,
            "ignore_warning": True,
        }

        print(f"🔢 Submit payload for {cid}: submit_case_url = {submit_event_url} headers = {_compact(headers)} json = {_compact(json_object)}")

        response = requests.post(submit_event_url, headers=headers, json=json_object)

        print(f"🔢 Submit Response status for {cid}: {response.status_code}:{_compact(_get_res_body_as_text(response))}")
        return response

    except Exception as e:
        print(f"❌ Network error while calling {submit_event_url}: {e}")
        return None


def process_event(env, ccdReference, runId, caseLinkPayload, PR_REFERENCE, overwrite=False):
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
            "StatusCode": getattr(e, "status_code", None),
            "Error": f"failed to gather IDAM token: {e}"
        }
        return result

    try:
        s2s_token = s2s_manager.get_token()
    except Exception as e:
        result = {
            "RunID": runId,
            "CCDCaseReferenceNumber": ccdReference,
            "CaseLinkCount": 0,
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "ERROR",
            "StatusCode": getattr(e, "status_code", None),
            "Error": f"failed to gather s2s token: {e}"
        }
        return result

    jid = "IA"
    ctid = "Asylum"
    etid = "createCaseLink"

    urls = {
        "sbox": f"https://ccd-data-store-api-ia-case-api-{PR_REFERENCE}.preview.platform.hmcts.net",
        "stg": "http://ccd-data-store-api-aat.service.core-compute-aat.internal",
        "prod": None,
    }

    try:
        ccd_base_url = urls[env]
        print(f"URL for {urls}")

    except KeyError:
        raise ValueError("Invalid environment")

    # # Not yet required. No issue with duplicate linking events at the moment.
    # # compare existing case link details if not overwriting
    # if not overwrite:
    #     print("Checking existing case link data")
    #     case_details = get_case_details(ccd_base_url, uid, jid, ctid, ccdReference, idam_token, s2s_token)
    #     existingCaseLinks = (case_details.json().get("case_data") or {}).get("caseLinks", [])
    #     if (existingCaseLinks == caseLinkPayload.get("caseLinks", [])):
    #         return {
    #             "RunID": runId,
    #             "CCDCaseReferenceNumber": ccdReference,
    #             "CaseLinkCount": len(existingCaseLinks),
    #             "StartDateTime": startDateTime,
    #             "EndDateTime": datetime.now(timezone.utc).isoformat(),
    #             "Status": "SKIPPED",
    #             "ERROR": None
    #         }

    # start case creation
    print("Starting case event")
    start_response = start_case_event(ccd_base_url, uid, jid, ctid, ccdReference, etid, idam_token, s2s_token)
    print(f"Start response for case {ccdReference}: {start_response.status_code if start_response is not None else 'None'}")

    if start_response is None or start_response.status_code != 200:
        if start_response is not None:
            status_code = start_response.status_code
            text = _get_res_body_as_text(start_response)
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
            "StatusCode": start_response.status_code if start_response is not None else None,
            "Error": f"Case link event failed: {status_code} - {text}"
        }
        return result

    else:
        event_token = start_response.json()["token"]
        print(f"Case creation started for case {ccdReference} with event token {event_token}")

    # validate case
    print("Starting case validation")
    validate_case_response = validate_case(ccd_base_url, uid, jid, ctid, ccdReference, etid, event_token, caseLinkPayload, idam_token, s2s_token)

    try:
        print(f"Validation response for case {ccdReference}: {_compact(validate_case_response.json())}")
    except Exception:
        try:
            print(_compact(_get_res_body_as_text(validate_case_response)))
        except Exception:
            print(f"Unable to parse validate_case_response for case {ccdReference}")

    if validate_case_response is None or validate_case_response.status_code not in {201, 200}:
        if validate_case_response is not None:
            status_code = validate_case_response.status_code
            text = _get_res_body_as_text(validate_case_response)
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
            "StatusCode": validate_case_response.status_code if validate_case_response is not None else None,
            "Error": f"Case link validation failed: {status_code} - {text}",
        }
        return result

    else:
        print(f"Validation passed for case {ccdReference}")

    # submit case
    print("Starting case submission")
    submit_case_response = submit_case_event(ccd_base_url, uid, jid, ctid, ccdReference, etid, event_token, caseLinkPayload, idam_token, s2s_token)

    try:
        print(f"Submit response for case {ccdReference}: {_compact(submit_case_response.json())}")
    except Exception:
        try:
            print(_compact(_get_res_body_as_text(submit_case_response)))
        except Exception:
            print(f"Unable to parse submit_case_response for case {ccdReference}")

    if submit_case_response is None or submit_case_response.status_code not in {201, 200}:
        if submit_case_response is not None:
            status_code = submit_case_response.status_code
            text = _get_res_body_as_text(submit_case_response)
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
            "StatusCode": submit_case_response.status_code if submit_case_response is not None else None,
            "Error": f"Case link submission failed: {status_code} - {text}",
        }

        return result

    else:
        result = {
            "RunID": runId,
            "CCDCaseReferenceNumber": ccdReference,
            "CaseLinkCount": len((submit_case_response.json().get("case_data") or {}).get("caseLinks", [])),
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "SUCCESS",
            "StatusCode": submit_case_response.status_code,
            "Error": None
        }

        print(f"✅ Case {ccdReference} submitted successfully with CCD Case ID: {submit_case_response.json()['id']}")
        return result
