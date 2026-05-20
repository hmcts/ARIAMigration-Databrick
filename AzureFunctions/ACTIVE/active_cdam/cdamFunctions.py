import json
import requests
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone
from urllib.parse import urlparse
try:
    from .retry_decorator import retry_on_result
except ImportError:
    from retry_decorator import retry_on_result

# tokenManager lives in the same package. When this module is imported by the
# Functions host the package root will be `AzureFunctions.ACTIVE.active_cdam`.
# Use a robust import that works both when running under the Functions host
# (package import) and when running the module directly (script import).
try:
    # package import when running under Functions host
    from .cdam_tokenManager import IDAMTokenManager, S2S_Manager
except Exception:
    # fallback when running as a script in the same folder
    from cdam_tokenManager import IDAMTokenManager, S2S_Manager

# Instantiate only one IDAMTokenManager / S2SManager instance per ccdFunctions import.
idam_token_mgr = IDAMTokenManager(env="sbox")
s2s_manager = S2S_Manager(env="sbox")


def upload_document(cdam_base_url, jid, ctid, cid, file_name, doc_binary, content_type, idam_token, s2s_token):
    upload_document_endpoint = "/cases/documents"
    upload_document_url = f"{cdam_base_url}{upload_document_endpoint}"

    headers = {
        "Authorization": f"Bearer {idam_token}",  # IDAM user JWT
        "ServiceAuthorization": f"{s2s_token}",  # service-to-service JWT
        "Accept": "application/json"
    }

    try:
        body = {
            "classification": "PUBLIC",
            "caseTypeId": ctid,
            "jurisdictionId": jid
        }

        files = [
            ("files", (file_name, doc_binary, content_type))
        ]

        print(f"🔢 Uploading document for CaseNo {cid}: upload_document_url = {upload_document_url}, headers = {headers}, body = {body}\n")

        response = requests.post(upload_document_url, headers=headers, data=body, files=files)

        print(f"🔢 Upload document response status for {cid}: {response.status_code}:{response.text}\n")
        return response

    except Exception as e:
        print(f"❌ Network error while calling {upload_document_url}: {e}")
        return None


@retry_on_result(
    max_retries=2,
    base_delay=30,
    max_delay=60,
    retry_on=lambda r: isinstance(r, dict) and r.get("Status") == "ERROR",
)
def process_event(env, caseNo, runId, file_name, file_url, file_content_type, storage_credential):
    print(f"Starting document upload for {caseNo} for {file_name} using file path {file_url} with content type {file_content_type}")

    startDateTime = datetime.now(timezone.utc).isoformat()

    try:
        idam_token, _ = idam_token_mgr.get_token()
    except Exception as e:
        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "ERROR",
            "Error": f"failed to gather IDAM token: {e}",
            "CDAMResponse": ""
        }
        return result

    try:
        s2s_token = s2s_manager.get_token()
    except Exception as e:
        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "ERROR",
            "Error": f"failed to gather s2s token: {e}",
            "CDAMResponse": ""
        }
        return result

    jid = "IA"
    ctid = "Asylum"

    urls = {
        "sbox": "http://ccd-case-document-am-api-aat.service.core-compute-aat.internal",  # Default preview instances connect to AAT DM API.
        "stg": "http://ccd-case-document-am-api-aat.service.core-compute-aat.internal",
        "prod": None,
    }

    try:
        cdam_base_url = urls[env]
        print(f"URL for {urls}")

    except KeyError:
        raise ValueError("Invalid environment")

    print(f"Getting binary for document at path: {file_url}")
    try:
        parsed_path = urlparse(file_url)
        account_url = f"{parsed_path.scheme}://{parsed_path.netloc}"

        blob_path = parsed_path.path.lstrip("/")
        container, _, blob = blob_path.partition("/")

        blob_service_client = BlobServiceClient(account_url, credential=storage_credential)
        print("Successfully authenticated with storage account {account_url}.")
        file_binary_in_bytes = blob_service_client.get_blob_client(container=container, blob=blob).download_blob().readall()
    except Exception as e:
        print(f"Error in downloading document binary: {e}")
        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "ERROR",
            "Error": f"Failed to read given blob: {e}",
            "CDAMResponse": ""
        }

        return result
    print(f"Downloaded binary for document at path: {file_url}")

    # submit case
    print("Starting CDAM upload")
    upload_document_response = upload_document(cdam_base_url, jid, ctid, caseNo, file_name, file_binary_in_bytes, file_content_type, idam_token, s2s_token)

    try:
        print(f"CDAM upload for case {caseNo}: {json.dumps(upload_document_response.json(), indent=2)}")
    except Exception:
        try:
            print(upload_document_response.text)
        except Exception:
            print(f"Unable to parse upload_document_response for case {caseNo}")

    if upload_document_response is None or upload_document_response.status_code not in {201, 200}:
        if upload_document_response is not None:
            status_code = upload_document_response.status_code
            text = upload_document_response.text
        else:
            status_code = "N/A"
            text = "No response from API"

        print(f"Document upload failed: {status_code} - {text}")

        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "ERROR",
            "Error": f"Document upload failed: {status_code} - {text}",
            "CDAMResponse": ""
        }

        return result

    else:
        result = {
            "RunID": runId,
            "CaseNo": caseNo,
            "StartDateTime": startDateTime,
            "EndDateTime": datetime.now(timezone.utc).isoformat(),
            "Status": "SUCCESS",
            "Error": None,
            "CDAMResponse": upload_document_response.json()
        }

        print(f"✅ Case {caseNo} document uploaded successfully with document response: {result['CDAMResponse']}")
        return result
