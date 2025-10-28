import requests
from AzureFunctions.Active.active_ccd.tokenManager import IDAMTokenManager,S2S_Manager
from datetime import datetime, timezone, timedelta
import json



def start_case_creation(ccd_base_url,uid,jid,ctid,etid,idam_token,s2s_token):


    start_case_endpoint = f"/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/event-triggers/{etid}/token"

    start_case_creation_url = f"{ccd_base_url}{start_case_endpoint}"

    headers = {
    "Authorization": f"Bearer {idam_token}",        # IDAM user JWT
    "ServiceAuthorization": f"{s2s_token}",  # service-to-service JWT
    "Accept": "application/json"
    }
    try:
        response = requests.get(start_case_creation_url,headers=headers)
        return response
    except Exception as e:
        print(f"❌ Network error while calling {start_case_creation_url}: {e}")
        return None
    
def validate_case(ccd_base_url,event_token, payloadData,jid,ctid,idam_token,uid,s2s_token):

    validate_case_endpoint = f"/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/validate"

    validate_case_url = f"{ccd_base_url}{validate_case_endpoint}"

    headers = {
    "Authorization": f"Bearer {idam_token}",        # IDAM user JWT
    "ServiceAuthorization": f"{s2s_token}",  # service-to-service JWT
    "Accept": "application/json"
    }


    # json_data = {
    # "data": payloadData,
    # "event": {"id":"ariaCreateCase"},
    # "event_token": event_token, 
    # "ignore_warning": True
    # }

    try:
        response = requests.post(validate_case_url,headers=headers,json={
    "data": payloadData,
    "event": {"id":"ariaCreateCase"},
    "event_token": event_token, 
    "ignore_warning": True
    }
    )
        return response
    except Exception as e:
        print(f"❌ Network error while calling {validate_case_url}: {e}")
        return None


def submit_case(ccd_base_url,event_token, payloadData,jid,ctid,idam_token,uid,s2s_token):

    submit_case_endpoint = f"/caseworkers/{uid}/jurisdictions/{jid}/case-types/{ctid}/cases" 

    headers = {
    "Authorization": f"Bearer {idam_token}",        # IDAM user JWT
    "ServiceAuthorization": f"{s2s_token}",  # service-to-service JWT
    "Accept": "application/json"
    }

    submit_case_url = ccd_base_url + submit_case_endpoint 
    try:
        response = requests.post(submit_case_url,headers=headers,json={
    "data": payloadData,
    "event": {"id":"ariaCreateCase"},
    "event_token": event_token, 
    "ignore_warning": True
    })
        return response
    except Exception as e:
        print(f"❌ Network error while calling {submit_case_url}: {e}")
        return None



### caseNo = event.key, payloadData = event.value


def process_case(env,caseNo,payloadData,runId,state,PR_NUMBER=2811):

    try:
        idam_token_mgr = IDAMTokenManager(env="sbox")
        idam_token,uid = idam_token_mgr.get_token()
    except Exception as e:
        result = {
            "RunID": runId,
            "caseNo": caseNo,
            "State": state,
            "status": "ERROR",
            "error": f"failed to gather IDAM token: {e}",
            "end_date_time": datetime.now(timezone.utc).isoformat()
            }
        return result
    try:
        s2s_manager = S2S_Manager("sbox",21)
        s2s_token = s2s_manager.get_token()
    except Exception as e:
        result = {
            "RunID": runId,
            "caseNo": caseNo,
            "State": state,
            "status": "ERROR",
            "error": f"failed to gather s2s token: {e}",
            "end_date_time": datetime.now(timezone.utc).isoformat()
            }
        return result

    jid = "IA"
    ctid = "Asylum"
    etid = "ariaCreateCase"

    urls = {
        "sbox":f"https://ccd-data-store-api-ia-case-api-pr-{PR_NUMBER}.preview.platform.hmcts.net",
        "stg":None,
        "prod":None
    }

    try:
        ccd_base_url = urls[env]
    except KeyError:
        raise ValueError("Invalid environment")

    ## start case creation

    start_response = start_case_creation(ccd_base_url,uid,jid,ctid,etid,idam_token,s2s_token)

    if start_response is None or start_response.status_code != 200 :

        status_code = start_response.status_code if start_response else "N/A"
        text = start_response.text if start_response else "No response from API"

        print(f"Case creation failed: {status_code} - {text}")

        result = {
        "RunID": runId,
        "caseNo": caseNo,
        "State": state,
        "status": "ERROR",
        "error": f"Case creation failed: {status_code} - {text}",
        "end_date_time": datetime.now(timezone.utc).isoformat()
        }
        return result
    else:

        event_token = start_response.json()["token"]
        print(f"Case creation started for case {caseNo} with event token {event_token}")

    # validate case

    validate_case_response = validate_case(ccd_base_url,event_token, payloadData,jid,ctid,idam_token,uid,s2s_token)
    print(f"Validation response for case {caseNo}: {validate_case_response.status_code}")
    try:
        print(json.dumps(validate_case_response.json(), indent=2))
    except Exception:
        print(validate_case_response.text)


    if validate_case_response is None or validate_case_response.status_code != 200:
        

        status_code = validate_case_response.status_code if validate_case_response else "N/A"
        text = validate_case_response.text if validate_case_response else "No response from API"

        print(f"Case validation failed: {status_code} - {text}")

        result = {
            "RunID": runId,
            "caseNo": caseNo,
            "State": state,
            "status": "EEROR", ### change this to the validate response code
            "error": f"Case validation failed: {status_code} - {text}",
            "end_date_time": datetime.now(timezone.utc).isoformat()
        }
        return result

    else:
        print(f"Validation passed for case {caseNo}")

    ## submit case
    submit_case_response = submit_case(ccd_base_url,event_token, payloadData,jid,ctid,idam_token,uid,s2s_token)
    if submit_case_response is None or submit_case_response.status_code != 201:

        status_code = submit_case_response.status_code if submit_case_response else "N/A"
        text = submit_case_response.text if submit_case_response else "No response from API"

        print(f"Case submission failed: {status_code} - {text}")

        result = {
            "RunID": runId,
            "caseNo": caseNo,
            "State": state,
            "status": "ERROR",
            "error": f"Case submission failed: {status_code} - {text}",
            "end_date_time": datetime.now(timezone.utc).isoformat()
        }
        print(f"Case {caseNo} submission failed.")
        return result

    else:
        result = {
            "RunID": runId,
            "caseNo": caseNo,
            "state": state,
            "status": "Success",
            "error": None,
            "end_date_time": datetime.now(timezone.utc).isoformat(),
            "ccd_case_id": submit_case_response.json()["id"]
        }
        print(f"Case {caseNo} submitted successfully with CCD Case ID: {submit_case_response.json()['id']}")
        return result








if __name__ == "__main__":

    payloadData = """
{
  "email": "example@test.com",
  "isEjp": "No",
  "feeCode": "FEE0238",
  "isAdmin": "Yes",
  "paidDate": "2024-08-05",
  "appealType": "refusalOfHumanRights",
  "feeVersion": "2",
  "paidAmount": "14000",
  "s94bStatus": "No",
  "paymentDate": "5 Aug 2024",
  "feeAmountGbp": "14000",
  "isIntegrated": "No",
  "appellantInUk": "Yes",
  "hearingCentre": "taylorHouse",
  "isNabaEnabled": "No",
  "paymentStatus": "Paid",
  "staffLocation": "Taylor House",
  "SearchCriteria": {
    "SearchParties": [
      {
        "id": "ec889f66-0475-4633-8d69-b31b80d76e5a",
        "value": {
          "Name": "GivenName Migration 3 FamilyName appealSubmitted",
          "PostCode": "SE10 0XX",
          "DateOfBirth": "2000-01-01",
          "AddressLine1": "Flat 101",
          "EmailAddress": "example@test.com"
        }
      }
    ],
    "OtherCaseReferences": [
      {
        "id": "65e7cf55-21c9-4d5b-af62-afd13222a8eb",
        "value": "HU/50009/2024"
      }
    ]
  },
  "feeDescription": "Appeal determined with a hearing",
  "feeWithHearing": "140",
  "searchPostcode": "SE10 0XX",
  "hasOtherAppeals": "No",
  "adminDeclaration1": [
    "hasDeclared"
  ],
  "appellantAddress": {
    "County": "",
    "Country": "United Kingdom",
    "PostCode": "SE10 0XX",
    "PostTown": "London",
    "AddressLine1": "Flat 101",
    "AddressLine2": "10 Cutter Lane",
    "AddressLine3": ""
  },
  "appellantPartyId": "45889c92-2cf4-4dae-ae9a-f64aa051d525",
  "ariaDesiredState": "appealSubmitted",
  "isAppellantMinor": "No",
  "isNabaAdaEnabled": "No",
  "isNabaEnabledOoc": "No",
  "hearingTypeResult": "No",
  "hmctsCaseCategory": "Human rights",
  "notificationsSent": [],
  "tribunalDocuments": [],
  "appealOutOfCountry": "No",
  "appellantStateless": "hasNationality",
  "legalRepFamilyName": "",
  "paymentDescription": "Appeal determined with a hearing",
  "appellantFamilyName": "FamilyName appealSubmitted",
  "appellantGivenNames": "GivenName Migration 3",
  "isFeePaymentEnabled": "Yes",
  "isRemissionsEnabled": "Yes",
  "submissionOutOfTime": "No",
  "appealSubmissionDate": "2024-08-07",
  "appellantDateOfBirth": "2000-01-01",
  "feePaymentAppealType": "Yes",
  "letterSentOrReceived": "Sent",
  "localAuthorityPolicy": {
    "Organisation": {},
    "OrgPolicyCaseAssignedRole": "[LEGALREPRESENTATIVE]"
  },
  "tribunalReceivedDate": "2024-08-05",
  "additionalPaymentInfo": "Additional paid information",
  "appealReferenceNumber": "HU/50009/2024",
  "caseNameHmctsInternal": "GivenName Migration 3 FamilyName appealSubmitted",
  "hmctsCaseNameInternal": "GivenName Migration 3 FamilyName appealSubmitted",
  "isOutOfCountryEnabled": "Yes",
  "appellantNationalities": [
    {
      "id": "520cd556-39b3-4729-9093-a07513f4b03e",
      "value": {
        "code": "GB"
      }
    }
  ],
  "caseManagementCategory": {
    "value": {
      "code": "refusalOfHumanRights",
      "label": "Refusal of a human rights claim"
    },
    "list_items": [
      {
        "code": "refusalOfHumanRights",
        "label": "Refusal of a human rights claim"
      }
    ]
  },
  "caseManagementLocation": {
    "region": "1",
    "baseLocation": "765324"
  },
  "homeOfficeDecisionDate": "2024-08-05",
  "internalAppellantEmail": "example@test.com",
  "appealGroundsForDisplay": [],
  "appellantsRepresentation": "Yes",
  "appellantNameForDisplay": "GivenName Migration 3 FamilyName appealSubmitted",
  "deportationOrderOptions": "No",
  "uploadTheAppealFormDocs": [],
  "appellantHasFixedAddress": "Yes",
  "decisionHearingFeeOption": "decisionWithHearing",
  "hasServiceRequestAlready": "No",
  "homeOfficeReferenceNumber": "012345678",
  "isDlrmFeeRemissionEnabled": "Yes",
  "legalRepIndividualPartyId": "f7159136-7bff-40fb-921a-c8a53633afc8",
  "legalRepOrganisationPartyId": "71c50709-b802-42c7-ac56-2ef03e6e14e7",
  "appealSubmissionInternalDate": "2024-08-07",
  "ccdReferenceNumberForDisplay": "1723 0197 9804 1350",
  "legalRepresentativeDocuments": [],
  "sendDirectionActionAvailable": "Yes",
  "uploadTheNoticeOfDecisionDocs": [],
  "automaticEndAppealTimedEventId": "fd614594-6b6b-4116-8568-f0d80298486e",
  "currentCaseStateVisibleToJudge": "appealSubmitted",
  "currentCaseStateVisibleToCaseOfficer": "appealSubmitted",
  "changeDirectionDueDateActionAvailable": "No",
  "currentCaseStateVisibleToAdminOfficer": "appealSubmitted",
  "markEvidenceAsReviewedActionAvailable": "No",
  "uploadAddendumEvidenceActionAvailable": "No",
  "currentCaseStateVisibleToHomeOfficeAll": "appealSubmitted",
  "currentCaseStateVisibleToHomeOfficeApc": "appealSubmitted",
  "currentCaseStateVisibleToHomeOfficePou": "appealSubmitted",
  "currentCaseStateVisibleToHomeOfficeLart": "appealSubmitted",
  "uploadAdditionalEvidenceActionAvailable": "No",
  "applicationChangeDesignatedHearingCentre": "taylorHouse",
  "currentCaseStateVisibleToHomeOfficeGeneric": "appealSubmitted",
  "haveHearingAttendeesAndDurationBeenRecorded": "No",
  "currentCaseStateVisibleToLegalRepresentative": "appealSubmitted",
  "markAddendumEvidenceAsReviewedActionAvailable": "No",
  "uploadAddendumEvidenceLegalRepActionAvailable": "No",
  "isServiceRequestTabVisibleConsideringRemissions": "Yes",
  "uploadAddendumEvidenceHomeOfficeActionAvailable": "No",
  "uploadAddendumEvidenceAdminOfficerActionAvailable": "No",
  "uploadAdditionalEvidenceHomeOfficeActionAvailable": "No",
  "remissionType": "hoWaiverRemission",
  "ariaMigrationTaskDueDays": "2"
} """


