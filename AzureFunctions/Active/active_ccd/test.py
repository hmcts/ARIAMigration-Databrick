import requests
from datetime import datetime,timezone
from ccdFunctions import start_case_creation,validate_case,submit_case, process_case
from tokenManager import IDAMTokenManager,S2S_Manager
# import json

caseNo = "1723 0197 9804 1350"
env = "sbox"
payload_data = """
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
# payload_data = """
# {"appealType":"refusalOfHumanRights","hmctsCaseCategory":"Human rights","appealReferenceNumber":"HU/00005/2019","appealTypeDescription":"Refusal of a human rights claim","isAppealReferenceNumberAvailable":"Yes","ccdReferenceNumberForDisplay":"","appellantsRepresentation":"Yes","submissionOutOfTime":"No","hearingCentre":"newport","staffLocation":"Newport","caseManagementLocation":{"region":"1","baseLocation":"227101"},"hearingCentreDynamicList":{"value":{"code":"227101","label":"Newport Tribunal Centre - Columbus House"},"list_items":[{"code":"227101","label":"Newport Tribunal Centre - Columbus House"},{"code":"231596","label":"Birmingham Civil And Family Justice Centre"},{"code":"28837","label":"Harmondsworth Tribunal Hearing Centre"},{"code":"366559","label":"Atlantic Quay - Glasgow"},{"code":"366796","label":"Newcastle Civil And Family Courts And Tribunals Centre"},{"code":"386417","label":"Hatton Cross Tribunal Hearing Centre"},{"code":"512401","label":"Manchester Tribunal Hearing Centre - Piccadilly Exchange"},{"code":"649000","label":"Yarls Wood Immigration And Asylum Hearing Centre"},{"code":"698118","label":"Bradford Tribunal Hearing Centre"},{"code":"765324","label":"Taylor House Tribunal Hearing Centre"}]},"caseManagementLocationRefData":{"region":"1","baseLocation":{"value":{"code":"227101","label":"Newport Tribunal Centre - Columbus House"},"list_items":[{"code":"227101","label":"Newport Tribunal Centre - Columbus House"},{"code":"231596","label":"Birmingham Civil And Family Justice Centre"},{"code":"28837","label":"Harmondsworth Tribunal Hearing Centre"},{"code":"366559","label":"Atlantic Quay - Glasgow"},{"code":"366796","label":"Newcastle Civil And Family Courts And Tribunals Centre"},{"code":"386417","label":"Hatton Cross Tribunal Hearing Centre"},{"code":"512401","label":"Manchester Tribunal Hearing Centre - Piccadilly Exchange"},{"code":"649000","label":"Yarls Wood Immigration And Asylum Hearing Centre"},{"code":"698118","label":"Bradford Tribunal Hearing Centre"},{"code":"765324","label":"Taylor House Tribunal Hearing Centre"}]}},"selectedHearingCentreRefData":"Newport Tribunal Centre - Columbus House","adminDeclaration1":["hasDeclared"],"appealSubmissionDate":"2019-11-25","appealSubmissionInternalDate":"2019-11-25","tribunalReceivedDate":"2019-11-25","caseLinks":[],"hasOtherAppeals":"NotSure","appellantFamilyName":"Test Old Appeal Types 4","appellantGivenNames":"Frans","appellantNameForDisplay":"Frans Test Old Appeal Types 4","appellantDateOfBirth":"2019-03-09","isAppellantMinor":"Yes","caseNameHmctsInternal":"Frans Test Old Appeal Types 4","hmctsCaseNameInternal":"Frans Test Old Appeal Types 4","appellantStateless":"hasNationality","appellantNationalities":[{"id":"96158815-f28c-4eb6-99f3-6f2b1389cb41","value":{"code":"GH"}}],"appellantNationalitiesDescription":"Ghana","deportationOrderOptions":"Yes","caseFlags":{"details":[{"id":"74d6246c-d07a-425b-bc95-d221d28b3a41","value":{"name":"Other","path":[{"id":"d6579a82-d750-4ea4-a01d-83b8e1533865","value":"Case"}],"status":"Active","flagCode":"OT0001","flagComment":"Expedite","dateTimeCreated":"2025-11-11T12:29:16Z","hearingRelevant":"Yes"}}]},"appellantLevelFlags":{"details":[{"id":"3c156489-139d-4c27-a012-54cd5a7e8184","value":{"name":"Unaccompanied minor","path":[{"id":"c869119c-7535-4c5f-8574-cfe1e0e0c797","value":"Party"}],"status":"Active","flagCode":"PF0013","dateTimeCreated":"2025-11-11T12:29:16Z","hearingRelevant":"No"}}],"partyName":"Functional PostDeployment","roleOnCase":"Appellant"},"s94bStatus":"No","journeyType":"aip","isAdmin":"Yes","isAriaMigratedFeeExemption":"No","isEjp":"No","appellantPartyId":"d139d5b8-261c-4720-bfd5-345eb6fd5355","isHomeOfficeIntegrationEnabled":"Yes","homeOfficeNotificationsEligible":"Yes","remissionType":"noRemission","hasSponsor":"No","sponsorAuthorisation":"No","feeAmountGbp":"14000","feeDescription":"Appeal determined with a hearing","feeWithHearing":"140","paymentDescription":"Appeal determined with a hearing","feePaymentAppealType":"Yes","paymentStatus":"Payment Pending","feeVersion":"2","decisionHearingFeeOption":"decisionWithHearing","hasServiceRequestAlready":"No","isServiceRequestTabVisibleConsideringRemissions":"No","applicationChangeDesignatedHearingCentre":"newport","notificationsSent":[],"submitNotificationStatus":"","isFeePaymentEnabled":"Yes","isRemissionsEnabled":"Yes","isOutOfCountryEnabled":"Yes","isIntegrated":"No","isNabaEnabled":"No","isNabaAdaEnabled":"Yes","isNabaEnabledOoc":"No","isCaseUsingLocationRefData":"Yes","hasAddedLegalRepDetails":"Yes","autoHearingRequestEnabled":"No","isDlrmFeeRemissionEnabled":"Yes","isDlrmFeeRefundEnabled":"Yes","sendDirectionActionAvailable":"Yes","changeDirectionDueDateActionAvailable":"No","markEvidenceAsReviewedActionAvailable":"No","uploadAddendumEvidenceActionAvailable":"No","uploadAdditionalEvidenceActionAvailable":"No","displayMarkAsPaidEventForPartialRemission":"No","haveHearingAttendeesAndDurationBeenRecorded":"No","markAddendumEvidenceAsReviewedActionAvailable":"No","uploadAddendumEvidenceLegalRepActionAvailable":"No","uploadAddendumEvidenceHomeOfficeActionAvailable":"No","uploadAddendumEvidenceAdminOfficerActionAvailable":"No","uploadAdditionalEvidenceHomeOfficeActionAvailable":"No","uploadTheAppealFormDocs":[],"caseNotes":[],"tribunalDocuments":[],"legalRepresentativeDocuments":[],"ariaDesiredState":"pendingPayment","ariaMigrationTaskDueDays":"14"}

# """

jid = "IA"
ctid = "Asylum"
etid = "ariaCreateCase"
PR_NUMBER = "2866"

ccd_base_url = f"https://ccd-data-store-api-ia-case-api-pr-{PR_NUMBER}.preview.platform.hmcts.net"

try:
    idam_token_mgr = IDAMTokenManager(env="sbox")
    idam_token,uid = idam_token_mgr.get_token()
    print(f"✅ Successfully Recieved IDAM Token for UID: {uid}")
except Exception as e:
    result = {
        "runID": "get run id from event body",
        "caseNo": caseNo,
        "state": "Add state from event body",
        "status": "ERROR",
        "error": f"failed to gather IDAM token: {e}",
        "end_date_time": datetime.now(timezone.utc).isoformat()
        }
    print(result) 
try:
    s2s_manager = S2S_Manager("sbox",21)
    s2s_token = s2s_manager.get_token()
    print(f"✅ Successfully Recieved S2S Token: {s2s_token}")
except Exception as e:
    result = {
        "runID": "get run id from event body",
        "caseNo": caseNo,
        "state": "Add state from event body",
        "status": "ERROR",
        "error": f"failed to gather s2s token: {e}",
        "end_date_time": datetime.now(timezone.utc).isoformat()
        }
    print(result) 

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

# print("Ready to start case creation")

url = "https://ccd-data-store-api-aat.service.core-compute-aat.internal"
headers = None
response = requests.get(url)
print(response.status_code)

## process funciton teest

# result = process_case(env=env,caseNo=caseNo,payloadData=payload_data,state="paymentPending",runId=123,PR_NUMBER=PR_NUMBER)
# print(result)

# ## start case creation

# start_response = start_case_creation(ccd_base_url,uid,jid,ctid,etid,idam_token,s2s_token)
# print(f"Start case creation response for case {caseNo}: {start_response.status_code} {start_response.text}")

# if start_response is None or start_response.status_code != 200 :

#     status_code = start_response.status_code if start_response else "N/A"
#     text = start_response.text if start_response else "No response from API"

#     print(f"Case creation failed: {status_code} - {text}")

#     result = {
#     "runID": "get run id from event body",
#     "caseNo": caseNo,
#     "state": "Add state from event body",
#     "status": "ERROR",
#     "error": f"Case creation failed: {status_code} - {text}",
#     "end_date_time": datetime.now(timezone.utc).isoformat()
#     }
#     print(result) 
# else:

#     event_token = start_response.json()["token"]
#     print(f"Case creation started for case {caseNo} with event token {event_token}")

# # validate case

# validate_case_response = validate_case(ccd_base_url,event_token, payload_data,jid,ctid,idam_token,uid,s2s_token)
# print(f"Validation response for case {caseNo}: {validate_case_response.status_code}")

# if validate_case_response is None or validate_case_response.status_code != 200:

#     status_code = validate_case_response.status_code if validate_case_response else "N/A"
#     text = validate_case_response.text if validate_case_response else "No response from API"

#     print(f"Case validation failed: {status_code} - {text}")

#     result = {
#         "runID": "get run id from event body",
#         "caseNo": caseNo,
#         "state": "Add state from event body",
#         "status": "EEROR", ### change this to the validate response code
#         "error": f"Case validation failed: {status_code} - {text}",
#         "end_date_time": datetime.now(timezone.utc).isoformat()
#     }
#     print(result) 

# else:
#     print(f"Validation passed for case {caseNo}")

# # ## submit case
# submit_case_response = submit_case(ccd_base_url,event_token, payload_data,jid,ctid,idam_token,uid,s2s_token)
# print(f"Submit response for case {caseNo}: {submit_case_response.status_code}")

# if submit_case_response is None or submit_case_response.status_code != 201:

#     status_code = submit_case_response.status_code if submit_case_response else "N/A"
#     text = submit_case_response.text if submit_case_response else "No response from API"

#     print(f"Case submission failed: {status_code} - {text}")

#     result = {
#         "caseNo": caseNo,
#         "state": "Add state from event body",
#         "status": "ERROR",
#         "error": f"Case submission failed: {status_code} - {text}",
#         "end_date_time": datetime.now(timezone.utc).isoformat()
#     }
#     print(result) 

# else:
#     result = {
#         "runID": "get run id from event body",
#         "caseNo": caseNo,
#         "state": "Add state from event body",
#         "status": "Success",
#         "error": None,
#         "end_date_time": datetime.now(timezone.utc).isoformat(),
#         "ccd_case_id": submit_case_response.json()["id"]
#     }
#     print(result) 
