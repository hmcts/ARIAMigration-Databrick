DecidedBNonMandatoryTypes = {

    'dateEntryClearanceDecision': 'string',
    'rpDcAppealHearingOption': 'string',
    'asylumSupportReference': 'string', #not mandatory
    'feeRemissionType': 'string', #not mandatory
    'legalAidAccountNumber': 'string', #not mandatory
    'legalRepAddressUK': 'struct<AddressLine1:string,AddressLine2:string,Country:string,County:string,PostCode:string,PostTown:string>', #not mandatory
    'helpWithFeesReferenceNumber': 'string', #not mandatory
    'remissionClaim': 'string', #not mandatory
    'appellantInterpreterSignLanguage': 'struct<languageManualEntry:array<string>,languageRefData:struct<list_items:array<struct<code:string,label:string>>,value:struct<code:string,label:string>>>',
    'inCameraCourtDecisionForDisplay': 'string',
    'inCameraCourtDescription': 'string',
    'inCameraCourtTribunalResponse': 'string',
    'isInCameraCourtAllowed': 'string',
    'isSingleSexCourtAllowed': 'string',
    'singleSexCourtDecisionForDisplay': 'string',
    'singleSexCourtTribunalResponse': 'string',
    'singleSexCourtType': 'string',
    'singleSexCourtTypeDescription': 'string',
    'oocAddressLine1': 'string',
    'oocAddressLine2': 'string',
    'oocAddressLine3': 'string',
    'oocAddressLine4': 'string',
    'oocLrCountryGovUkAdminJ': 'string',
    'ftpaRespondentOutOfTimeExplanation': 'string',
    'isFtpaRespondentOotDocsVisibleInDecided': 'string',
    'isFtpaRespondentOotDocsVisibleInSubmitted': 'string',
    'isFtpaRespondentOotExplanationVisibleInDecided': 'string',
    'isFtpaRespondentOotExplanationVisibleInSubmitted': 'string',
    'judgeAllocationExists': 'string',
    'allocatedJudge': 'string',
    'allocatedJudgeEdit': 'string',
    'isFtpaAppellantNoticeOfDecisionSetAside': 'string',
    'isFtpaRespondentNoticeOfDecisionSetAside': 'string',
    'isAppellantFtpaDecisionVisibleToAll': 'string',
    'isRespondentFtpaDecisionVisibleToAll': 'string',
    'ftpaAppellantNoticeDocument': 'array<string>',
    'ftpaRespondentNoticeDocument': 'array<string>'
}


