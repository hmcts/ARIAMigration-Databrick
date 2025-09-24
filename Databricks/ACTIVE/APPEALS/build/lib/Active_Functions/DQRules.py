def build_rule_expression(rules: dict) -> str:
    """
    Joins multiple rule expressions into one combined SQL expression.
    """
    return "({0})".format(" AND ".join(rules.values()))


def base_DQRules():
    """
    Return a dictionary of the DQ rules to be used in the expectations
    
    """
    # Define a dictionary to hold data quality checks
    checks = {}

    # ##############################
    # # ARIADM-669 (appealType)
    # ##############################
    checks["valid_appealReferenceNumber_not_null"] = "(appealReferenceNumber IS NOT NULL)"

    # ##############################
    # # ARIADM-671 (appealType)
    # ##############################
    checks["valid_appealtype_in_allowed_values"] = (
        "(AppealType IN ('refusalOfHumanRights', 'refusalOfEu', 'deprivation', 'protection', 'revocationOfProtection', 'euSettlementScheme'))"
    )
    checks["valid_hmctsCaseCategory_not_null"] = "(hmctsCaseCategory IS NOT NULL)"
    checks["valid_appealTypeDescription_not_null"] = "(appealTypeDescription IS NOT NULL)"
    # Null Values as accepted values as where Representation = AIP
    checks["valid_caseManagementCategory_code_in_list_items"] = """
    (
    caseManagementCategory.value.code IS NULL OR
    ARRAY_CONTAINS(
        TRANSFORM(caseManagementCategory.list_items, x -> x.code),
        caseManagementCategory.value.code
    )
    )
    """
    checks["valid_caseManagementCategory_label_in_list_items"] = """
    (
    caseManagementCategory.value.label IS NULL OR
    ARRAY_CONTAINS(
        TRANSFORM(caseManagementCategory.list_items, x -> x.label),
        caseManagementCategory.value.label
    )
    )
    """

    # ##############################
    # # ARIADM-673 (caseData)

    # \d is a regular expression (regex) metacharacter that matches any single digit from 0 to 9.
    # "yyyy-mm-ddTHH:mm:ssZ" r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'" for ISO 8601 datetime format
    # "yyyy-MM-dd" r'^\d{4}-\d{2}-\d{2}$' for ISO 8601 date format
    ##############################
    checks["valid_appealSubmissionDate_format"] = (
        "(appealSubmissionDate RLIKE r'^\\d{4}-\\d{2}-\\d{2}$')"
    )
    checks["valid_appealSubmissionInternalDate_format"] = (
        "(appealSubmissionInternalDate RLIKE r'^\\d{4}-\\d{2}-\\d{2}$')"
    )
    checks["valid_tribunalReceivedDate_format"] = (
        "(tribunalReceivedDate RLIKE r'^\\d{4}-\\d{2}-\\d{2}$')"
    )

    # ##############################
    # # ARIADM-675 (caseData)
    # ##############################
    checks["valid_appellantsRepresentation_yes_no"] = (
        "(appellantsRepresentation IS NOT NULL AND appellantsRepresentation IN ('Yes', 'No'))"
    )
    checks["valid_submissionOutOfTime_yes_no"] = (
        "(submissionOutOfTime IS NOT NULL AND submissionOutOfTime IN ('Yes', 'No'))"
    )
    checks["valid_recordedOutOfTimeDecision_yes_no_or_null"] = (
        "(recordedOutOfTimeDecision IS NULL OR recordedOutOfTimeDecision IN ('Yes', 'No'))"
    )
    checks["valid_applicationOutOfTimeExplanation_yes_no_or_null"] = (
        "(applicationOutOfTimeExplanation IS NULL OR applicationOutOfTimeExplanation IN ('Yes', 'No'))"
    )

    # ##############################
    # # ARIADM-708 (CaseData)
    # ##############################
    checks["valid_hearingCentre_in_allowed_values"] = """
    (
        hearingCentre IN ('taylorHouse', 'newport', 'newcastle', 'manchester', 'hattonCross', 
        'glasgow', 'bradford', 'birmingham', 'arnhemHouse', 'crownHouse', 'harmondsworth', 
        'yarlsWood', 'remoteHearing', 'decisionWithoutHearing')
    )
    """
    checks["valid_staffLocation_not_null"] = "(staffLocation IS NOT NULL)"
    checks["valid_caseManagementLocation_region_and_baseLocation"] = """
    (
    caseManagementLocation.region = '1' AND
    caseManagementLocation.baseLocation IN (
        '231596', '698118', '366559', '386417', '512401',
        '227101', '765324', '366796', '324339', '649000',
        '999971', '420587', '28837'
    )
    )
    """
    checks["valid_hearingCentreDynamicList_code_in_list_items"] = """
    (
    hearingCentreDynamicList.value.code IS NOT NULL AND
    ARRAY_CONTAINS(
        TRANSFORM(hearingCentreDynamicList.list_items, x -> x.code),
        hearingCentreDynamicList.value.code
    )
    )
    """
    checks["valid_hearingCentreDynamicList_label_in_list_items"] = """
    (
    hearingCentreDynamicList.value.label IS NOT NULL AND
    ARRAY_CONTAINS(
        TRANSFORM(hearingCentreDynamicList.list_items, x -> x.label),
        hearingCentreDynamicList.value.label
    )
    )
    """
    checks["valid_caseManagementLocationRefData_code_in_list_items"] = """
    (
    caseManagementLocationRefData.baseLocation.value.code IS NOT NULL AND
    ARRAY_CONTAINS(
        TRANSFORM(caseManagementLocationRefData.baseLocation.list_items, x -> x.code),
        caseManagementLocationRefData.baseLocation.value.code
    )
    )
    """
    checks["valid_caseManagementLocationRefData_label_in_list_items"] = """
    (
    caseManagementLocationRefData.baseLocation.value.label IS NOT NULL AND
    ARRAY_CONTAINS(
        TRANSFORM(caseManagementLocationRefData.baseLocation.list_items, x -> x.label),
        caseManagementLocationRefData.baseLocation.value.label
    )
    )
    """
    checks["valid_selectedHearingCentreRefData_not_null"] = "(selectedHearingCentreRefData IS NOT NULL)"


    # ##############################
    # # ARIADM-768 (legalRepDetails)
    # # Null Values as accepted values as where Representation = AIP
    # ##############################

    checks["valid_legalRepGivenName_not_null"] = "((dv_representation = 'LR' AND legalRepGivenName IS NOT NULL) OR (dv_representation != 'LR' AND legalRepGivenName IS NULL))"

    checks["valid_legalRepFamilyNamePaperJ_not_null"] = "((dv_representation = 'LR' AND legalRepFamilyNamePaperJ IS NOT NULL) OR (dv_representation != 'LR' AND legalRepFamilyNamePaperJ IS NULL))"

    checks["valid_legalRepCompanyPaperJ_not_null"] = "((dv_representation = 'LR' AND legalRepCompanyPaperJ IS NOT NULL) OR (dv_representation != 'LR' AND legalRepCompanyPaperJ IS NULL))"


    # ##############################
    # # ARIADM-756 (appellantDetails)
    # ##############################
    checks["valid_appellantFamilyName_not_null"] = "(appellantFamilyName IS NOT NULL)"
    checks["valid_appellantGivenNames_not_null"] = "(appellantGivenNames IS NOT NULL)"
    checks["valid_appellantFullName_not_null"] = "(appellantFullName IS NOT NULL)"
    checks["valid_appellantNameForDisplay_not_null"] = "(appellantNameForDisplay IS NOT NULL)"

    checks["valid_appellantDateOfBirth_format"] = (
        "(appellantDateOfBirth RLIKE r'^\\d{4}-\\d{2}-\\d{2}$')"
    )
    checks["valid_caseNameHmctsInternal_not_null"] = "(caseNameHmctsInternal IS NOT NULL)"
    checks["valid_hmctsCaseNameInternal_not_null"] = "(hmctsCaseNameInternal IS NOT NULL)"

    # ##############################
    # # ARIADM-771 (AppealType - legalRepDetails)
    # ##############################

    checks["valid_legalrepEmail_not_null"] = "((dv_representation = 'LR' AND legalRepEmail RLIKE r'^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') OR (dv_representation != 'LR' AND legalRepEmail IS NULL))"

    # ##############################
    # # ARIADM-758 (appellantDetails)
    # ##############################

    checks["valid_isAppellantMinor_yes_no"] = (
        "(isAppellantMinor IS NOT NULL AND isAppellantMinor IN ('Yes', 'No'))"
    )
    checks["valid_deportationOrderOptions_yes_no"] = (
        "(deportationOrderOptions IS NULL OR deportationOrderOptions IN ('Yes', 'No'))"
    )
    checks["valid_appellantInUk_yes_no"] = (
        "(appellantInUk IS  NULL OR appellantInUk IN ('Yes', 'No'))"
    )
    checks["valid_appealOutOfCountry_yes_no"] = (
        "(appealOutOfCountry IS  NULL OR appealOutOfCountry IN ('Yes', 'No'))"
    )

    # ##############################
    # # ARIADM-769 (legalRepDetails - Address logic)CaseRepAddress5
    # ##############################

    checks["valid_legalRepHasAddress_yes_no"] = ( #Omit non-LR records. NLE data will fail all expectations (55) as address are non-UK
    "((dv_representation = 'LR' AND legalRepHasAddress IS NOT NULL AND legalRepHasAddress = 'Yes') OR (dv_representation != 'LR' AND legalRepHasAddress IS NULL))"
    )
    checks["valid_legalRepHasAddressUK"]   = ( #Omit non-LR records. All fields are null, hence all expectations will fail. (55)
    "(((dv_representation = 'LR' AND oocAddressLine1 IS NOT NULL AND LEN(oocAddressLine1) < 151) OR (dv_representation != 'LR' AND oocAddressLine1 IS NULL)"
    "OR ((dv_representation = 'LR' AND LEN(oocAddressLine2) < 51) OR (dv_representation != 'LR' AND oocAddressLine2 IS NULL))" 
    "OR ((dv_representation = 'LR' AND LEN(oocAddressLine3) < 51) OR (dv_representation != 'LR' AND oocAddressLine3 IS NULL))"
    "OR ((dv_representation = 'LR' AND LEN(oocAddressLine4) < 51) OR (dv_representation != 'LR' AND oocAddressLine4 IS NULL))"
    "OR ((dv_representation = 'LR' AND LEN(CaseRep_Address5) < 51) OR (dv_representation != 'LR' AND CaseRep_Address5 IS NULL))"
    "OR ((dv_representation = 'LR' AND LEN(CaseRep_Postcode) < 15) OR (dv_representation != 'LR' AND CaseRep_Postcode IS NULL))))"
    )   
    checks["valid_oocAddressLine1"] = ( #Omit non-LR records. NLE data will fail all expectations (55) as fields are null
    "((dv_representation = 'LR' AND oocAddressLine1 IS NOT NULL) OR (dv_representation != 'LR' AND oocAddressLine1 IS NULL))"
    )
    checks["valid_oocAddressLine2"] = ( #Omit non-LR records. NLE data will fail all expectations (55) as fields are null
    "((dv_representation = 'LR' AND oocAddressLine2 IS NOT NULL) OR (dv_representation != 'LR' AND oocAddressLine2 IS NULL))"
    )
    checks["valid_oocAddressLine3"] = ( #Omit non-LR records. NLE data will fail all expectations (55) as fields are null
    "((dv_representation = 'LR' AND oocAddressLine3 IS NOT NULL) OR (dv_representation != 'LR' AND oocAddressLine3 IS NULL))"
    )
    checks["valid_oocAddressLine4"] = ( #Omit non-LR records. NLE data will fail all expectations (55) as fields are null
    "((dv_representation = 'LR' AND oocAddressLine4 IS NOT NULL) OR (dv_representation != 'LR' AND oocAddressLine4 IS NULL))"
    )
    checks["valid_oocrCountryGovUkAdminJ"] = ( #Omit non-LR records. NLE data will fail all expectations (55) as fields are null
    "((dv_representation = 'LR' AND CaseRep_Address5 IS NOT NULL AND valid_countryGovUkOocAdminJ IS NOT NULL ) OR (dv_representation != 'LR' AND CaseRep_Address5 IS NULL))"
    )

    # ##############################
    # # ARIADM-766 (appellantStateless)
    # ##############################
    checks["valid_appellantStateless_values"] = ("(appellantStateless IN ('isStateless', 'hasNationality'))")

    checks["valid_appellantNationalitiesDescription_not_null"] = "(appellantNationalitiesDescription IS NOT NULL)"

    checks["valid_appellantNationalities_not_null"] = ("(appellantNationalities IS NOT NULL)")

    ##############################
    # ARIADM-760 (appellantDetails) - appellantHasFixedAddress and appellantAddress
    ##############################

    # Only include if CategoryIdList contains 37; check for 'Yes' or 'No'
    checks["valid_appellantHasFixedAddress_yes_no_if_cat37"] = (
        "( (array_contains(valid_categoryIdList, 37) AND appellantHasFixedAddress IS NOT NULL AND appellantHasFixedAddress IN ('Yes', 'No')) OR (NOT array_contains(valid_categoryIdList, 37)) )"
    )

    # ARIADM-XXX (appellantAddress expectations)
    # Only include if array_contains(valid_categoryIdList, 37)
    checks["valid_appellantAddress_AddressLine1_mandatory_and_length"] = (
        "( (array_contains(valid_categoryIdList, 37) AND appellantAddress.AddressLine1 IS NOT NULL AND LENGTH(appellantAddress.AddressLine1) <= 150) OR (NOT array_contains(valid_categoryIdList, 37)) )"
    )
    checks["valid_appellantAddress_AddressLine2_length"] = (
        "( (array_contains(valid_categoryIdList, 37) AND (appellantAddress.AddressLine2 IS NULL OR LENGTH(appellantAddress.AddressLine2) <= 50)) OR (NOT array_contains(valid_categoryIdList, 37)) )"
    )
    checks["valid_appellantAddress_AddressLine3_length"] = (
        "( (array_contains(valid_categoryIdList, 37) AND (appellantAddress.AddressLine3 IS NULL OR LENGTH(appellantAddress.AddressLine3) <= 50)) OR (NOT array_contains(valid_categoryIdList, 37)) )"
    )
    checks["valid_appellantAddress_PostTown_length"] = (
        "( (array_contains(valid_categoryIdList, 37) AND (appellantAddress.PostTown IS NULL OR LENGTH(appellantAddress.PostTown) <= 50)) OR (NOT array_contains(valid_categoryIdList, 37)) )"
    )
    checks["valid_appellantAddress_County_length"] = (
        "( (array_contains(valid_categoryIdList, 37) AND (appellantAddress.County IS NULL OR LENGTH(appellantAddress.County) <= 50)) OR (NOT array_contains(valid_categoryIdList, 37)) )"
    )
    checks["valid_appellantAddress_PostCode_length"] = (
        "( (array_contains(valid_categoryIdList, 37) AND (appellantAddress.PostCode IS NULL OR LENGTH(appellantAddress.PostCode) <= 14)) OR (NOT array_contains(valid_categoryIdList, 37)) )"
    )
    checks["valid_appellantAddress_Country_length"] = (
        "( (array_contains(valid_categoryIdList, 37) AND (appellantAddress.Country IS NULL OR LENGTH(appellantAddress.Country) <= 50)) OR (NOT array_contains(valid_categoryIdList, 37)) )"
    )


    # #############################
    # # ARIADM-709 (flagsLabels)
    # #############################

    checks["valid_journeyType_aip_orNull"] = "((dv_representation = 'AIP' AND journeyType = 'aip') OR (dv_representation != 'AIP' AND journeyType IS NULL))"

    # #############################
    # # ARIADM-710 (flagsLabels)
    # #############################

    checks["valid_isAriaMigratedFeeExemption_yes_no"] = "((dv_CCDAppealType = 'DA' AND isAriaMigratedFeeExemption = 'Yes') OR (dv_CCDAppealType != 'DA' AND isAriaMigratedFeeExemption = 'No'))"

    # ##############################
    # # ARIADM-712 (flagsLabel)- caseFlags
    # ##############################
    checks["valid_caseFlags_name_in_list"] = """
    (
    caseFlags.details IS NULL OR
    ARRAY_CONTAINS(
        TRANSFORM(caseFlags.details, x -> x.value.name),
        caseFlags.details[0].value.name
    )
    )
    """
    checks["valid_caseFlags_pathId_in_list"] = """
    (
    caseFlags.details IS NULL OR
    ARRAY_CONTAINS(
        TRANSFORM(caseFlags.details, x -> x.value.path[0].id),
        caseFlags.details[0].value.path[0].id
    )
    )
    """
    checks["valid_caseFlags_flagCode_in_list"] = """
    (
    caseFlags.details IS NULL OR
    ARRAY_CONTAINS(
        TRANSFORM(caseFlags.details, x -> x.value.flagCode),
        caseFlags.details[0].value.flagCode
    )
    )
    """
    checks["valid_caseFlags_flagComment_in_list"] = """
    (
    caseFlags.details IS NULL OR
    ARRAY_CONTAINS(
        TRANSFORM(caseFlags.details, x -> x.value.flagComment),
        caseFlags.details[0].value.flagComment
    )
    )
    """
    checks["valid_caseFlags_hearingRelevant_in_list"] = """
    (
    caseFlags.details IS NULL OR
    ARRAY_CONTAINS(
        TRANSFORM(caseFlags.details, x -> x.value.hearingRelevant),
        caseFlags.details[0].value.hearingRelevant
    )
    )
    """

    # ##############################
    # # ARIADM-712 (flagsLabel)- appellantLevelFlags
    # ##############################

    checks["valid_appellantLevelFlags_name_in_details"] = """
    (
    appellantLevelFlags.details[0].value.name IS NULL OR
    ARRAY_CONTAINS(
        TRANSFORM(appellantLevelFlags.details, x -> x.value.name),
        appellantLevelFlags.details[0].value.name
    )
    )
    """

    checks["valid_appellantLevelFlags_path_id_in_details"] = """
    (
    appellantLevelFlags.details[0].value.path[0].id IS NULL OR
    ARRAY_CONTAINS(
        TRANSFORM(appellantLevelFlags.details, x -> x.value.path[0].id),
        appellantLevelFlags.details[0].value.path[0].id
    )
    )
    """

    checks["valid_appellantLevelFlags_flagCode_in_details"] = """
    (
    appellantLevelFlags.details[0].value.flagCode IS NULL OR
    ARRAY_CONTAINS(
        TRANSFORM(appellantLevelFlags.details, x -> x.value.flagCode),
        appellantLevelFlags.details[0].value.flagCode
    )
    )
    """

    checks["valid_appellantLevelFlags_flagComment_in_details"] = """
    (
    appellantLevelFlags.details[0].value.flagComment IS NULL OR
    ARRAY_CONTAINS(
        TRANSFORM(appellantLevelFlags.details, x -> x.value.flagComment),
        appellantLevelFlags.details[0].value.flagComment
    )
    )
    """

    checks["valid_appellantLevelFlags_hearingRelevant_in_details"] = """
    (
    appellantLevelFlags.details[0].value.hearingRelevant IS NULL OR
    ARRAY_CONTAINS(
        TRANSFORM(appellantLevelFlags.details, x -> x.value.hearingRelevant),
        appellantLevelFlags.details[0].value.hearingRelevant
    )
    )
    """

    # ##############################
    # # ARIADM-780 (PartyID)
    # ##############################

    checks["valid_appellantPartyId_not_null"] = (
    "(appellantPartyId IS NOT NULL)"
    )
    checks["valid_legalRepIndividualPartyId_not_null"] = ( #If appellantsRep = no then appellantsRep = LR
    "(legalRepIndividualPartyId IS NOT NULL AND appellantsRepresentation = 'No')"
    )
    checks["validlegalRepOrganisationPartyId_not_null"] = ( #If appellantsRep = no then appellantsRep = LR
    "(legalRepOrganisationPartyId IS NOT NULL AND appellantsRepresentation = 'No')"
    )
    checks["valid_sponsorPartyId_not_null"] = (
    "(sponsorPartyId IS NOT NULL)"
    )

    # ##############################
    # # ARIADM-783 (payment)
    # ##############################
    checks["valid_feeAmountGbp"] = ( # fee amount is not null and is an int
        "(dv_CCDAppealType IN ('EA','EU','HU','PA') AND (feeAmountGbp IS NOT NULL) AND (TRY_CAST(feeAmountGbp AS INT) IS NOT NULL)) OR (dv_CCDAppealType NOT IN ('EA','EU','HU','PA') AND (feeAmountGbp IS NULL))"
        )

    # checks["valid_feeAmountGbp"] = ( # fee amount is not null and is an int
    #     "(feeAmountGbp IS NOT NULL AND TRY_CAST(feeAmountGbp AS INT) IS NOT NULL)"
    # )

    checks["valid_feeDescription"] = ( #feeDescription is not null
        "(dv_CCDAppealType IN ('EA','EU','HU','PA') AND (feeDescription IS NOT NULL)) OR (dv_CCDAppealType NOT IN ('EA','EU','HU','PA') AND (feeDescription IS NULL))"
    )

    # checks["valid_feeDescription"] = ( #feeDescription is not null
    #     "(feeDescription IS NOT NULL)"
    # )

    checks["valid_feeWithHearing"] = ( # feeWithHearing is not null and is an int
        "(dv_CCDAppealType IN ('EA','EU','HU','PA') AND (feeWithHearing IS NOT NULL) AND (TRY_CAST(feeWithHearing AS INT) IS NOT NULL)) OR (dv_CCDAppealType NOT IN ('EA','EU','HU','PA') AND (feeWithHearing IS NULL))"
    )

    # checks["valid_feeWithHearing"] = ( # feeWithHearing is not null and is an int
    #     "(feeWithHearing IS NOT NULL AND TRY_CAST(feeWithHearing AS INT) IS NOT NULL)"
    # )

    checks["valid_feeWithoutHearing"] = (# feeWithoutHearing is not null and is an int
        "(dv_CCDAppealType IN ('EA','EU','HU','PA') AND (feeWithoutHearing IS NOT NULL) AND (TRY_CAST(feeWithoutHearing AS INT) IS NOT NULL)) OR (dv_CCDAppealType NOT IN ('EA','EU','HU','PA') AND (feeWithoutHearing IS NULL))"
    )

    # checks["valid_feeWithoutHearing"] = (# feeWithoutHearing is not null and is an int
    #     "(feeWithoutHearing IS NOT NULL AND TRY_CAST(feeWithoutHearing AS INT) IS NOT NULL)"
    # )

    checks["valid_paymentDescription"] = ( # paymentDescription is not null
        "(dv_CCDAppealType IN ('EA','EU','HU','PA') AND (paymentDescription IS NOT NULL)) OR (dv_CCDAppealType NOT IN ('EA','EU','HU','PA') AND (paymentDescription IS NULL))"
    )

    # checks["valid_paymentDescription"] = ( # paymentDescription is not null
    #     "(paymentDescription IS NOT NULL)"
    # )

    checks["valid_paymentStatus"] = ( # paymentStatus is not null
    "(dv_CCDAppealType IN ('EA','EU','HU','PA') AND (paymentStatus IS NOT NULL)) OR (dv_CCDAppealType NOT IN ('EA','EU','HU','PA') AND (paymentStatus IS NULL))"
    )

    # checks["valid_paymentStatus"] = ( # paymentStatus is not null
    #   "(paymentStatus IS NOT NULL)"
    # )

    checks["valid_feeVersion"] = ( # feeVersion is not null
    "(dv_CCDAppealType IN ('EA','EU','HU','PA') AND (feeVersion IS NOT NULL)) OR (dv_CCDAppealType NOT IN ('EA','EU','HU','PA') AND (feeVersion IS NULL))"
    )

    # checks["valid_feeVersion"] = ( # feeVersion is not null
    #   "(feeVersion IS NOT NULL)"
    # )

    checks["valid_feePaymentAppealType"] = ( # feePaymentAppealType is not null
    "(dv_CCDAppealType IN ('EA','EU','HU','PA') AND (feePaymentAppealType IS NOT NULL)) OR (dv_CCDAppealType NOT IN ('EA','EU','HU','PA') AND (feePaymentAppealType IS NULL))"
    )

    # checks["valid_feePaymentAppealType"] = ( # feePaymentAppealType is not null
    #   "(feePaymentAppealType IS NOT NULL)"
    # )

    # ##############################
    # # ARIADM-785 (remissionTypes)
    # ############################## 

    checks["valid_remissionType_in_list"] = (
        "(remissionType IN ('noRemission', 'hoWaiverRemission', 'helpWithFees', 'exceptionalCircumstancesRemission') AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')) OR (remissionType IS NULL AND dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA'))"
    )

    checks["valid_remissionClaim_in_list"] = (
        "(remissionClaim IN ('asylumSupport', 'legalAid', 'section17', 'section20', 'homeOfficeWaiver') AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')) OR (remissionClaim IS NULL AND dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA'))"
    )

    checks["valid_feeRemissionType_not_null"] = (
        "(feeRemissionType IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')) OR (dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA') AND feeRemissionType IS NULL)"
    )

    # ##############################
    # # ARIADM-786 (remissionTypes)
    # ##############################

    checks["valid_exceptionalCircumstances_not_null"] = (
        "(exceptionalCircumstances IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')) OR (dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA') AND exceptionalCircumstances IS NULL)"
    )

    checks["valid_helpWithFeesReferenceNumber_not_null"] = (
        "(helpWithFeesReferenceNumber IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')) OR (dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA') AND helpWithFeesReferenceNumber IS NULL)"
    )

    checks["valid_legalAidAccountNumber_not_null"] = (
        "(legalAidAccountNumber IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')) OR (dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA') AND legalAidAccountNumber IS NULL)"
    )

    checks["valid_asylumSupportReference_not_null"] = (
        "(asylumSupportReference IS NOT NULL AND dv_CCDAppealType IN ('EA', 'EU', 'HU', 'PA')) OR (dv_CCDAppealType NOT IN ('EA', 'EU', 'HU', 'PA') AND asylumSupportReference IS NULL)"
    )

    ##############################
    # ARIADM-773 (SponsorDetails)
    ##############################
    checks["valid_hasSponsor_yes_no"] = (
        "(hasSponsor IS NOT NULL AND hasSponsor IN ('Yes', 'No'))"
    )

    checks["valid_sponsorGivenNames_not_null"] = (
        "(((array_contains(valid_categoryIdList, 38)) AND hasSponsor = 'Yes' AND sponsorGivenNames IS NOT NULL) OR (NOT array_contains(valid_categoryIdList, 38) AND hasSponsor = 'No' AND sponsorGivenNames IS NULL))"
    )

    checks["valid_sponsorFamilyName_not_null"] = (
        "(((array_contains(valid_categoryIdList, 38) AND hasSponsor = 'Yes' AND sponsorFamilyName IS NOT NULL) OR (NOT array_contains(valid_categoryIdList, 38) AND hasSponsor = 'No' AND sponsorFamilyName IS NULL)))"
    )

    checks["valid_sponsorAuthorisation_yes_no"] = (
        "((array_contains(valid_categoryIdList, 38) AND hasSponsor = 'Yes' AND sponsorAuthorisation IN ('Yes', 'No')))"
    )

    ##############################
    # ARIADM-776 (SponsorDetails)
    ##############################
    checks["valid_sponsorAddress_not_null"] = (
        "(((array_contains(valid_categoryIdList, 38) AND hasSponsor = 'Yes' AND sponsorAddress IS NOT NULL) OR (NOT array_contains(valid_categoryIdList, 38) AND hasSponsor = 'No' AND sponsorAddress IS NULL)))"
    )
    ##############################
    # ARIADM-778 (SponsorDetails)
    ##############################
    checks["valid_sponsorEmailAdminJ"] = (
        "(((array_contains(valid_categoryIdList, 38) AND hasSponsor = 'Yes' AND sponsorEmailAdminJ IS NOT NULL) "
        "OR (NOT array_contains(valid_categoryIdList, 38) OR hasSponsor = 'No') AND sponsorEmailAdminJ IS NULL))"
    )

    checks["valid_sponsorMobileNumberAdminJ"] = (
        "(((array_contains(valid_categoryIdList, 38) AND hasSponsor = 'Yes' AND sponsorMobileNumberAdminJ IS NOT NULL) "
        "OR (NOT array_contains(valid_categoryIdList, 38) OR hasSponsor = 'No') AND sponsorMobileNumberAdminJ IS NULL))"
    )
    # ##############################
    # ARIADM-760 (appellantDetails)
    # ARIADM-762 (appellantDetails)
    # ##############################
    checks["valid_oocAppealAdminJ_values"] = (
        "( ( (array_contains(valid_categoryIdList, 38) OR MainRespondentId = 4) "
        "AND oocAppealAdminJ IN ('entryClearanceDecision', 'leaveUk', 'none') ) "
        "OR (oocAppealAdminJ IS NULL) )"
    )

    # Only IF CategoryId IN [38] = Include; ELSE null
    checks["valid_appellantHasFixedAddressAdminJ"] = (
        "( (array_contains(valid_categoryIdList, 38) AND appellantHasFixedAddressAdminJ IN ('Yes', 'No')) "
        "OR (NOT array_contains(valid_categoryIdList, 38) AND appellantHasFixedAddressAdminJ IS NULL) )"
    )

    # addressLine1AdminJ: IS NOT NULL when array_contains(valid_categoryIdList, 38) AND at least one of the coalesce fields is not null; ELSE can be NULL
    checks["valid_addressLine1AdminJ"] = (
        "( (array_contains(valid_categoryIdList, 38) AND "
        "(Appellant_Address1 IS NOT NULL OR Appellant_Address2 IS NOT NULL OR Appellant_Address3 IS NOT NULL OR Appellant_Address4 IS NOT NULL OR Appellant_Address5 IS NOT NULL OR Appellant_Postcode IS NOT NULL) "
        "AND addressLine1AdminJ IS NOT NULL) "
        "OR (addressLine1AdminJ IS NULL) )"
    )

    # addressLine2AdminJ: IS NOT NULL when array_contains(valid_categoryIdList, 38) AND dv_representation = 'LR' AND at least one of the coalesce fields is not null; ELSE can be NULL
    checks["valid_addressLine2AdminJ"] = (
        "( (array_contains(valid_categoryIdList, 38) AND dv_representation = 'LR' AND "
        "(Appellant_Address2 IS NOT NULL OR Appellant_Address3 IS NOT NULL OR Appellant_Address4 IS NOT NULL OR Appellant_Address5 IS NOT NULL OR Appellant_Postcode IS NOT NULL) "
        "AND addressLine2AdminJ IS NOT NULL) "
        "OR (addressLine2AdminJ IS NULL) )"
    )

    # addressLine3AdminJ: IS NOT NULL when array_contains(valid_categoryIdList, 38) AND at least one of the coalesce fields is not null; ELSE can be NULL
    checks["valid_addressLine3AdminJ"] = (
        "( (array_contains(valid_categoryIdList, 38) AND "
        "(Appellant_Address3 IS NOT NULL OR Appellant_Address4 IS NOT NULL) "
        "AND addressLine3AdminJ IS NOT NULL) "
        "OR ( addressLine3AdminJ IS NULL) )"
    )

    # addressLine4AdminJ: IS NOT NULL when array_contains(valid_categoryIdList, 38) AND at least one of the coalesce fields is not null; ELSE can be NULL
    checks["valid_addressLine4AdminJ"] = (
        "( (array_contains(valid_categoryIdList, 38) AND "
        "(Appellant_Address5 IS NOT NULL OR Appellant_Postcode IS NOT NULL) "
        "AND addressLine4AdminJ IS NOT NULL) "
        "OR ( addressLine4AdminJ IS NULL) )"
    )

    # countryGovUkOocAdminJ: IS NOT NULL when array_contains(valid_categoryIdList, 38); ELSE can be NULL
    checks["valid_countryGovUkOocAdminJ"] = (
        "( (array_contains(valid_categoryIdList, 38) AND countryGovUkOocAdminJ IS NOT NULL) "
        "OR (countryGovUkOocAdminJ IS NULL) )"
    )
    ##############################
    # AARIADM-764 (appellantDetails)
    ##############################
    # ^([a-zA-Z0-9_\-\.]+)@([a-zA-Z0-9_\-\.]+)\.([a-zA-Z]{2,5})$ 
    checks["valid_internalAppellantEmail_format"] = (
        "( internalAppellantEmail RLIKE r'^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$' OR internalAppellantEmail IS NULL)"
    )

    checks["valid_email_format"] = (
        "(email RLIKE r'^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$' OR email IS NULL)"
    )

    checks["valid_internalAppellantMobileNumber"] = (
        "(internalAppellantMobileNumber RLIKE r'^(?=(?:\\D*\\d){7,15}\\D*$)\\+?(\\d[\\d-. ]+)?(\\([\\d-. ]+\\))?[\\d-. ]*\\d$' OR internalAppellantMobileNumber IS NULL)"
    )

    # ^(?=(?:\D*\d){7,15}\D*$)\+?(\d[\d-. ]+)?(\([\d-. ]+\))?[\d-. ]*\d$
    checks["valid_mobileNumber"] = (
        "(mobileNumber RLIKE r'^(?=(?:\\D*\\d){7,15}\\D*$)\\+?(\\d[\\d-. ]+)?(\\([\\d-. ]+\\))?[\\d-. ]*\\d$' OR mobileNumber IS NULL)"
    )
    ##############################
    # ARIADM-778 (General)
    ##############################
    checks["isServiceRequestTabVisibleConsideringRemissions_yes_no"] = (
        "(isServiceRequestTabVisibleConsideringRemissions IS NOT NULL AND isServiceRequestTabVisibleConsideringRemissions IN ('Yes', 'No'))"
    )

    checks["lu_applicationChangeDesignatedHearingCentre_fixed_list"] = (
    "(lu_applicationChangeDesignatedHearingCentre IS NOT NULL AND lu_applicationChangeDesignatedHearingCentre IN ('taylorHouse', 'newport', 'newcastle', 'manchester', 'hattonCross' ,'glasgow' ,'bradford' ,'birmingham', 'arnhemHouse', 'crownHouse', 'harmondsworth', 'yarlsWood', 'remoteHearing', 'decisionWithoutHearing'))"
    )
    #########################################
    # ARIADM-788 and ARIADM-792 (homeOffice)
    #########################################
    checks["valid_homeOfficeDecisionDate_format"] = (
        "(homeOfficeDecisionDate IS NOT NULL AND homeOfficeDecisionDate RLIKE r'^\\d{4}-\\d{2}-\\d{2}$')"
    )

    checks["valid_decisionLetterReceivedDate_format"] = (
        "(decisionLetterReceivedDate IS NOT NULL AND decisionLetterReceivedDate RLIKE r'^\\d{4}-\\d{2}-\\d{2}$')"
    )

    checks["valid_dateEntryClearanceDecision_format"] = (
        "(dateEntryClearanceDecision IS NOT NULL AND dateEntryClearanceDecision RLIKE r'^\\d{4}-\\d{2}-\\d{2}$')"
    )

    checks["valid_homeOfficeReferenceNumber_not_null"] = (
        "(homeOfficeReferenceNumber IS NOT NULL)"
    )

    checks["valid_gwfReferenceNumber_not_null"] = (
        "(gwfReferenceNumber IS NOT NULL)"
    )

    #########################################
    # ARIADM-799 (Documents)
    #########################################

    checks["valid_uploadTheAppealFormDocs"] = (
    "(uploadTheAppealFormDocs IS NOT NULL)"
    )

    checks["valid_caseNotes"] = (
    "(caseNotes IS NOT NULL)"
    )

    checks["valid_tribunalDocuments"] = (
    "(tribunalDocuments IS NOT NULL)"
    )

    checks["valid_legalRepresentativeDocuments"] = (
    "(legalRepresentativeDocuments IS NOT NULL)"
    )

    return checks


if __name__ == "__main__":
    pass

