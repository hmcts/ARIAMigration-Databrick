from .dq_rules import DQRulesBase


class paymentPendingDetainedDQRules(DQRulesBase):

    def get_checks(self, checks={}):
        checks = checks | self.get_base_checks()

        return checks

    def get_base_checks(self, checks={}):

        #########################################
        # (detained)
        ######################################### 

        checks["valid_appellantInDetention"] = (
            """
            (
                Detained IN (1,2,4)
                AND appellantInDetention = 'Yes'
            )
            OR
            (
                Detained NOT IN (1,2,4)
                AND appellantInDetention = 'No'
            )
            """
        )

        checks["valid_detentionFacility"] = (
            """
            (
                Detained = 1
                AND detentionFacility = 'prison'
            )
            OR
            (
                Detained = 2
                AND detentionFacility = 'immigrationRemovalCentre'
            )
            OR
            (
                Detained = 4
                AND detentionFacility = 'other'
            )
            OR
            (
                Detained NOT IN (1,2,4)
                AND detentionFacility IS NULL
            )
            """
        )
        
        checks["valid_prisonName"] = (
            """ ( Detained != 1 AND  prisonName IS NULL) OR 
                ( prisonName <=> prisonName_det )
            
            """)
        
        checks["valid_prisonNOMSNumber"] = (
            """
            (
                (Detained != 1 OR PrisonRef IS NULL)
                AND prisonNOMSNumber IS NULL
            ) 
            OR 
            (
                Detained = 1 
                AND PrisonRef IS NOT NULL 
                AND prisonNOMSNumber.prison <=> PrisonRef
            )
            """
        )

        checks["valid_otherDetentionFacilityName"] = (
            """
            (
                (Detained != 4)
                AND otherDetentionFacilityName IS NULL
            )
            OR
            (
                Detained = 4
                AND otherDetentionFacilityName.other <=> coalesce(DetentionCentre_det, Appellant_Address1)
            )
            """
        )
        
        checks["valid_ircName"] = (
            """
            (
                (Detained != 2)
                AND ircName IS NULL
            )
            OR
            (
                Detained = 2
                AND ircName <=> ircName_det
            )
            """
        )
        
        checks["valid_releaseDateProvided"] = (
            """
            (
                (Detained NOT IN (1,4))
                AND releaseDateProvided IS NULL
            )
            OR
            (
                Detained IN (1,4)
                AND releaseDateProvided = 'Yes'
            )
            """
        )
        
        checks["valid_hasPendingBailApplications"] = (
            """
            (
                (Detained != 2)
                AND hasPendingBailApplications IS NULL
            )
            OR
            (
                Detained = 2
                AND hasPendingBailApplications = 'NotSure'
            )
            """
        )
        
        checks["valid_removalOrderOptions"] = (
            """
            (
                RemovalDate IS NOT NULL
                AND removalOrderOptions = 'Yes'
            )
            OR
            (
                RemovalDate IS NULL
                AND removalOrderOptions = 'No'
            )
            """
        )

        checks["valid_removalOrderDate"] = (
            """
            (
                RemovalDate IS NULL
                AND removalOrderDate IS NULL
            )
            OR
            (
                RemovalDate IS NOT NULL
                AND removalOrderDate = date_format(RemovalDate, "yyyy-MM-dd'T'HH:mm:ss.SSS")
            )
            """
        )

        checks["valid_detentionBuilding"] = (
            """
            (
                (Detained NOT IN (1,2))
                AND detentionBuilding IS NULL
            )
            OR
            (
                Detained IN (1,2)
                AND detentionBuilding <=> detentionBuilding_det
            )
            """
        )

        checks["valid_detentionAddressLines"] = (
            """
            (
                (Detained NOT IN (1,2))
                AND detentionAddressLines IS NULL
            )
            OR
            (
                Detained IN (1,2)
                AND detentionAddressLines <=> detentionAddressLines_det
            )
            """
        )

        checks["valid_detentionPostcode"] = (
            """
            (
                (Detained NOT IN (1,2))
                AND detentionPostcode IS NULL
            )
            OR
            (
                Detained IN (1,2)
                AND detentionPostcode <=> detentionPostcode_det
            )
            """
        )

        ##############################
        # ARIADM-773 (SponsorDetails)
        ##############################

        checks["valid_hasSponsor_yes_no"] = """
            (
                CASE
                    WHEN Detained IN (1,2,4)
                        THEN hasSponsor = 'No'

                    WHEN dv_appellantIsInUk = true
                        THEN hasSponsor = 'No'

                    WHEN dv_appellantIsInUk = false
                        AND Sponsor_Name IS NOT NULL
                        THEN hasSponsor = 'Yes'
                    
                    WHEN dv_appellantIsInUk = false
                        AND Sponsor_Name IS NULL
                        THEN hasSponsor = 'No'

                    ELSE hasSponsor = 'No'
                END
            )
            """

        checks["valid_sponsorGivenNames_not_null"] = """
        (
                CASE
                    WHEN Detained IN (1,2,4)
                        THEN sponsorGivenNames IS NULL

                    WHEN dv_appellantIsInUk = true
                        THEN sponsorGivenNames IS NULL

                    WHEN dv_appellantIsInUk = false
                        AND Sponsor_Name IS NOT NULL
                        THEN sponsorGivenNames IS NOT NULL
                    
                    WHEN dv_appellantIsInUk = false
                        AND Sponsor_Name IS NULL
                        THEN sponsorGivenNames IS NULL

                    ELSE sponsorGivenNames IS NULL
                END
            )
            """

        checks["valid_sponsorFamilyName_not_null"] = """
        (
                CASE
                    WHEN Detained IN (1,2,4)
                        THEN sponsorFamilyName IS NULL

                    WHEN dv_appellantIsInUk = true
                        THEN sponsorFamilyName IS NULL

                    WHEN dv_appellantIsInUk = false
                        AND Sponsor_Name IS NOT NULL
                        THEN sponsorFamilyName IS NOT NULL
                    
                    WHEN dv_appellantIsInUk = false
                        AND Sponsor_Name IS NULL
                        THEN sponsorFamilyName IS NULL

                    ELSE sponsorFamilyName IS NULL
                END
            )
            """

        checks["valid_sponsorAuthorisation_values"] = """
        (
            CASE
                WHEN Detained IN (1,2,4)
                    THEN sponsorAuthorisation IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN sponsorAuthorisation IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NULL
                    THEN sponsorAuthorisation IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NOT NULL
                    AND Sponsor_Authorisation = true
                    THEN sponsorAuthorisation = 'Yes'

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NOT NULL
                    THEN sponsorAuthorisation = 'No'

                ELSE sponsorAuthorisation IS NULL
            END
        )
        """

        checks["valid_sponsorAddress_values"] = """
        (
            CASE
                WHEN Detained IN (1,2,4)
                    THEN sponsorAddress IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN sponsorAddress IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NULL
                    THEN sponsorAddress IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NOT NULL
                    THEN sponsorAddress IS NOT NULL
                
                ELSE sponsorAddress IS NULL
            END
        )
        """

        checks["valid_sponsorAddressForDisplay"] = """
        (
            CASE
                WHEN Detained IN (1,2,4)
                    THEN sponsorAddressForDisplay IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN sponsorAddressForDisplay IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NULL
                    THEN sponsorAddressForDisplay IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NOT NULL
                    THEN sponsorAddressForDisplay IS NOT NULL
                
                ELSE sponsorAddressForDisplay IS NULL
            END
        )
        """

        checks["valid_sponsorNameForDisplay"] = """
        (
            CASE
                WHEN Detained IN (1,2,4)
                    THEN sponsorNameForDisplay IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN sponsorNameForDisplay IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NULL
                    THEN sponsorNameForDisplay IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NOT NULL
                    THEN sponsorNameForDisplay IS NOT NULL
                
                ELSE sponsorNameForDisplay IS NULL
            END
        )
        """

        checks["valid_sponsorEmailAdminJ"] = """
        (
            CASE
                WHEN Detained IN (1,2,4)
                    THEN sponsorEmailAdminJ IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN sponsorEmailAdminJ IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NULL
                    THEN sponsorEmailAdminJ IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NOT NULL
                    THEN true
                
                ELSE sponsorEmailAdminJ IS NULL
            END
        )
        """

        checks["valid_sponsorMobileNumberAdminJ"] = """
        (
            CASE
                WHEN Detained IN (1,2,4)
                    THEN sponsorMobileNumberAdminJ IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN sponsorMobileNumberAdminJ IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NULL
                    THEN sponsorMobileNumberAdminJ IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NOT NULL
                    THEN (
                        sponsorMobileNumberAdminJ IS NULL
                        OR sponsorMobileNumberAdminJ RLIKE '^((\\\\+44(\\\\s\\\\(0\\\\)\\\\s|\\\\s0\\\\s|\\\\s)?)|0)7\\\\d{3}(\\\\s)?\\\\d{6}$'
                        )
                        
                ELSE sponsorMobileNumberAdminJ IS NULL
            END
        )
        """

        checks["valid_sponsorPartyId_not_null"] = ("""
        (
            CASE
                WHEN Detained IN (1,2,4)
                    THEN sponsorPartyId IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN sponsorPartyId IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NULL
                    THEN sponsorPartyId IS NULL

                WHEN dv_appellantIsInUk = false
                    AND Sponsor_Name IS NOT NULL
                    THEN sponsorPartyId IS NOT NULL

                ELSE sponsorPartyId IS NULL
            END
        )                                                                                         
        """)

        ##############################
        # ARIADM-778 (General)
        ##############################

        checks["valid_applicationChangeDesignatedHearingCentre_fixed_list"] = (
            """(
                CASE
                    WHEN Detained IN (1,2) THEN applicationChangeDesignatedHearingCentre IN ('taylorHouse','hattonCross','birmingham','glasgow','manchester',
                    'newcastle','bradford','newport','yarlsWood')
                    ELSE
                    applicationChangeDesignatedHearingCentre IN ('taylorHouse', 'newport', 'newcastle', 'manchester', 'hattonCross' ,'glasgow' ,'bradford' ,'birmingham', 'arnhemHouse', 'crownHouse', 'harmondsworth', 'yarlsWood', 'remoteHearing', 'decisionWithoutHearing')
                END
            )"""
        )

        # ##############################
        # # ARIADM-708 (CaseData)
        # ##############################
        checks["valid_hearingCentre"] = ("""
        (
            (hearingCentre IS NOT NULL)
            AND CASE 
                WHEN Detained IN (1,2) THEN hearingCentre IN ('taylorHouse','hattonCross','birmingham','glasgow','manchester','newcastle','bradford','newport','yarlsWood')
                ELSE (hearingCentre IN ('taylorHouse', 'newport', 'newcastle', 'manchester', 'hattonCross',
                                    'glasgow', 'bradford', 'birmingham', 'arnhemHouse', 'crownHouse', 'harmondsworth',
                                    'yarlsWood', 'remoteHearing', 'decisionWithoutHearing')
                                    )
                END
        )
        """)

        checks["valid_staffLocation"] = ("""
                                         
                (staffLocation IS NOT NULL)
                AND 
                CASE 
                    WHEN Detained IN (1,2)  
                    THEN staffLocation IN ('Taylor House','Hatton Cross','Birmingham','Glasgow','Manchester','Newcastle','Bradford','Newport','Yarls Wood')
                    ELSE staffLocation IN ('Bradford','Birmingham','Glasgow','Hatton Cross','Manchester','Newcastle','Newport','Taylor House')
                END

        """)


        checks["valid_caseManagementLocation_region_and_baseLocation"] = ("""
        (
            CASE 
                    WHEN Detained IN (1,2)  
                        THEN    caseManagementLocation.region <=> '1' 
                                AND
                                caseManagementLocation.baseLocation IN ('227101','231596','366559','366796','386417','512401','512401','649000','698118','765324')

                    ELSE        caseManagementLocation.region <=> '1' AND
                                caseManagementLocation.baseLocation IS NOT NULL AND
                                caseManagementLocation.baseLocation IN (
                                    '231596', '698118', '366559', '386417', '512401',
                                    '227101', '765324', '366796', '324339', '649000',
                                    '999971', '420587', '28837')
            END
        )
        """)
        
        checks["valid_hearingCentreDynamicList_code_in_list_items"] = ("""
        (
            CASE 
                    WHEN Detained IN (1,2)  
                        THEN hearingCentreDynamicList.value.code IN ('765324','386417','231596','366559','512401','366796','698118','227101','649000')

                    ELSE hearingCentreDynamicList.value.code IS NOT NULL AND
                                hearingCentreDynamicList.value.code IN (
                                    '231596', '28837', '366559', '366796', '386417',
                                    '512401', '649000', '698118', '765324', '227101')
            END
            )
        """)

        checks["valid_hearingCentreDynamicList_label_in_list_items"] = ("""
        (
            CASE 
                    WHEN Detained IN (1,2)  
                        THEN hearingCentreDynamicList.value.label IN ('Taylor House Tribunal Hearing Centre','Hatton Cross Tribunal Hearing Centre','Birmingham Civil And Family Justice Centre','Atlantic Quay - Glasgow','Manchester Tribunal Hearing Centre - Piccadilly Exchange','Newcastle Civil And Family Courts And Tribunals Centre','Bradford Tribunal Hearing Centre','Newport Tribunal Centre - Columbus House','Yarls Wood Immigration And Asylum Hearing Centre')

                    ELSE hearingCentreDynamicList.value.label IS NOT NULL AND
                                hearingCentreDynamicList.value.label  IN (
                                    'Birmingham Civil And Family Justice Centre', 'Harmondsworth Tribunal Hearing Centre', 
                                    'Atlantic Quay - Glasgow', 'Newcastle Civil And Family Courts And Tribunals Centre', 'Hatton Cross Tribunal Hearing Centre',
                                    'Manchester Tribunal Hearing Centre - Piccadilly Exchange', 'Yarls Wood Immigration And Asylum Hearing Centre', 'Bradford Tribunal Hearing Centre', 'Taylor House Tribunal Hearing Centre', 'Newport Tribunal Centre - Columbus House')
            END
            )
        """)

        checks["valid_caseManagementLocationRefData_code_in_list_items"] = ("""
        (
            CASE 
                    WHEN Detained IN (1,2)  
                        THEN caseManagementLocationRefData.baseLocation.value.code IN ('765324','386417','231596','366559','512401','366796','698118','227101','649000')

                    ELSE caseManagementLocationRefData.baseLocation.value.code IS NOT NULL AND
                                caseManagementLocationRefData.baseLocation.value.code IN (
                                    '231596', '28837', '366559', '366796', '386417',
                                    '512401', '649000', '698118', '765324', '227101')
            END
            )
        """)

        checks["valid_caseManagementLocationRefData_label_in_list_items"] = ("""
        (
            
            CASE 
                    WHEN Detained IN (1,2)  
                        THEN caseManagementLocationRefData.baseLocation.value.label IN ('Taylor House Tribunal Hearing Centre','Hatton Cross Tribunal Hearing Centre','Birmingham Civil And Family Justice Centre','Atlantic Quay - Glasgow','Manchester Tribunal Hearing Centre - Piccadilly Exchange','Newcastle Civil And Family Courts And Tribunals Centre','Bradford Tribunal Hearing Centre','Newport Tribunal Centre - Columbus House','Yarls Wood Immigration And Asylum Hearing Centre')

                    ELSE caseManagementLocationRefData.baseLocation.value.label IS NOT NULL AND
                                caseManagementLocationRefData.baseLocation.value.label IN (
                                    'Birmingham Civil And Family Justice Centre', 'Harmondsworth Tribunal Hearing Centre', 
                                    'Atlantic Quay - Glasgow', 'Newcastle Civil And Family Courts And Tribunals Centre', 'Hatton Cross Tribunal Hearing Centre',
                                    'Manchester Tribunal Hearing Centre - Piccadilly Exchange', 'Yarls Wood Immigration And Asylum Hearing Centre', 'Bradford Tribunal Hearing Centre', 'Taylor House Tribunal Hearing Centre', 'Newport Tribunal Centre - Columbus House')
            END

        )
        """)

        checks["valid_selectedHearingCentreRefData_not_null"] = ("""
                                                                 
                        CASE 
                            WHEN Detained IN (1,2) THEN 
                            selectedHearingCentreRefData IN ('Atlantic Quay - Glasgow','Birmingham Civil And Family Justice Centre',
                            'Bradford Tribunal Hearing Centre','Hatton Cross Tribunal Hearing Centre','Manchester Tribunal Hearing Centre - Piccadilly Exchange',
                            'Newcastle Civil And Family Courts And Tribunals Centre','Newport Tribunal Centre - Columbus House','Taylor House Tribunal Hearing Centre',
                            'Yarls Wood Immigration And Asylum Hearing Centre')

                            ELSE
                            selectedHearingCentreRefData IN ('Atlantic Quay - Glasgow','Birmingham Civil And Family Justice Centre','Bradford Tribunal Hearing Centre',
                            'Hatton Cross Tribunal Hearing Centre','Manchester Tribunal Hearing Centre - Piccadilly Exchange','Harmondsworth Tribunal Hearing Centre',
                            'Newcastle Civil And Family Courts And Tribunals Centre','Newport Tribunal Centre - Columbus House','Taylor House Tribunal Hearing Centre',
                            'Yarls Wood Immigration And Asylum Hearing Centre')
                        END
 
                """)

        # ##############################
        # # ARIADM-758 (appellantDetails)
        # ##############################

        checks["valid_appellantInUk"] = ("""
            CASE    WHEN Detained IN (1,2,4) THEN appellantInUk = 'Yes'
                    WHEN array_contains(valid_categoryIdList, 37) THEN appellantInUk = 'Yes'
                    WHEN array_contains(valid_categoryIdList, 38) THEN appellantInUk = 'No'
                    ELSE appellantInUk IN ('Yes', 'No')
            END
        """)

        checks["valid_appealOutOfCountry"] = ("""
            CASE    WHEN appellantInUk = 'Yes' THEN appealOutOfCountry = 'No'
                    WHEN appellantInUk = 'No' THEN appealOutOfCountry = 'Yes'
                    ELSE appealOutOfCountry IS NULL
            END
        """)

        ##############################
        # ARIADM-760 (appellantDetails) - appellantHasFixedAddress and appellantAddress
        ##############################

        # Only include if CategoryIdList contains 37; check for 'Yes'
        checks["valid_appellantHasFixedAddress_yes_no_if_cat37"] = ("""
                                                                    
            CASE    WHEN Detained IN (1,2) THEN appellantHasFixedAddress IS NULL 
                    WHEN array_contains(valid_categoryIdList, 37) THEN appellantHasFixedAddress = 'Yes'
                    ELSE appellantHasFixedAddress IS NULL
            END
        """)

        # Only include if array_contains(valid_categoryIdList, 37)
        checks["valid_appellantAddress_AddressLine1_mandatory_and_length"] = ("""
            CASE 
                WHEN Detained IN (1,2) THEN appellantAddress IS NULL
                ELSE
                (array_contains(valid_categoryIdList, 37) AND appellantAddress.AddressLine1 IS NOT NULL AND LENGTH(appellantAddress.AddressLine1) <= 150) OR (appellantAddress.AddressLine1 IS NULL) 
            END
        """)
        checks["valid_appellantAddress_AddressLine2_length"] = ("""
            CASE 
                WHEN Detained IN (1,2) THEN appellantAddress IS NULL
                ELSE                                                 
                 (array_contains(valid_categoryIdList, 37) AND (appellantAddress.AddressLine2 IS NULL OR LENGTH(appellantAddress.AddressLine2) <= 50)) OR ( appellantAddress.AddressLine2 IS NULL)
            END
        """)
        checks["valid_appellantAddress_AddressLine3_length"] = ("""
            CASE 
                WHEN Detained IN (1,2) THEN appellantAddress IS NULL
                ELSE 
                    (array_contains(valid_categoryIdList, 37) AND (appellantAddress.AddressLine3 IS NULL OR LENGTH(appellantAddress.AddressLine3) <= 50)) OR (appellantAddress.AddressLine3 IS NULL)
            END
        """)
        checks["valid_appellantAddress_PostTown_length"] = ("""
            CASE 
                WHEN Detained IN (1,2) THEN appellantAddress IS NULL
                ELSE 
                    (array_contains(valid_categoryIdList, 37) AND (appellantAddress.PostTown IS NULL OR LENGTH(appellantAddress.PostTown) <= 50)) OR (appellantAddress.PostTown IS NULL)
            END
        """)
        checks["valid_appellantAddress_County_length"] = ("""
            CASE 
                WHEN Detained IN (1,2) THEN appellantAddress IS NULL
                ELSE 
                    (array_contains(valid_categoryIdList, 37) AND (appellantAddress.County IS NULL OR LENGTH(appellantAddress.County) <= 50)) OR (appellantAddress.County IS NULL)
            END
        """)
        checks["valid_appellantAddress_PostCode_length"] = ("""
            CASE 
                WHEN Detained IN (1,2) THEN appellantAddress IS NULL
                ELSE                                                
                    (array_contains(valid_categoryIdList, 37) AND (appellantAddress.PostCode IS NULL OR LENGTH(appellantAddress.PostCode) <= 14)) OR (appellantAddress.PostCode IS NULL)
            END
        """)
        checks["valid_appellantAddress_Country_length"] = ("""
            CASE 
                WHEN Detained IN (1,2) THEN appellantAddress IS NULL
                ELSE 
                    (array_contains(valid_categoryIdList, 37) AND (appellantAddress.Country IS NULL OR LENGTH(appellantAddress.Country) <= 50)) OR (appellantAddress.Country IS NULL)
            END
        """)

        checks["valid_TTL"] = ("""
            (
                TTL.Suspended = 'No'
                AND
                TTL.SystemTTL = date_format(date_add(DateLodged, 36524),'yyyy-MM-dd')
            )
        """)

        checks["valid_oocAppealAdminJ_values"] = """
        (
            CASE
                WHEN Detained IN (1,2,4)
                    THEN oocAppealAdminJ IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN oocAppealAdminJ IS NULL

                WHEN dv_appellantIsInUk = false
                    AND (
                        lu_HORef LIKE "%GWF%"
                    )
                    THEN oocAppealAdminJ = "entryClearanceDecision"

                WHEN dv_appellantIsInUk = false
                    THEN oocAppealAdminJ IS NULL

                ELSE oocAppealAdminJ IS NULL
            END
        )
        """

        checks["valid_appellantHasFixedAddressAdminJ_values"] = ("""
        (    CASE
                WHEN Detained IN (1,2,4)
                    THEN appellantHasFixedAddressAdminJ IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN appellantHasFixedAddressAdminJ IS NULL

                WHEN dv_appellantIsInUk = false
                    THEN appellantHasFixedAddressAdminJ = 'Yes'

                ELSE appellantHasFixedAddressAdminJ IS NULL
            END
        )
        """)

        checks["valid_addressLine1AdminJ_values"] = """
        (
            CASE
                WHEN Detained IN (1,2,4)
                    THEN addressLine1AdminJ IS NULL

                WHEN dv_representation NOT IN ('LR','AIP')
                    THEN addressLine1AdminJ IS NULL

                WHEN lu_appealType IS NULL
                    THEN addressLine1AdminJ IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN addressLine1AdminJ IS NULL

                WHEN dv_appellantIsInUk = false
                    AND coalesce(
                            Appellant_Address1,
                            Appellant_Address2,
                            Appellant_Address3,
                            Appellant_Address4,
                            Appellant_Address5,
                            Appellant_Postcode
                        ) IS NOT NULL
                    THEN addressLine1AdminJ = coalesce(
                            Appellant_Address1,
                            Appellant_Address2,
                            Appellant_Address3,
                            Appellant_Address4,
                            Appellant_Address5,
                            Appellant_Postcode
                        )

                WHEN dv_appellantIsInUk = false
                    THEN addressLine1AdminJ IS NULL

                ELSE addressLine1AdminJ IS NULL
            END
        )
        """

        checks["valid_addressLine2AdminJ_values"] = """
            CASE
                WHEN Detained IN (1,2,4)
                    THEN addressLine2AdminJ IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN addressLine2AdminJ IS NULL

                WHEN dv_representation NOT IN ('LR','AIP')
                    THEN addressLine2AdminJ IS NULL

                WHEN lu_appealType IS NULL
                    THEN addressLine2AdminJ IS NULL

                WHEN dv_appellantIsInUk = false 
                THEN
                    (
                        (
                            (
                                Appellant_Address2 IS NOT NULL
                                OR Appellant_Address3 IS NOT NULL
                                OR Appellant_Address4 IS NOT NULL
                                OR Appellant_Address5 IS NOT NULL
                                OR Appellant_Postcode IS NOT NULL
                            )
                            AND addressLine2AdminJ IS NOT NULL
                        )
                        OR
                        (
                            Appellant_Address2 IS NULL
                            AND Appellant_Address3 IS NULL
                            AND Appellant_Address4 IS NULL
                            AND Appellant_Address5 IS NULL
                            AND Appellant_Postcode IS NULL
                            AND addressLine2AdminJ IS NULL
                        )
                    )
            END
        """

        checks["valid_addressLine3AdminJ_values"] = """
        (
            CASE
                WHEN Detained IN (1,2,4)
                    THEN addressLine3AdminJ IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN addressLine3AdminJ IS NULL

                WHEN dv_representation NOT IN ('LR','AIP')
                    THEN addressLine3AdminJ IS NULL

                WHEN lu_appealType IS NULL
                    THEN addressLine3AdminJ IS NULL

                WHEN dv_appellantIsInUk = false
                    AND (
                        Appellant_Address3 IS NOT NULL
                        OR Appellant_Address4 IS NOT NULL
                    )
                    THEN addressLine3AdminJ =
                        concat_ws(', ', Appellant_Address3, Appellant_Address4)

                WHEN dv_appellantIsInUk = false
                    THEN addressLine3AdminJ IS NULL

                ELSE addressLine3AdminJ IS NULL
            END
        )
        """

        checks["valid_addressLine4AdminJ_values"] = """
        (
            CASE
                WHEN Detained IN (1,2,4)
                    THEN addressLine4AdminJ IS NULL

                WHEN dv_appellantIsInUk = true
                    THEN addressLine4AdminJ IS NULL

                WHEN dv_representation NOT IN ('LR','AIP')
                    THEN addressLine4AdminJ IS NULL

                WHEN lu_appealType IS NULL
                    THEN addressLine4AdminJ IS NULL

                WHEN dv_appellantIsInUk = false
                    AND (
                        Appellant_Address5 IS NOT NULL
                        OR Appellant_Postcode IS NOT NULL
                    )
                    THEN addressLine4AdminJ =
                        concat_ws(', ', Appellant_Address5, Appellant_Postcode)

                WHEN dv_appellantIsInUk = false
                    THEN addressLine4AdminJ IS NULL

                ELSE addressLine4AdminJ IS NULL
            END
        )
        """

        checks["valid_countryGovUkOocAdminJ"] = (
            """(
                    CASE
                        WHEN Detained IN (1,2,4)
                            THEN countryGovUkOocAdminJ IS NULL

                        WHEN dv_appellantIsInUk = true
                            THEN countryGovUkOocAdminJ IS NULL

                        WHEN dv_representation NOT IN ("LR","AIP")
                            THEN countryGovUkOocAdminJ IS NULL

                        WHEN lu_appealType IS NULL
                            THEN countryGovUkOocAdminJ IS NULL

                        WHEN dv_appellantIsInUk = false
                            THEN countryGovUkOocAdminJ IN ('AF', 'AX', 'AL', 'DZ', 'AD', 'AO', 'AI', 'AG', 'AR', 'AM', 'AW', 'AC', 'AU', 'AT', 'AZ', 'BS', 'BH', 'BD', 'BB', 'BY', 'BE', 'BZ', 'BJ', 'BM', 'BT', 'BO', 'BQ', 'BA', 'BW', 'BR', 'IO', 'VG', 'BN', 'BG', 'BF', 'BI', 'KH', 'CM', 'CA', 'IC', 'CV', 'KY', 'CF', 'EA', 'TD', 'CL', 'CN', 'CX', 'CO', 'KM', 'CD', 'CG', 'CK', 'CR', 'HR', 'CU', 'CW', 'CY', 'CZ', 'DK', 'DJ', 'DM', 'DO', 'EC', 'EG', 'SV', 'GQ', 'ER', 'EE', 'ET', 'FK', 'FO', 'FJ', 'FI', 'FR', 'GF', 'PF', 'TF', 'GA', 'GM', 'GE', 'DE', 'GH', 'GI', 'GR', 'GL', 'GD', 'GP', 'GT', 'GN', 'GW', 'GY', 'HT', 'HN', 'HK', 'HU', 'IS', 'IN', 'ID', 'IR', 'IQ', 'IE', 'IL', 'IT', 'CI', 'JM', 'JP', 'JO', 'KZ', 'KE', 'KI', 'KO', 'KW', 'KG', 'LA', 'LV', 'LB', 'LS', 'LR', 'LY', 'LI', 'LT', 'LU', 'MO', 'MK', 'MG', 'YT', 'MW', 'MY', 'MV', 'ML', 'MT', 'MQ', 'MR', 'MU', 'MX', 'MD', 'MN', 'ME', 'MS', 'MA', 'MZ', 'MM', 'NA', 'NR', 'NF', 'NP', 'NL', 'NC', 'NZ', 'NI', 'NE', 'NG', 'NU', 'KP', 'NO', 'OM', 'PK', 'PW', 'PA', 'PG', 'PY', 'PE', 'PH', 'PN', 'PL', 'PT', 'PR', 'QA', 'RE', 'RO', 'RU', 'RW', 'SM', 'ST', 'SA', 'SN', 'RS', 'SC', 'SL', 'SG', 'SK', 'SI', 'SB', 'ZA', 'KR', 'SS', 'ES', 'LK', 'BQ', 'SH', 'KN', 'LC', 'MF', 'VC', 'SD', 'SR', 'SZ', 'SE', 'CH', 'SY', 'TW', 'TJ', 'TZ', 'TH', 'TL', 'TG', 'TK', 'TO', 'TT', 'TN', 'TR', 'TM', 'TC', 'TV', 'UG', 'UA', 'AE', 'GB', 'UY', 'US', 'UZ', 'VU', 'VA', 'VE', 'VN', 'WF', 'EH', 'WS', 'YE', 'ZM', 'ZW', 'PS', 'SO', 'MH', 'MC', 'FM', 'BC', 'ZZ')
                        ELSE countryGovUkOocAdminJ IS NULL
                    END
            )"""
        )

        #########################################
        # ARIADM-2023 (homeOfficeDetails - Detained)
        #########################################

        # homeOfficeDecisionDate: populated when detained (1,2,4) OR in-UK; otherwise null.
        checks["valid_homeOfficeDecisionDate_format"] = (
            """(
                (
                    (Detained IN (1,2,4) OR dv_appellantIsInUk)
                    AND homeOfficeDecisionDate IS NOT NULL
                    AND homeOfficeDecisionDate RLIKE r'^\\d{4}-\\d{2}-\\d{2}$'
                )
                OR (homeOfficeDecisionDate IS NULL)
            )"""
        )

        # decisionLetterReceivedDate: only for OOC (not detained, not in-UK) AND not GWF; otherwise null.
        checks["valid_decisionLetterReceivedDate_format"] = (
            """(
                (
                    NOT dv_appellantIsInUk
                    AND Detained NOT IN (1,2,4)
                    AND decisionLetterReceivedDate IS NOT NULL
                    AND decisionLetterReceivedDate RLIKE r'^\\d{4}-\\d{2}-\\d{2}$'
                    AND COALESCE(lu_HORef, HORef, FCONumber, '') NOT LIKE '%GWF%'
                )
                OR (decisionLetterReceivedDate IS NULL)
            )"""
        )

        # dateEntryClearanceDecision: only for OOC (not detained, not in-UK) AND GWF; otherwise null.
        checks["valid_dateEntryClearanceDecision_format"] = (
            """(
                (
                    NOT dv_appellantIsInUk
                    AND Detained NOT IN (1,2,4)
                    AND COALESCE(lu_HORef, HORef, FCONumber, '') LIKE '%GWF%'
                    AND dateEntryClearanceDecision IS NOT NULL
                    AND dateEntryClearanceDecision RLIKE r'^\\d{4}-\\d{2}-\\d{2}$'
                )
                OR (dateEntryClearanceDecision IS NULL)
            )"""
        )

        # homeOfficeReferenceNumber: populated for detained/in-UK/non-GWF; '999999999' fallback for OOC+GWF.
        checks["valid_homeOfficeReferenceNumber_not_null"] = (
            """(
                (
                    (Detained IN (1,2,4) OR dv_appellantIsInUk
                     OR COALESCE(lu_HORef, HORef, FCONumber, '') NOT LIKE '%GWF%')
                    AND homeOfficeReferenceNumber IS NOT NULL
                )
                OR
                (
                    NOT dv_appellantIsInUk
                    AND Detained NOT IN (1,2,4)
                    AND COALESCE(lu_HORef, HORef, FCONumber, '') LIKE '%GWF%'
                    AND homeOfficeReferenceNumber = '999999999'
                )
            )"""
        )

        # gwfReferenceNumber: only for OOC (not detained, not in-UK) AND GWF; otherwise null.
        checks["valid_gwfReferenceNumber_not_null"] = (
            """(
                (
                    NOT dv_appellantIsInUk
                    AND Detained NOT IN (1,2,4)
                    AND COALESCE(lu_HORef, HORef, FCONumber, '') LIKE '%GWF%'
                    AND COALESCE(lu_HORef, HORef, FCONumber) IS NOT NULL
                    AND gwfReferenceNumber IS NOT NULL
                )
                OR
                (
                    dv_appellantIsInUk
                    OR Detained IN (1,2,4)
                    OR COALESCE(lu_HORef, HORef, FCONumber, '') NOT LIKE '%GWF%'
                    OR COALESCE(lu_HORef, HORef, FCONumber) IS NULL
                )
            )"""
        )

        return checks
