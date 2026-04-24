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
        checks["valid_hasSponsor_yes_no"] = (
            """(
                (Sponsor_Name IS NOT NULL AND hasSponsor <=> 'Yes')
                OR (Sponsor_Name IS NULL AND hasSponsor <=> 'No')
                OR (hasSponsor IS NULL)
            )"""
        )
        checks["valid_sponsorGivenNames_not_null"] = (
            "((Sponsor_Name IS NOT NULL AND sponsorGivenNames IS NOT NULL) OR (sponsorGivenNames IS NULL))"
        )

        checks["valid_sponsorFamilyName_not_null"] = (
            "((Sponsor_Name IS NOT NULL AND sponsorFamilyName IS NOT NULL) OR (sponsorFamilyName IS NULL))"
        )

        checks["valid_sponsorAuthorisation_yes_no"] = (
            """(
                (Sponsor_Name IS NOT NULL AND Sponsor_Authorisation <=> True AND sponsorAuthorisation <=> 'Yes')
                OR (Sponsor_Name IS NOT NULL AND Sponsor_Authorisation <=> False AND sponsorAuthorisation <=> 'No')
                OR (Sponsor_Name IS NULL AND sponsorAuthorisation IS NULL)
            )"""
        )

        ############################################################
        # ARIADM-776 (SponsorDetails) New Logic with ARIADM-1028
        ############################################################
        checks["valid_sponsorAddress_not_null"] = (
            "((Sponsor_Name IS NOT NULL AND sponsorAddress IS NOT NULL) OR (Sponsor_Name IS NULL AND sponsorAddress IS NULL))"
        )

        checks["valid_sponsorAddressForDisplay"] = (
            "(Sponsor_Name IS NOT NULL AND sponsorAddressForDisplay IS NOT NULL) OR (Sponsor_Name IS NULL AND sponsorAddressForDisplay IS NULL)"
        )

        checks["valid_sponsorNameForDisplay"] = (
            "(Sponsor_Name IS NOT NULL AND sponsorNameForDisplay IS NOT NULL) OR (Sponsor_Name IS NULL AND sponsorNameForDisplay IS NULL)"
        )

        ##############################
        # ARIADM-778 (SponsorDetails)
        ##############################
        checks["valid_sponsorEmailAdminJ"] = (
            "(( sponsorEmailAdminJ IS NOT NULL) OR (sponsorEmailAdminJ IS NULL))"
        )

        checks["valid_sponsorMobileNumberAdminJ"] = (
            "(((sponsorMobileNumberAdminJ IS NOT NULL AND sponsorMobileNumberAdminJ RLIKE r'^((\\+44(\\s\\(0\\)\\s|\\s0\\s|\\s)?)|0)7\\d{3}(\\s)?\\d{6}$')) OR (sponsorMobileNumberAdminJ IS NULL))"
        )

        ##############################
        # ARIADM-778 (General)
        ##############################

        checks["valid_applicationChangeDesignatedHearingCentre_fixed_list"] = (
            """(
                (applicationChangeDesignatedHearingCentre IS NOT NULL)
                AND
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
                    THEN staffLocation IN ('Taylor House','Hatton Cross','Birmingham','Glasgow','Manchester','Newcastle','Bradford','Newport','YarlsWood')
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
                                                                 
                        (selectedHearingCentreRefData IS NOT NULL)
                        AND 
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
            CASE    WHEN Detained IN (1,2,4) OR array_contains(valid_categoryIdList, 37) THEN appellantInUk = 'Yes'
                    WHEN array_contains(valid_categoryIdList, 38) THEN appellantInUk = 'No'
                    ELSE appellantInUk IS  NULL
            END
        """)

        checks["valid_appealOutOfCountry"] = ("""
            CASE    WHEN Detained IN (1,2,4) OR array_contains(valid_categoryIdList, 37) THEN appealOutOfCountry = 'No'
                    WHEN array_contains(valid_categoryIdList, 38) THEN appealOutOfCountry = 'Yes'
                    ELSE appealOutOfCountry IS  NULL
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


        return checks
