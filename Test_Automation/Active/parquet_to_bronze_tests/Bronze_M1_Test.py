
#############################
#TEST M1 Bronze : bronze_appealcase_crep_rep_floc_cspon_cfs
#############################

def test_m1_bronze_table(config):

    TEST_NAME = "M1_BRONZE"
    BRONZE_TABLE_NAME = "ariadm_active_appeals.bronze_appealcase_crep_rep_floc_cspon_cfs"

    #Tables and Field includes and Renames
    M1_TABLES_CONFIG = [
        {
            "name": "AppealCase",
            "include" : ["CaseNo", "CasePrefix", "OutOfTimeIssue" , "DateLodged", "DateAppealReceived", "CentreId", "NationalityId", "AppealTypeId", "DeportationDate", "RemovalDate", "VisitVisaType", "DateOfApplicationDecision", "HORef", "InCamera", "CourtPreference", "LanguageId", "Interpreter"],
            "renames": {        
            }
        },
        {
            "name": "CaseRep",
            "include" : ["CaseNo", "RepresentativeId", "Name", "Address1", "Address2", "Address3", "Address4", "Address5", "Postcode", "Contact", "Email","FileSpecificEmail"],
            "renames": {
                "Name": "CaseRep_Name",
                "Address1": "CaseRep_Address1",
                "Address2": "CaseRep_Address2",
                "Address3": "CaseRep_Address3",
                "Address4": "CaseRep_Address4",
                "Address5": "CaseRep_Address5",
                "Postcode": "CaseRep_Postcode",
                "Email": "CaseRep_Email", 
                "FileSpecificEmail": "CaseRep_FileSpecific_Email",           
            }
        },
        {
            "name": "Representative",
            "include" : ["RepresentativeId", "Name", "Address1", "Address2", "Address3", "Address4", "Address5", "Postcode", "Email"],
            "renames": {
                "Name": "Rep_Name",
                "Address1": "Rep_Address1",
                "Address2": "Rep_Address2",
                "Address3": "Rep_Address3",
                "Address4": "Rep_Address4",
                "Address5": "Rep_Address5",
                "Postcode": "Rep_Postcode",
                "Email": "Rep_Email",          
            }
        },
        {
            "name": "CaseSponsor",
            "include" : ["CaseNo","Name","Forenames", "Address1", "Address2", "Address3", "Address4", "Address5", "Postcode", "Email", "Telephone", "Authorised"], 
            "renames": {
                "Name": "Sponsor_Name",
                "Forenames": "Sponsor_Forenames",
                "Address1": "Sponsor_Address1",
                "Address2": "Sponsor_Address2",
                "Address3": "Sponsor_Address3",
                "Address4": "Sponsor_Address4",
                "Address5": "Sponsor_Address5",
                "Postcode": "Sponsor_Postcode",
                "Email": "Sponsor_Email",
                "Telephone": "Sponsor_Telephone",
                "Authorised": "Sponsor_Authorisation",

            }
        } ,
        {
            "name": "CaseRespondent",
            "include": ["CaseNo","MainRespondentId"],
            "renames": {
            }
        },
        {
            "name": "FileLocation",
            "include": ["CaseNo","DeptId"],
            "renames": {            
            }
        },
        {
            "name": "CaseFeeSummary",
            "include": ["CaseNo","PaymentRemissionRequested","PaymentRemissionReason", "PaymentRemissionGranted", "PaymentRemissionReasonNote", "LSCReference", "ASFReferenceNo", "DateCorrectFeeReceived", "CaseFeeSummaryId"],
            "renames": {            
            }
        }
    ]

    #Get SQL tables and Load into dict of dataframes
    pqdfs = config["pqhelper"].load_sql_tables_into_df(M1_TABLES_CONFIG,config["PARQUET_BASE_PATH"],config["dbutils"])

    #Alias tables
    ac = pqdfs["AppealCase"].alias("ac")
    crep = pqdfs["CaseRep"].alias("crep")
    rep = pqdfs["Representative"].alias("rep")
    cspon = pqdfs["CaseSponsor"].alias("cspon")
    cr = pqdfs["CaseRespondent"].alias("cr")
    floc = pqdfs["FileLocation"].alias("floc")
    cfs = pqdfs["CaseFeeSummary"].alias("cfs")

    #Join Into One large table
    sql_joined_df = (
        ac
        .join(crep, on="CaseNo", how="left")
        .join(rep, on="RepresentativeId", how="left")
        .join(cspon, on="CaseNo", how="left")
        .join(cr, on="CaseNo", how="left")
        .join(floc, on="CaseNo", how="left")
        .join(cfs, on="CaseNo", how="left")    
    )

    #Get DF of bronze table
    bronze_df = config["spark"].read.table(BRONZE_TABLE_NAME)
    
    #Get Parquet Data and Compare to Sql Data
    results, sql_h,pq_h = config["run_tests"].run_table_tests(TEST_NAME,sql_joined_df,bronze_df,config["spark"] )
    result_text = f"Finished Running Tests on : {TEST_NAME}"
    return results,result_text,sql_h,pq_h
   



