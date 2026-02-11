#############################
#TEST M2 Bronze : bronze_appealcase_caseappellant_appellant
#############################

def test_m2_bronze_table(config):

    TEST_NAME = "M2_BRONZE"
    BRONZE_TABLE_NAME = "ariadm_active_appeals.bronze_appealcase_caseappellant_appellant"

    #Tables and Field includes and Renames
    M2_TABLES_CONFIG = [
        {
            "name": "AppealCase",
            "include" : ["CaseNo"],
            "renames": {        
            }
        },
        {
            "name": "CaseAppellant",
            "include" : ["CaseNo", "Relationship","AppellantId"],
            "renames": {
            }
        },
        {
            "name": "Appellant",
            "include" : ["AppellantId", "Name", "Forenames","BirthDate","Email", "Telephone","Address1", "Address2", "Address3", "Address4", "Address5", "Postcode", "Detained", "AppellantCountryId", "FCONumber"],
            "renames": {
                "Name": "Appellant_Name",
                "Forenames": "Appellant_Forenames",
                "Email": "Appellant_Email",
                "Telephone": "Appellant_Telephone",            
                "Address1": "Appellant_Address1",
                "Address2": "Appellant_Address2",
                "Address3": "Appellant_Address3",
                "Address4": "Appellant_Address4",
                "Address5": "Appellant_Address5",
                "Postcode": "Appellant_Postcode",                      
            }
        }

    ]

    #Get SQL tables and Load into dict of dataframes    
    pqdfs = config["pqhelper"].load_sql_tables_into_df(M2_TABLES_CONFIG,config["PARQUET_BASE_PATH"],config["dbutils"])

    #Alias tables
    ac = pqdfs["AppealCase"].alias("ac")
    cap = pqdfs["CaseAppellant"].alias("cap")
    ap = pqdfs["Appellant"].alias("ap")

    #Join Into One large table
    sql_joined_df = (
        ac
        .join(cap, on="CaseNo", how="left")
        .join(ap, on="AppellantId", how="left")        
    )
    
    #drop AppellantId
    sql_joined_df = sql_joined_df.drop("AppellantId")

    #Get DF of bronze table
    bronze_df = config["spark"].read.table(BRONZE_TABLE_NAME)
    
    #Get Parquet Data and Compare to Sql Data
    results, sql_h,pq_h = config["run_tests"].run_table_tests(TEST_NAME,sql_joined_df,bronze_df,config["spark"] )
    result_text = f"Finished Running Tests on : {TEST_NAME}"
    return results,result_text,sql_h,pq_h



