
#############################
#TEST M6 Bronze : bronze_caseadjudicator_adjudicator
#############################

def test_m6_bronze_table(config):

    TEST_NAME = "M6_BRONZE"
    BRONZE_TABLE_NAME = "ariadm_active_appeals.bronze_caseadjudicator_adjudicator"

    #Tables and Field includes and Renames
    M6_TABLES_CONFIG = [
        {
            "name": "CaseAdjudicator",
            "include" : ["CaseNo", "Required","AdjudicatorId"],
            "renames": {        
            }
        },
        {
            "name": "Adjudicator",
            "include" : ["Surname", "Forenames", "Title","Adjudicatorid", "DoNotList"],
            "renames": {
                "Surname" : "Judge_Surname",
                "Forenames" : "Judge_Forenames",
                "Title" : "Judge_Title",
            }
        }
    ]

    #Get SQL tables and Load into dict of dataframes
    pqdfs = config["pqhelper"].load_sql_tables_into_df(M6_TABLES_CONFIG,config["PARQUET_BASE_PATH"],config["dbutils"])

    #Alias tables
    ca = pqdfs["CaseAdjudicator"].alias("ca")
    adj = pqdfs["Adjudicator"].alias("adj")
    
    #Join
    sql_joined_df = ca.join(adj,ca["AdjudicatorId"] == adj["AdjudicatorId"], how="inner" ).filter(adj["DoNotList"] == False)
        
    #Drop Fields not required
    sql_joined_df = sql_joined_df.drop("AdjudicatorId", "Adjudicatorid","DoNotList")

    #Get DF of bronze table
    bronze_df = config["spark"].read.table(BRONZE_TABLE_NAME)
    
    #Get Parquet Data and Compare to Sql Data
    results, sql_h,pq_h = config["run_tests"].run_table_tests(TEST_NAME,sql_joined_df,bronze_df,config["spark"] )
    result_text = f"Finished Running Tests on : {TEST_NAME}"
    return results,result_text,sql_h,pq_h
   




