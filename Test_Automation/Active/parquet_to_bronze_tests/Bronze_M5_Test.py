
#############################
#TEST M5 Bronze : bronze_appealcase_link_linkdetail
#############################

def test_m5_bronze_table(config):

    TEST_NAME = "M5_BRONZE"
    BRONZE_TABLE_NAME = "ariadm_active_appeals.bronze_appealcase_link_linkdetail"

    #Tables and Field includes and Renames
    M5_TABLES_CONFIG = [
        {
            "name": "Link",
            "include" : ["LinkNo", "CaseNo"],
            "renames": {        
            }
        },
        {
            "name": "LinkDetail",
            "include" : ["LinkNo", "ReasonLinkId"],
            "renames": {              
            }
        }
    ]

    #Get SQL tables and Load into dict of dataframes
    pqdfs = config["pqhelper"].load_sql_tables_into_df(M5_TABLES_CONFIG,config["PARQUET_BASE_PATH"],config["dbutils"])

    #Alias tables
    l = pqdfs["Link"].alias("l")
    ld = pqdfs["LinkDetail"].alias("ld")
    
    #Join Into One large table
    sql_joined_df = (
        l
        .join(ld, on="LinkNo", how="left")        
    )

    #Get DF of bronze table
    bronze_df = config["spark"].read.table(BRONZE_TABLE_NAME)
    
    #Get Parquet Data and Compare to Sql Data
    results, sql_h,pq_h = config["run_tests"].run_table_tests(TEST_NAME,sql_joined_df,bronze_df,config["spark"] )
    result_text = f"Finished Running Tests on : {TEST_NAME}"
    return results,result_text,sql_h,pq_h
   



