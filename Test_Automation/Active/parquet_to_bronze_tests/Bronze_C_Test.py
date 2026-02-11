
#############################
#TEST C Bronze : bronze_appealcategory
#############################

def test_c_bronze_table(config):

    TEST_NAME = "C_BRONZE"
    BRONZE_TABLE_NAME = "ariadm_active_appeals.bronze_appealcategory"

    #Tables and Field includes and Renames
    C_TABLES_CONFIG = [
        {
            "name": "AppealCategory",
            "include" : ["CaseNo", "CategoryId",],
            "renames": {        
            }
        }
    ]

    #Get SQL tables and Load into dict of dataframes
    pqdfs = config["pqhelper"].load_sql_tables_into_df(C_TABLES_CONFIG,config["PARQUET_BASE_PATH"],config["dbutils"])

    #Create DF of SQL table
    sql_joined_df = pqdfs["AppealCategory"]        

    #Get DF of bronze table
    bronze_df = config["spark"].read.table(BRONZE_TABLE_NAME)
    
    #Get Parquet Data and Compare to Sql Data
    results, sql_h,pq_h = config["run_tests"].run_table_tests(TEST_NAME,sql_joined_df,bronze_df,config["spark"] )
    result_text = f"Finished Running Tests on : {TEST_NAME}"
    return results,result_text,sql_h,pq_h
   




