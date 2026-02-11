
#############################
#TEST D Bronze : bronze_documentsreceived
#############################

def test_d_bronze_table(config):

    TEST_NAME = "D_BRONZE"
    BRONZE_TABLE_NAME = "ariadm_active_appeals.bronze_documentsreceived"

    #Tables and Field includes and Renames
    D_TABLES_CONFIG = [
        {
            "name": "DocumentsReceived",
            "include" : ["CaseNo", "ReceivedDocumentId","DateReceived"],
            "renames": {        
            }
        }
    ]

    #Get SQL tables and Load into dict of dataframes
    pqdfs = config["pqhelper"].load_sql_tables_into_df(D_TABLES_CONFIG,config["PARQUET_BASE_PATH"],config["dbutils"])

    #Create DF of SQL table
    sql_joined_df = pqdfs["DocumentsReceived"]        

    #Get DF of bronze table
    bronze_df = config["spark"].read.table(BRONZE_TABLE_NAME)
    
    #Get Parquet Data and Compare to Sql Data
    results, sql_h,pq_h = config["run_tests"].run_table_tests(TEST_NAME,sql_joined_df,bronze_df,config["spark"] )
    result_text = f"Finished Running Tests on : {TEST_NAME}"
    return results,result_text,sql_h,pq_h
   




