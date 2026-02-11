
#############################
#TEST M4 Bronze : bronze_appealcase_crep_rep_floc_cspon_cfs
#############################

def test_m4_bronze_table(config):

    TEST_NAME = "M4_BRONZE"
    BRONZE_TABLE_NAME = "ariadm_active_appeals.bronze_appealcase_transaction_transactiontype"

    #Tables and Field includes and Renames
    M4_TABLES_CONFIG = [
        {
            "name": "Transaction",
            "include" : ["CaseNo", "TransactionId", "TransactionTypeId" , "ReferringTransactionId", "Amount", "TransactionDate", "Status"],
            "renames": {        
            }
        },
        {
            "name": "TransactionType",
            "include" : ["TransactionTypeId", "SumBalance", "SumTotalFee", "SumTotalPay"],
            "renames": {              
            }
        }
    ]

    #Get SQL tables and Load into dict of dataframes
    pqdfs = config["pqhelper"].load_sql_tables_into_df(M4_TABLES_CONFIG,config["PARQUET_BASE_PATH"],config["dbutils"])

    #Alias tables
    t = pqdfs["Transaction"].alias("t")
    tt = pqdfs["TransactionType"].alias("tt")
    
    #Join Into One large table
    sql_joined_df = (
        t
        .join(tt, on="TransactionTypeId", how="left")        
    )

    #Get DF of bronze table
    bronze_df = config["spark"].read.table(BRONZE_TABLE_NAME)
    
    #Get Parquet Data and Compare to Sql Data
    results, sql_h,pq_h = config["run_tests"].run_table_tests(TEST_NAME,sql_joined_df,bronze_df,config["spark"] )
    result_text = f"Finished Running Tests on : {TEST_NAME}"
    return results,result_text,sql_h,pq_h
   



