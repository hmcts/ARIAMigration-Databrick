from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, col, monotonically_increasing_id
from models.test_result import TestResult
from pyspark.sql.functions import upper
import functions.compare_data_helper as datahelper

spark = SparkSession.builder.getOrCreate()

def get_parquet_root_paths(base_path,dbutils):
    paths = []
    files = dbutils.fs.ls(base_path)
    
    is_parquet_folder = any(f.path.endswith(".parquet") for f in files)
    
    if is_parquet_folder:
        return [base_path]
    
    for f in files:
        if f.isDir:
            paths.extend(get_parquet_root_paths(f.path, dbutils))
            
    return paths

# all_parquet_folders = get_parquet_root_paths(path)

# len(all_parquet_folders)
# for path in all_parquet_folders:
#     print(path, "\n")


def load_sql_tables_into_df(config,PARQUET_BASE_PATH, dbutils):
    pqdfs = {}

    #get all tables into separate dataframes
    for t in config:
        #Get Data From Parquet    
        table_parquet_path = PARQUET_BASE_PATH + t["name"] + "/"    
        try:
            parquet_folders = get_parquet_root_paths(table_parquet_path,dbutils)
        except:
            pass

        #Read Parquet and Select only included fields
        pq_df = spark.read.parquet(parquet_folders[0])   
        select_exprs = [col(c) for c in t["include"]]
        pq_df = pq_df.select(*select_exprs)

        #Rename Any Fields that need renaming
        pq_df = datahelper.rename_columns(pq_df, t.get("renames", {}))
        
        #Add to List for later combining    
        pqdfs[t["name"]] = pq_df
    
    return pqdfs



