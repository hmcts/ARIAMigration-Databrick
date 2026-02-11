from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


def get_sql_tables(SQL_JDBC_URL):  
    tables_df =spark.read \
    .format("jdbc") \
    .option("url", SQL_JDBC_URL) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("dbtable", "INFORMATION_SCHEMA.TABLES") \
    .load()

    sql_tables = [row.TABLE_NAME for row in tables_df.filter(tables_df.TABLE_TYPE == "BASE TABLE").select("TABLE_NAME").collect()]
    return sql_tables

def read_sql_table(table_name: str,SQL_JDBC_URL,SQL_SCHEMA):  
    return (
        spark.read
        .format("jdbc")
        .option("url", SQL_JDBC_URL)
        .option("dbtable", f"{SQL_SCHEMA}.{table_name}")
        .load()
    )