from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sha2, concat_ws, col, monotonically_increasing_id, upper,
    regexp_extract, max as spark_max,
)
from models.test_result import TestResult
import functions.compare_data_helper as datahelper

spark = SparkSession.builder.getOrCreate()


def get_parquet_root_paths(base_path, dbutils):
    # Kept for backwards-compatibility with any callers that still use it.
    paths = []
    files = dbutils.fs.ls(base_path)

    is_parquet_folder = any(f.path.endswith(".parquet") for f in files)

    if is_parquet_folder:
        return [base_path]

    for f in files:
        if f.isDir:
            paths.extend(get_parquet_root_paths(f.path, dbutils))

    return paths


def _deep_ls_parquet(path, dbutils):
    """Recursively list every .parquet file under a path."""
    found = []
    for f in dbutils.fs.ls(path):
        if f.path.endswith(".parquet"):
            found.append(f.path)
        elif f.isDir:
            found.extend(_deep_ls_parquet(f.path, dbutils))
    return found


def get_latest_parquet_file(table_name, base_path, dbutils):
    """Pick the latest snapshot file for a table, matching how the bronze builder
    sources its raw_* tables (read_latest_parquet):
      - look under {base_path}{table_name}/full/
      - take the file with the highest _<timestamp>.parquet suffix
    """
    folder_path = f"{base_path}{table_name}/full/"
    all_files = _deep_ls_parquet(folder_path, dbutils)
    if not all_files:
        raise FileNotFoundError(f"No .parquet files found in {folder_path}")

    file_df = spark.createDataFrame([(f,) for f in all_files], ["file_path"])
    file_df = file_df.withColumn(
        "timestamp", regexp_extract("file_path", r"_(\d+)\.parquet$", 1).cast("long")
    )
    max_ts = file_df.agg(spark_max("timestamp")).collect()[0][0]
    latest_file = file_df.filter(col("timestamp") == max_ts).first()["file_path"]
    return latest_file


def load_sql_tables_into_df(config, PARQUET_BASE_PATH, dbutils):
    pqdfs = {}

    # get each table into its own dataframe
    for t in config:
        # Read the latest snapshot from {table}/full/ — same source selection the
        # bronze builder uses, so the test compares against the snapshot the bronze
        # table was actually built from.
        latest_file = get_latest_parquet_file(t["name"], PARQUET_BASE_PATH, dbutils)
        pq_df = spark.read.option("inferSchema", "true").parquet(latest_file)

        # Read Parquet and select only included fields
        select_exprs = [col(c) for c in t["include"]]
        pq_df = pq_df.select(*select_exprs)

        # Rename any fields that need renaming
        pq_df = datahelper.rename_columns(pq_df, t.get("renames", {}))

        # Add to dict for later combining
        pqdfs[t["name"]] = pq_df

    return pqdfs
