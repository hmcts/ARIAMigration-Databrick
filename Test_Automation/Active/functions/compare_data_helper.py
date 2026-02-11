from pyspark.sql.functions import sha2, concat_ws, col, monotonically_increasing_id
from models.test_result import TestResult
from pyspark.sql.functions import upper

#####################
def check_schema(table, sql_df, parquet_df):
    from collections import namedtuple

    # Define compatible types mapping
    SAFE_TYPE_EQUIVALENCE = {
        "smallint": {"int", "bigint"},
        "int": {"bigint"},
        # You can add more mappings if needed
    }

    def types_match(sql_type, pq_type):
        """Return True if types are identical or safely compatible"""
        if sql_type == pq_type:
            return True
        return pq_type in SAFE_TYPE_EQUIVALENCE.get(sql_type, set())

    sql_cols = [(f.name, f.dataType.simpleString()) for f in sql_df.schema.fields]
    pq_cols = [(f.name, f.dataType.simpleString()) for f in parquet_df.schema.fields]

    # Compare columns by name and type
    if len(sql_cols) != len(pq_cols):
        return TestResult(
            table,
            "SCHEMA",
            "FAIL",
            f"Column count mismatch: SQL={len(sql_cols)}, Parquet={len(pq_cols)}\nsql_cols={str(sql_cols)}\npq_cols={str(pq_cols)}"
        )

    mismatches = []
    #TODO - look at why line below works for sql to parquet but for parquet to bronze, it needs set to order them correctly.
    for (sql_name, sql_type), (pq_name, pq_type) in zip(sql_cols, pq_cols):
    # for (sql_name, sql_type), (pq_name, pq_type) in zip(dict(sorted(sql_cols.items())), dict(sorted(pq_cols.items()))):
        if sql_name != pq_name:
            mismatches.append(f"Column name mismatch: SQL={sql_name}, Parquet={pq_name}")
        elif not types_match(sql_type, pq_type):
            mismatches.append(f"Type mismatch for {sql_name}: SQL={sql_type}, Parquet={pq_type}")

    if not mismatches:
        return TestResult(table, "SCHEMA", "PASS", "Schema matches")
    else:
        return TestResult(table, "SCHEMA", "FAIL", "; ".join(mismatches))
    
#####################
#Check Schema 2 - With sort on cols list
def check_schema2(table, sql_df, parquet_df):
    from collections import namedtuple

    # Define compatible types mapping
    SAFE_TYPE_EQUIVALENCE = {
        "smallint": {"int", "bigint"},
        "int": {"bigint"},
        # You can add more mappings if needed
    }

    def types_match(sql_type, pq_type):
        """Return True if types are identical or safely compatible"""
        if sql_type == pq_type:
            return True
        return pq_type in SAFE_TYPE_EQUIVALENCE.get(sql_type, set())

    sql_cols = [(f.name, f.dataType.simpleString()) for f in sql_df.schema.fields]
    pq_cols = [(f.name, f.dataType.simpleString()) for f in parquet_df.schema.fields]

    # Compare columns by name and type
    if len(sql_cols) != len(pq_cols):
        return TestResult(
            table,
            "SCHEMA",
            "FAIL",
            f"Column count mismatch: SQL={len(sql_cols)}, Parquet={len(pq_cols)}"
        )

    mismatches = []
    for (sql_name, sql_type), (pq_name, pq_type) in zip(set(sql_cols), set(pq_cols)):
        if sql_name != pq_name:
            mismatches.append(f"Column name mismatch: SQL={sql_name}, Parquet={pq_name}")
        elif not types_match(sql_type, pq_type):
            mismatches.append(f"Type mismatch for {sql_name}: SQL={sql_type}, Parquet={pq_type}")

    if not mismatches:
        return TestResult(table, "SCHEMA", "PASS", "Schema matches")
    else:
        return TestResult(table, "SCHEMA", "FAIL", "; ".join(mismatches))
#####################
def rename_columns(df, rename_map: dict):
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df

#####################
def union_all(dfs, reduce):
    return reduce(lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True), dfs)


#####################
def check_row_counts(table, sql_df, parquet_df):
    sql_count = sql_df.count()
    parquet_count = parquet_df.count()

    if sql_count == parquet_count:
        return TestResult(
            table, "ROW_COUNT", "PASS",
            f"Row count = {sql_count}"
        )
    else:
        return TestResult(
            table, "ROW_COUNT", "FAIL",
            f"SQL={sql_count}, Parquet={parquet_count}"
        )

#####################
def with_row_hash_and_index(df, exclude_cols=None):
    exclude_cols = exclude_cols or []
    cols = sorted([c for c in df.columns if c not in exclude_cols])

    # Add hash
    df_hashed = df.withColumn(
        "_row_hash",
        sha2(concat_ws("||", *[col(c).cast("string") for c in cols]), 256)
    )

    # Add row index
    df_indexed = df_hashed.withColumn("_row_num", monotonically_increasing_id())

    return df_indexed

#####################
def check_row_data(table, sql_df, parquet_df, exclude_cols=None):
    #for Spot_Work_Jobs - upper JobID column for compare
    try:
        if table == "Spot_Work_Jobs":
            parquet_df = parquet_df.withColumn("JobID", upper("JobID"))
    except:
        pass
    
    sql_h = with_row_hash_and_index(sql_df, exclude_cols)
    pq_h = with_row_hash_and_index(parquet_df, exclude_cols)
    # display (sql_h)
    # display (pq_h)



    # Join on hash to find mismatches
    diff_sql = sql_h.join(
        pq_h.select("_row_hash"),
        on="_row_hash",
        how="left_anti"
    )
    diff_pq = pq_h.join(
        sql_h.select("_row_hash"),
        on="_row_hash",
        how="left_anti"
    )

    diff_count = diff_sql.count() + diff_pq.count()

    if diff_count == 0:
        return TestResult(table, "DATA_HASH", "PASS", f"All rows match"), sql_h, pq_h
    else:
        # Collect row numbers of mismatches (first 5 only)
        sql_rows = [r._row_num for r in diff_sql.limit(5).collect()]
        pq_rows = [r._row_num for r in diff_pq.limit(5).collect()]

        return TestResult(
            table,
            "DATA_HASH",
            "FAIL",
            f"Mismatched rows: {diff_count}. "
            f"Sample SQL row indices: {sql_rows}, Parquet row indices: {pq_rows}"
        ), sql_h, pq_h


