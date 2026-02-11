


def run_table_tests(test_name,sql_joined_df,bronze_df,spark ):
    results = []
    import functions.compare_data_helper as datahelper
  
    #Check Counts
    results.append(datahelper.check_row_counts(test_name,sql_joined_df, bronze_df ))

    #Check Schema
    results.append(datahelper.check_schema2(test_name,sql_joined_df, bronze_df ))

    #Check Data
    result, sql_h,pq_h = datahelper.check_row_data(test_name, sql_joined_df, bronze_df, "")
    results.append(result)

    return results, sql_h,pq_h
