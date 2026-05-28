#############################
#TEST M3 Bronze : bronze_status_htype_clist_list_ltype_court_lsitting_adj
#This returns multiple rows per CaseNo (one per Status row).
#############################

from pyspark.sql.functions import col


def test_m3_bronze_table(config):

    TEST_NAME = "M3_BRONZE"
    BRONZE_TABLE_NAME = "ariadm_active_appeals.bronze_status_htype_clist_list_ltype_court_lsitting_adj"

    # Tables, included fields and renames.
    # Single-use tables: output columns keep their final bronze names; join-only keys
    # are renamed to a unique <alias>_<col> so they don't collide across the joins.
    # Adjudicator (x6) and ListSitting (x4) are loaded once here and copied per-use below.
    M3_TABLES_CONFIG = [
        {
            "name": "Status",
            "include": ["StatusId", "CaseNo", "CaseStatus", "Outcome", "KeyDate", "CentreId",
                        "DecisionDate", "Party", "DateReceived", "OutOfTime", "DecisionReserved",
                        "AdjudicatorId", "DeterminationBy", "Promulgated", "AdditionalLanguageId",
                        "ListedCentre"],
            "renames": {
                "KeyDate": "HearingDate",
                "Promulgated": "DateOfService",
                "ListedCentre": "HearingCentre",
            }
        },
        {
            "name": "CaseList",
            "include": ["StatusId", "ListId", "HearingTypeId", "StartTime", "TimeEstimate", "HearingDuration"],
            "renames": {
                "StatusId": "cl_StatusId",
                "ListId": "cl_ListId",
                "HearingTypeId": "cl_HearingTypeId",
            }
        },
        {
            "name": "List",
            "include": ["ListId", "ListName", "ListTypeId", "CourtId"],
            "renames": {
                "ListId": "l_ListId",
                "CourtId": "l_CourtId",
            }
        },
        {
            "name": "HearingType",
            "include": ["HearingTypeId", "Description"],
            "renames": {
                "HearingTypeId": "ht_HearingTypeId",
                "Description": "HearingType",
            }
        },
        {
            "name": "ListType",
            "include": ["ListTypeId", "Description"],
            "renames": {
                "ListTypeId": "lt_ListTypeId",
                "Description": "ListType",
            }
        },
        {
            "name": "Court",
            "include": ["CourtId", "CourtName"],
            "renames": {
                "CourtId": "c_CourtId",
            }
        },
        {
            "name": "AppealCase",
            "include": ["CaseNo", "Notes"],
            "renames": {
                "CaseNo": "ac_CaseNo",
            }
        },
        {
            "name": "Adjudicator",
            "include": ["AdjudicatorId", "Surname", "Forenames", "Title"],
            "renames": {}
        },
        {
            "name": "ListSitting",
            "include": ["ListId", "AdjudicatorId", "Position", "Cancelled"],
            "renames": {}
        },
    ]

    # Load each source table once into a dict of DataFrames
    data = config["pqhelper"].load_sql_tables_into_df(M3_TABLES_CONFIG, config["PARQUET_BASE_PATH"], config["dbutils"])

    # Single-use tables
    s  = data["Status"]
    cl = data["CaseList"]
    l  = data["List"]
    ht = data["HearingType"]
    lt = data["ListType"]
    c  = data["Court"]
    ac = data["AppealCase"]

    # Adjudicator is joined 6 times. Build an independently-named copy per use so the
    # self-joins have no column-name collisions (key column + the 3 output columns).
    adj = data["Adjudicator"]

    def adj_instance(key_alias, surname, forenames, title):
        return adj.select(
            col("AdjudicatorId").alias(key_alias),
            col("Surname").alias(surname),
            col("Forenames").alias(forenames),
            col("Title").alias(title),
        )

    adjs   = adj_instance("adjs_key",   "Adj_Surname",               "Adj_Forenames",               "Adj_Title")
    adjsd  = adj_instance("adjsd_key",  "Adj_Determination_Surname", "Adj_Determination_Forenames", "Adj_Determination_Title")
    adjLS1 = adj_instance("adjLS1_key", "Judge1FT_Surname",          "Judge1FT_Forenames",          "Judge1FT_Title")
    adjLS2 = adj_instance("adjLS2_key", "Judge2FT_Surname",          "Judge2FT_Forenames",          "Judge2FT_Title")
    adjLS3 = adj_instance("adjLS3_key", "Judge3FT_Surname",          "Judge3FT_Forenames",          "Judge3FT_Title")
    adjLS4 = adj_instance("adjLS4_key", "CourtClerk_Surname",        "CourtClerk_Forenames",        "CourtClerk_Title")

    # ListSitting is joined 4 times — same treatment
    lsit = data["ListSitting"]

    def ls_instance(prefix):
        return lsit.select(
            col("ListId").alias(f"{prefix}_ListId"),
            col("AdjudicatorId").alias(f"{prefix}_AdjudicatorId"),
            col("Position").alias(f"{prefix}_Position"),
            col("Cancelled").alias(f"{prefix}_Cancelled"),
        )

    LS1 = ls_instance("LS1")
    LS2 = ls_instance("LS2")
    LS3 = ls_instance("LS3")
    LS4 = ls_instance("LS4")

    # Join into one large table, mirroring the spec SQL
    joined = (
        s
        .join(cl,     s.StatusId          == cl.cl_StatusId,         "left_outer")
        .join(l,      l.l_ListId          == cl.cl_ListId,          "left_outer")
        .join(ht,     cl.cl_HearingTypeId == ht.ht_HearingTypeId,   "left_outer")
        .join(adjs,   s.AdjudicatorId     == adjs.adjs_key,         "left_outer")
        .join(adjsd,  s.DeterminationBy   == adjsd.adjsd_key,       "left_outer")
        .join(lt,     l.ListTypeId        == lt.lt_ListTypeId,      "left_outer")
        .join(c,      c.c_CourtId         == l.l_CourtId,           "left_outer")
        .join(ac,     ac.ac_CaseNo        == s.CaseNo,              "left_outer")
        .join(LS1,   (l.l_ListId == LS1.LS1_ListId) & (LS1.LS1_Position == 10) & (LS1.LS1_Cancelled == 0), "left_outer")
        .join(adjLS1, LS1.LS1_AdjudicatorId == adjLS1.adjLS1_key,   "left_outer")
        .join(LS2,   (l.l_ListId == LS2.LS2_ListId) & (LS2.LS2_Position == 11) & (LS2.LS2_Cancelled == 0), "left_outer")
        .join(adjLS2, LS2.LS2_AdjudicatorId == adjLS2.adjLS2_key,   "left_outer")
        .join(LS3,   (l.l_ListId == LS3.LS3_ListId) & (LS3.LS3_Position == 12) & (LS3.LS3_Cancelled == 0), "left_outer")
        .join(adjLS3, LS3.LS3_AdjudicatorId == adjLS3.adjLS3_key,   "left_outer")
        .join(LS4,   (l.l_ListId == LS4.LS4_ListId) & (LS4.LS4_Position == 3) & (LS4.LS4_Cancelled == 0), "left_outer")
        .join(adjLS4, LS4.LS4_AdjudicatorId == adjLS4.adjLS4_key,   "left_outer")
    )

    # Project only the columns present in the bronze table (drop the join-key helpers)
    sql_joined_df = joined.select(
        "StatusId", "CaseNo", "CaseStatus", "Outcome", "HearingDate", "CentreId",
        "DecisionDate", "Party", "DateReceived", "OutOfTime", "DecisionReserved",
        "AdjudicatorId", "DeterminationBy",
        "Adj_Surname", "Adj_Forenames", "Adj_Title",
        "Adj_Determination_Surname", "Adj_Determination_Forenames", "Adj_Determination_Title",
        "DateOfService", "AdditionalLanguageId", "HearingCentre",
        "CourtName", "ListName", "ListTypeId", "ListType", "HearingType",
        "StartTime", "TimeEstimate", "HearingDuration",
        "Judge1FT_Surname", "Judge1FT_Forenames", "Judge1FT_Title",
        "Judge2FT_Surname", "Judge2FT_Forenames", "Judge2FT_Title",
        "Judge3FT_Surname", "Judge3FT_Forenames", "Judge3FT_Title",
        "CourtClerk_Surname", "CourtClerk_Forenames", "CourtClerk_Title",
        "Notes",
    )

    # Get DF of bronze table
    bronze_df = config["spark"].read.table(BRONZE_TABLE_NAME)

    # Compare to bronze
    results, sql_h, pq_h = config["run_tests"].run_table_tests(TEST_NAME, sql_joined_df, bronze_df, config["spark"])
    result_text = f"Finished Running Tests on : {TEST_NAME}"
    return results, result_text, sql_h, pq_h
