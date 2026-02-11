#############################
#TEST M3 Bronze : bronze_status_htype_clist_list_ltype_court_lsitting_adj
#############################

def test_m3_bronze_table(config):

    TEST_NAME = "M3_BRONZE"
    BRONZE_TABLE_NAME = "ariadm_active_appeals.bronze_status_htype_clist_list_ltype_court_lsitting_adj"

    #Tables and Field includes and Renames
    
    M3_TABLES_CONFIG = [
       {
            "name": "Status",
            "include" : ["StatusId", "CaseNo", "CaseStatus", "Outcome", "KeyDate", "CentreId", "DecisionDate", "Party", "DateReceived", "OutOfTime", "DecisionReserved", "AdjudicatorId", "Promulgated", "AdditionalLanguageId","ListedCentre"],
            "renames": {        
            }
        },
       {          
            "name": "CaseList",
            "include" : ["StatusId", "StartTime","TimeEstimate", "HearingDuration"],
            "renames": {
                "Surname": "Adj_Surname",
                "Forenames":"Adj_Forenames",
                "Title": "Adj_Title"
            }
        },
        {            
            "name": "Adjudicator",
            "include" : ["Surname", "Forenames","Title", "AdjudicatorId"],
            "renames": {
                "Surname": "Adj_Surname",
                "Forenames":"Adj_Forenames",
                "Title": "Adj_Title"
            }
        },
        {             
            "name": "Court",
            "include" : ["CourtId", "CourtName"],
            "renames": {        
            }
        },
        {            
            "name": "List",
            "include" : ["ListId", "ListName", "ListTypeId"],
            "renames": {                       
            }
        },
           {            
            "name": "ListType",
            "include" : ["ListTypeId", "Description"],
            "renames": {   
                "Description":"ListType",                    
            }
        },
        {            
            "name": "HearingType",
            "include" : ["HearingTypeId", "Description"],
            "renames": { 
                  "Description":"HearingType",                        
            }
        } 
    ]

    #Get SQL tables and Load into dict of dataframes    
    data = config["pqhelper"].load_sql_tables_into_df(M3_TABLES_CONFIG,config["PARQUET_BASE_PATH"],config["dbutils"])

    #Alias tables
    # s = pqdfs["AppealCase"].alias("s")
    # adjs = pqdfs["Adjudicator"].alias("adjs")
    s = data["Status"].alias("s")
    cl = data["CaseList"].alias("cl")
    l = data["List"].alias("l")
    ht = data["HearingType"].alias("ht")
    adjs = data["Adjudicator"].alias("adjs")
    adjsd = data["Adjudicator"].alias("adjsd")
    lt = data["ListType"].alias("lt")
    c = data["Court"].alias("c")
    ac = data["AppealCase"].alias("ac")
    LS1 = data["ListSitting"].alias("LS1")
    adjLS1 = data["Adjudicator"].alias("adjLS1")
    LS2 = data["ListSitting"].alias("LS2")
    adjLS2 = data["Adjudicator"].alias("adjLS2")
    LS3 = data["ListSitting"].alias("LS3")
    adjLS3 = data["Adjudicator"].alias("adjLS3")
    LS4 = data["ListSitting"].alias("LS4")
    adjLS4 = data["Adjudicator"].alias("adjLS4")
       

    joined_df = (
    s
    .join(cl, s.StatusId == cl.StatusId, "left_outer")
    .join(l, l.ListId == cl.ListId, "left_outer")
    .join(ht, ht.HearingTypeId == cl.HearingTypeId, "left_outer")
    .join(adjs, adjs.AdjudicatorId == s.AdjudicatorId, "left_outer")
    .join(adjsd, adjsd.AdjudicatorId == s.DeterminationBy, "left_outer")
    .join(lt, lt.ListTypeId == l.ListTypeId, "left_outer")
    .join(c, c.CourtId == l.CourtId, "left_outer")
    .join(ac, ac.CaseNO == s.CaseNo, "left_outer")
    # LS1 and adjLS1
    .join(LS1, (LS1.ListId == l.ListId) & (LS1.Position == 10) & (LS1.Cancelled == 0), "left_outer")
    .join(adjLS1, adjLS1.AdjudicatorId == LS1.AdjudicatorId, "left_outer")
    # LS2 and adjLS2
    .join(LS2, (LS2.ListId == l.ListId) & (LS2.Position == 11) & (LS2.Cancelled == 0), "left_outer")
    .join(adjLS2, adjLS2.AdjudicatorId == LS2.AdjudicatorId, "left_outer")
    # LS3 and adjLS3
    .join(LS3, (LS3.ListId == l.ListId) & (LS3.Position == 12) & (LS3.Cancelled == 0), "left_outer")
    .join(adjLS3, adjLS3.AdjudicatorId == LS3.AdjudicatorId, "left_outer")
    # LS4 and adjLS4
    .join(LS4, (LS4.ListId == l.ListId) & (LS4.Position == 3) & (LS4.Cancelled == 0), "left_outer")
    )
    
    #drop AppellantId
    # sql_joined_df = sql_joined_df.drop("AppellantId")

    #Get DF of bronze table
    bronze_df = config["spark"].read.table(BRONZE_TABLE_NAME)
    
    #Get Parquet Data and Compare to Sql Data
    results, sql_h,pq_h = config["run_tests"].run_table_tests(TEST_NAME,sql_joined_df,bronze_df,config["spark"] )
    result_text = f"Finished Running Tests on : {TEST_NAME}"
    return results,result_text,sql_h,pq_h



