PREPARE_QUERIES_USER = {
    #"distinct_kunnr_adrc" : "queries/mixed/distinct_kunnr_adrc.json",                                                                                                                                              
    #"distinct_kunnr_kna1" : "queries/mixed/distinct_kunnr_kna1.json",                                                                                                                                              
    #"distinct_matnr_mara" : "queries/mixed/vbap_mara_group.json",                                                                                                                                                  
    #"distinct_vbeln_vbak" : "queries/mixed/distinct_vbeln_vbak.json",                                                                                                                                              
    #"indices": "queries/mixed/create_indices.json" ,                                                                                                                                                 
}

PREPARE_QUERIES_SERVER = {
    #"distinct_kunnr_adrc" : "queries/mixed/distinct_kunnr_adrc.json",                                                                                                                                              
    #"distinct_kunnr_kna1" : "queries/mixed/distinct_kunnr_kna1.json",                                                                                                                                              
    #"distinct_matnr_mara" : "queries/mixed/vbap_mara_group.json",                                                                                                                                                  
    #"distinct_vbeln_vbak" : "queries/mixed/distinct_vbeln_vbak.json",                                                                                                                                              
    #"indices": "queries/mixed/create_indices.json" ,
    "preload_vbap" : "queries/mixed/preload_vbap.json",
    "create_vbak_index" : "queries/mixed/create_vbak_index.json"                                                                                                                                                      
}

# locations for json query files sorted by OLTP (TPCC) and OLAP (TPC-H).                                                                                                                                      
OLAP_QUERY_FILES = {
    "q10"                    : "queries/mixed/q10.json",
    "q11"                    : "queries/mixed/q11.json",
    "q12"                    : "queries/mixed/q12.json",
    "xselling"               : "queries/mixed/xselling.json",
    "xselling100"               : "queries/mixed/xselling100.json",
    "xselling500"               : "queries/mixed/xselling500.json",
    "xselling2000"               : "queries/mixed/xselling2000.json",
    "q6_ch"                  : "queries/mixed/q6_ch.json",
    "busy"                  : "queries/mixed/busy.json",

    }

OLTP_QUERY_FILES ={
    "q2" : "queries/mixed/q2.json",
    "q3" : "queries/mixed/q3.json",
    "q5" : "queries/mixed/q5.json",
    "q6a" : "queries/mixed/q6a.json",
    "q6b" : "queries/mixed/q6b.json",
    "q7" : "queries/mixed/q7.json",
    "q8" : "queries/mixed/q8.json",
    "q7idx_vbak" : "queries/mixed/q7idx_vbak.json",
    "q8idx" : "queries/mixed/q8idx.json",
    "q3_c"  : "queries/mixed/q3_c.json"
    #"q9" : "queries/mixed/q9.json",                                                                                                                                                                                
}


# relative weights for queries in format (id, weight)                                                                                                                                                         
OLAP_WEIGHTS = (
    ("q10", 1),
    ("q11", 1),
    ("q12", 1),
    ("xselling", 1),
    ("xselling100", 1),
    ("xselling500", 1),
    ("xselling2000", 1),
    ("q6_ch", 1),
    ("busy", 1),
    )

OLTP_WEIGHTS = (
    ("q2", 1),
    ("q3", 1),
    ("q5", 1),
    ("q6a", 1),
    ("q6b", 1),
    ("q7", 1),                                                                                                                                                                                                
    ("q8", 1),                                                                                                                                                                                                
    ("q7idx_vbak", 1),                                                                                                                                                                                        
    ("q8idx", 1),                                                                                                                                                                                             
    ("q3_c", 1),                                                                                                                                                                                              
    #("q9", 1),                                                                                                                                                                                               
    )                                                                                                                                                                                                         
                                                                                                                                                                                                              
# All queries                                                                                                                                                                                                 
ALL_QUERIES = OLTP_WEIGHTS + OLAP_WEIGHTS                                                                                                                                                                     


