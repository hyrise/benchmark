PREPARE_QUERIES = {
    "distinct_kunnr_adrc" : "queries/distinct_kunnr_adrc.json",
    "distinct_kunnr_kna1" : "queries/distinct_kunnr_kna1.json",
    "distinct_matnr_mara" : "queries/vbap_mara_group.json",
    "distinct_vbeln_vbak" : "queries/distinct_vbeln_vbak.json",
#    "indices": "queries/create_indices.json"
}

# locations for json query files sorted by OLTP (TPCC) and OLAP (TPC-H).
OLAP_QUERY_FILES = { 
    "q10"                    : "queries/q10.json",
    "q11"                    : "queries/q11.json",
    "q12"                    : "queries/q12.json",
    }

OLTP_QUERY_FILES ={
    "q2" : "queries/q2.json",
    "q3" : "queries/q3.json",
    "q5" : "queries/q5.json",
    "q6a" : "queries/q6a.json",
    "q6b" : "queries/q6b.json",
    "q7" : "queries/q7.json",
    "q8" : "queries/q8.json",
    "q7idx" : "queries/q7idx.json",
    "q8idx" : "queries/q8idx.json",
    #"q9" : "queries/q9.json",
}


# relative weights for queries in format (id, weight)
OLAP_WEIGHTS = (
    ("q10", 1),
    ("q11", 1),
    ("q12", 1),
    )

OLTP_WEIGHTS = (
    ("q2", 1),
    ("q3", 1),
    ("q5", 1),
    ("q6a", 1),
    ("q6b", 1),
    ("q7", 1),
    ("q8", 1),
    ("q7idx", 1),
    ("q8idx", 1),
    #("q9", 1),
    )

# All queries
ALL_QUERIES = OLTP_WEIGHTS + OLAP_WEIGHTS
