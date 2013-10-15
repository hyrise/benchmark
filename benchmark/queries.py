QUERY_FILES = {
    "distinct_kunnr_adrc"      : "queries/distinct_kunnr_adrc.json",
    "distinct_kunnr_kna1"      : "queries/distinct_kunnr_kna1.json",
    "distinct_matnr_mara"      : "queries/vbap_mara_group.json",
    "distinct_vbeln_vbak"      : "queries/distinct_vbeln_vbak.json",
    "distinct_kunnr_knvp"      : "queries/distinct_kunnr_knvp.json",
    "distinct_kunnr_knvp_mini" : "queries/distinct_kunnr_knvp_mini.json",
    "q2"                       : "queries/q2.json",
    "q3"                       : "queries/q3.json",
    "q5"                       : "queries/q5.json",
    "q6a"                      : "queries/q6a.json",
    "q6b"                      : "queries/q6b.json",
    "q7"                       : "queries/q7.json",
    "q8"                       : "queries/q8.json",
    "q7idx"                    : "queries/q7idx.json",
    "q8idx"                    : "queries/q8idx.json",
    "q13insert"                : "queries/q13insert.json",
    "q10"                      : "queries/q10.json",
    "q11"                      : "queries/q11.json",
    "q12"                      : "queries/q12.json"
}

QUERIES_PREPARE = [
    "distinct_kunnr_adrc",
    "distinct_kunnr_kna1",
    "distinct_matnr_mara",
    "distinct_vbeln_vbak",
    "distinct_kunnr_knvp",
    "distinct_kunnr_knvp_mini"
]

QUERIES_OLAP = [
    "q10",
    "q11",
    "q12"
]

QUERIES_OLTP = [
    "q2",
    "q3",
    "q5",
    "q6a",
    "q6b",
    "q7",
    "q8",
    "q7idx",
    "q8idx",
    "q13insert"
]

QUERIES_ALL = QUERIES_OLAP + QUERIES_OLTP
