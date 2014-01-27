BASE_POPULATION = 10
#BASE_POPULATION = 100
SCALEFACTORS = [1,2,5,10,20,50]

# Table Names
TABLENAME_SUBSCRIBER        = "SUBSCRIBER"
TABLENAME_ACCESS_INFO       = "ACCESS_INFO"
TABLENAME_SPECIAL_FACILITY  = "SPECIAL_FACILITY"
TABLENAME_CALL_FORWARDING   = "CALL_FORWARDING"

ALL_TABLES = [
    TABLENAME_SUBSCRIBER,
    TABLENAME_ACCESS_INFO,
    TABLENAME_SPECIAL_FACILITY,
    TABLENAME_CALL_FORWARDING
]

HEADERS = {
TABLENAME_ACCESS_INFO:"""S_ID|AI_TYPE|DATA1|DATA2|DATA3|DATA4
INTEGER|INTEGER|INTEGER|INTEGER|STRING|STRING
0_R|0_R|0_R|0_R|0_R|0_R
===""",
TABLENAME_CALL_FORWARDING:"""S_ID|SF_TYPE|START_TIME|END_TIME|NUMBERX
INTEGER|INTEGER|INTEGER|INTEGER|STRING
0_R|0_R|0_R|0_R|0_R
===""",
TABLENAME_SPECIAL_FACILITY:"""S_ID|SF_TYPE|IS_ACTIVE|ERROR_CNTRL|DATA_A|DATA_B
INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|STRING
0_R|0_R|0_R|0_R|0_R|0_R
===""",
TABLENAME_SUBSCRIBER:"""S_ID|SUB_NBR|BIT_1|BIT_2|BIT_3|BIT_4|BIT_5|BIT_6|BIT_7|BIT_8|BIT_9|BIT_10|HEX_1|HEX_2|HEX_3|HEX_4|HEX_5|HEX_6|HEX_7|HEX_8|HEX_9|HEX_10|BYTE2_1|BYTE2_2|BYTE2_3|BYTE2_4|BYTE2_5|BYTE2_6|BYTE2_7|BYTE2_8|BYTE2_9|BYTE2_10|MSC_LOCATION|VLR_LOCATION
INTEGER|STRING|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER
0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R
==="""
}

QUERY_FILES = {
    "GET_SUBSCRIBER_DATA": {
        'GetSubscriberData':'GetSubscriberData-GetSubscriberData.json'},
    "GET_NEW_DESTINATION": {
        'GetNewDestination':'GetNewDestination-GetNewDestination.json'},
    "GET_ACCESS_DATA": {
        'GetAccessData':'GetAccessData-GetAccessData.json'},
    "UPDATE_SUBSCRIBER_DATA": {
        'UpdateSubscriberData':'UpdateSubscriberData-UpdateSubscriberData.json'},
    "UPDATE_LOCATION" : {
        'UpdateLocation':'UpdateLocation-UpdateLocation.json'},
    "INSERT_CALL_FORWARDING": {
        'GetSubscriberId':'InsertCallForwarding-GetSubscriberId.json',
        'GetFacilityType':'InsertCallForwarding-GetFacilityType.json',
        'InsertCallForwarding':'InsertCallForwarding-InsertCallForwarding.json',
        'CheckPrimaryKeys':'InsertCallForwarding-CheckPrimaryKeys.json'
        ,
        'CheckForeignKeys':'InsertCallForwarding-CheckForeignKeys.json'
        },
    "DELETE_CALL_FORWARDING": {
        'GetSubscriberId':'DeleteCallForwarding-GetSubscriberId.json',
        'DeleteCallForwarding':'DeleteCallForwarding-DeleteCallForwarding.json'}
}

LOAD_FILE = 'Load-Load.json'
#LOAD_FILE = 'Load-LoadWithIndizes.json'

# Transaction Types
def enum(*sequential, **named):
    enums = dict(map(lambda x: (x, x), sequential))
    # dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)
TransactionTypes = enum(
    "GET_SUBSCRIBER_DATA",
    "GET_NEW_DESTINATION",
    "GET_ACCESS_DATA",
    "UPDATE_SUBSCRIBER_DATA",
    "UPDATE_LOCATION",
    "INSERT_CALL_FORWARDING",
    "DELETE_CALL_FORWARDING"
)
