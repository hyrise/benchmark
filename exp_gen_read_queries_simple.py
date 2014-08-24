import sys
import random
import string


def write_request_to_file(_queryfile, query):
	postdata = "query="+query
	postlen = len(postdata) + 4
	request = "POST /jsonQuery HTTP/1.0\r\nConnection: Keep-Alive\r\nContent-Length: %s\r\nContent-Type: application/x-www-form-urlencoded\r\nHost: 127.0.0.1:5000\r\nUser-Agent: ApacheBench/2.3\r\nAccept: */*\r\n\r\n" % (postlen)
	requestlen = len(request)
	_queryfile.write('{:04d}'.format(requestlen)+'\0')
	_queryfile.write('{:04d}'.format(postlen)+'\0')
	_queryfile.write(request)
	_queryfile.write(postdata)
	_queryfile.write("\r\n\r\n")

filename = "/home/David.Schwalb/tpcc/queries_gen/revenue_reads_1M.txt"
_queryfile = open(filename, 'w+')

count = 1000000
for i in range(count):
	if i % (count/100) == 0:
		print i/(count/100), "percent"

	query = """{"operators": {
        "retrieve_revenue": {
            "type" : "GetTable",
            "name" : "revenue"
        },
	"1": {
            "type": "HashBuild",
            "fields": ["year"],
	     "key": "groupby"
        },
        "2": {
            "type": "GroupByScan",
            "fields": ["year"],
            "functions": [
		{"type": "COUNT", "field": "year", "distinct": false, "as": "count"}
             ]
        }
     },
    "edges" : [
        ["retrieve_revenue", "1"], ["retrieve_revenue", "2"], ["1", "2"]
    ]
}"""
	write_request_to_file(_queryfile, query)

	# postdata = postdata.replace("\n", "")
	# postdata += "\r\n\r\n"

# 	header = """POST /query/ HTTP/1.0
# Connection: Keep-Alive
# Content-length: %s
# Content-type: application/x-www-form-urlencoded
# Host: 127.0.0.1:5000
# User-Agent: ApacheBench/2.3
# Accept: */*""" % len(postdata) + "\n\n"

# 	header = header.replace("\n", "\r\n")

# 	sys.stdout.write('{:04d}'.format(len(header))+'\0')
# 	sys.stdout.write('{:04d}'.format(len(postdata))+'\0')
# 	sys.stdout.write(header)
# 	sys.stdout.write(postdata)
