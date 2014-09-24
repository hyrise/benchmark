import sys
import random
import string


def write_request_to_file(_queryfile, query):
  postdata = "query="+query
  postlen = len(postdata) + 4
  request = "POST /procedureRevenueInsert/ HTTP/1.0\r\nConnection: Keep-Alive\r\nContent-Length: %s\r\nContent-Type: application/x-www-form-urlencoded\r\nHost: 127.0.0.1:5000\r\nUser-Agent: ApacheBench/2.3\r\nAccept: */*\r\n\r\n" % (postlen)
  requestlen = len(request)
  _queryfile.write('{:04d}'.format(requestlen)+'\0')
  _queryfile.write('{:04d}'.format(postlen)+'\0')
  _queryfile.write(request)
  _queryfile.write(postdata)
  _queryfile.write("\r\n\r\n")

filename = "/home/David.Schwalb/tpcc/queries_gen/revenue_insert_storedproc_5M.txt"
_queryfile = open(filename, 'w+')

count = 1000000
for i in range(count):
  if i % (count/100) == 0:
    print i/(count/100), "percent"

  # query = """{"operators": {
  #       "retrieve_revenue": {
  #           "type" : "GetTable",
  #           "name" : "revenue"
  #       },
  #       "insert" : {
  #           "type" : "InsertScan",
  #           "data" : [
  #               [2013,1,2000],
  #               [2013,2,2500],
  #               [2013,3,3000],
  #               [2013,4,4000]
  #           ]
  #       },
  #       "commit" : {
  #           "type" : "Commit"
  #       },
  #       "noop" : {
  #           "type" : "NoOp"
  #       }
  #   },
  #   "edges" : [
  #       ["retrieve_revenue", "insert"],
  #       ["insert", "commit"],
  #       ["commit", "noop"]
  #   ]
  # }"""
  query = "x=1"
  write_request_to_file(_queryfile, query)

  # postdata = postdata.replace("\n", "")
  # postdata += "\r\n\r\n"

#   header = """POST /query/ HTTP/1.0
# Connection: Keep-Alive
# Content-length: %s
# Content-type: application/x-www-form-urlencoded
# Host: 127.0.0.1:5000
# User-Agent: ApacheBench/2.3
# Accept: */*""" % len(postdata) + "\n\n"

#   header = header.replace("\n", "\r\n")

#   sys.stdout.write('{:04d}'.format(len(header))+'\0')
#   sys.stdout.write('{:04d}'.format(len(postdata))+'\0')
#   sys.stdout.write(header)
#   sys.stdout.write(postdata)
