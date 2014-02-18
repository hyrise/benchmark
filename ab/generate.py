
number_queries = 100000;
f = open('prepared_data.txt', 'w')

for x in range(number_queries):
  postdata = """query={
  "priority" : 10,
  "performance": false,
  "operators": {
    "0": {
      "type": "NoOp"
    }
  },
  "edges": [["0", "0"]]
}"""
  postlen = len(postdata) + 4
  request = "POST / HTTP/1.0\r\nConnection: Keep-Alive\r\nContent-length: %s\r\nContent-type: application/x-www-form-urlencoded\r\nHost: 127.0.0.1:5000\r\nUser-Agent: ApacheBench/2.3\r\nAccept: */*\r\n\r\n" % postlen
  requestlen = len(request)
  f.write('{:04d}'.format(requestlen)+'\0')
  f.write('{:04d}'.format(postlen)+'\0')
  f.write(request)
  f.write(postdata)
  f.write("\r\n\r\n")