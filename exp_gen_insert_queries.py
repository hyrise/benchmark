import sys
import random
import string

table = "CUSTOMER"

for i in range(3000000):
	if table == "STOCK":
		data = [i,
				random.randint(1, 10),
				random.randint(1, 100),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				0,
				0,
				0,
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				]

	elif table == "CUSTOMER":
		data = [i,
				random.randint(1, 10),
				random.randint(1, 20),
				''.join(random.choice(string.ascii_lowercase) for _ in range(16)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(2)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(16)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(20)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(2)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(9)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(16)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(26)),
				''.join(random.choice(string.ascii_lowercase) for _ in range(2)),
				random.random(),
				0,
				0,
				0,
				0,
				0,
				''.join(random.choice(string.ascii_lowercase) for _ in range(100)),
				]

	postdata = """query={
    "operators": {
        "0" : {
            "type": "GetTable",
            "name": "%s"
        },
        "1" : {
            "type" : "InsertScan",
            "data" : [
                %s
            ]
        },
        "2" : {
        	"type" : "NoOp"
        },
        "3" : {
        	"type" : "Commit"
        }
    },
    "edges" : [["0", "1"], ["1", "2"], ["2", "3"]]
}&performance="true" """ % (table, str(data).replace("'", "\""))
	postdata = postdata.replace("\n", "")
	postdata += "\r\n\r\n"

	header = """POST /query/ HTTP/1.0
Connection: Keep-Alive
Content-length: %s
Content-type: application/x-www-form-urlencoded
Host: 127.0.0.1:5000
User-Agent: ApacheBench/2.3
Accept: */*""" % len(postdata) + "\n\n"

	header = header.replace("\n", "\r\n")

	sys.stdout.write('{:04d}'.format(len(header))+'\0')
	sys.stdout.write('{:04d}'.format(len(postdata))+'\0')
	sys.stdout.write(header)
	sys.stdout.write(postdata)