import requests
import time
payload = {'query': '{"operators": {"0": {"type": "ClusterMetaData"} } }'}
r = requests.post("http://localhost:5000/jsonQuery", data=payload)
print r.text
# while True:
# 	payload = {'query': '{"operators": {"0": {"type": "ClusterMetaData"} } }'}
# 	r = requests.post("http://localhost:6666/delay", data=payload)
# 	print r.text
# 	time.sleep(0.1)
r = requests.post("http://localhost:6666/statistics", data=" ")
print r.text