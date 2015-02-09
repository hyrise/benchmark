import requests
import time
import re


NUMBER_OF_MAX_REPLICAS = 3
differences = [[]] * NUMBER_OF_MAX_REPLICAS
shouldLeave = False

nOfNodes = 0

while True:
	payload = {'query': '{"operators": {"0": {"type": "ClusterMetaData"} } }'}
	r = requests.post("http://localhost:6666/delay", data=payload)
	if not "rows" in r.text:
		continue
	allNodes = r.text.split("rows")[1]
	allNodes = allNodes.split("],")[:-1]

	currentMasterCID = int(re.sub("\D", "", allNodes[-1].split(",")[2]))
	# print "CommitID of Master: %i" % (currentMasterCID)

	nodeNumber = 1
	for node in allNodes[:-1]:
		currentReplicaCID = int(node.split(",")[2])
		# print "CommitID of Replica %s: %i" % (nodeNumber, currentReplicaCID)

		differences[nodeNumber - 1].append(currentMasterCID - currentReplicaCID)

		nodeNumber += 1

	nOfNodes = nodeNumber - 1

	# if there are enough measurements check if test case is over
	if len(differences[nodeNumber - 2]) > 50:
		shouldLeave = True
		while nodeNumber <> 0:
			if differences[nodeNumber - 2][-1] <> 0:
				shouldLeave = False
				break
			nodeNumber -= 1

	if shouldLeave:
		print "CommitID of Master: %i" % (currentMasterCID)
		break

	time.sleep(0.1)

print "########### Analysis ###########"

for i in range(nOfNodes):
	avg = sum(differences[i]) / len(differences[i])
	print "Replica %i: Average: %i Maximum: %i" % (i, avg, max(differences[i]))
