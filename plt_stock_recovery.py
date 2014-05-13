import matplotlib as mpl
mpl.use('Agg')
from pylab import *
import os

def collectRecoveryTimes(groupId):
    resultDir = os.path.join(os.getcwd(), "results", groupId)
    runs = os.listdir(resultDir)
    builds = os.listdir(os.path.join(resultDir, runs[0]))
    results = {}
    for build in builds:
        results[build] = {
            0:  {"tableSizes": [], "recoveryTimes": []},
            10: {"tableSizes": [], "recoveryTimes": []},
            20: {"tableSizes": [], "recoveryTimes": []},
            50: {"tableSizes": [], "recoveryTimes": []}
        }
    for run in runs:
        tmp = run.split("mio_delta")
        tableSize = int(tmp[0])
        deltaPercentage = int(tmp[1])
        for build in builds:
            resultFile = os.path.join(resultDir, run, build, "recoverytime.txt")
            result = open(resultFile).readline().split(";")
            recoveryTime = float(result[0])
            results[build][deltaPercentage]["tableSizes"].append(tableSize)
            results[build][deltaPercentage]["recoveryTimes"].append(recoveryTime / 1000.0 / 1000.0)
    return results

if __name__ == "__main__":
    groupId = "STOCK_recovery"
    results = collectRecoveryTimes(groupId)
    fig = plt.figure()
    plt.title("TPC-C Recovery")
    plt.ylabel("Recovery Time in s")
    plt.xlabel("STOCK Table Size in m")
    plt.ylim(-5, 160)

    lineColors = ["r", "g", "b"]
    lineStyles = ["-", "--", "-.", ":"]
    c = 0
    for build in results:
        s = 0
        for deltaPercentage in [0, 10, 20, 50]:
            plotX = []
            plotY = []
            plotX, plotY = (list(t) for t in zip(*sorted(zip(results[build][deltaPercentage]["tableSizes"], results[build][deltaPercentage]["recoveryTimes"]))))
            plt.plot(plotX, plotY, color=lineColors[c], linestyle=lineStyles[s], label="%s (%i%% in Delta)" % (build, deltaPercentage))
            s += 1
        c += 1
    plt.legend(loc='upper left', prop={'size':8})
    plt.savefig("recovery.pdf")
    plt.close()
