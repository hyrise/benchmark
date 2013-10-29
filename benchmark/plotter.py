import os
import matplotlib as mpl
mpl.use('Agg')
from pylab import *

class Plotter:

    def __init__(self, benchmarkGroupId):
        self._groupId = benchmarkGroupId
        self._dirOutput = os.path.join(os.getcwd(), "plots", str(self._groupId))
        self._runs = self._collect()
        self._buildIds = self._runs[self._runs.keys()[0]].keys()

        if not os.path.isdir(self._dirOutput):
            os.makedirs(self._dirOutput)

    def printStatistics(self):
        for runId, runData in self._runs.iteritems():
            numUsers = len(runData[runData.keys()[0]])
            print "Run ID: %s [%s users]" % (runId, numUsers)
            print "=============================="
            for buildId, buildData in runData.iteritems():
                print "|\n+-- Build ID: %s" % buildId
                print "|"
                print "|     Transaction       #      min     max     avg     median"
                print "|     --------------------------------------------------------"
                for txId, txData in self._aggregateUsers(runId, buildId).iteritems():
                    print "|     {:16s}  {:5s}  {:1.4f}  {:1.4f}  {:1.4f}  {:1.4f}".format(txId, str(txData["total"]), txData["min"], txData["max"], txData["average"], txData["median"])
                print "|\n"

    def plotTotalThroughput(self):
        plt.title("Total Transaction Throughput")
        plt.ylabel("Number of Transactions")
        plt.xlabel("Number of Parallel Users")
        for buildId in self._buildIds:
            plotX = []
            plotY = []
            for runId, runData in self._runs.iteritems():
                numUsers = len(runData[runData.keys()[0]])
                aggData = self._aggregateUsers(runId, buildId)
                plotX.append(numUsers)
                total = 0
                for txId, txData in aggData.iteritems():
                    total += txData["total"]
                plotY.append(total)
            plotX, plotY = (list(t) for t in zip(*sorted(zip(plotX, plotY))))
            plt.plot(plotX, plotY, label=buildId)
        plt.xticks(arange(1, plotX[-1]+1))
        plt.legend(loc='upper left', prop={'size':10})
        fname = os.path.join(self._dirOutput, "total_throughput.pdf")
        plt.savefig(fname)
        plt.close()

    def plotResponseTimesVaryingUsers(self):
        plt.figure(1, figsize=(10, 20))
        curPlt = 0
        for buildId in self._buildIds:
            curPlt += 1
            plt.subplot(len(self._buildIds), 1, curPlt)
            plt.tight_layout()
            plt.yscale('log')
            plt.ylabel("Response Time in s")
            pltData = []
            xtickNames = []
            for runId, runData in self._runs.iteritems():
                numUsers = len(runData[runData.keys()[0]])
                aggData  = self._aggregateUsers(runId, buildId)
                for txId, txData in aggData.iteritems():
                    pltData.append(txData["raw"])
                    xtickNames.append("%s, %s users" % (txId, numUsers))
            bp = plt.boxplot(pltData, notch=0, sym='+', vert=1, whis=1.5)
            plt.title("Transaction response times for varying users in build '%s" % buildId)
            plt.setp(bp['boxes'], color='black')
            plt.setp(bp['whiskers'], color='black')
            plt.setp(bp['fliers'], color='red', marker='+')
            plt.xticks(arange(len(xtickNames)), xtickNames, rotation=45, fontsize=5)
        fname = os.path.join(self._dirOutput, "varying_users.pdf")
        plt.savefig(fname)
        plt.close()

    def plotResponseTimeFrequencies(self):
        for buildId in self._buildIds:
            for runId, runData in self._runs.iteritems():
                aggData = self._aggregateUsers(runId, buildId)
                maxPlt = len(aggData.keys())
                curPlt = 0
                plt.figure(1, figsize=(10, 4*maxPlt))
                for txId, txData in aggData.iteritems():
                    curPlt += 1
                    plt.subplot(maxPlt, 1, curPlt)
                    plt.tight_layout()
                    plt.title("RT Frequency in %s (build '%s', run '%s')" % (txId, buildId, runId))
                    plt.xlabel("Response Time in s")
                    plt.ylabel("Number of Transactions")
                    plt.xlim(txData["min"], txData["max"])
                    plt.xticks([txData["min"], txData["average"], percentile(txData["raw"], 90), txData["max"]],
                               ["" % txData["min"], "avg\n(%s)" % txData["average"], "90th percentile\n(%s)" % percentile(txData["raw"], 90), "max\n(%s)" % txData["max"]],
                               rotation=45, fontsize=5)
                    plt.grid(axis='x')
                    y, binEdges = np.histogram(txData["raw"], bins=10)
                    binCenters = 0.5*(binEdges[1:]+binEdges[:-1])
                    plt.plot(binCenters, y, '-')
                fname = os.path.join(self._dirOutput, "rt_freq_%s_%s.pdf" % (buildId, runId))
                plt.savefig(fname)
                plt.close()

    def _collect(self):
        """
        Returns a collection dictionary with information about a benchmark group.
        structure:
        {
            <Run ID>: {
                <Build ID>: [
                    {<Transaction1>: [<runtime 1>, <runtime 2>, ...], ...}, // User 1
                    {<Transaction2>: [<runtime 1>, <runtime 2>, ...], ...}, // User 2
                    ...
                ]
            }
        }
        """
        runs = {}
        dirResults = os.path.join(os.getcwd(), "results", self._groupId)
        if not os.path.isdir(dirResults):
            raise Exception("Group result directory '%s' not found!" % dirResults)
        for run in os.listdir(dirResults):
            dirRun = os.path.join(dirResults, run)
            if os.path.isdir(dirRun):
                runs[run] = {}
                for build in os.listdir(dirRun):
                    dirBuild = os.path.join(dirRun, build)
                    if os.path.isdir(dirBuild):
                        runs[run][build] = []
                        for user in os.listdir(dirBuild):
                            dirUser = os.path.join(dirBuild, user)
                            if os.path.isdir(dirUser):
                                userStats = {}
                                if not os.path.isfile(os.path.join(dirUser, "transactions.log")):
                                    print "WARNING: no transaction log found for user %s in run %s!" % (user, run)
                                    continue
                                for rawline in open(os.path.join(dirUser, "transactions.log")):
                                    linedata = rawline.split(",")
                                    if len(linedata) < 2:
                                        continue
                                    transaction = linedata[0]
                                    runtime = linedata[1]
                                    userStats.setdefault(transaction,[]).append(float(runtime))
                                runs[run][build].append(userStats)
        return runs

    def _aggregateUsers(self, runId, buildId):
        buildData = self._runs[runId][buildId]
        numUsers = len(buildData)
        stats = {}
        for userStats in self._runs[runId][buildId]:
            for txId, txData in userStats.iteritems():
                stats.setdefault(txId, {})
                stats[txId].setdefault("raw", [])
                stats[txId].setdefault("total", 0)
                stats[txId].setdefault("min", 0.0)
                stats[txId].setdefault("max", 0.0)
                stats[txId].setdefault("average", 0.0)
                stats[txId].setdefault("median", 0.0)
                stats[txId].setdefault("stddev", 0.0)
                stats[txId]["raw"] += txData
                stats[txId]["total"] += len(txData)
                stats[txId]["min"] += amin(txData) / numUsers
                stats[txId]["max"] += amax(txData) / numUsers
                stats[txId]["average"] += average(txData) / numUsers
                stats[txId]["median"] += median(txData) / numUsers
                stats[txId]["stddev"] += std(txData) / numUsers
        return stats
