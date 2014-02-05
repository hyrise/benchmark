import os, sys
import shutil
import matplotlib as mpl
mpl.use('Agg')
from pylab import *
from scipy.stats import gaussian_kde
from matplotlib.ticker import FixedFormatter
import numpy as np

# const factor to convert result times from logs
# if papi is deactivated, logs contain time in seconds
# we want ms, so factor ist 1000
z = 1000
DEFAULT_FORMAT='bs'

class Plotter:

    def __init__(self, benchmarkGroupId):
        self._groupId = benchmarkGroupId
        self._dirOutput = os.path.join(os.getcwd(), "plots", str(self._groupId))
        self._varyingParameter = None
        self._runs = self._collect()
        self._buildIds = self._runs[self._runs.keys()[0]].keys()

        if not os.path.isdir(self._dirOutput):
            os.makedirs(self._dirOutput)
        print "Initialized TATP Plotter."

    def tick(self):
        sys.stdout.write('.')
        sys.stdout.flush()

    def printStatistics(self):
        for runId, runData in self._runs.iteritems():
            numUsers = runData[runData.keys()[0]]["numUsers"]
            print "Run ID: %s [%s users]" % (runId, numUsers)
            print "=============================="
            for buildId, buildData in runData.iteritems():
                if buildData == {'numUsers': 0, 'txStats': {}}:
                    continue
                print "|\n+-- Build ID: %s" % buildId
                print "|"
                print "|     Transaction       tps      min(ms)    max(ms)   avg(ms)    median(ms)"
                totalRuns = 0.0
                totalTime = 0.0
                for txId, txData in buildData["txStats"].iteritems():
                    totalRuns += txData["totalRuns"]*z
                    totalTime += txData["userTime"]
                for txId, txData in buildData["txStats"].iteritems():
                    print "|     -------------------------------------------------------------------------------------------"
                    print "|     TX: {:14s} tps: {:05.2f}, min: {:05.2f}, max: {:05.2f}, avg: {:05.2f}, med: {:05.2f} (all in ms)".format(txId, float(txData["totalRuns"])*z / totalTime, txData["rtMin"], txData["rtMax"], txData["rtAvg"], txData["rtMed"])
                    print "|                        succeeded: {:d}, failed: {:d}, ratio: {:1.3f}".format(txData["totalRuns"], txData["totalFail"], float(txData["totalFail"]) / float(txData["totalRuns"] + txData["totalFail"]))
                    print "|     -------------------------------------------------------------------------------------------"
                    if txData["operators"] and len(txData["operators"].keys()) > 0:
                        print "|       Operator                   #perTX     min(ms)    max(ms)   avg(ms)    median(ms)"
                        print "|       ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
                        for opName, opData in txData["operators"].iteritems():
                            print "|       {:25s}  {:05.2f}      {:05.2f}      {:05.2f}      {:05.2f}      {:05.2f}".format(opName, opData["avgRuns"], opData["rtMin"], opData["rtMax"], opData["rtAvg"], opData["rtMed"])
                print "|     -------------------------------------------------------------------------------------------"
                print "|     total:            %1.2f tps\n" % (totalRuns / totalTime)
                print "totalRuns: %1.2f, totalTime: %1.2f" % (totalRuns, totalTime)

    def plotTotalThroughput(self):
        sys.stdout.write('plotTotalThroughput: ')
        plt.title("Total Transaction Throughput")
        plt.ylabel("Transactions per Second")
        plt.xlabel("Number of Parallel Users")
        #plt.xlabel("Number of Threads")
        for buildId in self._buildIds:
            plotX = []
            plotY = []
            for runId, runData in self._runs.iteritems():
                self.tick()
                plotX.append(runData[buildId]["numUsers"])
                totalRuns = 0.0
                totalTime = 0.0
                for txId, txData in runData[buildId]["txStats"].iteritems():
                    totalRuns += txData["totalRuns"]
                    totalTime += txData["userTime"]
                plotY.append(totalRuns * z / totalTime)
            plotX, plotY = (list(t) for t in zip(*sorted(zip(plotX, plotY))))
            plt.plot(plotX, plotY, DEFAULT_FORMAT, label=buildId)
        plt.xticks(plotX)
        plt.legend(loc='upper left', prop={'size':10})
        fname = os.path.join(self._dirOutput, "total_throughput.pdf")
        plt.show()
        plt.savefig(fname)
        plt.close()
        sys.stdout.write('\n')

    def plotTotalFails(self):
        sys.stdout.write('plotTotalFails: ')
        plt.title("Total Failed Transactions")
        plt.ylabel("Failed Transactions per Second")
        plt.xlabel("Number of Parallel Users")
        for buildId in self._buildIds:
            plotX = []
            plotY = []
            for runId, runData in self._runs.iteritems():
                self.tick()
                plotX.append(runData[buildId]["numUsers"])
                totalFails = 0.0
                totalTime = 0.0
                for txId, txData in runData[buildId]["txStats"].iteritems():
                    totalFails += txData["totalFail"]
                    totalTime += txData["userTime"]
                plotY.append(totalFails * z / totalTime)
            plotX, plotY = (list(t) for t in zip(*sorted(zip(plotX, plotY))))
            plt.plot(plotX, plotY, DEFAULT_FORMAT, label=buildId)
        plt.xticks(plotX)
        plt.legend(loc='upper left', prop={'size':10})
        fname = os.path.join(self._dirOutput, "total_fails.pdf")
        plt.savefig(fname)
        plt.close()
        sys.stdout.write('\n')

    def plotTransactionResponseTimes(self):
        sys.stdout.write('plotTransactionResponseTimes: ')
        #for txId in ["NEW_ORDER"]: #,"PAYMENT","STOCK_LEVEL","DELIVERY","ORDER_STATUS"]:
        for txId in ["GET_SUBSCRIBER_DATA","GET_NEW_DESTINATION","GET_ACCESS_DATA","UPDATE_SUBSCRIBER_DATA","UPDATE_LOCATION","INSERT_CALL_FORWARDING","DELETE_CALL_FORWARDING"]:
        #for txId in ["INSERT_CALL_FORWARDING"]:
            plt.title("%s Response Times" % txId)
            plt.ylabel("Avg. Response Time in ms", labelpad=8)
            plt.xlabel("Number of Parallel Users", labelpad=8)
            for buildId in self._buildIds:
                plotX = []
                plotY = []
                for runId, runData in self._runs.iteritems():
                    self.tick()
                    if runData[buildId]["txStats"].has_key(txId):
                        plotX.append(runData[buildId]["numUsers"])
                        plotY.append(runData[buildId]["txStats"][txId]["rtAvg"])
                plotX, plotY = (list(t) for t in zip(*sorted(zip(plotX, plotY))))
                plt.plot(plotX, plotY, DEFAULT_FORMAT, label=buildId)
            plt.legend(loc='upper left', prop={'size':10})
            fname = os.path.join(self._dirOutput, "%s_avg_rt.pdf" % txId)
            plt.savefig(fname)
            plt.close()
        sys.stdout.write('\n')

    def plotResponseTimesVaryingUsers(self):
        sys.stdout.write('plotResponseTimesVaryingUsers: ')
        plt.figure(1, figsize=(5, 10))
        curPlt = 0
        for buildId in self._buildIds:
            curPlt += 1
            plt.subplot(len(self._buildIds), 1, curPlt)
            plt.tight_layout()
            plt.yscale('log')
            plt.ylabel("Response Time in ms")
            pltData = []
            xtickNames = []
            sorted_items = sorted(self._runs.iteritems(), key=lambda tup: tup[1][buildId]["numUsers"])
            for runId, runData in sorted_items:
                numUsers = runData[buildId]["numUsers"]
                for txId, txData in runData[buildId]["txStats"].iteritems():
                    self.tick()
                    pltData.append(txData["rtRaw"])
                    xtickNames.append("%s" % (txId))
            bp = plt.boxplot(pltData, notch=0, sym='+', vert=1, whis=1.5)
            #plt.title("Transaction response times for varying users in build '%s" % buildId)
            plt.setp(bp['boxes'], color='black')
            plt.setp(bp['whiskers'], color='black')
            plt.setp(bp['fliers'], color='red', marker='+')
            plt.xticks(arange(len(xtickNames)+1), xtickNames, rotation=90, fontsize=10)
            gcf().subplots_adjust(bottom=0.25)
        fname = os.path.join(self._dirOutput, "varying_users.png")
        plt.savefig(fname)
        plt.close()
        sys.stdout.write('\n')

    def plotResponseTimeFrequencies(self):

        sys.stdout.write('plotResponseTimeFrequencies: ')
        number_of_bins = 150


        for buildId in self._buildIds:
            overall_x_min = 0
            overall_x_max = 0
            overall_y_max = 0
            overall_y_min = 0

            x_max_items = []
            y_max_items = []

            for runId, runData in self._runs.iteritems():
                for txId, txData in runData[buildId]["txStats"].iteritems():
                    hist, bins = np.histogram(txData["rtRaw"], bins=number_of_bins)
                    x_max_items.append(percentile(txData["rtRaw"], 95))
                    y_max_items.append(max(hist))

            overall_x_max = max(x_max_items)
            overall_y_max = max(y_max_items)

            for runId, runData in self._runs.iteritems():
                maxPlt = len(runData[buildId]["txStats"].keys())
                curPlt = 0
                fig = plt.figure(1, None)
                fig.subplots_adjust(bottom=0.1)
                plt.xlabel("Response Time in ms", labelpad=30)
                plt.ylabel("Number of Transactions", labelpad=20)
                plt.axes().xaxis.set_ticks([])
                plt.axes().yaxis.set_ticks([])
                for txId, txData in runData[buildId]["txStats"].iteritems():
                    self.tick()
                    curPlt += 1
                    ax = fig.add_subplot(maxPlt, 1, curPlt)
                    #plt.title("%s" % txId, fontsize=7)
                    plt.text(overall_x_max, overall_y_max,txId, horizontalalignment='right',verticalalignment='top', fontsize=7)
                    p90 = percentile(txData["rtRaw"], 90)
                    x2ticks = [txData["rtMin"], txData["rtAvg"], txData["rtMax"]]
                    x2labels = ["min", "avg", "90th percentile", "max"]

                    hist, bins = np.histogram(txData["rtRaw"], bins=np.arange(overall_x_min, overall_x_max+1, overall_x_max/number_of_bins))
                    width = 0.7 * (bins[1] - bins[0])
                    center = (bins[:-1] + bins[1:]) / 2
                    plt.bar(center, hist, align='center', width=width, color='#ffffff')


                    xs = linspace(txData["rtMin"]*0.8, txData["rtMax"]*1.05, 500)
                    ax.set_xticks(x2ticks, minor=True)

                    ax.get_xaxis().grid(True, which="minor")
                    ax.get_xaxis().set_major_formatter(FormatStrFormatter("%.2f"))
                    ax.get_xaxis().set_minor_formatter(FixedFormatter(["min\n%.1f"%txData["rtMin"], "avg\n%.1f"%txData["rtAvg"], "90th\n%.1f"%p90, "max\n%.1f"%txData["rtMax"]]))
                    ax.get_xaxis().set_tick_params(which='minor', pad=18)

                    for tick in ax.get_xaxis().get_minor_ticks():
                      tick.label.set_fontsize(7)

                    plt.xlim(overall_x_min, overall_x_max*1.05)
                    plt.ylim(overall_y_min, overall_y_max*1.05)
                    plt.setp(ax.get_yticklabels(), fontsize=7)
                    plt.setp(ax.get_xticklabels(), fontsize=7)
                    # density = gaussian_kde(txData["rtRaw"])
                    # density_scale_factor = hist[number_of_bins/2] / density((overall_x_max-overall_x_min)/2)[0]
                    # plt.plot(xs, [density(x)*density_scale_factor for x in xs])

                fname = os.path.join(self._dirOutput, "rt_freq_%s_%s.pdf" % (buildId, runId))
                #plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
                #plt.tight_layout()
                plt.savefig(fname)
                plt.close()
        sys.stdout.write('\n')



    def _collect(self):
        runs = {}
        dirResults = os.path.join(os.getcwd(), "results", self._groupId)
        if not os.path.isdir(dirResults):
            raise Exception("Group result directory '%s' not found!" % dirResults)

        sys.stdout.write('_collect: ')
        # --- Runs --- #
        for run in os.listdir(dirResults):
            dirRun = os.path.join(dirResults, run)
            if os.path.isdir(dirRun):
                runs[run] = {}

                # --- Builds --- #
                for build in os.listdir(dirRun):
                    self.tick()
                    dirBuild = os.path.join(dirRun, build)
                    if os.path.isdir(dirBuild):
                        #runs[run][build] = []

                        # -- Count Users --- #
                        numUsers = 0
                        for user in os.listdir(dirBuild):
                            dirUser = os.path.join(dirBuild, user)
                            if os.path.isdir(dirUser):
                                numUsers += 1

                        txStats = {}
                        opStats = {}
                        hasOpData = False

                        # -- Users --- #
                        for user in os.listdir(dirBuild):
                            dirUser = os.path.join(dirBuild, user)
                            if os.path.isdir(dirUser):
                                if not os.path.isfile(os.path.join(dirUser, "transactions.log")):
                                    print "WARNING: no transaction log found in %s!" % dirUser
                                    continue
                                for rawline in open(os.path.join(dirUser, "transactions.log")):
                                    linedata = rawline.split(";")
                                    if len(linedata) < 2:
                                        continue

                                    txId        = linedata[0]
                                    runtime     = float(linedata[1]) * z # convert from s to ms
                                    starttime   = float(linedata[2]) * z # convert from s to ms

                                    opStats.setdefault(txId, {})
                                    txStats.setdefault(txId,{
                                        "totalTime": 0.0,
                                        "userTime":  0.0,
                                        "totalRuns": 0,
                                        "totalFail": 0,
                                        "rtTuples":  [],
                                        "rtMin":     0.0,
                                        "rtMax":     0.0,
                                        "rtAvg":     0.0,
                                        "rtMed":     0.0,
                                        "rtStd":     0.0
                                    })
                                    txStats[txId]["totalTime"] += runtime
                                    txStats[txId]["userTime"]  += runtime / float(numUsers)
                                    txStats[txId]["totalRuns"] += 1
                                    txStats[txId]["rtTuples"].append((starttime, runtime))

                                    if len(linedata) > 3:
                                        for opStr in linedata[3:]:
                                            opData = opStr.split(",")
                                            opStats[txId].setdefault(opData[0], {
                                                "rtTuples":  [],
                                                "avgRuns":   0.0,
                                                "rtMin":     0.0,
                                                "rtMax":     0.0,
                                                "rtAvg":     0.0,
                                                "rtMed":     0.0,
                                                "rtStd":     0.0
                                            })
                                            opStats[txId][opData[0]]["rtTuples"].append((float(opData[1]), float(opData[2])))

                                if os.path.isfile(os.path.join(dirUser, "failed.log")):
                                    for rawline in open(os.path.join(dirUser, "failed.log")):
                                        linedata = rawline.split(";")
                                        if len(linedata) < 2:
                                            continue
                                        txId        = linedata[0]
                                        runtime     = float(linedata[1]) * z # convert from s to ms
                                        starttime   = float(linedata[2]) * z # convert from s to ms
                                        if txStats.has_key(txId):
                                            txStats[txId]["totalFail"] += 1
                                            txStats[txId]["totalTime"] += runtime
                                            txStats[txId]["userTime"]  += runtime / float(numUsers)

                        for txId, txData in txStats.iteritems():
                            allRuntimes = [a[1] for a in txData["rtTuples"]]
                            txStats[txId]["rtTuples"].sort(key=lambda a: a[0])
                            txStats[txId]["rtRaw"] = allRuntimes
                            txStats[txId]["rtMin"] = amin(allRuntimes)
                            txStats[txId]["rtMax"] = amax(allRuntimes)
                            txStats[txId]["rtAvg"] = average(allRuntimes)
                            txStats[txId]["rtMed"] = median(allRuntimes)
                            txStats[txId]["rtStd"] = std(allRuntimes)
                            for opId, opData in opStats[txId].iteritems():
                                opStats[txId][opId]["avgRuns"] = average([a[0] for a in opData["rtTuples"]])
                                opStats[txId][opId]["rtMin"] = amin([a[1] for a in opData["rtTuples"]])
                                opStats[txId][opId]["rtMax"] = amax([a[1] for a in opData["rtTuples"]])
                                opStats[txId][opId]["rtAvg"] = average([a[1] for a in opData["rtTuples"]])
                                opStats[txId][opId]["rtMed"] = median([a[1] for a in opData["rtTuples"]])
                                opStats[txId][opId]["rtStd"] = std([a[1] for a in opData["rtTuples"]])
                            txStats[txId]["operators"] = opStats[txId]
                        if txStats != {}:
                            runs[run][build] = {"txStats": txStats, "numUsers": numUsers}
        sys.stdout.write('\n')
        return runs
