import os, sys
import shutil
import matplotlib as mpl
mpl.use('Agg')
from pylab import *
from scipy.stats import gaussian_kde
from matplotlib.ticker import FixedFormatter
import multiprocessing as mp
from matplotlib.font_manager import FontProperties

import matplotlib.pyplot as plt
import copy 
import re

#Direct input 
plt.rcParams['text.latex.preamble']=[r"\usepackage{lmodern}"]
#Options
params = {'text.usetex' : True,
          'font.size' : 11,
          'font.family' : 'lmodern',
          'text.latex.unicode': True,
          }
plt.rcParams.update(params)


import numpy as np

# const factor to convert result times from logs
# if papi is deactivated, logs contain time in secons
# we want ms, so factor ist 1000
z = 1000

def process_ab_logfile(plotter, dirResults, run, build, preview_count):
    try:
        txStats = {}
        opStats = {}

        dirRun = os.path.join(dirResults, run)
        run_parameters = eval(run.replace("@", ","))
        dirBuild = os.path.join(dirRun, build)
        hasOpData = False

        if not os.path.isdir(dirBuild):
            print "WARNING: result dir not found: %s!" % dirBuild
            return

        if not os.path.isfile(os.path.join(dirBuild, "ab.log")):
            print "WARNING: no transaction log found in %s!" % dirBuild
            return

        i = 0
        for rawline in open(os.path.join(dirBuild, "ab.log")):
            i = i + 1
            # preview mode?
            if preview_count != None and preview_count>0 and i>preview_count:
                break

            if rawline.startswith("starttime"):
                continue

            rawline = rawline.strip('\n')
            linedata = rawline.split("\t")

            if len(linedata) < 2:
                continue

            txId        = linedata[6]
            runtime     = float(linedata[4]) / 1000 # convert from usec to ms
            starttime   = int(linedata[1]) # seconds
            txStatus    = int(linedata[7])

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
                "rtStd":     0.0,
                "starttime": 0,
                "endtime":   0
            })

            if starttime < txStats[txId]["starttime"] or txStats[txId]["starttime"] == 0:
                txStats[txId]["starttime"] = starttime

            if starttime > txStats[txId]["endtime"] or txStats[txId]["endtime"] == 0:
                txStats[txId]["endtime"] = starttime

            if txStatus >= 200 and txStatus < 300:
                txStats[txId]["totalRuns"] += 1
                txStats[txId]["rtTuples"].append((starttime, runtime))
            else:
                txStats[txId]["totalFail"] += 1

        for txId, txData in txStats.iteritems():
            allRuntimes = [a[1] for a in txData["rtTuples"]]
            if len(allRuntimes) == 0:
                allRuntimes = [0]

            txStats[txId]["rtTuples"].sort(key=lambda a: a[0])
            txStats[txId]["rtRaw"] = allRuntimes
            txStats[txId]["rtMin"] = amin(allRuntimes)
            txStats[txId]["rtMax"] = amax(allRuntimes)
            txStats[txId]["rtAvg"] = average(allRuntimes)
            txStats[txId]["rtMed"] = median(allRuntimes)
            txStats[txId]["rtStd"] = std(allRuntimes)
            txStats[txId]["totalTime"] = txStats[txId]["endtime"] - txStats[txId]["starttime"]

            for opId, opData in opStats[txId].iteritems():
                opStats[txId][opId]["avgRuns"] = average([a[0] for a in opData["rtTuples"]])
                opStats[txId][opId]["rtMin"] = amin([a[1] for a in opData["rtTuples"]])
                opStats[txId][opId]["rtMax"] = amax([a[1] for a in opData["rtTuples"]])
                opStats[txId][opId]["rtAvg"] = average([a[1] for a in opData["rtTuples"]])
                opStats[txId][opId]["rtMed"] = median([a[1] for a in opData["rtTuples"]])
                opStats[txId][opId]["rtStd"] = std([a[1] for a in opData["rtTuples"]])
            txStats[txId]["operators"] = opStats[txId]
        
        plotter.tick()
        return {"run": run, "build": build, "txStats": txStats, "parameters": run_parameters}

    except Exception as e:
        print "Unexpected error in process_ab_logfile:", e
        raise

def process_over_time_plot(buildId, runId, runData, dirOutput):

    try:
        number_of_bins = 50
        build_run_identifier_name = "(build '%s', run '%s')" % (buildId, runId.translate(None, '{}_'))
        fig = plt.figure(1, figsize=(10, 10))

        ax_latencies = fig.add_subplot(2, 1, 1)
        ax_latencies.set_xlabel("Runtime in Seconds")
        ax_latencies.set_ylabel("Latency in ms")

        ax_throughput = fig.add_subplot(2, 1, 2)
        ax_throughput.set_xlabel("Runtime in Seconds")
        ax_throughput.set_ylabel("Throughput per Second")

        for txId, txData in runData[buildId]["txStats"].iteritems():
            if txId != "TPCC-NewOrder":
                continue
             # sort runtimes by starttime and create bins
            tx_start = txData["starttime"]
            latency_dict = {}
            if len(txData["rtTuples"]) == 0:
                continue
            
            bin_factor = (txData["endtime"] - txData["starttime"]) / number_of_bins
            for starttime, runtime in txData["rtTuples"]:
                binned_time = (starttime-tx_start)/bin_factor
                if not binned_time in latency_dict:
                    latency_dict[binned_time] = []
                latency_dict[binned_time].append(runtime)
            sorted_keys = [x for x in sorted(latency_dict.iterkeys())]

            ax_latencies.plot([x*bin_factor for x in sorted_keys], [np.median(latency_dict[x]) for x in sorted_keys], label=txId)
            ax_throughput.plot([x*bin_factor for x in sorted_keys], [len(latency_dict[x])/bin_factor for x in sorted_keys], label=txId)
            sys.stdout.write(".")
            sys.stdout.flush()

        fname = os.path.join(dirOutput, "over_time_%s_%s.pdf" % (buildId, runId))
        plt.savefig(fname)
        plt.close() 

    except Exception as e:
        print "Unexpected error in process_over_time_plot:", e
        raise


class Plotter:

    def __init__(self, benchmarkGroupId, use_ab = False, preview = None):
        self._groupId = benchmarkGroupId
        self._dirOutput = os.path.join(os.getcwd(), "plots", str(self._groupId))
        self._varyingParameter = None
        self._use_ab = use_ab
        self._preview = preview

        if use_ab:
            self._runs = self._collect_ab()
        else:
            self._runs = self._collect()

        # get build ids from all runs
        self._buildIds = list(set([build_id for x in self._runs.keys() for build_id in self._runs[x].keys()]))


        #inlfate default values
        if "{'default': 'yes'}" in self._runs:
            default_values = self._runs["{'default': 'yes'}"]
            del self._runs["{'default': 'yes'}"]
            for build_id in default_values:
                for key in self._runs:
                    self._runs[key][build_id] = copy.copy(default_values[build_id])
                    self._runs[key][build_id]["parameters"] = eval(key)

        if not os.path.isdir(self._dirOutput):
            os.makedirs(self._dirOutput)

    def tick(self):
        sys.stdout.write('.')
        sys.stdout.flush()

    def setAxLinesBW(self,ax):
        """
        Take each Line2D in the axes, ax, and convert the line style to be 
        suitable for black and white viewing.
        """
        MARKERSIZE = 3

        COLORMAP = {
            'b': {'marker': None, 'dash': (None,None)},
            'g': {'marker': None, 'dash': [5,5]},
            'r': {'marker': None, 'dash': [5,3,1,3]},
            'c': {'marker': None, 'dash': [1,3]},
            'm': {'marker': None, 'dash': [5,2,5,2,5,10]},
            'y': {'marker': None, 'dash': [5,3,1,2,1,10]},
            'k': {'marker': 'o', 'dash': (None,None)} #[1,2,1,10]}
            }

        lines = ax.get_lines()
        if ax.get_legend() != None:
         lines = lines + ax.get_legend().get_lines()

        for line in lines:
            origColor = line.get_color()
            line.set_color('black')
            line.set_dashes(COLORMAP[origColor]['dash'])
            line.set_marker(COLORMAP[origColor]['marker'])
            line.set_markersize(MARKERSIZE)

    def setFigLinesBW(self, fig):
        """
        Take each axes in the figure, and for each line in the axes, make the
        line viewable in black and white.
        """
        for ax in fig.get_axes():
            self.setAxLinesBW(ax)

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
                print "consolidating:"
                # consolidate all transactions
                for txId, txData in buildData["txStats"].iteritems():
                    print txId, txData["totalTime"]
                    totalRuns += txData["totalRuns"]
                    totalTime = max(txData["totalTime"], totalTime)
                print "---->", txId, totalTime

                for txId, txData in buildData["txStats"].iteritems():
                    print "|     -------------------------------------------------------------------------------------------"
                    print "|     TX: {:14s} tps: {:05.2f}, min: {:05.2f}, max: {:05.2f}, avg: {:05.2f}, med: {:05.2f} (all in ms)".format(txId, float(txData["totalRuns"]) / totalTime, txData["rtMin"], txData["rtMax"], txData["rtAvg"], txData["rtMed"])
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

    def plotTotalThroughput(self, xtitle, x_parameter, xtitle_converter=None):
        sys.stdout.write('plotTotalThroughput: ')

        fig = plt.figure()
        fig.set_size_inches(5.31,3.54) 

        plt.title("Total Transaction Throughput")
        plt.ylabel("Transactions per Minute")
        plt.xlabel(xtitle)

        for buildId in self._buildIds:
            plotX = []
            plotY = []
            for runId, runData in self._runs.iteritems():
                self.tick()
                totalRuns = 0.0
                totalTime = 0.0
                x_value = runData[buildId]["parameters"][x_parameter] 
                plotX.append(x_value)
                # consolidate all transactions
                for txId, txData in runData[buildId]["txStats"].iteritems():
                    totalRuns += txData["totalRuns"]
                    totalTime = max(txData["totalTime"], totalTime)
                plotY.append(60 * totalRuns / totalTime)
            plotX, plotY = (list(t) for t in zip(*sorted(zip(plotX, plotY))))

            if xtitle_converter != None:
                plotX = [xtitle_converter(x) for x in plotX]

            plt.plot(plotX, plotY, label=buildId.replace("_", "-"))

        # customize legend
        fontP = FontProperties()
        fontP.set_size('small')
        plt.legend(loc='lower right', prop={'size':8}, handlelength=3)

        fname = os.path.join(self._dirOutput, "total_throughput.pdf")
        self.setFigLinesBW(fig)
        plt.savefig(
            fname,
             #This is simple recomendation for publication plots
            dpi=1000, 
            # Plot will be occupy a maximum of available space
            bbox_inches='tight')
        plt.close()
        sys.stdout.write('\n')

    def plotOverTime(self):

        sys.stdout.write('plotResponseTimesOverTime: ')
        pool = mp.Pool(mp.cpu_count())

        for buildId in self._buildIds:
            overall_x_min = 0
            overall_x_max = 0
            overall_y_max = 0
            overall_y_min = 0

            for runId, runData in self._runs.iteritems():
                # pool.apply_async(process_over_time_plot, [buildId, runId, runData, self._dirOutput])
                process_over_time_plot(buildId, runId, runData, self._dirOutput)

        pool.close()
        pool.join()
        sys.stdout.write('\n')

    def plotTotalFails(self, xtitle, x_parameter, xtitle_converter=None):
        fig = plt.figure()
        fig.set_size_inches(5.31,3.54) 

        plt.title("Total Failed Transactions")
        plt.ylabel("Failed Transactions per Minute")
        plt.xlabel(xtitle)

        for buildId in self._buildIds:
            plotX = []
            plotY = []
            for runId, runData in self._runs.iteritems():
                self.tick()
                totalFails = 0.0
                totalTime = 0.0
                x_value = runData[buildId]["parameters"][x_parameter] 
                plotX.append(x_value)
                # consolidate all transactions
                for txId, txData in runData[buildId]["txStats"].iteritems():
                    totalFails += txData["totalFail"]
                    totalTime = max(txData["totalTime"], totalTime)
                plotY.append(60 * totalFails / totalTime)
            plotX, plotY = (list(t) for t in zip(*sorted(zip(plotX, plotY))))

            if xtitle_converter != None:
                plotX = [xtitle_converter(x) for x in plotX]

            plt.plot(plotX, plotY, label=buildId.replace("_", "-"))

        # customize legend
        fontP = FontProperties()
        fontP.set_size('small')
        plt.legend(loc='lower right', prop={'size':8}, handlelength=3)

        fname = os.path.join(self._dirOutput, "total_fails.pdf")
        self.setFigLinesBW(fig)
        plt.savefig(
            fname,
             #This is simple recomendation for publication plots
            dpi=1000, 
            # Plot will be occupy a maximum of available space
            bbox_inches='tight')
        plt.close()
        sys.stdout.write('\n')

    def plotTransactionResponseTimes(self):
        sys.stdout.write('plotTransactionResponseTimes: ')
        for txId in ["NEW_ORDER"]: #,"PAYMENT","STOCK_LEVEL","DELIVERY","ORDER_STATUS"]:
            plt.title("%s Response Times" % txId)
            plt.ylabel("Avg. Response Time in ms")
            plt.xlabel("Number of Parallel Users")
            for buildId in self._buildIds:
                plotX = []
                plotY = []
                for runId, runData in self._runs.iteritems():
                    self.tick()
                    plotX.append(runData[buildId]["numUsers"])
                    plotY.append(runData[buildId]["txStats"][txId]["rtAvg"])
                plotX, plotY = (list(t) for t in zip(*sorted(zip(plotX, plotY))))
                plt.plot(plotX, plotY, label=buildId)
            plt.legend(loc='upper left', prop={'size':10})
            fname = os.path.join(self._dirOutput, "%s_avg_rt.pdf" % txId)
            plt.savefig(fname)
            plt.close()
        sys.stdout.write('\n')

    def plotResponseTimesVaryingUsers(self):
        sys.stdout.write('plotResponseTimesVaryingUsers: ')
        plt.figure(1, figsize=(10, 20))
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
                fig = plt.figure(1, figsize=(10, 10))
                fig.subplots_adjust(bottom=0.2, hspace=0.6)
                for txId, txData in runData[buildId]["txStats"].iteritems():
                    self.tick()
                    curPlt += 1

                    ax = fig.add_subplot(maxPlt, 1, curPlt)
                    plt.title("RT Frequency in %s (build '%s', run '%s')" % (txId, buildId, runId))
                    plt.xlabel("Response Time in ms")
                    plt.ylabel("Number of Transactions")

                    p90 = percentile(txData["rtRaw"], 90)
                    x2ticks = [txData["rtMin"], txData["rtAvg"], p90, txData["rtMax"]]
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

                    # density = gaussian_kde(txData["rtRaw"])
                    # density_scale_factor = hist[number_of_bins/2] / density((overall_x_max-overall_x_min)/2)[0]
                    # plt.plot(xs, [density(x)*density_scale_factor for x in xs])

                fname = os.path.join(self._dirOutput, "rt_freq_%s_%s.pdf" % (buildId, runId))
                plt.savefig(fname)
                plt.close()
        sys.stdout.write('\n')

    def _collect_ab(self):
        pool = mp.Pool(mp.cpu_count())
        results = []
        
        runs = {}
        dirResults = os.path.join(os.getcwd(), "results", self._groupId)
        if not os.path.isdir(dirResults):
            raise Exception("Group result directory '%s' not found!" % dirResults)

        sys.stdout.write('_collect from ab: ')
        sys.stdout.flush()

        # --- Runs --- #
        for run in os.listdir(dirResults):
            
            dirRun = os.path.join(dirResults, run)
            if not os.path.isdir(dirRun):
                continue
            runs[run] = {}

            # --- Builds --- #
            for build in os.listdir(dirRun):

                dirBuild = os.path.join(dirRun, build)
                if not os.path.isdir(dirBuild):
                    continue
                result = pool.apply_async(process_ab_logfile, [self, dirResults, run, build, self._preview])
                results.append(result)

        pool.close()
        pool.join()

        for result_obj in results:
            result = result_obj.get()
            if result == None:
                continue
            run = result["run"]
            build = result["build"]
            txStats = result["txStats"]
            parameters = result["parameters"]
            if txStats != None:
                runs[run][build] = {"txStats": txStats, "parameters": parameters}
                    
        sys.stdout.write('\n')
        return runs


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
