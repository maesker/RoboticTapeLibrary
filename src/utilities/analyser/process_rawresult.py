import json
import gzip
import argparse
import sys
import os
import numpy
import datetime

from utilities.tiny_helper import percentile

class RawResult:
    def __init__(self, filepath):
        self.filepath = filepath

    def debug(self):
        dt_jun13 = datetime.datetime(2013,6,1)
        dt_jul13 = datetime.datetime(2013,7,1)
        june13 =  self.get_results(dt_jun13,dt_jul13,self.filepath)
        for date in sorted(june13.keys()):
            print date
            for k,v in june13[date].iteritems():
                print k, v

    def get_results(self, start_dt, end_dt, inputfile):
        ret = {}
        with gzip.open(inputfile, 'r') as fp:
            results = json.load(fp)
            for dtstr, res in results['results'].iteritems():
                dtinst = datetime.datetime(year=int(dtstr[:4]),
                                           month=int(dtstr[4:6]),
                                           day=int(dtstr[6:8]),
                                           hour=int(dtstr[9:11]))
                if start_dt <= dtinst <= end_dt:
                    ret[dtinst] = res
        return ret

    def compare(self, f1, f2):
        dt_jun13 = datetime.datetime(2013,6,1)
        dt_jul13 = datetime.datetime(2013,7,1)
        f1_june13 =  self.get_results(dt_jun13,dt_jul13,f1)
        f2_june13 = self.get_results(dt_jun13, dt_jul13, f2)
        self._compare_latency_load(f1_june13, f2_june13)
        self._compare_latency_unload(f1_june13, f2_june13)

    def _get_percentiles(self, val):
        sortedval = sorted(val)
        ret = []
        for i in [0.02, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.98]:
            ret.append(percentile(sortedval, i))
        return ret

    def _get_list_diff(self, l1, l2):
        l1_perc = self._get_percentiles(l1)
        l2_perc = self._get_percentiles(l2)
        diffs = []
        for i in range(len(l1_perc)):
            diffs.append(abs(l1_perc[i]-l2_perc[i]))
        return numpy.mean(diffs)

    def _compare_latency_load(self, f1res, f2res):
        loadlat_diff = {}
        diffs = []
        for date in sorted(f1res.keys()):
            diff = self._get_list_diff(f1res[date]['latency_mount'],
                                       f2res[date]['latency_mount'])
            loadlat_diff[date] = diff
            diffs.append(diff)
        print "Load Latency Diff: %3.2f +- %2.2f"%(
            numpy.mean(diffs),numpy.std(diffs))


    def _compare_latency_unload(self, f1res, f2res):
        loadlat_diff = {}
        diffs = []
        for date in sorted(f1res.keys()):
            diff = self._get_list_diff(f1res[date]['latency_unmount'],
                                       f2res[date]['latency_unmount'])
            loadlat_diff[date] = diff
            diffs.append(diff)
        print "Unload Latency Diff: %3.2f +- %2.2f" % (
            numpy.mean(diffs), numpy.std(diffs))


if __name__ == '__main__':
    f1 = '/tmp/real_rawoutput.json.gz'
    f2 = '/tmp/finalex99_dd_opt0_60_rawoutput.json.gz'

    r = RawResult(None)
    r.compare(f1, f2)
