import collections
import os
import datetime
import math

__author__ = 'maesker'


def translate_drive_id(drvid):
    return "D_%s_%s"%(drvid[1:3],drvid[4:6])

def get_cartridge_dirs(basedir):
    crts = collections.deque()
    for i in os.listdir(basedir):
        if i[0]==".": continue
        tmp = os.path.join(basedir,i)
        for j in os.listdir(tmp):
            #crts.append(os.path.join(tmp,j))
            crts.append(os.path.join(i,j))
    return crts


# actually the same as crt
def get_drive_dirs(basedir):
    drvs = collections.deque()
    for i in os.listdir(basedir):
        tmp = os.path.join(basedir,i)
        for j in os.listdir(tmp):
            drvs.append(os.path.join(i,j))
    return drvs

def get_diff_in_seconds(start, end):
    diff = end - start
    return diff.total_seconds()
    #return diff.days * 86400 + diff.seconds


def percentile(N, P):
    """
    Find the percentile of a list of values

    @parameter N - A list of values.  N must be sorted.
    @parameter P - A float value from 0.0 to 1.0

    @return - The percentile of the values.
    """
    n = int(math.ceil(P * len(N) - 0.5))
    return N[min(n,len(N)-1)]


def get_total_reading_time():
    import json, gzip, sys

    with gzip.open(sys.argv[1], 'r') as f:
        total_readingtime = 0
        events = json.load(f)
        number_of_events = len(events['events'])-1 # minus bye statement
        for event in events['events']:
            try:
                total_readingtime += int(event[2])
            except:
                print event
        print total_readingtime/(3600.0*24), number_of_events

#get_total_reading_time() ## just a quick access