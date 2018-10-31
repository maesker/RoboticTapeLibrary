__author__ = 'maesker'

import datetime
import json
import re
import sys
import os
import bz2
import gzip
from ConfigParser import SafeConfigParser

from utilities.tiny_helper import percentile

class CLNParser:
    def __init__(self):
        self.drives = {}
        self.cln = {}
        self.filepat = re.compile("\s*robot_mounts_log_(?P<year>[0-9]+)-(?P<month>[0-9]+).log.gz")
        self.linepat = re.compile(".+#(?P<date>[0-9]+):(?P<time>[0-9]+)#.* DISMOUNT (?P<crt>[A-Z0-9]{6}).* Drive (?P<drv>1,[0-9]+,1,[0-9]+).*")

    def parse(self,dir):
        files = {}
        for i in os.listdir(dir):
            m = self.filepat.match(i)
            if m:
                year = int(m.group('year'))
                month = int(m.group('month'))
                if not year in files:
                    files[year]={}
                files[year][month] = i
        for year in sorted(files.keys()):
            for month in sorted(files[year].keys()):
                with gzip.open(os.path.join(dir,files[year][month]), 'r') as fp:
                    for line in fp.readlines():
                        self.handle_line(line)

        time_between_cln = []
        mnts_between_cln = []
        dur_cln_approx = []
        for drvid, obj in self.drives.items():
            time_between_cln.extend(obj['time_between_cln'])
            mnts_between_cln.extend(obj['mnts_between_cln'])
            dur_cln_approx.extend(obj['duration_cln_approx'])
        time_between_cln.sort()
        mnts_between_cln.sort()
        dur_cln_approx.sort()
        for i in range(0,101):
            perc = float(i)*0.01
            p1 = percentile(time_between_cln,perc)
            p2 = percentile(mnts_between_cln,perc)
            p3 = percentile(dur_cln_approx,perc)
            print perc, p1, p2, p3

    def handle_line(self, line):
        "hdre01#VLAD#20110822:001706#     MOUNT WB3575 Home 1,5,35,0,0 Drive 1,5,1,12 Client Host Id 136.156.216.161"
        m = self.linepat.match(line)
        if m:
            crt = m.group('crt')
            drv = m.group('drv')
            self.handle_dismount(drv,crt, m.group('date'), m.group('time'))

    def handle_dismount(self, drv, crt, date, time):
        if not drv in self.drives:
            self.drives[drv] = {
                'mnts_between_cln':[],
                'time_between_cln':[],
                'duration_cln_approx': [],
                'tmp': {
                    'mntcnt':0,
                    'last_clean':None,
                    'last_dm':None
                }
            }
        obj = self.drives[drv]
        currentdt = datetime.datetime.strptime("%s%s"%(date,time), "%Y%m%d%H%M%S")
        if crt.startswith('CLN'):
            if isinstance(obj['tmp']['last_clean'], datetime.datetime):
                diff = currentdt -obj['tmp']['last_clean']
                asint = diff.days*24*60*60 + diff.seconds
                obj['time_between_cln'].append(asint)

            if isinstance(obj['tmp']['last_dm'], datetime.datetime):
                diff = currentdt -obj['tmp']['last_dm']
                asint = diff.days*24*60*60 + diff.seconds
                obj['duration_cln_approx'].append(asint)

            obj['mnts_between_cln'].append(obj['tmp']['mntcnt'])
            obj['tmp']['mntcnt'] = 0
            obj['tmp']['last_clean'] = currentdt
        else:
            obj['tmp']['mntcnt'] += 1
        obj['tmp']['last_dm'] = currentdt


def read(robotdir, outputdir):
    filter_cln = True
    ejectre = re.compile(".+#(?P<YYYY>[0-9]{4})(?P<MM>[0-9]{2})(?P<DD>[0-9]{2}):(?P<hh>[0-9]{2})(?P<mm>[0-9]{2})(?P<ss>[0-9]{2})#\s+EJECT (?P<crt>[A-Z,a-z0-9]+).*")
    enterre = re.compile(".+#(?P<YYYY>[0-9]{4})(?P<MM>[0-9]{2})(?P<DD>[0-9]{2}):(?P<hh>[0-9]{2})(?P<mm>[0-9]{2})(?P<ss>[0-9]{2})#\s+ENTER (?P<crt>[A-Z,a-z0-9]+).*")
    otherre = re.compile(".+#(?P<YYYY>[0-9]{4})(?P<MM>[0-9]{2})(?P<DD>[0-9]{2}):(?P<hh>[0-9]{2})(?P<mm>[0-9]{2})(?P<ss>[0-9]{2})#\s+.+\s+(?P<crt>[A-Z,a-z0-9]+).*")
    crts = {}
    traces = os.listdir(robotdir)
    traces.sort()
    ejectre_match_ref = ejectre.match
    otherre_match_ref = otherre.match
    for i in traces:
        if i.startswith('robot_mounts'):
            print i
            with gzip.open(os.path.join(robotdir,i),'r') as gzfp:
                for line in gzfp:
                    m = ejectre_match_ref(line)
                    if m:
                        xxx = m.group("crt")
                        #print line
                        if filter_cln:
                            if xxx.startswith("CLN") or xxx.startswith('K'):
                                continue
                        crts[xxx] = "%s%s%s:%s%s%s"%(m.group("YYYY"),m.group("MM"),m.group("DD"),m.group("hh"),m.group("mm"),m.group("ss"))
                    else:
                        m2 = otherre_match_ref(line)
                        if m2:
                            if m2.group("crt") in crts.keys():
                                del crts[m2.group("crt")]
                                print "deleted ", m2.group("crt")

    swap = {}
    for k,v in crts.iteritems():
        if v not in swap:
            swap[v] = []
        swap[v].append(k)

    jsondmp = {'eject':[]}
    for k in sorted(swap.keys()):
        for crt in swap[k]:
            jsondmp['eject'].append((k,crt))

    out = os.path.join(outputdir, "crt_eject_events.json.bz2")
    output = bz2.BZ2File(out, 'wb')
    json.dump(jsondmp, output)
    output.close()



if __name__ == "__main__":
    #parser = argparse.ArgumentParser(description='CRT Ejections')
    #parser.add_argument("--robotdir", description="Robot trace dir")
    #args = parser.parse_args()

    cfg = SafeConfigParser()
    full = os.path.join(os.getcwd(), sys.argv[1])
    if not os.path.isfile(full):
        raise Exception("Config file %s not found", sys.argv[1])
    cfg.read(full)

    # extract the crt eject events
    read(cfg.get('ROBOT','tracedir'), cfg.get('OUTPUT','directory'))

    # print statistics of the cleaning cartridge usage
    c = CLNParser()
    c.parse(cfg.get('ROBOT','tracedir'))

