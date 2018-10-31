import os
import numpy
import logging
import multiprocessing
import datetime
import collections
import json
import gzip
import argparse
import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from utilities import tiny_helper
from utilities.io.persistent_cartridge_data import CartridgePersistence


days = mdates.DayLocator()
weeks = mdates.WeekdayLocator()
month = mdates.MonthLocator()
yearlocator = mdates.YearLocator()
yearsFmt = mdates.DateFormatter("%b'%y")
FONTSIZE1 = 18
MARKERSIZE = 9

font = {'family' : 'serif','serif': 'Times','size': FONTSIZE1}
matplotlib.rc('font', **font)
matplotlib.rcParams['text.usetex'] = True

static = {
    'realsystem_analysis':("ECMWF", 'o', '0.0', '-'),
    'LRU':      ('LRU',     'D', 'g',   '--'),
    'LRUA' :    ('LRUA',    '*', 'm',   '-.'),
    'FIFO' :    ('FIFO',    'o', 'c',   ':'),
    'BELADY' :  ('BELADY',  '>', 'b',   '--'),
    'DD1800':   ("DD1800",  '^', '0.5', '-'),
    'DDA1800':  ("DDA1800", 'v', '0.5', '-'),
    'DD60' :    ('DD60',    'v', '0.0', '-.'),
    'DD120' :   ('DD120',   '*', '0.0', '--'),
}

black_white_configs = []
#markelist = markers.MarkerStyle().markers
markerlist = ['o','v','^','x','p','<','>','s','*','h','+','d']
for color in reversed(['0.0','0.6','0.2','0.4','0.8']):
    for m in range(len(markerlist)):
        black_white_configs.append((markerlist[m],color,'--'))


def get_visual_config(key):
    if key in static:
        res = static[key]
    else:
        conf =  black_white_configs.pop()
        res = [key,conf[0],conf[1],conf[2]]
        static[key]=res
    return res

def cdf_plot(data, title, xlabel="xlable", ylabel="ylabel", order=[]):
    maxy = 0
    fig, ax = plt.subplots()
    lx = [':','--','-.']
    matplotlib.rc('font', **font)

    for key in order:
        values  = data[key]
        x = []
        y = []
        for j in reversed(sorted(values.keys())):
            if float(j) > 0.98: continue
            x.append(j)
            yval = values[j]
            maxy = max(maxy,yval)
            y.append(yval)
        (l,m,c, ls) = get_visual_config(key)
        ax.plot(y,x, label=l, color='0.0',linestyle=lx.pop(), linewidth=2.0)
        # format the ticks
        #ax.xaxis.set_major_locator(weeks)
        #ax.xaxis.set_major_formatter(yearsFmt)
        #ax.xaxis.set_minor_locator(days)

    ax.grid(True)
    box = ax.get_position()
    #ax.set_xlim(self.start, self.end)
    #ax.set_ylim(0, 100)

    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    #fig.autofmt_xdate()
    # rotates and right aligns the x labels, and moves the bottom of the
    # axes up to make room for them
    #plt.title("")
    plt.ylabel(ylabel)
    plt.xlabel(xlabel)
    plt.ylim(ymin=0)
    fig.tight_layout()
    plt.legend(loc=4)
    #plt.legend(bbox_to_anchor=(1.05,1), loc=4, prop={'size':14})
    #label = string.replace(r,'/','-')
    fig.set_size_inches(10,4)
    fig.set_dpi(40)
    plt.savefig(os.path.join(TGT, 'cdf_%s.pdf'%(xlabel.replace(" ","_"))))

def cb_map(item, out, start, end):
    def put(res, ts, key, val, action="incr"):
        if isinstance(ts, datetime.datetime):
            tsstr = ts.strftime("%Y%m%dT%H")
            if not tsstr in res:
                res[tsstr] = {}
            if action=="incr":
                if not key in res[tsstr]:
                    res[tsstr][key] = 0
                res[tsstr][key] += val
            elif action=="app":
                if not key in res[tsstr]:
                    res[tsstr][key] = []
                res[tsstr][key].append(val)

    res = {}
    persio = CartridgePersistence(item, 'sessions')
    operations = persio.get_operations()

    for obj in operations:
        obj__loadcomplete = obj.get('load_complete',None)
        if obj__loadcomplete:
            if obj__loadcomplete < start:
                continue
            if obj__loadcomplete > end:
                break
            put(res, obj__loadcomplete, 'load_complete', 1)
            if obj['load_request'] != None:
                latency_mount = obj__loadcomplete-obj['load_request']
                put(res, obj__loadcomplete, 'latency_mount', latency_mount.seconds, "app")
        obj__unload_complete = obj.get('unload_complete',None)
        if obj__unload_complete:
                put(res, obj__unload_complete, 'unload_complete', 1)
                if obj['unload_request'] != None:
                    latency_unmount = obj__unload_complete-obj['unload_request']
                    put(res,obj__unload_complete, 'latency_unmount', latency_unmount.seconds, "app")
        for m in  obj['mount_operations']:

            put(res, m[0], 'mount', 1)
    d = os.path.dirname(out)
    if not os.path.isdir(d):
        try:
            os.makedirs(d)
        except OSError:
            pass
        except:
            raise
    with open(out,'w') as fp:
        json.dump(res, fp)

class Base:
    def __init__(self, rootdir, start, end):
        self.logger = logging.getLogger()
        self.start = start
        self.end = end
        self.rootdir = rootdir
        self.datadir = os.path.join(self.rootdir, 'data')


    def parse_raw_jason(self):
        master_stats={
            'total':{
                'latency_unmount':[],
                'latency_mount':[],
                'mount':0,
                'load_complete':0,
                'unload_complete':0
            },
            'monthly':{}
        }
        if 'results' in os.listdir(self.datadir):
            outp = os.path.join(self.datadir,'results','rawoutput.json')
            if os.path.isfile(outp):
                with open(outp,'r') as fp:
                    rawjson = json.load(fp)
                    ref = rawjson['results']
                    for k in ref.keys():
                        dt = datetime.datetime(year=int(k[0:4]), month=int(k[4:6]), day=1)
                        if self.start <= dt <= self.end:
                            for param in ['latency_unmount','latency_mount' ]:
                                if param in ref[k]:
                                    master_stats['total'][param].extend(ref[k][param])
        return  master_stats

# # # # # # # #  START STATS # # # # # # # # # # # # # # # #

def run_stats(args):
    start = datetime.datetime.strptime(args.start, "%Y%m%d")
    end = datetime.datetime.strptime(args.end, "%Y%m%d")
    try:
        s = Stats(args.rootdir, start, end)
    except:
        pass

class Stats(Base):
    """
    @brief examine.py stats --rootdir <path_to_res> --start <YYYYMMDD> --end <YYYYMMDD>
    """
    def __init__(self,datadir, start, end):
        Base.__init__(self,datadir, start, end)
        self.crtdir = os.path.join(self.datadir, 'cartridge')
        if os.path.isdir(self.crtdir):
            self.tmpdir = os.path.join(self.datadir , "__tmp__")
            if not os.path.isdir(self.tmpdir):
                #shutil.rmtree(self.tmpdir)
                self.resultdir = os.path.join(self.datadir, "results")
                for x in [self.datadir, self.tmpdir, self.resultdir]:
                    if not os.path.isdir(x):
                        os.makedirs(x)
                print "Processing ", datadir
                self.processes = multiprocessing.cpu_count()
                crts = collections.deque()
                for i in os.listdir(self.crtdir):
                    tmp = os.path.join(self.crtdir,i)
                    for j in os.listdir(tmp):
                        crts.append(os.path.join(i,j))
                #self.processes = 1
                #self.process_map(crts)
                self.process_map(filter(lambda x:not x.startswith('CLN') ,crts))
                self.process_reduce()
            else:
                "Tmp dir found: ", self.tmpdir
        else:
            print "Cartridge dir not found", self.crtdir

    def process_map(self, items):
        processes = {}
        cnt = 0
        total = len(items)
        for i in range(self.processes):
            processes[i] = None
        for item in items:
            if item.endswith('sqlite'):
                continue
            index = None
            while index == None:
                for i, p in processes.items():
                    if p == None:
                        index = i
                        break
                        # no process has been started
                    elif p.is_alive() == False:
                        index = i
                        processes[i]=None
                        break
                        # process terminated
                if index == None:
                    for i, p in processes.items():
                        p.join(0.01)

            #print self.start, self.end, item
            processes[index] = multiprocessing.Process(
                target=cb_map, args=(
                    os.path.join(self.crtdir, item),
                    os.path.join(self.tmpdir, item),
                    self.start,
                    self.end,)
            )
            processes[index].daemon = True
            #print "Starting index", index, processes[index]
            processes[index].start()
            cnt += 1
            if not cnt%1000:
                print "Number %s of %s started index %i"%(cnt, total, index)
        print "Done"

    def process_reduce(self):
        print "Reduce"
        res = {}
        open__ref = open
        list_ref = list
        int_ref = int
        isinstance_ref = isinstance
        for itempart1 in os.listdir(self.tmpdir):
            #for itempart2 in os.listdir(os.path.join(self.tmpdir,itempart1)):
                p = os.path.join(self.tmpdir,itempart1)
                for tail in os.listdir(p):
                    print p, tail
                    try:
                        p2 = os.path.join(p,tail)
                        with open__ref(p2,'r') as fp:
                            part = json.load(fp)
                            for k,v in part.items():
                                if k not in res:
                                    res[k]={}
                                res_k_ref = res[k]
                                for att,val in v.items():
                                    if isinstance_ref(val, list_ref):
                                        if att not in res_k_ref:
                                            res_k_ref[att] = []
                                        res_k_ref[att].extend(val)
                                    elif isinstance_ref(val,int_ref):
                                        if att not in res_k_ref:
                                            res_k_ref[att] = 0
                                        res_k_ref[att] += val
                    except ValueError, e:
                        print e.message
                        print p, tail

        all_load_latency = []
        all_unload_latency = []
        json_dump = {'resdir':self.resultdir, 'start':str(self.start), 'end':str(self.end), 'results':res}
        with gzip.open(os.path.join(self.resultdir,'rawoutput.json.gz'), 'w') as fp:
            json.dump(json_dump, fp)
        with open(os.path.join(self.resultdir,"output.csv"), 'w') as fp:
            fp.write("Date;mounts;loads;unloads;----;minLL;meanLL,maxLL;---;minUL;meanUL;maxUL;sumLL\n")
            for k,v in sorted(res.items()):
                #for att,val in v.items():

                mean_loadlat = "0"
                min_loadlat = "0"
                max_loadlat = "0"
                mean_unloadlat = "0"
                min_unloadlat = "0"
                max_unloadlat = "0"
                sum_loadlat = "0"
                mount = "0"
                lc = "0"
                uc = "0"

                try:
                    if 'latency_mount' in v:
                        all_load_latency.extend(v['latency_mount'])
                        mean_loadlat = "%03.1f"%numpy.mean(v['latency_mount'])
                        mean_loadlat = "%s"%mean_loadlat.rjust(7)
                        min_loadlat = "%03i"%min(v['latency_mount'])
                        max_loadlat = "%03i"%max(v['latency_mount'])
                        sum_loadlat = "%03i"%sum(v['latency_mount'])
                    if 'latency_unmount' in v:
                        all_unload_latency.extend(v['latency_unmount'])
                        mean_unloadlat = "%3.1f"%numpy.mean(v['latency_unmount'])
                        mean_unloadlat = "%s"%mean_unloadlat.rjust(7)
                        min_unloadlat = "%03i"%min(v['latency_unmount'])
                        max_unloadlat = "%03i"%max(v['latency_unmount'])
                    if 'mount' in v:
                        mount = "%04i"%v['mount']
                    if 'load_complete' in v:
                        lc = "%04i"%v['load_complete']
                    if 'unload_complete' in v:
                        uc = "%04i"%v['unload_complete']

                    fp.write("%s; %s; %s; %s;---; %s; %s; %s;----; %s; %s; %s;%s\n"%(
                        k, mount , lc , uc,
                        min_loadlat,mean_loadlat,max_loadlat,
                        min_unloadlat,mean_unloadlat,max_unloadlat, sum_loadlat
                    ))
                except Exception, e:
                    self.logger.exception(e)
                    print "Exception at ",k, v
                    pass

        # # percentiles
        all_load_latency.sort()
        all_unload_latency.sort()
        with open(os.path.join(self.resultdir,"percentiles.csv"), 'w') as fp:
            fp.write("Percentile;loadlatency;unloadlatency\n")
            for i in range(0,101):
                perc = float(i)*0.01
                p1 = tiny_helper.percentile(all_load_latency, perc)
                p2 = tiny_helper.percentile(all_unload_latency, perc)
                fp.write("%f;%i;%i\n"%(perc, p1, p2))

# # # # # # # #  END STATS # # # # # # # # # # # # # # # #


# # # # # # # #  START VISUAL  # # # # # # # # # # # # # # #

def run_visual(args):
    start = datetime.datetime.strptime(args.start, "%Y%m%d")
    end = datetime.datetime.strptime(args.end, "%Y%m%d")
    try:
        s = Visualize(args.rootdir, start, end)
    except:
        raise

class Visualize(Base):
    """
    Basic usage examine.py visual --rootdir <path-to-results> --start YYYYMMDD --end YYYYMMDD
    Only displays what has been analysed with 'stats' previously  """
    def __init__(self, datadir, start, end):
        Base.__init__(self,datadir, start, end)
        self.figures = os.path.join(self.datadir, 'figures')
        if not os.path.isdir(self.figures ):
            os.makedirs(self.figures )

        #self.draw_cdf()

    def draw_cdf(self):
        master_stats = self.parse_raw_jason()

        loadlat = {}
        for ex in order2:
            loadlat[ex] = {}
            lats = master_stats[ex]['total']['latency_mount']
            lats.sort()
            for i in range(0, 101):
                perc = float(i) * 0.01
                loadlat[ex][perc] = tiny_helper.percentile(lats, perc)

        xlabel = "Load Latency in Seconds"
        ylabel = "Percentage of Requests"
        title = "1. Jan. 2013 to 20. May 2014: Load Latency"
        cdf_plot(loadlat, title, xlabel, ylabel, order2)

        unloadlat = {}
        for ex in order2:
            unloadlat[ex] = {}
            unlats = master_stats[ex]['total']['latency_unmount']
            unlats.sort()
            for i in range(0, 101):
                perc = float(i) * 0.01
                unloadlat[ex][perc] = tiny_helper.percentile(unlats, perc)
        xlabel = "Unload Latency in Seconds"
        cdf_plot(unloadlat, "1. Jan. 2013 to 20. May 2014: Unload Latency", xlabel, ylabel, order2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Examine HPSS Parser results')
    subparsers = parser.add_subparsers()

    parser_stats = subparsers.add_parser('stats', help="Get trace statistics")
    parser_stats.set_defaults(func=run_stats)
    parser_stats.add_argument("--rootdir",
                        help="Path to the root directory", default="simulator")
    parser_stats.add_argument("--start",
                        help="starting time of analysis - start at YYYYMMDD", default="20110820")
    parser_stats.add_argument("--end",
                        help="ending time of analysis - end of YYYYMMDD", default="20140521")


    parser_visual = subparsers.add_parser('visual', help="draw plots")
    parser_visual.set_defaults(func=run_visual)
    parser_visual.add_argument("--rootdir",
                              help="Path to the root directory",
                              default="simulator")
    parser_visual.add_argument("--start",
                              help="starting time of analysis - start at YYYYMMDD",
                              default="20110820")
    parser_visual.add_argument("--end",
                              help="ending time of analysis - end of YYYYMMDD",
                              default="20140521")

    args = parser.parse_args()
    args.func(args)




