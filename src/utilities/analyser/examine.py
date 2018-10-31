import argparse
import collections
import csv
import datetime
import gzip
import json
import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import multiprocessing
import numpy
import os
import re
import string
import sys
import gzip

from utilities.io.persistent_cartridge_data import CartridgePersistence
from utilities import tiny_helper


def nametranslation(name, subscript=True):
    if name == "DD_opt0_par300":
        if not subscript:
            return "DD300"
        return "DD300_0"
    if name == "DD_opt1_par300":
        return "DD300_1"
    if name == "DD_opt4_par300":
        return "DD300_2"
    if name == "DD_opt0_par900":
        if not subscript:
            return "DD900"
        return "DD900_0"
    if name == "DD_opt1_par900":
        return "DD900_1"
    if name == "DD_opt4_par900":
        return "DD900_2"
    if name == "DD_opt0_par1800":
        if not subscript:
            return "DD1800"
        return "DD1800_0"
    if name == "DD_opt1_par1800":
        return "DD1800_1"
    if name == "DD_opt4_par1800":
        return "DD1800_2"
    if name == "DD_opt0_par0":
        if not subscript:
            return "DD0"
        return "DD0_0"
    if name == "DD_opt0_par60":
        if not subscript:
            return "DD60"
        return "DD60_0"
    if name == "DD_opt0_par120":
        if not subscript:
            return "DD120"
        return "DD120_0"
    if name == "DD_opt0_par180":
        if not subscript:
            return "DD180"
        return "DD180_0"
    if name == "DD_opt0_par600":
        if not subscript:
            return "DD600"
        return "DD600_0"
    if name == "DD_opt0_par1200":
        if not subscript:
            return "DD1200"
        return "DD1200_0"
    if name == "FIFO_opt0":
        if not subscript:
            return "FIFO"
        return "FIFO_0"

    if name == "BELADY_opt0":
        if not subscript:
            return "BELADY"
        return "BELADY_0"


    if name == "LRU_opt0":
        if not subscript:
            return "LRU"
        return "LRU_0"
    if name == "LRU_opt1":
        return "LRU_1"
    if name == "LRU_opt4":
        return "LRU_2"
    if name == 'realsystem_analysis':
        return 'realsystem_analysis'



TGT = '/tmp/images'

days = mdates.DayLocator()
weeks = mdates.WeekdayLocator()
month = mdates.MonthLocator()
yearlocator = mdates.YearLocator()
yearsFmt = mdates.DateFormatter("%b'%y")
FONTSIZE1 = 18
MARKERSIZE = 9

font = {'family' : 'serif',
				'serif': 'Times',
        'size'   : FONTSIZE1}
matplotlib.rc('font', **font)

#rc_pdf = {'fonttype':1}
#matplotlib.rc('pdf', **rc_pdf)
matplotlib.rcParams['text.usetex'] = True

static = {
    'realsystem_analysis': ("ECMWF", 'o', '0.0', '-'),
    'LRU' : ('LRU', 'D', 'g', '--'),
    'LRUA' : ('LRUA', '*', 'm', '-.'),
    'FIFO' : ('FIFO', 'o', 'c', ':'),
    'BELADY' : ('BELADY', '>', 'b', '--'),
    'DD1800': ("DD1800", '^', '0.5', '-'),
    'DDA1800': ("DDA1800", 'v', '0.5', '-'),
    'DD60' : ('DD60', 'v', '0.0', '-.'),
    'DD120' : ('DD120', '*', '0.0', '--'),
}

black_white_configs = []
#markelist = markers.MarkerStyle().markers
markerlist = ['o','v','^','x','p','<','>','s','*','h','+','d']
for color in reversed(['0.0','0.6','0.2','0.4','0.8']):
    for m in range(len(markerlist)):
        black_white_configs.append((markerlist[m],color,'--'))



__author__ = 'maesker'

DD0_900 = "experiment99/DD_opt0_par900"
DD1_900 = "experiment99/DD_opt1_par900"
DD2_900 = "experiment99/DD_opt2_par900"
DD3_900 = "experiment99/DD_opt3_par900"
DD4_900 = "experiment99/DD_opt4_par900"

DEFAULTX = ['realsystem_analysis',DD0_900,DD1_900,DD2_900,DD3_900,DD4_900]

DEF_A = 1

DEFINE_MAX_Y_VALUE = 2000

CSV_INDEX_DATE              = 0          # the date
CSV_INDEX_MOUNTS            = 1        # number of mounts
CSV_INDEX_LOADS             = 2         # number of loads
CSV_INDEX_UNLOADS           = 3       # number of unloads
# 4 unused
CSV_INDEX_LOAD_LAT_MIN      = 5  # minimal load latency
CSV_INDEX_LOAD_LAT_MEAN     = 6 # mean load latency
CSV_INDEX_LOAD_LAT_MAX      = 7 # maximal load latency
# 8 unused
CSV_INDEX_UNLOAD_LAT_MIN    = 9 # minimal unload latency
CSV_INDEX_UNLOAD_LAT_MEAN   = 10 # mean unload latency
CSV_INDEX_UNLOAD_LAT_MAX    = 11 # max  unload latency
CSV_INDEX_SUM_LOADLAT       = 12


def plot_validity(vals, path):
    order = ['realsystem_analysis','DD60','DD120']
    for i in ['load_complete','latency_unmount','latency_mount']:
        fig, ax = plt.subplots()
        maxy = 0
        miny = sys.maxint
        for o in order:
            #print i,o, vals.keys()
            try:
                if o in vals:
                    x =[]
                    y =[]
                    #print vals[o]['monthly'].keys()
                    for k,v in sorted(vals[o]['monthly'].items()):
                        try:
                            #print v.keys()
                            if k != "201405":
                                    dt = datetime.datetime(year=int(k[0:4]), month=int(k[4:6]), day=1)
                                    y.append(numpy.mean(v[i]))
                                    x.append(dt)
                        except KeyError, e:
                            print e
                        except ValueError, e:
                            print e
                            #pass

                    #o2 = nametranslation(o, False)
                    (l,m,c, ls) = get_visual_config(o.split('/')[-1])
                    ax.plot(x,y, label=l, color=c, marker=m, markersize=MARKERSIZE, linestyle=ls)
                    maxy = max(maxy,max( y))
                    miny = min(miny,min( y))
                    #print y, maxy
            except:
                raise
        ax.grid(True)
        ax.xaxis.set_major_locator(yearlocator)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.xaxis.set_minor_locator(month)
        (title,ylab,xlab) = get_label(i)
        ax.set_ylim(0,maxy)
        plt.legend(loc=4)
        fig.set_size_inches(10,5)

        plt.ylabel(ylab)
        fig.set_dpi(80)
        plt.savefig(os.path.join(path,'monthly2_%s.pdf'%(i)))
        plt.close('all')




def __barplot(x, y, title, ylabel, path, yerr=None):
    width = 0.5
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ind = numpy.arange(len(x))
    ax.bar(ind,y , width , zorder=3, yerr=yerr, color='0.6')
    ax.grid(True)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(ind + width/2., x)
    fig.set_size_inches(20,12)
    fig.set_dpi(80)
    plt.savefig(os.path.join(path,'total_%s.pdf'%(ylabel)))

def get_label(feature):
    t,y,x = "","",""
    if feature == "mount":
        y = "Number of Cartridge Mount Operations"
        t = "mount operations"
    if feature == "load_complete":
        y = "Number of Cartridge Load Operations"
        t = "cartridge load operations"
    if feature == "latency_unmount_mean":
        y = "Mean Unload Latency in Seconds"
        t = "mean unload latency"
    if feature == "latency_unmount":
        y = "Mean Unload Latency in Seconds"
        t = "mean unload latency"
    if feature == "latency_mount_mean":
        y = "Mean Load Latency in Seconds"
        t = "mean load latency"
    if feature == "latency_mount":
        y = "Mean Load Latency in Seconds"
        t = "mean load latency"
    if feature == "unload_complete":
        y = "Number of Unload Operations"
        t = "unload operations"
    if feature == "sumlatency_days":
        y = "Total Load Latency in Days"
        t = "total Load Latency"

    return (t,y,x)

def plot_bar2(vals, path, defa=DEF_A):
    order = ['DD_opt0_par0','DD_opt0_par120','DD_opt0_par300','DD_opt0_par900','DD_opt0_par1800','FIFO_opt0', 'LRU_opt0', 'BELADY_opt0']
    for i in ['mount','load_complete','latency_unmount_mean','latency_mount_mean','unload_complete', 'sumlatency_days']:
        #x,y, yerr = [],[],[]
        #for o in order:
        #    if o in vals['total']:
        #        x.append(o.split('/')[-1])
        #        y.append(vals['total'][o][i])
        #        if i.endswith("mean"):
        #            yerr.append(vals['total'][o]["%sstd"%i])
        #if len(yerr) == len(y) and 0:
        #    __barplot(x,y, "%s: 20120101 - 20140430"%i, i, path=path, yerr=yerr)
        #else:
        #    __barplot(x,y, "%s: 20120101 - 20140430"%i, i, path=path)
        fig, ax = plt.subplots()
        maxy = 0
        miny = sys.maxint
        for o in order:
            print i,o
            try:
                #print vals['monthly'].keys()
                x =[]
                y =[]
                for k,v in sorted(vals['monthly'].items()):
                    #print v
                    try:
                        if k != "201405":
                                #int(v[o][i])
                                dt = datetime.datetime(year=int(k[0:4]), month=int(k[4:6]), day=1)
                                y.append(v["experiment99/%s"%o][i])
                                x.append(dt)
                    except KeyError, e:
                        print e
                    except ValueError, e:
                        print e
                        #pass

                name = nametranslation(o, False)
                #(l,m,c, ls) = get_visual_config(o.split('/')[-1])
                (l,m,c, ls) = get_visual_config(name)
                print name
                ax.plot(x,y, label=l, color=c, marker=m, markersize=MARKERSIZE, linestyle=ls)
                maxy = max(maxy,max( y))
                miny = min(miny,min( y))
                #print y, maxy
            except:
                raise
        #print maxy
        #max101 = int(maxy / 100.0 *101)
        #min99 = int()

        ax.grid(True)
        ax.xaxis.set_major_locator(yearlocator)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.xaxis.set_minor_locator(month)
        (title,ylab,xlab) = get_label(i)
        if defa:
            box = ax.get_position()
            ax.set_position([box.x0*1.2, box.y0 , box.width, box.height*0.8])
            plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3, borderaxespad=0., ncol=3,  mode="expand")
            miny = (int(miny)/10)*10
            if miny >= 40:
                miny =0
            ax.set_ylim(miny,maxy)

            fig.set_size_inches(10,7)
        else:
            ax.set_ylim(0,maxy)
            plt.legend(loc=4)
            fig.set_size_inches(10,5)

        plt.ylabel(ylab)
        #plt.legend()
        #plt.legend(bbox_to_anchor=(0., 1.05, 1., .105), loc=3, ncol=2, mode="expand", borderaxespad=0.)
        #plt.show()
        fig.set_dpi(80)
        plt.savefig(os.path.join(path,'monthly_%s.pdf'%(i)))
        plt.close('all')


def plot_bar(vals, path, defa=DEF_A):
    #order = DEFAULTX
    order = ['realsystem_analysis','DD_opt0_par60','DD_opt0_par120']

    for i in ['mount','load_complete','latency_unmount_mean','latency_mount_mean','unload_complete', 'sumlatency_days']:
        #x,y, yerr = [],[],[]
        #for o in order:
        #    if o in vals['total']:
        #        x.append(o.split('/')[-1])
        #        y.append(vals['total'][o][i])
        #        if i.endswith("mean"):
        #            yerr.append(vals['total'][o]["%sstd"%i])
        #if len(yerr) == len(y) and 0:
        #    __barplot(x,y, "%s: 20120101 - 20140430"%i, i, path=path, yerr=yerr)
        #else:
        #    __barplot(x,y, "%s: 20120101 - 20140430"%i, i, path=path)
        fig, ax = plt.subplots()
        maxy = 0
        miny = sys.maxint
        for o in order:
        #    print i,o
            try:
                if o in vals['total']:
                    x =[]
                    y =[]
                    for k,v in sorted(vals['monthly'].items()):
                        try:
                            if k != "201405":
                                    int(v[o][i])
                                    dt = datetime.datetime(year=int(k[0:4]), month=int(k[4:6]), day=1)
                                    y.append(v[o][i])
                                    x.append(dt)
                        except KeyError, e:
                            print e
                        except ValueError, e:
                            print e
                            #pass

                    (l,m,c, ls) = get_visual_config(o.split('/')[-1])
                    ax.plot(x,y, label=l, color=c, marker=m, markersize=MARKERSIZE, linestyle=ls)
                    maxy = max(maxy,max( y))
                    miny = min(miny,min( y))
                    #print y, maxy
            except:
                raise
        #print maxy
        #max101 = int(maxy / 100.0 *101)
        #min99 = int()

        ax.grid(True)
        ax.xaxis.set_major_locator(yearlocator)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.xaxis.set_minor_locator(month)
        (title,ylab,xlab) = get_label(i)
        if defa:
            box = ax.get_position()
            ax.set_position([box.x0*1.2, box.y0 , box.width, box.height*0.8])
            plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3, borderaxespad=0., ncol=3,  mode="expand")
            miny = (int(miny)/10)*10
            if miny >= 40:
                miny =0
            ax.set_ylim(miny,maxy)

            fig.set_size_inches(10,7)
        else:
            ax.set_ylim(0,maxy)
            plt.legend(loc=4)
            fig.set_size_inches(10,5)

        plt.ylabel(ylab)
        #plt.legend()
        #plt.legend(bbox_to_anchor=(0., 1.05, 1., .105), loc=3, ncol=2, mode="expand", borderaxespad=0.)
        #plt.show()
        fig.set_dpi(80)
        plt.savefig(os.path.join(path,'monthly_%s.pdf'%(i)))
        plt.close('all')


def plot_dd_lines(vals, path):
    def cb_plot_dd_lines(x, y, path):
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ind = numpy.array(x)
        ax.scatter(ind,y , zorder=3, color='0.0')
        ax.grid(True)
        ax.set_ylim(0, 3000)
        ax.set_xlim(0, max(x))

        fig.set_size_inches(10,5)
        fig.set_dpi(80)

        plt.gcf().subplots_adjust(bottom=0.15)
        plt.ylabel("Total Load Latency in Days")
        plt.xlabel("Defer Dismount Parameter in Seconds")
        plt.savefig(os.path.join(path,'dd_parameter_scatter.pdf'))

    pat = re.compile("DD(?P<dd>[0-9]+)")
    tmp = {}
    tmp2 = {}
    for i in vals['total'].keys():
        try:
            m = pat.match(i)
            if m:
                tmp[int(m.group('dd'))] = vals['total'][i]
    #            tmp2[int(m.group('dd'))] = int(vals['maximal_load_latency'][i])
        except:
            pass
            #raise
    #print tmp,tmp2
    x,y = [],[]
    for i in sorted(tmp.keys()):
        print tmp[i], i
        x.append(int(i))
        y.append(tmp[i])

    cb_plot_dd_lines(x,y, path)



def remove_0_100_percentile(sample):
    return  sample
    length = len(sample)
    oneperc = int(length/100.0)
    return sample[oneperc:oneperc*99]

def remove_outlier(sample):
    return sample

    if len(sample)>10:
        return sample[5:-5]
    return sample



def listdirs(datadir):
    bd = os.path.join(os.getcwd(), datadir)
    res = []
    res.append(os.path.join(bd,'realsystem_analysis'))
    for i in ['experiment99','experiment1','experiment2']:
        #break
        d = os.path.join(bd,i)
        if os.path.isdir(d):
            for eviction in os.listdir(d):
                d2 = os.path.join(d,eviction)
                if os.path.isdir(d2):
                    res.append(d2)
    return res

def parse_line(line):
    try:
        if line[CSV_INDEX_MOUNTS] == "mounts":
            return {} ## skip header
        x = {
            'mounts':int(line[CSV_INDEX_MOUNTS]),
            'loads':int(line[CSV_INDEX_LOADS]),
            'unloads':int(line[CSV_INDEX_UNLOADS]),
            'loadlat_mean':float(line[CSV_INDEX_LOAD_LAT_MEAN]),
            'unloadlat_mean':float(line[CSV_INDEX_UNLOAD_LAT_MEAN]),
            'date':datetime.datetime.strptime(line[CSV_INDEX_DATE], "%Y%m%dT%H")
        }
        if len(line)>CSV_INDEX_SUM_LOADLAT:
            try:
                x['sumLL']= int(line[CSV_INDEX_SUM_LOADLAT])
            except:
                pass
        return x
    except ValueError:
        raise 
        #return {}
    except:
        raise
        #return {}

# # # # # # # # # # # #  END HELPER # # # # # # # # # # # #
# # # # # # # # # START VISUAL # # # # # # # # # # ## # # #

def run_visual(args):
    bd = os.path.join(os.getcwd(), args.rootdir)
    start = datetime.datetime.strptime(args.start, "%Y%m%d")
    end = datetime.datetime.strptime(args.end, "%Y%m%d")
    vis = Visualize(bd, start, end)
    if args.quick:
        vis.quick_quality_comparison(args.quick)
    elif args.cdf:
        vis.draw_cdf()
    elif args.opt:
        vis.draw_opt()
    elif args.compare:
        vis.compare()
    elif args.tex:
        vis.gen_tex()
    elif args.hotcold:
        vis.gen_hotcold()
    elif args.drives:
        vis.gen_drivescomp()
    elif args.all:
        vis.all()
    elif args.dd:
        vis.gen_sim_dd()

class Visualize(Base):
    """
    Basic usage examine.py visual --rootdir <path-to-results> --start YYYYMMDD --end YYYYMMDD
    Only displays what has been analysed with 'stats' previously  """
    def __init__(self, datadir, start, end):
        Base.__init__(self,datadir, start, end)
        self.figures = os.path.join(self.rootdir, 'figures')
        if not os.path.isdir(self.figures ):
            os.makedirs(self.figures )

    def all(self):
        #self.run_alpha()
        #self.draw_cdf()
        #self.gen_eval_dd()
        self.comparison_rawjson()
        #self.gen_tex()

    def read_system_lbjson(self, experiment, eviction):
        target = os.path.join(self.rootdir, experiment, eviction, "data/system_summary.lbjson")
        if os.path.isfile(target):
            pass

    def read_rawjson(self, experiment, eviction):
        target = os.path.join(self.rootdir, experiment, eviction, "data/results/rawoutput.json")
        if eviction == "realsystem_analysis":
            target = os.path.join(self.rootdir, eviction, "data/results/rawoutput.json")
        if os.path.isfile(target):
            res={
                'total':{
                    'latency_unmount':[],
                    'latency_mount':[],
                    'mount':0,
                    'load_complete':0,
                    'unload_complete':0
                },
                'monthly':{},
                'maximal_load_latency':0}

            features_listtype = ['latency_unmount','latency_mount']
            features_inttype = ['unload_complete','mount', 'load_complete']

            with open(target,'r') as fp:
                maxlatency = 0
                rawjson = json.load(fp)
                for k,v in rawjson['results'].iteritems():
                    dt = datetime.datetime(year=int(k[0:4]), month=int(k[4:6]), day=1)
                    if dt < self.start or dt > self.end:
                        continue
                    # # master results
                    if 'latency_mount' in v:
                        maxlatency = max(maxlatency, max(v['latency_mount']))
                    yyyymm = k[0:6]
                    if yyyymm not in res['monthly']:
                        res['monthly'][yyyymm]={}
                        for opt in  features_inttype:
                            res['monthly'][yyyymm][opt]=0
                        for opt in  features_listtype:
                            res['monthly'][yyyymm][opt]=[]
                    for opt in features_listtype:
                        if opt in v:
                            res['total'][opt].extend(v[opt])
                            res['monthly'][yyyymm][opt].extend(v[opt])
                    for opt in features_inttype:
                        if opt in v:
                            res['total'][opt] += v[opt]
                            res['monthly'][yyyymm][opt] += v[opt]
                res['maximal_load_latency']=maxlatency
                #print "reading file %s, maximal latency:%s"%(target,maxlatency)
            return res
        return None

    def draw_cdf(self):
        order = [ 'realsystem_analysis', 'DD_opt0_par60','DD_opt0_par120']
        order2 = ['realsystem_analysis' , 'DD60','DD120']
        master_stats ={}
        for key in order:
            key2 = 'realsystem_analysis'
            if key == 'DD_opt0_par60':
                key2 = 'DD60'
            elif key == 'DD_opt0_par120':
                key2 = 'DD120'
            master_stats[key2] = self.parse_raw_jason(key)

        loadlat = {}
        for ex in order2:
            loadlat[ex]={}
            lats = master_stats[ex]['total']['latency_mount']
            lats.sort()
            for i in range(0,101):
                perc = float(i)*0.01
                loadlat[ex][perc] = tiny_helper.percentile(lats, perc)

        xlabel = "Load Latency in Seconds"
        ylabel = "Percentage of Requests"
        title = "1. Jan. 2013 to 20. May 2014: Load Latency"
        cdf_plot(loadlat, title, xlabel, ylabel, order2)

        unloadlat = {}
        for ex in order2:
            unloadlat[ex]={}
            unlats = master_stats[ex]['total']['latency_unmount']
            unlats.sort()
            for i in range(0,101):
                perc = float(i)*0.01
                unloadlat[ex][perc] = tiny_helper.percentile(unlats, perc)
        xlabel = "Unload Latency in Seconds"
        cdf_plot(unloadlat, "1. Jan. 2013 to 20. May 2014: Unload Latency", xlabel, ylabel , order2)

    def gen_eval_dd(self):
        exp = "experiment99"
        master = {}
        order= ['realsystem_analysis',"DD_opt0_par0",  "DD_opt0_par60", "DD_opt0_par120", "DD_opt0_par180", 'DD_opt0_par300', "DD_opt0_par600", "DD_opt0_par900",  "DD_opt0_par1200","DD_opt0_par1800" ]
        for ev in order:
            r = self.read_rawjson(exp, ev)
            if r:
                master[nametranslation(ev,False)]=r
            else:
                "%s not parsed correctly"%ev
        vals = {'total':{}, 'monthly':{}}
        for k,v in master.iteritems():
            if k != 'realsystem_analysis':
                vals['total'][k] = sum(v['total']['latency_mount'])/(3600.0*24)

        plot_dd_lines(vals,TGT)
        plot_validity(master, TGT)

    def gen_sim_dd(self):
        exp = "experiment99"
        master = {}
        order= ['realsystem_analysis',"DD_opt0_par60", "DD_opt0_par120"]
        for ev in order:
            r = self.read_rawjson(exp, ev)
            if r:
                master[nametranslation(ev,False)]=r
            else:
                "%s not parsed correctly"%ev
        #vals = {'total':{}, 'monthly':{}}
        #for k,v in master.iteritems():
        #    if k != 'realsystem_analysis':
        #        vals['total'][k] = sum(v['total']['latency_mount'])/(3600.0*24)
        plot_validity(master, TGT)

    def compare(self):
        exps = ["experiment99","experiment1","experiment2"]
#        master = {}
        for ex in exps:
            p = os.path.join(self.rootdir,ex)
            #print p
            if os.path.isdir(p):
                for ev in sorted(os.listdir(p)):
                    r = self.read_rawjson(ex,ev)
                    if r:
                        print ex,ev, "total latency", sum(r['total']['latency_mount'])/(3600.0*24), "#loads", r['total']['load_complete']
            print "------------------------"

    def gen_tex(self):
        """ :return: new header
        \multirow{3}{*}{Strategy} & \multirow{3}{*}{\begin{tabular}{c@{}}Total \\\#Loads  \end{tabular}} & \multirow{3}{*}{\begin{tabular}{@{}c@{}c@{}}Total \\ Latency \\ in Days \end{tabular}}   & \multicolumn{3}{|c|}{Load Latency in Seconds} \\
\cline{4-6}
 & & & \multirow{2}{*}{Min} & \multirow{2}{*}{Mean $\pm$ Std}  & \multirow{2}{*}{Max} \\
 & & & & & \\
        """

        with open(os.path.join(TGT,"tableeval1.tex"), 'w') as fp:
            s = """\\begin{table}\\centering \\small
\\begin{tabular}{|c|r|r|r|r|r|}
\\hline
\\multirow{2}{*}{Strategy} &  \multicolumn{1}{c}{Total} &  \multicolumn{1}{c}{Total}  & \\multicolumn{3}{|c|}{Load Latency in Seconds} \\\\ \n
 & \\#Loads & Latency & Min & Mean $\\pm$ Std  & Max \\\\
 & & in Days & & & \\\\
\\hline
"""
            fp.write(s)

            order = ["DD_opt0_par0","DD_opt0_par60","DD_opt0_par120","DD_opt0_par300","DD_opt0_par600","DD_opt0_par900", "DD_opt0_par1800", "FIFO_opt0", "LRU_opt0", "BELADY_opt0"]
            exp = "experiment99"
            for ev2 in order:
                r = self.read_rawjson(exp, ev2)
                if r:
                    ev = nametranslation(ev2, False)
                    if ev in ['FIFO', 'BELADY']:
                        fp.write("\n \\hline \n")
                    mean = numpy.mean(r['total']['latency_mount'])
                    std = numpy.std(r['total']['latency_mount'])
                    _min_ = min(r['total']['latency_mount'])
                    _max_ = max(r['total']['latency_mount'])
                    _sum_ = sum(r['total']['latency_mount'])/(3600.0*24)
                    fp.write("""%s & %i & %2.1f &  %i &  %2.1f $\\pm$ %2.1f &  %i \\\\ \n"""%(
                        ev, r['total']['load_complete'],_sum_, _min_, mean,std, _max_ ))
            foot = """\\hline\n\\end{tabular}
\\caption{Results of the eviction strategies.}
\\label{tab:eval_evict1}
\\end{table}"""
            fp.write(foot)

    def gen_hotcold(self):
        res = {}
        oldkey = "none"
        with open(os.path.join(TGT,"tableevalhc.tex"), 'w') as fp:
            s = """\\begin{table}\\centering \\small
\\begin{tabular}{|c|r|r|r|r|r|}
\\hline
\\multirow{2}{*}{Strategy} &  \multicolumn{1}{c}{Total} &  \multicolumn{1}{c}{Total}  & \\multicolumn{3}{|c|}{Load Latency in Seconds} \\\\ \n
 & \\#Loads & Latency & Min & Mean $\\pm$ Std  & Max \\\\
 & & in Days & & & \\\\
\\hline
"""
            fp.write(s)
            ex = "experiment99"
            order = ['DD_opt0_par300','DD_opt1_par300','DD_opt4_par300', 'DD_opt0_par900', 'DD_opt1_par900', 'DD_opt4_par900','DD_opt0_par1800', 'DD_opt1_par1800', 'DD_opt4_par1800', 'LRU_opt0','LRU_opt1','LRU_opt4']
            for ev in order:
                r = self.read_rawjson(ex,ev)
                if r:
                    res[ev] = {}
                    res[ev]['totallatency'] = sum(r['total']['latency_mount'])/(3600.0*24)
                    res[ev]['totalloads'] = r['total']['load_complete']
                    res[ev]['meanlatency'] = numpy.mean(r['total']['latency_mount'])
                    res[ev]['stdlatency'] = numpy.std(r['total']['latency_mount'])
                    res[ev]['minlatency'] = min(r['total']['latency_mount'])
                    res[ev]['maxlatency'] = max(r['total']['latency_mount'])
                else:
                    print ev, "not found"
            #print res
                    #print ex,ev, "total latency", sum(r['total']['latency_mount'])/(3600.0*24), "#loads", r['total']['load_complete']
            fig = plt.figure()
            ax = fig.add_subplot(111)
            ax.grid(zorder=0)

            for k in order:
                v = res[k]
                #y = v['meanlatency']
                x = v['totalloads']/1000000.0
                y = v['totallatency']
                xl = x+0.01
                yl = y
                ax.scatter(x,y, s=130 ,color='0.0', zorder=3)
                if k == "DD_opt0_par900":
                    k2 = "DD900_0"
                elif k == "DD_opt1_par900":
                    k2 = "DD900_1"
                elif k == "DD_opt4_par900":
                    k2 = "DD900_2"
                elif k == "DD_opt0_par300":
                    k2 = "DD300_0"
                    xl -= 0.30
                    yl += 15
                elif k == "DD_opt1_par300":
                   k2 = "DD300_1"
                   xl -= 0.30
                   yl += 15
                elif k == "DD_opt4_par300":
                    k2 = "DD300_2"
                    yl -= 45
                    xl -= 0.25
                elif k == "DD_opt0_par1800":
                    k2="DD1800_0"
                    yl += 15
                    #xl -= 0.16
                elif k == "DD_opt1_par1800":
                    k2="DD1800_1"
                    #xl -= 0.15
                    yl += 10
                elif k == "DD_opt4_par1800":
                    k2="DD1800_2"
                    #xl -= 0.3
                    #yl -= 15
                elif k=="LRU_opt0":
                    k2="LRU_0"
                    yl += 5
                elif k=="LRU_opt1":
                    k2="LRU_1"
                    yl-=50
                    xl -= 0.12
                elif k=="LRU_opt4":
                    k2="LRU_2"
                    yl-=35
                    
                else:
                    raise "key not found"

                if k2=="DD1800_0":
                    label1 = r"$%s$"%k2#, v['meanlatency'], v['stdlatency'])
                    #label2 = r"$(%2.1f\pm%2.1f$)"%(v['meanlatency'], v['stdlatency'])
                    ax.annotate(label1, xy = (x, y), xytext = (xl, yl) )
                    #ax.annotate(label2, xy = (x, y), xytext = (xl, yl-40) )
                else:
                    #label = r"$%s(%2.1f\pm%2.1f$)"%(k2, v['meanlatency'], v['stdlatency'])
                    label = r"$%s$"%(k2)
                    ax.annotate(label, xy = (x, y), xytext = (xl, yl) )
                if k2[:3]!=oldkey[:3]:
                    fp.write("\n \\hline \n")
                oldkey = k2
                fp.write("""$%s$ & %i & %2.1f &  %i & %2.1f $\\pm$ %2.1f &  %i \\\\ \n"""%(
                        k2, v['totalloads'],v['totallatency'], v['minlatency'], v['meanlatency'],v['stdlatency'], v['maxlatency'] ))


            ax.set_ylabel('Total Load Latency in Days')
            ax.set_xlabel("Total Number of Load Operations in Million")

            #plt.show()
            #plt.tight_layout()
            #p = os.path.join(self.figures,"evalhotcold1.pdf")
            p = os.path.join(TGT,"evalhotcold1.pdf")
            plt.savefig(p)

            foot = """\\hline\n\\end{tabular}
\\caption{Results of hot/cold classification experiments.}
\\label{tab:eval_hotcold1}
\\end{table}"""
            fp.write(foot)

    def gen_drivescomp(self):
        exps = ["experiment1","experiment2"]
        master = {}
        for ex in exps:
            master[ex] = {}
            p = os.path.join(self.rootdir,ex)
            if os.path.isdir(p):
                for ev in sorted(os.listdir(p)):
                    r = self.read_rawjson(ex,ev)
                    if r:
                        master[ex][ev] = {}
                        master[ex][ev]['totallatency'] = sum(r['total']['latency_mount'])/(3600.0*24)
                        master[ex][ev]['totalloads'] = r['total']['load_complete']
                        master[ex][ev]['meanlatency'] = numpy.mean(r['total']['latency_mount'])
                        master[ex][ev]['stdlatency'] = numpy.std(r['total']['latency_mount'])
                        master[ex][ev]['minlatency'] = min(r['total']['latency_mount'])
                        master[ex][ev]['maxlatency'] = max(r['total']['latency_mount'])


                        print ex,ev, "total latency", sum(r['total']['latency_mount'])/(3600.0*24), "#loads", r['total']['load_complete']
            print "------------------------"

        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.grid(zorder=0)
        oldkey = "none"
        for ex, d in master.iteritems():
            caption = "Results of the configuration $C_1$ with twelve drives per LSM."
            marker = 'o'
            if ex=="experiment2":
                marker='*'
                caption = "Results of the configuration $C_2$ with 16 drives per LSM."
            with open(os.path.join("/tmp","tableevaldries%s.tex"%ex), 'w') as fp:
                fp.write("""\\begin{table}\\centering \\small
\\begin{tabular}{|c|r|r|r|r|r|}
\\hline
\\multirow{2}{*}{Strategy} &  \multicolumn{1}{c}{Total} &  \multicolumn{1}{c}{Total}  & \\multicolumn{3}{|c|}{Load Latency in Seconds} \\\\ \n
 & \\#Loads & Latency & Min & Mean $\\pm$ Std  & Max \\\\
   & & in Days & & & \\\\
\\hline
""")
                for ev in ['DD_opt0_par300', 'DD_opt1_par300','DD_opt4_par300','DD_opt0_par900','DD_opt1_par900','DD_opt4_par900','LRU_opt0','LRU_opt1','LRU_opt4']:
                    try:
                        name = nametranslation(ev)
                        if name[:3]!=oldkey[:3]:
                            fp.write("\n \\hline \n")
                        oldkey = name

                        res = d[ev]
                        fp.write("""$%s$ & %i & %2.1f &  %i &  %2.1f $\\pm$ %2.1f &  %i \\\\ \n"""%(
                                name, res['totalloads'],res['totallatency'], res['minlatency'], res['meanlatency'],res['stdlatency'],res['maxlatency'] ))

                        x = res['totalloads']/1000000.0
                        y = res['totallatency']
                        xl = x+0
                        yl = y+5

                        if name == "DD300_0" and marker=="*":
                            xl -= 0.44
                        if name == "DD300_1" and marker=="*":
                            xl -= 0.44
                            #yl += 10
                        if name == "DD300_2" and marker=="o":
                            yl -= 50
                            xl += 0.02
                        if name == "DD300_2" and marker=="*":
                            yl -= 50
                            xl += 0.02
                        if name == "DD900_2" and marker=="o":
                            #yl -= 50
                            xl += 0.02
                        if name == "LRU_0" and marker=="o":
                            xl -= 0.3
                        if name == "LRU_1" and marker=="o":
                            xl -= 0.3
                        if name == "LRU_2" and marker=="o":
                            xl -= 0.3

                        l = ax.scatter(x,y, s=130 ,color='0.0', zorder=3, marker=marker)
                        if name == "LRU_0":
                            if marker=="o":
                                l1 = l
                            elif marker=='*':
                                l2 = l

                        label = r"$%s$"%(name)
                        ax.annotate(label, xy = (x, y), xytext = (xl, yl) )
                    except:
                        pass
                foot = """\\hline\n\\end{tabular}
\\caption{%s}
\\label{tab:eval_drives_%s}
\\end{table}"""%(caption,ex)
                fp.write(foot)

        ax.set_ylabel('Total Load Latency in Days')
        ax.set_xlabel("Total Number of Load Operations in Million")
        ax.set_xlim(2.0, 5.0)
        #plt.show()
        #plt.tight_layout()
        lines = ["16x12 drives", '16x16 drives']
        plt.legend((l1,l2), lines, loc=4)


        p = os.path.join(TGT,"evalhotdrives.pdf")
        plt.savefig(p)


    def draw_opt(self):
        master_stats ={}
        for key in ['DD_opt1_par900','DD_opt0_par900','DD_opt4_par900']:
            master_stats[key] = self.parse_raw_jason(key)
        print master_stats

    def run_alpha(self):
        res = self.run_output()
        reference_key = "realsystem_analysis"
        reference = res[reference_key]
        del res[reference_key]
        #self.plot(res, 'mounts', 'number of mounts',reference_key,reference)
        #self.plot(res, 'loads', 'number of loads',reference_key,reference)
        #self.plot(res, 'unloads', 'number of unloads',reference_key,reference)
        self.plot(res, 'loadlat_mean', 'mean load latency in seconds',reference_key,reference)
        self.plot(res, 'unloadlat_mean', 'mean unload latency in seconds',reference_key,reference)
        #self.plot(res, 'sumLL', 'mean unload latency in seconds',reference_key,reference)
        # # merge pdfs
        #os.chdir(self.figures)
        #pp = ('mounts', 'loads', 'unloads','loadlat_mean','unloadlat_mean')
        #pp.append('sumLL')
        #for i in pp:
        #    os.system("pdfunite %s* merged_%s.pdf"%(i,i))

        #res = self.run_percentile()
        #reference_key = "realsystem_analysis"
        #reference = res[reference_key]
        #del res[reference_key]
        #self.cdf(res, 'loadlat',reference_key,reference, "Load latency in seconds")
        #self.cdf(res, 'unloadlat',reference_key,reference, "Unload latency in seconds" )
        #self.cdf2(res, 'loadlat',reference_key,reference, "Load latency in seconds", "Summed up requests in %" )
        #self.cdf2(res, 'unloadlat',reference_key,reference, "Unload latency in seconds", "Summed up requests in %" )

        for i in ('cdf_loadlat', 'cdf_unloadlat'):
            os.system("pdfunite %s* merged_%s.pdf"%(i,i))
        del res
        self.comparison_rawjson()

    def run_output(self):
        res = {}
        for i in sorted(listdirs(self.rootdir)):
            label = i[len(self.rootdir)+1:]
            res[label]={}
            datadir = os.path.join(i,'data')
            if os.path.isdir(datadir):
                if 'results' in os.listdir(datadir):
                    outp = os.path.join(datadir,'results','output.csv')
                    if os.path.isfile(outp):
                        with open(outp,'r') as fp:
                            #raw_input("contunie")
                            sum_loads = 0
                            sum_load_latency = 0
                            spamreader = csv.reader(fp, delimiter=';')
                            for line in spamreader:
                                vals = parse_line(line)
                                if 'date' in vals:
                                    if self.start <= vals['date'] and self.end >= vals['date']:
                                        res[label][vals['date']] = vals
                                        sum_loads += vals['loads']
                                        #print vals
                                        if 'sumLL' in vals:
                                            sum_load_latency += vals['sumLL']
                                        else:
                                            sum_load_latency += vals['loads']*vals['loadlat_mean']
                            print "Total Loads %s; Load latency in days: %s; %s"%(sum_loads,float(sum_load_latency)/(24*3600.0), outp)
        return res

    def run_percentile(self):
        res = {}
        for i in listdirs(self.rootdir):
            label = i[len(self.rootdir)+1:]
            res[label]={}
            datadir = os.path.join(i,'data')
            if 'results' in os.listdir(datadir):
                outp = os.path.join(datadir,'results','percentiles.csv')
                if os.path.isfile(outp):
                    with open(outp,'r') as fp:
                        spamreader = csv.reader(fp, delimiter=';')
                        for line in spamreader:
                            res[label][line[0]] = (line[1],line[2])
        return res

    def plot(self,res, key, ylabel,reference_key,reference):
        #years    = mdates.YearLocator()   # every year
        #months   = mdates.MonthLocator()  # every month

        for r,obj in res.items():
            fig, ax = plt.subplots()
            x = []
            y = []
            for j in sorted(obj.keys()):
                x.append(j)
                y.append(min(obj[j][key],DEFINE_MAX_Y_VALUE))

            ax.plot(x,y, label=r)

            x = []
            y = []
            for j in sorted(reference.keys()):
                x.append(j)
                y.append(min(reference[j][key],DEFINE_MAX_Y_VALUE))
            ax.plot(x,y, label=reference_key)
            # format the ticks
            ax.xaxis.set_major_locator(month)
            ax.xaxis.set_major_formatter(yearsFmt)
            ax.xaxis.set_minor_locator(weeks)

            ax.grid(True)
            box = ax.get_position()
            ax.set_xlim(self.start, self.end)

            ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
            #fig.autofmt_xdate()
            # rotates and right aligns the x labels, and moves the bottom of the
            # axes up to make room for them
            plt.title("%s: %s - %s"%(key,self.start,self.end))
            plt.ylabel(ylabel)

            plt.legend(bbox_to_anchor=(1.05,1), loc=2, prop={'size':10})
            label = string.replace(r,'/','-')
            fig.set_size_inches(10,6)
            fig.set_dpi(80)
            plt.savefig(os.path.join(self.figures, '%s_%s.pdf'%(key,label)))
            plt.close('all')

    def cdf(self, res, key, reference_key,reference, ylabel):
        index = 0
        maxy = 0
        if key == "unloadlat":
            index = 1
        for r,obj in res.items():
            fig, ax = plt.subplots()
            x = []
            y = []
            for j in sorted(reference.keys()):
                if j == "Percentile":continue
                if float(j) > 0.98: continue
                x.append(j)
                yval = reference[j][index]
                maxy = max(maxy,yval)
                y.append(yval)
            (l,m,c, ls) = get_visual_config(reference_key.split('/')[-1])
            ax.plot(y,x, label=l, color=c,linestyle=ls, linewidth=2.0)
            #ax.plot(y,x, label=reference_key.split('/')[-1])

            x = []
            y = []
            for j in sorted(obj.keys()):
                if j == "Percentile":continue
                if float(j) > 0.98: continue
                x.append(j)
                yval = obj[j][index]
                maxy = max(maxy,yval)
                y.append(yval)
            (l,m,c, ls) = get_visual_config(r.split('/')[-1])
            ax.plot(y,x, label=l, color=c, linestyle=ls, linewidth=3.0)

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
            #plt.title("CDF of %s %s"%(key, r.split('/')[-1]))
            plt.xlabel(ylabel)
            plt.ylim(ymin=0)

            plt.legend(loc=4, prop={'size':14})
            #plt.legend(bbox_to_anchor=(1.05,1), loc=4, prop={'size':14})
            label = string.replace(r,'/','-')
            fig.set_size_inches(12,8)
            fig.set_dpi(80)
            plt.savefig(os.path.join(self.figures, 'cdf_%s_%s.pdf'%(key,label)))

    def cdf2(self, res, key, reference_key,reference, ylabel, realylabel):
        index = 0
        maxy = 0
        if key == "unloadlat":
            index = 1
        fig, ax = plt.subplots()
        x = []
        y = []
        for j in sorted(reference.keys()):
            if j == "Percentile":continue
            if float(j) > 0.98: continue
            x.append(float(j)*100)
            yval = reference[j][index]
            maxy = max(maxy,yval)
            y.append(yval)
        (l,m,c, ls) = get_visual_config(reference_key.split('/')[-1])
        ax.plot(y,x, label=l, color=c,linestyle=ls, linewidth=2.0)
        #ax.plot(y,x, label=reference_key.split('/')[-1])

        for r,obj in res.items():
            x = []
            y = []
            for j in sorted(obj.keys()):
                if j == "Percentile":continue
                if float(j) > 0.98: continue
                if (float(j)*100)%2: continue

                x.append(float(j)*100)
                yval = obj[j][index]
                maxy = max(maxy,yval)
                y.append(yval)

            (l,m,c, ls) = get_visual_config(r.split('/')[-1])
            if l in ['ECMWF', "DD60", "DD300"]:
                ax.plot(y,x, label=l, color=c, linestyle=ls, linewidth=2.0, marker=m, markersize=MARKERSIZE*0.7)
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
        #plt.title("CDF of %s %s"%(key, r.split('/')[-1]))
        plt.xlabel(ylabel)
        plt.ylabel(realylabel)
        plt.ylim(ymin=0)

        plt.legend(loc=4, prop={'size':12})
        #plt.legend(bbox_to_anchor=(1.05,1), loc=4, prop={'size':14})
        label = string.replace(r,'/','-')
        fig.set_size_inches(12,5)
        fig.set_dpi(80)
        plt.savefig(os.path.join(self.figures, 'cdf_all_%s.pdf'%(key)))

    def quick_quality_comparison(self, interval):
        isinstance_ref = isinstance
        atts = {
            'latency_mount':interval,
            'latency_unmount':interval,
            'load_complete':interval
        }
        res = {}
        for i in listdirs(self.rootdir):
            datadir = os.path.join(i,'data')
            name = i.split('/')[-1]
            res[name] = self.process_raw_by_experiment(datadir,atts)

        processed = {}
        for k,v in res.iteritems():
            processed[k]=collections.defaultdict(dict)
            for date, attributes in v.iteritems():
                for att, value in attributes.iteritems():
                    if isinstance_ref(value,list):
                        processed[k][date][att] = numpy.mean(value)
                    elif isinstance_ref(value, int):
                        processed[k][date][att] = value

        if 'realsystem_analysis' in processed:
            realsys_ref = processed['realsystem_analysis']
            print "difference to real system"
            for k,v in processed.iteritems():
                totalloads = 0
                mean_latencymount=[]
                mean_latencyunmount=[]
                mean_loadcomplete = []
                if k=='realsystem_analysis':
                    continue
                for date in sorted(v.keys()):
                    try:
                        mean_latencymount.append(v[date]['latency_mount']-realsys_ref[date]['latency_mount'])
                        mean_latencyunmount.append(v[date]['latency_unmount']-realsys_ref[date]['latency_unmount'])
                        mean_loadcomplete.append(v[date]['load_complete']-realsys_ref[date]['load_complete'])
                        totalloads += v[date]['load_complete']
                    except:
                        pass
                        # print k,date
                #    print "%s: %s"%(date,str(round(diff,2)).rjust(10,' ') )
                print "Mount Latency   %s: %s, +-%s"%( k, round(numpy.mean(mean_latencymount),1), round(numpy.std(mean_latencymount),1))
                print "Unmount Latency %s: %s, +-%s"%( k, round(numpy.mean(mean_latencyunmount),1), round(numpy.std(mean_latencyunmount),1))
                print "Load Complete   %s: %s, +-%s, total loads %s"%( k, round(numpy.mean(mean_loadcomplete),1), round(numpy.std(mean_loadcomplete),1), totalloads)

    def process_raw_by_experiment(self, datadir, attributes):
        isinstance_ref = isinstance
        #res = collections.defaultdict(collections.defaultdict({}))
        res = collections.defaultdict(dict)
        if 'results' in os.listdir(datadir):
            outp = os.path.join(datadir,'results','rawoutput.json')
            if os.path.isfile(outp):
                with open(outp,'r') as fp:
                    rawjson = json.load(fp)
                    results_ref = rawjson['results']
                    filtered = self.filter_raw(results_ref.keys())
                    for k in filtered:
                        for attribute, interval in attributes.iteritems():
                            if attribute in results_ref[k]:
                                reskey = k
                                val = results_ref[k][attribute]
                                if interval=='day':
                                    reskey = k[:8]
                                #if not reskey in res:
                                #    res[reskey] ={}
                                if isinstance_ref(val, list):
                                    if not attribute in res[reskey]:
                                        res[reskey][attribute]=[]
                                    res[reskey][attribute].extend(val)
                                elif isinstance_ref(val, int):
                                    if not attribute in res[reskey]:
                                        res[reskey][attribute]=0
                                    res[reskey][attribute]+=val
                                else:
                                    raise TypeError(type(val))
                            #else:
        return res

    def comparison_rawjson(self):
        res = {}
        master_stats = {}
        experiments = []
        features_listtype = ['latency_unmount','latency_mount']
        features_inttype = ['unload_complete','mount', 'load_complete']

        for i in sorted(listdirs(self.rootdir)):
            label = i[len(self.rootdir)+1:]
            use = DEFAULTX
            if DEF_A:
                use = ['experiment99/DD_opt0_par0', 'experiment99/DD_opt0_par60' ,'experiment99/DD_opt0_par300', "experiment99/DD_opt0_par120", "experiment99/DD_opt0_par180",'experiment99/DD_opt0_par900', "experiment99/DD_opt0_par1800","experiment99/DD_opt0_par1200","experiment99/BELADY_opt0", "experiment99/FIFO_opt0", "experiment99/LRU_opt0"]

            if label not in use:
                    print "Skipping ",label, use
                    continue
                #if label in ['experiment99/DD7200', "experiment99/DD10800", 'realsystem_analysis']:
            #    print "Skipping ",label
            #    continue
            #print label
            master_stats[label]={
                'total':{
                    'latency_unmount':[],
                    'latency_mount':[],
                    'mount':0,
                    'load_complete':0,
                    'unload_complete':0
                },
                'monthly':{},
                'maximal_load_latency':0}
            experiments.append(label)
            datadir = os.path.join(i,'data')
            if os.path.isdir(datadir):
                if 'results' in os.listdir(datadir):
                    master_stats_label_ref = master_stats[label]
                    outp = os.path.join(datadir,'results','rawoutput.json')
                    if os.path.isfile(outp):
                        with open(outp,'r') as fp:
                            maxlatency = 0
                            rawjson = json.load(fp)
                            for k,v in rawjson['results'].iteritems():
                                dt = datetime.datetime(year=int(k[0:4]), month=int(k[4:6]), day=1)
                                if dt < self.start or dt > self.end:
                                    continue
                                if k not in res:
                                    res[k]={}
                                res[k][label] = {}
                                for k2,v2 in v.iteritems():
                                    res[k][label][k2]=v2
                                # # master results
                                if 'latency_mount' in v:
                                    maxlatency = max(maxlatency, max(v['latency_mount']))
                                yyyymm = k[0:6]
                                if yyyymm not in master_stats_label_ref['monthly']:
                                    master_stats_label_ref['monthly'][yyyymm]={}
                                    for opt in  features_inttype:
                                        master_stats_label_ref['monthly'][yyyymm][opt]=0
                                    for opt in  features_listtype:
                                        master_stats_label_ref['monthly'][yyyymm][opt]=[]
                                for opt in features_listtype:
                                    if opt in v:
                                        master_stats_label_ref['total'][opt].extend(v[opt])
                                        master_stats_label_ref['monthly'][yyyymm][opt].extend(v[opt])
                                for opt in features_inttype:
                                    if opt in v:
                                        master_stats_label_ref['total'][opt] += v[opt]
                                        master_stats_label_ref['monthly'][yyyymm][opt] += v[opt]
                            master_stats_label_ref['maximal_load_latency']=maxlatency
                            print "reading file %s, maximal latency:%s"%(outp,maxlatency)

        # # calc total latency
        for exp,val in sorted(master_stats.items()):
            print "Calculating tota llatency of %s"%exp

            master_stats[exp]['total']['sumlatency_days'] = sum(master_stats[exp]['total']['latency_mount'])/(24.0*3600)
            for k,v in master_stats[exp]['monthly'].items():
                master_stats[exp]['monthly'][k]['sumlatency_days'] = sum(master_stats[exp]['monthly'][k]['latency_mount'])/(24.0*3600)

        features_inttype.append('sumlatency_days')
        jsondmp = {'total':{}, 'monthly':{}, 'maximal_load_latency':{}}
        for k,v in master_stats.iteritems():
            jsondmp['total'][k]={}
            jsondmp['maximal_load_latency'][k] = v['maximal_load_latency']
            for opt in features_listtype:
                try:
                    jsondmp['total'][k]["%s_mean"%opt] = numpy.mean(v['total'][opt])
                    jsondmp['total'][k]["%s_meanstd"%opt] = numpy.std(v['total'][opt])
                except: pass
            for opt in features_inttype:
                try:
                    jsondmp['total'][k][opt] = v['total'][opt]
                except: pass
            for yyyymm, val in v['monthly'].iteritems():
                try:
                    if yyyymm not in jsondmp['monthly']:
                        jsondmp['monthly'][yyyymm] = {}
                    if k not in jsondmp['monthly'][yyyymm]:
                        jsondmp['monthly'][yyyymm][k]={}
                    for opt in features_listtype:
                        try:
                            jsondmp['monthly'][yyyymm][k]["%s_mean"%opt] = numpy.mean(val[opt])
                            jsondmp['monthly'][yyyymm][k]["%s_meanstd"%opt] = numpy.std(val[opt])
                        except: pass
                    for opt in features_inttype:
                        try:
                            jsondmp['monthly'][yyyymm][k][opt] = val[opt]
                        except: pass
                except: pass
            print k, jsondmp['total'][k]

        with open(os.path.join(self.figures,'masterresults.json'),'w') as fp:
            json.dump(jsondmp, fp)
        plt.close('all')
        print "plotting monthly data of all"
        plot_bar2(jsondmp, self.figures)
        plt.close('all')
        #plot_dd_lines(jsondmp, self.figures)
        #plt.close('all')

        features = ["load_complete"]
        features2 = ['latency_mount']
        realsys = 'realsystem_analysis'
        #experiments.remove(realsys)
        experiments.sort()
        report = {}
        try:
            for date, roottree in res.iteritems():
                report[date] = {}
                if realsys in roottree:
                    for feature in features:
                        if feature in roottree[realsys]:
                            ref = float(roottree[realsys][feature]) # reference value of the real system
                            for exp in experiments:
                                if exp in roottree:
                                    if exp not in report[date]:
                                        report[date][exp] = {}
                                    if feature in roottree[exp]:
                                        report[date][exp][feature]=abs(roottree[exp][feature]-ref)/ref

                    for f2 in features2:
                        if f2 in roottree[realsys]:
                            ref = float(numpy.mean(remove_outlier(roottree[realsys][f2])))
                            for exp in experiments:
                                if exp in roottree:
                                    if exp not in report[date]:
                                        report[date][exp] = {}
                                    if feature in roottree[exp]:
                                        report[date][exp][f2]=abs(numpy.mean(roottree[exp][f2])-ref)/ref
        except Exception, e:
            raise

        sorted_keys = sorted(report.keys())
        all = features
        all.extend(features2)
        meanerr = {}

        head = "date"
        for i in experiments:
            fixed = i.split('/')[-1]
            head = "%s;%s"%(head,fixed)
            meanerr[i]={}
        for f in all:
            with open(os.path.join(self.figures,"%s.csv"%f), "w") as fp:
                fp.write("%s\n"%head)
                for k in sorted_keys:
                    #if realsys in report[k]:
                    #    realval = report[k][realsys][f]
                    #else:
                    #    realval  = "-"
                        s = "%s"%(k)
                        for i in experiments:
                            try:
                                val = report[k][i][f]
                                if f not in meanerr[i]:
                                    meanerr[i][f]=[]
                                meanerr[i][f].append(val)
                            except:
                                val = "-"
                            s = "%s;%s"%(s,val)
                        fp.write("%s\n"%s)
        for k,v in meanerr.items():
            for k2,v2 in v.iteritems():
                print k, k2, "mean:%s"%round(numpy.mean(v2) , 3), "std:%s"%(round(numpy.std(v2) , 3))

    def filter_raw(self,keys):
        int_ref = int
        self_start_ref = self.start
        self_end_ref = self.end
        filtered = []
        for key in keys:
            dt_obj = datetime.datetime(year=int_ref(key[:4]), month=int_ref(key[4:6]), day=int_ref(key[6:8]), hour=int_ref(key[9:11]))
            if self_start_ref <= dt_obj and self_end_ref >= dt_obj:
                filtered.append(key)
            #else:
            #    print self_start_ref , self_end_ref, dt_obj
        return  filtered

# # # # # # # # END VISUAL # # # # # # # # # # # # # # # # # # # #

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Examine HPSS Parser results')
    subparsers = parser.add_subparsers()

    parser_stats = subparsers.add_parser('stats', help="Get trace statistics")
    parser_stats.set_defaults(func=run_stats)
    #parser_stats.add_argument("--drivehits", action="store_true",
    #                    help="count drive hits")
    #parser_stats.add_argument("--mounts", action="store_true",
    #                    help="count mounts")
    #parser_stats.add_argument("--reading", action="store_true",
    #                    help="Count reading time in hours")
    #parser_stats.add_argument("--loaddelay", action="store_true",
    #                    help="average load delay")
    parser_stats.add_argument("--rootdir",
                        help="Path to the root directory", default="simulator")
    parser_stats.add_argument("--start",
                        help="starting time of analysis - start at YYYYMMDD", default="20110820")
    parser_stats.add_argument("--end",
                        help="ending time of analysis - end of YYYYMMDD", default="20140521")


    parser_visual = subparsers.add_parser('visual', help="visualize results")
    parser_visual.set_defaults(func=run_visual)
    parser_visual.add_argument("--rootdir", default="simulator",
                        help="Path to the data directory")
    parser_visual.add_argument("--start", default="20110901",
                        help="starting time of analysis - start at YYYYMMDD")
    parser_visual.add_argument("--end", default="20140521",
                        help="ending time of analysis - end of YYYYMMDD")
    parser_visual.add_argument("--quick",
                        help="quick quality evaluation, [day, hour, month]")
    parser_visual.add_argument("--cdf",
                        help="draw CDFs for the papar", action="store_true")
    parser_visual.add_argument("--dd",
                        help="draw DD for the papar", action="store_true")
    parser_visual.add_argument("--opt",
                        help="draw optimizations figures for the papar", action="store_true")
    parser_visual.add_argument("--all",
                        help="do everything necessary", action="store_true")
    parser_visual.add_argument("--compare",
                        help="compare the key features", action="store_true")
    parser_visual.add_argument("--tex",
                        help="print tex tables", action="store_true")
    parser_visual.add_argument("--hotcold",
                        help="generate hotcold graphs", action="store_true")
    parser_visual.add_argument("--drives",
                        help="generate drive graphs", action="store_true")



    args = parser.parse_args()
    args.func(args)
    #print args
    #absd = os.path.abspath(os.path.join(os.getcwd(),args.datadir, "cartridge"))
    #print absd, start, end
    #x = X1(absd, start, end)
