import argparse
import bz2
import datetime
import getpass
import glob
import json
import logging
import math
import multiprocessing
import os
import random
import re

from TraceAnalyser.components.MainSystem import System
from TraceAnalyser.robot.RobotParser import RobotParserInstance
from TraceAnalyser.whpss.WhpssParser import WhpssParserInstance
from simulator.cartridges import CartridgeClass
from simulator.drives import DriveClass
from simulator.homecells import LibraryClass
from utilities import tiny_helper
from utilities.SMTPclient import SMTP_client
from utilities.python_daemon import createDaemon

__author__ = 'maesker'


# #
# @brief Main class to access the parser, analyser and simulation
#
class ECMWF(System):

    ##
    # @param self self pointer
    # @param year year that the logs start, e.g. 2011.
    # @param workdir the working dir containing the "robot" and "whpss"
    #  directories. These directories need entries called 'db' and 'traces'
    # @param email if True, enables email notification
    # @return none
    #
    # @brief constructor function
    #
    # the year is not part of the whpss time stamp entries and my base
    # parser is not prepared to extract the date from file name. Espcially
    # this will cause other problems at year switches.
    def __init__(self, args):
        System.__init__(self, args.conf)

        # # @todo parameterize these and add them to the config file?
        self.register_componentclass(
            'cartridgeid', CartridgeClass.CartridgeClass)
        self.register_componentclass(
            'driveid', DriveClass.DriveClass)
        self.register_componentclass(
            'libraryid', LibraryClass.LibraryClass)

        robotinst = RobotParserInstance(
            pattern=os.path.join(
                self.cfg.get("ROBOT", "tracedir"),
                self.cfg.get("ROBOT", "pattern")),
            year=self.cfg.getint("ROBOT", 'starting-year')
        )
        self.register_parser("robot", robotinst)

        whpssinst = WhpssParserInstance(
            pattern=os.path.join(
                self.cfg.get("WHPSS", 'tracedir'),
                self.cfg.get("WHPSS", 'pattern')),
            year=self.cfg.getint("WHPSS", 'starting-year'))
        self.register_parser("whpss", whpssinst)

        self.patcrt = {'crt': re.compile('[A-Z][B|C|D][0-9]{4}', re.I)}

        self.multiprocessing = self.cfg.getint('DEFAULT', 'multiprocessing')
        self._ioevents_filename = self.cfg.get('OUTPUT', 'io-event-filename')
        self._driveevents_filename = self.cfg.get('OUTPUT',
                                                  'driveevents-filename')
        self._output_dir = self.cfg.get('OUTPUT', 'directory')
        if not os.path.isdir(self._output_dir):
            os.makedirs(self._output_dir)

        if self.cfg.getint("NOTIFY", "email-active"):
            self.eventcnt = 0
            pw = self.cfg.get("NOTIFY", 'passwd')
            address = self.cfg.get('NOTIFY', 'emailaddress')
            if pw == '<stdin>':
                print "Password for " + address
                pw = getpass.getpass()
            self.mailcl = SMTP_client(pw=pw)
            self.send("Starting now", "starting")
        else:
            self.mailcl = None

    ##
    # @nbrief overload superclass thread exit function to send a email
    #  notification
    def threadrunexit(self):
        self.send("Threadrun done", "exiting...")

    ##
    # @brief Send a msg via mail
    # @param subject: email subject text
    # @param content:  email content
    # @return none
    def send(self, subject, content):
        if self.mailcl:
            self.mailcl.send(subject, content)

    ##
    # @brief extract the necessary events from the parsed traces
    # @param mode: what to extract
    def analyse(self, mode):
        if mode == "request":
            self._get_requests()

        if mode == "drive":
            self._get_drive_events()

        if mode == "all":
            self._get_requests()
            self._get_drive_events()

        self.send("analyse run complete", "none")

    ##
    #  @brief return an instance of the given ids
    #  only one of the parameter should be specified
    def get_instance(self, crt=None, drive=None, library=None):
        if crt:
            return CartridgeClass.CartridgeClass(crt, self.datadir)
        if drive:
            return DriveClass.DriveClass(drive, self.datadir)
        if library:
            return LibraryClass.LibraryClass(
                library, self.datadir, simulationmode=True)
        raise Exception(
            "No instance found crt:%s, drv:%s, lib:%s" % (crt, drive, library))

    ##
    # @brief extract all cartridge request events
    def _get_requests(self):
        int_ref = int

        def run(ids):
            reads = {}
            for i in ids:
                crtid = i.translate(None, '/')
                inst = self.get_instance(crt=crtid)
                tmp = inst.extract_read_requests()
                for k, v in tmp.items():
                    if k not in reads:  # .keys():
                        reads[k] = []
                    if v == 0:  # round up to one second
                        v = 1
                    reads[k].append((crtid, v))
            j = {"events": []}
            lastdate = None
            with open("/tmp/events_%s.json" % str(
                    multiprocessing.current_process().pid), 'w') as fp:
                for i in sorted(reads.keys()):
                    for (crt, l) in reads[i]:
                        lastdate = i
                        j['events'].append(
                            [crt, i.strftime("%Y%m%dT%H%M%S"), l])
                lastdate += datetime.timedelta(minutes=5)
                j['events'].append(
                    ['Bye', lastdate.strftime("%Y%m%dT%H%M%S"), 0])
                json.dump(j, fp)

        crts = self.get_cartridges()
        random.shuffle(crts)
        if self.multiprocessing:  # multiprocesses
            procs = multiprocessing.cpu_count()
            perinst = int(math.ceil(len(crts) / float(procs)))
            pool = []
            while len(crts) > 0:
                tmp = []
                for i in range(perinst):
                    if len(crts) > 0:
                        tmp.append(crts.pop())
                    else:
                        break
                p = multiprocessing.Process(target=run, args=(tmp,))
                p.daemon = True
                p.start()
                print "Started ", str(p)
                pool.append(p)
            while len(pool) > 0:
                p = pool.pop()
                p.join()
                print "joined ", p
            print "Done"
        else:  # for profiling
            run(crts)

        # # merge partial events
        events = []
        for f in sorted(glob.glob(os.path.join("/tmp", "events*"))):
            with open(f, 'r') as fp:
                j = json.load(fp)

                events.extend(j['events'])

        events.sort(key=lambda x: x[1])
        ts = None
        output = bz2.BZ2File(self._ioevents_filename, 'wb')
        try:
            for (crt, ts, dur) in events:
                if crt != u"Bye":
                    output.write("%s;%s;%s\n" % (crt, ts, int_ref(dur)))
        finally:
            output.write("Bye;%s;0" % ts)
            output.close()

    ##
    # @brief get all cartridge ids
    # @return return a list of all observed cartridges
    def get_cartridges(self):
        basedir = os.path.join(self.datadir, "cartridge")
        return tiny_helper.get_cartridge_dirs(basedir)

    def get_drives(self):
        basedir = os.path.join(self.datadir, "drive")
        return tiny_helper.get_drive_dirs(basedir)

    ##
    # filtering some events. The corresponding cartridges and drives are
    # not part of the analysed hpss system, but share the same tape library.
    def filtered(self, eventinst, id):
        if id == 'driveid':
            instance_id = eventinst.attributes[id]
            if len(instance_id) != 6:
                logging.warning("Filtering drive %s", instance_id)
                return True
        if id == 'cartridgeid':
            instance_id = eventinst.attributes[id]
            if (not self.patcrt['crt'].match(instance_id) and not
                    instance_id.startswith('CLN')) or \
                    instance_id.startswith('E'):
                logging.warning("Filtering crt %s", instance_id)
                return True
        if self.mailcl:
            self.eventcnt += 1
            if self.eventcnt % 100000 == 0:
                self.send("still parsing",
                          "After %s events\ncurrent date %s " % (
                              self.eventcnt, eventinst.get_time()))

    ##
    # @brief generate finite state machine images of the drives, cartridges,
    # and libraries
    def graph(self):
        for obj in [CartridgeClass.CartridgeClass,
                    DriveClass.DriveClass,
                    LibraryClass.LibraryClass]:
            obj("0", self.datadir).generate_graph()

    def _get_drive_events(self):
        drives = self.get_drives()
        disableevents = {}
        enableevents = {}
        for drv in drives:
            drvid = drv.translate(None, '/')
            inst = self.get_instance(drive=drvid)
            (enabled,disabled) = inst.get_enable_disable_events()
            for dis_ts in disabled:
                if dis_ts not in disableevents:
                    disableevents[dis_ts] = set()
                disableevents[dis_ts].add(drvid)
            for en_ts in enabled:
                if en_ts not in enableevents:
                    enableevents[en_ts] = set()
                enableevents[en_ts].add(drvid)

        jsondmp = {'enable': [], 'disable': []}
        for enabled, drvs in sorted(enableevents.items()):
            for d in drvs:
                jsondmp['enable'].append(
                    [str(enabled), tiny_helper.translate_drive_id(d)])
        for disable, drvs in sorted(disableevents.items()):
            for d in drvs:
                jsondmp['disable'].append(
                    [str(disable), tiny_helper.translate_drive_id(d)])

        for x in ['enable', 'disable']:
            cnt=0
            for ev in jsondmp[x]:
                cnt += len(ev)
            print x, cnt


        fixed = self.drive_events_fix(jsondmp)
        # print some statistics
        for x in ['enable', 'disable']:
            cnt=0
            for ev in fixed[x]:
                cnt += len(ev)
            print x, cnt

        out = os.path.join(self._output_dir, self._driveevents_filename)
        output = bz2.BZ2File(out, 'wb')
        json.dump(fixed, output)
        output.close()

    def drive_events_fix(self, jsondmp):
        # # first fix
        fixed = {'enable':[], 'disable':[]}
        for event in jsondmp['enable']:
            if event[0].startswith('2012-05-16 0'):
                t = datetime.datetime.strptime(event[0], "%Y-%m-%d %H:%M:%S")
                event[0] = str(t + datetime.timedelta(hours=1))
            fixed['enable'].append(event)
        fixed['disable'] = jsondmp['disable']

        # # seconds fix
        fixed2 = {'enable':[], 'disable':[]}
        for event in fixed['enable']:
            if event[1] != "D_11_87":
                fixed2['enable'].append(event)
        for event in fixed['disable']:
            if event[1] != "D_11_87":
                fixed2['disable'].append(event)
        return fixed2

    def take_snapshot(self, snapshotdate):
        class LSM:
            def __init__(self, id):
                self.id = id
                self.drives = set()
                self.cartridges = set()

            def add(self, crtid, drive):
                self.drives.add(drive)
                self.cartridges.add(crtid)

            def report(self):
                return {'drives': list(self.drives),
                        'crts': list(self.cartridges)}

        lsm = {}
        for i in range(0,16):
            lsm[i] = LSM(i)
        crts = self.get_cartridges()
        for c in crts:
            if not len(crts)%1000:
                print 'remaining crts', len(crts)

            crtid = c.translate(None, '/')
            inst = self.get_instance(crt=crtid)
            (lsmid,driveid) = inst.take_snapshot(snapshotdate)
            try:
                if lsmid:
                    lsm[int(lsmid)].add(crtid, driveid)
                else:
                    print crtid, lsmid, driveid
            except:
                print crtid, lsmid, driveid
                raise
        dmp = {}
        for i, v in lsm.items():
            dmp[i]=v.report()

        out = os.path.join(self._output_dir,"snapshot_%s.json.bz2"%snapshotdate)
        output = bz2.BZ2File(out, 'wb')
        json.dump(dmp, output)
        output.close()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='HPSS Parser')
    # parser.add_argument("--train", help="Parser to train")
    p.add_argument("-c", "--conf", required=True,
                   help="Config file that configures the trace parser")
    p.add_argument('-p', "--parse", action="store_true",
                   help="Parser running in threaded mode, non interactively")
    p.add_argument('-d', "--daemon",
                   help="Daemonize process", action='store_true')
    p.add_argument('-a', "--analyse",
                   help="Analysis mode: all, request, drive")
    p.add_argument('-g', "--graph", default=False, action='store_true',
                   help="Generate state machine graphs of the Cartridge," +
                        " Drive and Library class")
    p.add_argument('-s', '--snapshot',
                   help="Take of snapshot at a current date: YYYYMMDD")
    args = p.parse_args()
    if args.daemon:
        createDaemon()

    e = ECMWF(args)
    if args.parse:
        e.threadrun()
    if args.graph:
        e.graph()
    if args.analyse:
        e.analyse(args.analyse)
    if args.snapshot:
        e.take_snapshot(args.snapshot)