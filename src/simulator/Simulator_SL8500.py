import Queue
import argparse
import bz2
import json
import logging
import os
import threading
import time
from datetime import timedelta, datetime

from utilities.python_daemon import createDaemon
from utilities.fsm.simulation_event import SimulationEvent
from utilities.analyser.base_analyser import Stats

from simulator.SL8500_SimStates import STATE_SL8500_RBT_LOADREQ
from simulator.Simulator import SimClass
from simulator.cartridges.CartridgeClass import *
from simulator.lsm.MasterLevel import LMaster


def ioevent_queue_writer(args):
    int_ref = int
    logger = logging.getLogger('root')
    strptime_ref = datetime.strptime
    shared_queue = args['queue']

    bzfile = bz2.BZ2File(args['inputfile'])
    cnt = 100000000
    try:
        while True:
            event_line = bzfile.readline()
            event = event_line.split(';')
            cnt += 1
            t = strptime_ref(event[1], "%Y%m%dT%H%M%S")
            if event[0] == 'Bye':
                break
            else:
                if event[2] > 0:
                    event_obj = SimulationEvent("MntOp", cnt,
                                                "crt_read_request",
                                                {'datetime': t,
                                                 'cartridgeid': event[0],
                                                 'readlength': int_ref(
                                                     event[2].rstrip('\n'))})
                    event_obj.set('state', STATE_SL8500_RBT_LOADREQ)
                    while True:
                        try:
                            # block, no timeout
                            shared_queue.put(event_obj, True)
                            break
                        except Queue.Full:
                            time.sleep(1)
                        except Exception, e:
                            logger.exception(e)
                            raise e
    except Exception, e:
        logger.exception(e)
        raise e


class TapeLib(SimClass):

    def __init__(self, args):
        custom_dir = (
            ('simulation','eviction_strategy'),
            ('simulation','eviction_parameter'),
            ('simulation','optimization'),
        )
        SimClass.__init__(self, args.config, custom_dir)
        queue_writer_minentries = 10000
        self._do_verify = self.cfg_parser.getint('results','verify')
        self._result_server = self.cfg_parser.get('results', 'server')
        self._result_dir = self.cfg_parser.get('results', 'directory')
        self._do_archive = self.cfg_parser.get('results', 'archive_results')

        self.eventqueue_driveenable = collections.deque()
        self.eventqueue_drivedisable = collections.deque()
        self.read_drive_events(
            os.path.join(self.tracedir,
                         self.cfg_parser.get('traces', 'drive_events')))

        self.eventqueue_cartridge_events = collections.deque()
        self.read_cartridge_events(
            os.path.join(self.tracedir,
                         self.cfg_parser.get('traces', 'crt_events')))

        self.eventqueue_io_events = Queue.Queue(queue_writer_minentries)
        self._ioqueue_args = {
            'inputfile': os.path.join(self.tracedir,
                                      self.cfg_parser.get('traces',
                                                          'io_events')),
            'queue': self.eventqueue_io_events
        }

        self._levelmaster = LMaster(
            eviction=self.cfg_parser.get("simulation", "eviction_strategy"),
            evic_param=self.cfg_parser.get("simulation", "eviction_parameter"),
            datadir=self.datadir,
            eventlog=self.eventlog,
            optimization=self.cfg_parser.get('simulation', 'optimization'))

        self.init_cartridge = self._levelmaster.init_cartridge
        self.process_every_ten_minutes = \
            self._levelmaster.process_every_ten_minutes

        self.load_config()

    def load_config(self):
        ##
        # @brief read config file to load the drive and library configuration
        # init drives and libraries with 'drive_enabled' and
        # 'lib_init' events. After that they should be in ready state
        # @return None

        # with open(os.path.normpath(os.path.join(
        # self.cfgdir, self.cfg_parser.get("system", "config"))), 'r') as f:
        fullpath = os.path.normpath(
            os.path.join(self.cfgdir, self.cfg_parser.get("system", "config")))
        bzfile = bz2.BZ2File(fullpath)
        res = json.loads(bzfile.read())
        self._levelmaster.add_level(res['level'])

    def run(self):
        t1 = threading.Thread(
            target=ioevent_queue_writer, args=(self._ioqueue_args,))
        t1.daemon = True
        t1.start()
        # started
        self.logger.info("Threads started.")

        curtime = datetime.now()
        push_cb = self._levelmaster.push

        eventq_de_pop_ref = self.eventqueue_driveenable.popleft
        eventq_dd_pop_ref = self.eventqueue_drivedisable.popleft
        eventq_crt_pop_ref = self.eventqueue_cartridge_events.popleft
        eventq_io_pop_ref = self.eventqueue_io_events.get

        next_io_event = eventq_io_pop_ref(True, 30)
        next_io_event_ts = next_io_event.get_time()

        next_crt_event = eventq_crt_pop_ref()
        next_crt_event_ts = next_crt_event.get_time()

        next_drivedis_event = eventq_dd_pop_ref()
        next_drivedis_event_ts = next_drivedis_event.get_time()

        next_driveen_event = eventq_de_pop_ref()
        next_driveen_event_ts = next_driveen_event.get_time()

        if 0:
            mintime = min(next_io_event_ts, next_crt_event_ts,
                          next_drivedis_event_ts, next_driveen_event_ts)
        else:
            mintime = next_io_event_ts

        globalclock = datetime(
            mintime.year, mintime.month, mintime.day, mintime.hour)
        hour = globalclock.hour
        onesecond = timedelta(seconds=self.stepsize_sec)
        quit_next = False

        while True:
            try:
                if next_drivedis_event_ts == globalclock:
                    try:
                        while next_drivedis_event.get_time() == globalclock:
                            push_cb(next_drivedis_event)
                            next_drivedis_event = eventq_dd_pop_ref()
                            next_drivedis_event_ts = \
                                next_drivedis_event.get_time()
                    except IndexError, e:
                        self.logger.exception(e)
                        next_drivedis_event_ts = False

                if next_driveen_event_ts == globalclock:
                    try:
                        while next_driveen_event.get_time() == globalclock:
                            push_cb(next_driveen_event)

                            next_driveen_event = eventq_de_pop_ref()
                            next_driveen_event_ts = \
                                next_driveen_event.get_time()
                    except IndexError, e:
                        self.logger.exception(e)
                        next_driveen_event_ts = False

                if next_io_event_ts == globalclock:
                    try:
                        while next_io_event.get_time() == globalclock:
                            push_cb(next_io_event)
                            next_io_event = eventq_io_pop_ref(True, 10)
                            next_io_event_ts = next_io_event.get_time()
                    except IndexError, e:
                        self.logger.exception(e)
                        next_io_event_ts = False
                    except Queue.Empty, e:
                        self.logger.exception(e)
                        next_io_event_ts = False

                if next_crt_event_ts == globalclock:
                    try:
                        while next_crt_event.get_time() == globalclock:
                            push_cb(next_crt_event)
                            next_crt_event = eventq_crt_pop_ref()
                            next_crt_event_ts = next_crt_event.get_time()
                    except IndexError, e:
                        self.logger.exception(e)
                        next_crt_event_ts = False

                self._levelmaster.superstep(globalclock)

                globalclock = globalclock + onesecond
                if hour != globalclock.hour:
                    # self.hourlyreport()
                    self.process_hourly()
                    hour = globalclock.hour
                    new = datetime.now()
                    diff = new - curtime
                    self.logger.info("Hour:%s took %f seconds", globalclock,
                                     diff.seconds +
                                     diff.microseconds / 1000000.0)
                    curtime = new
                    if quit_next:
                        break
                    if not next_io_event_ts:
                        self._keep_simulating_for_x_hours -= 1
                        if self._keep_simulating_for_x_hours <= 0:
                            quit_next = True

            except Exception, e:
                self.logger.exception(e)
                self.logger.error("An unhandled error occured. Exiting now")
                time.sleep(2)
                break

        self.finalize()
        t1.join()
        self.logger.info("Simulation Done.")
        self.analyse()
        if self._do_archive:
            cmd = "rsync -r -u %s %s:%s"%(os.path.dirname(self.tmpdir),
                                          self._result_server,
                                          self._result_dir)
            self.logger.info("Archiving results: cmd:%s"%cmd)
            os.system(cmd)

    def finalize(self):
        self.logger.info("Finalizing...")
        self._levelmaster.finalize()

    def read_drive_events(self, eventfile):
        # @todo handle priorities better
        if os.path.isfile(eventfile):
            bzfile = bz2.BZ2File(eventfile)
            queue_cb = self.eventqueue_driveenable.append
            cnt = 110000

            events = json.loads(bzfile.read())
            for event in events['enable']:
                t = datetime.strptime(event[0], "%Y-%m-%d %H:%M:%S")
                cnt += 1
                atts = {
                    'datetime': t,
                    'driveid': event[1]
                }
                queue_cb(
                    SimulationEvent("EvtDrvEn", cnt, "drive_enabled", atts))
            self.logger.info("Loaded %i drive enable events", cnt)

            cnt = 100000
            queue_cb = self.eventqueue_drivedisable.append
            for event in events['disable']:
                cnt += 1
                t = datetime.strptime(event[0], "%Y-%m-%d %H:%M:%S")
                atts = {
                    'datetime': t,
                    'driveid': event[1]
                }
                queue_cb(SimulationEvent(
                    "EvtDrvDi", cnt, "drive_disabled", atts))
            self.logger.info("Loaded %i drive disable events", cnt)
        else:
            self.logger.warn("Drive Event file %s not found" % eventfile)

    def read_cartridge_events(self, eventfile):
        if os.path.isfile(eventfile):
            bzfile = bz2.BZ2File(eventfile)
            queue_cb = self.eventqueue_cartridge_events.append
            cnt = 1000
            events = json.loads(bzfile.read())
            for event in events['eject']:
                cnt += 1
                queue_cb(
                    SimulationEvent("EvtCrtEj", cnt, "crt_eject",
                                    {'datetime': datetime.strptime(
                                        event[0], "%Y%m%d:%H%M%S"),
                                     'driveid': event[1]
                                     }))
            self.logger.info("Loaded %i cartridge events", cnt)
        else:
            self.logger.warn("Cartridge Event file %s not found" % eventfile)

    def process_hourly(self):
        self._levelmaster.process_hourly()

    def analyse(self):
        self.logger.info("Start Analyser")

        s = datetime(2011,9,1)
        e = datetime(2014,5,21)
        stat = Stats(self.tmpdir, s, e)

        if self._do_verify:
            self.verify()

    def verify(self):
        self.logger.info("Start verification")
        crterrors = 0
        inputtrace = os.path.join(
            self.tracedir, self.cfg_parser.get('traces', 'io_events'))
        self.logger.debug("Reading event list: %s" % inputtrace)
        crts = {}
        bzfile = bz2.BZ2File(inputtrace)
        try:
            while True:
                event_line = bzfile.readline()  # .split('\n')
                event = event_line.split(';')
                crtid = event[0]
                if crtid == "Bye" or crtid == "":
                    break
                else:
                    if crtid not in crts:
                        crts[crtid] = []
                    crts[crtid].append(int(event[2].rstrip('\n')))
        except Exception, e:
            self.logger.exception(e)

        cnt = 0
        maxcnt = len(crts.keys())
        sum_ref = sum
        total_error = 0
        for k, v in sorted(crts.items()):
            splitid = "%s/%s" % (k[0:4], k[4:6])
            fullp = os.path.join(self.tmpdir, "data/cartridge", splitid)
            pers = CartridgePersistence(fullp, 'sessions')
            cnt += 1

            registered_operations = []
            for event in pers.get_operations():
                for start, end in event['mount_operations']:
                    if end is None:
                        self.logger.error("Incomplete operation start:%s, crt:%s"%(start, k))
                        raise Exception("Incomplete operation")
                    diff = end - start
                    registered_operations.append(
                        diff.days * 86400 + diff.seconds)
            if sum_ref(registered_operations) != sum_ref(v):
                crterrors += 1
                total_error += abs(sum_ref(v) - sum_ref(registered_operations))
                self.logger.error("%s: sim does not match Trace:%s != Sim:%s" % (k, sum_ref(v), sum_ref(registered_operations)))
            if cnt % 1000 == 0:
                print "%s of %s done." % (cnt, maxcnt)
        self.logger.info("No. of errors:%i"%crterrors)
        self.logger.info("Reading time miss [h]: %f "%(total_error / 3600.0))
        return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='SL8500 Simulator')
    parser.add_argument("--daemon",
                        help="daemonize simulation",
                        action="store_true")

    parser.add_argument("-c", "--config",
                        default="conf",
                        help="Config file to use")

    parser.add_argument("-v", '--verify', action="store_true",
                        help="Verify simulation")

    args = parser.parse_args()

    if args.verify:
        t = TapeLib(args)
        t.verify()
    else:
        if args.daemon:
            createDaemon()
        t = TapeLib(args)
        t.run()
