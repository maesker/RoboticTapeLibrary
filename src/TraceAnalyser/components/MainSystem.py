__author__ = 'maesker'

import ConfigParser
import datetime

from TraceAnalyser.components.TraceParser import *


class System:
    def __init__(self, configfile):
        self.cfg = ConfigParser.SafeConfigParser()
        full = os.path.join(os.getcwd(), configfile)
        if not os.path.isfile(full):
            raise Exception("Config file %s not found", configfile)
        self.cfg.read(full)

        self.workdir = self.cfg.get("DEFAULT","workingdir")
        if not os.path.isdir(self.workdir):
            os.makedirs(self.workdir)
        self.datadir = os.path.join(self.workdir, 'data')
        if not os.path.isdir(self.datadir):
            os.makedirs(self.datadir)

        logfile = os.path.join(
            self.workdir,
            self.cfg.get("LOG","filename"))
        loglevel=logging.DEBUG
        if self.cfg.get("LOG", "level") == "INFO":
            loglevel = logging.INFO
        elif self.cfg.get("LOG", "level") == "WARNING":
            loglevel = logging.WARNING
        elif self.cfg.get("LOG", "level") == "ERROR":
            loglevel = logging.ERROR
        elif self.cfg.get("LOG", "level") == "CRITICAL":
            loglevel = logging.CRITICAL
        logging.basicConfig(filename=logfile, level=loglevel)
        logging.info('Starting global system log')

        self.component_classes = {}
        self.component_order = []
        self.instances = {}
        self.parserorder = []

        self.parser = {}
        self.event_cache = {}
        self.eventlogging=self.cfg.getint("LOG","eventlog")

    def register_componentclass(self, id, classref):
        self.component_order.append(id)
        self.component_classes[id] = classref
        self.instances[id] = {}

    def register_parser(self, name, parserinst):
        self.parser[name] = parserinst
        self.parserorder.append(name)

    # def parse_trace(self, **kwargs):
        #parser = self.parserref(self.config['pattern'], self.process, **kwargs)
        # parser.preparse()
        # parser.parse()

        # auf der linken Seite = to the left or on the left side ...
        # parser.postparse()

    def process(self, eventinst):
        try:
            for id in self.component_order:
                ref = self.component_classes[id]
                ids = [id]
                ids.extend(ref.alias_id)

                for tmpid in ids:
                    if tmpid in eventinst.attributes.keys():
                        # print eventinst.name, id, "in attributes",
                        # eventinst.attributes
                        inst = self.instances[id].get(
                            eventinst.attributes[tmpid], None)
                        if inst == None:
                            if not self.filtered(eventinst, id):
                                inst = ref(
                                    eventinst.attributes[tmpid],
                                    basedir=self.datadir,
                                    eventlog=self.eventlogging)
                                self.instances[id][
                                    eventinst.attributes[tmpid]] = inst
                        if inst:
                            inst.process(eventinst)
        except Exception,e:
            logging.exception(e)

    def filtered(self, eventinst, id):
        return False

    def close(self):
        for k, i in self.instances.items():
            for devid, devinst in i.items():
                devinst.close()

    def parser_training(self, parsername):
        obj = self.parser.get(parsername, None)
        if obj:
            obj.training()

    def threadrunexit(self):
        return None

    def threadrun(self):
        if not self.parserorder:
            self.parserorder = sorted(self.parser.keys())
        cnt = 60
        for k, inst in self.parser.items():
            inst.threadrun()
        time.sleep(5)
        logging.info("Start fetching events")
        while cnt:
            try:
                evnt = self.__fetch_next_event()
                if evnt:
                    cnt = 60
                    self.process(evnt)
                else:
                    time.sleep(2)
                    cnt -= 1
            except Exception, e:
                logging.exception("traceback:%s",e)

        logging.info("done")
        for k, inst in self.parser.items():
            logging.info("joining instance %s", k)
            inst.threadjoin()
            logging.info("joining instance %s done", k)
        logging.info("exit")
        return self.threadrunexit()

    def __fetch_next_event(self):
        nextevt_ts = datetime.datetime(2970, 1, 1, 0, 0)
        nextevt_id = None
        finalevnt = None

        # for id, inst in self.parser.items():
        for id in self.parserorder:
            inst = self.parser.get(id)
            if id not in self.event_cache:
                emptyqueue_retry_cnt = 10
                while emptyqueue_retry_cnt:
                    try:
                        self.event_cache[id] = inst.pop()
                        if self.event_cache[id] != None:
                            break
                    except Queue.Empty:
                        self.event_cache[id] = None
                    except:
                        raise
                    emptyqueue_retry_cnt -= 1
                    #logging.warning("id:%s no entries found, cnt:%s"%(id, emptyqueue_retry_cnt))
                    time.sleep(1)

            evnt = self.event_cache.get(id)
            # logging.debug("cache:%s"%str(self.event_cache))
            if evnt and evnt.get_time() < nextevt_ts:
                nextevt_id = id
                nextevt_ts = evnt.get_time()
        # print self.event_cache
        # logging.debug("%s",str(self.event_cache))
        if nextevt_id != None:
            finalevnt = self.event_cache[nextevt_id]
            del self.event_cache[nextevt_id]
        return finalevnt
