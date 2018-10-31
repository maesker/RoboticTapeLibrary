import Queue
import anydbm
import glob
import gzip
import logging
import os
import re
import sys
import threading
import time
import traceback
import uuid

from utilities.fsm import StateMachineEvent

__author__ = 'maesker'

QUEUESIZE_LIMIT = 10000


class UnparsedLine(BaseException):

    def __init__(self, *args, **kwargs):
        BaseException.__init__(self, *args, **kwargs)


class BaseTraceParser:

    def __init__(self, basedir, pattern, **kwargs):
        self.interactive = False
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.logger = logging.getLogger()

        self.pattern = pattern
        self.pattern_attribute_compiled = {}
        dbdir = os.path.join(basedir, 'db')
        if not os.path.isdir(dbdir):
            os.makedirs(dbdir)
        self.logger.info("DB dir:%s",dbdir)
        try:
            self.pattern_attribute = anydbm.open(
                os.path.join(dbdir, 'attribute_pattern.anydb'), 'c')
            self.pattern_events_compiled = {}
            self.pattern_events = anydbm.open(
                os.path.join(dbdir, 'events_pattern.anydb'), 'c')
            self.pattern_ignore_lines_compiled = {}
            self.pattern_ignore_lines = anydbm.open(
                os.path.join(dbdir, "ignorelines_pattern.anydb"), 'c')
            self.event_complete_list = anydbm.open(
                os.path.join(dbdir, "eventcomplete_list.anydb"), 'c')
        except Exception, e:
            self.logger.exception(e)

        self.evntcnt = 0

        self.__queue = Queue.Queue(maxsize=10000)
#        self.queuesize = self.__queue.qsize
        self.compile_pattern()

    def pop(self):
        if self.__queue.qsize() > 0:
            return self.__queue.get(True, 1)
        return None

    def training(self):
        self.interactive = True
        self.parse()

    def threadrun(self):
        self.interactive = False
        self.thread = threading.Thread(target=self.parse)
        self.thread.start()

    def threadjoin(self):
        self.logger.info("Joining process.")
        self.thread.join()

    def parsekey(self, key):
        parts = key.split(';')
        k = parts[0]
        l = []
        if len(parts) > 1:
            l = parts[1]
        return k, l

    def close(self):
        self.pattern_attribute.close()
        self.pattern_events.close()
        self.pattern_ignore_lines.close()
        self.event_complete_list.close()

    def compile_pattern(self):
        for k, v in self.pattern_attribute.iteritems():
            # print k,v
            if len(v) != 0:
                self.pattern_attribute_compiled[v] = re.compile(v)
        for k, v in self.pattern_events.iteritems():
            # print k,v
            if len(v) != 0:
                self.pattern_events_compiled[v] = re.compile(v)
        for k, v in self.pattern_ignore_lines.iteritems():
            # print k,v
            if len(v) != 0:
                self.pattern_ignore_lines_compiled[v] = re.compile(v)
        self.logger.info("PatternAttributeCompiled: %i pattern" % len(
            self.pattern_attribute_compiled))
        self.logger.info("PatternEventsCompiled: %i pattern" % len(
            self.pattern_events_compiled))
        self.logger.info("PatternIgnoreCompiled: %i pattern" % len(
            self.pattern_ignore_lines_compiled))

    def parse(self):
        def handle(f, cb):
            with cb(f, 'r') as source:
                for line in source.readlines():
                    parsed = self.handle_line(line)
                    if not parsed:
                        self.handle_unparsed_line(line)

        try:
            for f in sorted(glob.glob(self.pattern)):
                self.logger.info("Processing file %s" % f)
                if f.endswith('.gz'):
                    handle(f, gzip.open)
                else:
                    handle(f, open)

        except Exception, e:
            self.logger.exception(e)
            raise
        finally:
            self.close()

    def handle_line(self, line):
        for ignore, pat in self.pattern_ignore_lines_compiled.items():
            if pat.search(line):
                # print "IGNR:",line,
                return True
        for eventunparsed, patname in self.pattern_events.items():
            event, cnt = self.parsekey(eventunparsed)
            pat = self.pattern_events_compiled[patname]
            m = pat.search(line)
            if m:
                atts = self.__identify_attributes(line)
                atts.update(m.groupdict())
                if event not in self.event_complete_list.keys():
                    print "Event:", event, "with attributes ", atts
                    print line
                    if self.interactive:
                        while True:
                            if raw_input("Get more attributes:[no|yes]:") ==\
                                    "yes":
                                self.__add_attribute_match(line)
                            else:
                                self.event_complete_list[event] = "1"
                                break
                atts.update(self.__identify_attributes(line))
                self.__queue.put(self.event_factory(event, atts), True)
                return True
        self.logger.warning("Unmatched line: %s", line)
        return False

    def __identify_attributes(self, line):
        # print "identify"
        attributes = {}
        for k, v in self.pattern_attribute_compiled.iteritems():
            m = v.search(line)
            if m:
                attributes.update(m.groupdict())
        return attributes

    def handle_unparsed_line(self, line):
        if not self.interactive or len(line) < 5:
            return

        print "Unhandled line:\n", line
        print "Identified Attributes:"
        self.__identify_attributes(line)

        print "[0]: ignore"
        print "[1]: add attribute match"
        print "[2]: add ignore match"
        print "[3]: add event match"
        cmd = raw_input("?:")
        if cmd == '0':
            return
        elif cmd == "1":
            self.__add_attribute_match(line)
        elif cmd == '2':
            self.__add_ignore_match(line)
        elif cmd == "3":
            key = raw_input("event name:")
            if len(key) == 0:
                key = uuid.uuid4()
            self.__add_event_match(line, key)
        else:
            raise UnparsedLine()  # unparsed line

    def __add_attribute_match(self, line):
        return self.__add_match(line, self.pattern_attribute)

    def __add_ignore_match(self, line):
        return self.__add_match(line, self.pattern_ignore_lines)

    def __add_event_match(self, line, key):
        return self.__add_match(line, self.pattern_events, key)

    def __add_match(self, line, reference, key=None):
        # for k,v in self.pattern_attribute_compiled.items():
        #    m = v.search(line)
        #    if m:
        #        print m.groupdict()
        while True:
            if not key:
                key = uuid.uuid4()
            pat = raw_input("New pattern: 0 to exit")
            if pat != '0':
                while key in self.pattern_events.keys():
                    print sorted(self.pattern_events.keys())
                    key = raw_input("Key exists in event key, enter new one")

                patcomp = re.compile(pat)
                if patcomp.search(line):
                    reference[str(key)] = pat
                    key = None
                    self.compile_pattern()
                    break
                else:
                    print "did not match"
            else:
                break

    def event_factory(self, eventname, attributes):
        self.evntcnt = self.evntcnt + 1
        #print "old factory"
        return StateMachineEvent.Event(self.evntcnt, eventname, attributes)
