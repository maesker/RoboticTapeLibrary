__author__ = 'maesker'

import datetime

from TraceAnalyser.components import Events
from TraceAnalyser.components.TraceParser import *


class RobotParserInstance(BaseTraceParser):

    def __init__(self, pattern, **kwargs):
        BaseTraceParser.__init__(self,
            os.path.dirname(os.path.abspath(__file__)), pattern, **kwargs)
        self.currentdate = datetime.datetime(self.year, 1, 1, 0, 0)

    def event_factory(self, eventname, attributes):
        self.evntcnt = self.evntcnt + 1
        inst = Events.RobotEvent(self.evntcnt, eventname, attributes)
        try:
            if int(inst.attributes['MM']) < self.currentdate.month:
                self.year += 1
                # print "Incrementing year to ", self.year
            self.currentdate = datetime.datetime(self.year, int(inst.attributes['MM']), int(inst.attributes['DD']),
                                                 int(inst.attributes['hh']), int(inst.attributes['mm']), int(inst.attributes['ss']))
            inst.attributes['datetime'] = self.currentdate
            for i in ['mm', 'MM', 'DD', 'hh', 'ss']:
                del inst.attributes[i]
        except Exception:
            raise
        return inst
