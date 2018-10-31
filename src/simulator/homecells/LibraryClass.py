__author__ = 'maesker'

import datetime
import os

from utilities.fsm.StateMachine import BaseStateMachine, NONE_STATE

DRIVE_LATENCY_KEY = "drive_latency"
STATE_LIB_LOADED = "LOADED"
STATE_LIB_EMPTY = "EMPTY"


class LibraryClass(BaseStateMachine):
    alias_id = ['newlibraryid', 'oldlibraryid']

    transitions = {
        NONE_STATE: (
            ('robot_move',STATE_LIB_EMPTY),
            ('robot_eject',STATE_LIB_EMPTY),
            ('lib_init', STATE_LIB_EMPTY)   ,
            ('robot_acscr',STATE_LIB_EMPTY),
            ('robot_audit',STATE_LIB_EMPTY),
            ('robot_mount',STATE_LIB_EMPTY),
            ('robot_dismount',STATE_LIB_LOADED),
            ('robot_enter',STATE_LIB_LOADED)
        ),
        STATE_LIB_EMPTY: (
            ('robot_acscr',STATE_LIB_EMPTY),
            ('robot_mount',STATE_LIB_EMPTY),
            ('robot_audit',STATE_LIB_EMPTY),
            ('robot_dismount',STATE_LIB_LOADED),
            ('robot_enter',STATE_LIB_LOADED),
            ('robot_move',STATE_LIB_LOADED),
            ('robot_eject',STATE_LIB_EMPTY)

        ),
        STATE_LIB_LOADED: (
            ('robot_move',STATE_LIB_EMPTY),
            ('robot_eject',STATE_LIB_EMPTY),
            ('robot_acscr',STATE_LIB_EMPTY),
            ('robot_mount',STATE_LIB_EMPTY),
            ('robot_audit',STATE_LIB_LOADED),
            ('robot_dismount',STATE_LIB_LOADED),
            ('robot_enter',STATE_LIB_LOADED)
        )
    }

    def __init__(self, id, basedir, eventlog=True, simulationmode=False):
        BaseStateMachine.__init__(self, id,
                                  basedir=os.path.join(basedir, "library"),
                                  eventlog=eventlog,
                                  simulationmode=simulationmode)
       # self.alias_id = ['newlibraryid', 'oldlibraryid']
        #self.sessions = PersIO(self.basedir, "sessions",True)
        self._simulation_allocationcache = None
        self.home_cell_of = None
        self.init_tmp()


    def filtered(self, eventinst):
        return False

    def close(self):
        pass
        #self.sessions.close()

    def save(self):
        if self._simulation_mode or 1:
            self.init_tmp()
            return
        #def savesuccess(d):
        #    self.datashelve['success'].append(d)

        #def savefailure(d):
        #    self.datashelve['failure'].append(d)

        #failure = False
        #for k, v in self.tmp.items():
        #    if v == None:
        #        failure = True
        #if failure:
        #    savefailure(self.tmp)
        #else:
        #    savesuccess(self.tmp)
        #raise  Exception('implement me')

        if isinstance(self.tmp['unloaded'], datetime.datetime):
            #key = self.tmp['unloaded'].strftime("%Y%m%d-%H%M%S")
            self.sessions.put( self.tmp)
        else:
            raise Exception("wrong type %s"%self.tmp['unloaded'])
        self.init_tmp()


    def flush(self):
        pass
        #self.sessions.flush()

    def init_tmp(self):
        self.tmp = {
            'cartridgeid': None,
            'loaded': None,
            'unloaded': None
        }

    def verify(self):
        return True  # verify self.tmp data for completion

    def entercb_LOADED(self, event):
        self._simulation_allocationcache = None
        if self.tmp['cartridgeid'] == None:
            self.tmp['cartridgeid'] = event.attributes['cartridgeid']
        else:
            self.raiseDump(event)
        self.tmp['loaded'] = event.attributes['datetime']

    def instatecb_LOADED(self, event):
        if 'cartridgeid' in event.attributes:
            if event.attributes['cartridgeid'] != self.tmp['cartridgeid']:
             #               if self.tmp['cartridgeid'].startswith('CLN'):
                #self.save()
                self.tmp['cartridgeid'] = event.attributes['cartridgeid']
                self.tmp['loaded'] = event.attributes['datetime']
#                else:
                # self.raiseDump(event)

    def leavecb_LOADED(self, event):
        # print event
        #self.tmp['cardridgeid'] = event.attributes['cartridgeid']
        if self.tmp['cartridgeid'] != event.attributes['cartridgeid']:
            self.save()
        else:
            self.tmp['unloaded'] = event.attributes['datetime']
            self.save()




    # simulation data
    def is_empty(self):
        return self.state == STATE_LIB_EMPTY and self._simulation_allocationcache is None

    def deallocate(self, crtid):
        if self._simulation_allocationcache is crtid:
            self._simulation_allocationcache =None


    def allocate(self, crtid):
        if self._simulation_allocationcache is None:
            self._simulation_allocationcache =crtid
            self.home_cell_of = crtid
        elif self._simulation_allocationcache == crtid:
            pass
        else:
            raise Exception("Cant allocate %s, already registered %s"%(crtid, self._simulation_allocationcache))

    def get_crt(self):
        return  self._simulation_allocationcache
