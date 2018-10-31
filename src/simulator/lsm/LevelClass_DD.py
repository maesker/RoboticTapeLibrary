__author__ = 'maesker'


#from ecmwf.DriveClass import *
from LevelClass_LRU import *

from utilities.tiny_helper import get_diff_in_seconds


class LC_DD_0(LC_LRU):
    def __init__(self, id, datadir, eventlog,**kwargs):
        LC_LRU.__init__(self,id, datadir, eventlog,**kwargs)
        self._defer_dismount_interval = 0
        if 'parameter' in kwargs:
            self._defer_dismount_interval = int(kwargs['parameter'])
        self.log.info("Defer dismount interval = %s",self._defer_dismount_interval)

    def custom_step(self):
        idle = self._robot_system.get_idle_robots()
        if len(idle) > 0:
            loaded_state_drive = None
            loaded_state_in_seconds = 0
            for k, v in self._drives.iteritems():
                if v.state == STATE_DRV_LOADED:
                    diffsec = get_diff_in_seconds(v.entered_loadedstate,self.globalclock)
                    #diff = self.globalclock - v.entered_loadedstate
                    #diffsec = diff.seconds + diff.days*86400
                    if diffsec > loaded_state_in_seconds:
                        loaded_state_in_seconds = diffsec
                        loaded_state_drive = k
            if loaded_state_in_seconds >= self._defer_dismount_interval:
                if loaded_state_drive:
                    self.log.debug('%s:drive dismount %s is a candidate',self.globalclock, loaded_state_drive)
                    self.perform_cartridge_eviction(loaded_state_drive)
                    #raise Exception("robot idle, system idle, could do something relevant with drive %s"%loaded_state_drive)


#class LC_DD_0A(LC_DD_0):
#    def __init__(self, id, datadir, eventlog,**kwargs):
#        kwargs['ENABLE_LBMODULE']=True
#        LC_DD_0.__init__(self,id, datadir, eventlog,**kwargs)
