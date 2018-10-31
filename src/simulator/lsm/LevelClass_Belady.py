__author__ = 'maesker'

from LevelClass import *

class LC_Belady(SL8500_Level):
    def __init__(self, id, datadir, eventlog,**kwargs):
        SL8500_Level.__init__(self,id, datadir, eventlog,**kwargs)
        self._get_eviction_candidate = self._drive_eviction_BELADY

    def _drive_eviction_BELADY(self, **kwargs):
        list_of_drives_in_loaded_state = self.get_drives_in_loaded_state()
        if len(list_of_drives_in_loaded_state)==0:
            return None

        crts = {}
        for driveid in list_of_drives_in_loaded_state:
            drvobj = self._drives[driveid]
            if len(drvobj._simulation_cache_allocated)<2:
                crts[drvobj.get_current_cartridge()] = driveid
        #self.log.error("request:%s, existing:%s"%(crtid, crts.keys()))

        for x in [self._active_events, self._blocked_events,self._upcoming_events]:
            for ev in x:
                if len(crts)<=1:
                    break
                if ev.get('cartridgeid') in crts.keys():
                    del crts[ev.get('cartridgeid')]
            #self.log.error("request:%s, evict:%s"%(crtid, crts))
        if len(crts)==0:
            return None
        return crts.values()[0]

class LC_BeladyA(LC_Belady):
    def __init__(self, id, datadir, eventlog,**kwargs):
        kwargs['ENABLE_LBMODULE']=True
        LC_Belady.__init__(self,id, datadir, eventlog,**kwargs)