__author__ = 'maesker'

class CacheNotLockedException(Exception):pass
class NotCachedException(Exception):pass


from collections import deque
from LevelClass import *

class LC_LFU(SL8500_Level):
    def __init__(self, id, datadir, eventlog, **kwargs):
        SL8500_Level.__init__(self,id, datadir, eventlog,**kwargs)
        self._get_eviction_candidate = self._get_eviction_candidate_LFU


    def _get_eviction_candidate_LFU(self, **kwargs):
        coldest_temp = 0
        coldest_drvid = None
        for drvid, obj in self._drives.iteritems():
            if obj.can_accept_load_operation():
                crtid = obj.get_current_cartridge()
                crtobj = self.get_cartridge(crtid)
                if crtobj:
                    temp = crtobj.get_heat(self.globalclock)
                    if temp > coldest_temp:
                        coldest_temp = temp
                        coldest_drvid = drvid
        return coldest_drvid


