__author__ = 'maesker'

class CacheNotLockedException(Exception):pass
class NotCachedException(Exception):pass


from collections import deque
from LevelClass import *

class LC_LRU(SL8500_Level):
    def __init__(self, id, datadir, eventlog, **kwargs):
        SL8500_Level.__init__(self,id, datadir, eventlog,**kwargs)
        self._get_eviction_candidate = self._get_eviction_candidate_LRU_v2
        #xxself._lru_locked = deque()
        #xxself._lru_unlocked = deque()
        #xxself.c = len(self._drives)




    def _get_eviction_candidate_LRU_v2(self, **kwargs):
        maxunmounttime = datetime.datetime.now()
        eviction_candidate = None
        for drvid, obj in self._drives.iteritems():
            if obj.can_accept_load_operation():
                unmount = obj.get_last_unmount_timestamp()
                if unmount:
                    if maxunmounttime>unmount:
                        maxunmounttime=unmount
                        eviction_candidate=drvid
        return eviction_candidate


