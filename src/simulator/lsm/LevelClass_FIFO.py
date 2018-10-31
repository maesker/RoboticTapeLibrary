__author__ = 'maesker'


#from ecmwf.DriveClass import STATE_DRV_LOADED
#from ecmwf.tiny_helper import get_diff_in_seconds


from collections import deque
from LevelClass import *

class LC_FIFO(SL8500_Level):
    def __init__(self, id, datadir, eventlog, **kwargs):
        SL8500_Level.__init__(self,id, datadir, eventlog,**kwargs)
        #self._get_eviction_candidate = self._drive_eviction_fifo
        #xxself._fifoentry = deque()

    def _get_eviction_candidate(self, **kwargs):
        maxloadtime = datetime.datetime.now()
        eviction_candidate = None
        for drvid, obj in self._drives.iteritems():
            if obj.can_accept_load_operation():
                loadcompl = obj.get_loadcomplete_timestamp()
                if loadcompl:
                    if maxloadtime>loadcompl:
                        maxloadtime=loadcompl
                        eviction_candidate=drvid
        return eviction_candidate




class LC_FIFOa(SL8500_Level):
    def __init__(self, id, datadir, eventlog, **kwargs):
        kwargs['ENABLE_LBMODULE']=True
        SL8500_Level.__init__(self,id, datadir, eventlog,**kwargs)
        #self._get_eviction_candidate = self._drive_eviction_fifo
        #xxself._fifoentry = deque()


    # def check(self, crtid):
    #     crtobj = self.get_cartridge(crtid)
    #     if crtobj:
    #         location = crtobj.get_current_location()
    #         if location not in self._drives:
    #             for drvid, obj in self._drives.iteritems():
    #                 if obj.is_expecting_crt(crtid):
    #                     return None
    #             self.log.error("what happend here")
    #             raise Exception("not a valid eviction")
    #         else:
    #             drvobj = self._drives[location]
    #             if drvobj.is_empty():
    #                 return location
    #
    #
    #
    # def _drive_eviction_fifo(self,**kwargs):
    #     #self.log.debug()
    #     if len(self._fifoentry)>0:
    #         location = self.check(self._fifoentry[0])
    #         if location:
    #             return self._fifoentry.popleft()
    #         for i in self._fifoentry:
    #             location = self.check(i)
    #             if location:
    #                 self._fifoentry.remove(i)
    #                 return location
    #

    #xxdef cache_insert(self, crtid):
    #xx    if crtid not in self._fifoentry:
    #xx        self._fifoentry.append(crtid)
    #xx        return True

    #xxdef cache_access(self, crtid):
    #xx    if crtid in self._fifoentry:
    #xx        return True


    #xxdef cache_delete(self, crtid):
    #xx     if crtid in self._fifoentry:
    #xx         self._fifoentry.remove(crtid)
    #
