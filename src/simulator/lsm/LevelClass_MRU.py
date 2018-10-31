__author__ = 'maesker'

from LevelClass import *

class LC_MRU(SL8500_Level):
    def __init__(self, id, datadir, eventlog, libraryplacement, driveplacement, inqueue,outqueue):
        SL8500_Level.__init__(self,id, datadir, eventlog, libraryplacement, driveplacement,inqueue,outqueue)
        self._get_eviction_candidate = self._drive_eviction_MRU

    def _drive_eviction_MRU(self, crtid, sourcelib, **kwargs):
        list_of_drives_in_loaded_state = self.get_drives_in_loaded_state()
        minimum_drvid = None
        minimum_value = sys.maxint
        for driveid in list_of_drives_in_loaded_state:
            drvobj = self._drives[driveid]
            idletime = drvobj.get_idletime(self.globalclock)
            if idletime < minimum_value:
                minimum_value=idletime
                minimum_drvid = driveid
        return minimum_drvid


    def cache_update_size(self):
        pass

    def cache_access(self, crt):
        #raise Exception("Implement me")
        return (False,None)

    def cache_unlock(self, crtid):
        pass
        #raise Exception("Implement me")
