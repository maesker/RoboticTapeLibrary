__author__ = 'maesker'

from LevelClass import *

class LC_Random(SL8500_Level):
    def __init__(self, id, datadir, eventlog, libraryplacement, driveplacement, inqueue,outqueue):
        SL8500_Level.__init__(self,id, datadir, eventlog, libraryplacement, driveplacement, inqueue,outqueue)
        self._get_eviction_candidate = self._drive_eviction_RANDOM

    def _drive_eviction_RANDOM(self, crtid, sourcelib, **kwargs):
        list_of_drives_in_loaded_state = self.get_drives_in_loaded_state()
        if len(list_of_drives_in_loaded_state) > 0:
            return random.choice(list_of_drives_in_loaded_state)
        return None



    def cache_update_size(self):
        pass

    def cache_access(self, crt):
        #raise Exception("Implement me")
        return (False,None)

    def cache_unlock(self, crtid):
        pass
        #raise Exception("Implement me")
