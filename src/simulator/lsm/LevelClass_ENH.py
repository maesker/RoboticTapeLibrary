__author__ = 'maesker'
import collections

from LevelClass_LRU import LC_LRU
from simulator.dump.Prefetching import *



class LC_ENH (LC_LRU):
    def __init__(self, id, datadir, eventlog, **kwargs):
        LC_LRU.__init__(self,id, datadir, eventlog,**kwargs)
        #self._test_prefetching
        self.prefetching_results = [0,0,0] # success,neutral,fail
        #self.loaded_state_seconds_limit = 60

        self._prefetching_hints_rotation_pointer = None
        self._prefetching_hints = collections.deque(maxlen=HIT_INTERVAL_MINUTES)

    def receive_prefetching_hints(self, hints):
        if self.globalclock.minute != self._prefetching_hints_rotation_pointer:
            #n = hints
            self._prefetching_hints.append(hints)
            self._prefetching_hints_rotation_pointer = self.globalclock.minute
        else:
            self._prefetching_hints[-1].update(hints)
        #self._prefetching_hints_cnt += len(hints)
        #self.stat.register_prefetching_hints(self.globalclock, len(hints))

    def check_prefetching_applicability(self):
        idle = self._robot_system.get_idle_robots()
        if len(idle) > 0:
            if len(self._prefetching_hints)>0:
                self_get_cartridge_ref = self.get_cartridge
                for x in self._prefetching_hints:
                    items = x.items()
                    for (crtid, probability) in items:
                        crtobj = self_get_cartridge_ref(crtid,False)
                        if crtobj:
                            loc = crtobj.get_current_location()
                            if loc[0] == "L":
                                if self.test_prefetching(crtobj):
                                    self.clear(crtid)
                                    self.log.warning("%s:prefetching results:%s"%(self.id,self.prefetching_results))
                                return
                            #elif loc[0] == "D":
                            #    pass
                        else:
                            del x[crtid]

    def test_prefetching(self, crtobj):
        if self.c == 0:
            return False
        if len(self._lru_unlocked)/self.c >= LOADED_DRIVE_RATIO:
            eviction_candidate = self._lru_unlocked[-1]
            prefetch_candidate = crtobj.id

            #for x in [self._active_events, self._blocked_events]:
            #    for ev in x:
            #        print ev
            self_globalclock = self.globalclock
            for ev in self._upcoming_events:
                diff = ev.get_time() - self_globalclock
                if diff.seconds > HIT_INTERVAL_MINUTES:
                    break
                ev_crtid = ev.get('cartridgeid')
                if ev_crtid == eviction_candidate:
                    #print "EVICTION_CANDIDATE HIT %s DOH!"%eviction_candidate
                    self.prefetching_results[2]+=1
                    return True
                elif ev_crtid == prefetch_candidate:
                    #print "PREFETCHING HIT %s WOHOO! "%prefetch_candidate
                    self.prefetching_results[0]+=1
                    return True
            #print '%s:Nothing happend for either eviction candidate %s or prefetching candidate %s'%(self.globalclock, eviction_candidate, prefetch_candidate)
            self.prefetching_results[1]+=1
            return True
        return False


    def clear(self, removecrtid):
        for x in self._prefetching_hints:
            items = x.items()
            for (crtid, probability) in items:
                if crtid==removecrtid:
                    del x[crtid]
                    break
