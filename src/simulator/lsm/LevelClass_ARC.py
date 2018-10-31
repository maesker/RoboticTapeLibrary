__author__ = 'maesker'

import string
import random
from collections import deque

from LevelClass import SL8500_Level

class LC_Arc(SL8500_Level):
    def __init__(self, id, datadir, eventlog, libraryplacement,
                 driveplacement, inqueue,outqueue):
        SL8500_Level.__init__(self,id, datadir, eventlog,
                              libraryplacement, driveplacement, inqueue,outqueue)
        self._get_eviction_candidate = self._drive_eviction_ARC

        self.c = len(self._drives)
        self.t1l = deque()
        self.t1u = deque()
        self.t2l = deque()
        self.t2u = deque()
        self.bl = deque()
        self.bu = deque()

    def cache_update_size(self):
        self.c = len(self._drives) # ARC Cache update

    def cache_access(self, element):
        # cache hits
        if element in self.t1l:         # move from t1l to t2l
            self.t1l.remove(element)
            self.t2l.appendleft(element)
            return (True, None)
        if element in self.t1u:       # move from t1u to t2l
            self.t1u.remove(element)
            self.t2l.appendleft(element)
            return (True, None)
        if element in self.t2l:       # mru in t2l
            self.t2l.remove(element)
            self.t2l.appendleft(element)
            return (True, None)
        if element in self.t2u:       # mru in t2l
            self.t2u.remove(element)
            self.t2l.appendleft(element)
            return (True, None)

        # blocked cache misses !!!
        if len(self.t1l) + len(self.t2l) == self.c:
            return (False, None)                # complete cache is locked
        elif element in self.bl:       # error case. cant load element
            return (False, None)

        res = True,
        evict = None
        # swapable cache misses
        if element in self.bu:
            self.bu.remove(element)
            evict = self.__insert(element, self.t2l)
        else:   # element not in either of the lists
            if self._T_size()< self.c:
                self.t1l.appendleft(element)
            else:
                evict = self.__insert(element, self.t1l)
        return (res,evict)

    def __insert(self, element, tlist):
        tlist.appendleft(element)
        evicted = self.__evict()
        self.__insertB(evicted)
        return evicted

    def __evict(self):
        if self._1_size() > self._2_size():
            if len(self.t1u) > 0:
                return self.t1u.pop()
            else:
                return self.t2u.pop()
        else:
            if len(self.t2u) > 0:
                return self.t2u.pop()
            else:
                return self.t1u.pop()

    def __insertB(self, element):
        if self._B_size() < self.c:
            self.bl.appendleft(element)
        else:
            if len(self.bu) > 0:
                self.bu.pop()
                self.bl.appendleft(element)
            else:
                pass
                #raise Exception("cant cache element in B1")

    def cache_unlock(self, element):
        for a,b in [(self.t1l,self.t1u),(self.t2l,self.t2u),(self.bl,self.bu)]:
            if element in a:
                b.appendleft(element)
                a.remove(element)
                return
        raise Exception("where is the locked element?")

    def _1_size(self):
        return len(self.t1l)+len(self.t1u)

    def _2_size(self):
        return  len(self.t2l)+len(self.t2u)

    def _T_size(self):
        return len(self.t1l)+len(self.t1u) + len(self.t2l)+len(self.t2u)

    def _B_size(self):
        return len(self.bl)+len(self.bu)

    #def handle_lock(self, unlockprobability=0.2):
    #    for locked in [self.t1l, self.t2l, self.bl]:
    #        l = list(locked)
    #        for element in l:
    #            x = random.random()
    #            if x <= unlockprobability:
    #                self.unlock(element)



    def _drive_eviction_ARC(self, crtid, sourcelib, **kwargs):
        #list_of_drives_in_loaded_state = self.get_drives_in_loaded_state()
        #if len(list_of_drives_in_loaded_state) > 0:
        if 1:
            (evict, evictioncrt) = self.cache_access(crtid)
            if evict:
                crtobj = self.get_cartrige(evictioncrt)
                if crtobj:
                    location = crtobj.get_current_location()
                    if location not in self._drives.keys():
                        self.log.error("what happend here")
                        raise  Exception("not a valid eviction")
                    else:
                        return location
            self.log.error("why is there no eviction")
        return None
'''
class lARC: # adaptive replacement cache hierarchie
    def __init__(self, c=8):
        self.c = c
        self.t1l = deque()
        self.t1u = deque()
        self.t2l = deque()
        self.t2u = deque()
        self.bl = deque()
        self.bu = deque()

    def access(self, element):
        # cache hits
        if element in self.t1l:         # move from t1l to t2l
            self.t1l.remove(element)
            self.t2l.appendleft(element)
            return (True, None)
        if element in self.t1u:       # move from t1u to t2l
            self.t1u.remove(element)
            self.t2l.appendleft(element)
            return (True, None)
        if element in self.t2l:       # mru in t2l
            self.t2l.remove(element)
            self.t2l.appendleft(element)
            return (True, None)
        if element in self.t2u:       # mru in t2l
            self.t2u.remove(element)
            self.t2l.appendleft(element)
            return (True, None)

        # blocked cache misses !!!
        if len(self.t1l) + len(self.t2l) == self.c:
            return (False, None)                # complete cache is locked
        elif element in self.bl:       # error case. cant load element
            return (False, None)

        res = True,
        evict = None
        # swapable cache misses
        if element in self.bu:
            self.bu.remove(element)
            evict = self.__insert(element, self.t2l)
        else:   # element not in either of the lists
            if self._T_size()< self.c:
                self.t1l.appendleft(element)
            else:
                evict = self.__insert(element, self.t1l)
        return (res,evict)

    def __insert(self, element, tlist):
        tlist.appendleft(element)
        evicted = self.__evict()
        self.__insertB(evicted)
        return evicted

    def __evict(self):
        if self._1_size() > self._2_size():
            if len(self.t1u) > 0:
                return self.t1u.pop()
            else:
                return self.t2u.pop()
        else:
            if len(self.t2u) > 0:
                return self.t2u.pop()
            else:
                return self.t1u.pop()

    def __insertB(self, element):
        if self._B_size() < self.c:
            self.bl.appendleft(element)
        else:
            if len(self.bu) > 0:
                self.bu.pop()
                self.bl.appendleft(element)
            else:
                pass
                #raise Exception("cant cache element in B1")

    def unlock(self, element):
        for a,b in [(self.t1l,self.t1u),(self.t2l,self.t2u),(self.bl,self.bu)]:
            if element in a:
                b.appendleft(element)
                a.remove(element)


    def _1_size(self):
        return len(self.t1l)+len(self.t1u)

    def _2_size(self):
        return  len(self.t2l)+len(self.t2u)

    def _T_size(self):
        return len(self.t1l)+len(self.t1u) + len(self.t2l)+len(self.t2u)

    def _B_size(self):
        return len(self.bl)+len(self.bu)

    def handle_lock(self, unlockprobability=0.2):
        for locked in [self.t1l, self.t2l, self.bl]:
            l = list(locked)
            for element in l:
                x = random.random()
                if x <= unlockprobability:
                    self.unlock(element)




if __name__ == "__main__":
    larc = lARC(10)
    sequence = []#['A', 'K', 'W', 'F', 'R', 'K', 'N', 'N', 'B', 'Y', 'V', 'X','W', 'N', 'U', 'J', 'A', 'G', 'C', 'T', 'Y', 'K', 'N', 'C', 'W', 'J', 'G', 'Y', 'I', 'V']
    for i in range(1000):
        sequence.append(random.choice(string.uppercase))
    #print sequence
    cnt = 0
    for i in sequence:
        cnt += 1
        larc.access(i)
        larc.handle_lock()
        if cnt%10==0:
            print cnt





'''






