import Queue
import random
import re
import socket
import sys

from simulator.cartridges.CartridgeClass import *
from simulator.drives.DriveClass import *
from simulator.homecells.LibraryClass import *
from simulator.lsm.Exceptions import *
from simulator.lsm.handbots.Robot import RobotSystem
from utilities.fsm.simulation_event import SimulationEvent

if socket.gethostname() == "elwood" and 0: #0 if debug
    #ENABLE_LB = True
    pass
else:
    #ENABLE_LB = False
    pass

__author__ = 'maesker'

MODULE_KEY_LOADBALANCER = 'LB'

DEFAULT_NUM_ROBOTS=2



class Libcontainer:
    def __init__(self):
        self.libraryregex = re.compile("L_(?P<level>[0-9]{2})_(?P<wall>[0-9]{1})_(?P<capacity>[0-9]{1})_(?P<lib>[0-9]{3})")
        self.home_cell_cache = collections.OrderedDict()
        self.cap1 = collections.OrderedDict()
        self.cap2 = collections.OrderedDict()
        self.cap3 = collections.OrderedDict()
        self.cap4 = collections.OrderedDict()
        self.cap5 = collections.OrderedDict()
        self.cap6 = collections.OrderedDict()
        self.caps = (self.cap1,self.cap2,self.cap3,self.cap4,self.cap5,self.cap6)

    def add(self, key, obj):
        m = self.libraryregex.match(key)
        if m:
            cap = m.group("capacity")
            if cap == "1":
                self.cap1[key]=obj
            elif cap == "2":
                self.cap2[key]=obj
            elif cap == "3":
                self.cap3[key]=obj
            elif cap == "4":
                self.cap4[key]=obj
            elif cap == "5":
                self.cap5[key]=obj
            elif cap == "6":
                self.cap6[key]=obj
            else:
                raise Exception("unknown lib %s"%key)

    def deallocate(self, crtid):
        if crtid in self.home_cell_cache:
            libid =  self.home_cell_cache[crtid]
            libobj = self.get(libid)
            libobj.home_cell_of = None
            del self.home_cell_cache[crtid]

    def get_home_cell(self, crtid):
        if crtid  not in self.home_cell_cache:
            for cap in self.caps:
                for libid, libobj in cap.iteritems():
                    if libobj.is_empty() and libobj.home_cell_of == None:
                        self.home_cell_cache[crtid] = libid
                        libobj.home_cell_of = crtid
                        return libid
        return self.home_cell_cache.get(crtid,None)

    def empty_count(self):
        cnt = 0
        for cap in self.caps:
            for k,val in cap.iteritems():
                if val.is_empty():
                    cnt += 1
        return cnt

    def size(self):
        return len(self.cap1)+len(self.cap2)+len(self.cap3)+len(self.cap4)+len(self.cap5)+len(self.cap6)

    def items(self, capid, allowedwalls = None):
        if capid==1:
            its = self.cap1
        elif capid==2:
            its =  self.cap2
        elif capid==3:
            its =  self.cap3
        elif capid==4:
            its =  self.cap4
        elif capid==5:
            its =  self.cap5
        elif capid==6:
            its =  self.cap6
        else:
            its = collections.OrderedDict()
        if allowedwalls:
            res = collections.OrderedDict()
            for k,v in its.iteritems():
                if k[5] in allowedwalls:
                    res[k]=v
            return res.items()
        else:
            return its.items()

    def values(self):
        e = self.cap1.values()
        e.extend(self.cap2.values())
        e.extend(self.cap3.values())
        e.extend(self.cap4.values())
        e.extend(self.cap5.values())
        e.extend(self.cap6.values())
        return e

    def get(self, key):
        for x in self.caps:
            if key in x:
                return x.get(key)

    def exists(self, key):
        for x in self.caps:
            if key in x:
                return True

    def get_randomized_libids(self, capid):
        if capid==1:
            its = self.cap1
        elif capid==2:
            its =  self.cap2
        elif capid==3:
            its =  self.cap3
        elif capid==4:
            its =  self.cap4
        elif capid==5:
            its =  self.cap5
        elif capid==6:
            its =  self.cap6
        else:
            its = {}
        keys = its.keys()
        random.shuffle(keys)
        return keys

    #def get_random(self):
    #    for cap in self.caps:
    #        for k,v in cap.iteritems():
    #            if not v.is_empty():
    #                return k,v



class BaseModule:
    def __init__(self, id):
        self.id = id
        self.log = logging.getLogger('simulator')

    def process_hourly(self, clock):
        pass

    def custom_step(self):
        pass

    def on_idle(self):
        pass

    def dummyfunction(self, *args, **kwargs):
        pass

class LoadBalancer(BaseModule):
    def __init__(self, id, optimizationlevel, **kwargs):
        BaseModule.__init__(self, id)
        self.__optimizationlevel = optimizationlevel
        self._libraries_ref = kwargs['libraries']
        self._crtcache_ref = kwargs['crtcache']
        self._drives_ref = kwargs['drives']
        self._hotcold_classification_violators = []

        self._LB_heat_percentiles = [sys.maxint,sys.maxint,sys.maxint,sys.maxint,sys.maxint]
        self._LB_relative_load_percentiles = [sys.maxint,sys.maxint,sys.maxint,sys.maxint,sys.maxint]
        self._LB_numdrives = 0
        self._update_interval_size = 3600 # seconds

        if self.__optimizationlevel==1:
            self.get_action = self.dummyfunction
            self.check_cartridge_homecell_placement = self.dummyfunction
        elif self.__optimizationlevel >= 2:
            self.get_action = self.__opt2_get_action
            self.check_cartridge_homecell_placement = self.__opt2_check_cartridge_homecell_placement


    def process_hourly(self, clock):
        #libs = 0
        #items = self._libraries_ref.values()
        #for inst in items:
        #    if not inst.is_empty():
        #        libs += 1
        #if  libs > len(self._crtcache_ref):
        #    self.log.warning("Verify: %s of %s libs allocated, %s crt in level"%(libs,len(items),len(self._crtcache_ref)))
        self.calculate_loadbalancing_data(clock)
        self.check_cartridge_homecell_placement(clock)

    def calculate_loadbalancing_data(self, clock):
        capacity_limit_cap0 = 0.143
        capacity_limit_cap1 = 0.315
        capacity_limit_cap2 = 0.487
        capacity_limit_cap3 = 0.658
        capacity_limit_cap4 = 0.829

        self._LB_numdrives = 0.0
        for drvobj in self._drives_ref.itervalues():
            if drvobj.state!=STATE_DRV_DISABLED:
                self._LB_numdrives += 1                     # number of active drives
        num_homecells = float(self._libraries_ref.size())   # number of homechells
        num_cartridges = float(len(self._crtcache_ref))            # number of cartridges
        hc_coverage = num_cartridges/num_homecells

        heats = []
        for crtid, crtobj in self._crtcache_ref.iteritems():
            heats.append(crtobj.get_heat(clock))
        heats.sort()
        self._LB_heat_percentiles[0] = tiny_helper.percentile(heats, min(capacity_limit_cap0 / hc_coverage, 1.0)) + self._update_interval_size
        self._LB_heat_percentiles[1] = tiny_helper.percentile(heats, min(capacity_limit_cap1 / hc_coverage, 1.0)) + self._update_interval_size
        self._LB_heat_percentiles[2] = tiny_helper.percentile(heats, min(capacity_limit_cap2 / hc_coverage, 1.0)) + self._update_interval_size
        self._LB_heat_percentiles[3] = tiny_helper.percentile(heats, min(capacity_limit_cap3 / hc_coverage, 1.0)) + self._update_interval_size
        self._LB_heat_percentiles[4] = tiny_helper.percentile(heats, min(capacity_limit_cap4 / hc_coverage, 1.0)) + self._update_interval_size

        self._LB_relative_load_percentiles=[]
        if self._LB_numdrives>0:
            factor = self._LB_numdrives/num_cartridges
            for i in self._LB_heat_percentiles:
                self._LB_relative_load_percentiles.append(round(factor*i))
        self.log.debug("%02i:HomeCells:%s:#c/d:%s/%s:abs/rel-Heat:%s/%s"%(self.id, num_homecells,num_cartridges,self._LB_numdrives,self._LB_heat_percentiles,self._LB_relative_load_percentiles))

    def get_loadbalancing_attributes(self):
        #self.calculate_loadbalancing_data(data)
        return {
            'relative_load_percentiles': self._LB_relative_load_percentiles,
            'heat_percentiles': self._LB_heat_percentiles,
            'numdrives':self._LB_numdrives,
            'libleft': len(self._crtcache_ref)/float(self._libraries_ref.size())}

    #def receive_loadbalancing_targetlevel_candidates(self, candidates):
    #    if len(candidates)>0:
    #        self.log.debug("Got LB_Candidates %s"%candidates)
    #    self._LB_targetlevel_candidates = set(candidates)

    def __get_capextention_starting_index(self, heat, timecritical):
        tc = False
        if self.__optimizationlevel>=3:
            tc = timecritical
        if tc or heat <= self._LB_heat_percentiles[0]:
            index = 1
        elif heat <= self._LB_heat_percentiles[1]:
            index = 2
        elif heat <= self._LB_heat_percentiles[2]:
            index = 3
        elif heat <= self._LB_heat_percentiles[3]:
            index = 4
        elif heat <= self._LB_heat_percentiles[4]:
            index = 5
        else:
            index = 6
        return index


    def _empty_library_heataware(self,crtobj, sourcedrv=None, **kwargs):
        def check(obj):
            return obj.is_empty()

        def check_with_limit(obj):
            # libraryid: L_%02i_%i_%i_%03i # second is wall
            if obj.id[5] in limit_to_walls:
                return obj.is_empty()

        heat = crtobj.get_heat(kwargs['clock'])
        index = self.__get_capextention_starting_index(heat, kwargs.get('timecritial',False))

        limit_to_walls = []
        if 'walls' in kwargs:
            limit_to_walls  = kwargs['walls']
            check = check_with_limit

        self_libref_get = self._libraries_ref.get
        for i in range(index,7):
            for libid in self._libraries_ref.get_randomized_libids(i):
                obj = self_libref_get(libid)
                if check(obj):
                    return libid
        for i in reversed(range(1,index)):
            for libid in self._libraries_ref.get_randomized_libids(i):
                obj = self_libref_get(libid)
                if check(obj):
                    return libid


    def __opt2_check_cartridge_homecell_placement(self,clock):
        self._hotcold_classification_violators = []
        if len(self._drives_ref.keys())>0:
            self__crtcache_ref_get_ref = self._crtcache_ref.get
            for capid in range(1,4): # only check level 1 and 2
                #freecells = 0
                hclimit = self._LB_heat_percentiles[capid-1]
                for k,v in self._libraries_ref.items(capid):
                    crtobj = self__crtcache_ref_get_ref(v.get_crt(),None)
                    if crtobj:
                        heat = crtobj.get_heat(clock)
                        if heat > hclimit:
                            # colder than the threshold.
                            self._hotcold_classification_violators.append(crtobj.id)
                            #print self.id, capid, crtobj.id, heat
                        #else:
                        #    self._hotcold_classification_violators.append(crtobj.id)
                        #    return
                    #else:
                        #continue # disable about
                    #    freecells+=1
                    #    if freecells > 20:
                    #        self._hotcold_classification_violators=[]
                            #return # dont care, the sections are not jammed.
                    #        break
                if len(self._hotcold_classification_violators)>0:
                    #print self._hotcold_classification_violators
                    return

    def __opt2_get_action(self, clock):
        try:
            while len(self._hotcold_classification_violators):
                crtid = self._hotcold_classification_violators.pop()
                crtobj = self._crtcache_ref.get(crtid)
                if crtobj:
                    if crtobj.state == STATE_CRT_UNLOADED:
                        heat =crtobj.get_heat(clock)
                        curlib = crtobj.get_current_location()
                        kwargs = {'clock':clock }
                        if curlib[5] in ['0',"1"]:
                            kwargs['walls'] = ['0',"1"]
                        elif curlib[5] in ['3',"4"]:
                            kwargs['walls'] = ['3',"4"]
                        newlib = self._empty_library_heataware(crtobj, **kwargs)
                        if newlib:
                            if int(newlib[7])>int(curlib[7]):
                                self.log.error("Migrate crt %s to %s. It has heat %s",crtid,newlib,heat)
                                return {'crtid':crtid, 'targetlib':newlib}
        except:
            pass


class SL8500_Level_old:
    ## @brief one level of the SL8500 system
    #def __init__(self, id, datadir, eventlog, libraryplacement, externdrive_cb, migration_cache_put, migration_cache_get,**kwargs):
    def __init__(self, id, datadir, eventlog, **kwargs):
        if 'superstep' not in dir(self):
            self.superstep = self.step
        self.optimizationlevel=int(kwargs['optimizationlevel'])
        self.id=int(id)
        self._eventid_cnt = 1000000000
        self.log = logging.getLogger()
        self.globalclock = None
        self._drives = collections.OrderedDict()
        self._libraries = Libcontainer()
        self._cartridge_cache = {}
        self._cleaning_cartridges = []
        self.datadir = datadir
        self.eventlog = eventlog
        self._cleaning_tapes_idle_time = 0
        self.priority_queue = collections.deque(
            [Queue.PriorityQueue(),Queue.PriorityQueue()])

        self._robot_system = None
        self._out_of_HC_danger = False
        self.enqueue_robotevent = self.__enqueue_robotevent_simple
        self._get_free_library = self._empty_library_statichome

        self.log.info("Config:DEFAULT_NUM_ROBOTS:%s",DEFAULT_NUM_ROBOTS)

        self._modules_ = {}
        if self.optimizationlevel>0:
            self._modules_[MODULE_KEY_LOADBALANCER]=LoadBalancer(
                self.id,
                self.optimizationlevel,
                libraries = self._libraries,
                crtcache = self._cartridge_cache,
                drives = self._drives
            )
            self.get_loadbalancing_attributes =self._modules_[MODULE_KEY_LOADBALANCER].get_loadbalancing_attributes
            self._get_free_library = self._empty_library_heataware
        if self.optimizationlevel==4:
            self.enqueue_robotevent = self.__enqueue_robotevent_inject

    def push_priority_queue(self, event_inst):
        self.priority_queue[0].put((event_inst.priority, event_inst))

    def pushback_priority_queue(self, event_inst):
        self.priority_queue[1].put((event_inst.priority, event_inst))

    def pop_priority_queue(self):
        return self.priority_queue[0].get(False)

    # # # S T E P  # related

    def step(self, clock):
        # callbacks for performance improvements
        len_ref = len
        range_ref = range
        #self_push_cb = self.push
        self_push_cb = self.push_priority_queue
        self_handle_event_cb = self.__handle_event
        statesref = [STATE_SL8500_RBT_LOADREQ,STATE_SL8500_RBT_MIGRATE_AT_GATE]
        #self_active_events = self._active_events
        #self_active_events_popleft = self_active_events.popleft

        self.globalclock = clock

        #self.check_drive_hit()
        processed = True
        while processed:
            processed = False
            newevents = self._robot_system.superstep(clock)
            for d, obj in self._drives.iteritems():
                 r = obj.step(clock)
                 if r:
                     newevents.append(r)

            for event_inst in newevents:
                processed=True
                self.__handle_event(event_inst)
                #self.push_priority_queue(event)
            try:
                while True:
                    (priority, event_inst) = self.pop_priority_queue()
                    self.__handle_event(event_inst)
                    processed = True
            except Queue.Empty, e:
                # queue empty.
                pass
                # done break. The outer while loop will exit if nothing happend
        self.priority_queue.rotate()

        self.custom_step()
        idlerobs = self._robot_system.get_idle_robots()
        if len_ref(idlerobs) == self._robot_system.number_of_robots:
            self.__on_idle()
        else:
            self._cleaning_tapes_idle_time = 0


            #while len_ref(self_upcoming_events)>0:
        #    if clock >= self_upcoming_events[0].get('datetime'):
        #        self_push_cb(self_upcoming_events.popleft())
        #    else:
        #        break
        #self.log.error("%s:len events:%s, %s, upcomming %s"%(str(self),len(self._active_events),self.globalclock, len(self._upcomming_events)))
        # if 1:
        #     # check for mergeable requests
        #     crts = {}
        #     for evt in self_active_events:
        #         if evt.get('state') in statesref:
        #             crtid = evt.get('cartridgeid')
        #             if crtid not in crts:
        #                 crts[crtid] = []
        #             crts[crtid].append(evt)
        #     for k,v in crts.iteritems():
        #         for i in range_ref(1,len_ref(v)):
        #             v[0].set('readlength',  v[0].get('readlength')+v[i].get('readlength'))
        #             self_active_events.remove(v[i])
        # self.check_drive_hit()
        # while True:
        #     # superstep robots
        #     self_active_events.extendleft(self._robot_system.superstep(clock))
        #     # super step drives
        #     # retval are follow up events returned by the drive instances
        #     for d, obj in self._drives.iteritems():
        #         r = obj.step(clock)
        #         if r:
        #             self_active_events.appendleft(r)
        #     if len_ref(self_active_events)<=0:
        #         break
        #     while len_ref(self_active_events) > 0:
        #         self_handle_event_cb(self_active_events_popleft())
        #
        # self.custom_step()
        # idlerobs = self._robot_system.get_idle_robots()
        # if len_ref(idlerobs) == self._robot_system.number_of_robots:
        #     self.__on_idle()
        # else:
        #     self._cleaning_tapes_idle_time = 0
        #
        # self_active_events.extend(self._blocked_events)
        # self._blocked_events.clear()
        # # # collect stats
        # self.check_prefetching_applicability()

    def check_cleaning_tapes(self):
        available_cleaning_tapes = []
        for i in self._cleaning_cartridges:
            clnobj = self._cartridge_cache[i]
            if clnobj.state == STATE_CRT_UNLOADED:
                available_cleaning_tapes.append(i)
        if len(available_cleaning_tapes)>0:
            for d, obj in self._drives.iteritems():
                if obj.done_clean():
                    self.perform_cartridge_eviction(d)
                    # unload crt
                elif obj.do_clean():
                    cln_event_obj = self.event_factory(
                        "crt_read_request", {'datetime': self.globalclock,
                                             'cartridgeid': available_cleaning_tapes.pop(),
                                             'readlength': CLEANING_TAPE_DURATION,
                                             'driveid': d})
                    cln_event_obj.set('state',STATE_SL8500_RBT_LOADREQ)
                    self.__handle_event(cln_event_obj)
                    break

    def check_drive_hit(self):
        tmpevents = collections.deque()
        try:
            while True:
                tmpevents.append((self.pop_priority_queue()))
        except Queue.Empty:
            pass

        while len(tmpevents) > 0:
            (prio, evnt) = tmpevents.popleft()
            if evnt.name is 'crt_read_request':
                inst = self.get_cartridge(evnt.attributes.get('cartridgeid'))
                if inst:
                    if self.get_type(inst.get_current_location()) == 'drv' \
                            and inst.state == STATE_CRT_LOADED:
                        ## possible hit.
                        drvobj = self._drives[inst.get_current_location()]
                        drvobj.step(self.globalclock) ## updates drives global clock fist
                        if drvobj.can_extend_mounttime(evnt.attributes.get('cartridgeid')):
                            self.__handle_event(evnt)
                            continue
                            # this should mount the cartridge again
                            # cuntinue prevents the instance to be enqueued again
            self.push_priority_queue(evnt)

    def __handle_event(self, eventobj):
        try:
            s = eventobj.get('state')
            self.log.debug("%s-state:%s, name:%s, crt:%s",self.globalclock,s,eventobj.name, eventobj.get('cartridgeid'))
            if not s:
                if eventobj.name == "passthru_send":
                    return  self.handle_passthru_send(eventobj)
                else:
                    self.log.error("unhandled event name %s"%eventobj.name )
                    raise Exception("Unhandled event %s" % eventobj)
            if s == STATE_SL8500_RBT_LOADREQ:
                self.__handle_rbt_loadreq(eventobj)
            elif s == STATE_SL8500_RBT_LOADCOMPLETE:
                self.__handle_rbt_loadcomplete(eventobj)
            elif s == STATE_SL8500_DRV_READINGDONE:
                # drive should unmount volume
                self._handle_drv_readingdone(eventobj)
            elif s == STATE_SL8500_RBT_UNLOADCOMPLETE:
                self._handle_rbt_unloadcomplete(eventobj)
            elif s == STATE_SL8500_RBT_MIGRATE_AT_GATE:
                crtid = eventobj.get('cartridgeid')
                self.api_migration_cache_put(eventobj.get('passthru'),self._cartridge_cache[crtid])
                self._cartridge_cache[crtid] = None
                del self._cartridge_cache[crtid]
            elif s == STATE_SL8500_RBT_MIGRATE_RECEIVE:
                self.handle_cartridge_receive(eventobj)
            elif s == STATE_SL8500_RBT_LOADING:
                pass
                #self.log.debug("must be wrong doe to drive failure. restart")
                #eventobj.state = STATE_SL8500_RBT_LOADREQ
                #self._active_events.append(eventobj)
            elif s == STATE_SL8500_RBT_MOVE_COMPLETE:
                self._handle_move_complete(eventobj)
            else:
                self.log.error("unhandled event state %s" % s)
                raise Exception("Unhandled event %s"%eventobj)
        except CleaningTapeMigration, e:
            self.log.exception(e)
            self.log.warning("dropping event %s"%self.globalclock)
            #crtobj = self._cartridge_cache.get(eventobj.get('cartridgeid'),None)
            #if crtobj:
                #crtobj.
        except:
            raise

    def __handle_rbt_loadreq(self, event):
        # @brief process load request.
        # determines the current library location and finds a
        # target drive
        crtid = event.get('cartridgeid')
        crtobj = self._cartridge_cache.get(crtid,None)
        #crtobj = self.get_cartridge(crtid)
        if not crtobj:
            #for blocked in self._blocked_events:
            #    if blocked.get('cartridgeid')==crtid:
            #        if blocked.get('state') in (STATE_SL8500_RBT_MIGRATE_RECEIVE,STATE_SL8500_RBT_MIGRATE_AT_GATE):
            #            self.pushback_priority_queue(event)
            #            return
            self.log.warning("PUSHBACK:lsm:%i; cartridge not found, event:%s"%(self.id, str(event)))
            self.pushback_priority_queue(event)
            return
            #raise Exception("cartridge not found, event:%s"%str(event))
        ###self.log.debug("crt:%s in state %s"%(event.get('cartridgeid'), crtobj.get_state()))
        if crtobj.state == STATE_CRT_UNLOADED:
            # # generate load request event
            self.__perform_crt_load_request(event)
        else:
            # handle still loaded case / remount
            crtevnt = crtobj.get_current_event()
            if crtevnt:
                if crtevnt != event:
                    crtevnt_state = crtevnt.get('state')
                    #self.log.debug("already processing a different request %s: %s!=%s"%(crtid,crtevnt.get_id(), event.get_id()))
                    if crtevnt_state in (STATE_SL8500_RBT_LOADREQ,STATE_SL8500_RBT_LOADING):
                        crtevnt.set("readlength", crtevnt.get("readlength") + event.get("readlength"))
                        #xxself.cache_access(crtid)
                        #if not self.cache_access(crtid):
                        #    pass
                        #    ##self.log.debug("Cartridge %s not cached?!"%(event.get('cartridgeid')))
                        return
                    elif crtevnt_state in (STATE_SL8500_DRV_READING,STATE_SL8500_RBT_LOADCOMPLETE):
                        try:
                            self.perform_mount_extension(event)
                            #xxself.cache_access(crtid)
                        except CantExtendException, e:
                            self.log.warning("resetting event:%s"%event)
                            self._reset_load_request(event)
                        #if not self.cache_access(event.get('cartridgeid')):
                            #pass
                            ##self.log.debug("Cartridge %s not cached?!"%(event.get('cartridgeid')))
                        return
                    elif crtevnt_state in (STATE_SL8500_DRV_UNLOADREQ,STATE_SL8500_DRV_UNLOADING):
                        self.pushback_priority_queue(event)
                        return

            if crtobj.state == STATE_CRT_LOAD_RQST:
                if len(self._robot_system.get_idle_robots()) > 0:
                    # print "need to load the cartridge first"
                    current_library = crtobj.get_current_location()
                    driveid = event.get('driveid')
                    event.set('libraryid', current_library)
                    #self.log.debug("crt:%s, drive:%s, evt:%s",crtid,driveid,event)
                    if not driveid:
                        driveid, levelid, gateway = self.get_target_drive(sourcelibrary=current_library)
                        if gateway:
                            event.set('targetlevel', levelid)
                            event.set('sourcelevel', self.id)
                            event.set('gateway', gateway)
                    if driveid not in [None,False]:
                        ##self.log.debug("crt:%s, in lib %s, target drive %s"%( crtobj.id, current_library, driveid))
                        event.set('driveid', driveid)
                        return self.__handle_load_request_drive_defined(event)
                    else:
                        # # no eviction candidate
                        self.log.debug("No eviction candidate found: %s",str(event))
                        self.pushback_priority_queue(event)
                else:
                    # # cant do anything. robot system busy
                    self.pushback_priority_queue(event)
            elif crtobj.state == STATE_CRT_LOADED:
                self.perform_mount_complete(event)
            elif crtobj.state == STATE_CRT_MOUNTED:
                try:
                    self.perform_mount_extension(event)
                except CantExtendException, e:
                    self.log.warning("resetting event:%s"%event)
                    raise Exception("What to do about the cartridge state? is mounted %s"%crtobj.id)
                #self.log.warning("""crt %s already mounted""" % event.get('cartridgeid'))
            elif crtobj.state == STATE_CRT_UNLOAD_REQ:
                ##self.log.debug("%s: mount while unloading. shit...:%s"%(event.get_time(), event.get('cartridgeid')))
                self.pushback_priority_queue(event)
            else:
                self.log.warning("""crt %s not in unloaded or loadreq state"""\
                                 % event.get('cartridgeid'))
                self.pushback_priority_queue(event)

    def __handle_load_request_drive_defined(self, event):
        """
        Expects an event object with registered driveid
        :param event: crt load request with registered driveid
        :return:
        """
        try:
            crtid = event.get('cartridgeid')
            driveid = event.get('driveid')
            drvobj = self._drives.get(driveid,None)
            event.set('target', driveid)
            event.set('state', STATE_SL8500_RBT_LOADREQ)
            self.log.debug("%s:Load request: crt %s, drive %s",self.globalclock,crtid,driveid)
            if drvobj:
                if drvobj.state == STATE_DRV_DISABLED:
                    self.log.debug("drive is disabled, reset event %s"%event)
                    self._reset_load_request(event)
                    return
                # its a local drive
                self.prepare_drive_load(driveid,crtid)
                if drvobj.is_loaded():
                    #if drvobj.state != STATE_DRV_UNLOADING:
                        self.perform_cartridge_eviction(driveid, event)
                    #else:
                        ###self.log.debug("What is going on here:%s,%s"%(event,drvobj.get_id()))
                        #self._robot_system.enqueue_load_request(event)
                    #    self.block_event(event)
                elif drvobj.state == STATE_DRV_UNLOADING:
                    self.pushback_priority_queue(event)
                elif drvobj.is_expecting_crt(crtid):
                    # drive is empty
                    #drvobj.allocate(event.get('cartridgeid'))
                    #self.cache_insert(event.get('cartridgeid'))
                    #if self.cache_access():
                    #    self.log.warning("Why is there a eviction returned new crt %s: to evict %s"%(event.get('cartridgeid'),crt))
                    #    self.perform_cartridge_eviction(driveid, event)
                    #else:
                    crtobj = self.get_cartridge(crtid)
                    crtobj.set_current_event(event)
                    self.enqueue_robotevent(event)
                else:
                    self._reset_load_request(event)
            else:
                # cartridge migration necessary
                if crtid.startswith('CLN'):
                    raise CleaningTapeMigration("Shoulnt migrate cleaning tape %s"%crtid)
                self.log.debug("Migrating: %s from %s to %s via %s", event.get('cartridgeid'), event.get('sourcelevel'), event.get('target'), event.get('gateway'))
                self.api_issue_cartridge_migration(event)

        except DriveDisabled, e:
            self.log.exception(e)
            self._reset_load_request(event)
        except CacheFullException, e:
            self.log.exception(e)
            self._reset_load_request(event)
        except CartridgeBusyException, e:
            self.log.exception(e)
            self._reset_load_request(event)
        except:
            raise

    def _handle_move_complete(self, eventobj):
        crtid = eventobj.get('cartridgeid')
        crtobj = self._cartridge_cache.get(crtid,None)
        #crtobj = self.get_cartridge(crtid)
        if crtobj:
            crtobj.set_current_location(eventobj.get('target'))

            newlibobj = self._libraries.get(eventobj.get('target'))
            if newlibobj:
                newlibobj.process(eventobj)
            else:
                self.log.debug("Library %s not found, newlib of %s",eventobj.get('target'),crtid)
            oldlibobj = self._libraries.get(eventobj.get('libraryid'))
            if oldlibobj:
                oldlibobj.process(eventobj)
            else:
                self.log.debug("Library %s not found, oldlib of %s",eventobj.get('libraryid'),crtid)
            self.log.info("Migration of %s from %s -> %s done, %s",crtid,oldlibobj.id, newlibobj.id,self.globalclock)
        else:
            raise  Exception("unknown cartridge %s, move complete function", crtid)

    def _reset_load_request(self, event):
        event.set('target', None)
        event.set('driveid', None)
        event.set('state',STATE_SL8500_RBT_LOADREQ)

        self.push_priority_queue(event)
        self.log.debug("reinitialize request")

    def __handle_rbt_loadcomplete(self, eventobj):
        try:
            ##self.log.debug("handle %s" % str(eventobj))
            #self.stat.register_load_complete(self.globalclock, eventobj)
            # applying the changes to the drive, cartridge and home library state
            # machines
            drvid = eventobj.get('driveid')
            drvobj = self._drives[drvid]
            crtid = eventobj.get('cartridgeid')
            libraryid = eventobj.get('libraryid',None)
            passthru = eventobj.get('passthru',None)

            if self.api_migration_cache_get(libraryid,True) or self.api_migration_cache_get(passthru,True) or self.api_migration_cache_get(eventobj.get('gateway'),True):
                self.log.debug("%s: Clearing elevator/passthru %s",self.globalclock,libraryid)
                ## deallocated pass thru or elevator.
                ## was the previous library freed?
            else:
                self.__deallocate_library_slot(libraryid, crtid)
                ## deallocated the library slot

            if drvobj.state == STATE_DRV_DISABLED:
                #pass
                #raise Exception("mounted to disabled drive")
                ##olddate = eventobj.get('datetime') - datetime.timedelta(seconds=eventobj.get('load_completion_latency'))
                newevent = self.event_factory('crt_read_request', {
                    #'datetime':olddate
                    'datetime':eventobj.get('datetime'),
                    'cartridgeid':crtid,
                    'state':STATE_SL8500_RBT_LOADREQ,
                    'readlength':eventobj.get('readlength'),
                    'driveid':None })
                self._active_events.append(newevent)
                crtobj = self._cartridge_cache.get(crtid)
                crtobj.set_current_location(eventobj.get('target'))
                crtobj.set_current_event(newevent)

            else:
                #drvobj.allocate(eventobj.get('cartridgeid'))
                drvobj.process(self.event_factory("robot_mount", {
                    'cartridgeid': crtid,
                    'libraryid': eventobj.get('libraryid')}))
                # drive loaded
                drvobj.process(self.event_factory("exiting_pvl_mountadd",
                    {'cartridgeid': crtid}))
                # volume mounted start reading the device
                #crtobj = self.get_cartridge(eventobj.get('cartridgeid'))
                crtobj = self._cartridge_cache.get(crtid)
                crtobj.process(self.event_factory('robot_mount',
                    {'libraryid': eventobj.get('libraryid'),'drive': drvid}))
                # cartridge loaded
                crtobj.process(self.event_factory("exiting_pvl_mountadd",
                                       {'drive': drvid}))
                # cartridge volume mounted
                # drive is reading the cartridge
                eventobj.set('state', STATE_SL8500_DRV_READING)
                drvobj.register_current_event(eventobj)
                #self.cache_insert(eventobj.get('cartridgeid'))
                #if not self.cache_access(eventobj.get('cartridgeid')):
                #    self.log.error("why is %s not cached"%(eventobj.get('cartridgeid')))
                #    raise Exception("unwanted eviction")
                # ## done ###
        except:
            self.log.error("some exception occured:%s"%eventobj)
            raise

    def _empty_drive_minid(self, **kwargs):
        for k,v in sorted(self._drives.items()):
            if v.is_empty():
                return k

    def prepare_drive_load(self, driveid, crtid):
        """
        After this function crtid is allocated to the given drive and the
        existing cartridge has been removed from cache.
        :param driveid: drive to load crt to
        :param crtid:  crt to be loaded
        :return: None
        """
        self.log.debug("%s:drivid:%s, crtid:%s"%(self.globalclock, driveid, crtid))
        drvobj = self._drives[driveid]
        if drvobj.state != STATE_DRV_DISABLED:
            #xxcurcrt = drvobj.get_current_cartridge()
            #xxif curcrt:
                #self.perform_cartridge_eviction(driveid)
            #xx    self.cache_delete(curcrt)
            #xxif not self.cache_access(crtid):
            #xx    self.cache_insert(crtid)
            drvobj.allocate(crtid)
            return True
        else:
            raise DriveDisabled("Drive %s disabled, cant prepare load of %s"%(driveid,crtid))

    def __deallocate_library_slot(self,libraryid, crtid):
        libobj = self._libraries.get(libraryid)
        if libobj: # does not exist if crt in passthru
            libobj.process(self.event_factory('robot_mount', {'cartridgeid': crtid}))
            # library empty now
        else:
            self.log.warning("Library object not found %s, cant deallocate %s. Musst be a PT, EL"%(libraryid, crtid))
            if not self.api_migration_cache_get(libraryid,True):
                self.log.warning("could not deallocate PTEL:%s",libraryid)
            #raise Exception("Library object not found %s, cant deallocate %s"%(libraryid, crtid))

    def __perform_crt_load_request(self, eventobj):
        # @brief create a pvl mountadd request and process it at crt
        # @param crtid: cartridge id to be loaded
        # @return returns what crtobj.process returns
        crtid = eventobj.get('cartridgeid')
        crtobj = self.get_cartridge(crtid)
        self.log.debug("create and process mountadd request for %s", crtid)
        if not crtobj:
            self.log.error("crt %s not found, event:%s"%(crtid,eventobj))
            raise Exception("crt %s not found"%crtid)
        if crtobj.state != STATE_CRT_UNLOADED:
            self.pushback_priority_queue(eventobj)
            ##self.log.debug("crt %s not in unloaded state... %s"%(crtid, crtobj.get_state()))
        else:
            try:
                drvid = eventobj.get('driveid')
                if drvid:
                    self.prepare_drive_load(drvid, crtid)
                crtobj.process(self.event_factory("entering_pvl_mountadd", {}))
                crtobj.set_current_event(eventobj)
                self.push_priority_queue(eventobj)
            except Exception, e:
                self.log.exception(e)
                self._reset_load_request(eventobj)

    def process_hourly(self):
        self._cleaning_cartridges = filter(lambda x: x.startswith('CLN'), self._cartridge_cache.keys())
        self._out_of_HC_danger = True
        # recheck every hour
        if MODULE_KEY_LOADBALANCER in self._modules_:
            self._modules_[MODULE_KEY_LOADBALANCER].process_hourly(self.globalclock)
        #self.idle_action_validate_drives()

        drives = 0
        for k,v in self._drives.iteritems():
            if v.state != STATE_DRV_DISABLED:
                drives+=1

        return {
            'lsm':self.id,
            'drives':drives,
            'crt':len(self._cartridge_cache),
            'homecells': self._libraries.empty_count()
        }

    def idle_action_validate_drives(self):
        clock = self.globalclock
        mnt=0
        load=0
        empty=0
        disabl=0
        for drvid, drvobj in self._drives.iteritems():
            lastum = drvobj.get_last_unmount_timestamp()
            if lastum:
                diff = tiny_helper.get_diff_in_seconds(lastum, clock)
                if diff > 3600:
                    self.log.warning("DRVCHECK:Drive:%s:State:%s;CRT:%s; idle for %s seconds"%(drvid, drvobj.state, drvobj.get_current_cartridge(),diff))
            #else:
            #    if drvobj.state!=STATE_DRV_MOUNTED:
            #        self.log.warning("DRVCHECK:Drive:%s:State:%s;CRT:%s; no unmount timestamp, drive must be empty"%(drvid, drvobj.state, drvobj.get_current_cartridge()))
            if drvobj.state==STATE_DRV_MOUNTED:
                crtid = drvobj.get_current_cartridge()
                crtobj = self.get_cartridge(crtid)
                crtloc =crtobj.get_current_location()
                mnt+=1
                if crtloc!=drvid:
                    self.log.error("DRVCHECK:Drive:%s:State:%s;CRT:%s;CRTState:%s;CRT location:%s"%(drvid, drvobj.state, crtid, crtobj.state, crtloc))
            elif drvobj.state==STATE_DRV_LOADED:
                load+=1
            elif drvobj.state==STATE_DRV_EMPTY:
                empty+=1
            elif drvobj.state==STATE_DRV_DISABLED:
                disabl += 1
        self.log.warning("DRVCHECK:States: MNT:%s, LOAD:%s, EMPTY:%s, DISBL:%s;\tQUEUE length: active:%s, blocked:%s"%(mnt,load,empty,disabl,len(self._active_events),len(self._blocked_events)))

    def drive_action(self, driveid, action):
        def localpush(event):
            if event.name == 'crt_read_request':
                event.set('state',STATE_SL8500_RBT_LOADREQ)
            event.delete('driveid')
            event.delete('target')
            #event.delete('follow_up_event')

            crtobj = self.get_cartridge(event.get('cartridgeid'))
            crtobj.state = STATE_CRT_UNLOADED
            self.log.debug("localpush event:%s"%str(event))
            self.push_priority_queue(event)

        if action == 'drive_disabled':
            drvobj = self._drives.get(driveid, None)
            if drvobj:
                curcrt = drvobj.get_current_cartridge()
                self.log.debug("drive %s, crt:%s"%(driveid,curcrt))
                if curcrt:
                    #xxself.cache_delete(curcrt)
                    crtobj = self._cartridge_cache.get(curcrt)
                    if crtobj is None:
                        self.log.warning("Why is there no crt %s, drive disabled:%s",curcrt,driveid)
                        pass
                    else:
                        curevnt = drvobj.get_current_event()
                        if curevnt:
                            if curevnt.name == 'crt_read_request':
                                if not curcrt.startswith('CLN'):
                                    already_read__secs = tiny_helper.get_diff_in_seconds(curevnt.get('datetime'), self.globalclock)
                                    newevent = self.event_factory("crt_read_request",
                                        {'cartridgeid':curcrt,'readlength': curevnt.get('readlength')-already_read__secs},
                                        event_priority=curevnt.priority)
                                    pt = curevnt.get('passthru')
                                    if pt:
                                        self.log.info("interrupted event in PT %s",pt)
                                        newevent.set('passthru', pt)
                                    newevent.set('state', STATE_SL8500_RBT_LOADREQ)
                                    self.pushback_priority_queue(newevent)
                            elif curevnt.name == 'entering_pvr_dismountcart':
                                self.log.warning("Dropping event %s"%str(curevnt))
                                pass
                            else:
                                raise Exception("what event %s"%str(curevnt))
                        if crtobj.state == STATE_CRT_MOUNTED:
                            self._handle_drv_readingdone(curevnt)
                        if crtobj.state == STATE_CRT_LOADED:
                            crtobj.process(self.event_factory("entering_pvr_dismountcart",
                                {'cartridgeid': curcrt, 'drive': driveid}))
                        current_crt_location = crtobj.get_current_location()
                        if self.get_type(current_crt_location) == 'drv':
                            freelib = self._get_free_library(crtobj.id)

                            crtobj.process(self.event_factory('robot_dismount',
                                           {'driveid': driveid, 'libraryid': freelib}))
                            crtobj.set_current_location(freelib)
                            libobj = self._libraries.get(freelib)
                            libobj.process(self.event_factory('robot_dismount',
                                        {'cartridgeid': curcrt, 'driveid': driveid}))
                        else:
                            self.log.debug("Crt:%s in %s, no need to relocate."%(curcrt,current_crt_location))
                        crtobj.set_current_event(None)
                for alloc in drvobj._simulation_cache_allocated:
                    self.log.debug("removing from allocation list %s"%(alloc))
                    #xxself.cache_delete(alloc)
                drvobj.process(self.event_factory('warn_drive_disabled', {}))
                for evt in self._robot_system.cancel_events(driveid):
                    self.log.debug("robot system canceled:%s"%str(evt))
                    if evt.name == 'robot_dismount':
                        followup = evt.get('follow_up_event')
                        if followup:
                            localpush(followup)
                            #self.push(followup)
                        continue
                    elif evt.name == 'crt_read_request':
                        followup = evt.get('follow_up_event')
                        if followup:
                            evt.delete('follow_up_event')
                            if followup.name != 'robot_dismount':
                                localpush(followup)
                            else:
                                crtobj = self._cartridge_cache[followup.get('cartridgeid')]
                                drvobj = self._drives[followup.get('driveid')]
                                libobj = self._libraries.get(followup.get('libraryid'))
                                libobj.deallocate(crtobj.id)

                                cancel = self.event_factory('cancel_event',{'cartridgeid':crtobj.id, 'driveid':drvobj.id, 'libraryid':libobj.id})
                                if crtobj.state==STATE_CRT_UNLOAD_REQ:
                                    crtobj.process(cancel)
                                if drvobj.state ==STATE_DRV_UNLOADING:
                                    drvobj.process(cancel)

                    localpush(evt)
                    #self.push(evt)
            #else:
            #    raise Exception("Unknown drive id %s"%driveid)
        elif action == "drive_enabled":
            drvobj = self._drives.get(driveid, None)
            if not drvobj:
                self._drives[driveid] = DriveClass(driveid, self.datadir,self.eventlog,
                    simulationmode=False,
                    librarylatency=self._robot_system.move_to_costs)
                drvobj = self._drives[driveid]
                drvobj.process(self.event_factory('drive_enabled', {}))
            else:
                if drvobj.state == STATE_DRV_DISABLED:
                    #raise Exception("")
                    drvobj.process(self.event_factory('drive_enabled', {}))
                    allocated = self._robot_system.check_driveallocation(driveid)
                    if allocated:
                        drvobj.allocate(allocated)
                        self.log.debug("reallocated %s to %s"%(allocated, driveid))
        else:
            #unknown action.
            pass
        #xxself.cache_update_size()

    def flush(self):
        try:
            for drvobj in self._drives.values():
                drvobj.flush()
            for libobj in self._libraries.values():
                libobj.flush()
            for crtid in self._cartridge_cache.keys():
                crtobj = self.get_cartridge(crtid,False)
                crtobj.flush()
        except: #debug breakpoint
            raise

    def event_factory(self, event_name, atts={}, event_priority=None):
        ##
        # @brief create a new event instance
        # @param event_name: name of the event
        # @param atts: dictionary of the associated attributes
        # @param id: id of the event
        # @return Event instance
        # @todo should i consider the given id at all?
        if 'datetime' not in atts:
            atts['datetime'] = self.globalclock
        if not event_priority:
            self._eventid_cnt += 1
            event_priority=self._eventid_cnt
        return SimulationEvent("Lvl",event_priority, event_name, atts)

    def custom_step(self):
        pass

    def __on_idle(self):
        self._cleaning_tapes_idle_time += 1
        if self._cleaning_tapes_idle_time >= 10:
            self.check_cleaning_tapes()

        if MODULE_KEY_LOADBALANCER in self._modules_:
            lbmod = self._modules_[MODULE_KEY_LOADBALANCER]
            action = lbmod.get_action(self.globalclock)
            if action:
                #print "MIGRATE ", action
                self.perform_cartridge_migration_intern(action)
            if self.optimizationlevel>=4:
                if self.__cnt_empty_drives()==0:
                    eviction_candidate = self._get_eviction_candidate()
                    if eviction_candidate:
                        res = self.perform_cartridge_eviction(eviction_candidate)
                        self.log.debug("eviction drive %s, to restore one free drive",eviction_candidate)


    def load_config(self, args):
        # @brief initialize system based on resdict provided by sl8500 class
        (res, defaults) = args
        capacityextensions = len(res['libraries'])/440
        #self._robot_system = RobotSystem(res.get('robots',DEFAULT_NUM_ROBOTS),capacityextensions)
        self._robot_system = RobotSystem(self.id, DEFAULT_NUM_ROBOTS,capacityextensions)

        for k, v in res['drives'].iteritems():
            self._drives[k] = DriveClass(k, self.datadir,self.eventlog,
                simulationmode=False,
                librarylatency=self._robot_system.move_to_costs)

        # libraries
        for k, v in res['libraries'].iteritems():
            self._libraries.add(k,LibraryClass(k, self.datadir, self.eventlog,simulationmode=True ))

        driveenable = self.event_factory("drive_enabled")
        libraryenable = self.event_factory("lib_init")
        for drvid, obj in self._drives.iteritems():
            obj.process(driveenable)
        for obj in self._libraries.values():
            obj.process(libraryenable)

        for i in range(0,1):
            self.init_cartridge("CLN%02i%02i"%(int(self.id), i))
        #xxself.cache_update_size()

    def init_cartridge(self, crtid):
        ##
        # @bried Initialize a new cartridge. A free library is detected and
        # the crt loaded accordingly.
        # @param crtid: cartridge id as a string
        crtobj = CartridgeClass(crtid, self.datadir, self.eventlog, simulationmode=False)
        self._cartridge_cache[crtid] = crtobj
        libid = self._get_free_library(crtid)
        if not libid:
            self.log.error("NO free library available, how is this possible?")
            raise Exception("No library free")
        libobj = self._libraries.get(libid)
        libobj.process(
            self.event_factory("robot_enter", {'cartridgeid': crtid}))
        crtobj.process(
            self.event_factory("robot_enter", {'libraryid': libid}))
        return crtobj

    def finalize(self):
        for crtid in self._cartridge_cache.keys():
            crtobj = self.get_cartridge(crtid)
            if crtobj.state != STATE_CRT_UNLOADED:
                libid = self._get_free_library(crtid)
                driveid = crtobj.get_current_location()
                crtobj.process(self.event_factory("entering_pvr_dismountcart",
                    {'cartridgeid': crtid, 'drive': driveid}))
                crtobj.process(self.event_factory('robot_dismount',
                    {'driveid': driveid, 'libraryid': libid}))
            crtobj.close()

    def handle_passthru_send(self, eventobj):
        inst = self.get_cartridge(eventobj.get('cartridgeid'))
        if inst:
            location = inst.get_current_location()
            #if self.get_type(location) != 'lib':
            #    self.log.error("Where is the cartridge?, %s, location:%s"%(inst,location))
            #    raise Exception("Where is the cartridge?, %s, location:%s"%(inst,location))
            eventobj.set('libraryid', location)
            eventobj.set('target',eventobj.get('passthru'))
            eventobj.set('state', STATE_SL8500_RBT_MIGRATE_SEND)

            self.__deallocate_library_slot(location, eventobj.get('cartridgeid'))
            self._libraries.deallocate(crtid=eventobj.get('cartridgeid'))
            #libraryobj = self._libraries.get(location)
            #if libraryobj:
            #    libraryobj.process(self.event_factory('robot_eject',{'cartridgeid':eventobj.get('cartridgeid')}))
            self.enqueue_robotevent(eventobj)
        else:
            self.log.error("Crt not available :%s"%eventobj)
            raise Exception("Cartridge object not available")

    def handle_cartridge_receive(self, eventobj):
        crtobj = self.api_migration_cache_get(eventobj.get('passthru'),False)
        if crtobj:
            self.log.debug("crt %s arrived at %s"%(crtobj.id, eventobj.get('passthru')))
            self._cartridge_cache[crtobj.id] = crtobj
            crtobj.set_current_location(eventobj.get('passthru'))
            followup = eventobj.get('follow_up_event')
            if followup:
                # # check whether drive is still empty
                targetdrive = followup.get('driveid')
                if not self._verify_drive_is_expecting_mount(crtobj.id, targetdrive):
                    followup.set('driveid', None)
                    followup.set('target', None)
                    followup.set('state', STATE_SL8500_RBT_LOADREQ)
                    followup.set('libraryid',eventobj.get('passthru'))
                    self.log.warning("Drive %s did not expect %s to be loaded, reset event",targetdrive,crtobj.id)
                    #return self._reset_load_request(followup)
                    #xxself.cache_delete(crtobj.id)
                self.push_priority_queue(followup)
            else:
                self.log.error("no follow up event... %s"%(eventobj))
                raise Exception("Why is there no event.")
        else:
            # # not in passthru yet.
            self.pushback_priority_queue(eventobj)

    def _verify_drive_is_expecting_mount(self, crtid, driveid):
        driveobj = self._drives.get(driveid)
        return driveobj.is_expecting_crt(crtid)

    def get_number_of_free_libraries(self):
        return len(self._libraries)-len(self._cartridge_cache)

    def get_type(self, id):
        # @brief return the type of the id
        # @param id either a cartridge, drive or library id
        # @return 'crt', 'drv', 'lib' or None
        if id in self._drives:
            return 'drv'
        if self._libraries.exists(id):
            return 'lib'
        #if id in self._cartridge_cache:
        #    return 'crt'
        return None

    def get_cartridge(self, crtid, initialize=True):
        # @brief get cartridge instance by id
        # @param cartridge id
        # @return cartridge class instance, or none if id not registered
        try:
            return self._cartridge_cache.get(crtid,None)
        except:
            return None

    def _empty_library_statichome(self, crtid, sourcedrv=None,**kwargs):
        return self._libraries.get_home_cell(crtid)

    def _empty_library_minlatency(self, crtid=None, sourcedrv=None, **kwargs):
        try:
            freerobots = self._robot_system.get_idle_robots()
            allowedwall = set()
            for robot in freerobots:
                if robot.id == 0:
                    allowedwall.add("0")
                    allowedwall.add("1")
                    allowedwall.add("2")
                    if not self._robot_system.robot_right:
                        allowedwall.add("4")
                        allowedwall.add("3")
                elif robot.id == 1:
                    allowedwall.add("4")
                    allowedwall.add("3")
                    allowedwall.add("2")
            minlat_libid = None
            minlat = sys.maxint
            if not sourcedrv:
                sourcedrv = random.choice(self._drives.keys())
            for i in range(1,7):
                for libid, obj in self._libraries.items(i,allowedwall):
                    if obj.is_empty():
                        if minlat_libid==None:
                            minlat_libid=libid  #
                        latency = self._robot_system.move_to_costs(libid,sourcedrv, self.id)
                        if minlat > latency:
                            minlat = latency
                            minlat_libid = libid
                    if minlat < sys.maxint:
                        return minlat_libid
        except:
            for i in range(1,7):
                for libid, obj in self._libraries.items(i):
                    if obj.is_empty():
                        return libid
        return minlat_libid

    def _empty_library_heataware(self,crtid=None, sourcedrv=None, **kwargs):
        crtobj = self.get_cartridge(crtid)
        if crtobj:
            kwargs['clock']=self.globalclock
            return self._modules_[MODULE_KEY_LOADBALANCER]._empty_library_heataware(crtobj, sourcedrv, **kwargs)

    def _empty_drive_minlatency(self, crtid, sourcelib, **kwargs):
        if sourcelib in self._libraries:
            libobj = self._libraries[sourcelib]
            for (driveid, latency) in libobj.get_list_of_drives_by_latencies():
                drvobj = self._drives[driveid]
                if drvobj.is_empty():
                    return drvobj.get_id()

    def get_latency(self, source, target):
        return self._robot_system.move_to_costs(source,target)

    def get_drives_by_state(self, state):
        ret = []
        for drvid, obj in self._drives.iteritems():
            if obj.state == state:
                ret.append(drvid)
        return ret

    def get_drives_in_loaded_state(self):
        return self.get_drives_by_state(STATE_DRV_LOADED)

    def get_libraries_by_state(self, state):
        # @brief return all library ids with ids in state 'state'
        # @return list of library ids in given state
        ret = []
        for libid, obj in self._libraries.items():
            if obj.state == state:
                ret.append(libid)
        return ret

    # ## State handler ###
    def _handle_drv_readingdone(self, eventobj):
        drvobj = self._drives[eventobj.get('driveid')]
        drvobj.process(self.event_factory(
            "exiting_pvl_dismountvolume",
            {'cartridgeid': eventobj.get('cartridgeid')}))
        crtobj = self.get_cartridge(eventobj.get('cartridgeid'))

        crtobj.process(self.event_factory(
            "exiting_pvl_dismountvolume", {'drive': eventobj.get('driveid')}))
        #if 1:
        #    secs = get_diff_in_seconds(crtobj.tmp['mount_operations'][-1][0], crtobj.tmp['mount_operations'][-1][1])
        #    if crtobj._simulation_cache['current_event'].get('readlength') != secs:
        #        raise Exception("Wrong read time")
        #xxself.cache_unlock(crtobj.id)
        self.log.debug("event %s done at %s.", eventobj.get_id(), self.globalclock)

    def _handle_rbt_unloadcomplete(self, eventobj):
        try:
            self.log.debug("clock:%s, id:%s, crt:%s" % (self.globalclock, eventobj.get_id(), eventobj.get('cartridgeid')))
            driveid = eventobj.get('driveid')
            libid = eventobj.get('libraryid')
            crtid = eventobj.get('cartridgeid')
            #xxself.cache_unlock(crtid)
            crtobj = self.get_cartridge(crtid)
            crtobj.process(
                self.event_factory('robot_dismount',
                                   {'driveid': driveid, 'libraryid': libid}))

            drvobj = self._drives.get(driveid)
            drvobj.process(self.event_factory(
                'robot_dismount', {'cartridgeid': crtid, 'libraryid': libid}))
            drvobj.deallocate(crtid)
            #xxself.cache_delete(crtid)
            libobj = self._libraries.get(libid)
            libobj.process(self.event_factory(
                'robot_dismount', {'cartridgeid': crtid, 'driveid': driveid}))
            # in case there was a disable drive event previously
            #allocated = self._robot_system.check_driveallocation(driveid)
            #followup = eventobj.get('follow_up_event')
            #if followup:
            #    drvobj.allocate(followup.get('cartridgeid'))
            #    self.log.debug("reallocated %s to %s"%(followup.get('cartridgeid'), driveid))
            # not allows don't know when this will finish
            #self._cartridge_cache[crtid] = crtobj.get_current_location() # delete object
        except DeallocationError, e:
            #self.d
            raise e

    def perform_cartridge_eviction(self, driveid, event=None):
        kwargs = {}
        if event:
            kwargs['timecritical']=True
            kwargs['walls'] = self.__get_walls_of_event(event)
        unloadevent = self.__perform_cartridge_eviction__check_and_prepare(driveid,**kwargs)
        if unloadevent:
            if event:
                unloadevent.set("follow_up_event", event)
                drvobj = self._drives.get(driveid)
                drvobj.allocate(event.get('cartridgeid'))
            self.enqueue_robotevent(unloadevent)
            return True


    def __perform_cartridge_eviction__check_and_prepare(self, driveid, **kwargs):
        self.log.debug("drive id:%s" % driveid)
        drvobj = self._drives[driveid]
        if not drvobj:
            self.log.error("%s,unknwon id %s, exiting" %(self.globalclock,driveid))
            return False
        if drvobj.state in (STATE_DRV_UNLOADING, STATE_DRV_EMPTY):
            return False
        crtid = drvobj.get_current_cartridge()
        if not crtid:
            self.log.debug("%s, no cartridge found, %s"%(self.globalclock,driveid))
            return False
        crtobj = self.get_cartridge(crtid)
        if not crtobj:
            self.log.debug("no crt found",crtid)
            return False
        crtstate = crtobj.state
        if crtstate == STATE_CRT_LOAD_RQST:# wieso true und nicht false
            return False
        if crtstate == STATE_CRT_UNLOADED:
            drvobj.reset()
            return False
        if crtstate != STATE_CRT_LOADED:
            self.log.debug("crt %s not loaded in drive %s, cant evict, drivestate:%s"%(crtid, driveid, drvobj.state))
            raise CartridgeBusyException("Cant evict crt %s from %s in state %s",crtid,driveid,crtstate)
            return False

        libid = self._get_free_library(crtid, driveid, **kwargs)
        if libid:
            #unloadevent = None
            self.log.debug("evicting crt %s from drive %s to library %s" % (crtid, driveid, libid))
            crtobj.process(self.event_factory(
                "entering_pvr_dismountcart",
                {'cartridgeid': crtid, 'drive': driveid}))
            evt = self.event_factory("entering_pvr_dismountcart", {
                'cartridgeid': crtid,
                'drive': driveid,
                'state': STATE_SL8500_DRV_UNLOADING})
            drvobj.process(evt)
            drvobj.register_current_event(evt)
            self.log.debug("drive:%s, crt:%s, pvr dismount issued, now robotdismnt to libid %s", driveid, crtid, libid)
            libobj = self._libraries.get(libid)
            libobj.allocate(crtid)

            return self.event_factory(
                "robot_dismount", {
                    'cartridgeid': crtid,
                    'driveid': driveid,
                    'libraryid': libid,
                    'target': libid,
                    'state': STATE_SL8500_DRV_UNLOADREQ})
        else:
            self.log.error("No free library available in level %s", self.id)
        return False

    def perform_mount_complete(self,event):
        crtobj = self.get_cartridge(event.get('cartridgeid'))
        #xxself.cache_access(event.get('cartridgeid'))
        drvid = crtobj.get_current_location()
        #todo measure time here
        ##self.log.debug("cartidge %s already loaded in %s. remount it." % (crtobj.get_id(), drvid))
        if drvid:
            drvobj = self._drives[drvid]
            crtobj.process(self.event_factory('exiting_pvl_mount_complete',
                {'drive': drvid}))
            crtobj.set_current_event(event)
            drvobj.process(self.event_factory('exiting_pvl_mount_complete',
                {'cartridgeid': crtobj.id}))
            event.set('state', STATE_SL8500_DRV_READING)
            event.set('driveid', drvid)
            drvobj.register_current_event(event)
            #drvobj.handle_mounted(self.globalclock, event)
            #self.stat.register_mount(self.globalclock)
        else:
            self.log.error("Cartridge not in a drive")
            raise "not in drive"

    def perform_mount_extension(self, event):
        # # @brief read request arrived while beeing mounted,
        # extend reading
        crtobj = self.get_cartridge(event.get('cartridgeid'))
        if not crtobj:
            raise CartridgeNotFoundException("Event:%s"%event)
        if crtobj.state == STATE_CRT_LOAD_RQST:
            crtobj.extend_reading_time_of_current_request(event.get('readlength'))
        elif crtobj.state == STATE_CRT_MOUNTED:
            drvid = crtobj.get_current_location()

            self.log.debug("Crt %s extend mount"%crtobj.id)
            if drvid:
                #xxif not self.cache_access(event.get('cartridgeid')):
                #xx    self.log.error("Why is %s not cached"%event.get('cartridgeid'))
                #xx    raise CantExtendException("Why is %s not cached"%event.get('cartridgeid'))
                drvobj = self._drives[drvid]
                if drvobj.get_current_cartridge()==crtobj.id:
                    drvobj.extend_mounttime(event.get('readlength'))
                else:
                    self.log.warning("Cartridge %s not mounted in drive %s"%(crtobj.id, drvid))
                    raise CantExtendException("Cartridge %s not mounted in drive %s"%(crtobj.id, drvid))
            else:
                self.log.error("Crt %s not loaded. cant extend mount"%crtobj.id)
                raise CantExtendException("Crt %s not loaded. cant extend mount"%crtobj.id)
        else:
            raise Exception("Unhandled Cartridge State %s"%crtobj.state)

    def librarystate(self):
        free_libs = 0
        for lib in self._libraries.values():
            if lib.is_empty():
                free_libs += 1
        return "Free:%s, Total:%s"%(free_libs,len(self._libraries))

    def crt_action(self, event):
        cartridgeid = event.get('cartridgeid')
        self.log.debug("CRT %s action %s"%(cartridgeid,event.name))
        crtobj = self.get_cartridge(cartridgeid)
        if not crtobj:
            raise CartridgeNotFoundException("%s, %s"%(cartridgeid,event))
        state = crtobj.state
        if state == STATE_CRT_UNLOADED:
            homecell = crtobj.get_current_location()
            if not homecell:
                raise HomeCellNotFound("Crtid %s"%cartridgeid)
            hcobj = self._libraries.get(homecell)
            self._libraries.deallocate(cartridgeid)
            crtobj.process(self.event_factory(
                "robot_eject", {'cartridgeid': cartridgeid}))
            crtobj.close()
            hcobj.process(self.event_factory(
                'robot_eject', {'cartridgeid': cartridgeid}))
            del self._cartridge_cache[cartridgeid]
            #xxself.cache_delete(cartridgeid)
            self.log.debug("removed cartridge %s"%cartridgeid)
            return True
        #self.log.error("Why am i here?")
        #return False
        return True

    def get_crt_per_drive_ratio(self):
        try:
            active_drives = 0.0
            for k,v in self._drives.iteritems():
                if v.state != STATE_DRV_DISABLED:
                    active_drives += 1
            return len(self._cartridge_cache)/active_drives
        except:
            return sys.maxint

    def perform_cartridge_migration_intern(self, parameter):
        crtid = parameter['crtid']
        newlib = parameter['targetlib']
        self.log.info("crt migration: crt:%s, newlib:%s",crtid, newlib)
        crtobj = self._cartridge_cache.get(crtid)
        if crtobj:
            curlib = crtobj.get_current_location()
            if not curlib:
                self.log.error("location of %s unknown", crtid)
            targetlibobj = self._libraries.get(newlib)
            if targetlibobj:
                targetlibobj.allocate(crtid)

                robot_move_event = self.event_factory(
                "robot_move", {
                    'cartridgeid': crtid,
                    'libraryid': curlib,
                    'target': newlib,
                    'state':STATE_SL8500_RBT_MOVE})
                crtobj.process(robot_move_event)
                crtobj.set_current_event(robot_move_event)
                self.log.info("migrate %s from %s -> %s, %s",crtid,curlib,newlib,self.globalclock)
                self.enqueue_robotevent(robot_move_event)

        else:
            self.log.warning("crtid %s not in this lsm", crtid)

    def __enqueue_robotevent_simple(self, event):
        self._robot_system.enqueue(event)

    def __enqueue_robotevent_inject(self, event):
        if event.name == "crt_read_request":
            targetdrv = event.get('driveid')
            if int(targetdrv[2:4]) == self.id: # otherwise its a migration
                followup =  event.get('follow_up_event')
                if not followup and self.__cnt_empty_drives()==0:
                    crtobj = self._cartridge_cache.get(event.get('cartridgeid'))
                    kwargs = {'walls':self.__get_walls_of_event(event)}
                    evictioncandidate = self._get_eviction_candidate()
                    if evictioncandidate:
                        unloadevent = self.__perform_cartridge_eviction__check_and_prepare(evictioncandidate,**kwargs)
                        if isinstance(unloadevent, Event):
                            event.set("follow_up_event", unloadevent)
                            self.log.debug("Appended followupevent to %s read request. Unloading %s",crtobj.id,unloadevent.get('cartridgeid'))

        self._robot_system.enqueue(event)

    def __get_walls_of_event(self, event):
        crtobj = self._cartridge_cache.get(event.get('cartridgeid'))
        homecell = crtobj.get_current_location()
        if homecell[5] in ['0',"1"]:
            return  ['0',"1"]
        elif homecell[5] in ['3',"4"]:
            return ['3',"4"]
        return ['0','1','2','3','4']


    def __cnt_empty_drives(self):
        c = 0
        for obj in self._drives.values():
            if obj.is_empty():
                c+=1
        return c

class SL8500_Level(SL8500_Level_old):
    def __init__(self, id, datadir, eventlog, **kwargs):
        if 'api_check_migration_free_drive' in kwargs:
            self.api_check_migration_free_drive = kwargs['api_check_migration_free_drive']
        if 'api_migration_cache_get' in kwargs:
            self.api_migration_cache_get = kwargs['api_migration_cache_get']
        if 'api_issue_cartridge_migration' in kwargs:
            self.api_issue_cartridge_migration = kwargs['api_issue_cartridge_migration']
        if 'api_migration_cache_put' in kwargs:
            self.api_migration_cache_put = kwargs['api_migration_cache_put']
        if 'api_check_migration_evict_drive' in kwargs:
            self.api_check_migration_evict_drive = kwargs['api_check_migration_evict_drive']
        if 'api_get_migration_options' in kwargs:
            self.api_get_migration_options = kwargs['api_get_migration_options']
        self._denyremote = False
        SL8500_Level_old.__init__(self,id,datadir,eventlog, **kwargs)

    # # # API for MasterLevel
    def api_local_free_drive(self, **kwargs):
        """
        :param kwargs: should be empty
        :return: ID of a free drive of this level or None
        """
        #self.log.debug("API local free %s",str(kwargs))
        return self._empty_drive_minid(**kwargs)

    def api_local_evictable_drive(self, **kwargs):
        #self.log.debug("API local eviction %s",str(kwargs))
        if self._out_of_HC_danger:
            #crtcnt = float(len(self._cartridge_cache))
            libcnt =self._libraries.empty_count()
            #ratio = self._libraries.size()/float(len(self._cartridge_cache))
            #ratio = crtcnt/libcnt
            #ratio = self._libraries.size()
            if libcnt < 100:
                # keep danger level up
                if libcnt < 50:
                    self.log.warning("LSM:%s free libs:%s, %s"%(self.id, libcnt, self.globalclock))
                    self._denyremote = True
                    return False # forces migration of cartridge.
            else:
                self.log.info("%s free libs:%s" % (self.id, libcnt))
                self._out_of_HC_danger = False
                self._denyremote = False
        return self._get_eviction_candidate(**kwargs)

    # # # local functions # # #
    def _get_free_drive(self, **kwargs):
        #self.log.debug("Get free drive ")
        free = self.api_local_free_drive(source=kwargs['sourcelibrary'])
        if free:
            return (free, self.id, None)
        return self.api_check_migration_free_drive(self.id)

    def _eviction_candidate(self,**kwargs):
        levelid=None
        gateway=None
        driveid = self._get_eviction_candidate(**kwargs)
        if not driveid:
            kwargs['sourcelevel']=self.id
            (driveid, levelid, gateway) = self.api_check_migration_evict_drive(**kwargs)
        return (driveid, levelid, gateway)

    def get_target_drive(self, **kwargs):
        drvid, levelid, gateway = self._get_free_drive(**kwargs)
        if drvid is None:
            drvid, levelid, gateway = self._eviction_candidate(**kwargs)
        return drvid, levelid, gateway

    def accept_remote_cartridge(self):
        return not self._denyremote

