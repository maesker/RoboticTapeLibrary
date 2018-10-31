__author__ = 'maesker'

import datetime

import sys
import os
import logging
import Queue
import json

#from tape_simulator_source.ecmwf.simulator.Prefetching import *
#from LevelClass import CleaningTapeMigration
#from LevelClass_ARC import LC_Arc
from LevelClass_Belady import *
from LevelClass_LFU import *
from LevelClass_LRU import *
from LevelClass_FIFO import *
#from LevelClass_ENH import LC_ENH
#from LevelClass_MRU import LC_MRU
#from LevelClass_Random import LC_Random
from LevelClass_DD import *
from simulator.lsm.migration import *
from simulator.lsm.tape_library_abstraction_layer import TapeLibraryAbstractionLayer, DeletionFailure
#from simulator.SL8500_SimStates import STATE_SL8500_RBT_MIGRATE_RECEIVE, Sim_Event


# # Optimization level # #
# 0: off
# 1: Hot/cold cartridge classification,
#     + home cell placement based on classification
# 2: crt relocation of cooled down cartridges
# 3: fast crt dropoff during force evictions
# 4: invert unload/load procedure to load+unload

class LMaster(TapeLibraryAbstractionLayer):
    def __init__(self, **kwargs):
        TapeLibraryAbstractionLayer.__init__(self, kwargs['datadir'])
        self.optimizationlevel = kwargs['optimization']
        self.evictionparam = kwargs['evic_param']
        self.eventlog = kwargs['eventlog']

        self._cartridge_lookuptable = collections.OrderedDict()
        self.crtmap=collections.OrderedDict()
        if os.path.isfile(os.path.join(self.datadir,'../crtmapping.json')):
            with open(os.path.join(self.datadir,'../crtmapping.json'),'r') as fp:
                for k,v in json.load(fp).items():
                    #key = k.replace("/","")
                    self.crtmap[k]=v
        self.level = collections.OrderedDict()
        self.__pass_thrus = collections.OrderedDict()
        self.__elevators = collections.OrderedDict()
        self._level_migration_options = collections.OrderedDict()
        if kwargs['eviction'].upper() == "LRU":
            self.LevelClassRef = LC_LRU
        elif kwargs['eviction'].upper() == "FIFO":
            self.LevelClassRef = LC_FIFO
        elif kwargs['eviction'].upper() == "BELADY":
            self.LevelClassRef = LC_Belady
        elif kwargs['eviction'].upper()[0:2] == "DD":
            self.LevelClassRef = LC_DD_0
        elif kwargs['eviction'].upper() == "LFU":
            self.LevelClassRef = LC_LFU
        else:
            self.log.error("unknown strategy %s"%kwargs['eviction'])
            raise Exception("unknown strategy %s"%kwargs['eviction'])
        self.log.info("Initialization done.")

    def add_level(self, conf):
        for level, d in conf.iteritems():
            obj = self.LevelClassRef(level, self.datadir, self.eventlog,
                    #upcomming_events=self.upcomming_events,
                    parameter=self.evictionparam,
                    api_check_migration_free_drive = self.api_check_migration_free_drive,
                    api_check_migration_evict_drive = self.api_check_migration_evict_drive,
                    api_migration_cache_get = self.api_migration_cache_get,
                    api_issue_cartridge_migration = self.api_issue_cartridge_migration,
                    api_migration_cache_put = self.api_migration_cache_put,
                    api_get_migration_options = self.api_get_migration_options,
                    optimizationlevel = self.optimizationlevel)
            self.level[int(level)] = obj
            #self._LB_p0_hourly_tracker.append([])
            obj.load_config((d,{}))
            #obj.initclock(clock)
        for levelid, level in self.level.iteritems():
            if levelid+4 in self.level:
                ptid = self._get_passthru_key(levelid,levelid+4)
                self.__pass_thrus[ptid] = PassThru(ptid, levelid, levelid+4)
            if levelid%4==0:
                elevatorkey_r = "el_r_%i"%levelid
                elevatorkey_l = "el_l_%i"%levelid
                self.__elevators[elevatorkey_l] = Elevator(elevatorkey_l, 'L', levelid)
                self.__elevators[elevatorkey_r] = Elevator(elevatorkey_r, 'R', levelid)
        self.__calculate_migration_options()


    # # # pass thru stuff
    def _get_passthru_key(self, source, target):
            if source > target:
                return "pt_%s_%s"%(target,source)
            return "pt_%s_%s"%(source,target)

    def _get_passthru_neighbours(self, sourcelevel):
        pt = []
        if sourcelevel-4 in self.level:
            pt.append(sourcelevel-4)
        if sourcelevel+4 in self.level:
            pt.append(sourcelevel+4)
        return pt

    def _get_free_passthrus(self, sourcelevel):
        free = []
        for i in self._get_passthru_neighbours(sourcelevel):
            key = self.get_passthru_key(sourcelevel,i)
            if key in self.__pass_thrus:
                pt = self.__pass_thrus[key]
                if pt.isfree():
                    free.append(i)
        return free

    # # # elevator stuff

    def _get_elevator_keys(self,sourcelevel):
        levelid = (sourcelevel/4)*4
        return ("el_l_%i"%levelid, "el_r_%i"%levelid)

    def _get_elevator_neighbours(self, sourcelevel):
        offset = (sourcelevel/4)*4
        lv = []
        for i in range(0,4):
            lv.append(offset+i)
        if sourcelevel in lv:
            lv.remove(sourcelevel)
        return lv

    def free_elevator_available(self, sourcelevel):
        for i in self.get_elevator_keys(sourcelevel):
            el = self.__elevators.get(i)
            if el:
                if el.isfree():
                    return i
        return False

    # # # cartridge specific stuff

    def init_cartridge(self,crtid):
        try:
            lid = None
            if self.crtmap:
                lid = self.crtmap.get(crtid, None)
            if lid == None:
                lid=0
                minratio = sys.maxint
                for k,v in self.level.iteritems():
                    ratio = v.get_crt_per_drive_ratio()
                    if ratio<minratio:
                        minratio=ratio
                        lid = k
                #self._new_crt_roundrobin = (self._new_crt_roundrobin+1)%len(self.level)
                #lid = self._new_crt_roundrobin
            self._cartridge_lookuptable[crtid] = lid
            self.level[lid].init_cartridge(crtid)
        except Exception, e:
            self.log.exception(e)
            for i,l in self.level.iteritems():
                self.log.error("Report:ID:%s:%s"%(i,l.librarystate()))
            raise e

    # # # migration stuff
    def api_get_migration_options(self, sourcelevel):
        res = collections.OrderedDict()
        for i in self.api_get_migration_options(sourcelevel):
            res[i] = self.__get_migration_instance(i)
        return  res

    def __get_migration_instance(self, key):
        if key in self.__pass_thrus:
            return self.__pass_thrus[key]
        elif key in self.__elevators:
            return self.__elevators[key]
        else:
            raise Exception("Invalid Key %s, neither PT nore EL"%key)

    def __calculate_migration_options(self):
        for levelid in self.level.iterkeys():
            self._level_migration_options[levelid]=deque()
            for i in self._get_passthru_neighbours(levelid):
                self._level_migration_options[levelid].append(self._get_passthru_key(levelid, i))
            for i in self._get_elevator_keys(levelid):
                self._level_migration_options[levelid].append(i)
        pass

    def api_check_migration_evict_drive(self, **kwargs):
        try:
            sourcelevel = kwargs['sourcelevel']
            range_ref = range
            len_ref = len
            already_checked_level = []
            self_level_get_ref = self.level.get
            migrationdeque = self._level_migration_options[sourcelevel]
            for i in range_ref(len_ref(migrationdeque)):
                migrationdeque.rotate()
                inst = self.__get_migration_instance(migrationdeque[0])
                if inst.isfree():
                    reach = inst.reachable
                    for j in range_ref(len_ref(reach)):
                        reach.rotate() # loadbalance the level of elevators
                        levelid = reach[0]
                        if levelid not in already_checked_level:
                            levelinst = self_level_get_ref(levelid)
                            free = levelinst.api_local_evictable_drive()
                            if free:
                                self.log.debug("Migration from %s to %s",sourcelevel,levelid)
                                return (free, levelid, migrationdeque[0])
                            already_checked_level.append(levelid)
        except:
            raise
        return (None, None, None)

    def api_check_migration_free_drive(self, sourcelevel):
        """
        :param sourcelevel: source level id to check for free drive in different level
        :return: drive id, levelid, gateway id of the free drive to use

         Check if a free drive is available in the level directly connected to the sourcelevel via pass-thru or elevator.
         Only the ids are returned, No allocation/registration is taking place
        """
        try:
            already_checked_level = []
            self_level_get_ref = self.level.get
            migrationdeque = self._level_migration_options[sourcelevel]
            for i in range(len(migrationdeque)):
                migrationdeque.rotate()
                inst = self.__get_migration_instance(migrationdeque[0])
                if inst.isfree():
                    reach = inst.reachable
                    for j in range(len(reach)):
                        reach.rotate() # loadbalance the level of elevators
                        levelid = reach[0]
                        if levelid not in already_checked_level:
                            levelinst = self_level_get_ref(levelid)
                            if levelinst.accept_remote_cartridge():
                                free = levelinst.api_local_free_drive()
                                if free:
                                    self.log.debug("Migration from %s to %s",sourcelevel,levelid)
                                    return (free, levelid, migrationdeque[0])
                            already_checked_level.append(levelid)
        except:
            raise
        return (None, None, None)

    def api_issue_cartridge_migration(self, event):
        """
        :param event: read request event
        :return: None
        """
        gw_ref = event.get('gateway')
        crtid_ref = event.get('cartridgeid')
        if crtid_ref.startswith('CLN'):
            raise CleaningTapeMigration("request to migrate cleaning tape %s"%crtid_ref)
        gateway_inst = self.__get_migration_instance(gw_ref) # raise if not found
        if gateway_inst.isfree():
            gateway_inst.allocate(crtid_ref)

            sourcelevel = self.level[event.get('sourcelevel')]
            sourceevent = self.event_factory("PTsnd",
                event.priority, 'passthru_send',
                {'cartridgeid':crtid_ref,'passthru': gw_ref}
            )
            sourcelevel.push_priority_queue(sourceevent)

            targetlevel = self.level[event.get('targetlevel')]
            targetev = self.event_factory("PTrcv",
                event.priority, "passthru_receive",
                {'cartridgeid': crtid_ref,'passthru':gw_ref}
            )
            targetev.set('state',STATE_SL8500_RBT_MIGRATE_RECEIVE)
            targetev.set("follow_up_event", event)
            targetlevel.push_priority_queue(targetev)
            self._cartridge_lookuptable[crtid_ref] = int(targetlevel.id)

            targetlevel.prepare_drive_load(event.get('driveid'), crtid_ref)

        else:
            raise Exception("Gateway %s not free for %s to migrate to %s"%(event.get('gateway'), event.get('cartridgeid'), event.get('target')))

    def api_migration_cache_get(self, key, delete_if_found=False):
        ret = None
        if key in self.__pass_thrus:
            ret = self.__pass_thrus[key].get(delete_if_found)
        elif key in self.__elevators:
            ret = self.__elevators[key].get(delete_if_found)
        if ret:
            self.log.debug("key:%s, %s",key,str(ret))
        return ret

    def api_migration_cache_put(self, key, crtobj):
        self.log.debug("key:%s, crt:%s",key,crtobj.id)
        if key in self.__pass_thrus:
            self.__pass_thrus[key].put(crtobj)
        elif key in self.__elevators:
            self.__elevators[key].put(crtobj)
        else:
            raise Exception("unknown key %s"%key)


    def push(self, newevent):
        if newevent.name == "crt_read_request":
            #self.delaydeques[0].append(newevent)
            if newevent.get('cartridgeid') not in self._cartridge_lookuptable:
                self.init_cartridge(newevent.get('cartridgeid'))
            try:
                self.level[self._cartridge_lookuptable[newevent.get('cartridgeid')]].push_priority_queue(newevent)
            except:
                raise
        elif newevent.name in ["drive_enabled","drive_disabled"]:
            levelid = int(newevent.get('driveid')[2:4])
            if levelid in self.level:
                self.level[levelid].drive_action(newevent.get('driveid'), newevent.name)
            else:
                raise "Unknown level id %s"%newevent.get('driveid')
        elif newevent.name in ['crt_eject']:
            levelid = self._cartridge_lookuptable.get(newevent.get('cartridgeid'),None)
            if levelid:
                level = self.level[self._cartridge_lookuptable[newevent.get('cartridgeid')]]
                if level.crt_action(newevent):
                    del self._cartridge_lookuptable[newevent.get('cartridgeid')]
                else:
                    raise DeletionFailure("crt %s could not be deletec"%newevent.get('cartridgeid'))
            #else:
            #    raise Exception("Cant identify levelid for event %s"%str(newevent))
        else:
            raise "Unknown event"
