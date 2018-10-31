__author__ = 'maesker'

import datetime
import logging
import os

from utilities.fsm.StateMachine import *
from utilities.io.persistent_drive_data import \
    PersIO_Drive, PersIO_Disabled, NoEntryException

from simulator.SL8500_SimStates import *
from utilities import tiny_helper

STATE_DRV_ERROR_1 = 'ERROR1'
STATE_DRV_DISABLED = "DISABLED"
STATE_DRV_EMPTY = "EMPTY"
STATE_DRV_LOADING = "LOADING"
STATE_DRV_UNLOADING = "UNLOADING"
STATE_DRV_LOADED = "LOADED"
STATE_DRV_MOUNTED = 'MOUNTED'


CLEANING_TAPE_DURATION=200 # off
TAPE_THREADING_IN_SEC = 2

# median config.
CLEANING_TAPE_INTERVAL_SECS = 141601    # clean after 39 hours
CLEANING_TAPE_INTERVAL_MNTS = 123           # or clean fter 123 mounts
CLEANING_TAPE_DURATION=340


# 0.75 percentile config.
#CLEANING_TAPE_INTERVAL_SECS = 204535    # clean after 40 hours
#CLEANING_TAPE_INTERVAL_MNTS = 156           # or clean fter 123 mounts

# 0.85 percentile config.
#CLEANING_TAPE_INTERVAL_SECS = 259493    # clean after 40 hours
#CLEANING_TAPE_INTERVAL_MNTS = 208           # or clean fter 123 mounts


class DeallocationError(Exception):
    pass

class EmptyMountException(Exception):
    pass

class CantHandleEvent(Exception):
    pass


class DriveClass(BaseStateMachine):

    def __init__(self, id, basedir, eventlog=True, simulationmode=False,
                 librarylatency=None):
        BaseStateMachine.__init__(self, id,
                                  basedir=os.path.join(basedir, "drive"),
                                  eventlog=eventlog,
                                  simulationmode=simulationmode)
        self.simlog = logging.getLogger()
        #self.analyser = DriveAnalyser(self.id, self.shelvedir)
        self.global_event_timeout = datetime.timedelta(seconds=3600)
        self.globalclock = datetime.datetime(1970,1,1,0)
        self._simulation_cache_current_event = None
        self._simulation_cache_allocated = []
        self._simulation_reading_end = 0
        self.entered_loadedstate = None
        tmpid = int(id[-2:])
        #self._last_cleaning_seconds = random.randrange(0,CLEANING_TAPE_INTERVAL_SECS)
        #self._last_cleaning_mounts = random.randrange(0,CLEANING_TAPE_INTERVAL_MNTS)
        self._last_cleaning_seconds = CLEANING_TAPE_INTERVAL_SECS/(tmpid+1)
        self._last_cleaning_mounts = CLEANING_TAPE_INTERVAL_MNTS/(tmpid+1)

        self.get_latency_cb = librarylatency
        self.sessions = PersIO_Drive(self.basedir, "sessions",simulationmode)
        self.errors = PersIO_Drive(self.basedir, "errors",simulationmode)
        self.disabled = PersIO_Disabled(self.basedir, "disabled", simulationmode)

        self.simlog.debug("Config:  :CLEANING_TAPE_DURATION:%s, CLEANING_TAPE_INTERVAL_SECS:%s, CLEANING_TAPE_INTERVAL_MNTS:%s",CLEANING_TAPE_DURATION,CLEANING_TAPE_INTERVAL_SECS,CLEANING_TAPE_INTERVAL_MNTS)
        #self.errorcnt = 0
        for i in ['robot_eject','pvr_eject','robot_enter']:
            self.ignore(i)

        for e, s, t in [
            ('entering_pvl_mountadd',
             STATE_DRV_UNLOADING, STATE_DRV_UNLOADING),
            ('entering_pvl_mountadd', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            ('entering_pvl_mountadd', STATE_DRV_LOADED, STATE_DRV_MOUNTED),
            ('entering_pvl_mountadd', STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('entering_pvl_mountadd', STATE_DRV_MOUNTED, STATE_DRV_MOUNTED),
            ('entering_pvl_mountadd', STATE_DRV_DISABLED, STATE_DRV_MOUNTED),
            ('entering_pvl_mountadd', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),

            ('exiting_pvl_mountadd', STATE_DRV_UNLOADING, STATE_DRV_UNLOADING),
            ('exiting_pvl_mountadd', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            ('exiting_pvl_mountadd', STATE_DRV_MOUNTED, STATE_DRV_MOUNTED),
            ('exiting_pvl_mountadd', STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('exiting_pvl_mountadd', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('exiting_pvl_mountadd', STATE_DRV_LOADED, STATE_DRV_MOUNTED),

            ('enter_pvl_dismountvolume', STATE_DRV_LOADED, STATE_DRV_LOADED),
            ('enter_pvl_dismountvolume', STATE_DRV_MOUNTED, STATE_DRV_LOADED),
            ('enter_pvl_dismountvolume',
             STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('enter_pvl_dismountvolume', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('enter_pvl_dismountvolume',
             STATE_DRV_UNLOADING, STATE_DRV_UNLOADING),
            ('enter_pvl_dismountvolume', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            #('enter_pvl_dismountvolume', STATE_DRV_LOADED, STATE_DRV_LOADED),
            ('exiting_pvl_dismountvolume', STATE_DRV_LOADED, STATE_DRV_LOADED),
            ('exiting_pvl_dismountvolume',
             STATE_DRV_MOUNTED, STATE_DRV_LOADED),
            ('exiting_pvl_dismountvolume',
             STATE_DRV_UNLOADING, STATE_DRV_UNLOADING),
            ('exiting_pvl_dismountvolume',
             STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('exiting_pvl_dismountvolume',
             STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('exiting_pvl_dismountvolume', STATE_DRV_EMPTY, STATE_DRV_EMPTY),

            ('entering_pvr_dismountcart',
             STATE_DRV_MOUNTED, STATE_DRV_UNLOADING),
            ('entering_pvr_dismountcart',
             STATE_DRV_LOADED, STATE_DRV_UNLOADING),
            ('entering_pvr_dismountcart',
             STATE_DRV_UNLOADING, STATE_DRV_UNLOADING),
            ('entering_pvr_dismountcart',
             STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('entering_pvr_dismountcart', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            ('entering_pvr_dismountcart',
             STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),

            ('exiting_pvr_dismountcart', STATE_DRV_LOADED, STATE_DRV_EMPTY),
            ('exiting_pvr_dismountcart', STATE_DRV_UNLOADING, STATE_DRV_EMPTY),
            ('exiting_pvr_dismountcart', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            ('exiting_pvr_dismountcart',
             STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('exiting_pvr_dismountcart', STATE_DRV_ERROR_1, STATE_DRV_EMPTY),
            ('exiting_pvr_dismountcart', STATE_DRV_MOUNTED, STATE_DRV_ERROR_1),

            ('entering_pvr_mount', STATE_DRV_LOADED, STATE_DRV_LOADED),
            ('entering_pvr_mount', STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('entering_pvr_mount', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('entering_pvr_mount', STATE_DRV_MOUNTED, STATE_DRV_MOUNTED),
            ('entering_pvr_mount', STATE_DRV_UNLOADING, STATE_DRV_ERROR_1),
            ('entering_pvr_mount', STATE_DRV_EMPTY, STATE_DRV_LOADED),

            ('warn_reading_device', NONE_STATE,             STATE_DRV_ERROR_1),
            ('warn_reading_device', STATE_DRV_MOUNTED,
             STATE_DRV_ERROR_1),
            ('warn_reading_device', STATE_DRV_DISABLED,
             STATE_DRV_DISABLED),
            ('warn_reading_device', STATE_DRV_ERROR_1,      STATE_DRV_ERROR_1),
            ('warn_reading_device', STATE_DRV_LOADED,      STATE_DRV_ERROR_1),
            ('warn_drive_disabled', NONE_STATE,         STATE_DRV_DISABLED),
            ('warn_drive_disabled', STATE_DRV_ERROR_1,  STATE_DRV_DISABLED),
            ('warn_drive_disabled', STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('warn_drive_disabled', STATE_DRV_EMPTY,    STATE_DRV_DISABLED),
            ('warn_drive_disabled', STATE_DRV_MOUNTED,    STATE_DRV_DISABLED),
            ('warn_drive_disabled', STATE_DRV_LOADED,    STATE_DRV_DISABLED),
            ('warn_drive_disabled', STATE_DRV_UNLOADING,
             STATE_DRV_DISABLED),

            ('warn_dismount', STATE_DRV_EMPTY, STATE_DRV_ERROR_1),
            ('warn_dismount', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('warn_dismount', STATE_DRV_UNLOADING, STATE_DRV_ERROR_1),
            ('warn_dismount', STATE_DRV_MOUNTED, STATE_DRV_UNLOADING),
            ('warn_dismount', STATE_DRV_LOADED, STATE_DRV_UNLOADING),

            ('drive_enabled', NONE_STATE, STATE_DRV_EMPTY),
            ('drive_enabled', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            ('drive_enabled', STATE_DRV_LOADED, STATE_DRV_EMPTY),
            ('drive_enabled', STATE_DRV_ERROR_1, STATE_DRV_EMPTY),
            ('drive_enabled', STATE_DRV_DISABLED, STATE_DRV_EMPTY),
            ('drive_enabled', STATE_DRV_MOUNTED, STATE_DRV_EMPTY),
            ('drive_enabled', STATE_DRV_UNLOADING, STATE_DRV_EMPTY),

            ('enter_pvl_mountcompleted', NONE_STATE, STATE_DRV_MOUNTED),
            ('enter_pvl_mountcompleted', STATE_DRV_MOUNTED, STATE_DRV_MOUNTED),
            ('enter_pvl_mountcompleted', STATE_DRV_ERROR_1, STATE_DRV_MOUNTED),
            ('enter_pvl_mountcompleted',
             STATE_DRV_DISABLED, STATE_DRV_MOUNTED),
            ('enter_pvl_mountcompleted', STATE_DRV_LOADED, STATE_DRV_MOUNTED),
            ('enter_pvl_mountcompleted',
             STATE_DRV_UNLOADING, STATE_DRV_ERROR_1),
            ('enter_pvl_mountcompleted', STATE_DRV_EMPTY, STATE_DRV_MOUNTED),
            ('exiting_pvl_mountcompleted', NONE_STATE, STATE_DRV_MOUNTED),
            ('exiting_pvl_mount_complete', STATE_DRV_EMPTY, STATE_DRV_ERROR_1),
            ('exiting_pvl_mount_complete',
             STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('exiting_pvl_mount_complete',
             STATE_DRV_LOADED, STATE_DRV_MOUNTED),
            ('exiting_pvl_mount_complete',
             STATE_DRV_UNLOADING, STATE_DRV_UNLOADING),
            ('exiting_pvl_mount_complete',
             STATE_DRV_MOUNTED, STATE_DRV_MOUNTED),
            ('exiting_pvl_mount_complete', NONE_STATE, STATE_DRV_ERROR_1),
            ('exiting_pvl_mount_complete',
             STATE_DRV_DISABLED, STATE_DRV_DISABLED),

            ('robot_mount', NONE_STATE, STATE_DRV_LOADED),
            ('robot_mount', STATE_DRV_EMPTY, STATE_DRV_LOADED),
            ('robot_mount', STATE_DRV_LOADED, STATE_DRV_LOADED),
            ('robot_mount', STATE_DRV_ERROR_1, STATE_DRV_LOADED),
            ('robot_mount', STATE_DRV_DISABLED, STATE_DRV_LOADED),
            ('robot_mount', STATE_DRV_MOUNTED, STATE_DRV_MOUNTED),
            ('robot_mount', STATE_DRV_UNLOADING, STATE_DRV_ERROR_1),

            ('robot_dismount', NONE_STATE, STATE_DRV_EMPTY),
            ('robot_dismount', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            ('robot_dismount', STATE_DRV_LOADED, STATE_DRV_EMPTY),
            ('robot_dismount', STATE_DRV_UNLOADING, STATE_DRV_EMPTY),
            ('robot_dismount', STATE_DRV_MOUNTED, STATE_DRV_EMPTY),
            ('robot_dismount', STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('robot_dismount', STATE_DRV_ERROR_1, STATE_DRV_EMPTY),

            #('robot_eject', STATE_DRV_LOADED, STATE_DRV_EMPTY),
            #('robot_eject', STATE_DRV_MOUNTED, STATE_DRV_EMPTY),
            #('robot_eject', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            #('pvr_eject', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            #('pvr_eject', STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            #('pvr_eject', STATE_DRV_UNLOADING, STATE_DRV_EMPTY),
            #('robot_enter', STATE_DRV_LOADED, STATE_DRV_ERROR_1),

            ('warn_crt_not_readable', STATE_DRV_EMPTY, STATE_DRV_ERROR_1),
            ('warn_crt_not_readable', STATE_DRV_LOADED, STATE_DRV_ERROR_1),
            ('warn_crt_not_readable', STATE_DRV_MOUNTED, STATE_DRV_ERROR_1),
            ('warn_crt_not_readable', STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('warn_crt_not_readable', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('warn_crt_not_readable', STATE_DRV_UNLOADING, STATE_DRV_ERROR_1),

            ('job_canceled', STATE_DRV_MOUNTED, STATE_DRV_MOUNTED),
            ('job_canceled', STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('job_canceled', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('job_canceled', STATE_DRV_LOADED, STATE_DRV_LOADED),
            ('robot_acscr', STATE_DRV_LOADED, STATE_DRV_LOADED),
            ('robot_acscr', STATE_DRV_MOUNTED, STATE_DRV_MOUNTED),
            ('robot_acscr', STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('robot_acscr', STATE_DRV_UNLOADING, STATE_DRV_ERROR_1),
            ('robot_acscr', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('robot_acscr', STATE_DRV_EMPTY, STATE_DRV_EMPTY),

            ('robot_audit', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            ('robot_audit', STATE_DRV_MOUNTED, STATE_DRV_MOUNTED),
            ('robot_audit', STATE_DRV_LOADED, STATE_DRV_LOADED),
            ('robot_audit', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('robot_audit', STATE_DRV_DISABLED, STATE_DRV_DISABLED),

            ('robot_move', STATE_DRV_MOUNTED, STATE_DRV_ERROR_1),
            ('robot_move', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('robot_move', STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('robot_move', STATE_DRV_EMPTY,STATE_DRV_EMPTY),
            ('robot_move', STATE_DRV_LOADED,STATE_DRV_EMPTY),

            ('exiting_pvl_import', STATE_DRV_MOUNTED, STATE_DRV_MOUNTED),
            ('exiting_pvl_import', STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('exiting_pvl_import', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('exiting_pvl_import', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            ('pvl_import', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            # xb0729 2012 03 20 11 23 26
            ('pvl_import', STATE_DRV_MOUNTED, STATE_DRV_MOUNTED),
            ('pvl_import', NONE_STATE, STATE_DRV_EMPTY),
            ('pvl_import', STATE_DRV_EMPTY, STATE_DRV_EMPTY),

            ('enter_pvr_inject', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            ('enter_pvr_inject', STATE_DRV_MOUNTED, STATE_DRV_EMPTY),

            ('failed_crt', STATE_DRV_DISABLED, STATE_DRV_DISABLED),
            ('failed_crt', STATE_DRV_ERROR_1, STATE_DRV_ERROR_1),
            ('failed_crt', STATE_DRV_UNLOADING, STATE_DRV_UNLOADING),
            ('failed_crt', STATE_DRV_EMPTY, STATE_DRV_EMPTY),
            ('cancel_event', STATE_DRV_UNLOADING, STATE_DRV_LOADED),

        ]:
            self.new_transition(e, s, t)

        self.default_target('failed_mount', STATE_DRV_ERROR_1)
        self.default_target('failed_dismount', STATE_DRV_ERROR_1)
        self.default_target('dismount_drive', STATE_DRV_DISABLED)
        self.default_target('pvl_deletedrive', STATE_DRV_DISABLED)
        self.init_tmp()

    def flush(self):
        self.sessions.flush()
        self.errors.flush()
        self.disabled.flush()

    def close(self):
        self.sessions.close()
        self.errors.close()
        self.disabled.close()

    def init_tmp(self):
        self.tmp = {
            'load_complete': None,
            'mount_operations': [],
            'unload_complete': None,
            'crtid': None,
            'loaded_from_libraryid': None,
            'unloaded_to_libraryid': None
        }

    def save(self):
        if self._simulation_mode:
            self.init_tmp()
            return
        #success = True
        #if len(self.tmp['mount_operations']) == 0:
        #    success = False
        #for i in ['load_request', 'load_complete', 'unload_request', 'unload_complete', 'cartridgeid',
        #          'loaded_from_libraryid', 'unloaded_to_libraryid']:
        #    if self.tmp[i] == None:
        #        success = False
        #if success:
        #    self.datashelve['success'].append(self.tmp)
        #else:
        #    self.datashelve['error'].append(self.tmp)
        if isinstance(self.tmp['unload_complete'], datetime.datetime) and isinstance(self.tmp['load_complete'], datetime.datetime):
            #key = self.tmp['unload_complete'].strftime("%Y%m%d-%H%M%S")
            self.sessions.put(self.tmp)
        else:
            #key = "%i"%self.errorcnt
            #self.errorcnt+=1
            self.errors.put(self.tmp)
            #raise Exception("wrong type %s"%self.tmp['unload_complete'])
        self.init_tmp()

    def entercb_EMPTY(self, event):
        self.tmp['unload_complete'] = event.get_time()
        if 'libraryid' in event.attributes:
            if self.tmp['unloaded_to_libraryid'] == None:
                self.tmp['unloaded_to_libraryid'] = event.attributes[
                    'libraryid']
            else:
                pass
        self.save()

    def entercb_LOADED(self, event):

        if self.tmp['load_complete'] == None:
            self.tmp['load_complete'] = event.get_time()
        # else:
            #diff = event.get_time() - self.tmp['load_complete']
            # if diff < self.global_event_timeout:
            #    pass ## duplicate msg
            # else:
            # self.raiseDump(event) CantHandleEvent("%s:%s"%(self.id, event))
        if 'libraryid' in event.attributes:
            if self.tmp['loaded_from_libraryid'] == None:
                self.tmp['loaded_from_libraryid'] = event.attributes[
                    'libraryid']
            else:
                pass
            #    self.raiseDump(event)
        if event.name=="cancel_event":
            self._simulation_cache_current_event=None
        self.entered_loadedstate = event.get_time()
        self.__check_cartridge_id(event)

    def instatecb_LOADED(self, event):
        return self.entercb_LOADED(event)

    def leavecb_LOADED(self, event):
        self.entered_loadedstate = None

    def entercb_MOUNTED(self, event):
        self.tmp['mount_operations'].append([event.get_time(), None])

    def leavecb_MOUNTED(self, event):
        if self.tmp['mount_operations'][-1][1] == None:
            self.tmp['mount_operations'][-1][1] = event.get_time()
        else:
            self.raiseDump(event)

    def leavecb_ERROR1(self, event):
        #self.tmp['enderror'] = event.get_time()
        self.save()

    def entercb_DISABLED (self, event):
        self.enter_disabled = event.get_time()
        self.clear()

    def leavecb_DISABLED(self, event):
        self.disabled.put({'disabled':self.enter_disabled,
                           'enabled':event.get_time()})
        self.enter_disabled = None
    ##
    # @brief checks if cartridgeid is an attribute of the event and adds
    # it to self.tmp if so.
    # returns false if self.tmp already has a different id registered.
    # In this case the unloading event of the existing id got lost
    def __check_cartridge_id(self, event):
        if 'cartridgeid' in event.attributes:
            if self.tmp['crtid'] == None:
                self.tmp['crtid'] = event.attributes['cartridgeid']
            elif self.tmp['crtid'] == event.attributes['cartridgeid']:
                pass  # ok as expected
            else:
                return False
        return True

    def reset(self):
        self.state == STATE_DRV_EMPTY
        self.save()
        self.clear()

    def get_enable_disable_events(self):
        disabled,enabled = [],[]
        try:
            first = self.sessions.get_first_entry()
            enabled.append(
                first['mount_operations'][0][0] - datetime.timedelta(seconds=30))
        except NoEntryException:
            pass
        except IndexError:
            pass
        for d,e in self.disabled.get_operations():
            disabled.append(d)
            enabled.append(e)
        return (enabled, disabled)

    # simulation stuff

    def is_empty(self):
        return self.state == STATE_DRV_EMPTY and len(self._simulation_cache_allocated) == 0
        # return self.state == STATE_DRV_EMPTY

    def is_loaded(self):
        if self.state == STATE_DRV_LOADED:
            return self.get_current_cartridge()

    def is_expecting_crt(self, crtid):
        if self.state == STATE_DRV_MOUNTED:
            curcrt = self.get_current_cartridge()
            if not curcrt:
                raise EmptyMountException("mounted state but no cartridge drive:%s"%self.id)
            self._simulation_cache_allocated = [curcrt]
            return False
        if len(self._simulation_cache_allocated)>0:
            if self._simulation_cache_allocated[-1] == crtid:
                return True

    def can_accept_load_operation(self):
        if self.state == STATE_DRV_EMPTY:
            if len(self._simulation_cache_allocated)==0:
                return True
        if self.state == STATE_DRV_LOADED:
            if len(self._simulation_cache_allocated)<=1:
                return True
        return False

    def can_extend_mounttime(self, crtid):
        try:
            if self.state != STATE_DRV_DISABLED:
                if self._simulation_cache_allocated[0]==crtid and \
                    len(self._simulation_cache_allocated) == 1:
                    return True
        except:
            raise

    def deallocate(self, crtid):
        try:
            if self.state != STATE_DRV_DISABLED:
                if len(self._simulation_cache_allocated)>0:
                    if crtid == self._simulation_cache_allocated[0]:
                        crt = self._simulation_cache_allocated.pop(0)
                        self.simlog.debug("deallocating %s from %s"%(crt,self.id))
                        curev = self._simulation_cache_current_event
                        if curev:
                            if curev.get('cartridgeid') == crtid:
                                self._simulation_cache_current_event = None
                                self._simulation_cache_current_event_done = None
                        return True
                    else:
                        self.simlog.error(" %s not lowest entry in allocation list:%s",crtid,self._simulation_cache_allocated)
                        return False
        except Exception, e:
            self.simlog.exception("exception:%s",e)
            raise DeallocationError("In %s, tried to deallocate %s, state:%s"%(self.id,crtid, self.state))

    def allocate(self, crtid):
        if crtid not in self._simulation_cache_allocated:
            self._simulation_cache_allocated.append(crtid)
            self._last_cleaning_mounts += 1
        return None

    def get_current_cartridge(self):
        if self._simulation_cache_allocated:
            return self._simulation_cache_allocated[0]


    def register_current_event(self, eventobj):
        if self._simulation_cache_current_event != None:
            self.simlog.error("Overwriting current drive event %s:%s"%(self.id, self.state))
            self.simlog.error("current event:%s"%str(self._simulation_cache_current_event))
            self.simlog.error("current event:%s"%str(eventobj))
            if self._simulation_cache_current_event.get('state') != STATE_SL8500_DRV_UNLOADING:
                raise Exception("overwriting event")
        self._simulation_cache_current_event = eventobj
        if eventobj.get('readlength'):
            self._simulation_cache_current_event_done = self.globalclock + datetime.timedelta(seconds=eventobj.get('readlength'))

    def get_current_event(self):
        return self._simulation_cache_current_event

    def extend_mounttime(self, time):
        if self._simulation_cache_current_event is not None:
            self._simulation_cache_current_event.attributes['readlength']+=time
            self._simulation_cache_current_event_done += datetime.timedelta(seconds=time)
        else:
            self.simlog.error("no event available")

    def step(self, clock):
        ## @brief perform a simulation step.
        # @param clock simulation global
        #try:
            if clock > self.globalclock:
                self._last_cleaning_seconds += (clock-self.globalclock).total_seconds()
                self.globalclock = clock
            if self._simulation_cache_current_event:
                if self._simulation_cache_current_event.get('state') == STATE_SL8500_DRV_READING:
                    #event = self._simulation_cache_current_event
                    #eventend = event.get(
                    #    'datetime') + datetime.timedelta(seconds=event.get('readlength'))
                    if self._simulation_cache_current_event_done > clock:
                        return
                    elif self._simulation_cache_current_event_done == clock:
                        self._simulation_cache_current_event.set('state', STATE_SL8500_DRV_READINGDONE)
                        crtid = self._simulation_cache_current_event.get('cartridgeid')
                        if crtid.startswith("CLN"):
                            self._last_cleaning_mounts=0
                            self._last_cleaning_seconds=0
                        event = self._simulation_cache_current_event
                        self._simulation_cache_current_event = None
                        return event
                    else:
                        self.simlog.error('%s.step:how did i get here', str(self))
                elif self._simulation_cache_current_event.get('state') == STATE_SL8500_DRV_UNLOADING:
                    pass  # event handled by robot instance
                else:
                    self.simlog.warning("unhandled state %s", self._simulation_cache_current_event.get('state'))
        #except:
        #    pass

    def get_idletime(self, clock):
        if self.state == STATE_DRV_LOADED:
            if self._simulation_cache_current_event==None:
                return tiny_helper.get_diff_in_seconds(
                    self.tmp['mount_operations'][-1][1], clock)
        #else:
        #    self.simlog.warning("drvid:%s, not loaded, why is there an idle request... "%self.id)

    def get_latency(self, target):
        return self.get_latency_cb(self.id, target)

    def do_clean(self):
        if self._simulation_cache_current_event!= None:
            return False
        if ( self.state is STATE_DRV_LOADED and \
                        len(self._simulation_cache_allocated) <= 1 ) or \
                ( self.state is STATE_DRV_EMPTY and \
                        len(self._simulation_cache_allocated) == 0) :
            if self._last_cleaning_mounts>CLEANING_TAPE_INTERVAL_MNTS or \
                    self._last_cleaning_seconds>CLEANING_TAPE_INTERVAL_SECS:
                self.simlog.debug("issue cleaning %s",self.id)
                return True

    def done_clean(self):
        if self.state is STATE_DRV_LOADED:
            crt = self.get_current_cartridge()
            if crt:
                return crt.startswith('CLN')
    #            if crt.startswith('CLN'):
    #                self._last_cleaning_mounts=0
    #                self._last_cleaning_seconds=0
    #                self.simlog.debug("cleaning tape in loaded state. done. evict it")
    #                return True

    def get_loadcomplete_timestamp(self):
        return self.tmp['load_complete']

    def get_last_unmount_timestamp(self):
        #if len(self.tmp['mount_operations'])>0:
        try:
            return self.tmp['mount_operations'][-1][1]
        #else:
        except:
            return datetime.datetime(year=1970, month=1, day=1, hour=0)

    def clear(self):
        self._simulation_cache_allocated = []
        self._simulation_cache_current_event = None
        self._simulation_cache_current_event_done = None