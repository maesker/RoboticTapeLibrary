import collections
import sys
import os
from datetime import datetime

from utilities.fsm.StateMachine import BaseStateMachine, NONE_STATE
from utilities.io.persistent_cartridge_data import CartridgePersistence
from utilities.tiny_helper import get_diff_in_seconds

__author__ = 'maesker'

# Cartridge States
STATE_CRT_ERROR_1 = 'ERROR_1'
STATE_CRT_LOAD_RQST = "LOAD_REQ"
STATE_CRT_LOADED = "LOADED"
STATE_CRT_MOUNTED = "MOUNTED"
STATE_CRT_UNLOAD_REQ = "UNLOAD_REQ"
STATE_CRT_UNLOADED = "UNLOADED"
STATE_CRT_INJECTING = "INJECTING"
STATE_CRT_EJECTED = "EJECTED"
STATE_CRT_MNTREQ_WHILE_UNLOADING = 'MNTREQ_WHILE_UNLOADING'

CRT_HEAT_DEQUE_LENGTH = 10
# CRT_LOAD_BALANCING = 0

##
# @brief Class to manage the Cartridge State, statistics etc.


class CartridgeClass(BaseStateMachine):

    global_event_timeout = 3600
    transitions = {
        STATE_CRT_INJECTING: (
            ('robot_mount', STATE_CRT_LOADED),
            ('robot_dismount', STATE_CRT_UNLOADED),
            ('robot_enter', STATE_CRT_UNLOADED),
            ('enter_pvr_inject', STATE_CRT_INJECTING),
            ('exit_pvr_inject', STATE_CRT_UNLOADED),
            ('pvl_import', STATE_CRT_INJECTING),
            ('exiting_pvl_import', STATE_CRT_UNLOADED),
            ('robot_eject', STATE_CRT_EJECTED),
            ('pvr_eject', STATE_CRT_EJECTED),
            ('failed_crt', STATE_CRT_ERROR_1),
            ('failed_mount', STATE_CRT_ERROR_1),
            ('failed_dismount', STATE_CRT_ERROR_1),
            ('crt_not_checked_in', STATE_CRT_ERROR_1),
            ('warn_reading_device', STATE_CRT_ERROR_1),
            ('warn_crt_not_readable', STATE_CRT_ERROR_1),
        ),
        STATE_CRT_LOADED: (
            ('entering_pvl_mountadd', STATE_CRT_MOUNTED),
            ('exiting_pvl_mountadd', STATE_CRT_MOUNTED),
            ('enter_pvl_mountcompleted', STATE_CRT_LOADED),
            ('exiting_pvl_mount_complete', STATE_CRT_MOUNTED),
            ('enter_pvl_dismountvolume', STATE_CRT_LOADED),
            ('exiting_pvl_dismountvolume', STATE_CRT_LOADED),
            ('entering_pvr_mount', STATE_CRT_LOADED),
            ('entering_pvr_dismountcart', STATE_CRT_UNLOAD_REQ),
            ('exiting_pvr_dismountcart', STATE_CRT_UNLOADED),
            ('robot_mount', STATE_CRT_LOADED),
            ('robot_dismount', STATE_CRT_UNLOADED),
            ('robot_enter', STATE_CRT_ERROR_1),
            ('warn_dismount', STATE_CRT_ERROR_1),
            ('crt_checkout', STATE_CRT_EJECTED),
            ('job_canceled', STATE_CRT_LOADED),
            ('robot_audit', STATE_CRT_LOADED),
            ('robot_eject', STATE_CRT_EJECTED),
            ('pvr_eject', STATE_CRT_EJECTED),
            ('failed_crt', STATE_CRT_ERROR_1),
            ('failed_mount', STATE_CRT_ERROR_1),
            ('failed_dismount', STATE_CRT_ERROR_1),
            ('crt_not_checked_in', STATE_CRT_ERROR_1),
            ('warn_reading_device', STATE_CRT_ERROR_1),
            ('warn_crt_not_readable', STATE_CRT_ERROR_1),
        ),
        STATE_CRT_EJECTED: (
            ('entering_pvl_mountadd', STATE_CRT_LOAD_RQST),
            ('exiting_pvl_mountadd', STATE_CRT_EJECTED),
            ('exiting_pvl_mount_complete', STATE_CRT_EJECTED),
            ('enter_pvl_dismountvolume', STATE_CRT_EJECTED),
            ('exiting_pvl_dismountvolume', STATE_CRT_ERROR_1),
            ('entering_pvr_mount', STATE_CRT_EJECTED),
            ('entering_pvr_dismountcart', STATE_CRT_EJECTED),
            ('exiting_pvr_dismountcart', STATE_CRT_EJECTED),
            ('robot_mount', STATE_CRT_EJECTED),
            ('robot_dismount', STATE_CRT_EJECTED),
            ('robot_move', STATE_CRT_EJECTED),
            ('robot_enter', STATE_CRT_UNLOADED),
            ('pvl_import', STATE_CRT_INJECTING),
            ('robot_audit', STATE_CRT_EJECTED),
            ('robot_eject', STATE_CRT_EJECTED),
            ('pvr_eject', STATE_CRT_EJECTED),
            ('failed_crt', STATE_CRT_ERROR_1),
            ('failed_mount', STATE_CRT_ERROR_1),
            ('failed_dismount', STATE_CRT_ERROR_1),
            ('crt_not_checked_in', STATE_CRT_ERROR_1),
            ('warn_reading_device', STATE_CRT_ERROR_1),
            ('warn_crt_not_readable', STATE_CRT_ERROR_1),
        ),
        STATE_CRT_UNLOAD_REQ: (
            ('entering_pvl_mountadd', STATE_CRT_MNTREQ_WHILE_UNLOADING),
            ('exiting_pvl_mountadd', STATE_CRT_MNTREQ_WHILE_UNLOADING),
            ('enter_pvl_mountcompleted', STATE_CRT_ERROR_1),
            ('exiting_pvl_mount_complete', STATE_CRT_UNLOAD_REQ),
            ('enter_pvl_dismountvolume', STATE_CRT_UNLOAD_REQ),
            ('exiting_pvl_dismountvolume', STATE_CRT_UNLOAD_REQ),
            ('entering_pvr_mount', STATE_CRT_ERROR_1),
            ('entering_pvr_dismountcart', STATE_CRT_UNLOAD_REQ),
            ('exiting_pvr_dismountcart', STATE_CRT_UNLOADED),
            ('cancel_event', STATE_CRT_LOADED),
            ('robot_mount', STATE_CRT_ERROR_1),
            ('robot_dismount', STATE_CRT_UNLOADED),
            ('warn_dismount', STATE_CRT_ERROR_1),
            ('robot_eject', STATE_CRT_EJECTED),
            ('pvr_eject', STATE_CRT_EJECTED),
            ('failed_crt', STATE_CRT_ERROR_1),
            ('failed_mount', STATE_CRT_ERROR_1),
            ('failed_dismount', STATE_CRT_ERROR_1),
            ('crt_not_checked_in', STATE_CRT_ERROR_1),
            ('warn_reading_device', STATE_CRT_ERROR_1),
            ('warn_crt_not_readable', STATE_CRT_ERROR_1),
        ),
        STATE_CRT_UNLOADED: (
            ('entering_pvl_mountadd', STATE_CRT_LOAD_RQST),
            ('exiting_pvl_mountadd', STATE_CRT_LOAD_RQST),
            ('enter_pvl_mountcompleted', STATE_CRT_ERROR_1),
            ('exiting_pvl_mount_complete', STATE_CRT_ERROR_1),
            ('enter_pvl_dismountvolume', STATE_CRT_ERROR_1),
            ('entering_pvr_mount', STATE_CRT_LOAD_RQST),
            ('entering_pvr_dismountcart', STATE_CRT_UNLOADED),
            ('exiting_pvr_dismountcart', STATE_CRT_UNLOADED),
            ('robot_mount', STATE_CRT_LOADED),
            ('robot_dismount', STATE_CRT_UNLOADED),
            ('robot_move', STATE_CRT_UNLOADED),
            ('robot_enter', STATE_CRT_UNLOADED),
            ('warn_dismount', STATE_CRT_UNLOADED),
            ('enter_pvr_inject', STATE_CRT_UNLOADED),
            ('exit_pvr_inject', STATE_CRT_UNLOADED),
            ('pvl_import', STATE_CRT_UNLOADED),
            ('exiting_pvl_import', STATE_CRT_UNLOADED),
            ('robot_audit', STATE_CRT_ERROR_1),
            ('robot_eject', STATE_CRT_EJECTED),
            ('pvr_eject', STATE_CRT_EJECTED),
            ('failed_crt', STATE_CRT_ERROR_1),
            ('failed_mount', STATE_CRT_ERROR_1),
            ('failed_dismount', STATE_CRT_ERROR_1),
            ('crt_not_checked_in', STATE_CRT_ERROR_1),
            ('warn_reading_device', STATE_CRT_ERROR_1),
            ('warn_crt_not_readable', STATE_CRT_ERROR_1),
        ),
        NONE_STATE: (
            ('entering_pvl_mountadd', STATE_CRT_LOAD_RQST),
            ('exiting_pvl_mountadd', NONE_STATE),
            ('enter_pvl_mountcompleted', STATE_CRT_MOUNTED),
            ('exiting_pvl_mount_complete', NONE_STATE),
            ('enter_pvl_dismountvolume', STATE_CRT_LOADED),
            ('exiting_pvl_dismountvolume', NONE_STATE),
            ('entering_pvr_mount', STATE_CRT_LOAD_RQST),
            ('entering_pvr_dismountcart', STATE_CRT_UNLOAD_REQ),
            ('exiting_pvr_dismountcart', STATE_CRT_UNLOADED),
            ('robot_mount', STATE_CRT_LOADED),
            ('robot_dismount', STATE_CRT_UNLOADED),
            ('robot_move', STATE_CRT_UNLOADED),
            ('robot_enter', STATE_CRT_UNLOADED),
            ('warn_dismount', STATE_CRT_ERROR_1),
            ('enter_pvr_inject', STATE_CRT_INJECTING),
            ('pvl_import', STATE_CRT_INJECTING),
            ('crt_checkout', STATE_CRT_EJECTED),
            ('robot_audit', STATE_CRT_ERROR_1),
            ('robot_eject', STATE_CRT_EJECTED),
            ('pvr_eject', STATE_CRT_EJECTED),
            ('failed_crt', STATE_CRT_ERROR_1),
            ('failed_mount', STATE_CRT_ERROR_1),
            ('failed_dismount', STATE_CRT_ERROR_1),
            ('crt_not_checked_in', STATE_CRT_ERROR_1),
            ('warn_reading_device', STATE_CRT_ERROR_1),
            ('warn_crt_not_readable', STATE_CRT_ERROR_1),
        ),
        STATE_CRT_MNTREQ_WHILE_UNLOADING: (
            ('entering_pvl_mountadd', STATE_CRT_MNTREQ_WHILE_UNLOADING),
            ('exiting_pvl_mountadd', STATE_CRT_MNTREQ_WHILE_UNLOADING),
            ('enter_pvl_mountcompleted', STATE_CRT_MNTREQ_WHILE_UNLOADING),
            ('exiting_pvl_mount_complete', STATE_CRT_MNTREQ_WHILE_UNLOADING),
            ('enter_pvl_dismountvolume', STATE_CRT_ERROR_1),
            ('entering_pvr_mount', STATE_CRT_UNLOADED),
            ('entering_pvr_dismountcart', STATE_CRT_MNTREQ_WHILE_UNLOADING),
            ('exiting_pvr_dismountcart', STATE_CRT_UNLOADED),
            ('robot_dismount', STATE_CRT_UNLOADED),
            ('robot_eject', STATE_CRT_EJECTED),
            ('pvr_eject', STATE_CRT_EJECTED),
            ('failed_crt', STATE_CRT_ERROR_1),
            ('failed_mount', STATE_CRT_ERROR_1),
            ('failed_dismount', STATE_CRT_ERROR_1),
            ('crt_not_checked_in', STATE_CRT_ERROR_1),
            ('warn_reading_device', STATE_CRT_ERROR_1),
            ('warn_crt_not_readable', STATE_CRT_ERROR_1),
        ),
        STATE_CRT_ERROR_1: (
            ('entering_pvl_mountadd', STATE_CRT_ERROR_1),
            ('exiting_pvl_mountadd', STATE_CRT_ERROR_1),
            ('enter_pvl_mountcompleted', STATE_CRT_LOADED),
            ('exiting_pvl_mount_complete', STATE_CRT_ERROR_1),
            ('enter_pvl_dismountvolume', STATE_CRT_ERROR_1),
            ('exiting_pvl_dismountvolume', STATE_CRT_ERROR_1),
            ('entering_pvr_mount', STATE_CRT_LOAD_RQST),
            ('entering_pvr_dismountcart', STATE_CRT_ERROR_1),
            ('exiting_pvr_dismountcart', STATE_CRT_UNLOADED),
            ('robot_mount', STATE_CRT_LOADED),
            ('robot_dismount', STATE_CRT_UNLOADED),
            ('robot_move', STATE_CRT_UNLOADED),
            ('robot_enter', STATE_CRT_UNLOADED),
            ('warn_dismount', STATE_CRT_ERROR_1),
            ('pvl_import', STATE_CRT_INJECTING),
            ('exiting_pvl_import', STATE_CRT_UNLOADED),
            ('crt_checkout', STATE_CRT_EJECTED),
            ('job_canceled', STATE_CRT_ERROR_1),
            ('robot_audit', STATE_CRT_ERROR_1),
            ('robot_eject', STATE_CRT_EJECTED),
            ('pvr_eject', STATE_CRT_EJECTED),
            ('failed_crt', STATE_CRT_ERROR_1),
            ('failed_mount', STATE_CRT_ERROR_1),
            ('failed_dismount', STATE_CRT_ERROR_1),
            ('crt_not_checked_in', STATE_CRT_ERROR_1),
            ('warn_reading_device', STATE_CRT_ERROR_1),
            ('warn_crt_not_readable', STATE_CRT_ERROR_1),
        ),
        STATE_CRT_LOAD_RQST: (
            ('entering_pvl_mountadd', STATE_CRT_LOAD_RQST),
            ('exiting_pvl_mountadd', STATE_CRT_LOAD_RQST),
            ('enter_pvl_mountcompleted', STATE_CRT_LOADED),
            ('exiting_pvl_mount_complete', STATE_CRT_MOUNTED),
            ('enter_pvl_dismountvolume', STATE_CRT_LOADED),
            ('entering_pvr_mount', STATE_CRT_LOAD_RQST),
            ('entering_pvr_dismountcart', STATE_CRT_ERROR_1),
            ('exiting_pvr_dismountcart', STATE_CRT_LOAD_RQST),
            ('robot_mount', STATE_CRT_LOADED),
            ('robot_dismount', STATE_CRT_LOAD_RQST),
            ('robot_move', STATE_CRT_LOAD_RQST),
            ('robot_enter', STATE_CRT_LOAD_RQST),
            ('enter_pvr_inject', STATE_CRT_INJECTING),
            ('pvl_import', STATE_CRT_INJECTING),
            ('job_canceled', STATE_CRT_ERROR_1),
            ('robot_audit', STATE_CRT_LOAD_RQST),
            ('robot_eject', STATE_CRT_EJECTED),
            ('pvr_eject', STATE_CRT_EJECTED),
            ('failed_crt', STATE_CRT_ERROR_1),
            ('failed_mount', STATE_CRT_ERROR_1),
            ('failed_dismount', STATE_CRT_ERROR_1),
            ('crt_not_checked_in', STATE_CRT_ERROR_1),
            ('warn_reading_device', STATE_CRT_ERROR_1),
            ('warn_crt_not_readable', STATE_CRT_ERROR_1),
        ),
        STATE_CRT_MOUNTED: (
            ('entering_pvl_mountadd', STATE_CRT_MOUNTED),
            ('exiting_pvl_mountadd', STATE_CRT_MOUNTED),
            ('enter_pvl_mountcompleted', STATE_CRT_MOUNTED),
            ('exiting_pvl_mount_complete', STATE_CRT_MOUNTED),
            ('enter_pvl_dismountvolume', STATE_CRT_LOADED),
            ('exiting_pvl_dismountvolume', STATE_CRT_LOADED),
            ('entering_pvr_mount', STATE_CRT_MOUNTED),
            ('entering_pvr_dismountcart', STATE_CRT_UNLOAD_REQ),
            ('exiting_pvr_dismountcart', STATE_CRT_UNLOADED),
            ('robot_mount', STATE_CRT_MOUNTED),
            ('robot_dismount', STATE_CRT_UNLOADED),
            ('robot_move', STATE_CRT_UNLOADED),
            ('robot_enter', STATE_CRT_UNLOADED),
            ('warn_dismount', STATE_CRT_UNLOAD_REQ),
            ('pvl_import', STATE_CRT_MOUNTED),
            ('exiting_pvl_import', STATE_CRT_MOUNTED),
            ('job_canceled', STATE_CRT_MOUNTED),
            ('robot_eject', STATE_CRT_EJECTED),
            ('pvr_eject', STATE_CRT_EJECTED),
            ('failed_crt', STATE_CRT_ERROR_1),
            ('failed_mount', STATE_CRT_ERROR_1),
            ('failed_dismount', STATE_CRT_ERROR_1),
            ('crt_not_checked_in', STATE_CRT_ERROR_1),
            ('warn_reading_device', STATE_CRT_ERROR_1),
            ('warn_crt_not_readable', STATE_CRT_ERROR_1),
        )
    }

    def __init__(self, id, basedir, eventlog=True, simulationmode=False):
        # @brief initialize cartridge class
        # @param id cartridge id
        # @param datadir base directory where cartridge folder will be created
        # @readonlymode open shelve in read only mode [experimental]
        BaseStateMachine.__init__(self, id,
                                  basedir=os.path.join(basedir, "cartridge"),
                                  eventlog=eventlog,
                                  simulationmode=simulationmode)
        self.sessions = CartridgePersistence(
            self.basedir, 'sessions', simulationmode)
        self.errors = CartridgePersistence(
            self.basedir, "errors", simulationmode)

        self._simulation_cache_current_location = None
        self._simulation_cache_current_event = None
        self.init_tmp()

        self.mntreq_while_unmount_timestamp = None
        self.flushed = False
        self._heat = collections.deque((), CRT_HEAT_DEQUE_LENGTH)
        for i in range(CRT_HEAT_DEQUE_LENGTH):
            self._heat.append(datetime(1970, 1, 1))

        for i in ['drive_enabled', 'robot_acscr']:
            self.ignore(i)

    def flush(self):
        self.sessions.flush()
        self.errors.flush()

    def filtered(self, eventinst):
        return False

    def close(self):
        self.sessions.close()
        self.errors.close()

    def init_tmp(self):
        # if self.tmp['unloaded_to_libraryid'] != None:
        #    self.current_home_library = self.tmp['unloaded_to_libraryid']
        # else:
        #    self.current_home_library = None
        self.tmp = {
            'load_request': None,
            'load_complete': None,
            'mount_operations': [],
            'unload_request': None,
            'unload_complete': None,
            'driveid': None,
            'loaded_from_libraryid': None,
            'unloaded_to_libraryid': None
        }
        self.flushed = True

    def save(self):
        if self._simulation_mode:
            self.init_tmp()
            return

        if isinstance(self.tmp['unload_complete'], datetime):
            # key = self.tmp['unload_complete'].strftime("%Y%m%d-%H%M%S")
            self.sessions.put(self.tmp)
        else:
            # key = "%i"%self.errorcnt
            # self.errorcnt+=1
            self.errors.put(self.tmp)
            # raise Exception("wrong type %s"%self.tmp['unload_complete'])
        self.init_tmp()

    def handle_loaded_from_lib(self, event):
        if 'libraryid' in event.attributes:
            if self.tmp['loaded_from_libraryid'] == None:
                self.tmp['loaded_from_libraryid'] = event.attributes[
                    'libraryid']
            elif self.tmp['loaded_from_libraryid'] == \
                    event.attributes['libraryid']:
                pass
            else:
                pass
                # self.raiseDump(event)
        # else:
        #    if self.current_home_library!= None:
        #        if self.tmp['loaded_from_libraryid'] == None:
        #            self.tmp['loaded_from_libraryid'] =
        #               self.current_home_library
        #            event.attributes['libraryid'] = self.current_home_library
        #            self.current_home_library = None
        #        elif self.tmp['loaded_from_libraryid'] ==
        #               self.current_home_library:
        #            event.attributes['libraryid'] = self.current_home_library
        #            self.current_home_library = None
        #        else:
        #            self.raiseDump(event)

    def handle_unloaded_to_lib(self, event):
        if 'libraryid' in event.attributes:
            # else it is not a unload operation
            if self.tmp['load_complete'] != None:
                if self.tmp['unloaded_to_libraryid'] == None:
                    self.tmp['unloaded_to_libraryid'] = event.attributes[
                        'libraryid']
                elif self.tmp['unloaded_to_libraryid'] == \
                        event.attributes['libraryid']:
                    pass
                else:
                    if event.attributes['cartridgeid'].startswith('CLN'):
                        pass    # ignore this problem, is an instate callback
                    else:
                        pass  # @todo handle this
                        # self.raiseDump(event)  # unhandled state

    # # callbacks
    def handle_drive(self, event):
        if not self.flushed:
            if 'capid' in event.attributes:
                return
            if 'driveid' in event.attributes and \
                    event.attributes['driveid'] != '0':
                if not self.tmp['driveid']:
                    self.tmp['driveid'] = event.attributes['driveid']
                elif self.tmp['driveid'] == event.attributes['driveid']:
                    pass
                else:
                    # for l in [self.datashelve['success'],
                    #          self.datashelve['error']]:
                    #    if len(l) > 0:
                    lastentry = self.sessions.get_last_entry()
                    if lastentry.has_key('driveid'):
                        if lastentry['driveid'] == event.attributes['driveid']\
                                or lastentry['driveid'] is None:
                            if isinstance(lastentry['unload_complete'],
                                          datetime):
                                if get_diff_in_seconds(
                                    lastentry['unload_complete'],
                                    event.get_time()) \
                                        < self.global_event_timeout:
                                    # this seems to be a delayed robot
                                    # dismount request
                                    self.tmp['driveid'] = None
                                    return

                    if self.state == STATE_CRT_ERROR_1:
                        self.tmp['driveid'] = event.attributes['driveid']
                    elif self.id.startswith('CLN'):
                        self.tmp['driveid'] = event.attributes['driveid']
                    else:
                        pass  # must be an errorcase
                        # self.raiseDump(event)
            else:
                if self.tmp['driveid'] != None:
                    event.attributes['driveid'] = self.tmp['driveid']
        else:
            self.flushed = False

    def post_transition(self, eventinst):
        self.handle_drive(eventinst)

    def pre_transition(self, eventinst):
        self.handle_drive(eventinst)

    def entercb_LOAD_REQ(self, event):
        # if self.unload_ts != None:
        #    self.datashelve['time_between_unloadreq_reloadreq'].append(
        #        event.attributes['datetime']-self.unload_ts)
        #    self.unload_ts = None

        if not self.tmp['load_request']:
            # self.datashelve['total_loads'] += 1
            if self.mntreq_while_unmount_timestamp is not None:
                self.tmp['load_request'] = self.mntreq_while_unmount_timestamp
                self.mntreq_while_unmount_timestamp = None
            elif self.tmp['unload_request'] is not None:
                self.save()
                # flush error
                self.tmp['load_request'] = event.get_time()
            else:
                self.tmp['load_request'] = event.get_time()
        else:
            diff = event.attributes['datetime'] - self.tmp['load_request']
            if diff.seconds <= 1:  # same event
                pass
            elif diff.seconds > self.global_event_timeout:
                raise Exception("event:%s;%s;self.tmp:%s" %
                                (event, self.id, str(self.tmp)))
                #self.log("Already loadreq seen, but seems to have timed out")
                # self.raiseDump(event)
                self.save()
            else:

                # print "what happend here", self.id, str(event)
                # self.raiseDump(event)
                # @todo handle this better
                self.save()
        self.handle_loaded_from_lib(event)
        # self.handle_drive(event)

    def instatecb_LOAD_REQ(self, event):
        if not self.tmp['load_request']:
            self.tmp['load_request'] = event.get_time()
        self.handle_loaded_from_lib(event)

    def leavecb_LOAD_REQ(self, event):
        self.handle_loaded_from_lib(event)

    def entercb_LOADED(self, event):
        # self.datashelve['total_loads'] += 1
        self.handle_loaded_from_lib(event)
        if 'drive' in event.attributes:
            self.set_current_location(event.attributes['drive'])
        if self.tmp['load_complete'] == None:
            self.tmp['load_complete'] = event.get_time()
        #    if self.tmp['load_request'] != None:
        #        self.datashelve['latency_load'].append(
        #               self.tmp['load_complete']-self.tmp['load_request'])
        # self.handle_drive(event)
        if event.name == "cancel_event":
            self._simulation_cache_current_event = None

    def instatecb_LOADED(self, event):
        if 'drive' in event.attributes:
            self.set_current_location(event.attributes['drive'])

    def entercb_MOUNTED(self, event):
        # self.datashelve['total_mounts'] += 1
        # if self.unmount_ts!=None:
        #    self.datashelve['time_between_unmount_remount'].append(
        #           event.attributes['datetime']-self.unmount_ts)
        #    self.unmount_ts = None
        # self.tmp['mount_operations'].append(
        #           [event.attributes['datetime'],None])
        evtime = event.get_time()
        if self.tmp['load_complete'] == None:
            self.tmp['load_complete'] = evtime
            # if self.tmp['load_request']!=None:
            #    self.datashelve['latency_load'].append(
            #       self.tmp['load_complete']-self.tmp['load_request'])
        elif self.tmp['load_complete'] == evtime:
            pass  # duplicate msg
        # else:
        #    self.raiseDump(event)
        # self.handle_drive(event)
        self.tmp['mount_operations'].append([evtime, None])
        self._heat.append(evtime)

    def leavecb_MOUNTED(self, event):
        if self.tmp['mount_operations'][-1][1] == None:
            self.tmp['mount_operations'][-1][1] = event.get_time()
        else:
            self.raiseDump(event)
        # self.datashelve['duration_mount']=self.tmp['mount_operations'][-1][1]
        # -self.tmp['mount_operations'][-1][0]
        # self.unmount_ts=event.attributes['datetime']

    def entercb_UNLOAD_REQ(self, event):  # check alpha
        # self.unload_ts = event.attributes['datetime']
        self.tmp['unload_request'] = event.get_time()
        # self.handle_drive(event)
        # if self.unmount_ts!=None:
        # self.datashelve['time_between_unmount_unload'] =
        #   event.attributes['datetime'] - self.unmount_ts
        # self.unmount_ts=None
        self.handle_unloaded_to_lib(event)

    def entercb_UNLOADED(self, event):
        self.handle_unloaded_to_lib(event)
        # if len(self.tmp['mount_operations']) > 0 and \
        #        self.tmp['load_request'] is not None:
        self.tmp['unload_complete'] = event.get_time()
        self.save()
        # else:
        #    pass # init operation from import/inject/0 ...
        if 'libraryid' in event.attributes:
            self.set_current_location(event.attributes['libraryid'])

    def leavecb_UNLOADED(self, event):
        self.tmp['driveid'] = None  # must not exist at this stage
        self.handle_loaded_from_lib(event)

    def instatecb_UNLOADED(self, event):
        self.handle_unloaded_to_lib(event)
        self.tmp['driveid'] = None   # must not exist at this stage
        self.flushed = True

    def entercb_MNTREQ_WHILE_UNLOADING(self, event):
        # @todo fix import,
        t = event.get_time()
        if self.tmp['unload_request'] != None:  # must be the case
            if get_diff_in_seconds(self.tmp['unload_request'], t) < \
                    self.global_event_timeout:
                self.mntreq_while_unmount_timestamp = t
                return
        # in any other case the robot dismount operations seems to be lost
        # somewhere
        self.state = STATE_CRT_ERROR_1  # this is a hack. can i improve this

    def leavecb_ERROR_1(self, event):
        self.save()

    def entercb_EJECTED(self, event):
        # self.save()
        # self.datashelve['ejected'].append(event.get_time())
        self.init_tmp()

    def extract_read_requests(self):
        try:
            reads = {}
            for op in self.sessions.get_operations():
                for mnt in op['mount_operations']:
                    reads[mnt[0]] = get_diff_in_seconds(mnt[0], mnt[1])
            for op in self.errors.get_operations():
                for mnt in op['mount_operations']:
                    reads[mnt[0]] = get_diff_in_seconds(mnt[0], mnt[1])
        except:
            raise
        return reads

    # simulation functions
    def set_current_location(self, location):
        #self._simulation_cache['current_location'] = location
        self._simulation_cache_current_location = location

    def get_current_location(self):
        return self._simulation_cache_current_location

    def set_current_event(self, eventobj):
        self._simulation_cache_current_event = eventobj

    def get_current_event(self):
        return self._simulation_cache_current_event  # ', None)

    def get_heat(self, clock):
        try:
            return get_diff_in_seconds(self._heat[0], clock)
        except TypeError:
            return sys.maxint

    def extend_reading_time_of_current_request(self, seconds):
        self._simulation_cache_current_event.set('readlength',
                                                 self._simulation_cache_current_event.get('readlength') + seconds)


    def take_snapshot(self, snapshotdate):
        try:
            entry_list = self.sessions.get_entry_before_timestamp(
                int(snapshotdate[:4]),
                int(snapshotdate[4:6]),
                int(snapshotdate[6:8]))
            if len(entry_list) > 0:
                entry = entry_list[0]
                driveid = entry[5]
                levelid = int(entry[5][1:3])
                return (levelid, driveid)
        except:
            raise
        return (None, None)