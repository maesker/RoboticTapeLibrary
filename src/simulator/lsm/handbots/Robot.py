import logging
import datetime
import collections
import re
#from simulator.SL8500 import *
from simulator.SL8500_SimStates import *

__author__ = 'maesker'

COLLISION_DETECTION_OPTIMIZATION = 1
SOFTWARE_MAGIC_LOAD_DELAY = 15
SECONDS_ROBOTHAND = 1.0
SECONDS_PER_CAPACITY = 1.8

TIME_IN_COLLISION_AREA = round(SECONDS_ROBOTHAND + SECONDS_PER_CAPACITY, 0)

LIBRARY_FACE_SEP = 15.0
LIBRARY_FACE_INVERTER = 75/LIBRARY_FACE_SEP

LIBRARY_FACE_FACTOR = SECONDS_PER_CAPACITY/LIBRARY_FACE_INVERTER
#lib settings: total 75 for inner, outer walls,  62 front
# library face is 5x15

DRIVE_FACE_SEP = 4.0
DRIVE_FACE_INVERTER = 16.0/DRIVE_FACE_SEP

DRIVE_FACE_FACTOR = SECONDS_PER_CAPACITY/DRIVE_FACE_INVERTER
# my drive settings:
# 12  8 4 0
# 13  9 5 1
# 14 10 6 2
# 15 11 7 3
#


# Walls:
# 0: left outer
# 1: left inner
# 2: front, e.g. drives
# 3: right inner
# 4: right outer
WALLFACTOR = {
    0: {0:1, 1:1, 2:1, 3:2, 4:2}, # left out
    1: {0:1, 1:1, 2:1, 3:2, 4:2}, # left in
    2: {0:1, 1:1, 2:1, 3:1, 4:1}, # front
    3: {0:2, 1:2, 2:1, 3:1, 4:1}, # right in
    4: {0:2, 1:2, 2:1, 3:1, 4:1} # right out
}

class Robot:

    """ @brief Robot moving cartridges between drives and libraries
    """

    def __init__(self, levelid, id, move_callback):
        # @brief robot class constructor
        # @param id robot identifier
        # @param move_callback: reference to a robotsystem instance
        # function returning the costs in seconds to move to a given position
        #self.prefix = prefix
        self.id = int(id)
        self.prefix = int(levelid)
        self.move_callback = move_callback
        self.simlog = logging.getLogger()
        self.simlog.info("Robot: id:%i.%s\nConfig COLLISION_DETECTION_OPTIMIZATION:%s, SOFTWARE_MAGIC_LOAD_DELAY:%s, SECONDS_ROBOTHAND:%s, SECONDS_PER_CAPACITY:%s", id,self.prefix,COLLISION_DETECTION_OPTIMIZATION,SOFTWARE_MAGIC_LOAD_DELAY,SECONDS_ROBOTHAND,SECONDS_PER_CAPACITY)
        self.current_request = None
        self._actionperformed = 0
        self.request_start = 0
        self.current_location = u"D_%02i_%02i"%(self.prefix,id) # drive

        #self.libraryregex = re.compile("L_(?P<level>[0-9]{2})_(?P<wall>[0-9]{1})_(?P<capacity>[0-9]{1})_(?P<lib>[0-9]{3})")
        self._homecell = u"L_%02i_%i_1_000"%(self.prefix, self.id*4)
        # should result in left robot goint to left wall (0) cap extention 1
        # and right robot going to

    def move_aside(self, clock):
        costs_to_homelib = self.move_callback(self.current_location,self._homecell, self.id)
        self.request_end = clock + datetime.timedelta(seconds=round(costs_to_homelib,0))
        self.current_request = "MOVE_ASIDE"
        #self.simlog.debug("%s:%s move aside %s"%())

    def idle(self):
        # @brief return true if no current request is being handled
        return self.current_request is None and self._actionperformed == 0

    def process(self, eventobj, clock, collision_prevention_penalty=0):
        self._actionperformed = 1
        self.current_request = eventobj
        crtlocation = self.get_source()
        self.request_start = clock

        cur_to_newcrt = self.move_callback(self.current_location,crtlocation, self.id)
        newcrt_to_drive = self.move_callback(crtlocation, eventobj.get('target'), self.id)
        if eventobj.name == "crt_read_request":
            collision_prevention_penalty += SOFTWARE_MAGIC_LOAD_DELAY
        dur = round(2*SECONDS_ROBOTHAND + cur_to_newcrt + newcrt_to_drive + collision_prevention_penalty, 0)
        self.request_end = clock + datetime.timedelta(seconds=dur)
        if dur <=0:
            self.simlog.error("0 duration ")
            raise Exception("0 duration")

        if self.current_request.get('state') == STATE_SL8500_DRV_UNLOADREQ:
            self.current_request.set('state', STATE_SL8500_DRV_UNLOADING)

        elif self.current_request.get('state') == STATE_SL8500_RBT_LOADREQ:
            self.current_request.set('state', STATE_SL8500_RBT_LOADING)

        self.simlog.debug("Robot id:%s.%s, duration:%s to %s request:%s" % (
            self.id, self.prefix, self.request_start, self.request_end, str(eventobj)))
        return None

    def step(self, clock):
        self._actionperformed = 0
        if self.request_end != None:
            if self.request_end > clock:
                self._actionperformed = 1
                #self.simlog.debug("still processing request:%s"%str(self.current_request))
                pass  # still busy
            elif self.request_end == clock:
                self._actionperformed = 1
                return self.handle_request_completion(clock)
            else:
                self.simlog.error("Robot.step id:%s, missed event end. Why?" % (self.id))
        #else:
            #self.simlog.debug("I am idle")
        #    raise Exception("no request end? %s"%str(self.current_request))

    def handle_request_completion(self, clock):
        if self.current_request == "MOVE_ASIDE":
            self.current_location = self._homecell
            self.request_end = None
            self.request_start = None
            self.current_request = None
            self.simlog.debug("robot %s.%s move aside done"%(self.prefix,self.id))
            return
        domoveaside = False
        event = self.current_request
        self.simlog.debug("clock:%s, id:%s, oldstate:%s" %
                          (clock, event.get_id(), event.get('state')))
        if event.get('state') == STATE_SL8500_RBT_LOADING:
            if event.get('target') == event.get('driveid'):
                event.set('state', STATE_SL8500_RBT_LOADCOMPLETE)
                diff = clock-event.get('datetime')
                event.set("load_completion_latency",diff.seconds)
                self.current_location = event.get('target')
                event.set('datetime', self.request_end)
                self.simlog.debug("placed at drive")
                domoveaside=True
            else:
                self.simlog.debug("completed, but not into drive. must be a passthru or elevator")
        elif event.get('state') == STATE_SL8500_DRV_UNLOADING:
            diff = clock-event.get('datetime')
            event.set("unload_completion_latency",diff.seconds)
            event.set('state', STATE_SL8500_RBT_UNLOADCOMPLETE)
            self.current_location = event.get('target')
            event.set('datetime', self.request_end)
        elif event.get('state') == STATE_SL8500_RBT_MIGRATE_SEND:
            #diff = clock-event.get('datetime')
            event.set('state', STATE_SL8500_RBT_MIGRATE_AT_GATE)
            self.current_location = event.get('target')
        elif event.get('state') == STATE_SL8500_RBT_MOVE:
            event.set('state', STATE_SL8500_RBT_MOVE_COMPLETE)
            self.current_location = event.get('target')
        else:
            raise Exception("what is this")
        self.simlog.debug("clock:%s, id:%s, newstate:%s" %
                          (clock, event.get_id(), event.get('state')))

        followup = event.get('follow_up_event')
        if not followup:
            self.request_end = None
            self.request_start = None
            self.current_request = None
            if domoveaside:
                self.move_aside(clock)
        else:
            self.process(followup, clock)
        return event

    def get_source(self):
        # @brief return the location of the current event cartridge
        # based on the cartridges state
        if self.current_request.get('state') is STATE_SL8500_RBT_LOADREQ:
            return self.current_request.get('libraryid')
        elif self.current_request.get('state') is STATE_SL8500_DRV_UNLOADREQ:
            return self.current_request.get('driveid')
        elif self.current_request.get('state') is STATE_SL8500_RBT_MIGRATE_SEND:
            return self.current_request.get('libraryid')
        elif self.current_request.get('state') is STATE_SL8500_RBT_MOVE:
            return self.current_request.get('libraryid')
        else:
            raise Exception("cant identify soure location")
        # elif self.current_request.get('state') is STATE_SL8500_RBT_LOADREQ:

    def is_processing(self, crt):
        try:
            if self.current_request:
                if self.current_request.get('cartridgeid')==crt:
                    return True
        except:
            return False

    def is_processing_drive(self, driveid):
        try:
            if self.current_request:
                drv = self.current_request.get('driveid')
                if drv == driveid:
                    return self.current_request.get('cartridgeid')
        except:
            return False

    def cancel_current_event(self):
        ev = self.current_request
        self.request_end = None
        self.request_start = None
        self.current_request = None
        return ev

    def time_left(self, clock):
        if self.request_end:
            diff = self.request_end-clock
            return max(0, diff.seconds)
        return 0

    def time_until_no_collision_potential(self,clock):
        """
        :return: (a,b) a: time until robot enters coll area, b: time until it leaves
        """
        if not self.current_request:
            return (0,0)
        if self.current_request == "MOVE_ASIDE":
            return (0,0)
        target = self.current_request.get('target')
        if target[0]=="D":# drive target
            t = self.time_left(clock)
            return (t-TIME_IN_COLLISION_AREA,t)
        if target[0] in ('e','p'): # elevator or elevator
            return (0,0)
        distance_drive_to_reqtarget = self.move_callback(u"D_%02i_%02i"%(self.prefix,self.id),target, self.id)
        return (0,max(0, self.time_left(clock)-distance_drive_to_reqtarget))

class RobotSystem:
    """
    @brief Main robot system managing the individual robot instances
    """

    def __init__(self, levelid, number_of_robots, capacityextensions):
        self.capacityextensions = capacityextensions
        self.levelid = levelid
        self.robot_left = Robot(levelid,0, self.move_to_costs)
        self.robot_right = None
        self.robots = [self.robot_left]
        self.number_of_robots =number_of_robots
        if number_of_robots == 2:
            self.robot_right = Robot(levelid,1, self.move_to_costs)
            self.robots.append(self.robot_right)
        self.simlog = logging.getLogger()
        self.simlog.info("Robotsystem: %i robots created", number_of_robots)
        self.robot_queue = collections.deque()
        self.enqueue = self.robot_queue.append
        self.next_event_robotmapping = [None,None]
        # libraryid: L_%02i_%i_%i_%03i
        self.libraryregex = re.compile("L_(?P<level>[0-9]{2})_(?P<wall>[0-9]{1})_(?P<capacity>[0-9]{1})_(?P<lib>[0-9]{3})")
        self.elevator_regex = re.compile("el_(?P<wall>r|l)_(?P<level>[0-9]+)")
        self.passthru_regex = re.compile("pt_(?P<src>[0-9]+)_(?P<tgt>[0-9]+)")
        self.drive_regex = re.compile("D_(?P<level>[0-9]{2})_(?P<drvid>[0-9]{2})")

    def is_processing(self, crt):
        for r in self.robots:
            if r.is_processing(crt):
                return True

    def superstep(self, clock):
        return_events = []
        #self.simlog.debug("%s.queuesize %s",self.levelid,len(self.robot_queue))
        if self.next_event_robotmapping[0] == None:
            if len(self.robot_queue) > 0:
                if not self.robot_right:
                    self.next_event_robotmapping = [True,False]
                else:
                    nextevent = self.robot_queue[0]
                    self.next_event_robotmapping = [False,False]
                    homecell = nextevent.get('libraryid')
                    if homecell:
                        m = self.libraryregex.match(homecell)
                        if m:
                            wall = int(m.group('wall'))
                            if wall in [0,1]:
                                self.next_event_robotmapping[0]=True
                            elif wall in [3,4]:
                                self.next_event_robotmapping[1]=True
                            else:
                                self.next_event_robotmapping[0]=True
                                self.next_event_robotmapping[1]=True
                        else:
                            self.elevator_regex = re.compile("el_(?P<wall>r|l)_(?P<level>[0-9]+)")
                            m_el = self.elevator_regex.match(homecell)
                            if m_el:
                                if m_el.group('wall')=="r":
                                    self.next_event_robotmapping[0]=False
                                    self.next_event_robotmapping[1]=True
                                else:
                                    self.next_event_robotmapping[1]=False
                                    self.next_event_robotmapping[0]=True
                            else:
                                self.next_event_robotmapping[0]=True
                                self.next_event_robotmapping[1]=True
                    else:
                        raise Exception("unknown event %s"%nextevent.name)

        if self.robot_left.idle(): # handle left robot
            if len(self.robot_queue)>0:
                if self.next_event_robotmapping[0] :
                    collision_prevention_penalty=0
                    if self.robot_right:
                        (enter_ca,collision_prevention_penalty) = self.robot_right.time_until_no_collision_potential(clock)
                        if COLLISION_DETECTION_OPTIMIZATION:
                            #self.simlog.warning("COLDEC:%s",collision_prevention_penalty)
                            if self.robot_queue[0].name == "crt_read_request":
                                tocrt = self.move_to_costs(self.robot_left.current_location, self.robot_queue[0].get('libraryid'), self.robot_left.id)
                                #crttodrive =self.move_to_costs(self.robot_queue[0].get('libraryid'),"D_%02i_00"%self.levelid, self.robot_left.id)
                                total = tocrt#+SECONDS_ROBOTHAND+crttodrive
                                collision_prevention_penalty = round(max(0,collision_prevention_penalty-total) ,0)
                            else:
                                loctodrive =self.move_to_costs(self.robot_left.current_location,"D_%02i_07"%self.levelid, self.robot_left.id)
                                if loctodrive < enter_ca:
                                    collision_prevention_penalty=0 # will leave CA before left robot enters
                                else:
                                    self.simlog.debug("homecell cur/ location to drive %s"%loctodrive)
                                    collision_prevention_penalty = round(max(0,collision_prevention_penalty-loctodrive),0)

                    retval = self.robot_left.process(self.robot_queue.popleft(), clock, collision_prevention_penalty)
                    self.next_event_robotmapping = [None,None]
                    if retval:
                        return_events.append(retval)
        else:
            retval = self.robot_left.step(clock)
            if retval:
                return_events.append(retval)

        if self.robot_right:    # # right robot stuff # #
            if self.robot_right.idle():
                if len(self.robot_queue)>0:
                    if self.next_event_robotmapping[1]:
                        (enter_ca,collision_prevention_penalty) = self.robot_left.time_until_no_collision_potential(clock)
                        if COLLISION_DETECTION_OPTIMIZATION:
                            if self.robot_queue[0].name == "crt_read_request":
                                tocrt = self.move_to_costs(self.robot_right.current_location, self.robot_queue[0].get('libraryid'), self.robot_right.id)
                                #crttodrive =self.move_to_costs(self.robot_queue[0].get('libraryid'),"D_%02i_00"%self.levelid, self.robot_right.id)
                                total = tocrt#+SECONDS_ROBOTHAND#+crttodrive
                                collision_prevention_penalty = max(0,round(collision_prevention_penalty-total,0))
                            else:
                                loctodrive =self.move_to_costs(self.robot_right.current_location,"D_%02i_07"%self.levelid, self.robot_right.id)
                                self.simlog.debug("homecell cur/ location to drive %s"%loctodrive)
                                if loctodrive < enter_ca:
                                    collision_prevention_penalty=0 # will leave CA before left robot enters
                                else:
                                    collision_prevention_penalty = round(max(0,collision_prevention_penalty-loctodrive),0)
                        retval = self.robot_right.process(self.robot_queue.popleft(), clock, collision_prevention_penalty)
                        self.next_event_robotmapping = [None,None]
                        if retval:
                            return_events.append(retval)
            else:
                retval = self.robot_right.step(clock)
                if retval:
                    return_events.append(retval)
        return return_events

    def move_to_costs(self, source, target, robotid):
        # @brief calculate costs to move from source to target
        # @param source current location
        # @param target target location

        def libtolib(w1,c1, w2,c2):
            #w1,c1 = s
            #w2,c2 = t
            dist = 0
            wallfaktor = WALLFACTOR[w1][w2]
            if wallfaktor == 1:
                dist = abs(c2-c1)#-min(c2,c1)
            elif wallfaktor == 2:
                dist = c1+c2
            #dist = max(dist, 1)
            return SECONDS_PER_CAPACITY*dist

        # libraryid: L_%02i_%i_%i_%03i
        #if not isinstance(source, basestring):
        #    raise Exception("source is not a string, type:%s"%type(source))
        #if not isinstance(target, basestring):
        #    raise Exception("target is not a string, type:%s"%type(target))
        def _identify(location, robotid):
            lib = self.libraryregex.match(location)
            if lib:
                return (int(lib.group('wall')),int(lib.group('capacity')),int(lib.group('lib')))

            drvmatch = self.drive_regex.match(location)
            if drvmatch:
                return (2,0,int(drvmatch.group('drvid')))

            elmatch = self.elevator_regex.match(location)
            if elmatch:
                if elmatch.group('wall')=='l':
                    return (1, self.capacityextensions, 0)
                else:
                    return (4, self.capacityextensions, 0)

            ptmatch = self.passthru_regex.match(location)
            if ptmatch:
                if int(ptmatch.group('src'))==int(robotid):
                    return (4, 1, 0)
                else:
                    return (1, 1, 0)
            raise Exception("unknown location")

        def handle_same_side(src_cap, src_id, tgt_cap, tgt_id):
            tmp1 = src_id/LIBRARY_FACE_SEP
            tmp2 = tgt_id/LIBRARY_FACE_SEP
            if src_cap > tgt_cap:
                tmp2 = LIBRARY_FACE_SEP-tmp2
            elif tgt_cap > src_cap:
                tmp1 = LIBRARY_FACE_INVERTER-tmp1
            else:
                return (max(tmp2,tmp1) - min(tmp2,tmp1))*LIBRARY_FACE_FACTOR
            return (tmp1+tmp2)*LIBRARY_FACE_FACTOR

        try:
            src_wall, src_cap, src_id = _identify(source, robotid)
            tgt_wall, tgt_cap, tgt_id = _identify(target, robotid)
        except TypeError, e:
            raise e
        seconds_by_capactiy_index = libtolib(src_wall, src_cap, tgt_wall, tgt_cap)

        adjust = 0
        if src_wall == 2:
            #if source[0]!='D': # front face
            #    software_magic_load_delay = SOFTWARE_MAGIC_LOAD_DELAY
            tmp1 = src_id%DRIVE_FACE_SEP
            # source is a drive
            if tgt_wall == 2:
                 # target is also drive
                tmp2 = tgt_id%DRIVE_FACE_SEP
                adjust = DRIVE_FACE_FACTOR*(max(tmp1,tmp2) - min(tmp1,tmp2))
            else:
                tmp2 = tgt_id/LIBRARY_FACE_SEP
                if tgt_wall < 2:
                    # target is left sided library
                    # invert the drive id distance dactor
                    tmp1 = DRIVE_FACE_INVERTER-tmp1
                adjust = tmp1*DRIVE_FACE_FACTOR + tmp2*LIBRARY_FACE_FACTOR
        else:
            # src is left sided
            #software_magic_load_delay = SOFTWARE_MAGIC_LOAD_DELAY
            tmp1 = src_id/LIBRARY_FACE_SEP
            if src_wall < 2:
                if tgt_wall == 2:
                     # target is a drive
                    tmp2 = DRIVE_FACE_INVERTER-(tgt_id%DRIVE_FACE_SEP)
                    adjust = tmp1*LIBRARY_FACE_FACTOR + tmp2*DRIVE_FACE_FACTOR
                else:
                    tmp2 = tgt_id/LIBRARY_FACE_SEP
                    if tgt_wall < 2:
                        adjust = handle_same_side(src_cap, src_id, tgt_cap, tgt_id)
                    else:
                         adjust = (tmp1+tmp2)*LIBRARY_FACE_FACTOR

            else:
                if tgt_wall == 2:
                     # target is a drive
                    tmp2 = tgt_id/DRIVE_FACE_SEP
                    adjust = tmp1*LIBRARY_FACE_FACTOR + tmp2*DRIVE_FACE_FACTOR
                else:
                    tmp2 = tgt_id/LIBRARY_FACE_SEP
                    if tgt_wall > 2:
                        adjust = handle_same_side(src_cap, src_id, tgt_cap, tgt_id)
                    else:
                         adjust = (tmp1+tmp2)*LIBRARY_FACE_FACTOR

        #collision_penalty = 0
        #if self.potential_collision():
        #    collision_penalty = COLLISION_PENALTY
        seconds_by_capactiy_index += adjust
        return seconds_by_capactiy_index
        #ret = round(seconds_by_capactiy_index,0)
        #return ret

    def get_idle_robots(self):
        idle = []
        if len(self.robot_queue) <= 1:
            if self.robot_left.idle():
                idle.append(self.robot_left)
            if self.robot_right:
                if self.robot_right.idle():
                    idle.append(self.robot_right)
        return idle

    def check_driveallocation(self, drvid):
        for event in self.robot_queue:
            event_drvid = event.get('driveid')
            if event_drvid == drvid:
                if event.name == "robot_dismount":
                    followup = event.get('follow_up_event')
                    if followup:
                        return followup.get('cartridgeid')
                elif event.name == "crt_read_request":
                    return event.get('cartridgeid')
                else:
                    raise Exception("Unknown event type:%s"%(event))
        for robot in self.robots:
            crt = robot.is_processing_drive(drvid)
            if crt:
                return crt

    def cancel_events(self, driveid):
        ret = []
        queuerem = None
        for event in self.robot_queue:
            event_drvid = event.get('driveid')
            if event_drvid == driveid:
                queuerem = event
                break   # should only be one event
        if queuerem:
            ret.append(queuerem)
            self.robot_queue.remove(queuerem)
        for robot in self.robots:
            crt = robot.is_processing_drive(driveid)
            if crt:
                ret.append(robot.cancel_current_event())
        return ret

    #def potential_collision(self):
    #    if len(self.robots)>1:
    #        return not self.robots[0].idle() and not self.robots[1].idle()
    #    return False