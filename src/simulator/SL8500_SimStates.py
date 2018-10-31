from utilities.fsm.StateMachineEvent import Event

__author__ = 'maesker'

STATE_SL8500_RBT_LOADREQ = 1
STATE_SL8500_RBT_LOADING = 2
STATE_SL8500_RBT_LOADCOMPLETE = 3
STATE_SL8500_DRV_READING = 4
STATE_SL8500_DRV_READINGDONE = 5
STATE_SL8500_DRV_UNLOADREQ = 6
STATE_SL8500_DRV_UNLOADING = 7
STATE_SL8500_RBT_UNLOADCOMPLETE = 8
STATE_SL8500_RBT_MIGRATE_SEND = 9
STATE_SL8500_RBT_MIGRATE_AT_GATE = 10
STATE_SL8500_RBT_MIGRATE_RECEIVE = 11
STATE_SL8500_RBT_MOVE = 12
STATE_SL8500_RBT_MOVE_COMPLETE = 13
# STATE_SL8500_RBT_LOADREQ = "RBT_LOADREQ"
# STATE_SL8500_RBT_LOADING = "RBT_LOADING"
# STATE_SL8500_RBT_LOADCOMPLETE = "RBT_LOADCOMPLETE"
# STATE_SL8500_DRV_READING = "DRV_READING"
# STATE_SL8500_DRV_READINGDONE = "DRV_READINGDONE"
# STATE_SL8500_DRV_UNLOADREQ = "DRV_UNLOADREQ"
# STATE_SL8500_DRV_UNLOADING = "DRV_UNLOADING"
# STATE_SL8500_RBT_UNLOADCOMPLETE = "RBT_UNLOADCOMPLETE"


class Sim_Event(Event):
    __slots__ = ['id', 'name', 'attributes', 'get']

    def __init__(self, id, name, attributes):
        Event.__init__(self, id, name, attributes)

    def __repr__(self):
        s = ""
        for i in ['cartridgeid', 'driveid', 'libraryid']:
            if i in self.attributes:
                s += " %s:%s" % (i, self.attributes[i])
        return "ID:%07i;name:%27s;%s-%s" % (self.id, self.name,
                                            self.attributes['datetime'], s)

    def delete(self, att):
        if att in self.attributes:
            del self.attributes[att]


class Sim_Event_v2:

    def __init__(self, id, name):
        self.id = id
        self.name = name

    def __repr__(self):
        s = ""
        for i in ['cartridgeid', 'driveid', 'libraryid']:
            if i in self.attributes:
                s += " %s:%s" % (i, self.attributes[i])
        return "ID:%07i;name:%27s;%s-%s" % (self.id, self.name,
                                            self.attributes['datetime'], s)

    def delete(self, att):
        if att in self.attributes:
            del self.attributes[att]
