__author__ = 'maesker'


from utilities.fsm.StateMachineEvent import Event


class WHPSS_Event(Event):

    def __init__(self, id, name, attributes):
        Event.__init__(self, id, name, attributes)


class RobotEvent(Event):

    def __init__(self, id, name, attributes):
        Event.__init__(self, id, name, attributes)

        if 'driveid' in attributes.keys():
            a, b, c, d = self. attributes['driveid'].split(',')
            self.attributes['driveid'] = "%i%02i%i%02i" % (
                int(a), int(b), int(c), int(d))
