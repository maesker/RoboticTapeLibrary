__author__ = 'maesker'
import sys
import os
import logging
import traceback

NONE_STATE = 0


class NoTransition(Exception):
    pass


class BaseStateMachine:
    alias_id = []
    transitions = {}
    _simulation_mode=False
    def __init__(self, id, basedir=os.getcwd(), eventlog=True, simulationmode=False):
        self.id = id
        self.basedir = os.path.join(basedir, self.id[:4],self.id[4:])
        self.state = NONE_STATE
        if simulationmode:
            self._simulation_mode=True
        self.ignored_events = []
        self.eventlog = eventlog
        if eventlog:
            if not os.path.isdir(self.basedir):
                os.makedirs(self.basedir)


    def raiseDump(self, event="NoEvent"):
        raise BaseException("Error\nTmp:\n%s\nEvent:\n%s" %
                            (str(self.tmp), str(event)))

    def new_transition(self, eventname, source, target):
        if source not in self.transitions.keys():
            self.transitions[source] = []
        self.transitions[source].append((eventname, target))

    def default_target(self, eventname, target):
        for source in self.transitions.keys():
            self.new_transition(eventname, source, target)

    def ignore(self, event):
        if event not in self.ignored_events:
            self.ignored_events.append(event)

    def filtered(self, eventinst):
        return False

    def __filtered(self, eventinst):
        if eventinst.name in self.ignored_events:
            return True
        return self.filtered(eventinst)

    def pre_transition(self, eventinst):
        pass

    def post_transition(self, eventinst):
        pass

    def process(self, eventinst):
        # print "Processing", self.id, self.state,eventinst.name
        #self.open()
        if not self.__filtered(eventinst):
            transition = self.get_transition(eventinst.name)
            if transition:
                self.log_event(eventinst, transition)
                self.pre_transition(eventinst)
                if self.state != transition[1]:  # changing states
                    cb = self._get_leavecb_()
                    if cb:
                        cb(eventinst)  # print "leavecb:",cb(eventinst)

                    self.state = transition[1]

                    cb = self._get_entercb_()
                    if cb:
                        cb(eventinst)  # print "entercb:",cb(eventinst)

                else:
                    cb = self._get_instatecb_()
                    if cb:
                        cb(eventinst)  # print "instatecb:",cb(eventinst)
                self.post_transition(eventinst)
            else:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                logging.error("traceback:%s" % repr(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)))
                raise NoTransition("%s:%s: Missing transition event %s , state:%s; %s " % (
                    self, eventinst.attributes['datetime'], eventinst.name, self.state, eventinst.attributes))
            # self.datashelve.sync()
        #self.close()

    def _get_entercb_(self):
        key = "entercb_%s" % self.state
        return getattr(self, key, None)

    def _get_instatecb_(self):
        key = "instatecb_%s" % self.state
        return getattr(self, key, None)

    def _get_leavecb_(self):
        key = "leavecb_%s" % self.state
        return getattr(self, key, None)

    def get_transition(self, eventname):
        for i in self.transitions.get(self.state, []):
            if i[0] == eventname:
                return i

    def log_event(self, event, transition=None):
        if self.eventlog:
            t = None
            if transition:
                t = transition[1]
            with open(os.path.join(self.basedir, 'events_%s.log' % self.id), "a") as myfile:
                myfile.write("State:%s->%s;\t%s\n" % (self.state, t, event))

    def generate_graph(self):
        try:
            import pygraphviz as pgv
            G = pgv.AGraph(strict=False, directed=True)
            for sourcestate, trans in self.transitions.items():
                if sourcestate not in G:
                    G.add_node(sourcestate)
                edges = {}
                for (event, targetstate) in trans:
                    if targetstate not in edges:
                        edges[targetstate] = event
                    else:
                        edges[targetstate] = """%s\\n%s""" % (
                            edges[targetstate], event)
                for tg, label in edges.items():
                    if tg in G:
                        G.add_node(tg)
                    G.add_edge(sourcestate, tg, label=label)

            G.layout(prog='dot')
            G.draw('%s.svg' % self.__class__.__name__, prog='dot')
            # procs: neato, dot, twopi, circo, fdp, nop, wc, acyclic, gvpr, gvcolor, ccomps, sccmap, tred, sfdp
            # nicht ok: neato, twopi, circo, fdp, nop,wc,gvpr, sftp, tred
            # ok: dot
        except:
            print "Graphvis not found"

    def __repr__(self):
        return "%s/%s" % (self.__class__.__name__, self.id)

#--------------
