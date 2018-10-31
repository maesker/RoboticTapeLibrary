import json
import logging
import os

from utilities.fsm.simulation_event import SimulationEvent


class DeletionFailure(Exception): pass


DEFINE_LB_RELATIVE_LOAD_THRESHOLD = 0.10
DEFINE_PTEL_DEBUG = 0


class TapeLibraryAbstractionLayer:

    def __init__(self, datadir):
        self.datadir = datadir
        self.log = logging.getLogger()
        self.syssumfp = open(os.path.join(self.datadir, "system_summary.lbjson"), 'w')

    def event_factory(self, prefix, priority, name, args):
        return SimulationEvent(prefix, priority, name, args)

    def process_every_ten_minutes(self):
        pass

    def process_hourly(self):
        #if self._prefetching:
        #    self._prefetching.calculate_candidates()
        stats = [self.globalclock.strftime("%Y%m%dT%H%M%S")]
        for k,v in self.level.iteritems():
            stats.append(v.process_hourly())

        self.syssumfp.write("%s\n"%json.dumps(stats))


        if DEFINE_PTEL_DEBUG:
            s = ""
            for i in (self.__pass_thrus, self.__elevators):
                for k,v in i.iteritems():
                    free = v.isfree()
                    if not free:
                        s = "%s;%s"%(s,k)
            self.log.info("ELPT not free:%s"%s)
        #self._LB_process()

    def _LB_process(self):
        """
        relative_load_percentiles: ( heat percentile value * #drives / #crts ) of the respective level
            # the smaller the value, the hotter the level

        """
        try:
            self._LB_relative_heat =[]
            for k,v in self.level.iteritems():
                self._LB_level_reports[k] = v.get_loadbalancing_attributes()
                self._LB_relative_heat.append([])
            p = [[],[],[],[],[]]
            p_mean = []
            for index, atts in self._LB_level_reports.iteritems():
                relload = atts.get('relative_load_percentiles')
                if relload:
                    for i in range(len(relload)):
                        p[i].append(relload[i])
            for i in p:
                p_mean.append(float(sum(i))/len(i))

            with open(self._LB_hourly_tracker_file, 'a') as fp:
                fp.write("%s;%2.2f;%2.2f\n"%(self.globalclock,numpy.mean(p[0]), numpy.std(p[0])))

            for levelid, atts in self._LB_level_reports.iteritems():
                rel = [round((a-b)/a, 2) for a,b in zip(p_mean,atts.get('relative_load_percentiles'))]
                if len(rel)>0:
                    self._LB_relative_heat[levelid].append(rel[0])
                #    self._LB_p0_hourly_tracker[levelid+1].append(rel[0])
                else:
                    self._LB_relative_heat[levelid].append(0)
                #    self._LB_p0_hourly_tracker[levelid+1].append(0)
                self.log.info("HEAT_diff:%s:%s"%(levelid,rel))
            #self._LB_p0_hourly_tracker[0].append(self.globalclock)
            #self._LB_check_rebalancing()
        except:
            raise
            #pass
       # for levelid, atts in self._LB_level_reports.iteritems():
       #     self.log.debug("HEAT_abs:%s:%s"%(levelid,atts.get('relative_load_percentiles')))

    def _LB_check_rebalancing(self):
        def get_neighbours(self, source):
            n = self._get_passthru_neighbours(source)
            n.extend(self._get_elevator_neighbours(source))

            #'relative_load_percentiles': self._LB_relative_load_percentiles,
            #'heat_percentiles': self._LB_heat_percentiles,
            #'numdrives':self._LB_numdrives}
            #'numcrts': len(self._cartridge_cache)}
        for levelid,report in self._LB_level_reports.iteritems():
            if report['libleft'] > 0.97:
                self.level[levelid].receive_loadbalancing_targetlevel_candidates(get_neighbours(levelid))
            elif report['relative_load_percentiles'][0] > DEFINE_LB_RELATIVE_LOAD_THRESHOLD:
                candidates = []
                for neighbourid, neighbour_report in get_neighbours(levelid):
                    if neighbour_report['relative_load_percentiles'][0] < 0.0:
                        candidates.append(neighbourid)
                self.level[levelid].receive_loadbalancing_targetlevel_candidates(candidates)

    def superstep(self, clock):
        self.globalclock=clock
        for obj in self.level.itervalues():
            obj.superstep(clock)

    def finalize(self):
        self.syssumfp.close()
        for levelobj in self.level.values():
            levelobj.finalize()