import os
import shutil
from datetime import datetime
import logging
from logging.config import fileConfig
import time
from ConfigParser import SafeConfigParser

__author__ = 'maesker'


class SimClass:

    def __init__(self, config, custom_dir_pattern=None):
        ##
        # @brief Implement the basic simulator funtionality
        # @todo make local class variables private (name mangling)?
        configdir = os.path.dirname(config)
        basename = os.path.basename(config)
        cfg_parser = SafeConfigParser()
        cfg_parser.read(config)

        self.tracedir = cfg_parser.get('DEFAULT', 'tracedir')
        self.tmpdir = cfg_parser.get("DEFAULT", 'tmpdir')
        if custom_dir_pattern:
            custom=""
            for sec,opt in custom_dir_pattern:
                custom += "."+cfg_parser.get(sec,opt)
            self.tmpdir = os.path.join(self.tmpdir, custom[1:])

        self.datadir = os.path.join(self.tmpdir, 'data')
        if not os.path.isdir(self.datadir):
            os.makedirs(self.datadir)

        self.cfgdir = os.path.join(self.tmpdir, "conf")
        if not os.path.isdir(self.cfgdir):
            os.makedirs(self.cfgdir)
        for i in [basename, "logconfig.ini"]:
            shutil.copy(os.path.join(configdir, i), self.cfgdir)

        logfile_config = os.path.join(self.cfgdir, 'logconfig.ini')
        cfg_parser_log = SafeConfigParser()
        cfg_parser_log.read(logfile_config)

        cfg_parser_log.set("handler_fileHandler",
                           'args',
                           '("%s/tapelibsim.log",)' % self.tmpdir)
        with open(logfile_config, 'w') as fp:
            cfg_parser_log.write(fp)
        fileConfig(logfile_config)
        self.logger = logging.getLogger()
        self.cfg_parser = SafeConfigParser()
        self.cfg_parser.read(os.path.join(self.cfgdir, basename))

        self.eventlog = True
        self.logger.info("Initialization done.")
        self._keep_simulating_for_x_hours = 24
        self.stepsize_sec = 1

    def process_hourly(self):
        pass

    def process_every_ten_minutes(self):
        pass

    def finalize(self):
        # @brief issue unload to all loaded cartridges at the end of the sim
        # this way the results are (permanently) written to shelve
        raise Exception("Implement me")

    def shortreport(self):
        print "No report implemented"

    def hourlyreport(self):
        print "No hourly report implemented"
