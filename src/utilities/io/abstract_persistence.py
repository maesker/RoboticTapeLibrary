
import os
import re
import datetime
import sqlite3
from threading import Lock


class NoEntryException(Exception): pass

class AbstractPersIO:
    def __init__(self, iodir, id, simulationmode=False):
        self.cache = []
        ## @todo make this configurable?
        self.CACHELIMIT = 1000
        if not os.path.isdir(iodir):
            os.makedirs(iodir)
        self.dbfile = os.path.join(iodir,"%s.sqlite"%id)
        self.initdbs()

    def open(self):
        self.conn = sqlite3.connect(self.dbfile)
        self.conn.execute("PRAGMA SYNCHRONOUS = OFF")
        self.conn.execute("PRAGMA JOURNAL_MODE = OFF")
        self.conn.execute("PRAGMA COUNT_CHANGES=OFF")
        self.conn.execute("PRAGMA TEMP_STORE = MEMORY ")
        self.conn.execute("PRAGMA MMAP_SIZE = %i" % (1024 * 1024 * 16))
        self.conn.text_factory = sqlite3.OptimizedUnicode
        self.conn.commit()

    def put(self, value_dict):
        self.cache.append(value_dict)
        if len(self.cache)>=self.CACHELIMIT:
            self.write()

    def flush(self):
        self.write()

    def __del__(self):
        self.close()

    def close(self):
        self.write()

    def write(self):
        raise Exception("Implement me")