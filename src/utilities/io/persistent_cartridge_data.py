__author__ = 'maesker'

import os
import re
import datetime
import sqlite3
from threading import Lock


from abstract_persistence import AbstractPersIO


class CartridgePersistence(AbstractPersIO):

    def initdbs(self):
        self.open()
        db__cursor = self.conn.cursor()
        db__cursor.execute("SELECT * FROM sqlite_master WHERE type='table' AND name='loads';")
        res = db__cursor.fetchall()
        if len(res) == 0:
            self.conn.execute("CREATE TABLE loads ( loadid int PRIMARY KEY, load_request char(15), load_complete char(15), unload_request char(15), unload_complete char(15), driveid char(7), loaded_from_libraryid varchar(16), unloaded_to_libraryid varchar(16) );")
            self.conn.execute("CREATE TABLE mounts ( loadid int, mount char(15), unmount char(15) );")
            self.conn.commit()
        self.conn.close()


    def write(self):
        try:
            self.open()

            db__cursor = self.conn.cursor()

            db__cursor.execute("""SELECT * from loads ORDER BY loadid DESC LIMIT 1;""")
            load = db__cursor.fetchall()
            loadid = 1
            if len(load)>0:
                loadid = int(load[0][0])

            rows_loads = []
            rows_mounts = []
            for value in self.cache:
                loadid += 1
                rows_loads.append((loadid, value['load_request'], value['load_complete'],value['unload_request'], value['unload_complete'], value['driveid'], value['loaded_from_libraryid'],value['unloaded_to_libraryid']))

                for s,e in value['mount_operations']:
                    rows_mounts.append((loadid, s, e))

            db__cursor.executemany(
                """INSERT INTO loads VALUES (?, ?, ?, ?, ?, ?, ?, ? )""", rows_loads)
            db__cursor.executemany("""INSERT INTO mounts VALUES (?, ?, ?)""", rows_mounts)

            self.conn.commit()
            self.cache = []
        except:
            if self.conn:
                self.conn.rollback()
            raise

        finally:
            if self.conn:
                self.conn.close()


    def get_operations(self):
        loads = []
        db__conn = sqlite3.connect(self.dbfile)
        db__cursor = db__conn.cursor()
        db__cursor.execute("""SELECT * FROM loads;""")
        tmploads = db__cursor.fetchall()
        for load in tmploads:
            lr = datetime.datetime.strptime(load[1],"%Y-%m-%d %H:%M:%S") if isinstance(load[1],unicode) else None
            lc = datetime.datetime.strptime(load[2],"%Y-%m-%d %H:%M:%S") if isinstance(load[2],unicode) else None
            ur = datetime.datetime.strptime(load[3],"%Y-%m-%d %H:%M:%S") if isinstance(load[3],unicode) else None
            uc = datetime.datetime.strptime(load[4],"%Y-%m-%d %H:%M:%S") if isinstance(load[4],unicode) else None
            entry= {
                'loadid': load[0],
                'load_request':lr,
                'load_complete': lc,
                'unload_request': ur,
                'unload_complete': uc,
                'driveid': load[5],
                'loaded_from_libraryid':load[6],
                'unloaded_to_libraryid':load[7],
                'mount_operations': []
            }
            db__cursor.execute("""SELECT * FROM mounts WHERE loadid=?;""",(load[0],))
            for (id,s,e) in db__cursor.fetchall():
                sx = datetime.datetime.strptime(s,"%Y-%m-%d %H:%M:%S") if isinstance(s,unicode) else None
                ex = datetime.datetime.strptime(e,"%Y-%m-%d %H:%M:%S") if isinstance(e,unicode) else None

                entry['mount_operations'].append((sx,ex))
            loads.append(entry)
        db__conn.close()
        return loads

    def get_entry_before_timestamp(self, year, month, day=1):
        dtcheck = datetime.datetime(year=year, month=month, day=day)
        db__conn = sqlite3.connect(self.dbfile)
        db__cursor = db__conn.cursor()

        db__cursor.execute("""SELECT * FROM loads WHERE load_request<='%s' ;"""%(dtcheck))
        tmploads = db__cursor.fetchall()
        db__conn.close()
        return tmploads

    def get_last_entry(self):
        db__conn = sqlite3.connect(self.dbfile)
        db__cursor = db__conn.cursor()
        db__cursor.execute("""SELECT * from loads ORDER BY loadid DESC LIMIT 1;""")
        load = db__cursor.fetchall()
        if len(load)==0:
            return {}
        lr = datetime.datetime.strptime(load[0][1],"%Y-%m-%d %H:%M:%S") if isinstance(load[0][1],unicode) else None
        lc = datetime.datetime.strptime(load[0][2],"%Y-%m-%d %H:%M:%S") if isinstance(load[0][2],unicode) else None
        ur = datetime.datetime.strptime(load[0][3],"%Y-%m-%d %H:%M:%S") if isinstance(load[0][3],unicode) else None
        uc = datetime.datetime.strptime(load[0][4],"%Y-%m-%d %H:%M:%S") if isinstance(load[0][4],unicode) else None
        entry= {
            'loadid': load[0][0],
            'load_request':lr,
            'load_complete': lc,
            'unload_request': ur,
            'unload_complete': uc,
            'driveid': load[0][5],
            'loaded_from_libraryid':load[0][6],
            'unloaded_to_libraryid':load[0][7],
            'mount_operations': []
        }
        db__cursor.execute("""SELECT * FROM mounts WHERE loadid=?;""",(load[0][0],))
        for (id,s,e) in db__cursor.fetchall():
            sx = datetime.datetime.strptime(s,"%Y-%m-%d %H:%M:%S") if isinstance(s,unicode) else None
            ex = datetime.datetime.strptime(e,"%Y-%m-%d %H:%M:%S") if isinstance(e,unicode) else None

            entry['mount_operations'].append((sx,ex))
        db__conn.close()
        return entry