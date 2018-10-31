import datetime
import sqlite3
from abstract_persistence import AbstractPersIO, NoEntryException




class PersIO_Drive(AbstractPersIO):

    def initdbs(self):
        db__conn = sqlite3.connect(self.dbfile)
        db__cursor = db__conn.cursor()
        db__cursor.execute("SELECT * FROM sqlite_master WHERE type='table' AND name='loads';")
        res = db__cursor.fetchall()
        if len(res) == 0:
            db__conn.execute("CREATE TABLE loads ( loadid int PRIMARY KEY , load_complete char(15), unload_complete char(15), crtid char(7), loaded_from_libraryid varchar(16), unloaded_to_libraryid varchar(16) );")
            db__conn.execute("CREATE TABLE mounts ( loadid int, mount char(15), unmount char(15) );")
            db__conn.commit()
        db__conn.close()

    def write(self):
        try:
            data = self.cache
            self.cache = []
            self.open()
            db__cursor = self.conn.cursor()
            db__cursor.execute("""SELECT * from loads ORDER BY loadid DESC LIMIT 1;""")
            load = db__cursor.fetchall()
            loadid = 1
            if len(load)>0:
                loadid = int(load[0][0])

            rows_loads, rows_mounts = [], []
            for value in data:
                loadid += 1
                rows_loads.append((loadid, value['load_complete'], value['unload_complete'], value['crtid'], value['loaded_from_libraryid'],value['unloaded_to_libraryid']))

                for s,e in value['mount_operations']:
                    rows_mounts.append((loadid, s, e))

            db__cursor.executemany("INSERT INTO loads VALUES (?, ?, ?, ?, ?, ? )",rows_loads)
            db__cursor.executemany("INSERT INTO mounts VALUES (?, ?, ?)",rows_mounts)

        except:
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            if self.conn:
                self.conn.commit()
                self.conn.close()

    def get_operations(self):
        loads = []
        db__conn = sqlite3.connect(self.dbfile)
        db__cursor = db__conn.cursor()
        db__cursor.execute("""SELECT * FROM loads;""")

        tmploads = db__cursor.fetchall()
        for load in tmploads:
            lc = datetime.datetime.strptime(load[1],"%Y-%m-%d %H:%M:%S") if isinstance(load[1],unicode) else None
            uc = datetime.datetime.strptime(load[2],"%Y-%m-%d %H:%M:%S") if isinstance(load[2],unicode) else None
            entry= {
                'loadid': load[0],
                'load_complete': lc,
                'unload_complete': uc,
                'crtid': load[3],
                'loaded_from_libraryid':load[4],
                'unloaded_to_libraryid':load[5],
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

    def get_first_entry(self):
        db__conn = sqlite3.connect(self.dbfile)
        db__cursor = db__conn.cursor()
        db__cursor.execute("""SELECT * FROM loads;""")
        load = db__cursor.fetchone()
        if load is None:
            raise NoEntryException("db empty")
        lc = datetime.datetime.strptime(load[1],"%Y-%m-%d %H:%M:%S") if isinstance(load[1],unicode) else None
        uc = datetime.datetime.strptime(load[2],"%Y-%m-%d %H:%M:%S") if isinstance(load[2],unicode) else None
        entry= {
            'loadid': load[0],
            'load_complete': lc,
            'unload_complete': uc,
            'crtid': load[3],
            'loaded_from_libraryid':load[4],
            'unloaded_to_libraryid':load[5],
            'mount_operations': []
        }
        db__cursor.execute("""SELECT * FROM mounts WHERE loadid=?;""",(load[0],))
        for (id,s,e) in db__cursor.fetchall():
            sx = datetime.datetime.strptime(s,"%Y-%m-%d %H:%M:%S") if isinstance(s,unicode) else None
            ex = datetime.datetime.strptime(e,"%Y-%m-%d %H:%M:%S") if isinstance(e,unicode) else None

            entry['mount_operations'].append((sx,ex))
        db__conn.close()
        return entry




class PersIO_Disabled(AbstractPersIO):

    def initdbs(self):
        db__conn = sqlite3.connect(self.dbfile)
        db__cursor = db__conn.cursor()
        db__cursor.execute("SELECT * FROM sqlite_master WHERE type='table' AND name='disabled';")
        res = db__cursor.fetchall()
        if len(res) == 0:
            db__conn.execute("CREATE TABLE disabled( disabled char(15), enabled char(15) );")
            db__conn.commit()
        db__conn.close()

    def write(self):
        try:
            data = self.cache
            self.open()
            db__cursor = self.conn.cursor()
            rows = []
            for value in data:
                rows.append((value['disabled'], value['enabled']))
            db__cursor.executemany("INSERT INTO disabled VALUES (?, ? )",rows)

        except:
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            if self.conn:
                self.conn.commit()
                self.conn.close()


    def get_operations(self):
        loads = []
        db__conn = sqlite3.connect(self.dbfile)
        db__cursor = db__conn.cursor()
        db__cursor.execute("""SELECT * FROM disabled;""")
        for d,e in db__cursor.fetchall():
            loads.append((datetime.datetime.strptime(d,"%Y-%m-%d %H:%M:%S"),datetime.datetime.strptime(e,"%Y-%m-%d %H:%M:%S")))
        db__conn.close()
        return loads

