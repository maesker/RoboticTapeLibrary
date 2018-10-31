__author__ = 'maesker'

import anydbm
import sys
import argparse


def list(f):
    db = anydbm.open(f, 'c')
    for k in sorted(db.keys()):
        print "key:%40s , val: '%s' " % (k, str(db[k]))

    #db['entering_pvl_mountadd'] = """RQST.*Entering.*pvl_MountAdd.*arg = \\"(?P<cartridgeid>[A-Z0-9]{6})"""

    #del db['pvr_MountComplete']

    db.close()


def set(f):
    db = anydbm.open(f, 'c')
    key = raw_input("Key:")
    db[key] = raw_input("Pattern:")
    db.close()


def delete(f):
    db = anydbm.open(f, 'c')
    key = raw_input("Key:")
    del db[key]
    db.close()


def batchignore():
    db = anydbm.open('db/ignorelines_pattern.anydb', 'c')

    for x in ['pvl_RequestSetAttrs', 'pvl_RequestGetAttrs', 'pvl_MountNew',
              'pvr_CartridgeGetAttrs', 'pvr_CartridgeSetAttrs', 'pvr_MountComplete',
              'pvl_MountCommit', "pvl_QueueGetAttrs", "pvl_QueueSetAttrs", 'pvl_DriveSetAttrs',
              'pvl_VolumeGetAttrs', 'pvl_VolumeSetAttrs', 'pvl_PVLSetAttrs',
              "pvr_ServerSetAttrs", 'pvl_ServerSetAttrs', 'pvl_DismountJobId',
              'pvl_AllocateVol', 'pvl_DeallocateVol', 'pvl_Import', 'pvl_Export',
              'pvl_CheckInCompleted', 'pvl_DriveGetAttrs',
              'pvl_CreateDrive', 'pvl_Terminate', 'pvl_Move',
              'pvl_CancelAllJobs']:
        db[x] = "RQST.*%s" % x
    db.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Inspect tracer anydb files")
    parser.add_argument("--db", help="specify database to inspect")
    parser.add_argument("--list", help="show all entries", action='store_true')
    parser.add_argument("--set", help="set new value", action='store_true')
    parser.add_argument("--delete", help="delete a key", action='store_true')

    args = parser.parse_args()
    if (args.list):
        list(args.db)
    if args.set:
        set(args.db)
    if args.delete:
        delete(args.db)
