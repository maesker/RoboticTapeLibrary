__author__ = 'maesker'
import json
import argparse


FN = "sl8500.json"

LEVEL_MAX = 4

ROBOT_PER_LEVEL_MAX = 2

# drive format:
# <lsmid>_<driveid>:
DRIVE_LEVEL_MAX = 16


#INITIAL_LIBRARIES = 1448 in total, 362 per lsm
INITIAL_LIBRARIES_OUTER = 75
INITIAL_LIBRARIES_INNER = 75
INITIAL_LIBRARIES_FRONT = 62

# library format:
# <lsmid>_<wallid>_<capacitymoduleindex>_<libraryid>
# wallid: front:0, outerleft:1, innerleft:2, innerright:3, outerright:4
# the initial config is
# outer size 400 libs each
# inner size 325 libs each

## capacity module
# adds 1728 libraries, size 37.5 inch, 432 libs each, 108 per lsm and wall
CAPACITY_LIBRARIES_PER_WALL = 108
MAX_CAPACITY_LIBRARIES = 5
CAPACITY_INDEX_OFFSET = 0

def gen_crt_to_lsm_mapping(snapfile):
    with open(snapfile, 'r') as fp:
        config = {}
        snapshot = json.load(fp)
        for lsmid,data in snapshot.items():
            lvid = int(lsmid)

            for crtid in data['crts']:
                config[crtid.replace('/', "")] = lvid

    with open('crtmapping.json', 'w') as fp:
        json.dump(config, fp )

def gen_libraries(obj, lsm, CAPACITY_MODULES):
        capacity_index=1
        for i in range(INITIAL_LIBRARIES_FRONT):
            obj[lsm]['libraries']["L_%02i_%i_%i_%03i"%(
                lsm, 2, capacity_index+CAPACITY_INDEX_OFFSET, i)] = {}
        for wall in [0,4]:
            for i in range(INITIAL_LIBRARIES_OUTER):
                obj[lsm]['libraries']["L_%02i_%i_%i_%03i"%(
                    lsm, wall, capacity_index+CAPACITY_INDEX_OFFSET, i)] = {}
        for wall in [1,3]:
            for i in range(INITIAL_LIBRARIES_INNER):
                obj[lsm]['libraries']["L_%02i_%i_%i_%03i"%(
                    lsm, wall, capacity_index+CAPACITY_INDEX_OFFSET, i)] = {}
        for capacity_index in range(2,CAPACITY_MODULES+2):
            for wall in [0,1,3,4]:
                for i in range(CAPACITY_LIBRARIES_PER_WALL):
                    obj[lsm]['libraries']["L_%02i_%i_%i_%03i"%(
                        lsm, wall, capacity_index+CAPACITY_INDEX_OFFSET, i)] = {}
        return obj

def generate_from_snapshot(snapfile, CAPACITY_MODULES,ROBOTS_PER_LEVEL):
    with open(snapfile, 'r') as fp:
        config = {'lsm':{}}
        lv = config['lsm']
        snapshot = json.load(fp)
        for lsmid,data in snapshot.items():
            lvid = int(lsmid)
            lv[lvid] = {"drives":{}, "libraries":{}, "robots":ROBOTS_PER_LEVEL}
            for i in range(len(data['drives'])):
                lv[lvid]['drives']["D_%02i_%02i"%(lvid,i)] = {}
            lv = gen_libraries(lv, lvid, CAPACITY_MODULES)

    with open(FN, 'w') as fp:
        json.dump(config, fp )

def generate(LEVEL,DRIVES_PER_LEVEL,CAPACITY_MODULES,ROBOTS_PER_LEVEL):
    j = {
        "lsm":{}
    }

    for lsm in range(0,LEVEL):
        j['lsm'][lsm] = {"drives":{}, "libraries":{}, "robots":ROBOTS_PER_LEVEL}
        for i in range(DRIVES_PER_LEVEL):
            j['lsm'][lsm]['drives']["D_%02i_%02i"%(lsm,i)] = {}
        j['lsm'] = gen_libraries(j['lsm'], lsm, CAPACITY_MODULES)

    with open(FN, 'w') as fp:
        json.dump(j, fp )

if __name__ == '__main__':


    parser = argparse.ArgumentParser(description='SL8500 generator')
    parser.add_argument("--lsm", help="number of lsms, default 16", default=16)
    parser.add_argument("--extension", help="capacity extentions, default 5", default=5)
    parser.add_argument("--robots", help="number of robots per lsm (experimental), default 1", default=1)
    parser.add_argument("--drives", help="number of drives per lsm, max 16, default 8", default=8)
    parser.add_argument('--fromsnapshot', help="generate config fram snapshot", default=False)

    args = parser.parse_args()
    if args.fromsnapshot:
        generate_from_snapshot(args.fromsnapshot,int(args.extension),int(args.robots))
        gen_crt_to_lsm_mapping(args.fromsnapshot)

    else:
        generate(
            int(args.lsm),
            int(args.drives),
            int(args.extension),
            int(args.robots),
        )
