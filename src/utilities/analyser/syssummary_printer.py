import json
import argparse
import numpy

def load(filename, cb):
    with open(filename, 'r') as fp:
        for line in fp:
            stats = json.loads(line)
            cb(stats)

def homecells(stats):
    date = stats[0]
    ratios = []
    homecell_list = []
    for item in stats[1:]:
        hcs = int(item['homecells'])
        drv = float(item['drives'])
        if drv > 0:
            homecell_list.append(hcs)
            ratios.append(hcs/drv)
    print "%s: %4.1f +- %4.2f; minhc:%i, maxhc:%i"%(date, numpy.mean(ratios), numpy.std(ratios), min(homecell_list), max(homecell_list))





if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyses the system_summary.lbjson file')
    parser.add_argument("--homecells",
                        help="Print statistics of the home cells",
                        action="store_true")
    parser.add_argument("--file", required=True,
                        help="system_summary.lbjson file")

    args = parser.parse_args()
    if args.homecells:
        load(args.file, homecells)

