__author__ = 'maesker'


import os
import sys
import gzip
import re
import glob
import datetime
import shutil

# robot logs jump to daylight savings time on 00:59:40 25. march 2012
# whpss are in sync with robot logs no earlier than 09:09:13 16.05.2012

VERSION_TAG = "2"


def case1(basedir):
    year = 2012
    startdate = datetime.datetime(2012, 3, 25, 1, 0, 0)
    # 08:17:07 start skip hour here
    #
    enddate = datetime.datetime(2012, 5, 16, 8, 17, 8)
    flush_entries_until = datetime.datetime(2012, 5, 16, 9, 8, 40)
    #enddate = datetime.datetime(2012,5, 16, 9,0,0)
    olddir = os.path.join(basedir, 'old')
    os.makedirs(olddir)

    p = re.compile(
        '(?P<mm>[0-9]{2})/(?P<dd>[0-9]{2}) (?P<HH>[0-9]{2}):(?P<MM>[0-9]{2}):(?P<SS>[0-9]{2}) (?P<content>.+)')
    for f in sorted(glob.glob(os.path.join(basedir, "whpss_log_2012*"))):
        output = []
        # print f
        cb = gzip.open
        if not f.endswith('.gz'):
            cb = open
        with cb(os.path.join(basedir, f), 'r') as fp:
            for line in fp:
                added = False
                m = p.match(line)
                if m:
                    mm = int(m.group('mm'))
                    dd = int(m.group('dd'))
                    HH = int(m.group('HH'))
                    MM = int(m.group('MM'))
                    SS = int(m.group('SS'))
                    currentdate = datetime.datetime(year, mm, dd, HH, MM, SS)

                    if startdate < currentdate:
                        if currentdate < enddate:
                            # print "Handle line %s"%str(m)
                            currentdate = currentdate + \
                                datetime.timedelta(hours=1)
                            output.append("%02i/%02i %02i:%02i:%02i %s\n" % (currentdate.month, currentdate.day,
                                                                             currentdate.hour, currentdate.minute,
                                                                             currentdate.second, m.group('content')))
                            added = True
                        elif currentdate < flush_entries_until:
                            added = True

                if not added:
                    output.append(line)

        with gzip.open("%s.v%s.gz" % (f, VERSION_TAG), 'wb') as newfp:
            newfp.writelines(output)
        shutil.move(f, olddir)


case1(os.path.join(os.getcwd(), sys.argv[1]))
