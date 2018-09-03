import os
import sys
import time
from datetime import datetime


def get_date_difference(start, end):
    '''
    given two date strings in a rather horrid format, get the
    difference between them in some nice form for humans. ha!

    also return a bool indicting whether this duration is
    long enough to be noteworthy or not (limits entirely arbitrary).
    '''
    start_datetime = datetime.strptime(start, "%b %d %Y %H:%M:%S %Z")
    end_datetime = datetime.strptime(end, "%b %d %Y %H:%M:%S %Z")
    interval = (end_datetime - start_datetime).total_seconds()
    hours = int(interval / 3600)
    mins = int(interval - (hours * 3600)) / 60
    secs = int(interval - (hours * 3600) - (mins * 60)) / 60
    long = False
    if hours > 0 or mins > 20:
        long = True
    return "{start} to {end}: {hours}h {mins}m {secs}s ({total})".format(
        start=start, end=end,
        hours=hours, mins=mins, secs=secs, total=interval), long


def get_fields(line):
    '''
    extract the fields we wnt from the line and return a dict of them:
    host, date (as a string), rsync target dir
    '''
    fields = {}
    # Sep  3 06:59:40 labstore1007 rsyncd[21168]: rsync to
    #   data/xmldatadumps/public/ from dumpsdata1001.eqiad.wmnet (10.64.16.93)
    elts = line.split()
    fields['date'] = "{month} {day} 2018 {time} GMT".format(
        month=elts[0], day=elts[1], time=elts[2])
    fields['rsync_dir'] = elts[7]
    fields['host'] = elts[3]
    return fields


def show_duration(duration, big, rsync_dir, host):
    '''
    format the provided values into a nice output text
    and display it
    '''
    if big:
        duration = duration + " LONG"
    print "{duration} {host}:{rsync_dir}".format(
        duration=duration, rsync_dir=rsync_dir, host=host)
    return


def show_time(line, prevline, hosts):
    '''
    given two rsync log entries in succession,
    show how long a given rsync took, if the
    two lines cover an rsync we care about
    '''
    if prevline is None:
        return
    if line == prevline:
        # dup for some unknown reason
        return
    fields = get_fields(line)
    if fields['host'] not in hosts:
        # some entry that doesn't concern us
        return

    prev_fields = get_fields(prevline)

    duration = None
    if fields['rsync_dir'] == 'data/xmldatadumps/public/':
        if (prev_fields['rsync_dir'] == 'data/xmldatadumps/public/' and
                fields['host'] == prev_fields['host']):
            duration, big = get_date_difference(prev_fields['date'], fields['date'])
    elif fields['rsync_dir'] == 'data/xmldatadumps/public/other/':
        duration, big = get_date_difference(prev_fields['date'], fields['date'])
    if duration is not None:
        show_duration(duration, big, fields['rsync_dir'], fields['host'])


def show_rsync_times(logfile, hosts):
    '''
    read the log file and figure out how long the rsyncs
    of public/ and other/ took for each run for each host,
    and display them
    '''
    lines = open(logfile).read().splitlines()
    prevline = None
    for line in lines:
        show_time(line, prevline, hosts)
        prevline = line


def do_main():
    '''
    for the three hosts in the current rsync log,
    show how long each rsync took by looking at
    times between entries
    '''
    hosts = ['dumpsdata1002', 'labstore1006', 'labstore1007']
    date = time.strftime("%Y%m%d", time.gmtime())
    logfile = "rsync_logs_sorted_" + date + ".txt"
    if not os.path.exists(logfile):
        print "Can't find the log file {filename}, have you run the fetch script?".format(
            filename=logfile)
        sys.exit(1)
    show_rsync_times(logfile, hosts)


if __name__ == '__main__':
    do_main()
