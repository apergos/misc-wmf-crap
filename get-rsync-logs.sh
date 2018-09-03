#!/bin/bash

date=$( date -u +"%Y%m%d" )
for host in dumpsdata1002.eqiad.wmnet labstore1006.wikimedia.org labstore1007.wikimedia.org; do
    sshes $host 'sudo grep rsyncd /var/log/syslog | grep "rsync to" | grep dumpsdata1001' >> "rsync_logs_${date}.txt"
    sshes $host 'sudo grep rsyncd /var/log/syslog.1 | grep "rsync to" | grep dumpsdata1001' >> "rsync_logs_${date}.txt"
    sshes $host 'sudo zcat /var/log/syslog.*gz | grep rsyncd | grep "rsync to" | grep dumpsdata1001' >> "rsync_logs_${date}.txt"
done
sort "rsync_logs_${date}.txt" > "rsync_logs_sorted_${date}.txt"
