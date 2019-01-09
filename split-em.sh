#!/bin/bash

# given an xml file, split out the header, body, footer
# into three separate files

if [ -z "$1" -o -z "$2" ]; then
    echo "Usage: $0 inputfile outputfile"
    exit 1
fi

infile="$1"
outfile="$2"

endheader=$( zcat $infile | grep -n '</siteinfo>'| mawk -F':' '{print $1}' )
startfooter=$( zcat $infile | grep -n '</mediawiki>'| mawk -F':' '{print $1}' )
startbody=$(( $endheader + 1 ))
bodylines=$(( $startfooter - $startbody ))
zcat $infile | head -n $endheader | gzip >  "header-${outfile}"
zcat $infile | tail -n +"$startfooter" | gzip > "footer-${outfile}"
zcat $infile | tail -n +"$startbody" | head -n "$bodylines" | gzip > "$outfile"
