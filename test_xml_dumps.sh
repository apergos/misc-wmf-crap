#!/bin/bash

# This script is intended for testing changes to MediaWiki that may affect dump queries,
# on a local installation with imported data. Sure, the page numbers ought to be
# configurable but for now this works as is, and it's much faster than running all
# this crap by hand.

# Generally if we are exercising all code paths after an MCR or other deep change, we want to run
# * stubs, full revs
# * abstracts, current revs only
# * page content with dumpBackup, current revs only
# * page content with dumpBackup, full revs
# * special:exports via api

do_mw_copy() {
    IFS=',' read -a FILES <<< "$1"
    suffix=$2
    for filename in ${FILES[@]}; do
	$myecho cp "${mwbasedir}/${WIKI}/${filename}.${suffix}" "${mwbasedir}/${WIKI}/$filename"
    done
}

mv_mw_log() {
    $myecho service httpd stop
    sleep 1
    $myecho service php-fpm stop
    sleep 1
    $myecho mv /var/log/mediawiki/debug-stuff.log "${outdir}/debug-stuff.log.${job}.${runtype}"
    $myecho sudo -u $USER touch /var/log/mediawiki/debug-stuff.log
    $myecho service httpd start
}

mv_mysql_log() {
  $myecho service mariadb stop
  sleep 1
  $myecho mv /var/lib/mysql/queries.log "${outdir}/queries.log.${job}.${runtype}"
  $myecho gzip -f "${outdir}/queries.log.${job}.${runtype}"
  $myecho service mariadb start
  sleep 1
}

do_run() {
    outdir="$1"
    runtype="$2"

    $myecho sudo -u $USER mkdir -p $outdir
    chown -R $USER "$outputdir"

    job="prod"
    # move any existing logs out of the way etc
    mv_mw_log
    mv_mysql_log

    job="abstracts_none"
    echo "doing $job"
    # Abstracts (small number of pages, at least some are in main namespace but some are not, none of which have abstracts)
    $myecho sudo -u $USER php ${mwbasedir}/${WIKI}/maintenance/dumpBackup.php --wiki=${WIKIDB} ${mwbasedir}/${WIKI}          \
	 --plugin=AbstractFilter:${mwbasedir}/${WIKI}/extensions/ActiveAbstract/includes/AbstractFilter.php                  \
	 --current --report=1                                                                                                \
	 --output=file:"${outdir}/${WIKI}-${job}.${runtype}.xml.gz"                                                          \
	 --filter=namespace:NS_MAIN --filter=noredirect --filter=abstract                                                    \
	 --start=4327 --end=4341
    mv_mw_log
    mv_mysql_log

    job="abstracts_real"
    echo "doing $job"
    # Abstracts (one page, wikitext, different namespace, should produce one abstract)
    $myecho sudo -u $USER php ${mwbasedir}/${WIKI}/maintenance/dumpBackup.php --wiki=${WIKIDB} ${mwbasedir}/${WIKI}        \
	 --plugin=AbstractFilter:${mwbasedir}/${WIKI}/extensions/ActiveAbstract/includes/AbstractFilter.php                \
	 --current --report=1                                                                                              \
	 --output=file:"${outdir}/${WIKI}-${job}.${runtype}.xml.gz"                                                        \
	 --filter=noredirect --filter=abstract                                                                             \
	 --start=4438 --end=4439
    mv_mw_log
    mv_mysql_log

    job="stubs"
    echo "doing $job"
    # Stubs (small number of pages)
    $myecho sudo -u $USER php ${mwbasedir}/${WIKI}/maintenance/dumpBackup.php --wiki=${WIKIDB} --full --stub --report=1   \
	 --output=file:"${outdir}/${WIKI}-${job}.${runtype}.xml.gz"                                                       \
	 --start=4327 --end 4341
    mv_mw_log
    mv_mysql_log

    job="stubs_tiny"
    echo "doing $job"
    # Stubs (one page because too many revs)
    $myecho sudo -u $USER php ${mwbasedir}/${WIKI}/maintenance/dumpBackup.php --wiki=${WIKIDB} --full --stub --report=1   \
	 --output=file:"${outdir}/${WIKI}-${job}.${runtype}.xml.gz"                                                       \
	 --start=4327 --end 4328
    mv_mw_log
    mv_mysql_log

    job="content_current"
    echo "doing $job"
    # Content (small number of pages, current revs only)
    $myecho sudo -u $USER php ${mwbasedir}/${WIKI}/maintenance/dumpBackup.php --wiki=${WIKIDB} --current --report=1    \
	 --output=file:"${outdir}/${WIKI}-${job}.${runtype}.xml.gz"                                                    \
	 --start=4327 --end 4341
    mv_mw_log
    mv_mysql_log

    job="content_full"
    echo "doing $job"
    # Content (small number of pages, all revs)
    $myecho sudo -u $USER php ${mwbasedir}/${WIKI}/maintenance/dumpBackup.php --wiki=${WIKIDB} --full --report=1       \
	 --output=file:"${outdir}/${WIKI}-${job}.${runtype}.xml.gz"                                                    \
	 --start=4327 --end 4341
    mv_mw_log
    mv_mysql_log

    job="special_export"
    echo "doing $job"
    # Special:Exports via api (small number of pages, current rev only)
    $myecho sudo -u $USER curl -o "${outdir}/${WIKI}-${job}.${runtype}.json" http://localhost/${WIKI}/api.php'?action=query&export=true&pageids=4327|4328|4329&format=json&exportnowrap=true'
    mv_mw_log
    mv_mysql_log
}

date=$( date -u +"%Y%m%d" )
outputsubdir="output_${date}"
outputbasedir=$( pwd )
outputdir="${outputbasedir}/${outputsubdir}"
mwbasedir="/var/www/html"

# FIXME these should be command line args
# user as which to run various commands (non-root)
USER=ariel
# subdir where mediawiki lives for this wiki; this string will also be used in output file names
WIKI=wikidata
# name of mysql db for this wiki
WIKIDB=wikidatawiki

# FIXME this should be set via command line arg
#myecho="echo"
myecho=""

do_mw_copy includes/export/WikiExporter.php,includes/export/XmlDumpWriter.php orig
do_run "$outputdir" "old"
do_mw_copy includes/export/WikiExporter.php,includes/export/XmlDumpWriter.php new
do_run "$outputdir" "new"

chown -R $USER "$outputdir"
