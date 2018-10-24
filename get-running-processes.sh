#!/bin/bash

# customizable vars
dumpsuser="dumpsgen"
hostnums=("5" "6" "7" "9")
dumpsconfigfile="/etc/dumps/confs/wikidump.conf.dumps"
dumpsrepo="/srv/deployment/dumps/dumps/xmldumps-backup"

#subroutines
usage() {
    echo "Usage: $0 [--job jobname] [--wiki wikiname] [--phponly|--pythononly] [--nocruft]"
    echo
    echo "Shows what dumps processes are running on each worker host"
    echo "optionally for the specified wiki and/or job name,"
    echo "optionally only php or python processes,"
    echo "and optionally without any getSlaveServer.php or getConfiguration.php processes"
    echo "(--nocruft)".
    echo
    echo "For a list of jobs, run $0 --listjobs"
    exit 1
}

get_hostname_from_num() {
    num="$1"
    echo "snapshot100${num}.eqiad.wmnet"
}

get_jobs() {
    host="$1"
    jobname="$2"
    wikiname="$3"
    php="$4"
    python="$5"
    nocruft="$6"

    echo $host
    grepstring=" grep $dumpsuser "
    if [ -n "$wikiname" ]; then
	grepstring="$grepstring | grep $wikiname "
    fi
    if [ -n "$jobname" ]; then
	grepstring="$grepstring | grep $jobname "
    fi
    if [ -n "$php" ]; then
	grepstring="$grepstring | grep php "
    fi
    if [ -n "$python" ]; then
	grepstring="$grepstring | grep python "
    fi
    if [ -n "$nocruft" ]; then
	grepstring="$grepstring | grep -v getSlaveServer.php | grep -v getConfiguration.php "
    fi
    sshes $host "ps axuww | $grepstring" | grep -v 'ps axuww' | grep -v 'sh -c'
}

get_randhost() {
    randhost_index=$(( RANDOM % ${#hostnums[@]} ))
    hostnum=${hostnums[$randhost_index]}
    echo $( get_hostname_from_num "$hostnum" )
}

process_opts () {
    while [ $# -gt 0 ]; do
	case "$1" in
	    "--job"|"-j")
		JOBNAME="$2"
		shift; shift;
		;;
	    "--wiki"|"-w")
		WIKI="$2"
		shift; shift
		;;
	    "--listjobs"|"-l")
		listjobs=true
		shift
		;;
	    "--phponly"|"-P")
		PHPONLY=true
		shift
		;;
	    "--pythononly"|"-p")
		PYTHONONLY=true
		shift
		;;
	    "--nocruft"|"-n")
		NOCRUFT=true
		shift
		;;
	    "--help"|"-h"|"-H")
		usage && exit 1
		;;
	    *)
		echo "$0: Unknown option $1"
		usage && exit 1
		;;
	esac
    done
}

#main
process_opts "$@" || exit 1
if [ -n "$listjobs" ]; then
    randhost=$( get_randhost )
    command="cd $dumpsrepo; python ./worker.py --configfile $dumpsconfigfile --job help --dryrun aawiki"
    encoded=$( echo $command | base64 -w0 )
    sshes $randhost "echo $encoded | base64 -d | sudo -u www-data bash"
    exit 0;
fi

for num in ${hostnums[@]}; do
    hostname="snapshot100${num}.eqiad.wmnet"
    get_jobs $hostname "$WIKI" "$JOBNAME" "$PHPONLY" "$PYTHONONLY" "$NOCRUFT"
    echo "------"
done
