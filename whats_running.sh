#!/bin/bash

hostnums="5 6 7 9"

# for each host in the list, display:
# load, dump type (enwiki/wikidatawiki/regular? full/partial?), details about each dump job:
#    wiki, dump step, number of processes

# TODOs:

# how many regular dumps remain to be completed, whether en, wd are complete or not
# we might double-count 7za, dd, others if they are part of a pipeline

# namespaces is not covered (but it's so fast that who cares)
# pagetitles, allpagetitles are considered 'tables' but maybe this is ok


get_wikiname_from_args_via_filepath() {
    for subfield in $@ ; do
	if [[ $subfield =~ /\/*wik*\/[0-9]{8}\// ]]; then
	    gotten_wikiname="${BASH_REMATCH[1]}"
	    if [ -n "$gotten_wikiname" ]; then
		echo "$gotten_wikiname"
		return
	    fi
	fi
    done
    echo ""
}

get_dumptype() {
    # /bin/bash /usr/local/bin/fulldumps.sh 01 14 regular full 28
    host="$1"
    sshes sshes_quiet $host 'ps  -u dumpsgen -o args= | grep "/bin/bash /usr/local/bin/fulldumps.sh"'
}

get_processes() {
    host="$1"
    # the last thing in there is from /usr/bin/7za a -mx=4 :-P we don't want all the 7za because we don't want the decompress ones
    sshes sshes_quiet $host 'ps  -u dumpsgen -o args= | egrep "(dumpTextPass.php|xmlabstracts.py|xmlstubs.py|xmllogs.py|mysqldump|writeuptopageid|/dd|bzip2|mx=4)"'
}

get_load() {
    host="$1"
    sshes sshes_quiet $host 'uptime'
}

get_wiki_from_arg() {
    IFS="="
    arg_fields=($1)
    IFS="$DEFAULT_IFS"
    wikiname=${arg_fields[1]}
    echo "$wikiname"
}

get_wiki_from_filename() {
    # arwiki-20190301-stub-articles3.xml-p630707p1428626.gz
    IFS="-"
    arg_fields=($1)
    IFS="$DEFAULT_IFS"
    wikiname=${arg_fields[0]}
    echo "$wikiname"
}

get_contentdump_type() {
    for subfield in $@ ; do
	case "$subfield" in
	    *pages-meta-history* )
		echo "meta-history"
		return
		;;
	    *pages-meta-current* )
		echo "meta-current"
		return
		;;
	    *pages-articles* )
		echo "articles"
		return
		;;
	    * )
		continue
		;;
	esac
    done
    echo ""
}

get_dd_recombine_type() {
    for subfield in $@ ; do
	if [[ "$subfield" == of=* ]]; then
	    case $subfield in
		*abstract* )
		    echo "abstract-recombine"
		    return
		    ;;
		*pages-logging* )
		    echo "logs-recombine"
		    return
		    ;;
		*stub* )
		    echo "stubs-recombine"
		    return
		    ;;
	    esac
	fi
    done
    echo "unknown gz recombine"
}

get_lbzip2_recombine_type() {
    for subfield in $@ ; do
	case $subfield in
	    *pages-articles* )
		echo "articles-recombine"
		return
		;;
	    *meta-current* )
		echo "mea-current-recombine"
		return
		;;
	    *meta-history* )
		echo "meta-history-recombine"
		return
		;;
	esac
    done
    echo "unknown bzip2 recombine"
}

parse_entry() {
    command=""
    wiki=""

    while [ $# -gt 0 ]; do
	case "$1" in
	    "dumpTextPass.php" )
		# command="pagecontent"
                command=$( get_contentdump_type $@ )
		shift
		;;
	    "xmlstubs.py" )
                command="stubs"
		shift
		;;
	    "xmlabstracts.py" )
                command="abstracts"
		shift
		;;
	    "xmllogs.py" )
                command="logs"
		shift
		;;
	    "/usr/local/bin/recompressxml" )
                command="multistream"
		wiki=$( get_wikiname_from_args_via_filepath $@ )
		break
		;;
	    "/usr/local/bin/writeuptopageid" )
                command="prefetch-setup"
		shift
		;;
	    "/usr/bin/7za" )
                command="prefetch-setup"
		wiki=$( get_wikiname_from_args_via_filepath $@ )
		break
		;;
	    "/usr/bin/mysqldump" )
                command="tables"
		shift
		;;
	    "/bin/dd" )
                command=$( get_dd_recombine_type $@ )
		wiki=$( get_wikiname_from_args_via_filepath $@ )
		break
		;;
	    "/usr/bin/lbzip2" )
		shift
		if [ $1 == '-n' ]; then
                    command=$( get_lbzip2_recombine_type $@ )
		    wiki=$( get_wikiname_from_args_via_filepath $@ )
		    break
		fi
		;;
            "--fspecs" )
		shift
		wiki=$( get_wiki_from_filename "$1" )
		shift
		;;
            "--wiki" )
		shift
		wiki="$1"
		shift
		;;
	    "--wiki="* )
		wiki=$( get_wiki_from_arg "$1" )
		shift
		;;
	    * )
		shift
		;;
	esac
    done
    if [ -n "$command" -a -n "$wiki" ]; then
	if [ -z "${job_info[$wiki]}" ]; then
	    job_info[$wiki]="$command"
	else
	    job_info[$wiki]="${job_info[$wiki]} $command"
	fi
    fi
}

show_command_counts() {
    wiki_to_count=$1
    commands=${job_info[$wiki_to_count]}
    echo "  $wiki:"
    echo -n "    "
    echo -e "${commands// /\\n}" | sort | uniq -c
}

display_load() {
    #  11:52:47 up 10 days,  7:25,  1 user,  load average: 0,66, 0,54, 0,46
    if [ -z "$uptime" ]; then
	   echo "failed to get load"
    else
	load_fields=($uptime)
	load=${load_fields[10]}
	load=${load%,};
	echo "  load: $load"
    fi
}    

display_dumptype() {
    if [ -z "$dumpscript_line" ]; then
	echo "dumptype unknown (script not running)"
    else
	# /bin/bash /usr/local/bin/fulldumps.sh 01 14 regular full 28
	dumptype_fields=($dumpscript_line)
	dumptype="${dumptype_fields[4]} ${dumptype_fields[5]}"
	echo "  dump type: $dumptype"
    fi
}

display_processes() {
    for i in $( seq ${#process_entries[@]} ); do
	parse_entry ${process_entries[$i]}
    done
    for wiki in ${!job_info[@]}; do
	show_command_counts $wiki ${job_info[$wiki]}
    done
}

DEFAULTIFS="$IFS"
for hostnum in $hostnums; do
    unset job_info
    declare -A job_info
    hostname="snapshot100${hostnum}.eqiad.wmnet"
    processes=$( get_processes $hostname )
    IFS=$'\n'
    process_entries=($processes)
    IFS="$DEFAULTIFS"
    uptime=$( get_load $hostname )
    dumpscript_line=$( get_dumptype $hostname )
    echo "HOST $hostname:"
    display_load
    display_dumptype
    display_processes
done
