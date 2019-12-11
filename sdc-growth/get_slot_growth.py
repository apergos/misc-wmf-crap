#!/usr/bin/python3
"""
Get growth rate of mediainfo or other slots for a wiki
starting from most recent to an arbitrary stopping point.

Run this from a mwmaintenance host.

It's best to run this on a db server in the vslow or dumps
group, just in case some query is slower than you expect.
"""


import sys
import getopt
import time
import json
from subprocess import Popen, PIPE, SubprocessError
import MySQLdb


def usage(message):
    """display usage message for this script with an optional
    preceding message"""
    if message is not None:
        print(message)
    usage_message = """Usage: get_slot_growth.py --wiki <wikidb> --host <hostname>
  --interval <number> [--endrev <number>|max] [--minslots <number>] [--slotrole mediainfo|main]
 [--user <name>] [--verbose]| --help

Arguments:

  --wiki      (-w):   wikidb name which will be queried
                      default: commonswiki
  --host      (-h):   hostname of dbserver
                      default: none
  --interval  (-i):   number of revisions per query
                      default: 1 million
  --endrev    (-e):   biggest rev id to query, we start here. either a number or 'max'
                      default: max
  --slotrole  (-s):   name of role of slots to check
                      default: mediainfo
  --minslots  (-m):   continue until fewer than this number of slots of the specified type
                      are found in an interval
                      default: 0
  --user      (-u):   shell user to sudo to for running script to get db creds, ugh
                      default: www-data
  --verbose   (-v):   display messages about files as they are created
                      default: false
  --help      (-h):   display this usage message

Example uses:
   python3 get_slot-growth.py -h db2219 -v
"""
    print(usage_message)
    sys.exit(1)


def get_args():
    """get command-line args and fill in default values"""
    args = {'wiki': 'commonswiki',
            'host': None,
            'interval': '1000000',
            'slotrole': 'mediainfo',
            'endrev': 'max',
            'minslots': '0',
            'user': 'www-data',
            'verbose': False}

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "w:h:i:s:e:m:u:vh", [
                "wiki=", "host=", "interval=", "slotrole=", "endrev=", "minslots=",
                "user=", "verbose", "help"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-w", "--wiki"]:
            args['wiki'] = val
        elif opt in ["-h", "--host"]:
            args['host'] = val
        elif opt in ["-i", "--interval"]:
            args['interval'] = val
        elif opt in ["-s", "--slotrole"]:
            args['slotrole'] = val
        elif opt in ["-e", "--endrev"]:
            args['endrev'] = val
        elif opt in ["-m", "--minslots"]:
            args['minslots'] = val
        elif opt in ["-u", "--user"]:
            args['user'] = val
        elif opt in ["-v", "--verbose"]:
            args['verbose'] = True
        elif opt in ["-h", "--help"]:
            usage("Help for this script:")

    if remainder:
        usage("Unknown option(s) specified: {opt}".format(opt=remainder[0]))

    return args


def validate_args(args):
    """validate args or whine and die"""
    if not args['host']:
        usage('Mandatory arg "host" was not specified')
    if not args['endrev'].isdigit() and args['endrev'] != 'max':
        usage('Arg "endrev" must be a number or the value "max"')
    if not args['interval'].isdigit():
        usage('Arg "interval" must be a number')
    if not args['minslots'].isdigit():
        usage('Arg "minslots" must be a number')
    if args['endrev'].isdigit():
        args['endrev'] = int(args['endrev'])
    args['interval'] = int(args['interval'])
    args['minslots'] = int(args['minslots'])


def get_creds(wiki, shelluser, verbose):
    """get mediawiki credentials for the given wiki"""
    pull_vars = ["wgDBuser", "wgDBpassword"]
    command = ['sudo', '-u', shelluser, '/usr/local/bin/mwscript', 'getConfiguration.php']
    command.extend(["--wiki={dbname}".format(dbname=wiki),
                    '--format=json',
                    '--regex={vars}'.format(vars="|".join(pull_vars))])

    if verbose:
        print("running command: %s", command)
    proc = Popen(command, stdout=PIPE, stderr=PIPE)
    output, error = proc.communicate()
    if error:
        sys.stderr.write("got db creds: {creds}\n".format(creds=output.decode('utf-8')))
        raise SubprocessError("Errors encountered: {error}".format(error=error.decode('utf-8')))
    try:
        creds = json.loads(output.decode('utf-8'))
    except ValueError:
        sys.stderr.write("got db creds: {creds}\n".format(creds=output.decode('utf-8')))
        raise
    if 'wgDBuser' not in creds or not creds['wgDBuser']:
        sys.stderr.write("got db creds: {creds}\n".format(creds=output.decode('utf-8')))
        raise ValueError("Missing value for wgDBuser, bad dbcreds file?")
    if 'wgDBpassword' not in creds or not creds['wgDBpassword']:
        sys.stderr.write("got db creds: {creds}\n".format(creds=output.decode('utf-8')))
        raise ValueError("Missing value for wgDBpassword, bad dbcreds file?")
    return creds


def get_cursor(host, user, password):
    '''
    open a connection, get and return a cursor
    '''
    port = 3306

    try:
        dbconn = MySQLdb.connect(host=host, port=port,
                                 user=user, passwd=password)
        return dbconn.cursor()
    except MySQLdb.Error as ex:
        raise MySQLdb.Error(
            "failed to connect to or get cursor from "
            "{host}:{port}, {errno}:{message}".format(
                host=host, port=port, errno=ex.args[0], message=ex.args[1])) from None
    return dbconn


def use_db(cursor, wiki):
    """select the given wiki as the database for all queries"""
    use_query = 'USE ' + wiki + ';'
    try:
        cursor.execute(use_query.encode('utf-8'))
        _result = cursor.fetchall()
    except MySQLdb.Error as ex:
        raise MySQLdb.Error("exception for use {wiki} ({errno}:{message})".format(
            wiki=wiki, errno=ex.args[0], message=ex.args[1])) from None


def get_starting_rev(endrev, db_cursor, verbose):
    """return the starting rev number: if endrev is a number,
    return that. if it is the special value 'max', ask the db
    for the max rev id and return that"""
    if endrev != 'max':
        return endrev

    # go ask the db
    query = 'SELECT MAX(rev_id) from revision;'
    try:
        if verbose:
            print(query)
        db_cursor.execute(query.encode('utf-8'))
    except MySQLdb.Error as ex:
        raise MySQLdb.Error("exception getting max rev id ({errno}:{message})".format(
            errno=ex.args[0], message=ex.args[1])) from None
    revid_rows = db_cursor.fetchall()
    rev_ids = []
    for revid_row in revid_rows:
        if not revid_row:
            break
        rev_ids.append(revid_row[0])
    if len(rev_ids) != 1:
        raise MySQLdb.Error("expected one entry with max rev id, found {count})".format(
            count=len(rev_ids))) from None
    return int(rev_ids[0])


def get_rev_query(start_rev):
    """return a query to get the fields we want for the slot range"""
    return "SELECT rev_id, rev_timestamp FROM revision WHERE rev_id = {revid};".format(
        revid=start_rev)


def do_rev_query(query, db_cursor, revid, verbose):
    """run a query designed to get the rev id and timestamp, return them if there,
    or None if no entries, which is weird but can happen"""
    try:
        if verbose:
            print(query)
        db_cursor.execute(query.encode('utf-8'))
    except MySQLdb.Error as ex:
        raise MySQLdb.Error("failed to get rev timestamp for {{revid}} ({errno}:{message})".format(
            errno=ex.args[0], message=ex.args[1])) from None
    rev_rows = db_cursor.fetchall()
    if not rev_rows:
        return None
    rev_info = []
    for rev_row in rev_rows:
        if not rev_row:
            break
        rev_info.append({'rev_id': rev_row[0], 'rev_timestamp': rev_row[1]})
    if len(rev_info) != 1:
        raise MySQLdb.Error("expected one rev timestamp entry, found {count} for {revid})".format(
            count=len(rev_info), revid=revid)) from None
    return rev_info[0]


def get_rev_timestamp_for_end_interval(rev, db_cursor, verbose):
    """get the timestamp for the largest revision, in some cases it could be missing
    so be prepared to decrement and keep trying until we find one. example: revid 359704594"""
    done = False
    count = 0
    while not done:
        query = get_rev_query(rev)
        rev_info = do_rev_query(query, db_cursor, rev, verbose)
        if rev_info:
            done = True
        else:
            rev += 1
            count += 1
            if count > 100:
                raise MySQLdb.Error("can't get rev timestamp for {rev} after 100 tries".format(
                    rev=rev)) from None
    return rev_info


def get_slots_query(start_rev, interval, slot_role_id):
    """return a query to get the fields we want for the slot range"""
    query = "SELECT count(*) FROM slots"
    minrev = start_rev - interval
    if minrev < 1:
        minrev = 1
    cond = " WHERE slot_revision_id > {minrev} AND slot_revision_id <= {maxrev} AND slot_role_id = {roleid}".format(
        minrev=minrev, maxrev=start_rev, roleid=slot_role_id)
    return query + cond


def do_slots_query(query, db_cursor, rev, verbose):
    """get slot count for the query covering some revision interval"""
    try:
        if verbose:
            print(query)
        db_cursor.execute(query.encode('utf-8'))
    except MySQLdb.Error as ex:
        raise MySQLdb.Error("failed to get rev timestamp for {{revid}} ({errno}:{message})".format(
            errno=ex.args[0], message=ex.args[1])) from None
    slot_rows = db_cursor.fetchall()
    if not slot_rows:
        return None
    slot_info = []
    for slot_row in slot_rows:
        if not slot_row:
            break
        slot_info.append({'slot_increment': slot_row[0], 'slot_rev_id': rev})
    if len(slot_info) != 1:
        raise MySQLdb.Error("expected one entry with slot count, found {count} for {revid})".format(
            count=len(slot_info), revid=rev)) from None
    return slot_info[0]


def get_slot_id(db_cursor, slot_role, verbose):
    """get and return the slot id for the given role or None if not found"""
    query = "SELECT role_id from slot_roles where role_name = '{role}';".format(
        role=slot_role)
    try:
        if verbose:
            print(query)
        db_cursor.execute(query.encode('utf-8'))
    except MySQLdb.Error as ex:
        raise MySQLdb.Error("exception getting slot id for role {role} ({errno}:{message})".format(
            role=slot_role, errno=ex.args[0], message=ex.args[1])) from None
    roleid_rows = db_cursor.fetchall()
    role_ids = []
    for roleid_row in roleid_rows:
        if not roleid_row:
            break
        role_ids.append(roleid_row[0])
    if len(role_ids) != 1:
        raise MySQLdb.Error("expected one entry with role id, found {count})".format(
            count=len(role_ids))) from None
    return int(role_ids[0])


def get_slot_growth(args):
    """get information on growth of slots for the given
    role and wikidb, return as a list with largest revid first"""
    results = []

    dbcreds = get_creds(args['wiki'], args['user'], args['verbose'])
    db_cursor = get_cursor(args['host'], dbcreds['wgDBuser'], dbcreds['wgDBpassword'])
    use_db(db_cursor, args['wiki'])

    slot_id = get_slot_id(db_cursor, args['slotrole'], args['verbose'])

    done = False
    start_rev = get_starting_rev(args['endrev'], db_cursor, args['verbose'])
    while not done:
        rev_info = get_rev_timestamp_for_end_interval(start_rev, db_cursor, args['verbose'])

        query = get_slots_query(start_rev, args['interval'], slot_id)
        slot_info = do_slots_query(query, db_cursor, start_rev, args['verbose'])

        if args['verbose']:
            print("revinfo is", rev_info, "and slotinfo is", slot_info)

        entry = {'revid': rev_info['rev_id'],
                 'timestamp': rev_info['rev_timestamp'].decode('utf-8'),
                 'slotcount': slot_info['slot_increment']}

        results.append(entry)
        start_rev = start_rev - args['interval']
        if start_rev < 1:
            done = True
        elif entry['slotcount'] <= args['minslots']:
            done = True
        else:
            # sleep a lot between queries because we are nice
            time.sleep(5)

    return results


def display_slot_growth(results):
    """given a mess of slot growth results, display it in
    some nice way"""
    # we eventually want two files for graphing purposes:
    # timestamp vs # revs
    # timestamp vs total number slots
    # we write json of each entry smallest to largest rev so it's easy for other scripts to parse
    for entry in reversed(results):
        print(json.dumps(entry))


def do_main():
    """entry point"""
    args = get_args()
    validate_args(args)
    results = get_slot_growth(args)
    display_slot_growth(results)


if __name__ == '__main__':
    do_main()
