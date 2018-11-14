# -*- coding: utf-8 -*-
"""
Read mysql queries from a file, substitute in the value of
a set of variables for each of a series of db servers and
wikidbs, for each query start running it, show processlist
and get the thread id from the server, then show explain that
thread id and collect the output.
Finally, shoot the thread and go to the next query.
Note that this means each query will require a new connection
and db cursor.
"""


# TODOs:
# * (some) verbose messages to one output channel,
#   the bare minimum (query text for show explain
#   and results, along with db hostname, wiki) in
#   another output channel? Maybe severe issues
#   (exceptions that cause the program to quit,
#   failed kills) to go to a third which is always
#   the console?
# * check other exceptions that can be raised
#   by mysql calls and see if there's anything
#   unexpected
#   consider what happens in worst case scenarios
#     * network goes away in the middle of a run
#       leaving the server still running its query
#     * other?


from __future__ import print_function
import os
import getopt
import json
import logging
import logging.config
import re
import sys
import threading
from subprocess import Popen, PIPE
import warnings
import MySQLdb
import yaml
from prettytable import PrettyTable


def async_query(cursor, wiki, query):
    '''
    meant to be run as a thread, execute a query via the specified cursor,
    don't bother to return the results, just read and throw them away
    this gets shot by the caller, we only care about it running so we
    can show explain on its mysql process
    '''
    try:
        cursor.execute(query)
        # we don't expect to get through this, we should be killed long before,
        # but let's have this here to be nice
        row = cursor.fetchone()
        while row is not None:
            row = cursor.fetchone()
        cursor.close()
    except MySQLdb.Error as ex:
        if ex[0] == 2013 or ex[0] == 1317:
            # this means it has been shot (probably), in any case we don't care
            # 1317: Query execution was interrupted
            # 2013: Lost connection to MySQL server during query
            print("Async Query: lost connection or query execution interrupted on wiki "
                  "%s (%s:%s)" % (wiki, ex[0], ex[1]))
        else:
            raise MySQLdb.Error(("Async Query: exception running query on wiki %s (%s:%s)" % (
                wiki, ex[0], ex[1])))


class QueryInfo(object):
    '''
    munge and run queries on db servers for specific wikis
    '''

    def __init__(self, yamlfile, queryfile, dryrun, verbose):
        self.settings = self.get_settings_from_yaml(yamlfile)
        self.queries = self.get_queries_from_file(queryfile)
        self.verbose = verbose
        self.dryrun = dryrun
        # db credentials should be set up later by calling
        # get_db_creds
        self.dbuser = None
        self.dbpasswd = None
        if verbose:
            log_type = 'verbose'
        else:
            log_type = 'normal'
        self.log = logging.getLogger(log_type)    # pylint: disable=invalid-name
        warnings.filterwarnings("ignore", category=MySQLdb.Warning)

    @staticmethod
    def get_settings_from_yaml(yamlfile):
        '''
        read and return the contents from the yaml settings file
        '''
        if not os.path.exists(yamlfile):
            raise ValueError("no such yaml file " + yamlfile)
        contents = open(yamlfile).read()
        settings = yaml.load(contents)
        return settings

    @staticmethod
    def get_queries_from_file(queryfile):
        '''
        read and return the contents from the query file
        '''
        if not os.path.exists(queryfile):
            raise ValueError("no such file for queries " + queryfile)
        contents = open(queryfile).read()
        return contents

    @staticmethod
    def pad_line(line):
        '''
        for all but blank lines and lines starting with '#', add a newline on the end,
        then return the result
        '''
        if line.startswith('#'):
            line = line + '\n'
        elif line:
            line = ' ' + line
        return line

    def prettyprint_query(self, querystring):
        '''
        strip newline from end of non-comment lines of the querystring, print the
        results
        '''
        lines = querystring.splitlines()
        lines = [self.pad_line(line) for line in lines]
        result = ''.join(lines)
        return result

    def get_db_creds(self, php, credsfile):
        '''
        initialize db credentials by this icky execution of a
        php script etc. gross.
        '''
        command = [php, 'display_wgdbcreds.php', credsfile]
        if self.dryrun:
            print("would run command:", command)
            return
        else:
            self.log.info("running command: %s", command)
        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        if error:
            raise MySQLdb.Error("Errors encountered: %s" % error)
        self.log.info("got db creds: %s", output)
        creds = json.loads(output)
        if 'wgDBuser' not in creds or not creds['wgDBuser']:
            raise ValueError("Missing value for wgDBuser, bad dbcreds file?")
        if 'wgDBpassword' not in creds or not creds['wgDBpassword']:
            raise ValueError("Missing value for wgDBpassword, bad dbcreds file?")
        self.dbuser = creds['wgDBuser']
        self.dbpasswd = creds['wgDBpassword']

    def get_db_cursor(self, dbhost):
        '''
        set up db connection, get a connection and a db cursor,
        return the cursor along the thread id for the connection
        '''
        if self.dryrun:
            return None, None

        if ':' in dbhost:
            fields = dbhost.split(':')
            host = fields[0]
            port = int(fields[1])
        else:
            host = dbhost
            port = 3306
        try:
            dbconn = MySQLdb.connect(
                host=host, port=port,
                user=self.dbuser, passwd=self.dbpasswd)
            return dbconn.cursor(), dbconn.thread_id()
        except MySQLdb.Error as ex:
            raise MySQLdb.Error("failed to connect to or get cursor from %s:%s, %s %s" % (
                host, port, ex[0], ex[1]))

    def fillin_query_template(self, wiki_settings):
        '''
        fill in and return the query template with the
        specified settings
        '''
        querytext = self.queries
        # to handle setting names where one name is a left substring
        # of another ($NS and $NS_NAME  for example), sort them by length
        # and do longest substitutions first
        sorted_settings = sorted(wiki_settings, key=len)
        for setting in sorted_settings:
            name = '$' + setting.upper()
            querytext = querytext.replace(name, wiki_settings[setting])
        querytexts = re.split(r'^-----+$', querytext, flags=re.MULTILINE)
        return querytexts

    def do_use_wiki(self, cursor, wiki):
        '''
        does a simple 'USE wikidbname'. That is all.
        '''
        usequery = 'USE ' + wiki + ';'
        if self.dryrun:
            print("would run", self.prettyprint_query(usequery))
            return
        self.log.info("running %s", usequery)
        try:
            cursor.execute(usequery)
            result = cursor.fetchall()
        except MySQLdb.Error as ex:
            if result is not None:
                print("returned from fetchall:", result)
            raise MySQLdb.Error("exception for use %s (%s:%s)" % (wiki, ex[0], ex[1]))

    def start_query(self, cursor, wiki, query):
        '''
        runs the passed query via the specified cursor, in a separate
        thread, so we can do other things while it's running
        (for loose values of 'while')
        '''
        if self.dryrun:
            print("would run", self.prettyprint_query(query).encode('utf-8'))
            return
        self.log.info("running:")
        self.log.info(self.prettyprint_query(query).encode('utf-8'))
        thr = threading.Thread(target=async_query, args=(cursor, wiki, query))
        thr.start()
        return thr

    def check_if_mysqlthr_exists(self, host, thread_id):
        '''
        return True if exists, not if not, and None if we
        couldn't get  result
        '''
        cursor, _unused = self.get_db_cursor(host)
        query = 'SHOW PROCESSLIST;'
        if self.dryrun:
            print("would run", self.prettyprint_query(query).encode('utf-8'))
            return
        self.log.info("running:")
        self.log.info(self.prettyprint_query(query).encode('utf-8'))
        try:
            cursor.execute(query)
            result = cursor.fetchall()
        except MySQLdb.Error as ex:
            print("exception looking for thread id on host", host, ex)
            return None
        self.log.info("show processlist:")
        self.log.info(self.prettyprint_rows(result, cursor.description))
        for row in result:
            if row[0] == thread_id:
                return True
        return False

    @staticmethod
    def prettyprint_rows(results, description):
        '''
        print output from sql query nicely formatted as a table
        the way mysql cli does
        '''
        if results is None:
            print("no results available")
        else:
            headers = [desc[0] for desc in description]
            table = PrettyTable(headers)
            for header in headers:
                table.align[header] = "l"
            for entry in results:
                table.add_row(list(entry))
            print(table)

    def explain(self, cursor, wiki, thread_id):
        '''
        show explain for a given thread id, given an
        initialized db cursor
        '''
        explain_query = 'SHOW EXPLAIN FOR ' + thread_id + ';'
        if self.dryrun:
            print("would run", self.prettyprint_query(explain_query).encode('utf-8'))
            return
        self.log.info("running:")
        self.log.info(self.prettyprint_query(explain_query).encode('utf-8'))
        try:
            cursor.execute(explain_query)
            description = cursor.description
            explain_result = cursor.fetchall()
        except MySQLdb.Error as ex:
            if ex[0] == 1933:
                # 1933:Target is not running an EXPLAINable command, ie query is already complete
                explain_result = None
                description = None
            else:
                raise MySQLdb.Error(("exception explaining query on wiki %s (%s:%s)" % (
                    wiki, ex[0], ex[1])))
        return explain_result, description

    def kill(self, cursor, wiki, thread_id):
        '''
        given a db cursor and a thread id, attempt to kill
        the thread and deal with errors
        '''
        try:
            cursor.execute('KILL ' + thread_id)
            kill_result = cursor.fetchall()
            self.log.info("result from kill: %s", kill_result)
        except MySQLdb.Error as ex:
            # 1094:Unknown thread id: <thread_id>
            if ex[0] != 1094:
                raise MySQLdb.Error(("exception killing query on wiki %s (%s:%s)" % (
                    wiki, ex[0], ex[1])))

    def explain_and_kill(self, host, wiki, thread_id, query):
        '''
        given the thread id of the thread running
        our query, show explain it, then shoot
        the query
        '''
        cursor, _unused = self.get_db_cursor(host)

        explain_result, description = self.explain(cursor, wiki, thread_id)
        self.kill(cursor, wiki, thread_id)
        print(self.prettyprint_query(query).encode('utf-8'))
        self.prettyprint_rows(explain_result, description)

        cursor.close()

        # additional insurance.
        result = self.check_if_mysqlthr_exists(host, thread_id)
        self.log.info("check if query still running: %s", result)
        if result is None or result:
            # we had a problem checking, or the thread is still there
            # and presumably the kill failed
            print("quitting while we're behind; run the following on host {host}:".format(
                host=host))
            print("echo 'kill {thread_id}' | mysql --skip-ssl")
            raise MySQLdb.Error("query thread {id} still running".format(id=thread_id))

    def run_on_wiki(self, host, wiki, wiki_settings):
        '''
        run all queries for a specific wiki, after filling in the
        query template; this assumes a db cursor is passed in
        '''
        print("wiki:", wiki)
        queries = self.fillin_query_template(wiki_settings)
        for query in queries:
            self.log.info("*** Starting new query check")
            cursor, thread_id = self.get_db_cursor(host)
            self.do_use_wiki(cursor, wiki)
            thr = self.start_query(cursor, wiki, query)
            if self.dryrun:
                thread_id = '<none (dryrun)>'
            else:
                thread_id = str(thread_id)
            self.explain_and_kill(host, wiki, thread_id, query)
            if cursor is not None:
                cursor.close()
            if not self.dryrun:
                thr.join()

    def run_on_server(self, host, wikis_info):
        '''
        run queries on all wikis for specified server, after
        filling in the query template
        '''
        print("host:", host)
        for wiki in wikis_info:
            self.run_on_wiki(host, wiki, wikis_info[wiki])

    def run(self):
        '''
        run all queries on all wikis for each host, with variables in
        the query template filled in appropriately
        '''
        for shard in self.settings['servers']:
            print("info for shard", shard)
            for host in self.settings['servers'][shard]['hosts']:
                self.run_on_server(host, self.settings['servers'][shard]['wikis'])


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''

    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write('\n')
    usage_message = """
Usage: explain_sql_query.py --yamlfile <path> --queryfile <path> --credsfile <path>
    [--dryrun] [--verbose] [--help]

This script reads server, wiki and variable names from the specified
yaml file, substitutes them into the query information read from the
specified query file, and show explains the resulting queries on the servers
and wiki dbs. The results are written to stdout.

Query file format:

Content should consist of standard SQL queries. Each query may be on one or
several lines. Lines that start with five or more hypens (-----) are taken
as separators between queries. Variable names to be interpolated must be
in all caps and beginning with a dollar sign. See samplequery.sql for an
exmple.

Settings file format:

Content should be yaml, describing servers, wikis and variable names and values.
Variable names must correspond to he variables in the query file, although
they may be in any case. See samplesettings.yaml for an example.

Arguments:
    --yamlfile   (-y)   File with yaml-formatted list of db servers, wiki db names
                        and variable names for substitution into the query template
                        default: none
    --queryfile  (-q)   File with queries, possibly containing variable names consisting
                        of upper case strings starting with $, which will have values
                        from the yaml files substituted in before the queries are run
                        default: none
    --credsfile  (-c)   php file with MediaWiki creds for wgdbuser and wgdbpassword
                        default: none
    --php        (-p)   path to php command
                        default: /usr/bin//php
Flags:
    --dryrun  (-d)    Don't execute any queries but show what would be done
    --verbose (-v)    Display progress messages as queries are executed on the wikis
    --help    (-h)    show this message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def check_mandatory_args(yamlfile, queryfile, credsfile):
    '''
    verify that all mandatory args are specified
    '''
    if yamlfile is None:
        usage("Mandatory argument 'yamlfile' not specified")
    if queryfile is None:
        usage("Mandatory argument 'queryfile' not specified")
    if credsfile is None:
        usage("Mandatory argument 'credsfile' not specified")


logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': "[%(levelname)s]: %(message)s"
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'stream': sys.stdout,
            'formatter': 'simple'
        },
    },
    'loggers': {
        'verbose': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True
        },
        'normal': {
            'handlers': ['console'],
            'level': 'WARNING',
            'propagate': True
        }
    }
})


def do_main():
    '''
    entry point
    '''
    yamlfile = None
    queryfile = None
    credsfile = None
    php = '/usr/bin/php'
    dryrun = False
    verbose = False

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], 'y:q:c:p:dvh', ['yamlfile=', 'queryfile=', 'credsfile=', 'php=',
                                          'dryrun', 'verbose', 'help'])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ['-y', '--yamlfile']:
            yamlfile = val
        elif opt in ['-q', '--queryfile']:
            queryfile = val
        elif opt in ['-c', '--credsfile']:
            credsfile = val
        elif opt in ['-p', '--php']:
            php = val
        elif opt in ['-h', '--help']:
            usage("Help for this script")
        elif opt in ['-d', '--dryrun']:
            dryrun = True
        elif opt in ['-v', '--verbose']:
            verbose = True

    if remainder:
        usage("Unknown option(s) specified: <%s>" % remainder[0])

    check_mandatory_args(yamlfile, queryfile, credsfile)

    query = QueryInfo(yamlfile, queryfile, dryrun, verbose)
    query.get_db_creds(php, credsfile)
    query.run()


if __name__ == '__main__':
    do_main()
