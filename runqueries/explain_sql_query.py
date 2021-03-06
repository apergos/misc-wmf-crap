#!/usr/bin/python3
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
# * consider what happens in worst case scenarios
#   - network goes away in the middle of a run
#     leaving the server still running its query
#   - other?


import getopt
import sys
import threading
import time
import MySQLdb
import queries.utils as qutils
import queries.queryinfo as qqueryinfo
import queries.args as qargs


def async_query(wiki, cursor, query, log):
    '''
    meant to be run as a thread, execute a query via the specified cursor,
    don't bother to return the results, just read and throw them away
    this gets shot by the caller, we only care about it running so we
    can show explain on its mysql process
    '''
    try:
        cursor.execute(query.encode('utf-8'))
        # we don't expect to get through this, we should be killed long before,
        # but let's have this here to be nice
        row = cursor.fetchone()
        while row is not None:
            row = cursor.fetchone()
        cursor.close()
    except MySQLdb.Error as ex:
        if ex.args[0] == 2013 or ex.args[0] == 1317:
            # this means it has been shot (probably), in any case we don't care
            # 1317: Query execution was interrupted
            # 2013: Lost connection to MySQL server during query
            log.info("Async Query: lost connection or query execution interrupted on wiki "
                     "%s (%s:%s)", wiki, ex.args[0], ex.args[1])
        else:
            raise MySQLdb.Error(("Async Query: exception running query on wiki "
                                 "{wiki} ({errno}:{message})".format(
                                     wiki=wiki, errno=ex.args[0], message=ex.args[1])))


class ExplainQueryInfo(qqueryinfo.QueryInfo):
    '''
    munge and run queries on db servers for specific wikis, doing a show
    explain on each one as it runs, then shooting it
    '''
    def start_query(self, wiki, cursor, query):
        '''
        runs the passed query via the specified cursor, in a separate
        thread, so we can do other things while it's running
        (for loose values of 'while')
        '''
        if self.args['dryrun']:
            self.log.info("would run %s", qutils.prettyprint_query(query))
            return None
        self.log.info("running:")
        self.log.info(qutils.prettyprint_query(query))
        thr = threading.Thread(target=async_query, args=(wiki, cursor, query, self.log))
        thr.start()
        return thr

    def check_if_mysqlthr_exists(self, host, thread_id):
        '''
        return True if exists, not if not, and None if we
        couldn't get  result
        '''
        if self.args['dryrun']:
            cursor = None
        else:
            cursor, _unused = self.dbinfo.get_cursor(host)
        query = 'SHOW PROCESSLIST;'
        if self.args['dryrun']:
            self.log.info("would run %s", qutils.prettyprint_query(query))
            return False
        self.log.info("running:")
        self.log.info(qutils.prettyprint_query(query))
        try:
            cursor.execute(query.encode('utf-8'))
            result = cursor.fetchall()
        except MySQLdb.Error as ex:
            self.log.warning("exception looking for thread id on host %s (%s:%s)",
                             host, ex.args[0], ex.args[1])
            return None
        self.log.info("show processlist:")
        self.log.info(qutils.prettyprint_rows(result, cursor.description))
        for row in result:
            if row[0] == thread_id:
                return True
        return False

    def explain(self, wiki, cursor, thread_id):
        '''
        show explain for a given thread id, given an
        initialized db cursor
        '''
        explain_query = 'SHOW EXPLAIN FOR ' + thread_id + ';'
        if self.args['dryrun']:
            self.log.info("would run %s", qutils.prettyprint_query(explain_query))
            return None, None
        self.log.info("running:")
        self.log.info(qutils.prettyprint_query(explain_query))
        try:
            cursor.execute(explain_query.encode('utf-8'))
            description = cursor.description
            explain_result = cursor.fetchall()
        except MySQLdb.Error as ex:
            if ex.args[0] == 1933 or ex.args[0] == 1094:
                # 1933:Target is not running an EXPLAINable command, i.e. query is already complete
                # 1094:Unknown thread id, i.e. query is already complete
                explain_result = None
                description = None
            else:
                raise MySQLdb.Error("exception explaining query on wiki "
                                    "{wiki} ({errno}:{message})".format(
                                        wiki=wiki, errno=ex.args[0], message=ex.args[1])) from None
        return explain_result, description

    def kill(self, wiki, cursor, thread_id):
        '''
        given a db cursor and a thread id, attempt to kill
        the thread and deal with errors
        '''
        kill_query = 'KILL ' + thread_id
        if self.args['dryrun']:
            self.log.info("would run %s", qutils.prettyprint_query(kill_query))
            return

        try:
            cursor.execute(kill_query.encode('utf-8'))
            kill_result = cursor.fetchall()
            self.log.info("result from kill: %s", kill_result)
        except MySQLdb.Error as ex:
            # 1094:Unknown thread id: <thread_id>
            if ex.args[0] != 1094:
                raise MySQLdb.Error(
                    ("exception killing query on wiki "
                     "{wiki} ({errno}:{message})".format(
                         wiki=wiki, errno=ex.args[0], message=ex.args[1]))) from None

    def explain_and_kill(self, host, wiki, thread_id, query):
        '''
        given the thread id of the thread running
        our query, show explain it, then shoot
        the query
        '''
        if self.args['dryrun']:
            cursor = None
        else:
            cursor, _unused = self.dbinfo.get_cursor(host)

        explain_result, description = self.explain(wiki, cursor, thread_id)
        self.kill(wiki, cursor, thread_id)
        qutils.print_and_log(self.log, "*** QUERY:")
        qutils.print_and_log(self.log, qutils.prettyprint_query(query))
        qutils.print_and_log(self.log, "*** SHOW EXPLAIN RESULTS:")
        qutils.print_and_log(self.log, qutils.prettyprint_rows(explain_result, description))

        if cursor is not None:
            cursor.close()

        # additional insurance.
        result = self.check_if_mysqlthr_exists(host, thread_id)
        self.log.info("check if query still running: %s", result)
        if result is None or result:
            # we had a problem checking, or the thread is still there
            # and presumably the kill failed
            self.log.error("quitting while we're behind; run the following on host %s", host)
            self.log.error("echo 'kill {thread_id}' | mysql --skip-ssl")
            raise MySQLdb.Error("query thread {id} still running".format(id=thread_id))

    def run_on_wiki(self, host, wiki, cursor, wiki_settings):
        '''
        run all queries for a specific wiki, after filling in the
        query template; this assumes a db cursor is passed in
        '''
        qutils.print_and_log(self.log, "*** WIKI: {wiki}".format(wiki=wiki))
        queries = self.fillin_query_template(wiki_settings)
        for query in queries:
            self.log.info("*** Starting new query check")
            if self.args['dryrun']:
                cursor, thread_id = None, None
            else:
                cursor, thread_id = self.dbinfo.get_cursor(host)
            self.dbinfo.do_use_wiki(wiki, cursor)
            # be nice to the servers
            time.sleep(0.05)
            thr = self.start_query(wiki, cursor, query)
            if self.args['dryrun']:
                thread_id = '<none (dryrun)>'
            else:
                thread_id = str(thread_id)
            self.explain_and_kill(host, wiki, thread_id, query)
            if cursor is not None:
                cursor.close()
            if not self.args['dryrun']:
                thr.join()


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''

    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write('\n')
    usage_message = """
Usage: python3 explain_sql_query.py --yamlfile <path> --queryfile <path> --configfile <path>
    [--dryrun] [--verbose] [--help]

This script reads server, wiki and variable names from the specified
yaml file, substitutes them into the query information read from the
specified query file, and show explains the resulting queries on the servers
and wiki dbs. The results are written to stdout.

"""
    usage_formats = qargs.get_common_arg_docs(['formats'])
    usage_output = qargs.get_common_arg_docs(['output'])
    usage_args = """
Arguments:
"""
    usage_common = qargs.get_common_arg_docs(['yamlfile', 'queryfile', 'settings'])
    usage_flags = qargs.get_common_arg_docs(['flags'])

    sys.stderr.write(usage_message + usage_formats + usage_output +
                     usage_args + usage_common + usage_flags)
    sys.exit(1)


def get_opt(opt, val, args):
    '''
    set option value in args dict if the option
    is one of the below
    '''
    if opt in ['-y', '--yamlfile']:
        args['yamlfile'] = val
    elif opt in ['-q', '--queryfile']:
        args['queryfile'] = val
    elif opt in ['-s', '--settings']:
        args['settings'] = val
    else:
        return False
    return True


def do_main():
    '''
    entry point
    '''
    args = qargs.get_arg_defaults(['yamlfile', 'queryfile', 'settings'],
                                  ['dryrun', 'verbose'])
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], 'y:q:s:dvh', ['yamlfile=', 'queryfile=', 'settings=',
                                        'dryrun', 'verbose', 'help'])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    qargs.handle_common_args(options, args, usage, remainder,
                             ['yamlfile', 'queryfile', 'settings'], get_opt)

    # even if this is set in the config file for use by other scripts, we want it off
    args['mwhost'] = None

    query = ExplainQueryInfo(args)
    query.run(keep_cursor=False)


if __name__ == '__main__':
    do_main()
