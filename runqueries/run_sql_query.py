#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
read mysql queries from a file, substitute in the value of
a set of variables for each of a series of db servers and
wikidbs, run the queries and collect the output, write
the output to stdout
"""


import getopt
import sys
import MySQLdb
import queries.config as qconfig
import queries.utils as qutils
import queries.queryinfo as qqueryinfo
import queries.args as qargs


class RunQueryInfo(qqueryinfo.QueryInfo):
    '''
    munge and run queries on db servers for specific wikis
    '''
    def run_on_wiki(self, cursor, host, wiki, wiki_settings):
        '''
        run all queries for a specific wiki, after filling in the
        query template; this assumes a db cursor is passed in
        '''
        print("wiki:", wiki)
        queries = self.fillin_query_template(wiki_settings)
        self.do_use_wiki(cursor, wiki)
        if self.args['dryrun']:
            for query in queries:
                print("would run", qutils.prettyprint_query(query))
            return
        for query in queries:
            if self.args['verbose']:
                print("running:")
                print(qutils.prettyprint_query(query))
            try:
                cursor.execute(query.encode('utf-8'))
                result = cursor.fetchall()
            except MySQLdb.Error as ex:
                raise MySQLdb.Error(
                    "exception running query on host "
                    "{host}, wiki {wiki} ({errno}:{message})".format(
                        host=host, wiki=wiki, errno=ex.args[0], message=ex.args[1]))
            print(qutils.prettyprint_query(query))
            print(qutils.prettyprint_rows(result, cursor.description))


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''

    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write('\n')
    usage_message = """
Usage: python3 run_sql_query.py --yamlfile <path> --queryfile <path> --settings <path>
    [--dryrun] [--verbose] [--help]

This script reads server, wiki and variable names from the specified
yaml file, substitutes them into the query information read from the
specified query file, and runs the resulting queries on the servers
and wiki dbs. The results are written to stdout.

"""
    usage_formats = qargs.get_common_arg_docs(['formats'])
    usage_args = """
Arguments:
"""
    usage_common = qargs.get_common_arg_docs(['yamlfile', 'queryfile', 'settings'])
    usage_flags = qargs.get_common_arg_docs(['flags'])

    sys.stderr.write(usage_message + usage_formats + usage_args + usage_common + usage_flags)
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
        args['configfile'] = val
    else:
        return False
    return True


def do_main():
    '''
    entry point
    '''
    args = {}
    args['yamlfile'] = None
    args['queryfile'] = None
    args['configfile'] = None
    args['dryrun'] = False
    args['verbose'] = False

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], 'y:q:s:dvh', ['yamlfile=', 'queryfile=', 'settings=',
                                        'dryrun', 'verbose', 'help'])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if not get_opt(opt, val, args):
            if not qargs.get_flag(opt, args, usage):
                usage("Unknown option specified: <{opt}>".format(opt=opt))

    if remainder:
        usage("Unknown option(s) specified: <{opt}>".format(opt=remainder[0]))

    qargs.check_mandatory_args(args, ['yamlfile', 'queryfile', 'configfile'], usage)

    configfile = args.get('configfile')
    conf = qconfig.config_setup(configfile)
    for setting in qconfig.SETTINGS:
        if setting not in args:
            args[setting] = conf[setting]

    # even if this is set in the config file for use by other scripts, we want it off
    args['mwhost'] = None

    query = RunQueryInfo(args)
    query.run(keep_cursor=True)


if __name__ == '__main__':
    do_main()
