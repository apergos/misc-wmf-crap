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
Usage: python3 run_sql_query.py --yamlfile <path> --queryfile <path> --configfile <path>
    [--dryrun] [--verbose] [--help]

This script reads server, wiki and variable names from the specified
yaml file, substitutes them into the query information read from the
specified query file, and runs the resulting queries on the servers
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
    --configfile (-c)   python-style config file with settings for the location of the
                        MW repo, the path to the multiversion directory if any,
                        and the path to the php binary. For more information see
                        sample.conf
                        default: none
Flags:
    --dryrun  (-d)    Don't execute queries but show what would be done
    --verbose (-v)    Display progress messages as queries are executed on the wikis
    --help    (-h)    show this message
"""
    sys.stderr.write(usage_message)
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
    elif opt in ['-c', '--configfile']:
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
            sys.argv[1:], 'y:q:c:dvh', ['yamlfile=', 'queryfile=', 'configfile=',
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
