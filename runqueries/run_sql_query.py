#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
read mysql queries from a file, substitute in the value of
a set of variables for each of a series of db servers and
wikidbs, run the queries and collect the output, write
the output to stdout
"""


import getopt
import re
import sys
import warnings
import MySQLdb
from prettytable import PrettyTable
import queries.config as qconfig
import queries.dbinfo as qdbinfo
import queries.utils as qutils


class QueryInfo():
    '''
    munge and run queries on db servers for specific wikis
    '''
    def __init__(self, yamlfile, queryfile, args):
        self.args = args
        self.settings = qutils.get_settings_from_yaml(yamlfile)
        self.queries = qutils.get_queries_from_file(queryfile)
        # choose the first wiki we find in the yaml file, for db creds.
        # yes, this means all your wikis better have the same credentials
        wikidb = self.get_first_wiki()
        self.dbinfo = qdbinfo.DbInfo(args)
        self.dbcreds = self.dbinfo.get_dbcreds(wikidb)
        warnings.filterwarnings("ignore", category=MySQLdb.Warning)

    def get_first_wiki(self):
        '''
        find and return the first wiki db name in the settings
        '''
        sections = list(self.settings['servers'].keys())
        return list(self.settings['servers'][sections[0]]['wikis'].keys())[0]

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

    def run_on_wiki(self, cursor, wiki, wiki_settings):
        '''
        run all queries for a specific wiki, after filling in the
        query template; this assumes a db cursor is passed in
        '''
        print("wiki:", wiki)
        queries = self.fillin_query_template(wiki_settings)
        usequery = 'USE ' + wiki + ';'
        if self.args['dryrun']:
            print("would run", qutils.prettyprint_query(usequery))
            for query in queries:
                print("would run", qutils.prettyprint_query(query))
            return
        if self.args['verbose']:
            print("running", usequery)
        try:
            cursor.execute(usequery)
            result = cursor.fetchall()
        except MySQLdb.Error as ex:
            raise MySQLdb.Error("exception for use {wiki} ({errno}:{message})".format(
                wiki=wiki, errno=ex.args[0], message=ex.args[1]))
        for query in queries:
            if self.args['verbose']:
                print("running:")
                print(qutils.prettyprint_query(query))
            try:
                cursor.execute(query.encode('utf-8'))
                result = cursor.fetchall()
            except MySQLdb.Error as ex:
                raise MySQLdb.Error("exception running query on wiki "
                                    "{wiki} ({errno}:{message})".format(
                                        wiki=wiki, errno=ex.args[0], message=ex.args[1]))
            print(qutils.prettyprint_query(query))
            headers = [desc[0] for desc in cursor.description]
            table = PrettyTable(headers)
            for header in headers:
                table.align[header] = "l"
            for entry in result:
                table.add_row(list(entry))
            print(table)

    def run_on_server(self, host, wikis_info):
        '''
        run queries on all wikis for specified server, after
        filling in the query template
        '''
        print("host:", host)
        if self.args['dryrun']:
            cursor = None
        else:
            cursor, _unused = self.dbinfo.get_cursor(host)
        for wiki in wikis_info:
            self.run_on_wiki(cursor, wiki, wikis_info[wiki])
        if not self.args['dryrun']:
            cursor.close()

    def run(self):
        '''
        run all queries on all wikis for each host, with variables in
        the query template filled in appropriately
        '''
        for section in self.settings['servers']:
            print("info for section", section)
            for host in self.settings['servers'][section]['hosts']:
                self.run_on_server(host, self.settings['servers'][section]['wikis'])


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


def do_main():
    '''
    entry point
    '''
    args = {}
    yamlfile = None
    queryfile = None
    configfile = None
    args['dryrun'] = False
    args['verbose'] = False

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], 'y:q:c:dvh', ['yamlfile=', 'queryfile=', 'configfile=',
                                        'dryrun', 'verbose', 'help'])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ['-y', '--yamlfile']:
            yamlfile = val
        elif opt in ['-q', '--queryfile']:
            queryfile = val
        elif opt in ['-c', '--configfile']:
            configfile = val
        elif opt in ['-h', '--help']:
            usage("Help for this script")
        elif opt in ['-d', '--dryrun']:
            args['dryrun'] = True
        elif opt in ['-v', '--verbose']:
            args['verbose'] = True

    if remainder:
        usage("Unknown option(s) specified: <{opt}>".format(opt=remainder[0]))

    if yamlfile is None:
        usage("Mandatory argument 'yamlfile' not specified")
    if queryfile is None:
        usage("Mandatory argument 'queryfile' not specified")
    if configfile is None:
        usage("Mandatory argument 'configfile' not specified")

    conf = qconfig.config_setup(configfile)
    for setting in qconfig.SETTINGS:
        if setting not in args:
            args[setting] = conf[setting]

    # even if this is set in the config file for use by other scripts, we want it off
    args['mwhost'] = None

    query = QueryInfo(yamlfile, queryfile, args)
    query.run()


if __name__ == '__main__':
    do_main()
