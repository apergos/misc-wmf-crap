#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
read mysql queries from a file, substitute in the value of
a set of variables for each of a series of db servers and
wikidbs, run the queries and collect the output, write
the output to stdout
"""


import os
import getopt
import json
import re
import sys
from subprocess import Popen, PIPE
import configparser
import warnings
import MySQLdb
import yaml
from prettytable import PrettyTable


class QueryInfo():
    '''
    munge and run queries on db servers for specific wikis
    '''
    def __init__(self, yamlfile, queryfile, configfile, dryrun, verbose):
        self.verbose = verbose
        self.dryrun = dryrun
        self.settings = self.get_settings_from_yaml(yamlfile)
        self.queries = self.get_queries_from_file(queryfile)
        # choose the first wiki we find in the yaml file, for db creds.
        # yes, this means all your wikis better have the same credentials
        wikidb = self.get_first_wiki()
        self.dbcreds = get_dbcreds(configfile, wikidb, dryrun, verbose)
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

    def get_first_wiki(self):
        '''
        find and return the first wiki db name in the settings
        '''
        shards = list(self.settings['servers'].keys())
        return list(self.settings['servers'][shards[0]]['wikis'].keys())[0]

    def prettyprint(self, querystring):
        '''
        strip newline from end of non-comment lines of the querystring, print the
        results
        '''
        lines = querystring.splitlines()
        lines = [self.pad_line(line) for line in lines]
        result = ''.join(lines)
        return result

    def get_db_cursor(self, dbhost):
        '''
        set up db connection, get and return a db cursor
        '''
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
                user=self.dbcreds['wgDBuser'], passwd=self.dbcreds['wgDBpassword'])
            return dbconn.cursor()
        except MySQLdb.Error as ex:
            raise MySQLdb.Error("failed to connect to or get cursor from "
                                "{host}:{port}, {errno}:{message}".format(
                                    host=host, port=port, errno=ex.args[0], message=ex.args[1]))

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
        if self.dryrun:
            print("would run", self.prettyprint(usequery))
            for query in queries:
                print("would run", self.prettyprint(query))
            return
        if self.verbose:
            print("running", usequery)
        try:
            cursor.execute(usequery)
            result = cursor.fetchall()
        except MySQLdb.Error as ex:
            raise MySQLdb.Error("exception for use {wiki} ({errno}:{message})".format(
                wiki=wiki, errno=ex.args[0], message=ex.args[1]))
        for query in queries:
            if self.verbose:
                print("running:")
                print(self.prettyprint(query))
            try:
                cursor.execute(query.encode('utf-8'))
                result = cursor.fetchall()
            except MySQLdb.Error as ex:
                raise MySQLdb.Error("exception running query on wiki "
                                    "{wiki} ({errno}:{message})".format(
                                        wiki=wiki, errno=ex.args[0], message=ex.args[1]))
            print(self.prettyprint(query))
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
        if self.dryrun:
            cursor = None
        else:
            cursor = self.get_db_cursor(host)
        for wiki in wikis_info:
            self.run_on_wiki(cursor, wiki, wikis_info[wiki])
        if not self.dryrun:
            cursor.close()

    def run(self):
        '''
        run all queries on all wikis for each host, with variables in
        the query template filled in appropriately
        '''
        for shard in self.settings['servers']:
            print("info for shard", shard)
            for host in self.settings['servers'][shard]['hosts']:
                self.run_on_server(host, self.settings['servers'][shard]['wikis'])


SETTINGS = ['multiversion', 'mwrepo', 'php']


def config_setup(configfile):
    '''
    return a dict of config settings and their (possibly empty but not None) values
    '''
    defaults = get_config_defaults()
    conf = configparser.ConfigParser(defaults)
    conf.read(configfile)
    if not conf.has_section('settings'):
        sys.stderr.write("The mandatory configuration section "
                         "'settings' was not defined.\n")
        raise configparser.NoSectionError('settings')
    settings = parse_config(conf)
    return settings


def get_config_defaults():
    '''
    get and return default config settings for this crapola
    '''
    return {
        'multiversion': '',
        'mwrepo': '/srv/mediawiki',
        'php': '/usr/bin/php'
    }


def parse_config(conf):
    '''
    grab values from configuration and assign them to appropriate variables
    '''
    args = {}
    # could be true if we re only using the defaults
    if not conf.has_section('settings'):
        conf.add_section('settings')
    for setting in SETTINGS:
        args[setting] = conf.get('settings', setting)
    return args


def get_dbcreds(configfile, wikidb, dryrun, verbose):
    '''
    initialize db credentials by running a MW maintenance script to get the
    value of the user and password
    '''
    config = config_setup(configfile)
    pull_vars = ["wgDBuser", "wgDBpassword"]
    phpscript = 'getConfiguration.php'
    if config['multiversion']:
        mwscript = os.path.join(config['multiversion'], 'MWScript.php')
        command = [config['php'], mwscript, phpscript]
    else:
        command = [config['php'],
                   "{repo}/maintenance/{script}".format(
                       repo=config['mwrepo'], script=phpscript)]

    command.extend(["--wiki={dbname}".format(dbname=wikidb),
                    '--format=json', '--regex={vars}'.format(vars="|".join(pull_vars))])
    if dryrun:
        print("would run command:", command)
        return {}
    if verbose:
        print("running command:", command)
    proc = Popen(command, stdout=PIPE, stderr=PIPE)
    output, error = proc.communicate()
    if error:
        print("Errors encountered:", error.decode('utf-8'))
        sys.exit(1)
    if verbose:
        print("got db creds:", output.decode('utf-8'))
    creds = json.loads(output.decode('utf-8'))
    if 'wgDBuser' not in creds or not creds['wgDBuser']:
        raise ValueError("Missing value for wgDBuser")
    if 'wgDBpassword' not in creds or not creds['wgDBpassword']:
        raise ValueError("Missing value for wgDBpassword")
    return creds


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
                        queryrunner-sample.conf
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
    yamlfile = None
    queryfile = None
    configfile = None
    dryrun = False
    verbose = False

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
            dryrun = True
        elif opt in ['-v', '--verbose']:
            verbose = True

    if remainder:
        usage("Unknown option(s) specified: <{opt}>".format(opt=remainder[0]))

    if yamlfile is None:
        usage("Mandatory argument 'yamlfile' not specified")
    if queryfile is None:
        usage("Mandatory argument 'queryfile' not specified")
    if configfile is None:
        usage("Mandatory argument 'configfile' not specified")

    query = QueryInfo(yamlfile, queryfile, configfile, dryrun, verbose)
    query.run()


if __name__ == '__main__':
    do_main()
