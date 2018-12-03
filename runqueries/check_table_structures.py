#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Retrieve table structure information from MediaWiki
database servers for various wikis and compare them

We don't use the info schema tables because we want
to be able to use a regular db user which may not
have access to them.

We sleep a tiny bit between requests because some
requests may be made on masters, and while they will
be fast, we don't want to impact other traffic.
"""


import os
import sys
import getopt
import logging
import logging.config
from collections import OrderedDict
import time
import MySQLdb
import queries.logger as qlogger
import queries.dbinfo as qdbinfo
import queries.args as qargs


class TableDiffs():
    '''
    methods for comparing and displaying db table structure info
    '''
    def __init__(self, dbinfo, verbose):
        self.dbinfo = dbinfo
        self.verbose = verbose
        # these are set in display_wikidb_diff as needed
        self.wiki = None
        self.dbhost = None
        self.master = None
        # used if we want to compare all hosts against one single db server
        # even across wikis etc.

    @staticmethod
    def indent(count):
        '''
        really pylint? fine. lets you call indent(3) instead
        of typing '   ' which is harder to eyeball and get right
        '''
        return ' '*count

    def display_table_structure(self, table_structure):
        '''
        given the table structure, display it nicely
        '''
        for table in table_structure:
            print(self.indent(3), 'table:', table)
            print(self.indent(7), 'columns:')
            if 'columns' in table_structure[table]:
                for column in table_structure[table]['columns']:
                    print(self.indent(11), 'name:', column)
                    print(self.indent(11), 'properties:', table_structure[table]['columns'][column])
            print(self.indent(7), 'keys:')
            if 'keys' in table_structure[table]:
                for key in table_structure[table]['keys']:
                    print(self.indent(12), 'info:', key)
            print(self.indent(7), 'parameters:')
            if 'parameters' in table_structure[table]:
                for param in table_structure[table]['parameters']:
                    print(self.indent(11), param.lstrip(') '))

    @staticmethod
    def params_to_dict(text):
        '''
        given a line of text like
        ) ENGINE=InnoDB AUTO_INCREMENT=1480546 DEFAULT CHARSET=binary ROW_FORMAT=COMPRESSED
        turn it into a dict of keys where some values may be None (thanks MySQL for making 'DEFAULT
        CHARSET have a space in it >_<) and return it
        '''
        entries = text.lstrip(') ').split()
        params = {}
        for entry in entries:
            fields = entry.split('=')
            if len(fields) > 1:
                val = fields[1]
            else:
                val = None
            params[fields[0]] = val
        return params

    def display_parameter_diffs(self, master_table, repl_table, table, wiki):
        '''
        display all differences in properties in a table on master, replica
        '''
        master_params = self.params_to_dict(master_table['parameters'][0])
        ignore = ['AUTO_INCREMENT', 'DEFAULT']
        if master_table['parameters'][0] != repl_table['parameters'][0]:
            master_params = self.params_to_dict(master_table['parameters'][0])
            repl_params = self.params_to_dict(repl_table['parameters'][0])
            master_params_missing = []
            repl_params_missing = []
            param_diffs = []
            for field in master_params:
                if field not in repl_params:
                    master_params_missing.append(field)
                elif master_params[field] != repl_params[field] and field not in ignore:
                    param_diffs.append('{field}: master {mval}, repl {rval}'.format(
                        field=field, mval=master_params[field], rval=repl_params[field]))
            for field in repl_params:
                if field not in master_params:
                    repl_params_missing.append(field)
            if master_params_missing:
                print("table", table, "has master parameters",
                      " ".join(master_params_missing),
                      "missing on replica", self.dbhost, "wiki", wiki)
            if repl_params_missing:
                print("table", table, "has parameters",
                      " ".join(repl_params_missing),
                      "extra on replica", self.dbhost, "wiki", wiki)
            if param_diffs:
                print("table", table, "has parameter value differences",
                      " ".join(param_diffs),
                      "on replica", self.dbhost, "wiki", wiki)

    def display_key_diffs(self, master_table, repl_table, table, wiki):
        '''
        display all differences in keys in a table on master, replica
        '''
        for key in master_table['keys']:
            if key not in repl_table['keys']:
                print("table", table, "has key", key,
                      "missing from replica", self.dbhost, "wiki", wiki)
        for key in repl_table['keys']:
            if key not in master_table['keys']:
                print("table", table, "has key", key,
                      "extra on replica", self.dbhost, "wiki", wiki)

    def display_column_diffs(self, master_table, repl_table, table, wiki):
        '''
        display all column differences in a table on master, replica
        '''
        for column in master_table['columns']:
            if column not in repl_table['columns']:
                print("table", table, "has column", column,
                      "missing from replica", self.dbhost, "wiki", wiki)
        for column in repl_table['columns']:
            if column not in master_table['columns']:
                print("table", table, "has column", column,
                      "extra on replica", self.dbhost, "wiki", wiki)
        for column in master_table['columns']:
            if column not in repl_table['columns']:
                continue
        if master_table['columns'][column] != repl_table['columns'][column]:
            print("repl table", table, "column", column, "structure mismatch",
                  repl_table['columns'][column])

    def display_table_diffs(self, master_table, repl_table, table, wiki):
        '''
        display all differences between tables on master and replica
        '''
        if not master_table:
            if repl_table:
                print("table", table, "missing on master")
            return
        if not repl_table:
            print("table", table, "missing on replica")
            return
        self.display_column_diffs(master_table, repl_table, table, wiki)
        self.display_key_diffs(master_table, repl_table, table, wiki)
        self.display_parameter_diffs(master_table, repl_table, table, wiki)

    def display_wikidb_diff(self, results, wiki, main_master, main_wiki):
        '''
        find and display all table structure differences between dbhost and master
        '''
        if self.dbhost not in results or wiki not in results[self.dbhost]:
            print("No tables for wiki on", self.dbhost)
            return
        repl_table_structure = results[self.dbhost][wiki]
        if main_wiki:
            # we compare everything to table structure of one wiki
            if main_master not in results or main_wiki not in results[main_master]:
                print("No diffs available: no tables for main wiki {wiki} on {host}".format(
                    wiki=main_wiki, host=main_master))
                return
            master_table_structure = results[main_master][main_wiki]
        else:
            master_table_structure = results[main_master][wiki]
        for table in master_table_structure:
            if table not in repl_table_structure:
                print("table", table, "missing from replica", self.dbhost, "wiki", wiki)
        for table in repl_table_structure:
            if table not in master_table_structure:
                print("table", table, "extra on replica", self.dbhost, "wiki", wiki)
        for table in master_table_structure:
            if table not in repl_table_structure:
                continue
            self.display_table_diffs(master_table_structure[table],
                                     repl_table_structure[table], table, wiki)

    def display_diffs_master_per_sect(self, results):
        '''
        for each master on a section, get the wiki table
        structure on the master, compare the table structure
        of that wiki on all replicas in that section
        '''
        masters = self.dbinfo.get_masters()
        for master in masters:
            wikis_todo = self.dbinfo.get_wikis_for_dbhost(master, self.dbinfo.args['wikilist'])
            if not wikis_todo:
                continue
            master_results = results[master]
            for wiki in master_results:
                print('master:', master)
                print('wiki:', wiki)
                if self.verbose:
                    self.display_table_structure(master_results[wiki])
                dbhosts_todo = self.dbinfo.get_dbhosts_for_wiki(wiki)
                for dbhost in dbhosts_todo:
                    dbhost_results = results[dbhost]
                    if self.verbose:
                        print('replica:', dbhost)
                        self.display_table_structure(dbhost_results[wiki])
                print('DIFFS ****')
                for dbhost in dbhosts_todo:
                    if dbhost == master:
                        continue
                    self.dbhost = dbhost
                    self.display_wikidb_diff(results, wiki, master, None)

    def display_diffs_master_all_sects(self, results, main_master, main_wiki):
        '''
        display table structure diffs taken against one wiki on a specified
        master
        example: use db2010 enwiki table structure against which to compare
        the table structure of all the s5 wikis on the s5 db hosts, even though
        enwiki is not in s5
        better exmple: decide enwiki on db2010 has the table structure you
        want on all wikis everywhere and compare them all against it
        '''
        if main_master in results:
            master_results = results[main_master]
        else:
            # example: a dry run will produce this
            master_results = {}
        masters = self.dbinfo.get_masters()
        print("all wiki tables will be checked against {db}:{wiki}".format(
            db=main_master, wiki=main_wiki))

        if self.verbose and master_results:
            self.display_table_structure(master_results[main_wiki])

        masters = self.dbinfo.get_masters()
        for section_master in masters:
            if section_master not in results:
                # this db (and so the whole section) did not have any of them
                # wikis in our list to check
                continue
            section_master_results = results[section_master]
            for wiki in section_master_results:
                dbhosts_todo = self.dbinfo.get_dbhosts_for_wiki(wiki)
                for dbhost in dbhosts_todo:
                    dbhost_results = results[dbhost]
                    if self.verbose:
                        print('replica:', dbhost)
                        self.display_table_structure(dbhost_results[wiki])
                print('DIFFS ****')
                for dbhost in dbhosts_todo:
                    if dbhost == main_master:
                        continue
                    self.dbhost = dbhost
                    self.display_wikidb_diff(results, wiki, main_master, main_wiki)

    def display_diffs(self, results, main_master, main_wiki):
        '''
        show differences between structure on master and replicas for
        each wiki
        if 'main_master', is not None, then we use that as the
        sole master and compare other dbs (masters or not) across all wikis
        to it; in this case main_wiki must also be supplied and that
        wiki's tables on the sole master will be used as the standard against
        which to diff everything else
        '''
        if main_master:
            self.display_diffs_master_all_sects(results, main_master, main_wiki)
        else:
            self.display_diffs_master_per_sect(results)


class TableInfo():
    '''
    methods for retrieving db table structure info
    '''
    def __init__(self, args, dbinfo):
        self.args = args
        self.dbinfo = dbinfo
        qlogger.logging_setup()
        if self.args['verbose'] or self.args['dryrun']:
            log_type = 'verbose'
        else:
            log_type = 'normal'
        self.log = logging.getLogger(log_type)    # pylint: disable=invalid-name
        self.tablediffs = TableDiffs(self.dbinfo, self.args['verbose'])

    @staticmethod
    def format_create_table_info(sql_results):
        '''
        given output from a SHOW CREATE TABLE query,
        format it into a nice dict and return that
        '''
        # format:     (('tablename', "CREATE TABLE `tablename` etc..."),)
        formatted = {}
        lines = sql_results[0][1].split('\n')
        lines = [line.strip(' ').rstrip(',') for line in lines]
        # CREATE TABLE `blah` (
        if not lines[0].startswith("CREATE TABLE "):
            # bad result somehow FIXME log this
            return []
        formatted['table'] = lines[0][14:-3]
        formatted['columns'] = OrderedDict()
        for line in lines:
            if line.startswith('`'):
                fields = line.split('`')
                # column lines look like "`name` some more stuff", we want to break out the name
                name = fields[1]
                properties = fields[2].strip(' ')
                formatted['columns'][name] = properties

        formatted['keys'] = [line for line in lines if 'KEY ' in line]
        formatted['parameters'] = [line for line in lines if line.startswith(')')]
        return formatted

    def check_version(self, dbcursor, dbhost):
        '''
        get the version of mysql. hey, it might differ, no?
        '''
        querystr = "SHOW VARIABLES LIKE 'version';"
        if self.args['dryrun']:
            print("on", dbhost, "would run", querystr)
            return
        self.log.info("on %s running %s", dbhost, querystr)

        try:
            dbcursor.execute(querystr)
            results = dbcursor.fetchall()
        except MySQLdb.Error as ex:
            print("Failed to retrieve mysql version, %s %s", ex.args[0], ex.args[1])

        # format:     (('version', '10.2.17-MariaDB-log'),)
        print("dbhost:", dbhost, "version:", results[0][1])

    def check_tables(self, wiki, dbcursor):
        '''
        collect and return output for show create table for:
        page, revision, text, comment (if there), actor (if there)
        '''
        if self.dbinfo.do_use_wiki(dbcursor, wiki, lost_conn_ok=True) is None:
            return []
        tables = {}
        for table in self.args['tables']:
            tables[table] = self.get_show_create_table(wiki, table, dbcursor)
        return tables

    def table_exists(self, wiki, table, dbcursor):
        '''
        check if table exists in wikidb, return True if
        so, False otherwise
        '''
        querystr = "SHOW TABLES LIKE '{table}'".format(table=table)
        if self.args['dryrun']:
            print("for wiki", wiki, "would run", querystr)
            return []
        self.log.info("for wiki %s running %s", wiki, querystr)
        try:
            dbcursor.execute(querystr)
            result = dbcursor.fetchall()
        except MySQLdb.Error as ex:
            # treat all errors as nonexistence so that
            # the caller can continue processing info
            # on other tables or from other hosts
            self.log.warning("exception checking table existence %s: %s %s",
                             table, ex.args[0], ex.args[1])
            return None
        return bool(result)

    def get_show_create_table(self, wiki, table, dbcursor):
        '''
        do a show create table for the specified table on the specified
        host, return the results
        '''
        # check if table exists first, or be able to catch the exception...
        if not self.table_exists(wiki, table, dbcursor):
            return []
        querystr = "SHOW CREATE TABLE {table};".format(table=table)
        if self.args['dryrun']:
            print("for wiki", wiki, "would run", querystr)
            return []
        self.log.info("for wiki %s running %s", wiki, querystr)

        # be nice to the servers
        time.sleep(0.05)

        try:
            dbcursor.execute(querystr)
            results = dbcursor.fetchall()
        except MySQLdb.Error as ex:
            # return something empty so that the caller can continue
            # processing info on other tables or from other hosts
            self.log.warning("exception checking table structure %s: %s %s",
                             table, ex.args[0], ex.args[1])
            return {}

        return self.format_create_table_info(results)

    def show_tables_for_wikis(self, wikilist, main_master, main_wiki):
        '''
        for all wikis in our list to do, get table information
        for them on each db server that hosts them, and display
        the differences in the structure on the replicas as
        compared to the master
        '''
        results = {}
        for dbhost in self.dbinfo.args['dbhosts']:
            wikis_todo = self.dbinfo.get_wikis_for_dbhost(dbhost, wikilist)
            if not wikis_todo:
                continue

            if dbhost not in results:
                results[dbhost] = {}
            if self.args['dryrun']:
                dbcursor = None
            else:
                dbcursor, _unused = self.dbinfo.get_cursor(
                    dbhost, set_domain=True, warn_on_err=True)
                if not dbcursor:
                    # problem host, move on
                    continue
            self.check_version(dbcursor, dbhost)
            self.log.info("for dbhost %s checking wikis %s", dbhost, ','.join(wikis_todo))
            for wiki in wikis_todo:
                results[dbhost][wiki] = self.check_tables(wiki, dbcursor)
            if not self.args['dryrun']:
                dbcursor.close()
        self.tablediffs.display_diffs(results, main_master, main_wiki)


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''

    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write('\n')
    usage_message = """
Usage: python3 check_table_structures.py  --tables name[,name...]
    [--wikifile <path>|--wikilist name[,name...]]
    [--dbhosts host[,host...]]
    [--master <host>] [--main_wiki <name>]
    [--dryrun] [--verbose] [--help]

This script checks the table structure for the wikis and tables specified
across various db servers and produces a report of the differences of
replicas agains the master in each case.

It also writes out the version of mysql/mariadb for each db server.

Options:
    --dbhosts   (-H)   List of db hostnames, comma-separated
                       If such a list is provided, it will be presumed that all wikidbs
                       specified can be found on all the db hostnames given
                       Default: none, get list from db server config file
    --master    (-m)   Hostname of the single db server that is presumed to have the
                       right table structure(s), against which all other dbs will
                       be checked; if omitted, the db master for each section will
                       be used and each section configured will be reported separately
                       If this arg is set then main_wiki must also be set.
                       This host must serve the db for the main_wiki specified.
                       Default: none
    --main_wiki (-W)   Name of the wikidb against which tables on all other wikis
                       will be compared; if omitted, tables will only be compared
                       against the master for the same wiki
                       If this arg is set then master must also be set
                       Default: none
    --tables    (-t)   List of table names, comma-separated
                       Default: none
    --wikifile  (-f)   File containing a list of wiki db names, one per line
                       Default: all.dblist in current directory
    --wikilist  (-l)   List of wiki db names, comma-separated
                       Default: none, read list from file
"""
    usage_common = qargs.get_common_arg_docs(['settings'])
    usage_flags = qargs.get_common_arg_docs(['flags'])

    sys.stderr.write(usage_message + usage_common + usage_flags)
    sys.exit(1)


def get_opt(opt, val, args):
    '''
    set option value in args dict if the option
    is one of the below
    '''
    if opt in ['-H', '--dbhosts']:
        args['dbhosts'] = val.split(',')
    elif opt in ['-f', '--wikifile']:
        args['wikifile'] = val
    elif opt in ['-l', '--wikilist']:
        args['wikilist'] = val.split(',')
    elif opt in ['-m', '--master']:
        args['main_master'] = val
    elif opt in ['-W', '--main_wiki']:
        args['main_wiki'] = val
    elif opt in ['-s', '--settings']:
        args['settings'] = val
    elif opt in ['-t', '--tables']:
        args['tables'] = val.split(',')
    else:
        return False
    return True


def check_dependent_opts(args):
    '''
    check for opts that depend on other opts and whine about them
    if needed
    '''
    count = 0
    if args['main_wiki'] is not None:
        count += 1
    if args['main_master'] is not None:
        count += 1
    if count == 1:
        usage("--master and --main_wiki must be provided together")


def setup_args():
    '''
    get and return a dict of args from the command line, falling
    back to config file values or to config defaults when these
    args are not passed in
    '''
    args = qargs.get_arg_defaults(['dbhosts', 'main_master', 'main_wiki'],
                                  ['dryrun', 'verbose'])

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], 'H:f:l:m:s:t:vh',
            ['dbhosts=', 'master=', 'main_wiki=',
             'settings=', 'tables=',
             'wikifile=', 'wikilist=',
             'dryrun', 'verbose', 'help'])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    args = qargs.handle_common_args(options, args, usage, remainder,
                                    ['tables', 'settings', 'wikilist'], get_opt)
    if args['wikilist'] is None:
        args['wikilist'] = get_wikis_from_file(args['wikifile'])
    check_dependent_opts(args)
    return args


def get_wikis_from_file(filename):
    '''
    read and return list of wiki db names from file
    one entry per line, no blank lines or comments allowed
    '''
    if not filename:
        usage("No filename provided with list of wikis to process")
    if not os.path.exists(filename):
        usage("No such file {filename} with list of wikis to process".format(
            filename=filename))
    wikis = open(filename).read().splitlines()
    return wikis


def do_main():
    '''
    entry point
    '''
    args = setup_args()

    # even if this is set in the config file for use by other scripts, we want it off
    args['mwhost'] = None

    dbinfo = qdbinfo.DbInfo(args)
    tableinfo = TableInfo(args, dbinfo)
    tableinfo.show_tables_for_wikis(args['wikilist'], args['main_master'], args['main_wiki'])


if __name__ == '__main__':
    do_main()
