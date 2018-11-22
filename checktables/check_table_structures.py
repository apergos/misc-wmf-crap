# -*- coding: utf-8 -*-
"""
Retrieve table structure information from MediaWiki
database servers for various wikis and compare them
"""


from __future__ import print_function
import os
import sys
import getopt
import json
import logging
import logging.config
from subprocess import Popen, PIPE
from collections import OrderedDict
import time
import ConfigParser
import MySQLdb


class ConfigReader(object):
    '''
    read stuff that would otherwise be command line args
    from a config file

    command line values override config file values which
    override built-in defaults (now set in config structure)
    '''
    SETTINGS = ['dbauth', 'dbconfig', 'domain', 'tables', 'wikifile', 'wikilist', 'php']

    def __init__(self, configfile):
        defaults = self.get_config_defaults()
        self.conf = ConfigParser.SafeConfigParser(defaults)
        if configfile is not None:
            self.conf.read(configfile)
            if not self.conf.has_section('settings'):
                # you don't have to have a config file, but if you do,
                # it needs to have the right stuff in it at least
                sys.stderr.write("The mandatory configuration section "
                                 "'settings' was not defined.\n")
                raise ConfigParser.NoSectionError('settings')

    @staticmethod
    def get_config_defaults():
        '''
        get and return default config settings for this crapola
        '''
        return {
            'dbauth': '',
            'dbconfig': '',
            'tables': '',
            'wikifile': 'all.dblist',
            'wikilist': '',
            'php': '/usr/bin/php',
            'domain': '',
        }

    def parse_config(self):
        '''
        grab values from configuration and assign them to appropriate variables
        '''
        args = {}
        # could be true if we re only using the defaults
        if not self.conf.has_section('settings'):
            self.conf.add_section('settings')
        for setting in self.SETTINGS:
            args[setting] = self.conf.get('settings', setting)
        for setting in ['wikilist', 'tables']:
            if args[setting]:
                args[setting] = args[setting].split(',')
        return args


class DbInfo(object):
    '''
    which db servers handle which wikis,
    database user credentials, etc
    are managed here
    '''
    def __init__(self, dbhosts, domain, dryrun, verbose):
        self.dbhosts = dbhosts
        self.domain = domain
        self.dryrun = dryrun
        self.dbuser = None
        self.dbpasswd = None
        self.wikis_to_sections = None
        self.dbhosts_by_section = None
        if verbose:
            log_type = 'verbose'
        else:
            log_type = 'normal'
        self.log = logging.getLogger(log_type)    # pylint: disable=invalid-name

    def get_dbcreds(self, php, dbcredsfile):
        '''
        looking for values for wgDBuser, wgDBpassword (no need for us to
        have a privileged user for this work)
        we'll run a php script that sources the specified file and writes
        out the value for those variables in json so we can load them
        up and return them
        '''
        command = [php, 'display_wgdbcreds.php', dbcredsfile]
        if self.dryrun:
            print("would run command:", command)
            return 'wikiadmin', 'fakepwd'
        self.log.info("running command: %s", ' '.join(command))
        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        if error:
            print("Errors encountered:", error)
            sys.exit(1)
        self.log.info("got db creds: %s", output)
        creds = json.loads(output)
        if 'wgDBuser' not in creds or not creds['wgDBuser']:
            raise ValueError("Missing value for wgDBuser, bad dbcreds file?")
        if 'wgDBpassword' not in creds or not creds['wgDBpassword']:
            raise ValueError("Missing value for wgDBpassword, bad dbcreds file?")
        self.dbuser = creds['wgDBuser']
        self.dbpasswd = creds['wgDBpassword']

    def setup_dbhosts(self, php, dbconfigfile):
        '''
        try to get and save dbhosts from wgLBFactoryConf stuff
        if we don't have an explicit list passed in
        '''
        if self.dbhosts is None:
            self.dbhosts, self.wikis_to_sections, self.dbhosts_by_section = (
                self.get_dbhosts_from_file(php, dbconfigfile))
        if self.dbhosts is None:
            raise ValueError("No list of db hosts provided to process")
        self.dbhosts = list(set(self.dbhosts))

    def is_master(self, dbhost):
        '''
        check if a dbhost is a master for some section; if no section info
        is available, assume the answer is yes
        this assumes no host is a master on one section and a replica on others
        '''
        listed = False
        if not self.dbhosts_by_section:
            return True
        for section in self.dbhosts_by_section:
            # master is in 'slot 0'
            if dbhost == self.dbhosts_by_section[section].keys()[0]:
                return True
            elif dbhost in self.dbhosts_by_section[section]:
                listed = True
        if listed:
            return False
        return True

    def get_masters(self):
        '''
        return all dbs that are (probably) masters based on config file info
        '''
        return [dbhost for dbhost in self.dbhosts if self.is_master(dbhost)]

    def get_dbhosts_from_file(self, php, dbconfigfile):
        '''
        get list of dbs and section-related info from wgLBFactoryConf stuff
        in a file
        '''
        if not dbconfigfile:
            raise ValueError("No db config filename provided")
        if not os.path.exists(dbconfigfile):
            raise ValueError("No such file {filename}".format(filename=dbconfigfile))
        wglbfactoryconf = self.get_wglbfactoryconf(php, dbconfigfile)
        if 'sectionLoads' not in wglbfactoryconf:
            raise ValueError("missing sectionLoads from wgLBFactoryConf, bad config?")
        if 'sectionsByDB' not in wglbfactoryconf:
            raise ValueError("missing sectionsByDB from wgLBFactoryConf, bad config?")
        dbs = []
        for section in wglbfactoryconf['sectionLoads']:
            if section.startswith('s') or section == 'DEFAULT':
                # only process these, anything else can be skipped
                dbs.extend(list(wglbfactoryconf['sectionLoads'][section]))
        return dbs, wglbfactoryconf['sectionsByDB'], wglbfactoryconf['sectionLoads']

    def get_wglbfactoryconf(self, php, dbconfigfile):
        '''
        run a php script to source the dbconfig file and write the
        contents as json, which we can read and convert to a python
        dict. yuck.
        '''
        command = [php, 'display_wgLBFactoryConf.php', dbconfigfile]
        self.log.info("running command: %s", ' '.join(command))
        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        if error:
            print("Errors encountered:", error)
            sys.exit(1)
        return json.loads(output, object_pairs_hook=OrderedDict)

    def get_dbhosts_for_wiki(self, wiki):
        '''
        get list of db servers that have a specified wiki db
        if we have no section information for the wiki and no
        info about a DEFAULT section, assume wiki is served
        by all db servers
        '''
        if not self.wikis_to_sections or not self.dbhosts_by_section:
            # assume all wikis are handled by all dbhosts
            return self.dbhosts
        if wiki not in self.wikis_to_sections:
            section = 'DEFAULT'
        else:
            section = self.wikis_to_sections[wiki]
        if section not in self.dbhosts_by_section:
            return None
        return list(self.dbhosts_by_section[section])

    def get_wikis_for_section(self, section, wikilist):
        '''
        get list of wikis from given list of wikis that are
        in a specified section
        '''
        if section == 'DEFAULT':
            # there will be no list, return everything not named in wiki-section mapping
            return [wiki for wiki in wikilist if wiki not in self.wikis_to_sections]
        # section should have wikis in the mapping, return those
        return [wiki for wiki in wikilist if wiki in self.wikis_to_sections and
                self.wikis_to_sections[wiki] == section]

    def get_wikis_for_dbhost(self, dbhost, wikilist):
        '''
        get wiki dbs served by a given db host, usign db config info
        previously read from a file, or if no such information is available
        for the given host, assume all wikis are hosted by it
        '''
        if not self.wikis_to_sections or not self.dbhosts_by_section:
            # assume all db hosts handle all wikis
            return wikilist
        wikis = []
        for section in self.dbhosts_by_section:
            if dbhost in self.dbhosts_by_section[section]:
                wikis.extend(self.get_wikis_for_section(section, wikilist))
        return list(set(wikis))

    def get_cursor(self, dbhost):
        '''
        split the db host string into hostname and port if necessary,
        open a connection, get and return a cursor
        '''
        if ':' in dbhost:
            fields = dbhost.split(':')
            host = fields[0]
            port = int(fields[1])
        else:
            host = dbhost
            port = 3306
        if self.domain:
            host = host + '.' + self.domain
        try:
            dbconn = MySQLdb.connect(
                host=host, port=port,
                user=self.dbuser, passwd=self.dbpasswd)
            return dbconn.cursor()
        except MySQLdb.Error as ex:
            self.log.warning("failed to connect to or get cursor from %s:%s, %s %s",
                             host, port, ex[0], ex[1])
            return None


class TableDiffs(object):
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

    def display_diffs_master_per_section(self, results):
        '''
        for each master on a section, get the wiki table
        structure on the master, compare the table structure
        of that wiki on all replicas in that section
        '''
        masters = self.dbinfo.get_masters()
        for master in masters:
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

    def display_diffs_master_all_sections(self, results, main_master, main_wiki):
        '''
        display table structure diffs taken against one wiki on a specified
        master
        example: use db2010 enwiki table structure against which to compare
        the table structure of all the s5 wikis on the s5 db hosts, even though
        enwiki is not in s5
        better exmple: decide enwiki on db2010 has the table structure you
        want on all wikis everywhere and compare them all against it
        '''
        master_results = results[main_master]
        masters = self.dbinfo.get_masters()
        print("all wiki tables will be checked against {db}:{wiki}".format(
            db=main_master, wiki=main_wiki))

        if self.verbose:
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
            self.display_diffs_master_all_sections(results, main_master, main_wiki)
        else:
            self.display_diffs_master_per_section(results)


class TableInfo(object):
    '''
    methods for retrieving db table structure info
    '''
    def __init__(self, dbinfo, tables, dryrun, verbose):
        self.dbinfo = dbinfo
        self.tables = tables
        self.dryrun = dryrun
        if verbose:
            log_type = 'verbose'
        else:
            log_type = 'normal'
        self.log = logging.getLogger(log_type)    # pylint: disable=invalid-name
        self.tablediffs = TableDiffs(self.dbinfo, verbose)

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
        if self.dryrun:
            print("on", dbhost, "would run", querystr)
            return []
        self.log.info("on %s running %s", dbhost, querystr)

        try:
            dbcursor.execute(querystr)
            results = dbcursor.fetchall()
        except MySQLdb.Error as ex:
            print("Failed to retrieve mysql version, %s %s", ex[0], ex[1])
            return

        # format:     (('version', '10.2.17-MariaDB-log'),)
        print("dbhost:", dbhost, "version:", results[0][1])

    def check_tables(self, wiki, dbcursor):
        '''
        collect and return output for show create table for:
        page, revision, text, comment (if there), actor (if there)
        '''
        if self.set_db(wiki, dbcursor) is None:
            return []
        tables = {}
        for table in self.tables:
            tables[table] = self.get_show_create_table(wiki, table, dbcursor)
        return tables

    def set_db(self, wiki, dbcursor):
        '''
        have to set which wiki db we are using before we start asking
        for any other info
        '''
        querystr = "USE {db};".format(db=wiki)
        if self.dryrun:
            print("for wiki", wiki, "would run", querystr)
            return False
        self.log.info("for wiki %s running %s", wiki, querystr)

        try:
            dbcursor.execute(querystr)
            dbcursor.fetchall()
        except MySQLdb.OperationalError as ex:
            if ex[0] == 1049:
                # this host no longer serves this wikidb
                return None
            raise
        return True

    def table_exists(self, wiki, table, dbcursor):
        '''
        check if table exists in wikidb, return True if
        so, False otherwise
        '''
        querystr = "SHOW TABLES LIKE '{table}'".format(table=table)
        if self.dryrun:
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
                             table, ex[0], ex[1])
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
        if self.dryrun:
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
                             table, ex[0], ex[1])
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
        for dbhost in self.dbinfo.dbhosts:
            wikis_todo = self.dbinfo.get_wikis_for_dbhost(dbhost, wikilist)
            if not wikis_todo:
                continue

            if dbhost not in results:
                results[dbhost] = {}
            if self.dryrun:
                dbcursor = None
            else:
                dbcursor = self.dbinfo.get_cursor(dbhost)
                if not dbcursor:
                    # problem host, move on
                    continue
            self.check_version(dbcursor, dbhost)
            self.log.info("for dbhost %s checking wikis %s", dbhost, ','.join(wikis_todo))
            for wiki in wikis_todo:
                results[dbhost][wiki] = self.check_tables(wiki, dbcursor)
            if not self.dryrun:
                dbcursor.close()
        self.tablediffs.display_diffs(results, main_master, main_wiki)


class OptSetup(object):
    '''
    methods for getting command line opts, setting defaults, checking resulting values
    '''
    def __init__(self):
        self.args = self.get_opts()

    @staticmethod
    def usage(message=None):
        '''
        display a helpful usage message with
        an optional introductory message first
        '''

        if message is not None:
            sys.stderr.write(message)
            sys.stderr.write('\n')
        usage_message = """
Usage: check_table_structures.py  --dbauth <path> --tables name[,name...]
    [--wikifile <path>|--wikilist name[,name...]]
    [--dbconfig <path>|--dbhosts host[,host...]]
    [--master <host>] [--main_wiki <name>]
    [--php <path>]
    [--dryrun] [--verbose] [--help]

This script checks the table structure for the wikis and tables specified
across various db servers and produces a report of the differences of
replicas agains the master in each case.

It also writes out the version of mysql/mariadb for each db server.

Options:
    --dbauth    (-a)   File with db user name and password
                       default: none
    --dbconfig  (-c)   Config file with db hostnames per section such as db-eqiad.php
                       default: none
    --dbhosts   (-h)   List of db hostnames, comma-separated
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
    --settings  (-s)   File with global settings which may include:
                       dbauth, dbconfig, wikifile, wikilist, php, domain
                       Default: none
    --tables    (-t)   List of table names, comma-separated
                       Default: none
    --php       (-p)   path to php command, used for grabbing db creds and possibly
                       list of db servers from php files
                       Default: /usr/bin/php
    --wikifile  (-f)   File containing a list of wiki db names, one per line
                       Default: all.dblist in current directory
    --wikilist  (-l)   List of wiki db names, comma-separated
                       Default: none, read list from file

Flags:
    --dryrun    (-d)   Don't execute queries but show what would be done
    --help      (-h)   show this message
"""
        sys.stderr.write(usage_message)
        sys.exit(1)

    @staticmethod
    def get_opt(opt, val, args):
        '''
        set option value in args dict if the option
        is one of the below
        '''
        if opt in ['-a', '--dbauth']:
            args['dbauth'] = val
        elif opt in ['-c', '--dbconfig']:
            args['dbconfig'] = val
        elif opt in ['-h', '--dbhosts']:
            args['dbhosts'] = val
        elif opt in ['-f', '--wikifile']:
            args['wikifile'] = val
        elif opt in ['-l', '--wikilist']:
            args['wikilist'] = val.split(',')
        elif opt in ['-m', '--master']:
            args['main_master'] = val
        elif opt in ['-W', '--main_wiki']:
            args['main_wiki'] = val
        elif opt in ['-p', '--php']:
            args['php'] = val
        elif opt in ['-s', '--settings']:
            args['settings'] = val
        elif opt in ['-t', '--tables']:
            args['tables'] = val.split(',')
        else:
            return False
        return True

    def get_flag(self, opt, args):
        '''
        set boolean flag in args dict if the flag is
        one of the below
        '''
        if opt in ['-d', '--dryrun']:
            args['dryrun'] = True
        elif opt in ['-v', '--verbose']:
            args['verbose'] = True
        elif opt in ['-h', '--help']:
            self.usage("Help for this script\n")
        else:
            return False
        return True

    def check_opts(self, args):
        '''
        check for missing opts and whine about them
        '''
        if not args['dbauth']:
            self.usage("Mandatory argument --dbauth not "
                       "specified on command line or in config file")
        if not args['tables']:
            self.usage("Mandatory argument --tables not "
                       "specified on command line or in config file")
        if args['wikilist'] is None:
            self.usage("No list of wikis provided to process")
        count = 0
        if args['main_wiki'] is not None:
            count += 1
        if args['main_master'] is not None:
            count += 1
        if count == 1:
            self.usage("--master and --main_wiki must be provided together")
        if args['wikilist'] is None:
            self.usage("No list of wikis provided to process")

    def get_opts(self):
        '''
        get and return a dict of args from the command line, falling
        back to config file values or to config defaults when these
        args are not passed in
        '''
        args = {}
        args['dbhosts'] = None
        args['main_master'] = None
        args['main_wiki'] = None
        args['dryrun'] = False
        args['verbose'] = False

        try:
            (options, remainder) = getopt.gnu_getopt(
                sys.argv[1:], 'a:c:h:f:l:m:p:s:t:vh',
                ['dbauth=', 'dbconfig=', 'dbhosts=', 'master=', 'main_wiki=',
                 'php=', 'settings=', 'tables=',
                 'wikifile=', 'wikilist=',
                 'dryrun', 'verbose', 'help'])
        except getopt.GetoptError as err:
            self.usage("Unknown option specified: " + str(err))

        for (opt, val) in options:
            if not self.get_opt(opt, val, args):
                if not self.get_flag(opt, args):
                    self.usage("Unknown option specified: <%s>" % opt)

        if remainder:
            self.usage("Unknown option specified: <%s>" % remainder)

        if 'settings' in args:
            globalconfigfile = args['settings']
        else:
            globalconfigfile = None
        conf = ConfigReader(globalconfigfile)
        conf_settings = conf.parse_config()
        # merge conf_settings into args, where conf_settings values are used for fallback
        for setting in ConfigReader.SETTINGS:
            if setting not in args:
                args[setting] = conf_settings[setting]

        if args['wikilist'] is None:
            args['wikilist'] = self.get_wikis_from_file(args['wikifile'])
        self.check_opts(args)
        return args

    def get_wikis_from_file(self, filename):
        '''
        read and return list of wiki db names from file
        one entry per line, no blank lines or comments allowed
        '''
        if not filename:
            self.usage("No filename provided with list of wikis to process")
        if not os.path.exists(filename):
            self.usage("No such file {filename} with list of wikis to process".format(
                filename=filename))
        wikis = open(filename).read().splitlines()
        return wikis


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
    setup = OptSetup()
    args = setup.args

    dbinfo = DbInfo(args['dbhosts'], args['domain'], args['dryrun'], args['verbose'])
    dbinfo.get_dbcreds(args['php'], args['dbauth'])
    dbinfo.setup_dbhosts(args['php'], args['dbconfig'])
    tableinfo = TableInfo(dbinfo, args['tables'], args['dryrun'], args['verbose'])
    tableinfo.show_tables_for_wikis(args['wikilist'], args['main_master'], args['main_wiki'])


if __name__ == '__main__':
    do_main()
