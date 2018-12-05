#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Retrieve table layout information from MediaWiki
database servers for various wikis and compare them

We don't use the info schema tables because we want
to be able to use a regular db user which may not
have access to them.

We sleep a tiny bit between requests because some
requests may be made on masters, and while they will
be fast, we don't want to impact other traffic.

Variables called 'table_info' are complex maps
corresponding to column, key and parameter information
of a table.

Variables called 'table_descr' are json-serialized
table descriptions derived from table info vars.
"""


import os
import sys
import getopt
import json
import copy
from collections import OrderedDict
import time
import MySQLdb
import queries.logger as qlogger
import queries.dbinfo as qdbinfo
import queries.args as qargs


class HostWikiGroups():
    '''
    methods for managing groups of hosts and/or wikis
    with the same table descriptions
    '''
    @staticmethod
    def get_grouped_hosts_one_wiki(table_descr_by_host_wiki, dbhosts, wiki):
        '''
        given table info for a list of dbhosts on the specified wiki,
        group hosts according to the results
        '''
        groups = {}
        for dbhost in dbhosts:
            if dbhost in table_descr_by_host_wiki and wiki in table_descr_by_host_wiki[dbhost]:
                if table_descr_by_host_wiki[dbhost][wiki] not in groups:
                    groups[table_descr_by_host_wiki[dbhost][wiki]] = [dbhost]
                else:
                    groups[table_descr_by_host_wiki[dbhost][wiki]].append(dbhost)
        return groups

    @staticmethod
    def group_hosts_across_wikis(table_descr_by_host_wiki, dbhosts, wikis):
        '''
        given table info for a list of dbhosts on specified wikis,
        group hosts according to the results
        a host may end up in more than one group if multiple wikis
        are specified and host output varies across wikis.
        example: host A has output outb on wiki b and output outc on wiki c
        host B has output outd on wiki b and output outc on wikic
        Then A and B will be in the same group for wiki b but a different
         group for wiki c
        '''
        groups = {}
        for wiki in wikis:
            groups[wiki] = HostWikiGroups.get_grouped_hosts_one_wiki(
                table_descr_by_host_wiki, dbhosts, wiki)
        return groups

    @staticmethod
    def get_grouped_dbhosts(table_descr_by_host_wiki, dbhosts=None, wiki=None):
        '''
        given table descriptions for some hosts for a specified wiki,
        return list of lists of hosts which have the same info
        if wiki is not specified then info for all wikis will
        be compared on the dbhosts; if dbhosts is not specified
        then all dbs in the results will be grouped if they
        have output for the specific wiki.
        if neither are specified then all will be grouped; we
        group a host according to output for each wiki, so it may
        appear in more than one group in this case.
        '''
        groups = {}
        if not dbhosts:
            dbhosts = table_descr_by_host_wiki.keys()
        if not wiki:
            wikilists = [table_descr_by_host_wiki[dbhost].keys() for dbhost in dbhosts]
            wikis = [wiki for wikis in wikilists for wiki in wikis]
            groups = HostWikiGroups.group_hosts_across_wikis(
                table_descr_by_host_wiki, dbhosts, wikis)
        else:
            groups = HostWikiGroups.group_hosts_across_wikis(
                table_descr_by_host_wiki, dbhosts, [wiki])
        return groups

    @staticmethod
    def get_groups_for_wikis(table_descr_by_wiki):
        '''
        given table descriptions for a dbhost's wikis, group together wikis that
        have the same table description and return a dict of such groups against
        the table description for each group
        '''
        groups = {}
        for wiki in table_descr_by_wiki:
            if table_descr_by_wiki[wiki] not in groups:
                groups[table_descr_by_wiki[wiki]] = [wiki]
            else:
                groups[table_descr_by_wiki[wiki]].append(wiki)
        return groups

    @staticmethod
    def get_grouped_wikis(table_descr_by_dbhost_wiki):
        '''
        given table descriptions for all dbhosts and all wikis on each
        dbhost, for each dbhost group wikis together that
        have the same table descriptions, and return a list
        of such groups per dbhost
        '''
        groups = {}
        dbhosts = table_descr_by_dbhost_wiki.keys()
        for dbhost in dbhosts:
            groups[dbhost] = HostWikiGroups.get_groups_for_wikis(table_descr_by_dbhost_wiki[dbhost])
        return groups

    @staticmethod
    def get_matching_wikis(wiki, grouped_wikis_for_dbhost):
        '''
        given the wikis grouped by their table description for a dbhost,
        return the group containing the specified wiki
        '''
        for table_descr in grouped_wikis_for_dbhost:
            if wiki in grouped_wikis_for_dbhost[table_descr]:
                return grouped_wikis_for_dbhost[table_descr]
        return []

    @staticmethod
    def get_matching_hosts(host, hosts_by_result):
        '''
        given dbhosts by result for one wiki, return the list of dbhosts that
        the specified host is in
        '''
        for table_descr in hosts_by_result:
            if host in hosts_by_result[table_descr]:
                return hosts_by_result[table_descr]
        return []


class TableDiffs():
    '''
    methods for comparing and displaying db table info
    '''
    def __init__(self, args, dbinfo):
        self.args = args
        self.dbinfo = dbinfo
        self.log = qlogger.get_logger(args)
        # these are set in display_wikidb_diff as needed
        self.wiki = None
        self.dbhost = None

    def display_table_info(self, table_info):
        '''
        given the table info, display it nicely
        '''
        for table in table_info:
            self.log.info("%stable:%s", ' ' * 3, table)
            self.log.info("%scolumns:", ' ' * 7)
            if 'columns' in table_info[table]:
                for column in table_info[table]['columns']:
                    self.log.info("%sname:%s", ' ' * 11, column)
                    self.log.info("%sproperties:%s", ' ' * 11,
                                  table_info[table]['columns'][column])
            self.log.info("%skeys:", ' ' * 7)
            if 'keys' in table_info[table]:
                for key in table_info[table]['keys']:
                    self.log.info("%sinfo:%s", ' ' * 12, key)
            self.log.info("%sparameters:", ' ' * 7)
            if 'parameters' in table_info[table]:
                for param in table_info[table]['parameters']:
                    self.log.info("%s%s", ' ' * 11, param.lstrip(') '))

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

    def display_parameter_diffs(self, master_table_info, repl_table_info, table, wiki):
        '''
        display all differences in properties in a table on master, replica
        '''
        master_params = self.params_to_dict(master_table_info['parameters'][0])
        if master_table_info['parameters'][0] != repl_table_info['parameters'][0]:
            master_params = self.params_to_dict(master_table_info['parameters'][0])
            repl_params = self.params_to_dict(repl_table_info['parameters'][0])
            master_params_missing = []
            repl_params_missing = []
            param_diffs = []
            for field in master_params:
                if field in self.args['params_ignore']:
                    continue
                if field not in repl_params:
                    master_params_missing.append(field)
                elif (master_params[field] != repl_params[field] and
                      field not in self.args['params_ignore']):
                    param_diffs.append('{field}: master {mval}, repl {rval}'.format(
                        field=field, mval=master_params[field], rval=repl_params[field]))
            for field in repl_params:
                if field in self.args['params_ignore']:
                    continue
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

    def display_key_diffs(self, master_table_info, repl_table_info, table, wiki):
        '''
        display all differences in keys in a table on master, replica
        '''
        for key in master_table_info['keys']:
            if key not in repl_table_info['keys']:
                print("table", table, "has key", key,
                      "missing from replica", self.dbhost, "wiki", wiki)
        for key in repl_table_info['keys']:
            if key not in master_table_info['keys']:
                print("table", table, "has key", key,
                      "extra on replica", self.dbhost, "wiki", wiki)

    def display_column_diffs(self, master_table_info, repl_table_info, table, wiki):
        '''
        display all column differences in a table on master, replica
        '''
        for column in master_table_info['columns']:
            if column not in repl_table_info['columns']:
                print("table", table, "has column", column,
                      "missing from replica", self.dbhost, "wiki", wiki)
        for column in repl_table_info['columns']:
            if column not in master_table_info['columns']:
                print("table", table, "has column", column,
                      "extra on replica", self.dbhost, "wiki", wiki)
        for column in master_table_info['columns']:
            if column not in repl_table_info['columns']:
                continue
        if master_table_info['columns'][column] != repl_table_info['columns'][column]:
            print("repl table", table, "column", column, "mismatch",
                  repl_table_info['columns'][column])

    def display_table_diffs(self, master_table_info, repl_table_info, table, wiki):
        '''
        display all differences between tables on master and replica
        '''
        if not master_table_info:
            if repl_table_info:
                print("table", table, "missing on master")
            return
        if not repl_table_info:
            print("table", table, "missing on replica")
            return
        self.display_column_diffs(master_table_info, repl_table_info, table, wiki)
        self.display_key_diffs(master_table_info, repl_table_info, table, wiki)
        self.display_parameter_diffs(master_table_info, repl_table_info, table, wiki)

    def display_wikidb_diff(self, table_info_by_dbhost_wiki, wiki, main_master, main_wiki):
        '''
        find and display all table differences between dbhost and master
        '''
        if (self.dbhost not in table_info_by_dbhost_wiki or
                wiki not in table_info_by_dbhost_wiki[self.dbhost]):
            print("No tables for wiki on", self.dbhost)
            return
        repl_table_info = table_info_by_dbhost_wiki[self.dbhost][wiki]
        if main_wiki:
            # we compare everything to tables of one wiki
            if (main_master not in table_info_by_dbhost_wiki or
                    main_wiki not in table_info_by_dbhost_wiki[main_master]):
                print("No diffs available: no tables for main wiki {wiki} on {host}".format(
                    wiki=main_wiki, host=main_master))
                return
            master_table_info = table_info_by_dbhost_wiki[main_master][main_wiki]
        else:
            master_table_info = table_info_by_dbhost_wiki[main_master][wiki]
        for table in master_table_info:
            if table not in repl_table_info:
                print("table", table, "missing from replica", self.dbhost, "wiki", wiki)
        for table in repl_table_info:
            if table not in master_table_info:
                print("table", table, "extra on replica", self.dbhost, "wiki", wiki)
        for table in master_table_info:
            if table not in repl_table_info:
                continue
            self.display_table_diffs(master_table_info[table],
                                     repl_table_info[table], table, wiki)

    def display_diffs_master_per_sect(self, table_info_per_dbhost_wiki, table_descr_by_host_wiki):
        '''
        for each master on a section, get the wiki table
        info on the master, compare the tables of
        that wiki on all replicas in that section
        '''
        masters = self.dbinfo.get_masters()
        for master in masters:
            wikis_todo = self.dbinfo.get_wikis_for_dbhost(master, self.args['wikilist'])
            if not wikis_todo:
                continue
            for wiki in table_info_per_dbhost_wiki[master]:
                print('master:', master)
                print('wiki:', wiki)
                self.display_table_info(table_info_per_dbhost_wiki[master][wiki])
                dbhosts_todo = self.dbinfo.get_dbhosts_for_wiki(wiki)
                grouped_dbhosts = HostWikiGroups.get_grouped_dbhosts(table_descr_by_host_wiki,
                                                                     dbhosts_todo, wiki)
                done = []
                self.dbinfo.log.info(
                    "hosts grouped by results for wiki: %s",
                    " ".join([grouped_dbhosts[wiki].values() for wiki in grouped_dbhosts]))
                for dbhost in dbhosts_todo:
                    if dbhost in done or dbhost == master:
                        continue
                    self.log.info('replica: %s', dbhost)
                    self.log.info('wiki: %s', wiki)
                    self.display_table_info(table_info_per_dbhost_wiki[dbhost][wiki])
                print('DIFFS ****')
                for dbhost in dbhosts_todo:
                    if dbhost in done or dbhost == master:
                        continue
                    same_result_hosts = HostWikiGroups.get_matching_hosts(
                        dbhost, grouped_dbhosts[wiki])
                    print("common results for hosts:", same_result_hosts)
                    done.extend(same_result_hosts)
                    self.dbhost = dbhost
                    self.display_wikidb_diff(table_info_per_dbhost_wiki, wiki, master, None)

    def display_diffs_master_all_sects(self, table_info_per_dbhost_wiki,
                                       table_descr_by_host_wiki, main_master, main_wiki):
        '''
        display table info diffs taken against one wiki on a specified
        master
        example: use db2010 enwiki table against which to compare
        the tables of all the s5 wikis on the s5 db hosts, even though
        enwiki is not in s5
        better exmple: decide enwiki on db2010 has the table layout you
        want on all wikis everywhere and compare them all against it
        '''
        # a dry run will produce this
        master_results = {}
        if main_master in table_info_per_dbhost_wiki:
            master_results = table_info_per_dbhost_wiki[main_master]
        print("all wiki tables will be checked against {db}:{wiki}".format(
            db=main_master, wiki=main_wiki))

        if master_results:
            self.display_table_info(master_results[main_wiki])

        grouped_wikis = HostWikiGroups.get_grouped_wikis(table_descr_by_host_wiki)
        wikis_done = {}
        masters = list(set(self.dbinfo.get_masters()))
        for section_master in masters:
            if section_master not in table_info_per_dbhost_wiki:
                # this db (and so the whole section) did not have any of them
                # wikis in our list to check
                continue
            grouped_dbhosts = HostWikiGroups.get_grouped_dbhosts(table_descr_by_host_wiki)
            self.log.info("db host groups by wiki results: %s",
                          [list(grouped_dbhosts[wiki].values()) for wiki in grouped_dbhosts])
            for wiki in table_info_per_dbhost_wiki[section_master]:
                dbhosts_todo = self.dbinfo.get_dbhosts_for_wiki(wiki)
                dbhosts_done = []
                for dbhost in dbhosts_todo:
                    if dbhost in dbhosts_done or dbhost == main_master:
                        continue
                    if dbhost in wikis_done and wiki in wikis_done[dbhost]:
                        continue
                    self.log.info('replica: %s', dbhost)
                    self.log.info('wiki: %s', wiki)
                    self.display_table_info(table_info_per_dbhost_wiki[dbhost][wiki])

                # we assume that if two hosts serve one wiki in common, they
                # serve the same list; this is what the MW sections config
                # guarantees us
                print('DIFFS ****', wiki)
                for dbhost in dbhosts_todo:
                    if dbhost in dbhosts_done or dbhost == main_master:
                        print("skipping", dbhost)
                        continue
                    if dbhost in wikis_done and wiki in wikis_done[dbhost]:
                        print("skipping", dbhost)
                        continue
                    same_result_hosts = HostWikiGroups.get_matching_hosts(
                        dbhost, grouped_dbhosts[wiki])
                    print("common results for hosts:", same_result_hosts)
                    same_result_wikis = HostWikiGroups.get_matching_wikis(
                        wiki, grouped_wikis[dbhost])
                    # don't display diffs for other wikis for dbhosts with the
                    # same table description
                    for same_result_host in same_result_hosts:
                        if same_result_host not in wikis_done:
                            wikis_done[same_result_host] = same_result_wikis
                        else:
                            wikis_done[same_result_host].extend(same_result_wikis)
                    # list all the wikis on the other dbs with the same table description,
                    # we won't display diffs for them either. if only the current wiki
                    # is in the list, don't bother to display that
                    if len(same_result_wikis) > 1:
                        print("wikis on these hosts with same table description:",
                              same_result_wikis)
                    dbhosts_done.extend(same_result_hosts)
                    self.dbinfo.log.warning("table description for wiki %s dbhost %s is %s",
                                            wiki, dbhost,
                                            table_descr_by_host_wiki[dbhost][wiki])
                    self.dbhost = dbhost
                    self.display_wikidb_diff(table_info_per_dbhost_wiki, wiki,
                                             main_master, main_wiki)

    def display_diffs(self, table_info_per_dbhost_wiki, table_descr_by_host_wiki,
                      main_master, main_wiki):
        '''
        show differences between table info on master and replicas for
        each wiki
        if 'main_master', is not None, then we use that as the
        sole master and compare other dbs (masters or not) across all wikis
        to it; in this case main_wiki must also be supplied and that
        wiki's tables on the sole master will be used as the standard against
        which to diff everything else
        '''
        if main_master:
            self.display_diffs_master_all_sects(table_info_per_dbhost_wiki,
                                                table_descr_by_host_wiki, main_master, main_wiki)
        else:
            self.display_diffs_master_per_sect(table_info_per_dbhost_wiki,
                                               table_descr_by_host_wiki)


class TableGetter():
    '''
    methods for retrieving db table layout info
    '''
    def __init__(self, args, dbinfo):
        self.args = args
        self.dbinfo = dbinfo
        self.log = qlogger.get_logger(args)
        self.tablediffs = TableDiffs(self.args, self.dbinfo)

    @staticmethod
    def flatten_one_wiki(table_info, ignores):
        '''
        flatten dict with info about one wiki's tables using
        alpha ordering so we can compare to other hosts and wikis
        '''
        table_info_copy = copy.deepcopy(table_info)
        for table in table_info_copy:
            if 'parameters' in table_info_copy[table]:
                table_info_copy[table]['parameters'] = TableDiffs.params_to_dict(
                    table_info_copy[table]['parameters'][0])
                # get rid of junk that will vary or is spurious
                for ignore in ignores:
                    table_info_copy[table]['parameters'].pop(ignore, None)
        return json.dumps(table_info_copy, sort_keys=True)

    @staticmethod
    def flatten_table_info(table_info, ignores):
        '''
        given the formatted (complex dict) version of table info
        for all the dbhosts on all the wikis, convert the info
        for each wiki on each db to a string with predictable ordering
        so that we can tell if two hosts are set up identically;
        this can be used to group hosts that have the same set of
        differences from the master
        '''
        flattened = {}
        for dbhost in table_info:
            flattened[dbhost] = {}
            for wiki in table_info[dbhost]:
                flattened[dbhost][wiki] = TableGetter.flatten_one_wiki(
                    table_info[dbhost][wiki], ignores)
        return flattened

    @staticmethod
    def format_create_table_info(sql_results, log):
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
            log.error("CREATE_TABLE returned unexpected result", sql_results)
            return {}
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

    def check_version(self, dbhost, dbcursor):
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
            print("dbhost: ", dbhost, "Failed to retrieve mysql version, %s %s",
                  ex.args[0], ex.args[1])
            return

        # format:     (('version', '10.2.17-MariaDB-log'),)
        print("dbhost:", dbhost, "version:", results[0][1])

    def check_tables(self, wiki, dbcursor):
        '''
        collect and return output for show create table for:
        page, revision, text, comment (if there), actor (if there)
        '''
        if self.dbinfo.do_use_wiki(wiki, dbcursor, lost_conn_ok=True) is None:
            return {}
        tables = {}
        for table in self.args['tables']:
            tables[table] = self.get_show_create_table(wiki, table, dbcursor)
        return tables

    def run_table_query(self, wiki, dbcursor, query):
        '''
        run some query for a table, returning None on error or dryrun
        or the query results otherwise
        '''
        if self.args['dryrun']:
            print("for wiki", wiki, "would run", query)
            return None
        self.log.info("for wiki %s running %s", wiki, query)
        try:
            dbcursor.execute(query)
            result = dbcursor.fetchall()
        except MySQLdb.Error as ex:
            # treat all errors as nonexistence so that
            # the caller can continue processing info
            # on other tables or from other hosts
            self.log.warning("exception running query %s: %s %s",
                             query, ex.args[0], ex.args[1])
            return None
        return result

    def table_exists(self, wiki, table, dbcursor):
        '''
        check if table exists in wikidb, return True if
        so, False otherwise
        '''
        querystr = "SHOW TABLES LIKE '{table}'".format(table=table)
        result = self.run_table_query(wiki, dbcursor, querystr)
        if not result:
            return None
        return bool(result)

    def get_show_create_table(self, wiki, table, dbcursor):
        '''
        do a show create table for the specified table on the specified
        host, return the results
        '''
        # check if table exists first, or be able to catch the exception...
        if not self.table_exists(wiki, table, dbcursor):
            return {}
        querystr = "SHOW CREATE TABLE {table};".format(table=table)
        # be nice to the servers
        time.sleep(0.05)
        results = self.run_table_query(wiki, dbcursor, querystr)
        if not results:
            return {}
        return self.format_create_table_info(results, self.log)

    def show_tables_for_wikis(self, wikilist, main_master, main_wiki):
        '''
        for all wikis in our list to do, get table information
        for them on each db server that hosts them, and display
        the differences in the table layout on the replicas as
        compared to the master
        '''
        table_info_per_dbhost_wiki = {}
        for dbhost in self.args['dbhosts']:
            wikis_todo = self.dbinfo.get_wikis_for_dbhost(dbhost, wikilist)
            if not wikis_todo:
                continue

            if dbhost not in table_info_per_dbhost_wiki:
                table_info_per_dbhost_wiki[dbhost] = {}
            if self.args['dryrun']:
                dbcursor = None
            else:
                dbcursor, _unused = self.dbinfo.get_cursor(
                    dbhost, set_domain=True, warn_on_err=True)
                if not dbcursor:
                    # problem host, move on
                    continue
            self.check_version(dbhost, dbcursor)
            self.log.info("for dbhost %s checking wikis %s", dbhost, ','.join(wikis_todo))
            for wiki in wikis_todo:
                # for hacky testing situations (reusing the same host
                # in several section configs) this can happen
                if wiki not in table_info_per_dbhost_wiki[dbhost]:
                    table_info_per_dbhost_wiki[dbhost][wiki] = self.check_tables(wiki, dbcursor)
            if not self.args['dryrun']:
                dbcursor.close()
        descriptions = self.flatten_table_info(table_info_per_dbhost_wiki,
                                               self.args['params_ignore'])
        self.tablediffs.display_diffs(table_info_per_dbhost_wiki, descriptions,
                                      main_master, main_wiki)


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

This script checks the table layout for the wikis and tables specified
across various db servers and produces a report of the differences of
replicas againts the master in each case.

It also writes out the version of mysql/mariadb for each db server.

"""
    usage_args = """
Options:
    --dbhosts   (-H)   List of db hostnames, comma-separated
                       If such a list is provided, it will be presumed that all wikidbs
                       specified can be found on all the db hostnames given
                       Default: none, get list from db server config file
    --master    (-m)   Hostname of the single db server that is presumed to have the
                       right table layout(s), against which all other dbs will
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
    usage_output = qargs.get_common_arg_docs(['output'])
    usage_common = qargs.get_common_arg_docs(['settings'])
    usage_flags = qargs.get_common_arg_docs(['flags'])

    sys.stderr.write(usage_message + usage_output + usage_args + usage_common + usage_flags)
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
    tableinfo = TableGetter(args, dbinfo)
    tableinfo.show_tables_for_wikis(args['wikilist'], args['main_master'], args['main_wiki'])


if __name__ == '__main__':
    do_main()
