# -*- coding: utf-8 -*-
"""
manage db server config info
"""


import logging
import sys
from subprocess import Popen, PIPE
import json
from collections import OrderedDict
import MySQLdb
import queries.logger as qlogger
import queries.utils as qutils


class DbInfo():
    '''
    which db servers handle which wikis,
    database user credentials, etc
    are managed here
    '''
    def __init__(self, args):
        qlogger.logging_setup(args['logfile'])
        if args['verbose'] or args['dryrun']:
            log_type = 'verbose'
        else:
            log_type = 'normal'
        self.log = logging.getLogger(log_type)    # pylint: disable=invalid-name
        self.args = args
        self.args['dbhosts'], self.wikis_to_sections, self.dbhosts_by_section = self.setup_dbhosts()
        self.dbcreds = self.get_dbcreds()

    def get_dbcreds(self, wikidb=None):
        '''
        looking for values for wgDBuser, wgDBpassword (no need for us to
        have a privileged user for this work) by running a mw maintenance script
        '''
        if not wikidb:
            wikidb = self.args['wikilist'][0]
        pull_vars = ["wgDBuser", "wgDBpassword"]
        phpscript = 'getConfiguration.php'
        mwscript, maint_script_path = qutils.get_maint_script_path(self.args, phpscript)
        subcommand = [maint_script_path]
        subcommand.extend(["--wiki={dbname}".format(dbname=wikidb),
                           '--format=json', '--regex={vars}'.format(vars="|".join(pull_vars))])
        command = qutils.build_command(
            subcommand, ssh_host=self.args['mwhost'], sudo_user=self.args['sudouser'],
            mwscript=mwscript, php=self.args['php'])

        if self.args['dryrun']:
            self.log.info("would run %s", " ".join(command))
            return {'wgDBuser': 'XXXXX', 'wgDBpassword': 'XXXXX'}
        self.log.info("running command: %s", command)
        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        if error:
            sys.stderr.write("got db creds: {creds}\n".format(creds=output.decode('utf-8')))
            self.log.error("Errors encountered: %s", error.decode('utf-8'))
            sys.exit(1)
        creds = json.loads(output.decode('utf-8'))
        if 'wgDBuser' not in creds or not creds['wgDBuser']:
            sys.stderr.write("got db creds: {creds}\n".format(creds=output.decode('utf-8')))
            raise ValueError("Missing value for wgDBuser, bad dbcreds file?")
        if 'wgDBpassword' not in creds or not creds['wgDBpassword']:
            sys.stderr.write("got db creds: {creds}\n".format(creds=output.decode('utf-8')))
            raise ValueError("Missing value for wgDBpassword, bad dbcreds file?")
        return creds

    def setup_dbhosts(self):
        '''
        try to get and save dbhosts from wgLBFactoryConf stuff
        if we don't have an explicit list passed in
        '''
        wikidb = self.args['wikilist'][0]
        if 'dbhosts' in self.args:
            dbhosts = self.args['dbhosts']
        else:
            dbhosts = None
        wikis_to_sections = {}
        dbhosts_by_section = {}
        if not dbhosts:
            dbhosts, wikis_to_sections, dbhosts_by_section = self.get_dbhosts(wikidb)
        if not dbhosts and not self.args['dryrun']:
            raise ValueError("No list of db hosts provided to process")
        return list(set(dbhosts)), wikis_to_sections, dbhosts_by_section

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
            if dbhost == list(self.dbhosts_by_section[section].keys())[0]:
                return True
            if dbhost in self.dbhosts_by_section[section]:
                listed = True
        if listed:
            return False
        return True

    def get_masters(self):
        '''
        return all dbs that are (probably) masters based on config file info
        '''
        return [dbhost for dbhost in self.args['dbhosts'] if self.is_master(dbhost)]

    def get_dbhosts(self, wikidb):
        '''
        get list of dbs and section-related info from wgLBFactoryConf stuff
        by running a mw maintenance script
        '''
        pull_var = 'wgLBFactoryConf'
        phpscript = 'getConfiguration.php'
        mwscript, maint_script_path = qutils.get_maint_script_path(self.args, phpscript)
        subcommand = [maint_script_path]
        subcommand.extend(["--wiki={dbname}".format(dbname=wikidb),
                           '--format=json', '--regex={var}'.format(var=pull_var)])
        command = qutils.build_command(
            subcommand, ssh_host=self.args['mwhost'], sudo_user=self.args['sudouser'],
            mwscript=mwscript, php=self.args['php'])
        if self.args['dryrun']:
            self.log.info("would run command: %s", " ".join(command))
            return [], {}, {}
        self.log.info("running command: %s", command)
        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        if error and not error.startswith(b'Warning'):
            # ignore stuff like "Warning: rename(/tmp/...) permission denied
            self.log.error("Errors encountered: %s", error.decode('utf-8'))
            sys.exit(1)
        if not output:
            raise IOError("Failed to retrieve db config")
        try:
            results = json.loads(output.decode('utf-8'), object_pairs_hook=OrderedDict)
        except ValueError:
            results = None
        if not results:
            sys.stderr.write("got db host info: {creds}\n".format(creds=output.decode('utf-8')))
            raise IOError(
                "Failed to get values for wgLBFactoryConf for {wiki}, got output {output}".format(
                    wiki=wikidb, output=output))
        lbfactoryconf = results['wgLBFactoryConf']
        if 'wgLBFactoryConf' not in results:
            sys.stderr.write("got db host info: {creds}\n".format(creds=output.decode('utf-8')))
            raise ValueError("missing wgLBFactoryConf")
        lbfactoryconf = results['wgLBFactoryConf']
        if 'sectionLoads' not in lbfactoryconf:
            sys.stderr.write("got db host info: {creds}\n".format(creds=output.decode('utf-8')))
            raise ValueError("missing sectionLoads from wgLBFactoryConf")
        if 'sectionsByDB' not in lbfactoryconf:
            sys.stderr.write("got db host info: {creds}\n".format(creds=output.decode('utf-8')))
            raise ValueError("missing sectionsByDB from wgLBFactoryConf")
        dbs = []
        for section in lbfactoryconf['sectionLoads']:
            if section.startswith('s') or section == 'DEFAULT':
                # only process these, anything else can be skipped
                dbs.extend(list(lbfactoryconf['sectionLoads'][section]))
        return(dbs, lbfactoryconf['sectionsByDB'],
               lbfactoryconf['sectionLoads'])

    def get_dbhosts_for_wiki(self, wiki):
        '''
        get list of db servers that have a specified wiki db
        if we have no section information for the wiki and no
        info about a DEFAULT section, assume wiki is served
        by all db servers
        '''
        if not self.wikis_to_sections or not self.dbhosts_by_section:
            # assume all wikis are handled by all dbhosts
            return self.args['dbhosts']
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
        get wiki dbs served by a given db host, using db config info
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

    def get_cursor(self, dbhost, set_domain=False, warn_on_err=False):
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
        if set_domain and self.args['domain']:
            host = host + '.' + self.args['domain']
        try:
            dbconn = MySQLdb.connect(
                host=host, port=port,
                user=self.dbcreds['wgDBuser'], passwd=self.dbcreds['wgDBpassword'])
            return dbconn.cursor(), dbconn.thread_id()
        except MySQLdb.Error as ex:
            if warn_on_err:
                self.log.warning("failed to connect to or get cursor from %s:%s, %s %s",
                                 host, port, ex.args[0], ex.args[1])
                return None, None
            raise MySQLdb.Error("failed to connect to or get cursor from "
                                "{host}:{port}, {errno}:{message}".format(
                                    host=host, port=port, errno=ex.args[0], message=ex.args[1]))

    def do_use_wiki(self, cursor, wiki, lost_conn_ok=False):
        '''
        does a simple 'USE wikidbname'. That is all.
        returns True on success, False for dryrun, None for lost conn,
        raises exception on any other error
        '''
        usequery = 'USE ' + wiki + ';'
        if self.args['dryrun']:
            self.log.info("would run %s", qutils.prettyprint_query(usequery))
            return False
        self.log.info("running %s", usequery)
        try:
            cursor.execute(usequery.encode('utf-8'))
            result = cursor.fetchall()
        except MySQLdb.Error as ex:
            if lost_conn_ok and ex.args[0] == 1049:
                # this host no longer serves this wikidb, but caller wishes to handle it
                return None
            if result is not None:
                self.log.error("returned from fetchall: %s", result)
            raise MySQLdb.Error("exception for use {wiki} ({errno}:{message})".format(
                wiki=wiki, errno=ex.args[0], message=ex.args[1]))
        return True
