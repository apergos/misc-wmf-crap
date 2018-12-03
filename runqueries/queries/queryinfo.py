# -*- coding: utf-8 -*-
"""
Deal with query templates, running queries on
some wikis, etc
"""


import logging
import logging.config
import re
import warnings
import MySQLdb
import queries.dbinfo as qdbinfo
import queries.logger as qlogger
import queries.utils as qutils


class QueryInfo():
    '''
    munge and run queries on db servers for specific wikis
    '''

    def __init__(self, args):
        self.args = args
        qlogger.logging_setup(args['logfile'])
        if self.args['verbose'] or self.args['dryrun']:
            log_type = 'verbose'
        else:
            log_type = 'normal'
        self.log = logging.getLogger(log_type)    # pylint: disable=invalid-name
        warnings.filterwarnings("ignore", category=MySQLdb.Warning)
        self.settings = qutils.get_settings_from_yaml(self.args['yamlfile'])
        self.queries = qutils.get_queries_from_file(self.args['queryfile'])
        # choose the first wiki we find in the yaml file, for db creds.
        # yes, this means all your wikis better have the same credentials
        wikidb = self.get_first_wiki()
        self.dbinfo = qdbinfo.DbInfo(args)
        self.dbcreds = self.dbinfo.get_dbcreds(wikidb)

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

    # Implement in subclass!
    def run_on_wiki(self, cursor, host, wiki, wiki_settings):
        '''
        run all queries for a specific wiki, after filling in the
        query template; this assumes an initialized db cursor
        is passed in
        '''
        raise MySQLdb.Error("Unimplemented class run_on_wiki")

    def run_on_server(self, host, wikis_info, keep_cursor):
        '''
        run queries on all wikis for specified server, after
        filling in the query template
        '''
        cursor = None
        if keep_cursor and not self.args['dryrun']:
            cursor, _unused = self.dbinfo.get_cursor(host)
        qutils.print_and_log(self.log, "*** HOST: {host}".format(host=host))
        for wiki in wikis_info:
            self.run_on_wiki(cursor, host, wiki, wikis_info[wiki])
        if keep_cursor and not self.args['dryrun']:
            cursor.close()

    def run(self, keep_cursor):
        '''
        run all queries on all wikis for each host, with variables in
        the query template filled in appropriately
        '''
        for section in self.settings['servers']:
            qutils.print_and_log(self.log, "*** SECTION: {section}".format(section=section))
            for host in self.settings['servers'][section]['hosts']:
                self.run_on_server(host, self.settings['servers'][section]['wikis'], keep_cursor)
