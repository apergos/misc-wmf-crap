# -*- coding: utf-8 -*-
"""
read and process config file settings
"""


import configparser
import sys


SETTINGS = ['domain', 'dumpsdir', 'dumpshost', 'logfile', 'multiversion', 'mwhost',
            'mwrepo', 'params_ignore', 'php', 'sudouser', 'tables', 'wikifile', 'wikilist']


def config_setup(configfile):
    '''
    return a dict of config settings and their (possibly empty but not None) values
    '''
    defaults = get_config_defaults()
    conf = configparser.ConfigParser(defaults)
    if configfile is None:
        settings = defaults
    else:
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
        'dumpshost': '',
        'dumpspath': '/dumps',
        'logfile': 'sql_checker_errors.log',
        'multiversion': '',
        'mwhost': '',
        'mwrepo': '/srv/mediawiki',
        'params_ignore': 'AUTO_INCREMENT,DEFAULT,AVG_ROW_LENGTH',
        'php': '/usr/bin/php',
        'sudouser': '',
        'tables': '',
        'wikifile': 'all.dblist',
        'wikilist': '',
        'domain': '',
    }


def parse_config(conf):
    '''
    grab values from configuration and assign them to appropriate variables
    '''
    args = {}
    # could be true if we are only using the defaults
    if not conf.has_section('settings'):
        conf.add_section('settings')
    for setting in SETTINGS:
        args[setting] = conf.get('settings', setting)
    for setting in ['wikilist', 'tables', 'params_ignore']:
        if args[setting]:
            args[setting] = args[setting].split(',')
    return args
