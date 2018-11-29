#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Given a config file and the name of a wiki database, write out
a stanza that can be added to a settings file for the show
explain script

Here's the hoops we jump through for the specified wiki:

ssh to the dumps host
   get a recent dump run date for the wiki that we hope has complete stubs
ssh to the dumps host
   from that stubs file, find page with the most revisions, display pageid, revcount
ssh to mw host
   see if mwscript exists or not -- must be done before any mw maint scripts can be run
ssh to mw host
   get $wgCanonicalServer and $wgScriptPath
use those values to put together the mw api path
curl to get info for the page with most revisions, using the api -- could be done via library
ssh to mw host
   get the id of the revision about halfway through the page history
ssh to mw host
   get the db configuration info ($wgLBFactoryConf)
use all of the above to display the configuration stanza for the show explain script

Six sshes, one call to command line util.
Yes, it's gross. Too bad.
"""


import sys
import getopt
import configparser
import json
import os
from subprocess import Popen, PIPE
from collections import OrderedDict


# SSH = '/usr/bin/ssh'
SSH = '/home/ariel/bin/sshes'
SUDO_USER = 'www-data'
MWSCRIPT = 'MWScript.php'


class ConfigReader():
    '''
    read stuff that would otherwise be command line args
    from a config file

    command line values override config file values which
    override built-in defaults (now set in config structure)
    '''
    SETTINGS = ['php', 'dumpshost', 'dumpsdir',
                'multiversion', 'mwhost', 'mwrepo']

    def __init__(self, configfile):
        defaults = self.get_config_defaults()
        self.conf = configparser.ConfigParser(defaults)
        if configfile is not None:
            self.conf.read(configfile)
            if not self.conf.has_section('settings'):
                # you don't have to have a config file, but if you do,
                # it needs to have the right stuff in it at least
                sys.stderr.write("The mandatory configuration section "
                                 "'settings' was not defined.\n")
                raise configparser.NoSectionError('settings')

    @staticmethod
    def get_config_defaults():
        '''
        get and return default config settings for this crapola
        '''
        return {
            'php': '/usr/bin/php',
            'dumpshost': '',
            'dumpspath': '/dumps',
            'multiversion': '',
            'mwhost': '',
            'mwrepo': '/srv/mediawiki'
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
        return args


class QueryRunner():
    '''
    run various db queries or collect info to run them
    '''
    def __init__(self, wikidb, config, dryrun, verbose):
        self.wikidb = wikidb
        self.config = config
        self.dryrun = dryrun
        self.verbose = verbose
        self.multiversion = self.check_if_multiversion()

    def get_page_info(self, pageid):
        '''
        get namespace and title of a specific page on the wiki
        '''
        api_url_base = self.get_api_url_from_wikidb()
        url = api_url_base + "?action=query&pageids={pageid}&format=json".format(
            pageid=pageid.decode('utf-8'))
        command = ["/usr/bin/curl", "-s", url]
        if not display_command_info(command, self.dryrun, self.verbose):
            return '0', 'MainPage'

        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        if error:
            print("Errors encountered:", error.decode('utf-8'))
            sys.exit(1)
        pageinfo = json.loads(output.decode('utf-8'))
        try:
            thispage = pageinfo['query']['pages'][pageid.decode('utf-8')]
            namespace = thispage['ns']
            title = thispage['title']
        except Exception:
            raise IOError("Failed to get pageinfo for page id {pageid}".format(
                pageid=pageid.decode('utf-8')))
        if namespace != 0 and ':' in title:
            # dump the namespace prefix that will have been stuffed onto the title
            title = title.split(':', 1)[1]
        title = title.replace(' ', '_')
        return namespace, title

    def check_if_multiversion(self):
        '''
        see if the multiversion script exists on the mw host
        '''
        mw_script_location = os.path.join(self.config['multiversion'], MWSCRIPT)
        remote_command = ['/bin/ls', mw_script_location]
        command = build_command(remote_command, ssh_host=self.config['mwhost'])
        if not display_command_info(command, self.dryrun, self.verbose):
            return ''

        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        if error:
            # "/bin/ls: cannot access..."
            return ''
        return output.rstrip()

    def get_api_url_from_wikidb(self):
        '''
        given a wiki database name, figure out the url for api requests;
        this requires running a MediaWiki php maintenance script. meh
        '''
        maintenance_script = "getConfiguration.php"
        mw_script_location, remote_command = get_maint_script_path(
            self.multiversion, self.config, maintenance_script)

        pull_vars = ["wgCanonicalServer", "wgScriptPath"]
        remote_command.extend([
            "--wiki={dbname}".format(dbname=self.wikidb),
            "--format=json", "'--regex={vars}'".format(vars="|".join(pull_vars))])

        command = build_command(
            remote_command, ssh_host=self.config['mwhost'], sudo_user=SUDO_USER,
            mwscript=mw_script_location, php=self.config['php'])
        if not display_command_info(command, self.dryrun, self.verbose):
            return 'http://example.com/w/api.php'

        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        # ignore stuff like "Warning: rename(/tmp/...) permission denied
        if error and not error.startswith(b'Warning'):
            print("Errors encountered:", error.decode('utf-8'))
            sys.exit(1)
        try:
            settings = json.loads(output.decode('utf-8'))
        except ValueError:
            settings = None
        if not settings or len(settings) != 2:
            raise IOError(
                "Failed to get values for wgCanonicalServer, " +
                "wgScriptPath for {wiki}, got output {output}".format(
                    wiki=self.wikidb, output=output))

        wgcanonserver = settings['wgCanonicalServer']
        wgscriptpath = settings['wgScriptPath']

        apibase = "/".join([
            wgcanonserver.rstrip('/'), wgscriptpath.strip('/'), "api.php"])
        return apibase

    def get_midpoint_revid(self, pageid, revcount):
        '''
        given a page id and how many revs it has, get the revid that's
        about halfway through the revs
        this requires running a MediaWiki php maintenance script. meh
        '''
        maintenance_script = "mysql.php"
        mw_script_location, mysql_command = get_maint_script_path(
            self.multiversion, self.config, maintenance_script)
        mysql_command.extend(["--wiki={dbname}".format(dbname=self.wikidb),
                              "--wikidb={dbname}".format(dbname=self.wikidb),
                              "--group=vslow", "--", "--silent"])
        query = ("select rev_id from revision where " +
                 "rev_page={pageid} order by rev_id desc limit 1 offset {revcounthalf};".format(
                     pageid=pageid.decode('utf-8'), revcounthalf=int(int(revcount)/2 + 1)))
        remote_mysql_command = build_command(
            mysql_command, ssh_host=self.config['mwhost'], sudo_user=SUDO_USER,
            mwscript=mw_script_location, php=self.config['php'])
        remote_mysql_command = " ".join(remote_mysql_command)
        command = "echo '{query}' | {mysql}".format(
            query=query, mysql=remote_mysql_command)
        if not display_command_info(command, self.dryrun, self.verbose):
            return b'0'

        proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        if error and not error.startswith(b"Warning:"):
            print("Errors encountered:", error.decode('utf-8'))
            sys.exit(1)
        revid = output.rstrip(b'\n')
        if not revid:
            raise IOError(
                "Failed to get midpoint revid for {wiki}".format(wiki=self.wikidb))
        return revid


class RevCounter():
    '''
    run a horrid little C binary to read through an xml stubs history file,
    count revs per page, nd write out the ones that have more than 10k
    revisions. it's nicer than asking the dbs to do it, meh
    '''
    REVCUTOFF = 10000

    def __init__(self, config, wikidb, dryrun, verbose):
        self.config = config
        self.wikidb = wikidb
        self.dryrun = dryrun
        self.verbose = verbose

    def get_biggest_page_info(self):
        '''
        get the date of a dump we think is complete (last minus one),
        use that date to find a stubs meta history xml file from which
        we'll get the page id of the page with the most revisions in it
        '''
        rundate = self.get_dump_rundate()
        pageid, revcount = self.get_pageid_revcount(rundate)
        return pageid, revcount

    def get_dump_rundate(self):
        '''
        guess that the second most recent date will have completed
        stubs. if it doesn't, we have some really broken crap out there
        and ought to hear about it in any case
        '''
        remote_command = " /bin/ls {dumpsdir}/{wiki}".format(
            dumpsdir=self.config['dumpsdir'], wiki=self.wikidb)
        command = build_command(remote_command, ssh_host=self.config['dumpshost'])
        if not display_command_info(command, self.dryrun, self.verbose):
            return b'99999999'

        proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        if error:
            print("Errors encountered:", error.decode('utf-8'))
            sys.exit(1)
        # expect something like
        # 20180920  20181001  20181020  20181101 20181120 latest
        entries = output.split()
        try:
            entries = [entry for entry in entries if entry.isdigit() and len(entry) == 8]
            return entries[-2]
        except Exception:
            raise "Errors encountered getting run dates for {wiki} dumps:".format(wiki=self.wikidb)

    def get_pageid_revcount(self, rundate):
        '''
        having the date of the specific dump run, scat the stubs meta history file as stdin
        to the rev counter, sort and get the page id with the most revs
        '''
        remote_command = ("'/bin/zcat " +
                          "{dumpsdir}/{wiki}/{rundate}/{wiki}-{rundate}-stub-meta-history.xml.gz " +
                          "| /usr/local/bin/revsperpage all " + str(self.REVCUTOFF) +
                          "| sort -k 2 -nr | head -1'")
        remote_command = remote_command.format(
            dumpsdir=self.config['dumpsdir'], wiki=self.wikidb, rundate=rundate.decode('utf-8'))
        command = build_command(remote_command, ssh_host=self.config['dumpshost'])
        if not display_command_info(command, self.dryrun, self.verbose):
            return b'0', b'0'

        proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        if error:
            print("Errors encountered:", error.decode('utf-8'))
            sys.exit(1)
        # expect pageid revcount as the last line
        lines = output.splitlines()
        pageid = lines[-1].split()[0]
        revcount = lines[-1].split()[1]
        return pageid, revcount


def get_start_end_pageids(pageid):
    '''
    nothing much, we just want 20k pages between start and end,
    with our pageid in the middle if possible
    '''
    startpage = pageid - 10000
    if startpage < 1:
        startpage = 1
    endpage = startpage + 20000
    return startpage, endpage


def display(namespace, title, bigpage_id, revid, startpage, endpage, section, wikidb):
    '''
    write one config stanza with the given values
    '''
    stanza = """servers:
  {section}:
    hosts:
      - FILL IN
    wikis:
      {wikidb}:
        bigpage: '{pageid}'
        revid: '{revid}'
        startpage: '{start}'
        endpage: '{end}'
        namespace: '{ns}'
        title: {title}"""
    print(stanza.format(wikidb=wikidb, section=section,
                        pageid=bigpage_id.decode('utf-8'), revid=revid.decode('utf-8'),
                        start=startpage, end=endpage, ns=namespace, title=title))


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''

    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write('\n')
    usage_message = """
Usage: python3 genrqsettings.py --configfile <path> --wikidb <name>
    [--dryrun] [--verbose] [--help]

This script reads a configfile path and the name of a wiki database,
and then by means of several somersaults and a few rabbits out of grody
hats generates a config stanza that can be used for the script
that runs show explain for given queries on certain dbs.

See the sample-genrq.conf for an example config file.

Arguments:
    --settings   (-s)   File with settings for host to run revsperpage, path
                        to php file with db section info
                        default: genrqsettings.conf
    --wikidb     (-w)   Name of wiki database (e.g. enwiki) for which to generate
                        show explain query config data
                        default: none

Flags:
    --dryrun  (-d)    Don't execute queries but show what would be done
    --verbose (-v)    Display progress messages as queries are executed on the wikis
    --help    (-h)    show this message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def get_dbconfig(config, multiversion, wikidb, dryrun, verbose):
    '''
    get section-related info from wgLBFactoryConf
    we must ssh to the remote mw host, run a php command to get the
    contents of the variable(s) we want, and process the output.
    so gross.
    '''
    maintenance_script = "getConfiguration.php"
    mw_script_location, remote_command = get_maint_script_path(
        multiversion, config, maintenance_script)

    pull_var = "wgLBFactoryConf"
    remote_command.extend([
        "--wiki={dbname}".format(dbname=wikidb),
        "--format=json", "'--regex={var}'".format(var=pull_var)])

    command = build_command(
        remote_command, ssh_host=config['mwhost'], sudo_user=SUDO_USER,
        mwscript=mw_script_location, php=config['php'])
    if not display_command_info(command, dryrun, verbose):
        return {}

    proc = Popen(command, stdout=PIPE, stderr=PIPE)
    output, error = proc.communicate()
    # ignore stuff like "Warning: rename(/tmp/...) permission denied
    if error and not error.startswith(b'Warning'):
        print("Errors encountered:", error.decode('utf-8'))
        sys.exit(1)
    if not output:
        raise IOError("Failed to retrieve db config")
    try:
        settings = json.loads(output.decode('utf-8'), object_pairs_hook=OrderedDict)
    except ValueError:
        settings = None
    if not settings:
        raise IOError(
            "Failed to get values for wgLBFactoryConf for {wiki}, got output {output}".format(
                wiki=wikidb, output=output))

    wglbfactoryconf = settings['wgLBFactoryConf']
    if 'sectionsByDB' not in wglbfactoryconf:
        raise ValueError("missing sectionsByDB from wgLBFactoryConf, bad config?")
    return wglbfactoryconf['sectionsByDB']


def get_section(config, multiversion, wikidb, dryrun, verbose):
    '''
    dig the section that the wikidb lives on out of the db config settings
    return it; it might be 'DEFAULT' but this is ok
    '''
    sections_by_db = get_dbconfig(config, multiversion, wikidb, dryrun, verbose)
    if wikidb not in sections_by_db:
        return 'DEFAULT'
    return sections_by_db[wikidb]


def run(config, wikidb, dryrun, verbose):
    '''
    given the config and the wiki database name,
    get all the values we need for a config stanza for
    the show explain script and write out a sample stanza
    '''
    revcounter = RevCounter(config, wikidb, dryrun, verbose)
    bigpage_id, revcount = revcounter.get_biggest_page_info()
    qrunner = QueryRunner(wikidb, config, dryrun, verbose)
    namespace, title = qrunner.get_page_info(bigpage_id)
    revid = qrunner.get_midpoint_revid(bigpage_id, revcount)
    startpage, endpage = get_start_end_pageids(int(bigpage_id))
    section = get_section(config, qrunner.multiversion, wikidb, dryrun, verbose)
    display(namespace, title, bigpage_id, revid, startpage, endpage, section, wikidb)


def prepend_command(base_command, prepends):
    '''
    add new parts of a command onto the front,
    doing the right thing if the base command is list or string
    '''
    if isinstance(base_command, str):
        command = ' '.join(prepends) + ' ' + base_command
    else:
        command = prepends + base_command
    return command


def get_maint_script_path(multiversion, config, maint_script_basename):
    '''
    if we are using a multiversion setup, return the path to mwscript and the
    unaltered maintenance script path
    if we are not, return None for mwscript and an adjusted path for the maint
    script
    '''
    if multiversion:
        mw_script_location = os.path.join(config['multiversion'], MWSCRIPT)
        maint_script_cmd = [maint_script_basename]
    else:
        mw_script_location = None
        maint_script_cmd = ["{repo}/maintenance/{script}".format(
            repo=config['mwrepo'], script=maint_script_basename)]
    return mw_script_location, maint_script_cmd


def build_command(command_base, ssh_host=None, sudo_user=None, mwscript=None, php=None):
    '''
    given a command, add the ssh, sudo and mwscript pieces as needed
    if command is a mw script to be run by php, the path to the script
    must already be set correctly in command_base (i.e. full path for
    php or relative path for mwscript), this method will not check that
    '''
    command = command_base[:]
    if mwscript:
        command = prepend_command(command, [mwscript])
    if php:
        command = prepend_command(command, [php])
    if sudo_user:
        sudocmd = ['sudo', '-u', sudo_user]
        command = prepend_command(command, sudocmd)
    if ssh_host:
        sshcmd = [SSH, ssh_host]
        command = prepend_command(command, sshcmd)
    return command


def display_command_info(command, dryrun, verbose):
    '''
    print the appropriate info message for the command that will or would
    be run, returning True if the command is to be run
    '''
    if isinstance(command, list):
        printable_cmd = " ".join(command)
    else:
        printable_cmd = command
    if dryrun:
        print("would run command:", printable_cmd)
        return False
    if verbose:
        print("running command:", printable_cmd)
    return True


def do_main():
    '''
    entry point
    '''
    configfile = "genrqsettings.conf"
    wikidb = None
    dryrun = False
    verbose = False

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], 's:w:dvh', ['settings=', 'wikidb=',
                                      'dryrun', 'verbose', 'help'])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ['-s', '--settings']:
            configfile = val
        elif opt in ['-w', '--wikidb']:
            wikidb = val
        elif opt in ['-h', '--help']:
            usage("Help for this script")
        elif opt in ['-d', '--dryrun']:
            dryrun = True
        elif opt in ['-v', '--verbose']:
            verbose = True

    if remainder:
        usage("Unknown option(s) specified: <{opt}>".format(opt=remainder[0]))

    if wikidb is None:
        usage("Mandatory argument 'wikidb' not specified")

    conf_reader = ConfigReader(configfile)
    config = conf_reader.parse_config()
    run(config, wikidb, dryrun, verbose)


if __name__ == '__main__':
    do_main()
