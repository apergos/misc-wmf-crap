# -*- coding: utf-8 -*-
"""
given a config file and the name of a wiki database, write out
a stanza that can be added to a settings file for the show
explain script
"""


import sys
import getopt
import ConfigParser
import json
import os
from subprocess import Popen, PIPE
from collections import OrderedDict


#SSH = '/usr/bin/ssh'
SSH = '/home/ariel/bin/sshes'


class ConfigReader(object):
    '''
    read stuff that would otherwise be command line args
    from a config file

    command line values override config file values which
    override built-in defaults (now set in config structure)
    '''
    SETTINGS = ['dbconfig', 'php', 'dumpshost', 'dumpsdir',
                'multiversion', 'mwhost', 'mwrepo']

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
            'dbconfig': '',
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


class QueryRunner(object):
    '''
    run various db queries or collect info to run them
    '''
    def __init__(self, wikidb, config, dryrun, verbose):
        self.wikidb = wikidb
        self.config = config
        self.dryrun = dryrun
        self.verbose = verbose
        if self.check_if_multiversion():
            self.multiversion = True
        else:
            self.multiversion = False

    def get_page_info(self, pageid):
        '''
        get namespace and title of a specific page on the wiki
        '''
        api_url_base = self.get_api_url_from_wikidb()
        url = api_url_base + "?action=query&pageids={pageid}&format=json".format(
            pageid=pageid)
        command = ["/usr/bin/curl", "-s", url]
        if self.dryrun:
            print "would run command:", command
            return '0', 'MainPage'
        elif self.verbose:
            print "running command:", command

        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        if error:
            print("Errors encountered:", error)
            sys.exit(1)
        pageinfo = json.loads(output)
        try:
            thispage = pageinfo['query']['pages'][pageid]
            namespace = thispage['ns']
            title = thispage['title']
        except Exception:
            raise IOError("Failed to get pageinfo for page id {pageid}".format(pageid=pageid))
        if namespace != 0 and ':' in title:
            # dump the namespace prefix that will have been stuffed onto the title
            title = title.split(':', 1)[1]
        title = title.replace(' ', '_')
        return namespace, title

    def check_if_multiversion(self):
        '''
        see if the multiversion script exists on the mw host
        '''
        mw_script_location = os.path.join(self.config['multiversion'], "MWScript.php")
        remote_command = ['/bin/ls', mw_script_location]
        ssh_prefix = [SSH, "{host}".format(host=self.config['mwhost'])]
        command = ssh_prefix + remote_command

        if self.dryrun:
            print "would run command:", command
            return 'http://example.com/w/api.php'
        elif self.verbose:
            print "running command:", command

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
        remote_command = ['sudo', '-u', 'www-data', self.config['php']]
        maintenance_script = "getConfiguration.php"
        if self.multiversion:
            mw_script_location = os.path.join(self.config['multiversion'], "MWScript.php")
            remote_command.extend([mw_script_location, maintenance_script])
        else:
            remote_command.extend(["%s/maintenance/%s" % (
                self.config['mwrepo'], maintenance_script)])

        pull_vars = ["wgCanonicalServer", "wgScriptPath"]
        remote_command.extend(["--wiki={dbname}".format(dbname=self.wikidb),
                               "--format=json", "'--regex={vars}'".format(vars="|".join(pull_vars))])
        ssh_prefix = [SSH, "{host}".format(host=self.config['mwhost'])]
        command = ssh_prefix + remote_command

        if self.dryrun:
            print "would run command:", command
            return 'http://example.com/w/api.php'
        elif self.verbose:
            print "running command:", command

        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        # ignore stuff like "Warning: rename(/tmp/...) permission denied
        if error and not error.startswith('Warning'):
            print("Errors encountered:", error)
            sys.exit(1)
        try:
            settings = json.loads(output)
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
        mysql_command = [self.config['php']]
        maintenance_script = "mysql.php"
        if self.multiversion:
            mw_script_location = os.path.join(self.config['multiversion'], "MWScript.php")
            mysql_command.extend([mw_script_location, maintenance_script])
        else:
            mysql_command.extend(["%s/maintenance/%s" % (
                self.config['mwrepo'], maintenance_script)])
        mysql_command.extend(["--wiki={dbname}".format(dbname=self.wikidb),
                              "--wikidb={dbname}".format(dbname=self.wikidb),
                              "--group=vslow", "--", "--silent"])
        query = ("select rev_id from revision where " +
                 "rev_page={pageid} order by rev_id desc limit 1 offset {revcounthalf};".format(
                     pageid=pageid, revcounthalf=int(revcount)/2 + 1))
        ssh_prefix = SSH + " {host} ".format(host=self.config['mwhost'])
        sudo_prefix = " sudo -u www-data "
        remote_mysql_command = ssh_prefix + sudo_prefix + " ".join(mysql_command)
        command = "echo '{query}' | {mysql}".format(
            query=query, mysql=remote_mysql_command)

        if self.dryrun:
            print "would run command:", command
            return '0'
        elif self.verbose:
            print "running command:", command

        proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        if error and not error.startswith("Warning:"):
            print("Errors encountered:", error)
            sys.exit(1)
        revid = output.rstrip('\n')
        if not revid:
            raise IOError(
                "Failed to get midpoint revid for {wiki}".format(wiki=self.wikidb))
        return revid


class RevCounter(object):
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
        remote_command = " /bin/ls {dumpsdir}/{wiki}"
        ssh_prefix = SSH + " {host}".format(host=self.config['dumpshost'])
        command = ssh_prefix.format(host=self.config['dumpshost']) + remote_command.format(
            dumpsdir=self.config['dumpsdir'], wiki=self.wikidb)
        if self.dryrun:
            print "would run command:", command
            return '99999999'
        elif self.verbose:
            print "running command:", command
        proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        if error:
            print("Errors encountered:", error)
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
        ssh_prefix = SSH + " {host} "
        command = ssh_prefix.format(host=self.config['dumpshost']) + remote_command.format(
            dumpsdir=self.config['dumpsdir'], wiki=self.wikidb, rundate=rundate)
        if self.dryrun:
            print "would run command:", command
            return 0, 0
        elif self.verbose:
            print "running command:", command

        proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        if error:
            print("Errors encountered:", error)
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
  shard: '{shard}'
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
    print(stanza.format(wikidb=wikidb, shard=section, pageid=bigpage_id, revid=revid,
                        start=startpage, end=endpage, ns=namespace, title=title.encode('utf-8')))


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''

    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write('\n')
    usage_message = """
Usage: genrqsettings.py --configfile <path> --wikidb <name>
    [--dryrun] [--verbose] [--help]

This script reads a configfile path and the name of a wiki database,
and then by means of several somersaults and a few rabbits out of grody
hats generates a config stanza that can be used for the script
that runs show explain for given queries on certain dbs.

See the sample-genrq.conf for an example config file.

Arguments:
    --settings   (-s)   File with settings for host to run revsperpage, path
                        to php file with db shard info
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


def get_dbconfig_from_file(config, dryrun, verbose):
    '''
    get section-related info from wgLBFactoryConf stuff in a file
    we must ssh to the remote mw host, run a php command to get the
    contents of the variable(s) we want, and process the output.
    so gross.
    '''
    # this is done remotely. yuck
    remote_command = [config['php'], 'display_wgLBFactoryConf.php', config['dbconfig']]
    ssh_prefix = [SSH, "{host}".format(host=config['mwhost'])]
    command = ssh_prefix + remote_command

    if dryrun:
        print "would run command:", ' '.join(command)
        return {}
    elif verbose:
        print "running command:", ' '.join(command)

    proc = Popen(command, stdout=PIPE, stderr=PIPE)
    output, error = proc.communicate()
    if error:
        print("Errors encountered:", error)
        sys.exit(1)
    if not output:
        raise IOError("Failed to process db config file " + config['dbconfig'])
    wglbfactoryconf = json.loads(output, object_pairs_hook=OrderedDict)
    if 'sectionsByDB' not in wglbfactoryconf:
        raise ValueError("missing sectionsByDB from wgLBFactoryConf, bad config?")
    return wglbfactoryconf['sectionsByDB']


def get_section(config, wikidb, dryrun, verbose):
    '''
    dig the section that the wikidb lives on,
    out of the dbconfig file and return it;
    it might be 'DEFAULT' but this is ok
    '''
    sections_by_db = get_dbconfig_from_file(config, dryrun, verbose)
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
    section = get_section(config, wikidb, dryrun, verbose)
    display(namespace, title, bigpage_id, revid, startpage, endpage, section, wikidb)


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
        usage("Unknown option(s) specified: <%s>" % remainder[0])

    if wikidb is None:
        usage("Mandatory argument 'wikidb' not specified")

    conf_reader = ConfigReader(configfile)
    config = conf_reader.parse_config()
    run(config, wikidb, dryrun, verbose)


if __name__ == '__main__':
    do_main()
