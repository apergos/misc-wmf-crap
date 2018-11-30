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
import json
from subprocess import Popen, PIPE
import queries.config as qconfig
import queries.utils as qutils
import queries.dbinfo as qdbinfo


# SSH = '/usr/bin/ssh'
SSH = '/home/ariel/bin/sshes'


class QueryRunner():
    '''
    run various db queries or collect info to run them
    '''
    def __init__(self, wikidb, config):
        self.wikidb = wikidb
        self.config = config
        self.multiversion = self.check_if_multiversion()

    def get_page_info(self, pageid):
        '''
        get namespace and title of a specific page on the wiki
        '''
        api_url_base = self.get_api_url_from_wikidb()
        url = api_url_base + "?action=query&pageids={pageid}&format=json".format(
            pageid=pageid.decode('utf-8'))
        command = ["/usr/bin/curl", "-s", url]
        if not display_command_info(command, self.config['dryrun'], self.config['verbose']):
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
        mw_script_location = qutils.get_mwscript_path(self.config)
        remote_command = ['/bin/ls', mw_script_location]
        command = qutils.build_command(remote_command, ssh_host=self.config['mwhost'])
        if not display_command_info(command, self.config['dryrun'], self.config['verbose']):
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
        mwscript, maint_script_path = qutils.get_maint_script_path(
            self.config, maintenance_script)
        subcommand = [maint_script_path]
        pull_vars = ["wgCanonicalServer", "wgScriptPath"]
        subcommand.extend([
            "--wiki={dbname}".format(dbname=self.wikidb),
            "--format=json", "--regex={vars}".format(vars="|".join(pull_vars))])

        command = qutils.build_command(
            subcommand, ssh_host=self.config['mwhost'], sudo_user=self.config['sudouser'],
            mwscript=mwscript, php=self.config['php'])
        if not display_command_info(command, self.config['dryrun'], self.config['verbose']):
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
        mw_script_location, maint_script_path = qutils.get_maint_script_path(
            self.config, maintenance_script)
        subcommand = [maint_script_path]
        subcommand.extend(["--wiki={dbname}".format(dbname=self.wikidb),
                           "--wikidb={dbname}".format(dbname=self.wikidb),
                           "--group=vslow", "--", "--silent"])
        query = ("select rev_id from revision where " +
                 "rev_page={pageid} order by rev_id desc limit 1 offset {revcounthalf};".format(
                     pageid=pageid.decode('utf-8'), revcounthalf=int(int(revcount)/2 + 1)))
        remote_mysql_command = qutils.build_command(
            subcommand, ssh_host=self.config['mwhost'], sudo_user=self.config['sudouser'],
            mwscript=mw_script_location, php=self.config['php'])
        command = ["echo", "'{query}'".format(query=query), '|'] + remote_mysql_command
        if not display_command_info(command, self.config['dryrun'], self.config['verbose']):
            return b'0'

        command = " ".join(command)
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

    def __init__(self, config, wikidb):
        self.config = config
        self.wikidb = wikidb

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
        remote_command = ["/bin/ls", "{dumpsdir}/{wiki}".format(
            dumpsdir=self.config['dumpsdir'], wiki=self.wikidb)]
        command = qutils.build_command(remote_command, ssh_host=self.config['dumpshost'])
        if not display_command_info(command, self.config['dryrun'], self.config['verbose']):
            return b'99999999'

        command = " ".join(command)
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
        # this command has single quotes at the beginning and end. yeah it's gross.
        remote_command = [
            "'/bin/zcat",
            "{dumpsdir}/{wiki}/{rundate}/{wiki}-{rundate}-stub-meta-history.xml.gz".format(
                dumpsdir=self.config['dumpsdir'], wiki=self.wikidb,
                rundate=rundate.decode('utf-8')),
            "|",
            "/usr/local/bin/revsperpage", "all", str(self.REVCUTOFF),
            "|",
            "sort", "-k", "2", "-nr",
            "|",
            "head", "-1'"
        ]
        command = qutils.build_command(remote_command, ssh_host=self.config['dumpshost'])
        if not display_command_info(command, self.config['dryrun'], self.config['verbose']):
            return b'0', b'0'

        command = " ".join(command)
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


def get_section(config, wikidb):
    '''
    dig the section that the wikidb lives on out of the db config settings
    return it; it might be 'DEFAULT' but this is ok
    '''
    dbinfo = qdbinfo.DbInfo(config)
    _dbs, sections_by_db, _section_loads = dbinfo.get_dbhosts(wikidb)
    if wikidb not in sections_by_db:
        return 'DEFAULT'
    return sections_by_db[wikidb]


def run(config, wikidb):
    '''
    given the config and the wiki database name,
    get all the values we need for a config stanza for
    the show explain script and write out a sample stanza
    '''
    revcounter = RevCounter(config, wikidb)
    bigpage_id, revcount = revcounter.get_biggest_page_info()
    qrunner = QueryRunner(wikidb, config)
    namespace, title = qrunner.get_page_info(bigpage_id)
    revid = qrunner.get_midpoint_revid(bigpage_id, revcount)
    startpage, endpage = get_start_end_pageids(int(bigpage_id))
    section = get_section(config, wikidb)
    display(namespace, title, bigpage_id, revid, startpage, endpage, section, wikidb)


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
    args = {}
    configfile = "genrqsettings.conf"
    wikidb = None
    args['dryrun'] = False
    args['verbose'] = False

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
            args['dryrun'] = True
        elif opt in ['-v', '--verbose']:
            args['verbose'] = True

    if remainder:
        usage("Unknown option(s) specified: <{opt}>".format(opt=remainder[0]))

    if wikidb is None:
        usage("Mandatory argument 'wikidb' not specified")

    conf = qconfig.config_setup(configfile)
    for setting in qconfig.SETTINGS:
        if setting not in args:
            args[setting] = conf[setting]

    run(args, wikidb)


if __name__ == '__main__':
    do_main()
