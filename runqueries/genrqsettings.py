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
from subprocess import Popen, PIPE, SubprocessError
import queries.utils as qutils
import queries.dbinfo as qdbinfo
import queries.args as qargs
import queries.logger as qlogger


# SSH = '/usr/bin/ssh'
SSH = '/home/ariel/bin/sshes'


class QueryRunner():
    '''
    run various db queries or collect info to run them
    '''
    def __init__(self, args):
        self.args = args
        self.log = qlogger.get_logger(args)
        self.multiversion = self.check_if_multiversion()

    def get_page_info(self, pageid):
        '''
        get namespace and title of a specific page on the wiki
        '''
        api_url_base = self.get_api_url_from_wikidb()
        url = api_url_base + "?action=query&pageids={pageid}&format=json".format(
            pageid=pageid.decode('utf-8'))
        command = ["/usr/bin/curl", "-s", url]
        if not display_command_info(command, self.args['dryrun'], self.log):
            return '0', 'MainPage'

        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        if error:
            raise SubprocessError("Errors encountered: {error}".format(error=error.decode('utf-8')))
        try:
            pageinfo = json.loads(output.decode('utf-8'))
            thispage = pageinfo['query']['pages'][pageid.decode('utf-8')]
            namespace = thispage['ns']
            title = thispage['title']
        except Exception:
            raise ValueError("Failed to get pageinfo for page id {pageid}".format(
                pageid=pageid.decode('utf-8'))) from None
        if namespace != 0 and ':' in title:
            # dump the namespace prefix that will have been stuffed onto the title
            title = title.split(':', 1)[1]
        title = title.replace(' ', '_')
        return namespace, title

    def check_if_multiversion(self):
        '''
        see if the multiversion script exists on the mw host
        '''
        mw_script_location = qutils.get_mwscript_path(self.args)
        remote_command = ['/bin/ls', mw_script_location]
        command = qutils.build_command(remote_command, ssh_host=self.args['mwhost'])
        if not display_command_info(command, self.args['dryrun'], self.log):
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
            self.args, maintenance_script)
        subcommand = [maint_script_path]
        pull_vars = ["wgCanonicalServer", "wgScriptPath"]
        subcommand.extend([
            "--wiki={dbname}".format(dbname=self.args['wikidb']),
            "--format=json", "--regex={vars}".format(vars="|".join(pull_vars))])

        command = qutils.build_command(
            subcommand, ssh_host=self.args['mwhost'], sudo_user=self.args['sudouser'],
            mwscript=mwscript, php=self.args['php'])
        if not display_command_info(command, self.args['dryrun'], self.log):
            return 'http://example.com/w/api.php'

        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        # ignore stuff like "Warning: rename(/tmp/...) permission denied
        if error and not error.startswith(b'Warning'):
            raise SubprocessError("Errors encountered: {error}".format(error=error.decode('utf-8')))
        try:
            settings = json.loads(output.decode('utf-8'))
        except ValueError:
            settings = None
        if not settings or len(settings) != 2:
            raise ValueError(
                "Failed to get values for wgCanonicalServer, " +
                "wgScriptPath for {wiki}, got output {output}".format(
                    wiki=self.args['wikidb'], output=output))

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
            self.args, maintenance_script)
        subcommand = [maint_script_path]
        subcommand.extend(["--wiki={dbname}".format(dbname=self.args['wikidb']),
                           "--wikidb={dbname}".format(dbname=self.args['wikidb']),
                           "--group=vslow", "--", "--silent"])
        query = ("select rev_id from revision where " +
                 "rev_page={pageid} order by rev_id desc limit 1 offset {revcounthalf};".format(
                     pageid=pageid.decode('utf-8'), revcounthalf=int(int(revcount)/2 + 1)))
        remote_mysql_command = qutils.build_command(
            subcommand, ssh_host=self.args['mwhost'], sudo_user=self.args['sudouser'],
            mwscript=mw_script_location, php=self.args['php'])
        command = ["echo", "'{query}'".format(query=query), '|'] + remote_mysql_command
        if not display_command_info(command, self.args['dryrun'], self.log):
            return b'0'

        command = " ".join(command)
        proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        if error and not error.startswith(b"Warning:"):
            raise SubprocessError("Errors encountered: {error}".format(error=error.decode('utf-8')))
        revid = output.rstrip(b'\n')
        if not revid:
            raise ValueError("Failed to get midpoint revid for {wiki}".format(
                wiki=self.args['wikidb']))
        return revid


class RevCounter():
    '''
    run a horrid little C binary to read through an xml stubs history file,
    count revs per page, nd write out the ones that have more than 10k
    revisions. it's nicer than asking the dbs to do it, meh
    '''
    REVCUTOFF = 10000

    def __init__(self, args):
        self.args = args
        self.log = qlogger.get_logger(args)

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
            dumpsdir=self.args['dumpsdir'], wiki=self.args['wikidb'])]
        command = qutils.build_command(remote_command, ssh_host=self.args['dumpshost'])
        if not display_command_info(command, self.args['dryrun'], self.log):
            return b'99999999'

        command = " ".join(command)
        proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        if error:
            raise SubprocessError("Errors encountered: {error}".format(error=error.decode('utf-8')))
        # expect something like
        # 20180920  20181001  20181020  20181101 20181120 latest
        entries = output.split()
        try:
            entries = [entry for entry in entries if entry.isdigit() and len(entry) == 8]
            return entries[-2]
        except Exception:
            raise ValueError("Errors encountered getting run dates for {wiki} dumps:".format(
                wiki=self.args['wikidb'])) from None

    def get_pageid_revcount(self, rundate):
        '''
        having the date of the specific dump run, scat the stubs meta history file as stdin
        to the rev counter, sort and get the page id with the most revs
        '''
        # this command has single quotes at the beginning and end. yeah it's gross.
        remote_command = [
            "'/bin/zcat",
            "{dumpsdir}/{wiki}/{rundate}/{wiki}-{rundate}-stub-meta-history.xml.gz".format(
                dumpsdir=self.args['dumpsdir'], wiki=self.args['wikidb'],
                rundate=rundate.decode('utf-8')),
            "|",
            "/usr/local/bin/revsperpage", "all", str(self.REVCUTOFF),
            "|",
            "sort", "-k", "2", "-nr",
            "|",
            "head", "-1'"
        ]
        command = qutils.build_command(remote_command, ssh_host=self.args['dumpshost'])
        if not display_command_info(command, self.args['dryrun'], self.log):
            return b'0', b'0'

        command = " ".join(command)
        proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        if error:
            raise SubprocessError("Errors encountered: {error}".format(error=error.decode('utf-8')))
        # expect pageid revcount as the last line
        lines = output.splitlines()
        if not lines:
            raise ValueError("No pages found with enough revisions to work on")
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
Usage: python3 genrqsettings.py --settings <path> --wikidb <name>
    [--dryrun] [--verbose] [--help]

This script reads a settings file path and the name of a wiki database,
and then by means of several somersaults and a few rabbits out of grody
hats generates a config stanza that can be used for the script
that runs show explain for given queries on certain dbs.

See the sample-genrq.conf for an example config file.

"""
    usage_args = """
Arguments:
    --wikidb     (-w)   Name of wiki database (e.g. enwiki) for which to generate
                        show explain query config data
                        default: none
"""
    usage_output = qargs.get_common_arg_docs(['output'])
    usage_common = qargs.get_common_arg_docs(['settings'])
    usage_flags = qargs.get_common_arg_docs(['flags'])
    sys.stderr.write(usage_message + usage_output + usage_args +
                     usage_common + usage_flags)
    sys.exit(1)


def get_section(args):
    '''
    dig the section that the wikidb lives on out of the db config settings
    return it; it might be 'DEFAULT' but this is ok
    '''
    dbinfo = qdbinfo.DbInfo(args)
    _dbs, sections_by_db, _section_loads = dbinfo.get_dbhosts(args['wikidb'])
    if args['wikidb'] not in sections_by_db:
        return 'DEFAULT'
    return sections_by_db[args['wikidb']]


def run(args):
    '''
    given the args and the wiki database name,
    get all the values we need for a config stanza for
    the show explain script and write out a sample stanza
    '''
    revcounter = RevCounter(args)
    bigpage_id, revcount = revcounter.get_biggest_page_info()
    qrunner = QueryRunner(args)
    namespace, title = qrunner.get_page_info(bigpage_id)
    revid = qrunner.get_midpoint_revid(bigpage_id, revcount)
    startpage, endpage = get_start_end_pageids(int(bigpage_id))
    section = get_section(args)
    display(namespace, title, bigpage_id, revid, startpage, endpage, section, args['wikidb'])


def display_command_info(command, dryrun, log):
    '''
    print the appropriate info message for the command that will or would
    be run, returning True if the command is to be run
    '''
    if isinstance(command, list):
        printable_cmd = " ".join(command)
    else:
        printable_cmd = command
    if dryrun:
        log.info("would run command: %s", printable_cmd)
        return False
    log.info("running command: %s", printable_cmd)
    return True


def get_opt(opt, val, args):
    '''
    set option value in args dict if the option
    is one of the below
    '''
    if opt in ['-y', '--yamlfile']:
        args['yamlfile'] = val
    elif opt in ['-q', '--queryfile']:
        args['queryfile'] = val
    elif opt in ['-w', '--wikidb']:
        args['wikidb'] = val
    elif opt in ['-s', '--settings']:
        args['settings'] = val
    else:
        return False
    return True


def do_main():
    '''
    entry point
    '''
    args = qargs.get_arg_defaults(['settings', 'wikidb'], ['dryrun', 'verbose'])
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], 's:w:dvh', ['settings=', 'wikidb=',
                                      'dryrun', 'verbose', 'help'])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    qargs.handle_common_args(options, args, usage, remainder,
                             ['wikidb', 'settings'], get_opt)

    run(args)


if __name__ == '__main__':
    do_main()
