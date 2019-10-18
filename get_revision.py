#!/usr/bin/python3
'''
Given a revision id, a slot role name, a wiki name, and a maintenance host name,
via some ssh commands get the blob id and then get the raw revision text for that
slot, displaying it to stdout with no processing but exactly as it appears in
the database after decompression. Oh yeah and there's a dns lookup in the mix for
the ip address dug out of the lb config for the external storage cluster. Gross.

It currently won't distinguish between no rows for your revision id (maybe it
doesn't have a mediainfo slot!) or whether some error was encountered, though
error text from sql commands will be printed out if there is any.

NOTE that this has been tested with Wikimedia's infrastructure only.
It assumes the existence of a wiki farm where maintenance scripts are invoked
by MWScript.php, and it assumes a wgLBFactoryConf populated with 'externalLoads'
and 'templateOverridesByCluster' for external storage hosts that keep blob tables
 of revision content separate from the wikis' text tables.

It also assumes you have ssh rights into a maintenance host with our 'sql' command
(maybe I should fix that and just use mysql.php directly?), that maintenance
scripts must be run as the www-data user (which exists), that you have ssh
rights and sudo rights on external storage hosts, that mysql is set up for root on
those hosts not to prompt you for the password, etc.

And finally, this script will break once data is migrated away from the text table;
we'll have to get the cluster info from somewhere else (TBD) and proceed accordingly.
'''
import base64
import sys
import zlib
from subprocess import Popen, PIPE, SubprocessError
import getopt
import json
from collections import OrderedDict


def usage(message=None):
    '''display usage info about this script'''
    if message is not None:
        print(message)
    usage_message = """Usage: get_revision.py --wiki <dbname> --revid <number>
         --slot <slot role> --host <maintenance-host name> [--multi] [--log]
         [--dryrun] [--verbose] | --help

Arguments:

  --wiki    (-w):   name of wiki database from which to retrieve revision content
                    default: none
  --revid   (-r):   id of revision for which to retrieve content
                    default: none
  --slot    (-s):   name of slot type ('mediainfo', 'main', etc.) for revision
                    default: 'main'
  --host    (-H):   name of host to which to ssh for retrieval of revision info
                    default: none
  --multi   (-m):   path to MWScript.php
                    default: /srv/mediawiki/multiversion/MWScript.php
  --log     (-l):   name of log file to which to log informative messages, if any
                    default: none, logged to stderr
  --dryrun  (-d):   display commands the script would run instead of running them
                    default: false
  --verbose (-v):   display progress messages while the script is running
                    default: false
  --help    (-h):   display this usage message

Example use:
    python3 get_revision.py -w elwiktionary -r 20050 -s main -H mwmaint1002.eqiad.wmnet -v
"""
    print(usage_message)
    sys.exit(1)


def get_default_args():
    '''return a dict of the default values for args'''
    args = {'wiki': None,
            'revid': None,
            'slot': None,
            'host': None,
            'multi': '/srv/mediawiki/multiversion/MWScript.php',
            'log': None,
            'dryrun': False,
            'verbose': False}
    return args


def get_args():
    '''get and return dict of command line args'''
    args = get_default_args()
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "w:r:s:m:H:l:dvh", ["wiki=", "revid=", "slot=", "host=", "log=",
                                              "multi=", "dryrun", "verbose", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-w", "--wiki"]:
            args['wiki'] = val
        elif opt in ["-r", "--revid"]:
            args['revid'] = val
        elif opt in ["-s", "--slot"]:
            args['slot'] = val
        elif opt in ["-m", "--multi"]:
            args['multi'] = val
        elif opt in ["-H", "--host"]:
            args['host'] = val
        elif opt in ["-l", "--log"]:
            args['log'] = val
        elif opt in ["-d", "--dryrun"]:
            args['dryrun'] = True
        elif opt in ["-v", "--verbose"]:
            args['verbose'] = True
        elif opt in ["-h", "--help"]:
            usage("Help for this script")
        else:
            usage("Unknown option specified: <%s>" % opt)

    if remainder:
        usage("Unknown option(s) specified: {opt}".format(opt=remainder[0]))

    return args


def validate_args(args):
    '''check that all mandatory args are set and that
    args have sane values'''
    for arg_name in ['wiki', 'host', 'revid']:
        if args[arg_name] is None:
            usage("Mandatory argument {name} not specified".format(name=arg_name))
    if not args['revid'].isdigit():
        usage("revid argument must be a number")


def log(message, args):
    '''log errors and other messages to log file or to stderr'''
    if args['log'] is not None:
        with open(log, "a+") as logfile:
            logfile.write(message + '\n')
    else:
        sys.stderr.write(message + '\n')


def get_lbconf(args):
    '''
    get list of dbs and section-related info from wgLBFactoryConf stuff
    by running a mw maintenance script
    '''
    command = ['sshes', 'sshes_quiet', args['host'], 'sudo', '-u' 'www-data']
    command.extend(['php', args['multi'], 'getConfiguration.php'])
    command.extend(["--wiki={wiki}".format(wiki=args['wiki']),
                    '--format=json',
                    '--regex={var}'.format(var='wgLBFactoryConf')])
    if args['dryrun']:
        log("would run command: {cmd}".format(cmd=" ".join(command)), args)
        return None
    if args['verbose']:
        log("running command: {cmd}".format(cmd=" ".join(command)), args)
    proc = Popen(command, stdout=PIPE, stderr=PIPE)
    output, error = proc.communicate()
    if error and not error.startswith(b'Warning'):
        # ignore stuff like "Warning: rename(/tmp/...) permission denied
        raise SubprocessError("Errors encountered: {error}".format(error=error.decode('utf-8')))
    if not output:
        raise ValueError("Failed to retrieve db config")
    try:
        results = json.loads(output.decode('utf-8'), object_pairs_hook=OrderedDict)
    except ValueError:
        sys.stderr.write("got db host info: {creds}\n".format(creds=output.decode('utf-8')))
        raise ValueError(
            "Failed to get values for wgLBFactoryConf for {wiki}, got output {output}".format(
                wiki=args['wiki'], output=output)) from None
    return results['wgLBFactoryConf']


def get_text_addr(args):
    '''get and return the address of the text content for the specific
    slot and revision, after lookup in the text table'''
    query = ('use {wiki}; select old_text FROM text ' +
             'INNER JOIN content ON old_id = SUBSTRING(content_address, 4) ' +
             'INNER JOIN slots ON slot_content_id = content_id  ' +
             'INNER JOIN slot_roles on slot_role_id = role_id ' +
             'WHERE role_name = "{slotrole}" and slot_revision_id = {revid};')
    query_formatted = query.format(wiki=args['wiki'],
                                   slotrole=args['slot'],
                                   revid=args['revid'])
    command = ["echo", "'{query}'".format(query=query_formatted), '|',
               "sshes", "sshes_quiet", args['host'],
               'sql', '--wiki={wiki}'.format(wiki=args['wiki']), '--', '-s', '-s', '-N']
    command = " ".join(command)
    if args['dryrun']:
        log("would run command: {cmd}".format(cmd=command), args)
        return None
    if args['verbose']:
        log("running command: {cmd}".format(cmd=command), args)
    proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
    output, error = proc.communicate()
    text_addr = output.rstrip(b'\n')
    if not text_addr:
        if error:
            log(error.decode('utf-8') + "\n", args)
        raise ValueError("Failed to get text address for {revid} for {wiki}".format(
            revid=args['revid'], wiki=args['wiki']))
    return text_addr.decode('utf-8')


def lookup_ip(address, args):
    '''
    given an ip address, try to look it up on the 'maintenance host' we
    were given via the command line
    '''
    command = ["sshes", "sshes_quiet", args['host'], 'dig', '-x', address, '+short']
    command = " ".join(command)
    if args['dryrun']:
        log("would run command: {cmd}".format(cmd=command), args)
        return None
    if args['verbose']:
        log("running command: {cmd}".format(cmd=command), args)
    proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
    output, error = proc.communicate()
    host = output.rstrip(b'\n')
    if not host:
        if error:
            log(error.decode('utf-8') + "\n", args)
        raise ValueError("Failed to look up ip address {address} for {revid} for {wiki}".format(
            address=address, revid=args['revid'], wiki=args['wiki']))
    host = host.rstrip(b'.')
    # es1019.eqiad.wmnet.  <-- trailing period
    return host.decode('utf-8')


def get_cluster_info(text_address, args):
    '''given the address of a text blob, turn it into cluster name and blob id,
    get the host and port info for the cluster, get also the blob table name,
    and return a dict with all that in it'''
    if args['dryrun'] and text_address is None:
        # text address will be None because of the dryrun, so let's use
        # some sample in here for command checking
        text_address = 'DB://samplecluster/12345'
    if not text_address.startswith('DB://'):
        raise ValueError("Bad text adddress {addr} found for {revid} for {wiki}".format(
            addr=text_address, revid=args['revid'], wiki=args['wiki']))
    fields = text_address[5:].split('/')
    if len(fields) != 2 or not fields[1].isdigit():
        raise ValueError("Bad text adddress {addr} found for {revid} for {wiki}".format(
            addr=text_address, revid=args['revid'], wiki=args['wiki']))
    lb_conf = get_lbconf(args)
    if args['dryrun'] and lb_conf is None:
        # lb conf will be None because of the dryrun, so let's use
        # some sample in here for command checking
        lb_conf = {'externalLoads': {'samplecluster': 'samplehost'},
                   'templateOverridesByCluster': {'samplecluster': {'blobs table':
                                                                    'blob_table_sample'}}}
    cluster_name = fields[0]
    blob_id = fields[1]
    # this could fail, so we'll except out. ok
    # OrderedDict([('10.64.32.65', 0), ('10.64.16.187', 1), ('10.64.48.116', 1)])
    host = list(lb_conf['externalLoads'][cluster_name])[-1]
    # this might be host:port, so check that
    if ':' in host:
        host, port = host.split(':')
    else:
        port = '3306'
    # OrderedDict([('blobs table', 'blobs_cluster25')])
    table = lb_conf['templateOverridesByCluster'][cluster_name]['blobs table']
    # if the host is a local network IP address, try looking it up on that same maintenance host
    if host.startswith('10.'):
        host = lookup_ip(host, args)
    return {'host': host, 'port': port, 'blob': blob_id, 'blob_table': table}


def get_blob(cluster_info, args):
    '''given external storage host, blob table name and blob id, get the
    blob content, gzipped and b64-encoded'''
    query = "use {wiki}; select TO_BASE64(blob_text) from {b_table} where blob_id = {b_id};"
    query_formatted = query.format(wiki=args['wiki'],
                                   b_table=cluster_info['blob_table'],
                                   b_id=cluster_info['blob'])
    command = ["echo", "'{query}'".format(query=query_formatted), '|',
               "sshes", "sshes_quiet", cluster_info['host'], 'sudo', '-s',
               'mysql', '-s', '-s', '-N', '-P', cluster_info['port']]
    command = " ".join(command)
    if args['dryrun']:
        log("would run command: {cmd}".format(cmd=command), args)
        return None
    if args['verbose']:
        log("running command: {cmd}".format(cmd=command), args)
    proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
    output, error = proc.communicate()
    blob_text = output.rstrip(b'\n')
    if not blob_text:
        if error:
            log(error.decode('utf-8') + "\n", args)
        raise ValueError("Failed to get blob text for {revid} for {wiki}".format(
            revid=args['revid'], wiki=args['wiki']))
    return blob_text


def blob_convert(blob, args):
    '''given a (presumably) gzipped and base64 encoded blob,
    convert it to the raw content and return that'''
    if args['dryrun'] and blob is None:
        # blob will be None if we are doing a dry run, put a sample
        # here for code checking
        blob = (b'ZVPJjpwwEP0Vy8oRJHZw3+YDImUuI0XJHApT7raaNsiYRK0W/57CRgMKl3L5uZZXi1/cPUfkF/7A\\n' +
                b'ToM2auAR1x0B32tRJyIrGgJ6aLGf+OXF0ayyB3Od4br6ERDxP9DP6+UnGD2xD7DDrOBOKrTD7Jgb\\n' +
                b'WIcjWMdAObTsNvSdNlc2WpwmJgej0KKRyLRhb+6GZuJLxLH/P1V/SPV7TvJWsfUQub+IVYL0eudl\\n' +
                b'vUqZBaPMQ6mH0uDt9dLL6uAMJ+c2GBW7W8jWyoNbdbIMaQQ7UErZgUZyoBHSiQM9HwlUxPZMx+gg\\n' +
                b'T6FkgGS+FxbSQrfrG7fyYIOhYLY/y+5EsDzGLvYWtV3ob6Drn0W9IyLlCw2zw0laPTo90HAvvz4j\\n' +
                b'Pjlw+EDj/F79SJuE8Bd/0BJOBu4ruJ7beoaxR3y0w4jWPQnyLhG/wXSjW5UITGTelFWjhGiLsqwL\\n' +
                b'6iSUSmYVZFJhnUDpt7kDB9savfiXQky0e8ZbPk3cyNTMD7RaxuuPKJKqabbP8e4v65Zu9n/1Xbcw\\n' +
                b'YRzCkNGyP36Vevpb3wpoC1BpFRe1wjiBvIlVVZZxTYUkFY0/zyR5WTDUEaoe6atYpOify/IP')
    blob = blob.decode('unicode_escape')
    try:
        raw = base64.b64decode(blob)
    except Exception:
        # shouldn't ever happen
        log(blob.decode('utf-8'), args)
        raise

    try:
        text = zlib.decompress(raw, -zlib.MAX_WBITS)
    except Exception:
        # maybe the blob isn't gzipped. we don't check the text flags after all...
        log(blob.decode('utf-8'), args)
        raise
    return text.decode('utf8')


def do_main():
    '''entry point'''
    args = get_args()
    validate_args(args)
    text_address = get_text_addr(args)
    cluster_info = get_cluster_info(text_address, args)
    blob_b64_encoded_gzipped = get_blob(cluster_info, args)
    blob_raw = blob_convert(blob_b64_encoded_gzipped, args)
    print(blob_raw)


if __name__ == '__main__':
    do_main()
