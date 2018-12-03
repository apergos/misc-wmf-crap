# -*- coding: utf-8 -*-
"""
common arg handling and messages
"""


def get_flag(opt, args, usage):
    '''
    set boolean flag in args dict if the flag is
    one of the below
    '''
    if opt in ['-d', '--dryrun']:
        args['dryrun'] = True
    elif opt in ['-v', '--verbose']:
        args['verbose'] = True
    elif opt in ['-h', '--help']:
        usage("Help for this script\n")
    else:
        return False
    return True


def check_mandatory_args(args, argnames_to_check, usage):
    '''
    make sure all mandatory args are present and have a value
    '''
    for argname in argnames_to_check:
        if argname not in args:
            usage("Mandatory argument --{arg} not ".format(arg=argname) +
                  "specified on command line or in config file")
        if not args[argname]:
            usage("Mandatory argument --{arg} cannot be empty".format(arg=argname))


doc = {}

doc['settings'] = """
    --settings  (-s)   File with global settings which may include:
                       location of the MediaWiki installation, path to
                       the multiversion directory if any, the path to
                       the php binary, and so on. For more information,
                       see the file sample.conf
                       Default: none
"""
doc['php'] = """
    --php       (-p)   Path to php command, used for grabbing db creds
                       and possibly a list of db servers via MediaWiki
                       maintenance scripts
                       Default: /usr/bin/php
"""
doc['yamlfile'] = """
    --yamlfile  (-y)   File with yaml-formatted list of db servers, wiki db names
                       and variable names for substitution into the query template
                       Default: none
"""
doc['queryfile'] = """
    --queryfile  (-q)  File with queries, possibly containing variable names consisting
                       of upper case strings starting with $, which will have values
                       from the yaml files substituted in before the queries are run
                       Default: none
"""

doc['flags'] = """
Flags:
    --dryrun    (-d)   Don't execute queries but show what would be done
    --verbose   (-v)   Display progress messages as queries are executed on the wikis
    --help      (-h)   Show this message
"""

doc['formats'] = """
Query file format:

Content should consist of standard SQL queries. Each query may be on one or
several lines. Lines that start with five or more hypens (-----) are taken
as separators between queries. Variable names to be interpolated must be
in all caps and beginning with a dollar sign. See sample-queries.sql for an
example of running queries and sample-explains.sql for an example of queries
in a format to be SHOW EXPLAINed.

Yaml file format:

Content should be yaml, describing servers, wikis and variable names and values.
Variable names must correspond to the variables in the query file, although
they may be in any case. See sample-settings.yaml for an example. The file
sample-explain-settings.yaml has the exact same format but fewer entries for
(my) testing convenience.
"""


def get_common_arg_docs(names):
    """
    remove first and last lines, presumed to be blank (if not, you
    know who to blame)
    """
    docs = "\n".join([doc[name][1:-1] for name in names]) + '\n'
    return docs


def get_arg_defaults(opts, flags):
    '''
    return a dict with None for the opts list and False for the flag list
    '''
    args = {}
    for opt in opts:
        args[opt] = None
    for flag in flags:
        args[flag] = False
    return args