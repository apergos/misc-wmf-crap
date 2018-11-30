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
