# -*- coding: utf-8 -*-
"""
set up logging configuration

modules using this library ought to call this
to set up logging
"""


import logging
import logging.config
import sys


def logging_setup(logfile):
    '''
    standard logging handlers and formatters for all
    library modules
    '''
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': "[%(levelname)s]: %(message)s"
            },
        },
        'handlers': {
            'console': {
                'level': 'ERROR',
                'class': 'logging.StreamHandler',
                'stream': sys.stderr,
                'formatter': 'simple'
            },
            'file': {
                'level': 'INFO',
                'class': 'logging.FileHandler',
                'filename': logfile,
                'formatter': 'simple'
            },
        },
        'loggers': {
            'verbose': {
                'handlers': ['console', 'file'],
                'level': 'INFO',
                'propagate': True
            },
            'normal': {
                'handlers': ['console', 'file'],
                'level': 'WARNING',
                'propagate': True
            }
        }
    })


def get_logger(args):
    '''
    return a logger which will log to the right file
    at the right verbosity
    '''
    logging_setup(args['logfile'])
    if args['verbose'] or args['dryrun']:
        log_type = 'verbose'
    else:
        log_type = 'normal'
    return logging.getLogger(log_type)    # pylint: disable=invalid-name
