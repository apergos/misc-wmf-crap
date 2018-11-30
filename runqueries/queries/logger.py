# -*- coding: utf-8 -*-
"""
set up logging configuration

modules using this library ought to call this
to set up logging
"""


import logging
import logging.config
import sys


def logging_setup():
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
                'level': 'WARNING',
                'class': 'logging.StreamHandler',
                'stream': sys.stderr,
                'formatter': 'simple'
            },
            'file': {
                'level': 'INFO',
                'class': 'logging.FileHandler',
                'filename': 'sql_checker_errors.log',
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
                'handlers': ['console'],
                'level': 'WARNING',
                'propagate': True
            }
        }
    })
