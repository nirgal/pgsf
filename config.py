'''
That module wraps access to configuration file.
Usage:
    import config
    print(config.DB_SCHEMA)
'''

import configparser
from os.path import expanduser


def get_config():
    '''
    Returns the configuration file as a ConfigParser object
    '''
    cfg = configparser.ConfigParser(interpolation=None)
    path = expanduser('~/.pgsf')
    assert cfg.read(path), "Could not read " + path
    return cfg


def get_section(name='DEFAULT'):
    '''
    Returns the ConfigParser section
    '''
    return __cfg[name]


__cfg = get_config()


DB_SCHEMA = __cfg['postgresql'].get('schema', None)

DB_QUOTE_NAMES = __cfg['postgresql'].getboolean('quote_name', False)
GRANT_TO = __cfg['postgresql'].get('grant_to', None)

JOB_DIR = __cfg['DEFAULT']['job_dir']

LOGFILE = __cfg['DEFAULT']['log_file']
LOGFORMAT = __cfg['DEFAULT']['log_format']
LOGLEVEL = __cfg['DEFAULT'].getint('log_level')
