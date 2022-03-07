import configparser
from os.path import expanduser


def get_config():
    cfg = configparser.ConfigParser(interpolation=None)
    path = expanduser('~/.pgsf')
    assert cfg.read(path), "Could not read " + path
    return cfg


__cfg = get_config()

CREDIDENTIALS = {}
CREDIDENTIALS['username'] = __cfg['salesforce']['username']
CREDIDENTIALS['password'] = __cfg['salesforce']['password']
CREDIDENTIALS['security_token'] = __cfg['salesforce']['security_token']
__val = __cfg['salesforce'].get('domain', None)
if __val:
    CREDIDENTIALS['domain'] = __val

SF_API_VERSION = __cfg['salesforce']['api_version']

DB_HOST = __cfg['postgresql'].get('host', None)
DB_PORT = __cfg['postgresql'].get('port', None)
DB_USER = __cfg['postgresql'].get('user', None)
DB_PASSWORD = __cfg['postgresql'].get('password', None)
DB_NAME = __cfg['postgresql'].get('db', None)

DB_SCHEMA = __cfg['postgresql'].get('schema', None)

DB_QUOTE_NAMES = __cfg['postgresql'].getboolean('quote_name', False)
GRANT_TO = __cfg['postgresql'].get('grant_to', None)

JOB_DIR = __cfg['DEFAULT']['job_dir']

LOGFILE = __cfg['DEFAULT']['log_file']
LOGFORMAT = __cfg['DEFAULT']['log_format']
LOGLEVEL = __cfg['DEFAULT'].getint('log_level')
