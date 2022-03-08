#!/usr/bin/python3

import argparse
import logging

import psycopg2

import config


def connect_string(with_password=True):
    # some default values:
    connect_params = {
            # seconds of inactivity after which TCP should send a keepalive
            # message to the server
            'keepalives_idle': 10,

            # the number of seconds after which a TCP keepalive message that is
            # not acknowledged by the server should be retransmitted
            'keepalives_interval': 10,

            # the number of TCP keepalives that can be lost before the client's
            # connection to the server is considered dead
            'keepalives_count': 3,
            }
    if config.DB_HOST:
        connect_params['host'] = config.DB_HOST
    if config.DB_PORT:
        connect_params['post'] = config.DB_PORT
    if config.DB_USER:
        connect_params['user'] = config.DB_USER
    if with_password and config.DB_PASSWORD:
        connect_params['password'] = config.DB_PASSWORD
    if config.DB_NAME:
        connect_params['dbname'] = config.DB_NAME
    connect_string = ' '.join(
            k + '=' + str(v)
            for k, v in connect_params.items())
    return connect_string


def get_conn():
    '''
    Return *the* common psycopg connection to the database
    based on config
    '''
    global __pg_connection
    try:
        return __pg_connection
    except NameError:
        logger = logging.getLogger(__name__)
        logger.debug('Opening new connection to postgres')
        __pg_connection = psycopg2.connect(connect_string())
        return __pg_connection


def cursor():
    '''
    Simple wrapper around cursor() for the one connection
    '''
    return get_conn().cursor()


def commit():
    '''
    Simple wrapper around commit for the one connection
    '''
    return get_conn().commit()


def set_autocommit(autocommit):
    '''
    Simple wrapper to set autocommit mode
    See https://www.psycopg.org/docs/usage.html#transactions-control
    '''
    conn = get_conn()
    conn.set_session(autocommit=autocommit)


def escape_str(text):
    '''
    Quote a text, doubling the quote when needed
    '''
    return "'" + text.replace("'", "''") + "'"


def escape_name(name):
    '''
    if DB_QUOTE_NAMES is set in config, quote the name
    '''
    assert '"' not in name
    if config.DB_QUOTE_NAMES:
        return '"' + name + '"'
    return name


def table_name(name, schema=None):
    '''
    leave schema empty for using config
    usage is temporary tables ( psycopg2.errors.InvalidTableDefinition:
            cannot create temporary relation in non-temporary schema)
    '''
    if schema is None:
        schema = config.DB_SCHEMA

    if schema:
        result = escape_name(schema)
        result += '.'
    else:
        result = ''
    result += escape_name(name)
    return result


if __name__ == '__main__':
    def main():
        parser = argparse.ArgumentParser(
            description='Print postgres connection string from config')
        parser.parse_args()

        logging.basicConfig(
                filename=config.LOGFILE,
                format=config.LOGFORMAT.format('pg'),
                level=config.LOGLEVEL)

        # You don't want to print your password. Ever.
        # Create a ~/.pgpass file if you need non-interractive work.
        print(connect_string(with_password=False))

    main()
