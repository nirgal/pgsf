#!/usr/bin/python3

import logging

import psycopg2

from config import (DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_QUOTE_NAMES,
                    DB_SCHEMA, DB_USER)


def connect_string():
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
    if DB_HOST:
        connect_params['host'] = DB_HOST
    if DB_PORT:
        connect_params['post'] = DB_PORT
    if DB_USER:
        connect_params['user'] = DB_USER
    if DB_PASSWORD:
        connect_params['password'] = DB_PASSWORD
    if DB_NAME:
        connect_params['dbname'] = DB_NAME
    connect_string = ' '.join(
            k + '=' + str(v)
            for k, v in connect_params.items())
    return connect_string


def get_pg():
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


def set_autocommit(autocommit):
    '''
    Simple wrapper to set autocommit mode
    See https://www.psycopg.org/docs/usage.html#transactions-control
    '''
    pg = get_pg()
    pg.set_session(autocommit=autocommit)


def pg_escape_str(text):
    '''
    Quote a text, doubling the quote when needed
    '''
    return "'" + text.replace("'", "''") + "'"


def pg_escape_name(name):
    '''
    if DB_QUOTE_NAMES is set in config, quote the name
    '''
    assert '"' not in name
    if DB_QUOTE_NAMES:
        return '"' + name + '"'
    return name


def pg_table_name(name, schema=None):
    '''
    leave schema empty for using config
    usage is temporary tables ( psycopg2.errors.InvalidTableDefinition:
            cannot create temporary relation in non-temporary schema)
    '''
    if schema is None:
        schema = DB_SCHEMA

    if schema:
        result = pg_escape_name(schema)
        result += '.'
    else:
        result = ''
    result += pg_escape_name(name)
    return result
