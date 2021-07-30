#!/usr/bin/python3

import logging

import psycopg2

from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME


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
