#!/usr/bin/python3

import argparse
import logging

import psycopg2

from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME


def connect_string():
    connect_params = {}
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='print connection string to stdout')

    args = parser.parse_args()
    print(connect_string())
