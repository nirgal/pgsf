#!/usr/bin/python3

import psycopg2

from config import DB_NAME


class Postgres:

    _connection = None

    def get_connection(self):
        if self._connection is None:
            self._connection = psycopg2.connect("dbname={}".format(DB_NAME))
        return self._connection

    def get_cursor(self):
        return self.get_connection().cursor()
