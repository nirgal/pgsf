#!/usr/bin/python3

import argparse
import logging
import sys

import psutil

import config
from createtable import postgres_table_name
from postgres import get_pg
from query_poll_table import update_sync_table
from tabledesc import TableDesc


def get_sync_status(tablename):
    logger = logging.getLogger(__name__)
    pg = get_pg()
    cursor = pg.cursor()

    cursor.execute(
        'SELECT status FROM {} WHERE tablename=%s'.format(
            postgres_table_name('__sync')
        ), (
            tablename,
        ))

    line = cursor.fetchone()
    if line is None:
        logger.error(f'TABLE {tablename} not found in __sync')
        return None

    return line[0]


def find_refresh_process(tablename, sync_check=True):
    logger = logging.getLogger(__name__)

    if sync_check:
        status = get_sync_status(tablename)
        if status != 'running':
            logger.error(f'TABLE {tablename} status is {status}')
            return
    else:
        logger.debug('Skipping sync table checks')

    for proc in psutil.process_iter():
        cmdline = proc.cmdline()
        if (
                len(cmdline) >= 3
                and 'python' in cmdline[0]
                and 'query_poll_table' in cmdline[1]
                and cmdline[2] == tablename
           ):
            return proc


if __name__ == '__main__':
    def main():
        parser = argparse.ArgumentParser(
            description='Abort a table refresh')

        parser.add_argument(
                'table',
                help='table name')
        parser.add_argument(
                '--no-check-sync',
                default=False, action='store_true',
                help="Don't check __sync table")

        args = parser.parse_args()

        logging.basicConfig(
                filename=config.LOGFILE,
                format=config.LOGFORMAT.format('abort_refresh '+args.table),
                level=config.LOGLEVEL)

        tablename = args.table
        sync_check = not args.no_check_sync

        logger = logging.getLogger(__name__)

        td = TableDesc(tablename)

        proc = find_refresh_process(tablename, sync_check)
        if not proc:
            logger.error('Process not found')
            print('Process not found', file=sys.stderr)
            return 1

        print(proc)

        update_sync_table(td, 'error')

        proc.kill()

    main()
