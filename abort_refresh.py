#!/usr/bin/python3

import argparse
import logging
import sys

import psutil

import config
from postgres import get_pg, pg_table_name
from synctable import get_sync_status, update_sync_table
from tabledesc import TableDesc


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


def kill_refresh(tablename, sync_check=True):
    logger = logging.getLogger(__name__)

    td = TableDesc(tablename)

    proc = find_refresh_process(tablename, sync_check)
    if not proc:
        logger.error('Process not found')
        return False

    update_sync_table(td, 'error')

    proc.kill()

    return True


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

        if not kill_refresh(tablename, sync_check):
            print('Failed', file=sys.stderr)

    main()
