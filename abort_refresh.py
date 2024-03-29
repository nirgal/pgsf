#!/usr/bin/python3
'''
Module to handle cancellation of refreshes.
It handles the system killing of process.
'''

import argparse
import logging
import sys

import psutil

import config
import synctable
from tabledesc import TableDesc


def find_refresh_process(tablename, sync_check=True):
    '''
    Find the psutil.Process that is currently refreshing a table.
    Can return None.

    If sync_check, __sync table is read first, verifying the state is
    'running'. Returns None if it is not.
    '''
    logger = logging.getLogger(__name__)

    if sync_check:
        status = synctable.get_status(tablename)
        if status != 'running':
            logger.error('TABLE %s status is %s',
                    tablename, status)
            return None
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
    return None


def kill_refresh(tablename, sync_check=True):
    '''
    Stop a table from being refreshed.
    Any running refresh process is killed.
    __sync table status is set to 'error'.
    '''
    logger = logging.getLogger(__name__)

    td = TableDesc(tablename)

    proc = find_refresh_process(tablename, sync_check)
    if not proc:
        logger.error('Process not found')
        return False

    synctable.update(td, 'error')

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
