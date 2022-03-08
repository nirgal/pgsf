'''
That module handles the __sync table
'''

import logging

from postgres import get_conn, pg_escape_name, pg_escape_str, pg_table_name


def get_sync_status(tablename):
    '''
    Returns the status of a table ('ready', 'error', 'runnning', ...)
    '''
    logger = logging.getLogger(__name__)
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute(
        'SELECT status FROM {} WHERE tablename=%s'.format(
            pg_table_name('__sync')
        ), (
            tablename,
        ))

    line = cursor.fetchone()
    if line is None:
        logger.error(f'TABLE {tablename} not found in __sync')
        return None

    return line[0]


def update_sync_table(td, newstatus,
                      update_syncuntil=False, update_last_refresh=False,
                      required_status=None):
    """
    Update table salesforce.__sync
    """
    logger = logging.getLogger(__name__)

    conn = get_conn()
    cursor = conn.cursor()

    field_updates = {
            'status': pg_escape_str(newstatus)
            }
    if update_syncuntil:
        timefield = td.get_timestamp_name()
        field_updates['syncuntil'] = '''
            (
            SELECT max({timefield})
            FROM {quoted_table_dest}
            )'''.format(
                    timefield=pg_escape_name(timefield),
                    quoted_table_dest=pg_table_name(td.name),
                )
    if update_last_refresh:
        field_updates['last_refresh'] = "current_timestamp at time zone 'UTC'"

    sync_name = pg_table_name('__sync')
    updates = ','.join([f'{key}={value}'
                        for key, value
                        in field_updates.items()])
    quoted_tablename = pg_escape_str(f'{td.name}')
    if required_status is not None:
        required_status_esc = pg_escape_str(required_status)
        andcondition = f"AND status={required_status_esc}"
    else:
        andcondition = ''

    sql = f'''UPDATE {sync_name}
        SET {updates}
        WHERE tablename={quoted_tablename}
            {andcondition}
        '''

    # print(sql)
    cursor.execute(sql)
    if cursor.rowcount == 0:
        logger.error('Cannot update __sync')
        # TODO print the current status
    conn.commit()
