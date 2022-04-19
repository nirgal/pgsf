'''
That module handles the __sync table
'''

import logging

import pg


def get_status(tablename):
    '''
    Returns the status of a table ('ready', 'error', 'runnning', ...)
    '''
    logger = logging.getLogger(__name__)
    cursor = pg.cursor()

    cursor.execute(
        'SELECT status FROM {} WHERE tablename=%s'.format(
            pg.table_name('__sync')
        ), (
            tablename,
        ))

    line = cursor.fetchone()
    if line is None:
        logger.error('TABLE %s not found in __sync', tablename)
        return None

    return line[0]


def update(td, newstatus,
           update_syncuntil=False, update_last_refresh=False,
           required_status=None):
    """
    Update table salesforce.__sync
    """
    logger = logging.getLogger(__name__)

    cursor = pg.cursor()

    field_updates = {
            'status': pg.escape_str(newstatus)
            }
    if update_syncuntil:
        timefield = td.get_timestamp_name()
        field_updates['syncuntil'] = '''
            (
            SELECT max({timefield})
            FROM {quoted_table_dest}
            )'''.format(
                    timefield=pg.escape_name(timefield),
                    quoted_table_dest=pg.table_name(td.name),
                )
    if update_last_refresh:
        field_updates['last_refresh'] = "current_timestamp at time zone 'UTC'"

    sync_name = pg.table_name('__sync')
    updates = ','.join([f'{key}={value}'
                        for key, value
                        in field_updates.items()])
    quoted_tablename = pg.escape_str(f'{td.name}')
    if required_status is not None:
        required_status_esc = pg.escape_str(required_status)
        andcondition = f'AND status={required_status_esc}'
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
    pg.commit()


def insert(td, date_last_refresh):
    '''
    Insert a "table is reasy" entry in sync table
    UTC date should be given a an argument
    '''
    print('refresh:', date_last_refresh)
    cursor = pg.cursor()

    cursor.execute("""
        INSERT INTO {} (tablename, syncuntil, last_refresh, status)
        VALUES(%s, %s, current_timestamp at time zone 'UTC', 'ready')
        ON CONFLICT (tablename)
        DO
            UPDATE
            SET syncuntil=EXCLUDED.syncuntil,
                last_refresh=EXCLUDED.last_refresh,
                status='ready'
        """.format(
                pg.table_name('__sync')
            ), (
                td.name,
                date_last_refresh))

    pg.commit()  # TODO remove that?
