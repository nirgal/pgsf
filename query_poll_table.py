#!/usr/bin/python3

import argparse
import logging
from datetime import datetime, timedelta
from psycopg2 import DatabaseError
import config
from createtable import (postgres_escape_name, postgres_escape_str,
                         postgres_table_name)
from csv_to_postgres import get_pgsql_import
from postgres import get_pg
from query import query
from tabledesc import TableDesc

SAFETY_SYNC_EXTRA = 1  # number of seconds we remove from now

def create_csv_query_file(tablename):
    return '{}/query_{}_{}.csv'.format(
            config.JOB_DIR, tablename,
            datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'))


def _csv_quote(value):
    # return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'
    return '"' + value.replace('"', '""').replace('\0','') + '"'

def postgres_json_to_csv(field, value):
    '''
    Given a field, this converts a json value returned by SF query into a csv
    compatible value.
    '''
    sftype = field['type']
    if value is None:
        return ''
    if sftype in (
            'email', 'encryptedstring', 'id', 'multipicklist',
            'picklist', 'phone', 'reference', 'string', 'textarea', 'url', 'anyType'):
        return _csv_quote(value)
    elif sftype == 'int':
        return str(value)
    elif sftype == 'date':
        return str(value)
    elif sftype == 'datetime':
        return str(value)  # 2019-11-18T15:28:14.000Z TODO check
    elif sftype == 'boolean':
        return 't' if value else 'f'
    elif sftype in ('currency', 'double', 'percent'):
        return str(value)
    else:
        return '"{}" NOT IMPLEMENTED '.format(sftype)


def download_changes(td, use_createddate=False):
    '''
    td is a tabledesc object
    returns the name of the csvfile where the changes where downloaded.
    '''
    logger = logging.getLogger(__name__)
    fieldnames = td.get_sync_field_names()

    pg = get_pg()
    cursor = pg.cursor()
    cursor.execute('SELECT syncuntil FROM sync.status WHERE tablename=%s',
                   (td.name,))
    line = cursor.fetchone()
    if line is None:
        logger.critical("Can't find sync info for table %s. "
                        "Please use bulk the first time",
                        td.name)
        return
    lastsync = line[0]  # type is datetime

    soql = "SELECT {} FROM {} WHERE {}>{}".format(
            ','.join(fieldnames),
            td.name,
            'CreatedDate' if use_createddate else 'SystemModStamp',
            lastsync.strftime('%Y-%m-%dT%H:%M:%SZ')  # UTC
            )
    logger.debug("%s", soql)
    qry = query(soql, include_deleted=True)
    output = None
    csvfilename = None
    for record in qry:
        if output is None:
            csvfilename = create_csv_query_file(td.name)
            output = open(csvfilename, 'w')
            output.write(','.join(fieldnames)+'\n')
        csv_formated_fields = []
        for fieldname in fieldnames:
            field = td.get_sync_fields()[fieldname]
            csv_field_value = postgres_json_to_csv(field, record[fieldname])
            csv_formated_fields.append(csv_field_value)
        # print(record)
        # print(','.join(csv_formated_fields)+'\n')
        # output.write(repr(record))
        output.write(','.join(csv_formated_fields)+'\n')
    if output is not None:
        output.close()
    return csvfilename


def pg_merge_update(td, tmp_tablename):
    logger = logging.getLogger(__name__)
    pg = get_pg()
    cursor = pg.cursor()

    fieldnames = td.get_sync_field_names()
    has_isdeleted = 'IsDeleted' in fieldnames
    quoted_table_dest = postgres_table_name(td.name)
    quoted_table_src = postgres_table_name(tmp_tablename, schema='')
    quoted_field_names = ','.join(
            [postgres_escape_name(f) for f in fieldnames])
    excluded_quoted_field_names = ','.join(
            ['EXCLUDED.'+postgres_escape_name(f) for f in fieldnames])
    sql = '''INSERT INTO {quoted_table_dest}
             ( {quoted_field_names} )
             SELECT {quoted_field_names}
             FROM {quoted_table_src}
             {wherenotdeleted}
             ON CONFLICT ( "Id" )
             DO UPDATE
                 SET ( {quoted_field_names} )
                 = ( {excluded_quoted_field_names} )
           '''.format(
            quoted_table_dest=quoted_table_dest,
            quoted_table_src=quoted_table_src,
            quoted_field_names=quoted_field_names,
            excluded_quoted_field_names=excluded_quoted_field_names,
            wherenotdeleted='WHERE NOT "IsDeleted"' if has_isdeleted else ''
            )
    cursor.execute(sql)
    logger.info("pg INSERT/UPDATE rowcount: %s", cursor.rowcount)

    if has_isdeleted:
        sql = '''DELETE FROM {quoted_table_dest}
                 WHERE {id} IN (
                     SELECT {id}
                     FROM {quoted_table_src}
                     WHERE "IsDeleted"
                     )
              '''.format(
              quoted_table_dest=quoted_table_dest,
              quoted_table_src=quoted_table_src,
              id=postgres_escape_name('Id'),
              )
        cursor.execute(sql)
        logger.info("pg DELETE rowcount: %s", cursor.rowcount)


def mark_synced(td, timestamp):
    pg = get_pg()
    cursor = pg.cursor()
    cursor.execute(
        'UPDATE sync.status SET syncuntil=%s WHERE tablename=%s', (
            timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f'),
            td.name))
    if cursor.rowcount != 1:
        raise Error("UPDATE sync.status failed")


def sync_table(tablename, use_createddate=False):
    logger = logging.getLogger(__name__)
    start_time = datetime.utcnow() - timedelta(seconds=SAFETY_SYNC_EXTRA)

    pg = get_pg()
    cursor = pg.cursor()

    td = TableDesc(tablename)
    csvfilename = download_changes(td, use_createddate)
    if csvfilename is None:
        logger.info('No change in table %s', tablename)
    else:
        logger.debug('Downloaded to %s', csvfilename)

        tmp_tablename = 'tmp_' + tablename
        sql = 'CREATE TEMPORARY TABLE {} ( LIKE {} )'.format(
            postgres_table_name(tmp_tablename, schema=''),
            postgres_table_name(tablename))

        cursor.execute(sql)

        sql = get_pgsql_import(td, csvfilename, tmp_tablename, schema='')
        with open(csvfilename) as file:
            cursor.copy_expert(sql, file)
            logger.info("pg COPY rowcount: %s", cursor.rowcount)

        pg_merge_update(td, tmp_tablename)

        sql = 'DROP TABLE {}'.format(
            postgres_table_name(tmp_tablename, schema=''))
        cursor.execute(sql)

    mark_synced(td, start_time)

    pg.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Refresh a table from salesforce to postgres')
    parser.add_argument(
            'table',
            help='the table name to refresh')
    parser.add_argument(
            '--use-createddate',
            action='store_true',
            help='Use CreatedDate instead of SystemModStamp for'
                 ' detecting changes')
    args = parser.parse_args()

    logging.basicConfig(
            filename=config.LOGFILE,
            format=config.LOGFORMAT.format('query_poll_table '+args.table),
            level=config.LOGLEVEL)

    sync_table(args.table, args.use_createddate)
