#!/usr/bin/python3

import argparse
import logging
from datetime import datetime

import config
from csv_to_postgres import get_pgsql_import
from postgres import get_conn, pg_escape_name, pg_table_name
from query import query
from synctable import update_sync_table
from tabledesc import TableDesc


def create_csv_query_file(tablename):
    return '{}/query_{}_{}.csv'.format(
            config.JOB_DIR, tablename,
            datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'))


def _csv_quote(value):
    # return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'
    return '"' + value.replace('"', '""').replace('\0', '') + '"'


def postgres_json_to_csv(field, value):
    '''
    Given a field, this converts a json value returned by SF query into a csv
    compatible value.
    '''
    sftype = field['type']
    if value is None:
        return ''
    if sftype in (
            'combobox', 'email', 'encryptedstring', 'id', 'multipicklist',
            'picklist', 'phone', 'reference', 'string', 'textarea', 'url'):
        return _csv_quote(value)
    if sftype == 'anyType':
        return _csv_quote(str(value))
    if sftype == 'int':
        return str(value)
    if sftype == 'date':
        return str(value)
    if sftype == 'datetime':
        return str(value)  # 2019-11-18T15:28:14.000Z TODO check
    if sftype == 'boolean':
        return 't' if value else 'f'
    if sftype in ('currency', 'double', 'percent'):
        return str(value)
    return '"{}" NOT IMPLEMENTED '.format(sftype)


def download_changes(td):
    '''
    td is a tabledesc object
    returns the name of the csvfile where the changes where downloaded.
    '''
    logger = logging.getLogger(__name__)
    fieldnames = td.get_sync_field_names()

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        'SELECT syncuntil FROM {} WHERE tablename=%s'.format(
            pg_table_name('__sync')
        ), (
            td.name,
        ))
    line = cursor.fetchone()
    if line is None:
        logger.critical("Can't find sync info for table %s. "
                        "Please use bulk the first time",
                        td.name)
        return None
    lastsync = line[0]  # type is datetime

    timefield = td.get_timestamp_name()

    soql = "SELECT {} FROM {} WHERE {}>{}".format(
            ','.join(fieldnames),
            td.name,
            timefield,
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
    conn = get_conn()
    cursor = conn.cursor()

    fieldnames = td.get_sync_field_names()
    has_isdeleted = 'IsDeleted' in fieldnames
    quoted_table_dest = pg_table_name(td.name)
    quoted_table_src = pg_table_name(tmp_tablename, schema='')
    quoted_field_names = ','.join(
            [pg_escape_name(f) for f in fieldnames])
    excluded_quoted_field_names = ','.join(
            ['EXCLUDED.'+pg_escape_name(f) for f in fieldnames])
    sql = '''INSERT INTO {quoted_table_dest}
             ( {quoted_field_names} )
             SELECT {quoted_field_names}
             FROM {quoted_table_src}
             {wherenotdeleted}
             ON CONFLICT ( {id} )
             DO UPDATE
                 SET ( {quoted_field_names} )
                 = ( {excluded_quoted_field_names} )
           '''.format(
            quoted_table_dest=quoted_table_dest,
            quoted_table_src=quoted_table_src,
            quoted_field_names=quoted_field_names,
            id=pg_escape_name(td.get_pk_fieldname()),
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
              id=pg_escape_name(td.get_pk_fieldname()),
              )
        cursor.execute(sql)
        logger.info("pg DELETE rowcount: %s", cursor.rowcount)


def sync_table(tablename):
    logger = logging.getLogger(__name__)

    td = TableDesc(tablename)

    update_sync_table(td, 'running', required_status='ready')

    try:
        csvfilename = download_changes(td)

        if csvfilename is None:
            logger.info('No change in table %s')

            update_sync_table(td, 'ready', update_last_refresh=True)

        else:
            conn = get_conn()
            cursor = conn.cursor()

            logger.debug('Downloaded to %s', csvfilename)

            tmp_tablename = 'tmp_' + tablename
            sql = 'CREATE TEMPORARY TABLE {} ( LIKE {} )'.format(
                pg_table_name(tmp_tablename, schema=''),
                pg_table_name(tablename))

            cursor.execute(sql)

            sql = get_pgsql_import(td, csvfilename, tmp_tablename, schema='')
            with open(csvfilename) as file:
                cursor.copy_expert(sql, file)
                logger.info("pg COPY rowcount: %s", cursor.rowcount)

            pg_merge_update(td, tmp_tablename)

            sql = 'DROP TABLE {}'.format(
                pg_table_name(tmp_tablename, schema=''))
            cursor.execute(sql)

            update_sync_table(
                    td, 'ready',
                    update_syncuntil=True,
                    update_last_refresh=True)

            conn.commit()
    except Exception as e:
        # Re-raise exception, so that stderr as a message
        # cron will mail it
        # TODO: detect some errors like a column that disapeared
        update_sync_table(td, 'ready')
        raise e


if __name__ == '__main__':
    def main():
        parser = argparse.ArgumentParser(
            description='Refresh a table from salesforce to postgres')
        parser.add_argument(
                'table',
                help='the table name to refresh')
        args = parser.parse_args()

        logging.basicConfig(
                filename=config.LOGFILE,
                format=config.LOGFORMAT.format('query_poll_table '+args.table),
                level=config.LOGLEVEL)

        sync_table(args.table)

    main()
