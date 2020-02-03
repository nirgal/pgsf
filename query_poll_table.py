#!/usr/bin/python3

import argparse
import sys
from datetime import datetime

from config import JOB_DIR
from createtable import (postgres_escape_name, postgres_escape_str,
                         postgres_json_to_csv, postgres_table_name)
from csv_to_postgres import get_pgsql_import
from postgres import get_pg
from query import query
from tabledesc import TableDesc


def create_csv_query_file(tablename):
    return '{}/query_{}_{}.csv'.format(
            JOB_DIR, tablename,
            datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'))


def download_changes(td):
    '''
    td is a tabledesc object
    returns the name of the csvfile where the changes where downloaded.
    '''
    fieldnames = td.get_sync_field_names()

    pg = get_pg()
    cursor = pg.cursor()
    cursor.execute('SELECT syncuntil FROM sync.status WHERE tablename=%s',
                   (td.name,))
    line = cursor.fetchone()
    if line is None:
        print("Can't find sync info for table {}. "
              "Please use bulk the first time".format(
                  td.name),
              file=sys.stderr)
        return
    lastsync = line[0]  # type is datetime

    pg.commit()

    soql = "SELECT {} FROM {} WHERE SystemModStamp>{}".format(
            ','.join(fieldnames),
            td.name,
            lastsync.strftime('%Y-%m-%dT%H:%M:%SZ')  # UTC
            )
    print(soql, file=sys.stderr)
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
    pg = get_pg()
    cursor = pg.cursor()

    fieldnames = td.get_sync_field_names()
    quoted_table_dest = postgres_table_name(td.name)
    quoted_table_src = postgres_table_name(tmp_tablename)
    quoted_field_names = ','.join(
            [postgres_escape_name(f) for f in fieldnames])
    excluded_quoted_field_names = ','.join(
            ['EXCLUDED.'+postgres_escape_name(f) for f in fieldnames])
    sql = '''INSERT INTO {quoted_table_dest} 
             ( {quoted_field_names} )
             SELECT {quoted_field_names}
             FROM {quoted_table_src}
             WHERE NOT "IsDeleted"
             ON CONFLICT ( "Id" )
             DO UPDATE
                 SET ( {quoted_field_names} ) = ( {excluded_quoted_field_names} )
           '''.format(
            quoted_table_dest=quoted_table_dest,
            quoted_table_src=quoted_table_src,
            quoted_field_names=quoted_field_names,
            excluded_quoted_field_names=excluded_quoted_field_names,
            )
    cursor.execute(sql)
    print("pg INSERT rowcount:", cursor.rowcount, file=sys.stderr)

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
    print("pg DELETE rowcount:", cursor.rowcount, file=sys.stderr)

    sql='''UPDATE sync.status
           SET syncuntil=(
               select max("SystemModstamp")
               FROM {quoted_table_dest}
               )
           WHERE tablename={str_table_name}
        '''.format(
                quoted_table_dest=quoted_table_dest,
                str_table_name=postgres_escape_str(td.name),
                )
    cursor.execute(sql)


    pg.commit()


def sync_table(tablename):
    td = TableDesc(tablename)
    csvfilename = download_changes(td)
    if csvfilename is None:
        print('No change in table', tablename, file=sys.stderr)
        return
    print('Downloaded to', csvfilename, file=sys.stderr)


    pg = get_pg()
    cursor = pg.cursor()

    tmp_tablename = 'tmp_' + tablename
    sql = 'CREATE TABLE {} ( LIKE {} )'.format(
        postgres_table_name(tmp_tablename),
        postgres_table_name(tablename))

    cursor.execute(sql)

    sql = get_pgsql_import(td, csvfilename, tmp_tablename)
    with open(csvfilename) as file:
        cursor.copy_expert(sql, file)
        print("pg COPY rowcount:", cursor.rowcount, file=sys.stderr)
    pg.commit()

    pg_merge_update(td, tmp_tablename)

    sql = 'DROP TABLE {}'.format(
        postgres_table_name(tmp_tablename))
    cursor.execute(sql)
    pg.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Refresh a table from salesforce to postgres')
    parser.add_argument(
            'table',
            help='the table name to refresh')
    args = parser.parse_args()

    sync_table(args.table)
