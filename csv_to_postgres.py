#!/usr/bin/python3

import argparse
import logging
import json

import config
from createtable import postgres_escape_name, postgres_table_name
from tabledesc import TableDesc


def get_pgsql_import(tabledesc, csv_file_name, target_tablename=None):
    if target_tablename is None:
        target_tablename = tabledesc.name
    with open(csv_file_name) as f:
        header = f.readline()[:-1]
        quoted_fields = header.split(',')
        fields = [quoted_field.strip('"') for quoted_field in quoted_fields]

        if config.DB_RENAME_ID:
            fields = [field if field != 'Id' else 'SfId'
                      for field in fields]

        forcenull_fields = []
        for fieldname, fieldinfo in tabledesc.get_sync_fields().items():
            if fieldinfo['nillable']:
                forcenull_fields.append(fieldname)
        if forcenull_fields:
            forcenull_fields = [
                    postgres_escape_name(f) for f in forcenull_fields]
            force_null = ', FORCE_NULL (' + ','.join(forcenull_fields) + ')'
        else:
            force_null = ''
        return """COPY {quoted_table_name} ({fields})
                  FROM STDIN WITH (FORMAT csv, HEADER{force_null})""".format(
                quoted_table_name=postgres_table_name(target_tablename),
                fields=','.join([postgres_escape_name(f) for f in fields]),
                force_null=force_null)


def job_csv_to_postgres(job):
    logger = logging.getLogger(__name__)

    with open(config.JOB_DIR + '/' + job + '/' + 'status.json') as file:
        job_status = json.loads(file.read())
    with open(config.JOB_DIR + '/' + job + '/' + 'batches.json') as file:
        batches = json.loads(file.read())

    table_name = job_status['object']
    td = TableDesc(table_name)

    successfull_csv_files = [
            '{}/{}/{}.{}'.format(
                config.JOB_DIR, job, batch['id'], job_status['contentType'])
            for batch in batches
            if batch['state'] == 'Completed'
            ]

    sql = get_pgsql_import(td, successfull_csv_files[0])

    logger.debug('%s', sql)

    from postgres import get_pg
    pg = get_pg()
    cursor = pg.cursor()
    for csv in successfull_csv_files:
        with open(csv) as file:
            cursor.copy_expert(sql, file)
            logger.debug("rowcount: %s", cursor.rowcount)

    cursor.execute("""
        INSERT INTO sync.status (tablename, syncuntil) VALUES(%s, %s);
        """,
                   (table_name, job_status['systemModstamp']))
    pg.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Import salesforce csv files in postgres')
    parser.add_argument(
            'job',
            help='Job id')
    args = parser.parse_args()

    logging.basicConfig(filename=config.LOGFILE,
            format=config.LOGFORMAT, level=config.LOGLEVEL)

    job_csv_to_postgres(args.job)
