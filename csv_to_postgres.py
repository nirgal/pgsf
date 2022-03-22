#!/usr/bin/python3

import argparse
import json
import logging

import config
import pg
import synctable
from abort_refresh import kill_refresh
from tabledesc import TableDesc


def get_pgsql_import(tabledesc,
                     csv_file_name,
                     target_tablename=None,
                     schema=None):
    """
    schema is set to '' for temporary tables
    else the config is used: use None as a parameter
    """
    if target_tablename is None:
        target_tablename = tabledesc.name
    with open(csv_file_name) as f:
        header = f.readline()[:-1]
        quoted_fields = header.split(',')
        fields = [quoted_field.strip('"') for quoted_field in quoted_fields]

        forcenull_fields = []
        for fieldname, fieldinfo in tabledesc.get_sync_fields().items():
            if fieldinfo['nillable']:
                forcenull_fields.append(fieldname)
        if forcenull_fields:
            forcenull_fields = [
                    pg.escape_name(f) for f in forcenull_fields]
            force_null = ', FORCE_NULL (' + ','.join(forcenull_fields) + ')'
        else:
            force_null = ''
        return """COPY {quoted_table_name} ({fields})
                  FROM STDIN WITH (FORMAT csv, HEADER{force_null})""".format(
                quoted_table_name=pg.table_name(
                    target_tablename,
                    schema),
                fields=','.join([pg.escape_name(f) for f in fields]),
                force_null=force_null)


def job_csv_to_postgres(job, autocommit=True):
    logger = logging.getLogger(__name__)

    with open(config.JOB_DIR + '/' + job + '/' + 'status.json') as file:
        job_status = json.loads(file.read())
    with open(config.JOB_DIR + '/' + job + '/' + 'batches.json') as file:
        batches = json.loads(file.read())

    table_name = job_status['object']

    kill_refresh(kill_refresh, sync_check=False)

    if autocommit:
        pg.set_autocommit(True)
    cursor = pg.cursor()

    td = TableDesc(table_name)

    if int(job_status['numberRecordsProcessed']):

        sql = "TRUNCATE TABLE {quoted_table_name}".format(
            quoted_table_name=pg.table_name(table_name))
        logger.debug(sql)
        cursor.execute(sql)

        successfull_csv_files = [
                '{}/{}/{}.{}'.format(
                    config.JOB_DIR,
                    job,
                    batch['id'],
                    job_status['contentType'])
                for batch in batches
                if batch['state'] == 'Completed'
                ]

        sql = get_pgsql_import(td, successfull_csv_files[0])

        logger.debug('%s', sql)

        for csv in successfull_csv_files:
            with open(csv) as file:
                cursor.copy_expert(sql, file)
                logger.debug("rowcount: %s", cursor.rowcount)
    else:
        logger.critical('%s is empty', table_name)

    synctable.insert_sync_table(td, job_status['systemModstamp'])

    if not autocommit:
        pg.commit()


if __name__ == '__main__':
    def main():
        parser = argparse.ArgumentParser(
            description='Import salesforce csv files in postgres')
        parser.add_argument(
                '--autocommit',
                action='store_true',
                help='enable autocommit')
        parser.add_argument(
                'job',
                help='Job id')
        args = parser.parse_args()

        logging.basicConfig(
                filename=config.LOGFILE,
                format=config.LOGFORMAT.format('csv_to_postgres '+args.job),
                level=config.LOGLEVEL)

        job_csv_to_postgres(args.job, args.autocommit)

    main()
