#!/usr/bin/python3

import argparse
import logging

import psycopg2

import config
import pg
from createtable import get_pgsql_create
from csv_to_postgres import job_csv_to_postgres
from download import download
from query_bulk import make_query
from tabledesc import TableDesc

if __name__ == '__main__':
    #main query bulk
    def main():
        parser = argparse.ArgumentParser(
                description='Start a query job in salesforce')
        parser.add_argument('table', help='table name')
        args = parser.parse_args()

        logging.basicConfig(
                filename=config.LOGFILE,
                format=config.LOGFORMAT.format('reload_table '+args.table),
                level=config.LOGLEVEL)
        logger = logging.getLogger(__name__)

        table_name = args.table
        tabledesc = TableDesc(table_name)
        job = make_query(tabledesc)

        logger.info('Created job %s', job)
        print('Created job {}'.format(job))

        download(job)

        print('Download finished')

    ## main create table

        drop_table = 'DROP TABLE IF EXISTS salesforce."{}";'.format(args.table)
        print(drop_table)
        cursor = pg.cursor()
        try:
            cursor.execute(drop_table)
        except (Exception, psycopg2.ProgrammingError) as exc:
            logging.error('Error while excuting drop : %s', drop_table)
            raise exc
        pg.commit()

        sql = get_pgsql_create(args.table)

        cursor = pg.cursor()
        for line in sql:
            try:
                cursor.execute(line)
            except (Exception, psycopg2.ProgrammingError) as exc:
                logging.error('Error while executing %s', line)
                raise exc
        pg.commit()
        
        print('table created')

    #main csv to postgre 

        job_csv_to_postgres(job)
        print('csv to postgres finished')

    main()
