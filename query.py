#!/usr/bin/python3

import argparse
import sys

from salesforce import get_SalesforceBulk
from tabledesc import TableDesc


def make_query(tabledesc,
               content_type='CSV',
               where=None, limit=None,
               pk_chunking=True):
    table_name = tabledesc.name
    fields = tabledesc.get_sync_field_names()

    bulk = get_SalesforceBulk()
    job = bulk.create_query_job(table_name,
                                contentType=content_type,
                                pk_chunking=pk_chunking)
    query = 'SELECT ' + ','.join(fields) + ' FROM ' + table_name
    if where:
        query += ' WHERE ' + where
    if limit:
        query += ' LIMIT ' + str(limit)
    print("Query: ", query, file=sys.stderr)
    bulk.query(job, query)
    # bulk.close_job(job)

    return job


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Start a query job in salesforce')
    parser.add_argument(
            'table',
            help='table name')
    parser.add_argument(
            '--where',
            help='condition')
    parser.add_argument(
            '--limit',
            type=int,
            help='limit number of rows')
    parser.add_argument(
            '--content-type',
            choices=('JSON', 'CSV'),
            default='CSV',
            help='limit number of rows')
    parser.add_argument(
            '--pk-chunking',
            type=int,
            help='chunk size')
    parser.add_argument(
            '--no-pk-chunking',
            action='store_true',
            help='disable pk chuncking')
    args = parser.parse_args()

    if args.pk_chunking:
        pk_chunking = args.pk_chunking
    elif args.no_pk_chunking:
        pk_chunking = None
    else:
        pk_chunking = True

    table_name = args.table
    tabledesc = TableDesc(table_name)
    job = make_query(
            tabledesc, where=args.where, limit=args.limit,
            pk_chunking=pk_chunking)

    print('Created job', job, file=sys.stderr)
