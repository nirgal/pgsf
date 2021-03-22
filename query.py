#!/usr/bin/python3

import argparse
import logging
#from concurrent.futures import ThreadPoolExecutor

import config
from salesforce import get_Salesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest

logger = logging.getLogger(__name__)


#def query_cb(soql, chunk_callback, include_deleted=False):

def _check_result(res):
    known_attributes = ('done', 'nextRecordsUrl', 'records', 'totalSize')
    for key in res.keys():
        if key not in known_attributes:
            logger.warning("Unexpected attribute %s in query result", key)


def updated(tablename, start, end):
    sf = get_Salesforce()
    return sf.__getattr__(tablename).updated(start, end)
# from datetime import datetime, timezone
# print(updated('Contact', datetime(2020, 11, 23, 0, 0, 0, tzinfo=timezone.utc), datetime(2099,12,31,0,0,0,tzinfo=timezone.utc)))


def query(soql, include_deleted=False):
    sf = get_Salesforce()
    result = sf.query(soql, include_deleted=include_deleted)
    while True:
        _check_result(result)
        records = result['records']
        logger.info('sf.query got %s record(s).', len(records))
        for record in records:
            yield record
        if not result['done']:
            result = sf.query_more(result['nextRecordsUrl'],
                                   identifier_is_url=True)
        else:
            break


def query_count(soql, include_deleted=False):
    '''
    Simmilar to query, but only returns 'totalSize' attribute.
    This is desirable for queries like "SELECT COUNT() ...".
    '''
    sf = get_Salesforce()
    try:
        result = sf.query(soql, include_deleted=include_deleted)
    except SalesforceMalformedRequest as exc:
        logger.error("%s", exc.content[0]['message'])
        return None

    return result['totalSize']


if __name__ == '__main__':
    def main():
        import json

        parser = argparse.ArgumentParser(
            description='Run an SOQL query Salesforce')
        parser.add_argument(
                '--include-deleted',
                default=False, action='store_true',
                help='include deleted records')
        parser.add_argument(
                '--count',
                default=False, action='store_true',
                help='only prints number of records')
        parser.add_argument(
                'soql',
                help='the query to tun')
        # exemple:
        # SELECT COUNT() FROM Campaign WHERE SystemModStamp>2019-12-18T11:14:55Z
        args = parser.parse_args()

        logging.basicConfig(
                filename=config.LOGFILE,
                format=config.LOGFORMAT.format('query'),
                level=config.LOGLEVEL)

        try:
            if args.count:
                print(query_count(args.soql, args.include_deleted))
            else:
                for record in query(args.soql, args.include_deleted):
                    print(json.dumps(record, indent=2))
        except SalesforceMalformedRequest as exc:
            print(exc.content[0]['message'])

    main()
