#!/usr/bin/python3

import argparse
import logging

import config
from salesforce import get_Salesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest


def query(soql, include_deleted=False):
    logger = logging.getLogger(__name__)

    def check_result(res):
        known_attributes = ('done', 'nextRecordsUrl', 'records', 'totalSize')
        for key in result.keys():
            if key not in known_attributes:
                logger.warning("Unexpected attribute %s in query result", key)

        if (result.get('done') is not True
                and result.get('nextRecordsUrl') is None):
            logger.warning("Expected 'done' or 'nextRecordsUrl'")

    sf = get_Salesforce()
    try:
        result = sf.query(soql, include_deleted=include_deleted)
    except SalesforceMalformedRequest as e:
        logger.error("%s", e.content[0]['message'])
        return None

    check_result(result)

    logger.info('sf.query got %s record(s).', len(result['records']))

    for record in result['records']:
        yield record

    while result.get('nextRecordsUrl'):
        result = sf.query_more(
                result['nextRecordsUrl'],
                identifier_is_url=True,
                include_deleted=include_deleted)
        logger.info('sf.query got %s record(s).', len(result['records']))

        check_result(result)

        for record in result['records']:
            yield record


def query_count(soql, include_deleted=False):
    '''
    Simmilar to query, but only returns 'totalSize' attribute.
    This is desirable for queries like "SELECT COUNT() ...".
    '''
    logger = logging.getLogger(__name__)
    sf = get_Salesforce()
    try:
        result = sf.query(soql, include_deleted=include_deleted)
    except SalesforceMalformedRequest as e:
        logger.error("%s", e.content[0]['message'])
        return None

    return result['totalSize']


if __name__ == '__main__':
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

    if args.count:
        print(query_count(args.soql, args.include_deleted))
    else:
        for record in query(args.soql, args.include_deleted):
            print(json.dumps(record, indent=2))
            print
