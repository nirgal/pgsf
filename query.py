#!/usr/bin/python3

import argparse
import sys

from salesforce import get_Salesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest


def spprint_ordereddict(od):
    '''
    Returns a nice string representation of an Ordered dict
    '''
    result = ''
    for key, value in od.items():
        result += '{}: {}\n'.format(key, value)
    return result


def query(soql, include_deleted=False):
    def check_result(res):
        known_attributes = ('done', 'nextRecordsUrl', 'records', 'totalSize')
        for key in result.keys():
            if key not in known_attributes:
                print("WARNING: Unexpected attribute {} in query result"
                      .format(key),
                      file=sys.stderr)

        if (result.get('done') is not True
                and result.get('nextRecordsUrl') is None):
            print("WARNING: expected 'done' or 'nextRecordsUrl'",
                  file=sys.stderr)

    sf = get_Salesforce()
    try:
        result = sf.query(soql, include_deleted=include_deleted)
    except SalesforceMalformedRequest as e:
        print("ERROR:", e.content[0]['message'], file=sys.stderr)
        return None

    check_result(result)

    print('DEBUG: sf.query got {} record(s).'.format(
            len(result['records'])),
          file=sys.stderr)
    for record in result['records']:
        yield record

    while result.get('nextRecordsUrl'):
        result = sf.query_more(
                result['nextRecordsUrl'],
                identifier_is_url=True,
                include_deleted=include_deleted)
        print('DEBUG: sf.query got {} record(s).'.format(
                len(result['records'])),
              file=sys.stderr)

        check_result(result)

        for record in result['records']:
            yield record


def query_count(soql, include_deleted=False):
    '''
    Simmilar to query, but only returns 'totalSize' attribute.
    This is desirable for queries like "SELECT COUNT() ...".
    '''
    sf = get_Salesforce()
    try:
        result = sf.query(soql, include_deleted=include_deleted)
    except SalesforceMalformedRequest as e:
        print("ERROR:", e.content[0]['message'], file=sys.stderr)
        return None

    return result['totalSize']


if __name__ == '__main__':

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

    if args.count:
        print(query_count(args.soql, args.include_deleted))
    else:
        for record in query(args.soql, args.include_deleted):
            print(spprint_ordereddict(record))
            print
