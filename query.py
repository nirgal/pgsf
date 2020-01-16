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
    sf = get_Salesforce()
    try:
        result = sf.query(soql, include_deleted=include_deleted)
    except SalesforceMalformedRequest as e:
        print("ERROR:", e.content[0]['message'], file=sys.stderr)
        return None

    assert result['done']
    if result['done'] is not True:
        print("QUERY returned done =", result['done'], file=sys.stderr)
        return None

    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run an SOQL query Salesforce')
    parser.add_argument(
            '--include-deleted',
            default=False, action='store_true',
            help='include deleted records')
    parser.add_argument(
            'soql',
            help='the query to tun')
    args = parser.parse_args()

    result = query(args.soql, args.include_deleted)
    # exemple:
    # SELECT COUNT() FROM Campaign WHERE SystemModStamp>2019-12-18T11:14:55Z

    nrecords = result['totalSize']
    try:
        firstrecord = result['records'][0]
    except IndexError:
        print("QUERY", nrecords, "records", file=sys.stderr)
    else:
        recordtype = firstrecord['attributes']['type']
        print("QUERY", nrecords, recordtype, "records", file=sys.stderr)

    for record in result['records']:
        print(spprint_ordereddict(record))
        print

    # print(result.keys())
    # print('Created job', job, file=sys.stderr)
