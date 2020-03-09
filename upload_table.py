#!/usr/bin/python3

import argparse
import csv
import logging
import sys

import config
from salesforce import get_SalesforceBulk
from tabledesc import TableDesc

def csv_split(csvfilename, max_size=1000000):
    """
    Takes a postgresql csv file (commas, headers, escape " as "")
    Yield readable StringIOs with the same format, and a maximum size
    """
    dialect = {
        'delimiter': ',',
        'doublequote': True,
        'escapechar': None,
        'lineterminator': '\n',
        'quotechar': '"',
        'quoting': csv.QUOTE_MINIMAL,
        'skipinitialspace': False,
        'strict': True,
    }
    csvfile = open(csvfilename)
    reader = csv.reader(csvfile, **dialect)
    #writer = csv.writer(
    lines = 0
    for line in reader:
        lines += 1
        print(line)
    print("Found", lines, "lines", file=sys.stderr)


def upload_csv(tabledesc, csvfilename):
    bulk = get_SalesforceBulk()
    jobid = bulk.create_update_job(tabledesc.name, contentType='CSV')

    # for....
    #csvfile = open(csvfilename).read()
    content = open(csvfilename)
    batchid = bulk.post_batch(jobid, content)
    bulk.close_job(jobid)

    bulk.wait_for_batch(jobid, batchid)

    print(bulk.get_batch_results(batchid), file=sys.stderr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Upload a table to salesforce')
    parser.add_argument(
            'sftable',
            help='salesforce table name')
    parser.add_argument(
            'csvfile',
            help='file to upload')
    #parser.add_argument(
    #        'table',
    #        help='local table name to upload')
    args = parser.parse_args()

    logging.basicConfig(
            filename=config.LOGFILE,
            format=config.LOGFORMAT,
            level=config.LOGLEVEL)

    td = TableDesc(args.sftable)
    csvfilename = args.csvfile

    #csv_split(csvfilename)

    upload_csv(td, csvfilename)
