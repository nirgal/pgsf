#!/usr/bin/python3

import argparse
import csv
import io
import logging
import time

import requests

import config
from salesforce import get_SalesforceBulk
from tabledesc import TableDesc

DEFAULT_MAX_UPLOAD_SIZE = 10000000
DEFAULT_MAX_UPLOAD_RECORDS = 10000
csvdialect = {
    'delimiter': ',',
    'doublequote': True,
    'escapechar': None,
    'lineterminator': '\n',
    'quotechar': '"',
    'quoting': csv.QUOTE_MINIMAL,
    'skipinitialspace': False,
    'strict': True,
}


def csv_reader(csvfilename):
    """
    Takes a postgresql csv file (commas, headers, escape " as "")
    Yields lines, as string (not list)
    """
    with open(csvfilename) as csvfile:
        reader = csv.reader(csvfile, **csvdialect)
        for line in reader:
            buf = io.StringIO()
            writer = csv.writer(buf, **csvdialect)
            writer.writerow(line)
            yield buf.getvalue()


def csv_split(
        csvfilename,
        max_size=DEFAULT_MAX_UPLOAD_SIZE,
        max_records=DEFAULT_MAX_UPLOAD_RECORDS):
    """
    Takes a postgresql csv file (commas, headers, escape " as "")
    Yield readable StringIOs with the same format, and a maximum size
    """
    logger = logging.getLogger(__name__)

    headers = None
    buff = ''

    for line in csv_reader(csvfilename):
        if headers is None:
            headers = line
            buff = headers
            chunk_nb_lines = 0
            continue
        if (chunk_nb_lines >= max_records
           or len(buff) + len(line) >= max_size):
            logger.debug(
                    "Chunk with %s bytes, %s lines",
                    len(buff), chunk_nb_lines)
            yield io.StringIO(buff)
            buff = headers
            chunk_nb_lines = 0
        buff += line
        chunk_nb_lines += 1
    logger.debug("Chunk with %s bytes, %s lines", len(buff), chunk_nb_lines)
    yield io.StringIO(buff)


def upload_csv(
        tabledesc,
        csvfilename,
        max_size=DEFAULT_MAX_UPLOAD_SIZE,
        max_records=DEFAULT_MAX_UPLOAD_RECORDS):
    logger = logging.getLogger(__name__)

    bulk = get_SalesforceBulk()
    jobid = bulk.create_update_job(tabledesc.name, contentType='CSV')

    for chunk in csv_split(csvfilename, max_size, max_records):
        batchid = bulk.post_batch(jobid, chunk)
        while True:
            try:
                bulk.wait_for_batch(jobid, batchid)
            except requests.exceptions.ConnectionError as exc:
                logger.error('wait_for_batch failed, retrying...: %s', exc)
                time.sleep(1)
            else:
                break
        logger.debug("%s", bulk.get_batch_results(batchid))

    bulk.close_job(jobid)


if __name__ == '__main__':
    def main():
        parser = argparse.ArgumentParser(
            description='Upload a scv file into a salesforce table',
            epilog='This uses a single Salesforce Bulk V1 "update" API.'
                   ' The CSV file is cut in chunks.'
                   ' Each chunk is submited as a bacth in the job.'
            )
        parser.add_argument(
            '--max-upload-size',
            type=int,
            default=DEFAULT_MAX_UPLOAD_SIZE,
            help='cut csv file in chunks no larger than %(metavar)s bytes.'
                 ' default=%(default)s',
            metavar='SIZE',
            )
        parser.add_argument(
            '--max-upload-records',
            type=int,
            default=DEFAULT_MAX_UPLOAD_RECORDS,
            help='cut csv file in chunks with no more than %(metavar)s records.'
                 ' default=%(default)s',
            metavar='LIMIT',
            )
        parser.add_argument(
                'sftable',
                help='salesforce table name')
        parser.add_argument(
                'csvfile',
                help='file to upload')
        args = parser.parse_args()

        logging.basicConfig(
                filename=config.LOGFILE,
                format=config.LOGFORMAT.format('upload_table '+args.sftable+' '+args.csvfile),
                level=config.LOGLEVEL)

        td = TableDesc(args.sftable)
        csvfilename = args.csvfile

        upload_csv(
                td,
                csvfilename,
                max_size=args.max_upload_size,
                max_records=args.max_upload_records)

    main()
