#!/usr/bin/python3

import argparse
import csv
import io
import logging

import config
from salesforce import get_SalesforceBulk
from tabledesc import TableDesc

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


def csv_split(csvfilename, max_size=10000000, max_records=10000):
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
            chunk_nb_lines = 1
            continue
        if (chunk_nb_lines >= max_records
           or len(buff) + len(line) >= max_size):
            logger.debug(
                    "Chunk with %s bytes, %s lines",
                    len(buff), chunk_nb_lines)
            yield io.StringIO(buff)
            buff = headers
            chunk_nb_lines = 1
        buff += line
        chunk_nb_lines += 1
    logger.debug("Chunk with %s bytes, %s lines", len(buff), chunk_nb_lines)
    yield io.StringIO(buff)


def upload_csv(tabledesc, csvfilename):
    logger = logging.getLogger(__name__)

    bulk = get_SalesforceBulk()
    jobid = bulk.create_update_job(tabledesc.name, contentType='CSV')

    for chunk in csv_split(csvfilename):
        batchid = bulk.post_batch(jobid, chunk)
        bulk.wait_for_batch(jobid, batchid)
        logger.debug("%s", bulk.get_batch_results(batchid))

    bulk.close_job(jobid)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Upload a table to salesforce')
    parser.add_argument(
            'sftable',
            help='salesforce table name')
    parser.add_argument(
            'csvfile',
            help='file to upload')
    args = parser.parse_args()

    logging.basicConfig(
            filename=config.LOGFILE,
            format=config.LOGFORMAT,
            level=config.LOGLEVEL)

    td = TableDesc(args.sftable)
    csvfilename = args.csvfile

    upload_csv(td, csvfilename)
