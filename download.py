#!/usr/bin/python3

import argparse
import json
import logging
import os
# from pprint import pprint
from time import sleep

import requests

import config
from salesforce import get_SalesforceBulk

# def write_csv_from_csv(outputfile, tabledesc, inputresult, write_header):
#     r = inputresult.read()
#     ru = str(r, encoding='utf-8')
#     header, ru = ru.split('\n', 1)
#     if write_header:
#         outputfile.write(header+'\n')
#         write_header = False
#     outputfile.write(ru)


# def write_record(tabledesc, jsrecord, output, write_header=False):
#     items = []
#     if write_header:
#         fields = [ '"' + key + '"' for key in jsrecord.keys()
#                    if key != 'attributes' ]
#         output.write(','.join(fields)+'\n')
#     for name, value in jsrecord.items():
#         if name == 'attributes':
#             continue  # skip
#         items.append(tabledesc.fields[name].type.json_to_csv(value))
#     output.write(','.join(items)+'\n')


# def write_csv_from_json(outputfile, tabledesc, inputresult, write_header):
#     js = json.loads(inputresult.read(), encoding='utf-8')
#
#     for record in js:
#         write_record(tabledesc, record, outputfile, write_header)
#         write_header = False  # Var local copy


def download(job, pool_time=5):
    logger = logging.getLogger(__name__)
    bulk = get_SalesforceBulk()

    done = False
    while not done:
        try:
            job_status = bulk.job_status(job)
            # pprint(job_status)

            nb_queued = int(job_status['numberBatchesQueued'])
            nb_inprogress = int(job_status['numberBatchesInProgress'])
            nb_completed = int(job_status['numberBatchesCompleted'])
            nb_failed = int(job_status['numberBatchesFailed'])
            nb_total = int(job_status['numberBatchesTotal'])
            logger.info(
                    "%(total)s batch: %(queued)s Queued, "
                    "%(inprogress)s In Progress, "
                    "%(completed)s Completed, "
                    "%(failed)s Failed.", {
                        'queued': nb_queued,
                        'inprogress': nb_inprogress,
                        'completed': nb_completed,
                        'failed': nb_failed,
                        'total': nb_total,
                    })
            if nb_queued == 0 and nb_inprogress == 0:
                break

        except requests.exceptions.ConnectionError:
            # At that point, a connection error is bad, but not fatal
            # Let's retry
            pass
        sleep(pool_time)

    try:
        os.mkdir(config.JOB_DIR + '/' + job)
    except FileExistsError:
        pass  # Already exists? Good!

    job_status = bulk.job_status(job)
    with open(config.JOB_DIR + '/' + job + '/' + 'status.json', 'w') as file:
        file.write(json.dumps(job_status, indent=4))

    batches = bulk.get_batch_list(job)
    with open(config.JOB_DIR + '/' + job + '/' + 'batches.json', 'w') as file:
        file.write(json.dumps(batches, indent=4))

    for batch in bulk.get_batch_list(job):
        batch_id = batch['id']
        if batch['state'] == 'NotProcessed':
            logger.debug('Skipping batch %s in state "NotProcessed".',
                         batch_id)
            continue
        filename = (config.JOB_DIR + '/' + job + '/' + batch_id + '.'
                    + job_status['contentType'])
        with open(filename, 'wb') as file:
            logger.info('Downloading batch %s', batch_id)
            for chunk in bulk.get_all_results_for_query_batch(batch_id, job):
                file.write(chunk.read())

    if job_status['state'] == 'Open':
        logger.info('Closing job')
        bulk.close_job(job)
        job_status = bulk.job_status(job)
        # Update the data after closing the job
        jsonfilename = config.JOB_DIR + '/' + job + '/' + 'status.json'
        with open(jsonfilename, 'w') as file:
            file.write(json.dumps(job_status, indent=4))


if __name__ == '__main__':
    def main():
        parser = argparse.ArgumentParser(
            description='Download csv data from salesforce')
        parser.add_argument(
                'job',
                help='job id')
        args = parser.parse_args()

        logging.basicConfig(
                filename=config.LOGFILE,
                format=config.LOGFORMAT.format('download '+args.job),
                level=config.LOGLEVEL)

        job = args.job

        download(job)

    main()
