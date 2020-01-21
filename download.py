#!/usr/bin/python3

import argparse
import json
import os
import sys
# from pprint import pprint
from time import sleep

import requests

from config import JOB_DIR
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
    bulk = get_SalesforceBulk()

    done = False
    while not done:
        try:
            job_status = bulk.job_status(job)
            # pprint(job_status)

            numberBatchesQueued = int(job_status['numberBatchesQueued'])
            numberBatchesInProgress = int(
                    job_status['numberBatchesInProgress'])
            numberBatchesCompleted = int(job_status['numberBatchesCompleted'])
            numberBatchesFailed = int(job_status['numberBatchesFailed'])
            numberBatchesTotal = int(job_status['numberBatchesTotal'])
            print("{total} batch: {queued} Queued, "
                  "{inprogress} In Progress, "
                  "{completed} Completed, "
                  "{failed} Failed".format(
                    queued=numberBatchesQueued,
                    inprogress=numberBatchesInProgress,
                    completed=numberBatchesCompleted,
                    failed=numberBatchesFailed,
                    total=numberBatchesTotal),
                  file=sys.stderr)
            if numberBatchesQueued == 0 and numberBatchesInProgress == 0:
                break

            # print('.', end='', file=sys.stderr, flush=True)
        except requests.exceptions.ConnectionError:
            # At that point, a connection error is bad, but not fatal
            # Let's retry
            pass
            # print('E', end='', file=sys.stderr, flush=True)
        sleep(pool_time)
    print('', file=sys.stderr)

    try:
        os.mkdir(JOB_DIR + '/' + job)
    except FileExistsError:
        pass  # Already exists? Good!

    job_status = bulk.job_status(job)
    with open(JOB_DIR + '/' + job + '/' + 'status.json', 'w') as file:
        file.write(json.dumps(job_status, indent=4))

    batches = bulk.get_batch_list(job)
    with open(JOB_DIR + '/' + job + '/' + 'batches.json', 'w') as file:
        file.write(json.dumps(batches, indent=4))

    for batch in bulk.get_batch_list(job):
        batch_id = batch['id']
        if batch['state'] == 'NotProcessed':
            print('Skipping batch {} in state "NotProcessed".'.format(
                batch_id),
                file=sys.stderr)
            continue
        filename = (JOB_DIR + '/' + job + '/' + batch_id + '.'
                    + job_status['contentType'])
        with open(filename, 'w') as file:
            print('Downloading batch', batch_id, end='',
                  file=sys.stderr, flush=True)
            for chunk in bulk.get_all_results_for_query_batch(batch_id, job):
                file.write(str(chunk.read(), encoding='utf-8'))
                print('.', end='', file=sys.stderr, flush=True)
            print('', file=sys.stderr)

    if job_status['state'] == 'Open':
        print('Closing job', file=sys.stderr)
        bulk.close_job(job)
        job_status = bulk.job_status(job)
        # Update the data after closing the job
        with open(JOB_DIR + '/' + job + '/' + 'status.json', 'w') as file:
            file.write(json.dumps(job_status, indent=4))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Download csv data from salesforce')
    parser.add_argument(
            'job',
            help='job id')
    args = parser.parse_args()

    job = args.job

    download(job)
