import sys
from time import sleep

from simple_salesforce import Salesforce
from simple_salesforce.bulk import SFBulkHandler, SFBulkType
from simple_salesforce.util import call_salesforce
from salesforce_bulk import SalesforceBulk

from config import CREDIDENTIALS, SF_API_VERSION

def get_Salesforce():
    params = {}
    params.update(CREDIDENTIALS)
    params['version'] = SF_API_VERSION
    return Salesforce(**params)

def get_SalesforceBulk():
    return SalesforceBulk(**CREDIDENTIALS)

class MySFBulkHandler(SFBulkHandler):
    def __getattr__(self, name):
        return MySFBulkType(object_name=name, bulk_url=self.bulk_url,
                          headers=self.headers, session=self.session)

class MySFBulkType(SFBulkType):
    # Same as super class, but print out activity
    def _bulk_operation(self, object_name, operation, data,
                        external_id_field=None, wait=5):
        """ String together helper functions to create a complete
        end-to-end bulk API request

        Arguments:

        * object_name -- SF object
        * operation -- Bulk operation to be performed by job
        * data -- list of dict to be passed as a batch
        * external_id_field -- unique identifier field for upsert operations
        * wait -- seconds to sleep between checking batch status
        """

        job = self._create_job(object_name=object_name, operation=operation,
                               external_id_field=external_id_field)

        print('Created job', job, file=sys.stderr)

        batch = self._add_batch(job_id=job['id'], data=data,
                                operation=operation)

        self._close_job(job_id=job['id'])

        print('Waiting for job', batch['jobId'], file=sys.stderr)

        batch_status = self._get_batch(job_id=batch['jobId'],
                                       batch_id=batch['id'])['state']

        while batch_status not in ['Completed', 'Failed', 'Not Processed']:
            sleep(wait)
            batch_status = self._get_batch(job_id=batch['jobId'],
                                           batch_id=batch['id'])['state']
            print('.', end='', file=sys.stderr, flush=True)
        print('', file=sys.stderr)

        results = self._get_batch_results(job_id=batch['jobId'],
                                          batch_id=batch['id'],
                                          operation=operation)
        return results


    def _get_batch_results(self, job_id, batch_id, operation):
        """ retrieve a set of results from a completed job """

        url = "{}{}{}{}{}{}".format(self.bulk_url, 'job/', job_id, '/batch/',
                                    batch_id, '/result')

        result = call_salesforce(url=url, method='GET', session=self.session,
                                  headers=self.headers)

        if operation == 'query':
            query_result = []
            for chunk_id in result.json():
                url_query_results = "{}{}{}".format(url, '/', chunk_id)
                query_result += call_salesforce(url=url_query_results, method='GET',
                                                session=self.session,
                                                headers=self.headers).json()
            return query_result

        return result.json()

