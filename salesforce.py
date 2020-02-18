from config import CREDIDENTIALS, SF_API_VERSION
from salesforce_bulk import SalesforceBulk
from simple_salesforce import Salesforce


def get_Salesforce():
    params = {}
    params.update(CREDIDENTIALS)
    params['version'] = SF_API_VERSION
    return Salesforce(**params)


def get_SalesforceBulk():
    return SalesforceBulk(**CREDIDENTIALS)
