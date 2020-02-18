from config import CREDIDENTIALS
from salesforce_bulk import SalesforceBulk
from simple_salesforce import Salesforce


def get_Salesforce():
    params = {}
    params.update(CREDIDENTIALS)
    return Salesforce(**params)


def get_SalesforceBulk():
    return SalesforceBulk(**CREDIDENTIALS)
