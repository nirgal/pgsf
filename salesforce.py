import config
from salesforce_bulk import SalesforceBulk
from simple_salesforce import Salesforce

__sf_config = config.get_section('salesforce')

CREDIDENTIALS = {}
CREDIDENTIALS['username'] = __sf_config['username']
CREDIDENTIALS['password'] = __sf_config['password']
CREDIDENTIALS['security_token'] = __sf_config['security_token']
__val = __sf_config.get('domain', None)
if __val:
    CREDIDENTIALS['domain'] = __val

SF_API_VERSION = __sf_config['api_version']


def get_Salesforce():
    params = {}
    params.update(CREDIDENTIALS)
    params['version'] = SF_API_VERSION
    return Salesforce(**params)


def get_SalesforceBulk():
    return SalesforceBulk(**CREDIDENTIALS)
