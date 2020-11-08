#!/usr/bin/python3

import argparse
import csv
import logging
from collections import OrderedDict

import config
from salesforce import get_Salesforce

logger = logging.getLogger(__name__)


class TableDesc:
    def __init__(self, name):
        self.name = name

    @property
    def sf_desc(self):
        '''
        Connects to saleforce and return raw description.
        Data is cached for reuse.
        '''
        try:
            return self.__sf_desc_cache
        except AttributeError:
            sf = get_Salesforce()
            accessor = sf.__getattr__(self.name)
            self.__sf_desc_cache = accessor.describe()
            return self.__sf_desc_cache

    @property
    def sf_field_definition(self):
        '''
        Run a query against salesforce FieldDefinition table to get extra
        field information.
        Data is cached for reuse.
        '''
        try:
            return self.__sf_field_definition_cache
        except AttributeError:
            import query
            soql = """SELECT QualifiedApiName,IsIndexed
                      FROM FieldDefinition
                      WHERE EntityDefinitionId='{}'""".format(self.name)
            qry = query.query(soql)
            self.__sf_field_definition_cache = list(qry)
            return self.__sf_field_definition_cache

    def get_sf_fields(self):
        '''
        Return the fields as an OrderedDict.
        Data is cached for reuse.
        '''
        try:
            # return cache if available
            return self.__fields_cache
        except AttributeError:
            self.__fields_cache = OrderedDict()
            # First get the info from sf_desc
            for sf_field_info in self.sf_desc['fields']:
                # if sf_field_info['name'] == 'ChannelProgramName':
                #     print('NNN1:', sf_field_info)
                self.__fields_cache[sf_field_info['name']] = sf_field_info
            # Then the the IsIndexed from table FieldDefinition
            sf_definition = self.sf_field_definition
            for record in sf_definition:
                name = record['QualifiedApiName']
                # if name == 'ChannelProgramName':
                #     print('NNN2:', record)
                if name in self.__fields_cache.keys():
                    self.__fields_cache[name]['IsIndexed'] = \
                            record['IsIndexed']
                else:
                    logger.warning(
                            'Table %s, field %s '
                            'is not available from describe',
                            self.name, name)
            return self.__fields_cache

    def get_sync_field_names(self):
        '''
        Returns a list of field names to be synchronized.
        That is the field names in first column of the csv mapping file having
        the second column set to 1.
        See self.make_csv_fieldlist
        '''
        filename = 'mapping/{}.csv'.format(self.name)
        result = []
        with open(filename) as f:
            for row in csv.reader(f):
                if row[1] == '1':
                    result.append(row[0])
        return result

    def get_indexed_sync_field_names(self):
        '''
        Returns a list of field names that should be indexed.
        That is the field names in first column of the csv mapping file having
        the third column set to 1.
        See self.make_csv_fieldlist
        '''
        filename = 'mapping/{}.csv'.format(self.name)
        result = []
        with open(filename) as f:
            for row in csv.reader(f):
                if row[2] == '1':
                    result.append(row[0])
        return result

    def get_sync_fields(self):
        '''
        Returns an OrderedDict of fields to be synchronized
        '''
        sf_fields = self.get_sf_fields()
        sync_field_names = self.get_sync_field_names()
        result = OrderedDict()
        for fieldname in sync_field_names:
            result[fieldname] = sf_fields[fieldname]
        return result

    def is_field_compound(self, fieldname):
        '''
        returns True if this field is a compound field, like address
        that is if another field as a compoundFieldName with that value
        '''
        for fieldinfo in self.get_sf_fields().values():
            if fieldinfo['compoundFieldName'] == fieldname:
                return True
        return False

    def make_csv_fieldlist(self, default=None):
        '''
        Creates the initial csv file with a list of fields that can be
        replicated.
        This downloads the field description from salesforce.
        default None means import all fields but formulas and compound fields.
        default 'minimal' means only import fields listed in
        default_import_fields.
        '''
        default_import_fields = (
                'Id', 'DurableId', 'CreatedDate', 'IsDeleted', 'SystemModstamp'
                )
        sf_fields = self.get_sf_fields()

        filename = 'mapping/{}.csv'.format(self.name)
        print('Writing', filename)
        with open(filename, 'x') as f:
            f.write('"FieldName", "Import", "Indexed", "Note"\n')  # header
            for fieldname, fieldinfo in sf_fields.items():
                logging.debug("Describing field %s : %s", fieldname, fieldinfo)
                if default == 'minimal':
                    if fieldname in default_import_fields:
                        isimport = '1'
                    else:
                        isimport = ''
                else:  # default default
                    if fieldinfo['calculated']:
                        isimport = ''
                    else:
                        isimport = '1'
                notes = []
                if self.is_field_compound(fieldname):
                    notes.append('compound')
                    isimport = ''
                # if fieldinfo['type'] == 'encryptedstring':
                #     notes.append('encryptedstring')
                #     isimport = ''
                if fieldinfo['calculated']:
                    notes.append('calculated')
                    isimport = ''

                isindexed = fieldinfo.get('IsIndexed', None)
                if isindexed is None:
                    notes.append('nofielddefinition')
                if isindexed:
                    isindexed = '1'
                else:
                    isindexed = ''

                f.write('"{}",{},{},{}\n'.format(
                    fieldname, isimport, isindexed, ','.join(notes)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create initial list of fields to synchronise')
    parser.add_argument(
            'table',
            help='table name')
    args = parser.parse_args()

    logging.basicConfig(
            filename=config.LOGFILE,
            format=config.LOGFORMAT.format('tabledesc '+args.table),
            level=config.LOGLEVEL)

    TableDesc(args.table).make_csv_fieldlist()
