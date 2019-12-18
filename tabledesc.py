#!/usr/bin/python3

import argparse
import csv
from collections import OrderedDict

from salesforce import get_Salesforce


class TableDesc:
    def __init__(self, name):
        self.name = name

    @property
    def sf_desc(self):
        '''
        Connects to saleforce and return raw description.
        Data is cached for reuse
        '''
        try:
            return self.__sf_desc_cache
        except AttributeError:
            sf = get_Salesforce()
            accessor = sf.__getattr__(self.name)
            self.__sf_desc_cache = accessor.describe()
            return self.__sf_desc_cache

    @property
    def sf_fields(self):
        return self.sf_desc['fields']

    @property
    def fields(self):
        """Ordered dict"""
        try:
            return self.__fields_cache
        except AttributeError:
            self.__fields_cache = OrderedDict()
            for sf_field_info in self.sf_fields:
                self.__fields_cache[sf_field_info['name']] = Field2(sf_field_info)
            return self.__fields_cache

    def get_all_fields_names(self):
        return [field['name'] for field in self.sf_fields]

    def get_sync_field_names(self):
        '''
        Returns a list of field names to be synchronized
        '''
        filename = 'mapping/{}.csv'.format(self.name)
        result = []
        with open(filename) as f:
            for row in csv.reader(f):
                if row[1] == '1':
                    result.append(row[0])
        return result

    def get_sync_fields(self):
        '''
        Returns an OrderedDict of fields to be synchronized
        '''
        result = OrderedDict()
        for fieldname in self.get_sync_field_names():
            result[fieldname] = self.fields[fieldname]
        return result

    def is_field_compound(self, fieldname):
        '''
        returns True if this field is a compound field, like address
        that is if another field as a compoundFieldName with that value
        '''
        for fieldinfo in self.fields.values():
            if fieldinfo['compoundFieldName'] == fieldname:
                return True
        return False

    def make_csv_fieldlist(self, default=None):
        '''
        Creates the initial csv file with a list of fields that can be replicated.
        This downloads the field description from salesforce
        '''
        default_import_fields = 'Id','CreatedDate','IsDeleted','SystemModstamp'
        filename = 'mapping/{}.csv'.format(self.name)
        print('Writing', filename)
        with open(filename, 'x') as f:
            f.write('"FieldName", "Import", "Note"\n')
            for fieldname, fieldinfo in self.fields.items():
                if default == 'minimal':
                    isimport = '1' if fieldname in default_import_fields else ''
                else:  # default default
                    if fieldinfo['calculated']:
                        isimport = ''
                    else:
                        isimport = '1'
                notes = []
                if fieldinfo['type'] == 'address':
                    notes.append('address')
                    isimport = ''
                if self.is_field_compound(fieldname):
                    notes.append('compound')
                    isimport = ''
                if fieldinfo['type'] == 'encryptedstring':
                    notes.append('encryptedstring')
                    isimport = ''
                if fieldinfo['calculated']:
                    notes.append('calculated')
                f.write('"{}",{},{}\n'.format(fieldname, isimport, ','.join(notes)))


class Field2(OrderedDict):
    pass
#    def __init__(self, sf_info):
#        self._sf_info = sf_info
#
#    def __getattr__(self, attrname):
#        return self._sf_info['attrname']

#class Field:
#    def __init__(self, sf_info):
#        self.sf_info = sf_info
#        self.name = sf_info['name']
#        ftype = sf_info['type']
#        if ftype == 'address':
#            self.type = SFTypeAddress(self)
#        elif ftype == 'boolean':
#            self.type = SFTypeBool(self)
#        elif ftype == 'complexvalue':
#            self.type = SFTypeComplexValue(self)
#        elif ftype == 'currency':
#            self.type = SFTypeNumeric(self)
#        elif ftype == 'date':
#            self.type = SFTypeString(self)
#        elif ftype == 'datetime':
#            self.type = SFTypeDateTime(self)
#        elif ftype == 'double':
#            self.type = SFTypeNumeric(self)
#        elif ftype == 'email':
#            self.type = SFTypeString(self)
#        elif ftype == 'encryptedstring':
#            self.type = SFTypeString(self)
#        elif ftype == 'id':
#            self.type = SFTypeString(self)
#        elif ftype == 'int':
#            self.type = SFTypeNumeric(self)
#        elif ftype == 'multipicklist':
#            self.type = SFTypeString(self)
#        elif ftype == 'percent':
#            self.type = SFTypeNumeric(self)
#        elif ftype == 'phone':
#            self.type = SFTypeString(self)
#        elif ftype == 'picklist':
#            self.type = SFTypeString(self)
#        elif ftype == 'reference':
#            self.type = SFTypeString(self)
#        elif ftype == 'string':
#            self.type = SFTypeString(self)
#        elif ftype == 'textarea':
#            self.type = SFTypeString(self)
#        elif ftype == 'url':
#            self.type = SFTypeString(self)
#        else:
#            print('Unknown type '+ftype, file=sys.stderr)
#            raise NotImplemented
#
#    def __repr__(self):
#        return 'Field({})'.format(self.name)
#
#
#class SFType:
#    def __init__(self, field):
#        self.sf_info = field.sf_info
#
#
#class SFTypeString(SFType):
#    def json_to_csv(self, value):
#        if value is None:
#            return ''
#        return '"' + value.replace('"', '""') + '"'
#
#
#class SFTypeBool(SFType):
#    def json_to_csv(self, value):
#        if value is None:
#            return ''
#        return '"' + str(value) + '"'
#
#
#class SFTypeDateTime(SFType):
#    def json_to_csv(self, value):
#        if value is None:
#            return ''
#        return '"'+str(datetime.utcfromtimestamp(value/1000))+'Z"'
#
#
#class SFTypeNumeric(SFType):
#    def json_to_csv(self, value):
#        if value is None:
#            return ''
#        return str(value)
#
#
#class SFTypeComplexValue(SFType):
#    def json_to_csv(self, value):
#        raise NotImplemented
#        # FeatureNotEnabled : Cannot serialize value for 'RecordTypesSupported' in CSV format
#
#
#class SFTypeAddress(SFType):
#    def json_to_csv(self, value):
#        raise Error('Address cannot have a json value')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create initial list of fields to synchronise')
    parser.add_argument(
            'table',
            help='table name')
    args = parser.parse_args()

    TableDesc(args.table).make_csv_fieldlist()
