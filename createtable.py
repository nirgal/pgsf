#!/usr/bin/python3

import argparse
import logging

import config
from tabledesc import TableDesc


def postgres_type_raw(field):
    sftype = field['type']
    if sftype in (
            'combobox', 'email', 'encryptedstring', 'id',
            'phone', 'reference', 'string', 'textarea', 'url'):
        return 'VARCHAR({})'.format(field['length'])
    if sftype in ('picklist', 'multipicklist'):
        return 'TEXT'  # size is not reliable
    if sftype == 'int':
        return 'INTEGER'
    if sftype == 'date':
        return 'DATE'
    if sftype == 'datetime':
        return 'TIMESTAMP'
    if sftype == 'boolean':
        return 'BOOLEAN'
    if sftype == 'currency':
        return 'NUMERIC({}, {})'.format(field['precision'], field['scale'])
    if sftype in ('double', 'percent'):
        return 'DOUBLE PRECISION'
    if sftype == 'anyType':
        return 'TEXT'
    return '"{}" NOT IMPLEMENTED '.format(sftype)


def postgres_escape_str(text):
    return "'" + text.replace("'", "''") + "'"


def postgres_escape_name(name):
    assert '"' not in name
    if config.DB_QUOTE_NAMES:
        return '"' + name + '"'
    return name


def postgres_table_name(name, schema=None):
    """
    leave schema empty for using config
    usage is temporary tables ( psycopg2.errors.InvalidTableDefinition:
            cannot create temporary relation in non-temporary schema)
    """
    if schema is None:
        schema = config.DB_SCHEMA

    if schema:
        result = postgres_escape_name(schema)
        result += '.'
    else:
        result = ''
    result += postgres_escape_name(name)
    return result


def postgres_const(value):
    if isinstance(value, str):
        return postgres_escape_str(value)
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    if isinstance(value, (int, float)):
        return str(value)
    return 'NOTIMPLEMENTED'


def postgres_coldef_from_sffield(field):
    field_name = field['name']
    field_type = field['type']

    if field_type == 'address':
        base_name = field_name
        if base_name.endswith('Address'):
            base_name = base_name[:-7]  # remove suffix
        return [
            ' {} {}'.format(postgres_escape_name(base_name+'Street'),
                            'VARCHAR(255)'),
            ' {} {}'.format(postgres_escape_name(base_name+'City'),
                            'VARCHAR(40)'),
            ' {} {}'.format(postgres_escape_name(base_name+'State'),
                            'VARCHAR(80)'),
            ' {} {}'.format(postgres_escape_name(base_name+'PostalCode'),
                            'VARCHAR(20)'),
            ' {} {}'.format(postgres_escape_name(base_name+'Country'),
                            'VARCHAR(80)'),
            ' {} {}'.format(postgres_escape_name(base_name+'Latitude'),
                            'DOUBLE PRECISION'),
            ' {} {}'.format(postgres_escape_name(base_name+'Longitude'),
                            'DOUBLE PRECISION'),
            ]
    pgtype = postgres_type_raw(field)
    if field_name in ('Id', 'DurableId'):
        pgtype += ' PRIMARY KEY'
    else:
        if not field['nillable']:
            pgtype += ' NOT NULL'
        if field['defaultValue']:
            pgtype += ' DEFAULT ' + postgres_const(field['defaultValue'])
        if field['unique']:
            pgtype += ' UNIQUE'
    return [' {} {}'.format(postgres_escape_name(field_name), pgtype)]


def get_pgsql_create(table_name):
    logger = logging.getLogger(__name__)
    logger.debug('Analyzing %s', table_name)

    tabledesc = TableDesc(table_name)

    lines = []
    sync_fields = tabledesc.get_sync_fields()
    for field_name, field in sync_fields.items():
        if field['calculated']:
            logger.warning('Field %s should be calculated locally',
                           field_name)
        if tabledesc.is_field_compound(field_name):
            logger.warning('Field %s should be composed/aggregated locally',
                           field_name)
        if field_name == 'Id' and 'DurableId' in sync_fields.keys():
            continue  # Ignore 'Id' if 'DurableId' exists
        lines += postgres_coldef_from_sffield(field)
    statements = [
        'CREATE TABLE {} (\n{}\n);'.format(
            postgres_table_name(table_name),
            ',\n'.join(lines))
        ]

    indexed_fields_names = tabledesc.get_indexed_sync_field_names()
    for field_name, field in sync_fields.items():
        if field_name in ('Id', 'DurableId'):
            continue  # primary key already indexed
        if field_name not in indexed_fields_names:
            continue
        if field.get('IsIndexed'):
            statements.append(
                'CREATE INDEX {} ON {} ({});'.format(
                    postgres_escape_name('{}_{}_idx'.format(
                            table_name, field_name)),
                    postgres_table_name(table_name),
                    postgres_escape_name(field_name)))
    return statements


if __name__ == '__main__':
    def main():
        parser = argparse.ArgumentParser(
            description='create postgresql table')
        parser.add_argument(
                '--dry-run',
                default=False, action='store_true',
                help='only print the sql statement to stdout')
        parser.add_argument(
                'table',
                help='table to create in postgresql')
        args = parser.parse_args()

        logging.basicConfig(
                filename=config.LOGFILE,
                format=config.LOGFORMAT.format('createtable '+args.table),
                level=config.LOGLEVEL)

        sql = get_pgsql_create(args.table)
        if args.dry_run:
            for line in sql:
                print(line)
        else:
            from postgres import get_pg, psycopg2
            pg = get_pg()
            cursor = pg.cursor()
            for line in sql:
                try:
                    cursor.execute(line)
                except (Exception, psycopg2.ProgrammingError) as exc:
                    logging.error('Error while executing %s', line)
                    raise exc
            pg.commit()

    main()
