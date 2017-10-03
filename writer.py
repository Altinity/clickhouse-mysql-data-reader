#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clickhouse_driver.client import Client


class Writer(object):

    client = None

    def __init__(self, connection_settings):
        self.client = Client(connection_settings['host'])

    def insert(self, schema, table, values):

        # values [{'id': 3, 'a': 3}, {'id': 2, 'a': 2}]
        # ensure values is a list
        values = [values] if isinstance(values, dict) else values

        sql = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES'.format(
            schema,
            table,
            ', '.join(map(lambda column: '`%s`' % column,  values[0].keys()))
        )
        print(sql)
        print(values)
        self.client.execute(sql, values)

if __name__ == '__main__':
    connection_settings = {
        'host': '192.168.74.230',
        'port': 9000,
        'user': 'default',
        'passwd': '',
    }

    writer = Writer(
        connection_settings=connection_settings,
    )

    writer.insert()
