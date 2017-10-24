#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clickhouse_driver.client import Client
from .writer import Writer
from ..event.event import Event
from ..converter.chdatatypeconverter import CHDataTypeConverter


class CHWriter(Writer):

    client = None

    def __init__(self, *args, **kwargs):
        self.client = Client(*args, **kwargs)

    def insert(self, event=None):

        converter = CHDataTypeConverter()
        event = converter.convert(event)

        # values [{'id': 3, 'a': 3}, {'id': 2, 'a': 2}]
        # ensure values is a list
        values = [event.row] if isinstance(event.row, dict) else event.row

        sql = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES'.format(
            event.schema,
            event.table,
            ', '.join(map(lambda column: '`%s`' % column,  values[0].keys()))
        )
        print('-------------------------')
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

    writer = CHWriter(connection_settings=connection_settings)
    writer.insert()
