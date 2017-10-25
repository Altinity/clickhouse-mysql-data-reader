#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clickhouse_driver.client import Client
from .writer import Writer
from ..event.event import Event
from ..converter.chdatatypeconverter import CHDataTypeConverter


class CHWriter(Writer):

    client = None
    dst_db = None
    dst_table = None

    def __init__(self, connection_settings, dst_db, dst_table):
        self.client = Client(**connection_settings)
        self.dst_db = dst_db
        self.dst_table = dst_table

    def insert(self, event=None):

        if self.dst_db:
            event.schema = self.dst_db
        if self.dst_table:
            event.table = self.dst_table

        converter = CHDataTypeConverter()
        event = converter.convert(event)

        # values [{'id': 3, 'a': 3}, {'id': 2, 'a': 2}]
        # ensure values is a list
        values = [event.row] if isinstance(event.row, dict) else event.row

        try:
            sql = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES'.format(
                event.schema,
                event.table,
                ', '.join(map(lambda column: '`%s`' % column, values[0].keys()))
            )
            self.client.execute(sql, values)
        except:
            print('QUERY FAILED -------------------------')
            print(sql)
            print(values)

    def batch(self, events):
        values = []
        converter = CHDataTypeConverter()

        for event in events:
            ev = converter.convert(event)
            values.append(ev.row)

        schema = self.dst_db if self.dst_db else ev.schema
        table = self.dst_table if self.dst_table else ev.table

        sql = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES'.format(
            schema,
            table,
            ', '.join(map(lambda column: '`%s`' % column, values[0].keys()))
        )
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
