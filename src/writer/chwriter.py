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

    def insert(self, event_or_events=None):
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]

        if event_or_events is None:
            # nothing to insert at all
            return

        elif isinstance(event_or_events, list):
            if len(event_or_events) < 1:
                # list is empty - nothing to insert
                return

        else:
            # event_or_events is instance of Event
            event_or_events = [event_or_events]

        converter = CHDataTypeConverter()

        values = []
        ev = None
        for event in event_or_events:
            ev = converter.convert(event)
            values.append(ev.row)

        schema = self.dst_db if self.dst_db else ev.schema
        table = self.dst_table if self.dst_table else ev.table

        try:
            sql = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES'.format(
                schema,
                table,
                ', '.join(map(lambda column: '`%s`' % column, values[0].keys()))
            )
            self.client.execute(sql, values)
        except:
            print('QUERY FAILED -------------------------')
            print(sql)
            print(values)


if __name__ == '__main__':
    connection_settings = {
        'host': '192.168.74.230',
        'port': 9000,
        'user': 'default',
        'passwd': '',
    }

    writer = CHWriter(connection_settings=connection_settings)
    writer.insert()
