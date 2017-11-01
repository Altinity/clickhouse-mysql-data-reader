#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clickhouse_driver.client import Client
from .writer import Writer


class CHWriter(Writer):

    client = None
    dst_db = None
    dst_table = None

    def __init__(
            self,
            connection_settings,
            dst_db=None,
            dst_table=None,
            next_writer_builder=None,
            converter_builder=None,
    ):
        super().__init__(next_writer_builder=next_writer_builder, converter_builder=converter_builder)

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

        events = self.listify(event_or_events)
        if len(events) < 1:
            return

        values = []
        event_converted = None
        for event in events:
            event_converted = self.convert(event)
            values.append(event_converted.row)

        schema = self.dst_db if self.dst_db else event_converted.schema
        table = self.dst_table if self.dst_table else event_converted.table

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
