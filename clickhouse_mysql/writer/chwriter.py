#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys

from clickhouse_driver.client import Client

from .writer import Writer
from ..event.event import Event


class CHWriter(Writer):

    client = None
    dst_schema = None
    dst_table = None

    def __init__(
            self,
            connection_settings,
            dst_schema=None,
            dst_table=None,
            next_writer_builder=None,
            converter_builder=None,
    ):
        super().__init__(next_writer_builder=next_writer_builder, converter_builder=converter_builder)

        self.client = Client(**connection_settings)
        self.dst_schema = dst_schema
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
            logging.warning('No events to insert. class: %s', __class__)
            return

        # assume we have at least one Event

        logging.debug('class:%s insert %d rows', __class__, len(events))

        rows = []
        event_converted = None
        for event in events:
            if not event.verify:
                logging.warning('Event verification failed. Skip one event. Event: %s Class: %s', event.meta(), __class__)
                continue # for event

            event_converted = self.convert(event)
            if isinstance(event_converted, Event):
                for row in event_converted:
                    rows.append(row)
            else:
                rows.append(event_converted)

        schema = self.dst_schema if self.dst_schema else event_converted.schema
        table = self.dst_table if self.dst_table else event_converted.table

        sql = ''
        try:
            sql = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES'.format(
                schema,
                table,
                ', '.join(map(lambda column: '`%s`' % column, rows[0].keys()))
            )
            self.client.execute(sql, rows)
        except Exception as ex:
            print('QUERY FAILED:')
            print('ex=', ex)
            print('sql=', sql)
            print('rows=', rows)
            sys.exit(0)



if __name__ == '__main__':
    connection_settings = {
        'host': '192.168.74.230',
        'port': 9000,
        'user': 'default',
        'passwd': '',
    }

    writer = CHWriter(connection_settings=connection_settings)
    writer.insert()
