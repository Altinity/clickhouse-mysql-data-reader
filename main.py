#!/usr/bin/env python
# -*- coding: utf-8 -*-

from reader import Reader
from writer import Writer
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent

import sys
if sys.version_info[0] < 3:
    raise "Must be using Python 3"


class Main(object):

    reader = None
    writer = None

    def __init__(self):
        connection_settings = {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'reader',
            'passwd': 'qwerty',
        }
        server_id = 1

        self.reader = Reader(
            connection_settings=connection_settings,
            server_id=server_id,
            callbacks={
                'WriteRowsEvent': self.write_rows_event,
                'WriteRowsEvent.EachRow': self.write_rows_event_each_row,
            }
        )

        connection_settings = {
            'host': '192.168.74.230',
            'port': 9000,
            'user': 'default',
            'passwd': '',
        }

        self.writer = Writer(
            connection_settings=connection_settings,
        )

    def run(self):
        self.reader.read()

    def write_rows_event(self, binlog_event=None):
#        binlog_event.dump()
        pass

    def write_rows_event_each_row(self, binlog_event=None, row=None):
        for column_name in row['values']:
            print(column_name, row['values'][column_name], type(row['values'][column_name]))
        self.writer.insert(binlog_event.schema, binlog_event.table, row['values'])
        pass


if __name__ == '__main__':
    main = Main()
    main.run()
