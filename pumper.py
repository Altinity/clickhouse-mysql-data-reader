#!/usr/bin/env python
# -*- coding: utf-8 -*-

from reader import Reader
from writer import Writer

import datetime
import decimal


class Pumper(object):

    reader = None
    writer = None

    def __init__(self, reader_config, writer_config):

        # append callbacks section for being expanded
        reader_config['callbacks'] =  {
            'WriteRowsEvent': self.write_rows_event,
            'WriteRowsEvent.EachRow': self.write_rows_event_each_row,
        }

        self.reader = Reader(**reader_config)
        self.writer = Writer(**writer_config)

    def run(self):
        self.reader.read()

    def write_rows_event(self, binlog_event=None):
#        binlog_event.dump()
        pass

    def write_rows_event_each_row(self, binlog_event=None, row=None):
        values = {}
        for column_name in row['values']:
#            print(column_name, row['values'][column_name], type(row['values'][column_name]))

            if row['values'][column_name] is None:
                print("Skip None value for column", column_name)
                continue

            types_to_convert = [
                datetime.timedelta,
                bytes,
                decimal.Decimal,
            ]
            for t in types_to_convert:
                if isinstance(row['values'][column_name], t):
                    print("Converting column", column_name, "of type", type(row['values'][column_name]))
                    values[column_name] = str(row['values'][column_name])
                    break
            else:
                print("Using asis column", column_name, "of type", type(row['values'][column_name]))
                values[column_name] = row['values'][column_name]

        self.writer.insert(binlog_event.schema, binlog_event.table, values)


if __name__ == '__main__':
    print("pumper")
