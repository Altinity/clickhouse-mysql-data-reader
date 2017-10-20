#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import decimal
import json


class Pumper(object):

    reader = None
    writer = None
    skip_empty = True

    def __init__(self, reader=None, writer=None, skip_empty=True):

        self.reader = reader
        if self.reader:
            self.reader.subscribe({
                'WriteRowsEvent': self.write_rows_event,
                'WriteRowsEvent.EachRow': self.write_rows_event_each_row,
            })
        self.writer = writer
        self.skip_empty = skip_empty

    def run(self):
        self.reader.read()

    def write_rows_event(self, binlog_event=None):
#        binlog_event.dump()
        pass

    def write_rows_event_each_row(self, binlog_event=None, row=None):
        values = {}
        for column_name in row['values']:
#            print(column_name, row['values'][column_name], type(row['values'][column_name]))

            if self.skip_empty and row(['values'][column_name] is None):
                print("Skip None value for column", column_name)
                continue

            types_to_convert = [
                datetime.timedelta,
                bytes,
                decimal.Decimal,

                # jsonify
                #object,
                dict,
                list,

                # set - how to migrate MySQL's `set` type and tell it from `json` type - both of which are presented as `dict`?
                set,
            ]
            for t in types_to_convert:
                if isinstance(row['values'][column_name], t):
                    print("Converting column", column_name, "of type", type(row['values'][column_name]), row['values'][column_name])
                    values[column_name] = str(row['values'][column_name])
#                    values[column_name] = json.dumps(row['values'][column_name])
                    print("res", values[column_name])
                    break
            else:
                print("Using asis column", column_name, "of type", type(row['values'][column_name]))
                values[column_name] = row['values'][column_name]

        self.writer.insert(binlog_event.schema, binlog_event.table, values)


if __name__ == '__main__':
    print("pumper")
