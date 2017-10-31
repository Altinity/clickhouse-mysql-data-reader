#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .converter import Converter

import datetime
import decimal


class CHWriteConverter(Converter):

    delete_empty_columns = False

    types_to_convert = [
        datetime.timedelta,
        bytes,
        decimal.Decimal,

        # jsonify
        # object,
        dict,
        list,

        # set - how to migrate MySQL's `set` type and tell it from `json` type - both of which are presented as `dict`?
        set,
    ]

    def convert(self, event):

        columns_to_delete = []
        for column_name in event.row:
#                print(column_name, row['values'][column_name], type(row['values'][column_name]))

            if self.delete_empty_columns and (event.row[column_name] is None):
#                print("Skip None value for column", column_name)
                columns_to_delete.append(column_name)
                continue

            for t in self.types_to_convert:
                if isinstance(event.row[column_name], t):
#                    print("Converting column", column_name, "of type", type(event.row[column_name]),
#                          event.row[column_name])
                    event.row[column_name] = str(event.row[column_name])
#                    print("res", event.row[column_name])
                    break
            else:
#                print("Using asis column", column_name, "of type", type(event.row[column_name]))
                pass

        for column_to_delete in columns_to_delete:
            event.row.pop(column_to_delete)

        return event
