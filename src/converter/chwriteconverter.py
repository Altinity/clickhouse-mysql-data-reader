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

        for column in event.row:
            if (event.row[column] is None) and self.delete_empty_columns:
                # include empty column to the list of to be deleted columns
                columns_to_delete.append(column)
                # move to next column
                continue

            for t in self.types_to_convert:
                if isinstance(event.row[column], t):
#                    print("Converting column", column, "of type", type(event.row[column]),
#                          event.row[column])
                    event.row[column] = str(event.row[column])
#                    print("res", event.row[column])
                    break
            else:
#                print("Using asis column", column, "of type", type(event.row[column]))
                pass

        # delete columns according to the list
        for column in columns_to_delete:
            event.row.pop(column)

        return event
