#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clickhouse_mysql.converter.converter import Converter

import datetime
import decimal
import logging


class CHWriteConverter(Converter):

    # do not include empty columns into converted row
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

    def __init__(self, column_skip):
        logging.debug("CHWriteConverter __init__()")
        super().__init__(column_skip=column_skip)

    def column(self, column, value):
        for _type in self.types_to_convert:
            if isinstance(value, _type):
                # print("Converting column", column, "of type", type(event.row[column]), event.row[column])
                return str(value)
        # print("Using asis column", column, "of type", type(event.row[column]))
        return value

    def row(self, row):
        """
        Convert row
        :param row: row to convert
        :return:  converted row
        """
        if row is None:
            return None

        # init list of columns to delete
        columns_to_delete = self.column_skip

        for column in row:

            # skip columns already prepared for deletion
            if column in columns_to_delete:
                continue

            # convert column
            row[column] = self.column(column, row[column])

            # include empty column to the list of to be deleted columns
            if (row[column] is None) and self.delete_empty_columns:
                columns_to_delete.append(column)

        # delete columns according to the list of columns to delete
        for column in columns_to_delete:
            row.pop(column)

        return row
