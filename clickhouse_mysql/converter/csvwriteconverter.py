#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from clickhouse_mysql.converter.converter import Converter


class CSVWriteConverter(Converter):

    # default values for columns - dict
    defaults = None

    def __init__(self, defaults=None, column_skip=None):
        logging.debug("CSVWriteConverter __init__()")
        self.defaults = [] if defaults is None else defaults
        super().__init__(column_skip=column_skip)

    def row(self, row):
        if row is None:
            return None

        # replace empty columns with default values (for default we have)
        for column in row:
            if (row[column] is None) and (column in self.defaults):
                row[column] = self.defaults[column]

        # delete columns according to the list of columns to skip
        for column in self.column_skip:
            if column in row:
                row.pop(column)

        return row

    def convert(self, event_or_row):
        # nothing to convert
        if not self.defaults and not self.column_skip:
            return event_or_row

        # have some convert setup
        return super().convert(event_or_row)
