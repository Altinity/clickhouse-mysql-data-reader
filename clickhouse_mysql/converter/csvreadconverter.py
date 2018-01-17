#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clickhouse_mysql.converter.converter import Converter
import ast


class CSVReadConverter(Converter):

    def row(self, row):
        if row is None:
            return None

        for column in row:
            if row[column] == '':
                row[column] = None
#            else:
#                try:
#                    event.row[column] = ast.literal_eval(event.row[column])
#                except:
#                    pass
        return row
