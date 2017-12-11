#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .converter import Converter
import ast


class CSVReadConverter(Converter):

    def row(self, row):
        for column in row:
            if row[column] == '':
                row[column] = None
#            else:
#                try:
#                    event.row[column] = ast.literal_eval(event.row[column])
#                except:
#                    pass
        return row
