#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .converter import Converter
import ast


class CSVReadConverter(Converter):

    def convert(self, event):
        for column in event.row:
            if event.row[column] == '':
                event.row[column] = None
#            else:
#                try:
#                    event.row[column] = ast.literal_eval(event.row[column])
#                except:
#                    pass
        return event
