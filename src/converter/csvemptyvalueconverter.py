#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .converter import Converter


class CSVEmptyValueConverter(Converter):

    def convert(self, event):
        for column in event.row:
            if event.row[column] == '':
                event.row[column] = None
        return event
