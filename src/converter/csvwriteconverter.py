#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .converter import Converter


class CSVWriteConverter(Converter):

    # default values for columns - dict
    defaults = None

    def __init__(self, defaults=None):
        self.defaults = defaults

    def row(self, row):
        for column in row:
            if (row[column] is None) and (column in self.defaults):
                row[column] = self.defaults[column]
        return row

    def convert(self, event_or_row):
        # no defaults - nothing to convert
        if not self.defaults:
            return event_or_row

        # defaults are empty - nothing to convert
        if len(self.defaults) < 1:
            return event_or_row

        # have defaults
        return super().convert(event_or_row)
