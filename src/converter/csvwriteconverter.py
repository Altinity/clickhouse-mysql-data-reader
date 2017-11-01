#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .converter import Converter


class CSVWriteConverter(Converter):

    # default values for columns - dict
    defaults = None

    def __init__(self, defaults=None):
        self.defaults = defaults

    def convert(self, event):
        # no defaults - nothing to convert
        if not self.defaults:
            return event

        # defaults are empty - nothing to convert
        if len(self.defaults) < 1:
            return event

        # have defaults
        for column in event.row:
            # replace None column with default value
            if event.row[column] is None and column in self.defaults:
                event.row[column] = self.defaults[column]

        return event
