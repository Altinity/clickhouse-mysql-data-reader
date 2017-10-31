#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .converter import Converter


class CSVWriteConverter(Converter):

    defaults = None

    def __init__(self, defaults={}):
        self.defaults = defaults


    def convert(self, event):
        if not self.defaults:
            return event

        for column in event.row:
            if column in self.defaults and event.row[column] is None:
                event.row[column] = self.defaults[column]
        return event
