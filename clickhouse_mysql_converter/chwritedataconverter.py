#!/usr/bin/env python
# -*- coding: utf-8 -*-

from chwriteconverter import CHWriteConverter
import datetime

class CHWriteDataConverter(CHWriteConverter):
    def column(self, column, value):

        if column == 'day':
            _datetime = datetime.datetime.strptime(value, '%Y-%m-%d')
            _date = _datetime.date()
            return _date

        return super().column(column, value)
