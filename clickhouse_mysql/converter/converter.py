#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from clickhouse_mysql.event.event import Event


class Converter(object):

    column_skip = []

    def __init__(self, column_skip):
        logging.debug("Converter __init__()")
        self.column_skip = [] if column_skip is None else column_skip
        logging.debug(self.column_skip)

    def row(self, row):
        return row

    def rows(self, rows):
        if rows is None:
            return None

        res = []
        for row in rows:
            res.append(self.row(row))

        return res

    def convert(self, event_or_row):
        if isinstance(event_or_row, Event):
            return event_or_row.convert(self)
        else:
            return self.row(event_or_row)
