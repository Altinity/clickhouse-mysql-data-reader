#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ..event.event import Event


class Converter(object):

    def row(self, row):
        return row

    def rows(self, rows):
        if rows is None:
            return None

        res = []
        for row in rows:
            res.append(self.row(row))

        return res

    def event(self, event):
        event.row = self.row(event.row)
        event.rows = self.rows(event.rows)
        return event

    def convert(self, event_or_row):
        if isinstance(event_or_row, Event):
            return self.event(event_or_row)
        else:
            return self.row(event_or_row)
