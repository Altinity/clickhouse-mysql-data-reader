#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Event(object):

    # db name
    schema = None

    # table name
    table = None

    # {'id':1, 'col1':1}
    row = None

    # [{'id': 1, 'col1':1}, {'id': 2, 'col1': 2}, {'id': 3, 'col1': 3}]
    rows = None

    # /path/to/csv/file.csv
    filename = None

    # ['id', 'col1', 'col2']
    fieldnames = None

    def first(self):
        if self.rows is None:
            return self.row
        else:
            return self.rows[0]

    def all(self):
        if self.rows is None:
            return [self.row]
        else:
            return self.rows
