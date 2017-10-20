#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .writer import Writer
import csv


class CSVWriter(Writer):

    file = None
    path = None
    writer = None

    def __init__(self, csv_file_path):
        self.path = csv_file_path

    def opened(self):
        return bool(self.file)

    def open(self):
        if not self.opened():
            self.file = open(self.path, 'w')

    def insert(self, schema=None, table=None, values=None):

        # values [{'id': 3, 'a': 3}, {'id': 2, 'a': 2}]
        # ensure values is a list
        values = [values] if isinstance(values, dict) else values

        if not self.opened():
            self.open()

        if not self.writer:
            self.writer = csv.DictWriter(self.file, fieldnames=values[0].keys())
            self.writer.writeheader()

        for row in values:
            self.writer.writerow(row)

    def close(self):
        if self.opened():
            self.file.close()
            self.file = None
            self.writer = None

if __name__ == '__main__':
    path = 'file.csv'

    writer = CSVWriter(path)
    writer.open()
    writer.insert(values={
        'a': 123,
        'b': 456,
        'c': 'qwe',
        'd': 'rty',
    })
    writer.insert(values={
        'a': 789,
        'b': 987,
        'c': 'asd',
        'd': 'fgh',
    })
    writer.close()
