#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .writer import Writer
from ..event.event import Event
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

    def insert(self, event):

        # values [{'id': 3, 'a': 3}, {'id': 2, 'a': 2}]
        # ensure values is a list
        values = [event.row] if isinstance(event.row, dict) else event.row

        if not self.opened():
            self.open()

        if not self.writer:
            self.writer = csv.DictWriter(self.file, fieldnames=values[0].keys())
            self.writer.writeheader()

        for row in values:
            self.writer.writerow(row)

    def batch(self, events):

        if len(events) < 1:
            return

        values = []

        for event in events:
            values.append(event.row)

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
    event = Event()
    event.row_converted={
        'a': 123,
        'b': 456,
        'c': 'qwe',
        'd': 'rty',
    }
    writer.insert(event)
    event.row_converted={
        'a': 789,
        'b': 987,
        'c': 'asd',
        'd': 'fgh',
    }
    writer.insert(event)
    writer.close()
