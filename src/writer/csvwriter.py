#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .writer import Writer
from ..event.event import Event
import csv
import os.path


class CSVWriter(Writer):

    file = None
    path = None
    writer = None
    header_written = False

    def __init__(self, csv_file_path):
        self.path = csv_file_path

    def opened(self):
        return bool(self.file)

    def open(self):
        if not self.opened():
            # do not write header to already existing file
            # assume it was written earlier
            if os.path.isfile(self.path):
                self.header_written = True
            # open file for write-at-the-end mode
            self.file = open(self.path, 'a+')

    def insert(self, event_or_events):
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]

        if event_or_events is None:
            # nothing to insert at all
            return

        elif isinstance(event_or_events, list):
            if len(event_or_events) < 1:
                # list is empty - nothing to insert
                return

        else:
            # event_or_events is instance of Event
            event_or_events = [event_or_events]

        if not self.opened():
            self.open()

        if not self.writer:
            self.writer = csv.DictWriter(self.file, fieldnames=sorted(event_or_events[0].row.keys()))
            if not self.header_written:
                self.writer.writeheader()

        for event in event_or_events:
            self.writer.writerow(event.row)

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
