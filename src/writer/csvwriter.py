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
    dst_schema = None
    dst_table = None
    fieldnames = None
    header_written = False
    path_prefix = None
    path_suffix_parts = []
    delete = False

    def __init__(
            self,
            csv_file_path=None,
            csv_file_path_prefix=None,
            csv_file_path_suffix_parts=[],
            csv_keep_file=False,
            dst_schema=None,
            dst_table=None,
            next_writer_builder=None,
            converter_builder=None,
    ):
        super().__init__(next_writer_builder=next_writer_builder, converter_builder=converter_builder)

        self.path = csv_file_path
        self.path_prefix = csv_file_path_prefix
        self.path_suffix_parts = csv_file_path_suffix_parts
        self.dst_schema = dst_schema
        self.dst_table = dst_table

        if self.path is None:
            self.path = self.path_prefix + '_'.join(self.path_suffix_parts) + '.csv'
            self.delete = not csv_keep_file

    def __del__(self):
        self.destroy()

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

        events = self.listify(event_or_events)
        if len(events) < 1:
            return

        if not self.opened():
            self.open()

        if not self.writer:
            self.fieldnames = sorted(events[0].row.keys())
            if self.dst_schema is None:
                self.dst_schema = events[0].schema
            if self.dst_table is None:
                self.dst_table = events[0].table

            self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames)
            if not self.header_written:
                self.writer.writeheader()

        for event in events:
            self.writer.writerow(self.convert(event).row)

    def push(self):
        if not self.next_writer_builder:
            return

        event = Event()
        event.schema = self.dst_schema
        event.table = self.dst_table
        event.file = self.path
        event.fieldnames = self.fieldnames
        self.next_writer_builder.get().insert(event)

    def close(self):
        if self.opened():
            self.file.flush()
            self.file.close()
            self.file = None
            self.writer = None

    def destroy(self):
        if self.delete and os.path.isfile(self.path):
            self.close()
            os.remove(self.path)

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
