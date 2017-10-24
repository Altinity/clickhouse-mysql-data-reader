#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Pumper(object):

    reader = None
    writer = None

    def __init__(self, reader=None, writer=None):

        self.reader = reader
        if self.reader:
            self.reader.subscribe({
                'WriteRowsEvent': self.write_rows_event,
                'WriteRowsEvent.EachRow': self.write_rows_event_each_row,
            })
        self.writer = writer

    def run(self):
        self.reader.read()

    def write_rows_event(self, event=None):
#        binlog_event.dump()
        pass

    def write_rows_event_each_row(self, event=None):
        self.writer.insert(event=event)

if __name__ == '__main__':
    print("pumper")
