#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .reader import Reader
import csv


class CSVReader(Reader):

    filename = None
    csvfile = None
    sniffer = None
    dialect = None
    has_header = False
    reader = None

    def __init__(self, csv_file_path, callbacks={}):
        super().__init__(callbacks=callbacks)

        self.csvfile = open(csv_file_path)
        self.sniffer = csv.Sniffer()
        self.dialect = self.sniffer.sniff(self.csvfile.read(1024))
        self.csvfile.seek(0)
        self.has_header = self.sniffer.has_header(self.csvfile.read(1024))
        self.csvfile.seek(0)
        self.reader = csv.DictReader(self.csvfile, dialect=self.dialect)
        if self.has_header:
            print('=======')
            print(self.reader.fieldnames)
            print('=======')
        else:
            # should raise error?
            pass

    def read(self):
        # fetch events
        try:
            #                    self.fire('WriteRowsEvent', binlog_event=event)
            for row in self.reader:
                for column in row:
                    if row[column] == '':
                        row[column] = None
                print(row)
#                        self.fire('WriteRowsEvent.EachRow', binlog_event=event, row=row)
        except KeyboardInterrupt:
            pass

        self.csvfile.close()

if __name__ == '__main__':
    reader = CSVReader(fileame='data.csv')
    reader.read()
