#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import os

from clickhouse_mysql.reader.reader import Reader
from clickhouse_mysql.event.event import Event
from clickhouse_mysql.converter.csvreadconverter import CSVReadConverter


class CSVReader(Reader):
    """Read data from CSV files"""

    csv_file_path = None
    csvfile = None
    sniffer = None
    dialect = None
    has_header = False
    reader = None

    def __init__(
            self,
            csv_file_path,
            converter=None,
            callbacks={}
    ):
        super().__init__(converter=converter, callbacks=callbacks)

        self.csv_file_path = csv_file_path
        self.csvfile = open(self.csv_file_path)
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
            event = Event()
            event.table = os.path.splitext(self.csv_file_path)[0]
            self.notify('WriteRowsEvent', event=event)
            for row in self.reader:
                event.row = row
                self.notify('WriteRowsEvent.EachRow', event=self.converter.convert(event) if self.converter else event)
        except KeyboardInterrupt:
            pass

        self.csvfile.close()

if __name__ == '__main__':
    reader = CSVReader(filename='data.csv')
    reader.read()
