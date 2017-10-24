#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.cliopts import CLIOpts
from src.pumper import Pumper
from src.daemon import Daemon
from src.reader.mysqlreader import MySQLReader
from src.reader.csvreader import CSVReader
from src.writer.chwriter import CHWriter
from src.writer.csvwriter import CSVWriter

import sys


if sys.version_info[0] < 3:
    raise "Must be using Python 3"


class Main(Daemon):

    config = None

    def __init__(self):
        cliopts = CLIOpts()
        self.config = cliopts.options
        print('---')
        print(self.config)
        print('---')
        super().__init__(pidfile=self.config['app-config']['pid_file'])

    def run(self):
        pumper = Pumper(
            reader=MySQLReader(**self.config['reader-config']['mysql']),
#            reader=CSVReader(**self.config['reader-config']['file']),
#            writer=CHWriter(**self.config['writer-config']['clickhouse'])
            writer = CSVWriter(**self.config['writer-config']['file']),
            skip_empty=False
        )
        pumper.run()

    def start(self):
        if self.config['app-config']['daemon']:
            if not super().start():
                print("Error going background. The process already running?")
        else:
            self.run()


if __name__ == '__main__':
    main = Main()
    main.start()
