#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .reader.mysqlreader import MySQLReader
from .reader.csvreader import CSVReader
from .writer.chwriter import CHWriter
from .writer.csvwriter import CSVWriter
from .writer.poolwriter import PoolWriter


class Config(object):

    config = None

    def __init__(self, config):
        self.config = config

    def __str__(self):
        return str(self.config)

    def __getitem__(self, item):
        return self.config[item]

    def pid_file(self):
        return self.config['app-config']['pid_file']

    def is_daemon(self):
        return self.config['app-config']['daemon']

    def is_pool(self):
        return self.config['app-config']['mempool']

    def reader(self):
        if self.config['reader-config']['file']['csv_file_path']:
            return CSVReader(**self.config['reader-config']['file'])
        else:
            return MySQLReader(**self.config['reader-config']['mysql'])

    def writer_class(self):
        if self.config['writer-config']['file']['csv_file_path']:
            return CSVWriter, self.config['writer-config']['file']
        else:
            return CHWriter, self.config['writer-config']['clickhouse']

    def writer(self):
        writer_class, writer_params = self.writer_class()

        if self.config['app-config']['mempool']:
            return PoolWriter(
                writer_class=writer_class,
                writer_params=writer_params,
                max_pool_size=self.config['app-config']['mempool-max-events-num'],
            )
        else:
            return writer_class(**writer_params)