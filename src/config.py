#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .reader.mysqlreader import MySQLReader
from .reader.csvreader import CSVReader

from .writer.chwriter import CHWriter
from .writer.csvwriter import CSVWriter
from .writer.chcsvwriter import CHCSVWriter
from .writer.poolwriter import PoolWriter
from .writer.processwriter import ProcessWriter
from .objectbuilder import ObjectBuilder

from .converter.csvwriteconverter import CSVWriteConverter
from .converter.chwriteconverter import CHWriteConverter


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

    def converter_builder(self):
        if not self.config['converter-config']['csv']['column_default_value']:
            # no default values for CSV columns provided
            return None

        return ObjectBuilder(
            instance=CSVWriteConverter(
                defaults=self.config['converter-config']['csv']['column_default_value']
            ))

    def writer_builder(self):
        if self.config['app-config']['csvpool']:
            return ObjectBuilder(class_name=ProcessWriter, constructor_params={
                'next_writer_builder': ObjectBuilder(class_name=CSVWriter, constructor_params={
                    **self.config['writer-config']['file'],
                    'next_writer_builder': ObjectBuilder(
                        class_name=CHCSVWriter,
                        constructor_params=self.config['writer-config']['clickhouse']['connection_settings']
                    ),
                    'converter_builder': self.converter_builder(),
                })
            })

        elif self.config['writer-config']['file']['csv_file_path']:
            return ObjectBuilder(class_name=CSVWriter, constructor_params={
                **self.config['writer-config']['file'],
                'converter_builder': self.converter_builder(),
            })

        else:
            return ObjectBuilder(class_name=CHWriter, constructor_params={
                **self.config['writer-config']['clickhouse'],
                'converter_builder': ObjectBuilder(instance=CHWriteConverter()),
            })

    def writer(self):
        if self.config['app-config']['mempool']:
            return PoolWriter(
                writer_builder=self.writer_builder(),
                max_pool_size=self.config['app-config']['mempool-max-events-num'],
                max_flush_interval=self.config['app-config']['mempool-max-flush-interval'],
            )
        else:
            return self.writer_builder().get()
