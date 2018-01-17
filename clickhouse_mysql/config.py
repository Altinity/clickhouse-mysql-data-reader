#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clickhouse_mysql.reader.mysqlreader import MySQLReader
from clickhouse_mysql.reader.csvreader import CSVReader

from clickhouse_mysql.writer.chwriter import CHWriter
from clickhouse_mysql.writer.csvwriter import CSVWriter
from clickhouse_mysql.writer.chcsvwriter import CHCSVWriter
from clickhouse_mysql.writer.poolwriter import PoolWriter
from clickhouse_mysql.writer.processwriter import ProcessWriter
from clickhouse_mysql.objectbuilder import ObjectBuilder

from clickhouse_mysql.converter.csvwriteconverter import CSVWriteConverter
from clickhouse_mysql.converter.chwriteconverter import CHWriteConverter
from clickhouse_mysql.tablebuilder import TableBuilder
from clickhouse_mysql.tablemigrator import TableMigrator

from clickhouse_mysql.util import Util

CONVERTER_CSV = 1
CONVERTER_CH = 2

class Config(object):

    config = None

    def __init__(self, config):
        self.config = config

    def __str__(self):
        return str(self.config)

    def __getitem__(self, item):
        return self.config[item]

    def log_file(self):
        return self.config['app-config']['log-file']

    def log_level(self):
        return self.config['app-config']['log-level']

    def nice_pause(self):
        return self.config['app-config']['nice-pause']

    def pid_file(self):
        return self.config['app-config']['pid_file']

    def mempool_max_events_num(self):
        return self.config['app-config']['mempool-max-events-num']

    def mempool_max_rows_num(self):
        return self.config['app-config']['mempool-max-rows-num']

    def is_daemon(self):
        return self.config['app-config']['daemon']

    def is_pool(self):
        return self.config['app-config']['mempool']

    def is_table_templates(self):
        return self.config['app-config']['table-templates']

    def is_table_templates_with_create_database(self):
        return self.config['app-config']['table-templates-with-create-database']

    def is_table_templates_json(self):
        return self.config['app-config']['table-templates-json']

    def table_builder(self):
        return TableBuilder(**self.config['table-builder-config']['mysql'])

    def is_table_migrate(self):
        return self.config['app-config']['table-migrate']

    def table_migrator(self):
        return TableMigrator(**self.config['table-migrator-config']['mysql'])

    def reader(self):
        if self.config['reader-config']['file']['csv_file_path']:
            return CSVReader(**self.config['reader-config']['file'])
        else:
            return MySQLReader(**self.config['reader-config']['mysql'])

    def converter_builder(self, which):
        if which == CONVERTER_CSV:
            if not self.config['converter-config']['csv']['column_default_value']:
                # no default values for CSV columns provided
                return None
            else:
                # default values for CSV columns provided
                return ObjectBuilder(
                    instance=CSVWriteConverter(
                        defaults=self.config['converter-config']['csv']['column_default_value']
                    ))
        elif which == CONVERTER_CH:
            if not self.config['converter-config']['clickhouse']['converter_file'] or not self.config['converter-config']['clickhouse']['converter_class']:
                # default converter
                return ObjectBuilder(instance=CHWriteConverter())
            else:
                # explicitly specfied converter
                _class = Util.class_from_file(self.config['converter-config']['clickhouse']['converter_file'], self.config['converter-config']['clickhouse']['converter_class'])
                return ObjectBuilder(instance=_class())

    def writer_builder(self):
        if self.config['app-config']['csvpool']:
            return ObjectBuilder(class_name=ProcessWriter, constructor_params={
                'next_writer_builder': ObjectBuilder(class_name=CSVWriter, constructor_params={
                    **self.config['writer-config']['file'],
                    'next_writer_builder': ObjectBuilder(
                        class_name=CHCSVWriter,
                        constructor_params=self.config['writer-config']['clickhouse']
                    ),
                    'converter_builder': self.converter_builder(CONVERTER_CSV),
                })
            })

        elif self.config['writer-config']['file']['csv_file_path']:
            return ObjectBuilder(class_name=CSVWriter, constructor_params={
                **self.config['writer-config']['file'],
                'converter_builder': self.converter_builder(CONVERTER_CSV),
            })

        else:
            return ObjectBuilder(class_name=CHWriter, constructor_params={
                **self.config['writer-config']['clickhouse'],
                'converter_builder': self.converter_builder(CONVERTER_CH),
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

    def class_from_file(self, file, class_name):
        """

        :param file: /path/to/file.py
        :param classname: CHWriteConverter
        :return:
        """
        spec = importlib.util.spec_from_file_location("custom.module", file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _class = globals()["custom.module.{}".format(class_name)]
        instance = _class()
        return instance
