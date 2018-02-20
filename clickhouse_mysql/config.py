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
from clickhouse_mysql.clioptions import Options, AggregatedOptions

from clickhouse_mysql.util import Util

import pprint

CONVERTER_CSV = 1
CONVERTER_CH = 2


class Config(object):

    config = None
    options = None

    def __init__(self):

        # get aggregated options from all sources (config, cli, env)
        self.options = AggregatedOptions()

        # build application config out of aggregated options
        self.config = {
            #
            #
            #
            'app': {
                'config_file': self.options['config_file'],
                'log_file': self.options['log_file'],
                'log_level': Options.log_level_from_string(self.options['log_level']),
                'dry': self.options.get_bool('dry'),
                'daemon': self.options.get_bool('daemon'),
                'table_templates': self.options['table_templates'] or self.options['table_templates_with_create_database'],
                'table_templates_with_create_database': self.options['table_templates_with_create_database'],
                'table_templates_json': self.options['table_templates_json'],
                'table_migrate': self.options.get_bool('table_migrate'),
                'pid_file': self.options['pid_file'],
                'binlog_position_file': self.options['binlog_position_file'],
                'mempool': self.options.get_bool('mempool') or self.options.get_bool('csvpool'), # csvpool assumes mempool to be enabled
                'mempool_max_events_num': self.options['mempool_max_events_num'],
                'mempool_max_rows_num': self.options['mempool_max_rows_num'],
                'mempool_max_flush_interval': self.options['mempool_max_flush_interval'],
                'csvpool': self.options.get_bool('csvpool'),
            },

            #
            #
            #
            'converter': {
                'clickhouse': {
                    'converter_file': self.options['ch_converter_file'],
                    'converter_class': self.options['ch_converter_class'],
                    'column_skip': self.options['column_skip'],
                },
                'csv': {
                    'column_default_value': self.options['column_default_value'],
                    'column_skip': self.options['column_skip'],
                },
            },

            #
            #
            #
            'table_builder': {
                'mysql': {
                    'host': self.options['src_host'],
                    'port': self.options.get_int('src_port'),
                    'user': self.options['src_user'],
                    'password': self.options['src_password'],
                    'dbs': self.options['src_schemas'],
                    'tables': self.options['src_tables'],
                    'tables_prefixes': self.options['src_tables_prefixes'],
                },
            },

            #
            #
            #
            'table_migrator': {
                'mysql': {
                    'host': self.options['src_host'],
                    'port': self.options.get_int('src_port'),
                    'user': self.options['src_user'],
                    'password': self.options['src_password'],
                    'dbs': self.options['src_schemas'],
                    'tables': self.options['src_tables'],
                    'tables_prefixes': self.options['src_tables_prefixes'],
                    'tables_where_clauses': self.options['src_tables_where_clauses'],
                },
                'clickhouse': {
                    'connection_settings': {
                        'host': self.options['dst_host'],
                        'port': self.options.get_int('dst_port'),
                        'user': self.options['dst_user'],
                        'password': self.options['dst_password'],
                    },
                    'dst_schema': self.options['dst_schema'],
                    'dst_table': self.options['dst_table'],
                },
            },

            #
            #
            #
            'reader': {
                'mysql': {
                    'connection_settings': {
                        'host': self.options['src_host'],
                        'port': self.options.get_int('src_port'),
                        'user': self.options['src_user'],
                        'password': self.options['src_password'],
                    },
                    'server_id': self.options.get_int('src_server_id'),
                    'schemas': self.options['src_schemas'],
                    'tables': self.options['src_tables'],
                    'tables_prefixes': self.options['src_tables_prefixes'],
                    'blocking': self.options.get_bool('src_wait'),
                    'resume_stream': self.options.get_bool('src_resume'),
                    'nice_pause': 0 if self.options.get_int('nice_pause') is None else self.options.get_int('nice_pause'),
                },
                'file': {
                    'csv_file_path': self.options['src_file'],
                    'nice_pause': 0 if self.options.get_int('nice_pause') is None else self.options.get_int('nice_pause'),
                },
            },

            #
            #
            #
            'writer': {
                'clickhouse': {
                    'connection_settings': {
                        'host': self.options['dst_host'],
                        'port': self.options.get_int('dst_port'),
                        'user': self.options['dst_user'],
                        'password': self.options['dst_password'],
                    },
                    'dst_schema': self.options['dst_schema'],
                    'dst_table': self.options['dst_table'],
                },
                'file': {
                    'csv_file_path': self.options['dst_file'],
                    'csv_file_path_prefix': self.options['csvpool_file_path_prefix'],
                    'csv_file_path_suffix_parts': [],
                    'csv_keep_file': self.options['csvpool_keep_files'],
                    'dst_schema': self.options['dst_schema'],
                    'dst_table': self.options['dst_table'],
                },
            },
        }

    def __str__(self):
        return pprint.pformat(self.config)

    def __getitem__(self, item):
        return self.config[item]

    def log_file(self):
        return self.config['app']['log_file']

    def log_level(self):
        return self.config['app']['log_level']

    def pid_file(self):
        return self.config['app']['pid_file']

    def mempool_max_rows_num(self):
        return self.config['app']['mempool_max_rows_num']

    def is_daemon(self):
        return self.config['app']['daemon']

    def is_table_templates(self):
        return self.config['app']['table_templates']

    def is_table_templates_with_create_database(self):
        return self.config['app']['table_templates_with_create_database']

    def is_table_templates_json(self):
        return self.config['app']['table_templates_json']

    def table_builder(self):
        return TableBuilder(
            host=self.config['table_builder']['mysql']['host'],
            port=self.config['table_builder']['mysql']['port'],
            user=self.config['table_builder']['mysql']['user'],
            password=self.config['table_builder']['mysql']['password'],
            dbs=self.config['table_builder']['mysql']['dbs'],
            tables=self.config['table_builder']['mysql']['tables'],
            tables_prefixes=self.config['table_builder']['mysql']['tables_prefixes'],
        )

    def is_table_migrate(self):
        return self.config['app']['table_migrate']

    def table_migrator(self):
        return TableMigrator(
            host=self.config['table_migrator']['mysql']['host'],
            port=self.config['table_migrator']['mysql']['port'],
            user=self.config['table_migrator']['mysql']['user'],
            password=self.config['table_migrator']['mysql']['password'],
            dbs=self.config['table_migrator']['mysql']['dbs'],
            tables=self.config['table_migrator']['mysql']['tables'],
            tables_prefixes=self.config['table_migrator']['mysql']['tables_prefixes'],
            tables_where_clauses=self.config['table_migrator']['mysql']['tables_where_clauses'],
        )

    def reader(self):
        if self.config['reader']['file']['csv_file_path']:
            return CSVReader(
                csv_file_path=self.config['reader']['file']['csv_file_path'],
            )
        else:
            return MySQLReader(
                connection_settings={
                    'host': self.config['reader']['mysql']['connection_settings']['host'],
                    'port': self.config['reader']['mysql']['connection_settings']['port'],
                    'user': self.config['reader']['mysql']['connection_settings']['user'],
                    'passwd': self.config['reader']['mysql']['connection_settings']['password'],
                },
                server_id=self.config['reader']['mysql']['server_id'],
                log_file=None,
                log_pos=None,
                schemas=self.config['reader']['mysql']['schemas'],
                tables=self.config['reader']['mysql']['tables'],
                tables_prefixes=self.config['reader']['mysql']['tables_prefixes'],
                blocking=self.config['reader']['mysql']['blocking'],
                resume_stream=self.config['reader']['mysql']['resume_stream'],
                nice_pause=self.config['reader']['mysql']['nice_pause'],
                binlog_position_file=self.config['app']['binlog_position_file'],
            )

    def converter_builder(self, which):
        if which == CONVERTER_CSV:
            return ObjectBuilder(
                instance=CSVWriteConverter(
                    defaults=self.config['converter']['csv']['column_default_value'],
                    column_skip=self.config['converter']['csv']['column_skip'],
                ))

        elif which == CONVERTER_CH:
            if not self.config['converter']['clickhouse']['converter_file'] or not self.config['converter']['clickhouse']['converter_class']:
                # default converter
                return ObjectBuilder(instance=CHWriteConverter(column_skip=self.config['converter']['clickhouse']['column_skip']))
            else:
                # explicitly specified converter
                _class = Util.class_from_file(
                    self.config['converter']['clickhouse']['converter_file'],
                    self.config['converter']['clickhouse']['converter_class']
                )
                return ObjectBuilder(instance=_class(column_skip=self.config['converter']['clickhouse']['column_skip']))

    def writer_builder(self):
        if self.config['app']['csvpool']:
            return ObjectBuilder(class_name=ProcessWriter, constructor_params={
                'next_writer_builder': ObjectBuilder(class_name=CSVWriter, constructor_params={
                    'csv_file_path': self.config['writer']['file']['csv_file_path'],
                    'csv_file_path_prefix': self.config['writer']['file']['csv_file_path_prefix'],
                    'csv_file_path_suffix_parts': self.config['writer']['file']['csv_file_path_suffix_parts'],
                    'csv_keep_file': self.config['writer']['file']['csv_keep_file'],
                    'dst_schema': self.config['writer']['file']['dst_schema'],
                    'dst_table': self.config['writer']['file']['dst_table'],
                    'next_writer_builder': ObjectBuilder(
                        class_name=CHCSVWriter,
                        constructor_params=self.config['writer']['clickhouse']
                    ),
                    'converter_builder': self.converter_builder(CONVERTER_CSV),
                })
            })

        elif self.config['writer']['file']['csv_file_path']:
            return ObjectBuilder(class_name=CSVWriter, constructor_params={
                'csv_file_path': self.config['writer']['file']['csv_file_path'],
                'csv_file_path_prefix': self.config['writer']['file']['csv_file_path_prefix'],
                'csv_file_path_suffix_parts': self.config['writer']['file']['csv_file_path_suffix_parts'],
                'csv_keep_file': self.config['writer']['file']['csv_keep_file'],
                'dst_schema': self.config['writer']['file']['dst_schema'],
                'dst_table': self.config['writer']['file']['dst_table'],
                'next_writer_builder': None,
                'converter_builder': self.converter_builder(CONVERTER_CSV),
            })

        else:
            return ObjectBuilder(class_name=CHWriter, constructor_params={
                'connection_settings': {
                    'host': self.config['writer']['clickhouse']['connection_settings']['host'],
                    'port': self.config['writer']['clickhouse']['connection_settings']['port'],
                    'user': self.config['writer']['clickhouse']['connection_settings']['user'],
                    'password': self.config['writer']['clickhouse']['connection_settings']['password'],
                },
                'dst_schema': self.config['writer']['clickhouse']['dst_schema'],
                'dst_table': self.config['writer']['clickhouse']['dst_table'],
                'next_writer_builder': None,
                'converter_builder': self.converter_builder(CONVERTER_CH),
            })

    def writer(self):
        if self.config['app']['mempool']:
            return PoolWriter(
                writer_builder=self.writer_builder(),
                max_pool_size=self.config['app']['mempool_max_events_num'],
                max_flush_interval=self.config['app']['mempool_max_flush_interval'],
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
