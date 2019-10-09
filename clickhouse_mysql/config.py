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
from clickhouse_mysql.tablesqlbuilder import TableSQLBuilder
from clickhouse_mysql.tablemigrator import TableMigrator
from clickhouse_mysql.clioptions import Options, AggregatedOptions

from clickhouse_mysql.dbclient.chclient import CHClient

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

        log_file = None
        log_pos = None
        if self.options['binlog_position_file'] and self.options.get_bool('src_resume'):
            try:
                with open(self.options['binlog_position_file'], 'r') as f:
                    position = f.read()
                    log_file, log_pos = position.split(':')
                    log_pos = int(log_pos)
                    print("binlog position from file {} is {}:{}".format(
                        self.options['binlog_position_file'],
                        log_file,
                        log_pos
                    ))
            except:
                log_file = None
                log_pos = None
                print("can't read binlog position from file {}".format(
                    self.options['binlog_position_file'],
                ))
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
                'create_table_sql_template': self.options.get_bool('create_table_sql_template'),
                'create_table_sql': self.options.get_bool('create_table_sql'),
                'with_create_database': self.options.get_bool('with_create_database'),
                'create_table_json_template': self.options.get_bool('create_table_json_template'),
                'migrate_table': self.options.get_bool('migrate_table'),
                'pid_file': self.options['pid_file'],
                'binlog_position_file': self.options['binlog_position_file'],
                'mempool': self.options.get_bool('mempool') or self.options.get_bool('csvpool'), # csvpool assumes mempool to be enabled
                'mempool_max_events_num': self.options.get_int('mempool_max_events_num'),
                'mempool_max_rows_num': self.options.get_int('mempool_max_rows_num'),
                'mempool_max_flush_interval': self.options.get_int('mempool_max_flush_interval'),
                'csvpool': self.options.get_bool('csvpool'),
                'pump_data': self.options.get_bool('pump_data'),
                'install': self.options.get_bool('install'),
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
                    'schemas': self.options.get_list('src_schemas'),
                    'tables': self.options.get_list('src_tables'),
                    'tables_prefixes': self.options.get_list('src_tables_prefixes'),
                    'column_skip': self.options['column_skip']
                },
                'clickhouse': {
                    'connection_settings': {
                        'host': self.options['dst_host'],
                        'port': self.options.get_int('dst_port'),
                        'user': self.options['dst_user'],
                        'password': self.options['dst_password'],
                    },
                    'dst_schema': self.options['dst_schema'],
                    'dst_distribute': self.options['dst_distribute'],
                    'dst_cluster': self.options['dst_cluster'],
                    'dst_table': self.options['dst_table'],
                    'dst_table_prefix': self.options['dst_table_prefix'],
                    'dst_create_table': self.options.get_bool('dst_create_table'),
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
                    'schemas': self.options.get_list('src_schemas'),
                    'tables': self.options.get_list('src_tables'),
                    'tables_prefixes': self.options.get_list('src_tables_prefixes'),
                    'tables_where_clauses': self.options.get_list('src_tables_where_clauses'),
                    'column_skip': self.options['column_skip']
                },
                'clickhouse': {
                    'connection_settings': {
                        'host': self.options['dst_host'],
                        'port': self.options.get_int('dst_port'),
                        'user': self.options['dst_user'],
                        'password': self.options['dst_password'],
                    },
                    'dst_schema': self.options['dst_schema'],
                    'dst_distribute': self.options['dst_distribute'],
                    'dst_cluster': self.options['dst_cluster'],
                    'dst_table': self.options['dst_table'],
                    'dst_table_prefix': self.options['dst_table_prefix'],
                    'dst_create_table': self.options.get_bool('dst_create_table'),
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
                    'schemas': self.options.get_list('src_schemas'),
                    'tables': self.options.get_list('src_tables'),
                    'tables_prefixes': self.options.get_list('src_tables_prefixes'),
                    'blocking': self.options.get_bool('src_wait'),
                    'resume_stream': self.options.get_bool('src_resume'),
                    'nice_pause': 0 if self.options.get_int('nice_pause') is None else self.options.get_int('nice_pause'),
                    'log_file': self.options['src_binlog_file'] if self.options['src_binlog_file'] else log_file,
                    'log_pos': self.options.get_int('src_binlog_position') if self.options.get_int('src_binlog_position') else log_pos,
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
                    'dst_distribute': self.options['dst_distribute'],
                    'dst_table': self.options['dst_table'],
                    'dst_table_prefix': self.options['dst_table_prefix'],
                },
                'file': {
                    'csv_file_path': self.options['dst_file'],
                    'csv_file_path_prefix': self.options['csvpool_file_path_prefix'],
                    'csv_file_path_suffix_parts': [],
                    'csv_keep_file': self.options['csvpool_keep_files'],
                    'dst_schema': self.options['dst_schema'],
                    'dst_distribute': self.options['dst_distribute'],
                    'dst_table': self.options['dst_table'],
                    'dst_table_prefix': self.options['dst_table_prefix'],
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

    def is_create_table_sql_template(self):
        return self.config['app']['create_table_sql_template']

    def is_create_table_sql(self):
        return self.config['app']['create_table_sql']

    def is_with_create_database(self):
        return self.config['app']['with_create_database']

    def is_dst_create_table(self):
        return self.config['table_builder']['clickhouse']['dst_create_table']

    def is_create_table_json_template(self):
        return self.config['app']['create_table_json_template']

    def is_install(self):
        return self.config['app']['install']

    def table_sql_builder(self):
        return TableSQLBuilder(
            host=self.config['table_builder']['mysql']['host'],
            port=self.config['table_builder']['mysql']['port'],
            user=self.config['table_builder']['mysql']['user'],
            password=self.config['table_builder']['mysql']['password'],
            dbs=self.config['table_builder']['mysql']['schemas'],
            dst_schema=self.config['table_builder']['clickhouse']['dst_schema'],
            dst_table=self.config['table_builder']['clickhouse']['dst_table'],
            dst_table_prefix=self.config['table_builder']['clickhouse']['dst_table_prefix'],
            distribute=self.config['table_builder']['clickhouse']['dst_distribute'],
            cluster=self.config['table_builder']['clickhouse']['dst_cluster'],
            tables=self.config['table_builder']['mysql']['tables'],
            tables_prefixes=self.config['table_builder']['mysql']['tables_prefixes'],
            column_skip=self.config['converter']['clickhouse']['column_skip'],
        )

    def is_migrate_table(self):
        return self.config['app']['migrate_table']

    def is_pump_data(self):
        return self.config['app']['pump_data']

    def chclient(self):
        return CHClient(self.config['writer']['clickhouse']['connection_settings'])

    def table_migrator(self):
        table_migrator = TableMigrator(
            host=self.config['table_migrator']['mysql']['host'],
            port=self.config['table_migrator']['mysql']['port'],
            user=self.config['table_migrator']['mysql']['user'],
            password=self.config['table_migrator']['mysql']['password'],
            dbs=self.config['table_migrator']['mysql']['schemas'],
            dst_schema=self.config['table_migrator']['clickhouse']['dst_schema'],
            dst_table=self.config['table_builder']['clickhouse']['dst_table'],
            dst_table_prefix=self.config['table_builder']['clickhouse']['dst_table_prefix'],
            distribute=self.config['table_migrator']['clickhouse']['dst_distribute'],
            cluster=self.config['table_migrator']['clickhouse']['dst_cluster'],
            tables=self.config['table_migrator']['mysql']['tables'],
            tables_prefixes=self.config['table_migrator']['mysql']['tables_prefixes'],
            tables_where_clauses=self.config['table_migrator']['mysql']['tables_where_clauses'],
            column_skip=self.config['converter']['clickhouse']['column_skip'],
        )
        table_migrator.chwriter = self.writer_builder_chwriter().get()
        table_migrator.chclient = self.chclient()
        table_migrator.pool_max_rows_num = self.mempool_max_rows_num()

        return table_migrator

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
                log_file=self.config['reader']['mysql']['log_file'],
                log_pos=self.config['reader']['mysql']['log_pos'],
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

    def writer_builder_csvpool(self):
        return ObjectBuilder(class_name=ProcessWriter, constructor_params={
            'next_writer_builder': ObjectBuilder(class_name=CSVWriter, constructor_params={
                'csv_file_path': self.config['writer']['file']['csv_file_path'],
                'csv_file_path_prefix': self.config['writer']['file']['csv_file_path_prefix'],
                'csv_file_path_suffix_parts': self.config['writer']['file']['csv_file_path_suffix_parts'],
                'csv_keep_file': self.config['writer']['file']['csv_keep_file'],
                'dst_schema': self.config['writer']['file']['dst_schema'],
                'dst_table': self.config['writer']['file']['dst_table'],
                'dst_table_prefix': self.config['writer']['file']['dst_table_prefix'],
                'next_writer_builder': ObjectBuilder(
                    class_name=CHCSVWriter,
                    constructor_params=self.config['writer']['clickhouse']
                ),
                'converter_builder': self.converter_builder(CONVERTER_CSV),
            })
        })

    def writer_builder_csv_file(self):
        return ObjectBuilder(class_name=CSVWriter, constructor_params={
            'csv_file_path': self.config['writer']['file']['csv_file_path'],
            'csv_file_path_prefix': self.config['writer']['file']['csv_file_path_prefix'],
            'csv_file_path_suffix_parts': self.config['writer']['file']['csv_file_path_suffix_parts'],
            'csv_keep_file': self.config['writer']['file']['csv_keep_file'],
            'dst_schema': self.config['writer']['file']['dst_schema'],
            'dst_table': self.config['writer']['file']['dst_table'],
            'dst_table_prefix': self.config['writer']['file']['dst_table_prefix'],
            'next_writer_builder': None,
            'converter_builder': self.converter_builder(CONVERTER_CSV),
        })

    def writer_builder_chwriter(self):
        return ObjectBuilder(class_name=CHWriter, constructor_params={
            'connection_settings': {
                'host': self.config['writer']['clickhouse']['connection_settings']['host'],
                'port': self.config['writer']['clickhouse']['connection_settings']['port'],
                'user': self.config['writer']['clickhouse']['connection_settings']['user'],
                'password': self.config['writer']['clickhouse']['connection_settings']['password'],
            },
            'dst_schema': self.config['writer']['clickhouse']['dst_schema'],
            'dst_table': self.config['writer']['clickhouse']['dst_table'],
            'dst_table_prefix': self.config['writer']['clickhouse']['dst_table_prefix'],
            'dst_distribute': self.config['writer']['clickhouse']['dst_distribute'],
            'next_writer_builder': None,
            'converter_builder': self.converter_builder(CONVERTER_CH),
        })

    def writer_builder(self):
        if self.config['app']['csvpool']:
            return self.writer_builder_csvpool()
        elif self.config['writer']['file']['csv_file_path']:
            return self.writer_builder_csv_file()
        else:
            return self.writer_builder_chwriter()

    def pool_writer(self):
        return PoolWriter(
            writer_builder=self.writer_builder(),
            max_pool_size=self.config['app']['mempool_max_events_num'],
            max_flush_interval=self.config['app']['mempool_max_flush_interval'],
        )

    def writer(self):
        if self.config['app']['mempool']:
            return self.pool_writer()
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
