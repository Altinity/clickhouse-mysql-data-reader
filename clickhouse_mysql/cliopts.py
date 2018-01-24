#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging

from clickhouse_mysql.config import Config


class CLIOpts(object):

    @staticmethod
    def join_into_dict(lists_to_join):
        """
        Join several lists into one dictionary

        :param lists_to_join: is a list of lists
        [['a=b', 'c=d'], ['e=f', 'z=x'], ]

        :return: None or dictionary
        {'a': 'b', 'c': 'd', 'e': 'f', 'z': 'x'}

        """

        if not isinstance(lists_to_join, list):
            return None

        res = {}
        for lst in lists_to_join:
            # lst = ['a=b', 'c=d']
            for column_value_pair in lst:
                # column_value_value = 'a=b'
                column, value = column_value_pair.split('=', 2)
                res[column] = value

        # res = dict {
        #   'col1': 'value1',
        #   'col2': 'value2',
        # }

        # return with sanity check
        if len(res) > 0:
            return res
        else:
            return None

    @staticmethod
    def join(lists_to_join):
        """
        Join several lists into one
        :param lists_to_join: is a list of lists
        [['a', 'b'], ['c', 'd'], ['e', 'f']]
        :return:
        ['a', 'b', 'c', 'd', 'e', 'f']
        """

        if not isinstance(lists_to_join, list):
            return None

        res = []
        for _list in lists_to_join:
            for _item in _list:
                res.append(_item)

        return res

    @staticmethod
    def log_level_from_string(log_level_string):
        """Convert string representation of a log level into logging.XXX constant"""

        level = log_level_string.upper()

        if level == 'CRITICAL':
            return logging.CRITICAL
        if level == 'ERROR':
            return logging.ERROR
        if level == 'WARNING':
            return logging.WARNING
        if level == 'INFO':
            return logging.INFO
        if level == 'DEBUG':
            return logging.DEBUG
        if level == 'NOTSET':
            return logging.NOTSET

        return logging.NOTSET

    @staticmethod
    def config():
        """Parse application's CLI options into options dictionary
        :return: instance of Config
        """

        argparser = argparse.ArgumentParser(
            description='ClickHouse data reader',
            epilog='==============='
        )

        #
        # general app section
        #
        argparser.add_argument(
            '--config-file',
            type=str,
            default=None,
            help='Path to config file. Default - not specified'
        )
        argparser.add_argument(
            '--log-file',
            type=str,
            default=None,
            help='Path to log file. Default - not specified'
        )
        argparser.add_argument(
            '--log-level',
            type=str,
            default="NOTSET",
            help='Log Level. Default - NOTSET'
        )
        argparser.add_argument(
            '--nice-pause',
            type=int,
            default=None,
            help='make nice pause between attempts to read binlog stream'
        )
        argparser.add_argument(
            '--dry',
            action='store_true',
            help='Dry mode - do not do anything that can harm. '
            'Useful for debugging.'
        )
        argparser.add_argument(
            '--daemon',
            action='store_true',
            help='Daemon mode - go to background.'
        )
        argparser.add_argument(
            '--pid-file',
            type=str,
            default='/tmp/reader.pid',
            help='Pid file to be used by app in daemon mode'
        )
        argparser.add_argument(
            '--mempool',
            action='store_true',
            help='Cache data in mem.'
        )
        argparser.add_argument(
            '--mempool-max-events-num',
            type=int,
            default=100000,
            help='Max events number to pool - triggering pool flush'
        )
        argparser.add_argument(
            '--mempool-max-rows-num',
            type=int,
            default=100000,
            help='Max rows number to pool - triggering pool flush'
        )
        argparser.add_argument(
            '--mempool-max-flush-interval',
            type=int,
            default=60,
            help='Max seconds number between pool flushes'
        )
        argparser.add_argument(
            '--csvpool',
            action='store_true',
            help='Cache data in CSV pool files on disk. Requires memory pooling, thus enables --mempool even if it is not explicitly specified'
        )
        argparser.add_argument(
            '--csvpool-file-path-prefix',
            type=str,
            default='/tmp/csvpool_',
            help='File path prefix to CSV pool files'
        )
        argparser.add_argument(
            '--csvpool-keep-files',
            action='store_true',
            help='Keep CSV pool files. Useful for debugging'
        )
        argparser.add_argument(
            '--table-templates',
            action='store_true',
            help='Prepare table templates.'
        )
        argparser.add_argument(
            '--table-templates-with-create-database',
            action='store_true',
            help='Prepare table templates. Prepend each template with CREATE DATABASE statement'
        )
        argparser.add_argument(
            '--table-templates-json',
            action='store_true',
            help='Prepare table templates as JSON. Useful for IPC'
        )
        argparser.add_argument(
            '--table-migrate',
            action='store_true',
            help='Migrate table(s). IMPORTANT!. Target table has to be created in ClickHouse already! See --table-templates option(s) for help.'
        )

        #
        # src section
        #
        argparser.add_argument(
            '--src-server-id',
            type=int,
            default=None,
            help='Set server_id to be used when reading date from MySQL src. Ex.: 1'
        )
        argparser.add_argument(
            '--src-host',
            type=str,
            default=None,
            help='Host to be used when reading from src. Ex.: 127.0.0.1'
        )
        argparser.add_argument(
            '--src-port',
            type=int,
            default=3306,
            help='Port to be used when reading from src. Ex.: 3306'
        )
        argparser.add_argument(
            '--src-user',
            type=str,
            default=None,
            help='Username to be used when reading from src. Ex.: root'
        )
        argparser.add_argument(
            '--src-password',
            type=str,
            default='',
            help='Password to be used when reading from src. Ex.: qwerty'
        )
        argparser.add_argument(
            '--src-schemas',
            type=str,
            default='',
            help='Comma-separated list of schemas to be used when reading from src. Ex.: db1,db2,db3'
        )
        argparser.add_argument(
            '--src-tables',
            type=str,
            default='',
            help='Comma-separated list of tables to be used when reading from src. Ex.: table1,table2,table3'
        )
        argparser.add_argument(
            '--src-tables-where-clauses',
            type=str,
            default='',
            help='Comma-separated list of WHERE clauses for tables to be migrated. Ex.: db1.t1="a=1 and b=2",db2.t2="c=3 and k=4"'
        )
        argparser.add_argument(
            '--src-tables-prefixes',
            type=str,
            default='',
            help='Comma-separated list of table prefixes to be used when reading from src.'
                 'Useful when we need to process unknown-in-advance tables, say day-named log tables, as log_2017_12_27'
                 'Ex.: mylog_,anotherlog_,extralog_3'
        )
        argparser.add_argument(
            '--src-wait',
            action='store_true',
            help='Wait indefinitely for new records to come.'
        )
        argparser.add_argument(
            '--src-resume',
            action='store_true',
            help='Resume reading from previous position.'
        )
        argparser.add_argument(
            '--src-file',
            type=str,
            default=None,
            help='Source file to read data from. CSV'
        )

        #
        # dst section
        #
        argparser.add_argument(
            '--dst-file',
            type=str,
            default=None,
            help='Target file to be used when writing data. CSV'
        )
        argparser.add_argument(
            '--dst-host',
            type=str,
            default=None,
            help='Host to be used when writing to dst. Ex.: 127.0.0.1'
        )
        argparser.add_argument(
            '--dst-port',
            type=int,
            default=9000,
            help='Port to be used when writing to dst. Ex.: 9000'
        )
        argparser.add_argument(
            '--dst-user',
            type=str,
            default='default',
            help='Username to be used when writing to dst. Ex: default'
        )
        argparser.add_argument(
            '--dst-password',
            type=str,
            default='',
            help='Password to be used when writing to dst. Ex.: qwerty'
        )
        argparser.add_argument(
            '--dst-schema',
            type=str,
            default=None,
            help='Database/schema to be used when writing to dst. Ex.: db1'
        )
        argparser.add_argument(
            '--dst-table',
            type=str,
            default=None,
            help='Table to be used when writing to dst. Ex.: table1'
        )

        #
        # converters section
        #
        argparser.add_argument(
            '--column-default-value',
            type=str,
            nargs='*',
            action='append',
            default=None,
            help='Set of key=value pairs for columns default values. Ex.: date_1=2000-01-01 timestamp_1=2002-01-01\ 01:02:03'
        )
        argparser.add_argument(
            '--column-skip',
            type=str,
            nargs='*',
            action='append',
            default=None,
            help='Set of column names to skip. Ex.: column1 column2'
        )
        argparser.add_argument(
            '--ch-converter-file',
            type=str,
            default=None,
            help='Filename where to search for CH converter class'
        )
        argparser.add_argument(
            '--ch-converter-class',
            type=str,
            default=None,
            help='Converter class name in --ch-converter-file file'
        )

        args = argparser.parse_args()

        # build options
        return Config({

            'app-config': {
                'config-file': args.config_file,
                'log-file': args.log_file,
                'log-level': CLIOpts.log_level_from_string(args.log_level),
                'dry': args.dry,
                'daemon': args.daemon,
                'table-templates': args.table_templates or args.table_templates_with_create_database,
                'table-templates-with-create-database': args.table_templates_with_create_database,
                'table-templates-json': args.table_templates_json,
                'table-migrate': args.table_migrate,
                'pid_file': args.pid_file,
                'mempool': args.mempool or args.csvpool, # csvpool assumes mempool to be enabled
                'mempool-max-events-num': args.mempool_max_events_num,
                'mempool-max-rows-num': args.mempool_max_rows_num,
                'mempool-max-flush-interval': args.mempool_max_flush_interval,
                'csvpool': args.csvpool,
            },

            'converter-config': {
                'clickhouse': {
                    'converter_file': args.ch_converter_file,
                    'converter_class': args.ch_converter_class,
                    'column_skip': CLIOpts.join(args.column_skip),
                },
                'csv': {
                    'column_default_value': CLIOpts.join_into_dict(args.column_default_value),
                    'column_skip': CLIOpts.join(args.column_skip),
                },
            },

            'table-builder-config': {
                'mysql': {
                    'host': args.src_host,
                    'port': args.src_port,
                    'user': args.src_user,
                    'password': args.src_password,
                    'dbs': [x for x in args.src_schemas.split(',') if x] if args.src_schemas else None,
                    'tables': [x for x in args.src_tables.split(',') if x] if args.src_tables else None,
                    'tables_prefixes': [x for x in args.src_tables_prefixes.split(',') if x] if args.src_tables_prefixes else None,
                },
            },

            'table-migrator-config': {
                'mysql': {
                    'host': args.src_host,
                    'port': args.src_port,
                    'user': args.src_user,
                    'password': args.src_password,
                    'dbs': [x for x in args.src_schemas.split(',') if x] if args.src_schemas else None,
                    'tables': [x for x in args.src_tables.split(',') if x] if args.src_tables else None,
                    'tables_prefixes': [x for x in args.src_tables_prefixes.split(',') if x] if args.src_tables_prefixes else None,
                    'tables_where_clauses': [x for x in args.src_tables_where_clauses.split(',') if x] if args.src_tables_where_clauses else None,
                },
                'clickhouse': {
                    'connection_settings': {
                        'host': args.dst_host,
                        'port': args.dst_port,
                        'user': args.dst_user,
                        'password': args.dst_password,
                    },
                    'dst_schema': args.dst_schema,
                    'dst_table': args.dst_table,
                },
            },

            'reader-config': {
                'mysql': {
                    'connection_settings': {
                        'host': args.src_host,
                        'port': args.src_port,
                        'user': args.src_user,
                        'passwd': args.src_password,
                    },
                    'server_id': args.src_server_id,
                    'schemas': [x for x in args.src_schemas.split(',') if x] if args.src_schemas else None,
                    'tables': [x for x in args.src_tables.split(',') if x] if args.src_tables else None,
                    'tables_prefixes': [x for x in args.src_tables_prefixes.split(',') if x] if args.src_tables_prefixes else None,
                    'blocking': args.src_wait,
                    'resume_stream': args.src_resume,
                    'nice_pause': 0 if args.nice_pause is None else args.nice_pause,
                },
                'file': {
                    'csv_file_path': args.src_file,
                    'nice_pause': 0 if args.nice_pause is None else args.nice_pause,
                },
            },

            'writer-config': {
                'clickhouse': {
                    'connection_settings': {
                        'host': args.dst_host,
                        'port': args.dst_port,
                        'user': args.dst_user,
                        'password': args.dst_password,
                    },
                    'dst_schema': args.dst_schema,
                    'dst_table': args.dst_table,
                },
                'file': {
                    'csv_file_path': args.dst_file,
                    'csv_file_path_prefix': args.csvpool_file_path_prefix,
                    'csv_file_path_suffix_parts': [],
                    'csv_keep_file': args.csvpool_keep_files,
                    'dst_schema': args.dst_schema,
                    'dst_table': args.dst_table,
                },
            },
        })
