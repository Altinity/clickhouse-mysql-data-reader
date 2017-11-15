#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from .config import Config
import logging


class CLIOpts(object):

    @staticmethod
    def join(lists_to_join):
        """Join several lists into one

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
    def log_level_from_string(log_level_string):
        level = log_level_string.upper()

        if level == 'CRITICAL':
            return logging.CRITICAL
        elif level == 'ERROR':
            return logging.ERROR
        elif level == 'WARNING':
            return logging.WARNING
        elif level == 'INFO':
            return logging.INFO
        elif level == 'DEBUG':
            return logging.DEBUG
        elif level == 'NOTSET':
            return logging.NOTSET
        else:
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
            '--src-server-id',
            type=int,
            default=1,
            help='Set server_id to be used when reading from src'
        )
        argparser.add_argument(
            '--src-host',
            type=str,
            default='127.0.0.1',
            help='Host to be used when reading from src'
        )
        argparser.add_argument(
            '--src-port',
            type=int,
            default=3306,
            help='Port to be used when reading from src'
        )
        argparser.add_argument(
            '--src-user',
            type=str,
            default='root',
            help='Username to be used when reading from src'
        )
        argparser.add_argument(
            '--src-password',
            type=str,
            default='',
            help='Password to be used when reading from src'
        )
        argparser.add_argument(
            '--src-only-schemas',
            type=str,
            default='',
            help='Comma-separated list of schemas to be used when reading from src'
        )
        argparser.add_argument(
            '--src-only-tables',
            type=str,
            default='',
            help='Comma-separated list of tables to be used when reading from src'
        )
        argparser.add_argument(
            '--src-wait',
            action='store_true',
            help='Wait for new records to come'
        )
        argparser.add_argument(
            '--src-resume',
            action='store_true',
            help='Resume reading from previous position'
        )
        argparser.add_argument(
            '--src-file',
            type=str,
            default=None,
            help='Source file to read data from'
        )

        argparser.add_argument(
            '--dst-file',
            type=str,
            default=None,
            help='Target file to be used when writing data'
        )
        argparser.add_argument(
            '--dst-host',
            type=str,
            default='127.0.0.1',
            help='Host to be used when writing to dst'
        )
        argparser.add_argument(
            '--dst-port',
            type=int,
            default=9000,
            help='Port to be used when writing to dst'
        )
        argparser.add_argument(
            '--dst-user',
            type=str,
            default='default',
            help='Username to be used when writing to dst'
        )
        argparser.add_argument(
            '--dst-password',
            type=str,
            default='',
            help='Password to be used when writing to dst'
        )
        argparser.add_argument(
            '--dst-schema',
            type=str,
            default=None,
            help='Database/schema to be used when writing to dst'
        )
        argparser.add_argument(
            '--dst-table',
            type=str,
            default=None,
            help='Table to be used when writing to dst'
        )

        argparser.add_argument(
            '--csv-column-default-value',
            type=str,
            nargs='*',
            action='append',
            default=None,
            help='Table to be used when writing to dst'
        )

        args = argparser.parse_args()

        # build options
        return Config ({
            'app-config': {
                'config-file': args.config_file,
                'log-file': args.log_file,
                'log-level': CLIOpts.log_level_from_string(args.log_level),
                'dry': args.dry,
                'daemon': args.daemon,
                'pid_file': args.pid_file,
                'mempool': args.mempool or args.csvpool, # csvpool assumes mempool to be enabled
                'mempool-max-events-num': args.mempool_max_events_num,
                'mempool-max-flush-interval': args.mempool_max_flush_interval,
                'csvpool': args.csvpool,
            },

            'converter-config': {
                'clickhouse': {

                },
                'csv': {
                    'column_default_value': CLIOpts.join(args.csv_column_default_value),
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
                    'only_schemas': [x for x in args.src_only_schemas.split(',') if x] if args.src_only_schemas else None,
                    'only_tables': [x for x in args.src_only_tables.split(',') if x] if args.src_only_tables else None,
                    'blocking': args.src_wait,
                    'resume_stream': args.src_resume,
                },
                'file': {
                    'csv_file_path': args.src_file,
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
