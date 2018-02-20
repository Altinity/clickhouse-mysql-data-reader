#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import pprint


class Options(object):
    @staticmethod
    def join_lists_into_dict(lists_to_join):
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
    def join_lists(lists_to_join):
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

        if isinstance(log_level_string, str):
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


class CLIOptions(Options):
    """Options extracted from command line"""

    @staticmethod
    def options():
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
            default=None,
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
            '--binlog-position-file',
            type=str,
            default=None,
            help='File to write binlog position to'
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
            default=None,
            help='Password to be used when reading from src. Ex.: qwerty'
        )
        argparser.add_argument(
            '--src-schemas',
            type=str,
            default=None,
            help='Comma-separated list of schemas to be used when reading from src. Ex.: db1,db2,db3'
        )
        argparser.add_argument(
            '--src-tables',
            type=str,
            default=None,
            help='Comma-separated list of tables to be used when reading from src. Ex.: table1,table2,table3'
        )
        argparser.add_argument(
            '--src-tables-where-clauses',
            type=str,
            default=None,
            help='Comma-separated list of WHERE clauses for tables to be migrated. Ex.: db1.t1="a=1 and b=2",db2.t2="c=3 and k=4"'
        )
        argparser.add_argument(
            '--src-tables-prefixes',
            type=str,
            default=None,
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
            default=None,
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

        return {
            #
            # general app section
            #
            'config_file': args.config_file,
            'log_file': args.log_file,
            'log_level': args.log_level,
            'nice_pause': args.nice_pause,
            'dry': args.dry,
            'daemon': args.daemon,
            'pid_file': args.pid_file,
            'binlog_position_file': args.binlog_position_file,
            'mempool': args.mempool, # csvpool assumes mempool to be enabled
            'mempool_max_events_num': args.mempool_max_events_num,
            'mempool_max_rows_num': args.mempool_max_rows_num,
            'mempool_max_flush_interval': args.mempool_max_flush_interval,
            'csvpool': args.csvpool,
            'csvpool_file_path_prefix': args.csvpool_file_path_prefix,
            'csvpool_keep_files': args.csvpool_keep_files,
            'table_templates': args.table_templates,
            'table_templates_with_create_database': args.table_templates_with_create_database,
            'table_templates_json': args.table_templates_json,
            'table_migrate': args.table_migrate,

            #
            # src section
            #
            'src_server_id': args.src_server_id,
            'src_host': args.src_host,
            'src_port': args.src_port,
            'src_user': args.src_user,
            'src_password': args.src_password,
            'src_schemas': [x for x in args.src_schemas.split(',') if x] if args.src_schemas else None,
            'src_tables': [x for x in args.src_tables.split(',') if x] if args.src_tables else None,
            'src_tables_where_clauses': [x for x in args.src_tables_where_clauses.split(',') if x] if args.src_tables_where_clauses else None,
            'src_tables_prefixes': [x for x in args.src_tables_prefixes.split(',') if x] if args.src_tables_prefixes else None,
            'src_wait': args.src_wait,
            'src_resume': args.src_resume,
            'src_file': args.src_file,

            #
            # dst section
            #
            'dst_file': args.dst_file,
            'dst_host': args.dst_host,
            'dst_port': args.dst_port,
            'dst_user': args.dst_user,
            'dst_password': args.dst_password,
            'dst_schema': args.dst_schema,
            'dst_table': args.dst_table,

            #
            # converters section
            #
            'column_default_value': CLIOptions.join_lists_into_dict(args.column_default_value),
            'column_skip': CLIOptions.join_lists(args.column_skip),
            'ch_converter_file': args.ch_converter_file,
            'ch_converter_class': args.ch_converter_class,
        }

from configobj import ConfigObj


class ConfigFileOptions(Options):
    """Options extracted from configuration files"""

    @staticmethod
    def options(filename):

        #
        def transform(section, key):
            newkey = key.replace('-', '_')
            section.rename(key, newkey)

        # fetch base config
        try:
            base_config = ConfigObj(
                infile='/etc/clickhouse-mysql/config.ini',
                encoding="utf-8",
                default_encoding="utf-8",
                list_values=True,
                create_empty=False, # create empty config file
                stringify=True,
                raise_errors=False,
                file_error=False,
            )
        except:
            base_config = None

        # fetch user config
        try:
            user_config = ConfigObj(
                filename,
                encoding="utf-8",
                default_encoding="utf-8",
                list_values=True,
                create_empty=False, # create empty config file
                stringify=True,
                raise_errors=False,
                file_error=False,
            )
        except:
            user_config = None

        # merge base and user configs
        # user config has priority over base config

        if base_config and user_config:
            base_config.merge(user_config)
            base_config.walk(transform, call_on_sections=True)
            return base_config

        if base_config:
            base_config.walk(transform, call_on_sections=True)
            return base_config

        if user_config:
            user_config.walk(transform, call_on_sections=True)
            return user_config

        return None


class AggregatedOptions(object):
    """Aggregated and prioritized options"""

    cli_opts = None
    cfg_opts = None
    env_opts = None

    def __init__(self):
        """Build aggregated options"""
        self.cli_opts = CLIOptions.options()
        self.cfg_opts = ConfigFileOptions.options(self.cli_opts['config_file'])

    def get_from_src(self, src, *coordinates):
        """Fetch an option by specified coordinates from provided source"""

        first_iteration = True
        for coordinate in coordinates:
            try:
                section = src[coordinate] if first_iteration else section[coordinate]
            except:
                return None
            first_iteration = False

        return section

    def get(self, *coordinates):
        """
        Fetch an option by specified coordinates according to source priorities.
        Priority would be:
        1. config (lower priority)
        2. CLI opts
        """
        cfg_opt = self.get_from_src(self.cfg_opts, *coordinates)
        cli_opt = self.get_from_src(self.cli_opts, *coordinates)

        if cli_opt:
            # CLI opt - top priority
            return cli_opt

        if cfg_opt:
            # cfg opt - lower priority
            return cfg_opt

        # option not available
        return None

    def get_int(self, *coordinates):
        value = self.get(*coordinates)
        if value is not None:
            value = int(value)
        return value

    def get_bool(self, *coordinates):
        value = self.get(*coordinates)

        if value is None:
            # None is not interpreted
            return None
        elif isinstance(value, bool):
            # bool is ready-to-use
            return value
        elif isinstance(value, str):
            # str can be interpreted as "yes", "1", "on"
            value = value.upper()
            if (value == '1') or (value == 'YES') or (value == 'ON'):
                return True
            else:
                return False
        else:
            # int and all the rest just cast into bool
            return bool(value)

    def __getitem__(self, coordinates_tuple):
        if isinstance(coordinates_tuple, tuple):
            return self.get(*coordinates_tuple)
        else:
            return self.get(coordinates_tuple)

    def __str__(self):
        str = 'OPTIONS:\n'
        if self.cli_opts:
            str += 'CLI: =================\n'
            str += pprint.pformat(self.cli_opts)
            str += '\n'

        if self.cfg_opts:
            dict = self.cfg_opts.walk(lambda section, key: section[key])
            str += 'CFG: =================\n'
            str += pprint.pformat(dict)
            str += '\n'

        return str
