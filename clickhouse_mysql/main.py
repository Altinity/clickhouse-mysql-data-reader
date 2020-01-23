#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import multiprocessing as mp
import logging
import traceback
import pprint
import json
import os
import pkg_resources
import shutil

if sys.version_info < (3, 4):
    print("Python version is NOT OK, need 3.4 at least")
    sys.exit(1)

from clickhouse_mysql.clioptions import CLIOptions
from clickhouse_mysql.pumper import Pumper
from clickhouse_mysql.daemon import Daemon
from clickhouse_mysql.config import Config


class Main(Daemon):

    config = None

    def __init__(self):

        # append 'converter' folder into sys.path
        # this helps to load custom modules
        converter_folder = os.path.dirname(os.path.realpath(__file__)) + '/converter'
        if converter_folder not in sys.path:
            sys.path.insert(0, converter_folder)

        # parse CLI options
        self.config = Config()

        # first action after config available - setup requested logging level
        logging.basicConfig(
            filename=self.config.log_file(),
            level=self.config.log_level(),
            format='%(asctime)s/%(created)f:%(levelname)s:%(message)s'
        )

        # and call parent
        super().__init__(pidfile=self.config.pid_file())

        # some verbosity
        logging.info('Starting')
        logging.debug(self.config)
        logging.info("sys.path")
        logging.info(pprint.pformat(sys.path))
#        mp.set_start_method('forkserver')

    @staticmethod
    def install():
        # install service file
        src_service_filepath = os.path.abspath(
            pkg_resources.resource_filename('clickhouse_mysql', '../clickhouse_mysql.init.d/clickhouse-mysql'))
        dst_service_dir = '/etc/init.d/'
        dst_service_filepath = dst_service_dir + 'clickhouse-mysql'
        try:
            print("Install service file ", src_service_filepath, ' to ', dst_service_filepath)
            # os.makedirs wants '/' at the end of the folder name
            os.makedirs(name=dst_service_dir, exist_ok=True)
            shutil.copy2(src_service_filepath, dst_service_filepath)
        except Exception as e:
            print(e)
            print('Looks like you have no permissions to write to ', dst_service_filepath)
            sys.exit(1)

        # install config example file
        src_service_filepath = os.path.abspath(
            pkg_resources.resource_filename('clickhouse_mysql', '../clickhouse_mysql.etc/clickhouse-mysql.conf'))
        dst_service_dir = '/etc/clickhouse-mysql/'
        dst_service_filepath = dst_service_dir + 'clickhouse-mysql-example.conf'
        try:
            print("Install config file ", src_service_filepath, ' to ', dst_service_filepath)
            # os.makedirs wants '/' at the end of the folder name
            os.makedirs(name=dst_service_dir, exist_ok=True)
            shutil.copy2(src_service_filepath, dst_service_filepath)
        except Exception as e:
            print(e)
            print('Looks like you have no permissions to write to ', dst_service_filepath)
            sys.exit(1)

        print("Ensure clickhouse user exists")
        os.system("useradd clickhouse")

        print("Ensure var/run folders available")
        os.system("mkdir -p /var/log/clickhouse-mysql")
        os.system("chown clickhouse /var/log/clickhouse-mysql")
        os.system("mkdir -p /var/run/clickhouse-mysql")
        os.system("chown clickhouse /var/run/clickhouse-mysql")

    def run(self):
        try:
            # what action are we going to do

            # first of all, check for auxiliary activities (such as CREATE TABLE SQL statements, etc)

            # run installation process
            if self.config.is_install():
                Main.install()
                return

            # get JSON-ed SQL statements for review
            if self.config.is_create_table_json_template():
                # we are going to prepare table templates in JSON form
                print(json.dumps(self.config.table_sql_builder().templates()))
                return

            # get SQL statements for review
            if self.config.is_create_table_sql() or self.config.is_create_table_sql_template():

                templates = self.config.table_sql_builder().templates()

                for db in templates:
                    for table in templates[db]:
                        if self.config.is_with_create_database():
                            print("{};".format(templates[db][table]['create_database']))
                        if self.config.is_create_table_sql_template():
                            print("{};".format(templates[db][table]['create_table_template']))
                        if self.config.is_create_table_sql():
                            print("{};".format(templates[db][table]['create_table']))

            # auxiliary activities completed
            # run main activities (such as migrate or pump data)

            # main activities may be prepended with dst tables creation
            if self.config.is_dst_create_table():
                migrator = self.config.table_migrator()
                migrator.migrate_all_tables(self.config.is_dst_create_table())

            # run data migration
            if self.config.is_migrate_table():
                # we are going to migrate data
                migrator = self.config.table_migrator()
                migrator.migrate_all_tables_data()

            # pump data to Clickhouse
            if self.config.is_pump_data():
                pumper = Pumper(
                    reader=self.config.reader(),
                    writer=self.config.writer(),
                )
                pumper.run()

        except Exception as ex:
            logging.critical(ex)
            print('=============')
            traceback.print_exc(file=sys.stdout)
            print('=============')
            print(ex)
            sys.exit(1);

    def start(self):
        if self.config.is_daemon():
            if not super().start():
                logging.error("Error going background. The process already running?")
        else:
            self.run()


if __name__ == '__main__':
    main = Main()
    main.start()
