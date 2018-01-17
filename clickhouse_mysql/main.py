#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import multiprocessing as mp
import logging
import traceback
import pprint
import json
import os

if sys.version_info < (3, 5):
    print("Python version is NOT OK, need 3.5 at least")
    sys.exit(1)

from clickhouse_mysql.cliopts import CLIOpts
from clickhouse_mysql.pumper import Pumper
from clickhouse_mysql.daemon import Daemon


class Main(Daemon):

    config = None

    def __init__(self):

        # append 'converter' folder into sys.path
        # this helps to load custom modules
        converter_folder = os.path.dirname(os.path.realpath(__file__)) + '/converter'
        if converter_folder not in sys.path:
            sys.path.insert(0, converter_folder)

        self.config = CLIOpts.config()

        logging.basicConfig(
            filename=self.config.log_file(),
            level=self.config.log_level(),
            format='%(asctime)s/%(created)f:%(levelname)s:%(message)s'
        )
        super().__init__(pidfile=self.config.pid_file())
        logging.info('Starting')
        logging.debug(pprint.pformat(self.config.config))
        logging.info("sys.path")
        logging.info(pprint.pformat(sys.path))
#        mp.set_start_method('forkserver')

    def run(self):
        try:
            if self.config.is_table_templates():
                templates = self.config.table_builder().templates(self.config.is_table_templates_json())

                for db in templates:
                    for table in templates[db]:
                        if self.config.is_table_templates_with_create_database():
                            print("CREATE DATABASE IF NOT EXISTS `{}`;".format(db))
                        print("{};".format(templates[db][table]))

            elif self.config.is_table_templates_json():
                print(json.dumps(self.config.table_builder().templates(self.config.is_table_templates_json())))

            elif self.config.is_table_migrate():
                migrator = self.config.table_migrator()
                migrator.chwriter = self.config.writer()
                migrator.pool_max_rows_num = self.config.mempool_max_rows_num()
                migrator.migrate()

            else:
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

    def start(self):
        if self.config.is_daemon():
            if not super().start():
                logging.error("Error going background. The process already running?")
        else:
            self.run()


if __name__ == '__main__':
    main = Main()
    main.start()
