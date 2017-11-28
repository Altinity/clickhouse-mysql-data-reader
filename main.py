#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.cliopts import CLIOpts
from src.pumper import Pumper
from src.daemon import Daemon

import sys
import multiprocessing as mp
import logging
import pprint
import json

if sys.version_info[0] < 3:
    raise "Must be using Python 3"



class Main(Daemon):

    config = None

    def __init__(self):
        self.config = CLIOpts.config()

        logging.basicConfig(
            filename=self.config.log_file(),
            level=self.config.log_level(),
            format='%(asctime)s/%(created)f:%(levelname)s:%(message)s'
        )
        super().__init__(pidfile=self.config.pid_file())
        logging.info('Starting')
        logging.debug(pprint.pformat(self.config.config))
#        mp.set_start_method('forkserver')

    def run(self):
        if self.config.is_table_templates():
            templates = self.config.table_builder().templates(self.config.is_table_templates_json())
            for db in templates:
                for table in templates[db]:
                    print(templates[db][table])

        elif self.config.is_table_templates_json():
            print(json.dumps(self.config.table_builder().templates(self.config.is_table_templates_json())))

        else:
            pumper = Pumper(
                reader=self.config.reader(),
                writer=self.config.writer(),
            )
            pumper.run()

    def start(self):
        if self.config.is_daemon():
            if not super().start():
                logging.error("Error going background. The process already running?")
        else:
            self.run()


if __name__ == '__main__':
    main = Main()
    main.start()
