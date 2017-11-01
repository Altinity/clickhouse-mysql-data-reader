#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.cliopts import CLIOpts
from src.pumper import Pumper
from src.daemon import Daemon

import sys
import multiprocessing as mp


if sys.version_info[0] < 3:
    raise "Must be using Python 3"


class Main(Daemon):

    config = None

    def __init__(self):
        mp.set_start_method('forkserver')
        self.config = CLIOpts.config()
        super().__init__(pidfile=self.config.pid_file())

        print('---')
        print(self.config)
        print('---')

    def run(self):
        pumper = Pumper(
            reader=self.config.reader(),
            writer=self.config.writer(),
        )
        pumper.run()

    def start(self):
        if self.config.is_daemon():
            if not super().start():
                print("Error going background. The process already running?")
        else:
            self.run()


if __name__ == '__main__':
    main = Main()
    main.start()
