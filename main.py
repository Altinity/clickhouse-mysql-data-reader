#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.pumper import Pumper
from src.cliopts import CLIOpts
import sys


if sys.version_info[0] < 3:
    raise "Must be using Python 3"


class Main(object):

    config = None

    def __init__(self):
        cliopts = CLIOpts()
        self.config = cliopts.options

    def run(self):
        pumper = Pumper(
            reader_config=self.config['reader-config'],
            writer_config=self.config['writer-config']
        )
        pumper.run()


if __name__ == '__main__':
    main = Main()
    main.run()
