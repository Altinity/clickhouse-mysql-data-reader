#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .writer import Writer
from ..event.event import Event
from ..pool import Pool


class PoolWriter(Writer):

    writer_class = None
    writer_params = None
    max_pool_size = None
    pool = None

    def __init__(
            self,
            writer_class=None,
            writer_params={},
            max_pool_size=500000,
    ):
        self.writer_class = writer_class
        self.writer_params = writer_params
        self.max_pool_size = max_pool_size

        self.pool = Pool(self.writer_class, self.writer_params, self.max_pool_size)

    def insert(self, event):
        self.pool.insert(event)

if __name__ == '__main__':
    path = 'file.csv'

    writer = CSVWriter(path)
    writer.open()
    event = Event()
    event.row_converted={
        'a': 123,
        'b': 456,
        'c': 'qwe',
        'd': 'rty',
    }
    writer.insert(event)
    event.row_converted={
        'a': 789,
        'b': 987,
        'c': 'asd',
        'd': 'fgh',
    }
    writer.insert(event)
    writer.close()
