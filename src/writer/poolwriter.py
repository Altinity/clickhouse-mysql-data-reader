#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .writer import Writer
from ..event.event import Event

pool = {}


class PoolWriter(Writer):

    writer = None
    max_pool_size = None

    def __init__(self, writer, max_pool_size=500000):
        self.writer = writer
        self.max_pool_size = max_pool_size

    def insert(self, event):
        key = str(event.schema) + '_' + str(event.table)
        if key not in pool:
            pool[key] = []
        pool[key].append(event)
        print('total:', len(pool), 'len(pool[', key, '])=', len(pool[key]))
        if len(pool[key]) > self.max_pool_size:
            self.writer.batch(pool[key])
            pool.pop(key)

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
