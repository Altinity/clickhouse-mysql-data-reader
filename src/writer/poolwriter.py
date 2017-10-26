#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .writer import Writer
from ..event.event import Event


# events pool
pool = {}


class PoolWriter(Writer):

    writer_class = None
    writer_params = None
    max_pool_size = None
    async_batch = False

    def __init__(
            self,
            writer_class=None,
            writer_params={},
            max_pool_size=500000,
            async_batch=False
    ):
        self.writer_class = writer_class
        self.writer_params = writer_params
        self.max_pool_size = max_pool_size
        self.async_batch = async_batch

    def insert(self, event):
        # build pool key
        key = str(event.schema) + '_' + str(event.table)

        # register key in pool
        if key not in pool:
            pool[key] = []

        # pool event
        pool[key].append(event)
        print('total:', len(pool), 'len(pool[', key, '])=', len(pool[key]))

        # may be it's time to flush pool
        if len(pool[key]) > self.max_pool_size:
            # time to flush data for specified key
            if self.async_batch:
                pass
            else:
                writer = self.writer_class(**self.writer_params)
                writer.batch(pool[key])
                del writer
            # data for specified key flushed, delete key from pool of data
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
