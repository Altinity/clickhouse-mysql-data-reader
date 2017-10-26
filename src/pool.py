#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Pool(object):

    # events pool
    pool = {}
    max_pool_size = None
    writer_class = None
    writer_params = None

    def __init__(
            self,
            writer_class=None,
            writer_params={},
            max_pool_size=500000,
    ):
        self.writer_class = writer_class
        self.writer_params = writer_params
        self.max_pool_size = max_pool_size
        self.max_pool_size = max_pool_size

    def insert(self, event):
        # build pool key
        key = str(event.schema) + '_' + str(event.table)

        # register key in pool
        if key not in self.pool:
            self.pool[key] = []

        # pool event
        self.pool[key].append(event)

        # may be it's time to flush pool
        if len(self.pool[key]) > self.max_pool_size:
            print('flushing key', key, 'len(pool[', key, '])=', len(self.pool[key]), 'keys:', len(self.pool))

            # time to flush data for specified key
            writer = self.writer_class(**self.writer_params)
            writer.batch(self.pool[key])
            del writer

            # data for specified key flushed, delete key from pool of data
            self.delete(key)

    def delete(self, key):
        try:
            self.pool.pop(key)
        except:
            pass
