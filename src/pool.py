#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Pool(object):

    # events pool
    pool = {
#       'key.1': [[event,], [event, event, event], [event, event, event]]
#       'key.2': [[event,], [event, event, event], [event, event, event]]
    }
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
        key = str(event.schema) + '.' + str(event.table)

        # register key in pool
        if key not in self.pool:
            self.pool[key] = [[]]

        # append to current pool key list
        self.pool[key][0].append(event)

        if len(self.pool[key][0]) >= self.max_pool_size:
            # shift pool key list
            self.pool[key].insert(0, [])

        while len(self.pool[key]) > 1:
            buckets = len(self.pool[key])
            last_bucket_size = len(self.pool[key][len(self.pool[key])-1])
            print('flushing key', key, 'backets', buckets, 'last backet size', last_bucket_size, 'keys:', len(self.pool))

            # time to flush data for specified key
            writer = self.writer_class(**self.writer_params)
            writer.batch(self.pool[key].pop())
            del writer
