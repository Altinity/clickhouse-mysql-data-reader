#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time


class Pool(object):

    # events pool
    pool = {
#       'key.1': [[event,], [event, event, event], [event, event, event]]
#       'key.2': [[event,], [event, event, event], [event, event, event]]
    }

    flushed_at = {
#        'key.1': UNIX TIMESTAMP
#        'key.2': UNIX TIMESTAMP
    }

    max_pool_size = None
    max_flush_interval = None
    writer_class = None
    writer_params = None

    def __init__(
            self,
            writer_class=None,
            writer_params={},
            max_pool_size=10000,
            max_flush_interval=60,
    ):
        self.writer_class = writer_class
        self.writer_params = writer_params
        self.max_pool_size = max_pool_size
        self.max_flush_interval = max_flush_interval

    def insert(self, event):
        # build pool key
        key = str(event.schema) + '.' + str(event.table)

        # register key in pool
        if key not in self.pool:
            self.pool[key] = [[]]
            self.flushed_at[key] = int(time.time())

        # append to current pool key list
        self.pool[key][0].append(event)

        if len(self.pool[key][0]) >= self.max_pool_size:
            self.flush_key(key)

    def flush(self, key=None):
        empty_keys = []
        if key is None:
            for k in self.pool:
                if self.flush_key(k):
                    empty_keys.append(k)
        else:
            if self.flush_key(key):
                empty_keys.append(key)

        for k in empty_keys:
            self.pool.pop(k)
            self.flushed_at.pop(k)

    def flush_key(self, key):

        need_flush = False
        now = int(time.time())
        flush_by = "U"

        if len(self.pool[key][0]) >= self.max_pool_size:
            # events number reached
            need_flush = True
            flush_by = "SIZE"
        elif now >= self.flushed_at[key] + self.max_flush_interval:
            # time interval reached
            need_flush = True
            flush_by = "TIME"

        if need_flush:
            # shift pool key list
            self.pool[key].insert(0, [])
            self.flushed_at[key] = int(time.time())

        while len(self.pool[key]) > 1:
            buckets = len(self.pool[key])
            last_bucket_size = len(self.pool[key][len(self.pool[key])-1])
            print(now, 'flushing key', key, 'flush by', flush_by, 'backets', buckets, 'last backet size', last_bucket_size, 'keys:', len(self.pool))

            # time to flush data for specified key
            writer = self.writer_class(**self.writer_params)
            writer.batch(self.pool[key].pop())
            del writer

        if len(self.pool[key][0]) < 1:
            # no events for key
            return True
        else:
            return False
