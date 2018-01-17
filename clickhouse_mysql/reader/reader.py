#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clickhouse_mysql.observable import Observable


class Reader(Observable):
    """Read data from source and notify observers"""

    converter = None

    event_handlers = {
        # called on each WriteRowsEvent
        'WriteRowsEvent': [],

        # called on each row inside WriteRowsEvent (thus can be called multiple times per WriteRowsEvent)
        'WriteRowsEvent.EachRow': [],

        # called when Reader has no data to read
        'ReaderIdleEvent': [],
    }

    def __init__(self, converter=None, callbacks={}):
        self.converter = converter
        self.subscribe(callbacks)

    def read(self):
        pass
