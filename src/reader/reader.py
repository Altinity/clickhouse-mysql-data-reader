#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Reader(object):

    converter = None

    callbacks = {
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

    def subscribe(self, callbacks):
        for callback_name in callbacks:
            if callback_name in self.callbacks:
                self.callbacks[callback_name].append(callbacks[callback_name])

    def fire(self, event_name, **attrs):
        for callback in self.callbacks[event_name]:
            callback(**attrs)

    def read(self):
        pass
