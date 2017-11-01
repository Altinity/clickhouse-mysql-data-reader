#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Writer(object):

    next_writer_builder = None
    converter_builder = None

    def opened(self):
        pass

    def open(self):
        pass

    def insert(self, event_or_events=None):
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]
        pass

    def flush(self):
        pass

    def push(self):
        pass

    def destroy(self):
        pass

    def close(self):
        pass
