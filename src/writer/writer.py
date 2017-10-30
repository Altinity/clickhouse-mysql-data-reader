#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Writer(object):

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

    def close(self):
        pass
