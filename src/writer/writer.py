#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Writer(object):

    next_writer_builder = None
    converter_builder = None

    def __init__(
            self,
            next_writer_builder=None,
            converter_builder=None
    ):
        self.next_writer_builder = next_writer_builder
        self.converter_builder = converter_builder

    def opened(self):
        pass

    def open(self):
        pass

    def listify(self, obj_or_list):
        """Ensure list"""

        if obj_or_list is None:
            # no value - return empty list
            return []

        elif isinstance(obj_or_list, list) or isinstance(obj_or_list, tuple) or isinstance(obj_or_list, set):
            if len(obj_or_list) < 1:
                # list/set/tuple is empty - nothing to do
                return []
            else:
                # list/set/tuple is good
                return obj_or_list

        else:
            # event_or_events is an object
            return [obj_or_list]

    def convert(self, data):
        return self.converter_builder.get().convert(data) if self.converter_builder else data

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
