#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .writer import Writer
import multiprocessing as mp
import logging


class ProcessWriter(Writer):

    args = None

    def __init__(self, **kwargs):
        next_writer_builder = kwargs.pop('next_writer_builder', None)
        converter_builder = kwargs.pop('converter_builder', None)
        super().__init__(next_writer_builder=next_writer_builder, converter_builder=converter_builder)
        for arg in kwargs:
            self.next_writer_builder.param(arg, kwargs[arg])

    def opened(self):
        pass

    def open(self):
        pass

    def process(self, event_or_events=None):
        writer = self.next_writer_builder.get()
        writer.insert(event_or_events)
        writer.close()
        writer.push()
        writer.destroy()

    def insert(self, event_or_events=None):
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]
        logging.debug('class:%s insert', __class__)
        process = mp.Process(target=self.process, args=(event_or_events,))

        logging.debug('class:%s insert - starting process', __class__)
        process.start()

        #process.join()
        logging.debug('class:%s insert - done process', __class__)
        pass

    def flush(self):
        pass

    def push(self):
        pass

    def destroy(self):
        pass

    def close(self):
        pass
