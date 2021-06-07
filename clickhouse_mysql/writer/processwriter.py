#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing as mp
import logging

from clickhouse_mysql.writer.writer import Writer


class ProcessWriter(Writer):
    """Start write procedure as a separated process"""
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
        """Separate process body to be run"""

        logging.debug('class:%s process()', __class__)
        writer = self.next_writer_builder.get()
        writer.insert(event_or_events)
        writer.close()
        writer.push()
        writer.destroy()
        logging.debug('class:%s process() done', __class__)

    def processDelete(self, event_or_events=None):
        """Separate process body to be run"""

        logging.debug('class:%s process()', __class__)
        writer = self.next_writer_builder.get()
        writer.deleteRow(event_or_events)
        writer.close()
        writer.push()
        writer.destroy()
        logging.debug('class:%s process() done', __class__)

    def processUpdate(self, event_or_events=None):
        """Separate process body to be run"""

        logging.debug('class:%s process()', __class__)
        writer = self.next_writer_builder.get()
        writer.delete(event_or_events)
        writer.close()
        writer.push()
        writer.destroy()
        logging.debug('class:%s process() done', __class__)

    def insert(self, event_or_events=None):
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]

        # start separated process with event_or_events to be inserted

        logging.debug('class:%s insert', __class__)
        process = mp.Process(target=self.process, args=(event_or_events,))

        logging.debug('class:%s insert.process.start()', __class__)
        process.start()

        #process.join()
        logging.debug('class:%s insert done', __class__)
        pass

    def delete(self, event_or_events=None):
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]

        # start separated process with event_or_events to be inserted

        logging.debug('class:%s delete', __class__)
        process = mp.Process(target=self.processDelete, args=(event_or_events,))

        logging.debug('class:%s delete.process.start()', __class__)
        process.start()

        #process.join()
        logging.debug('class:%s delete done', __class__)
        pass

    def update(self, event_or_events=None):
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]

        # start separated process with event_or_events to be inserted

        logging.debug('class:%s update', __class__)
        process = mp.Process(target=self.processUpdate, args=(event_or_events,))

        logging.debug('class:%s update.process.start()', __class__)
        process.start()

        #process.join()
        logging.debug('class:%s update done', __class__)
        pass

    def flush(self):
        pass

    def push(self):
        pass

    def destroy(self):
        pass

    def close(self):
        pass
