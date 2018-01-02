#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
import logging

from .writer import Writer


class CHCSVWriter(Writer):
    """Write into ClickHouse via CSV file and clickhouse-client tool"""

    host = None
    port = None
    user = None
    password = None

    def __init__(self, host=None, port=None, user=None, password=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def insert(self, event_or_events=None):
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]

        events = self.listify(event_or_events)
        if len(events) < 1:
            logging.warning('No events to insert. class: %s', __class__)
            return

        # assume we have at least one Event

        logging.debug('class:%s insert %d rows', __class__, len(events))

        for event in events:
            sql = 'INSERT INTO `{0}`.`{1}` ({2}) FORMAT CSV'.format(
                event.schema,
                event.table,
                ', '.join(map(lambda column: '`%s`' % column, event.fieldnames)),
            )

            choptions = ""
            if self.host:
                choptions += " --host=" + self.host
            if self.port:
                choptions += " --port=" + str(self.port)
            if self.user:
                choptions += " --user=" + self.user
            if self.password:
                choptions += " --password=" + self.password
            bash = "tail -n +2 '{0}' | clickhouse-client {1} --query='{2}'".format(
                event.filename,
                choptions,
                sql,
            )

            logging.info('starting clickhouse-client process')
            logging.debug('starting %s', bash)
            os.system(bash)

        pass
