#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import shlex

from clickhouse_mysql.writer.writer import Writer
from clickhouse_mysql.tableprocessor import TableProcessor


class CHCSVWriter(Writer):
    """Write into ClickHouse via CSV file and clickhouse-client tool"""

    dst_schema = None
    dst_table = None
    dst_distribute = None

    host = None
    port = None
    user = None
    password = None

    def __init__(
            self,
            connection_settings,
            dst_schema=None,
            dst_table=None,
            dst_table_prefix=None,
            dst_distribute=False,
    ):
        if dst_distribute and dst_schema is not None:
            dst_schema += "_all"
        if dst_distribute and dst_table is not None:
            dst_table += "_all"
        logging.info("CHCSWriter() connection_settings={} dst_schema={} dst_table={}".format(connection_settings, dst_schema, dst_table))
        self.host = connection_settings['host']
        self.port = connection_settings['port']
        self.user = connection_settings['user']
        self.password = connection_settings['password']
        self.dst_schema = dst_schema
        self.dst_table = dst_table
        self.dst_table_prefix = dst_table_prefix
        self.dst_distribute = dst_distribute

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
            schema = self.dst_schema if self.dst_schema else event.schema
            table = None
            if self.dst_distribute:
                table = TableProcessor.create_distributed_table_name(db=event.schema, table=event.table)
            else:
                table = self.dst_table if self.dst_table else event.table
                if self.dst_schema:
                    table = TableProcessor.create_migrated_table_name(prefix=self.dst_table_prefix, table=table)

            sql = 'INSERT INTO `{0}`.`{1}` ({2}) FORMAT CSV'.format(
                schema,
                table,
                ', '.join(map(lambda column: '`%s`' % column, event.fieldnames)),
            )

            choptions = ""
            if self.host:
                choptions += " --host=" + shlex.quote(self.host)
            if self.port:
                choptions += " --port=" + str(self.port)
            if self.user:
                choptions += " --user=" + shlex.quote(self.user)
            if self.password:
                choptions += " --password=" + shlex.quote(self.password)
            bash = "tail -n +2 '{0}' | clickhouse-client {1} --query='{2}'".format(
                event.filename,
                choptions,
                sql,
            )

            logging.info('starting clickhouse-client process')
            logging.debug('starting %s', bash)
            os.system(bash)

        pass
