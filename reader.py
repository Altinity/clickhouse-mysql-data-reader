#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
#from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent


class Reader(object):

    connection_settings = None
    server_id = None
    log_file = None
    log_pos = None
    only_schemas = None
    only_tables = None
    callbacks = {
        # called on each WriteRowsEvent
        'WriteRowsEvent': [],

        # called on each row inside WriteRowsEvent (thus can be called multiple times per WriteRowsEvent)
        'WriteRowsEvent.EachRow': [],
    }

    def __init__(
            self,
            connection_settings,
            server_id,
            log_file=None,
            log_pos=None,
            only_schemas=None,
            only_tables=None,
            callbacks={},
    ):
        self.connection_settings = connection_settings
        self.server_id = server_id
        self.log_file = log_file,
        self.log_pos = log_pos
        self.only_schemas = only_schemas
        self.only_tables = only_tables
        self.subscribe(callbacks)

    def subscribe(self, callbacks):
        for callback_name in callbacks:
            if callback_name in self.callbacks:
                self.callbacks[callback_name].append(callbacks[callback_name])

    def fire(self, event, **attrs):
        for callback in self.callbacks[event]:
            callback(**attrs)

    def read(self):
        binlog_stream = BinLogStreamReader(
            # MySQL server - data source
            connection_settings=self.connection_settings,
            server_id=self.server_id,
            # we are interested in reading CH-repeatable events only
            only_events=[
                # INSERT's are supported
                WriteRowsEvent,
            #    UpdateRowsEvent,
            #    DeleteRowsEvent
            ],
            only_schemas=self.only_schemas,
            only_tables=self.only_tables,
            log_file=self.log_file,
            log_pos=self.log_pos,
            freeze_schema=True, # If true do not support ALTER TABLE. It's faster.
            blocking=True,
#            resume_stream=True,
        )

        # fetch events
        try:
            for event in binlog_stream:
                if isinstance(event, WriteRowsEvent):
                    self.fire('WriteRowsEvent', binlog_event=event)
                    for row in event.rows:
                        self.fire('WriteRowsEvent.EachRow', binlog_event=event, row=row)
                else:
                    # skip non-insert events
                    pass
        except KeyboardInterrupt:
            pass


        binlog_stream.close()

if __name__ == '__main__':
    connection_settings = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'reader',
        'passwd': 'qwerty',
    }
    server_id = 1

    reader = Reader(
        connection_settings=connection_settings,
        server_id=server_id,
    )

    reader.read()
