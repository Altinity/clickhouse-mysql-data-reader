#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .reader import Reader
from ..event.event import Event
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
#from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
import time
import logging

class MySQLReader(Reader):

    connection_settings = None
    server_id = None
    log_file = None
    log_pos = None
    only_schemas = None
    only_tables = None
    blocking = None
    resume_stream = None
    binlog_stream = None
    nice_pause = 0

    write_rows_event_num = 0
    write_rows_event_each_row_num = 0;

    def __init__(
            self,
            connection_settings,
            server_id,
            log_file=None,
            log_pos=None,
            only_schemas=None,
            only_tables=None,
            blocking=None,
            resume_stream=None,
            nice_pause=None,
            callbacks={},
    ):
        super().__init__(callbacks=callbacks)

        self.connection_settings = connection_settings
        self.server_id = server_id
        self.log_file = log_file,
        self.log_pos = log_pos
        self.only_schemas = only_schemas
        self.only_tables = only_tables
        self.blocking = blocking
        self.resume_stream = resume_stream
        self.nice_pause = nice_pause
        self.binlog_stream = BinLogStreamReader(
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
            blocking=False,
            resume_stream=self.resume_stream,
        )

    def read(self):
        start_timestamp = int(time.time())
        # fetch events
        try:
            prev_stat_time = time.time()
            rows_num = 0

            while True:
                logging.debug('Check events in binlog stream')
                for mysql_event in self.binlog_stream:
                    if isinstance(mysql_event, WriteRowsEvent):
                        if self.subscribers('WriteRowsEvent'):
                            self.write_rows_event_num += 1
                            logging.debug('WriteRowsEvent #%d rows: %d', self.write_rows_event_num, len(mysql_event.rows))
                            rows_num += len(mysql_event.rows)
                            event = Event()
                            event.schema = mysql_event.schema
                            event.table = mysql_event.table
                            event.rows = []
                            for row in mysql_event.rows:
                                event.rows.append(row['values'])
                            self.notify('WriteRowsEvent', event=event)

                        if self.subscribers('WriteRowsEvent.EachRow'):
                            self.write_rows_event_each_row_num += 1
                            logging.debug('WriteRowsEvent.EachRow #%d', self.write_rows_event_each_row_num)
                            for row in mysql_event.rows:
                                rows_num += 1
                                event = Event()
                                event.schema = mysql_event.schema
                                event.table = mysql_event.table
                                event.row = row['values']
                                self.notify('WriteRowsEvent.EachRow', event=event)
                    else:
                        # skip non-insert events
                        pass

                now = time.time()
                if now > prev_stat_time + 60:
                    # time to calc stat
                    window_size = now - prev_stat_time
                    rows_per_sec = rows_num / window_size
                    logging.info(
                        'rows_per_sec:%f for last %f sec',
                        rows_per_sec,
                        window_size
                    )
                    prev_stat_time = now
                    rows_num = 0

                if not self.blocking:
                    break # while True

                # blocking
                if self.nice_pause > 0:
                    time.sleep(self.nice_pause)

                self.notify('ReaderIdleEvent')

        except KeyboardInterrupt:
            pass

        try:
            self.binlog_stream.close()
        except:
            pass
        end_timestamp = int(time.time())

        print('start', start_timestamp)
        print('end', end_timestamp)
        print('len', end_timestamp - start_timestamp)

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
