#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import logging

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent

from .reader import Reader
from ..event.event import Event
#from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent


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

    def performance_report(self, start, rows_num, rows_num_per_event_min=None, rows_num_per_event_max=None, now=None):
        # time to calc stat

        if now is None:
            now = time.time()

        window_size = now - start
        if window_size > 0:
            rows_per_sec = rows_num / window_size
            logging.info(
                'PERF - rows_per_sec:%f rows_per_event_min: %d rows_per_event_max: %d for last %d rows %f sec',
                rows_per_sec,
                rows_num_per_event_min if rows_num_per_event_min is not None else -1,
                rows_num_per_event_max if rows_num_per_event_max is not None else -1,
                rows_num,
                window_size,
            )
        else:
            logging.info("PERF - rows window size=0 can not calc performance for this window")


    def read(self):
        start_timestamp = int(time.time())
        # fetch events
        try:
            while True:
                logging.debug('Check events in binlog stream')

                start = time.time()
                rows_num = 0
                rows_num_since_interim_performance_report = 0
                rows_num_per_event = 0
                rows_num_per_event_min = None
                rows_num_per_event_max = None


                # fetch available events from MySQL
                for mysql_event in self.binlog_stream:
                    if isinstance(mysql_event, WriteRowsEvent):

                        rows_num_per_event = len(mysql_event.rows)
                        if (rows_num_per_event_min is None) or (rows_num_per_event < rows_num_per_event_min):
                            rows_num_per_event_min = rows_num_per_event
                        if (rows_num_per_event_max is None) or (rows_num_per_event > rows_num_per_event_max):
                            rows_num_per_event_max = rows_num_per_event

                        if self.subscribers('WriteRowsEvent'):
                            self.write_rows_event_num += 1
                            logging.debug('WriteRowsEvent #%d rows: %d', self.write_rows_event_num, len(mysql_event.rows))
                            rows_num += len(mysql_event.rows)
                            rows_num_since_interim_performance_report += len(mysql_event.rows)
                            event = Event()
                            event.schema = mysql_event.schema
                            event.table = mysql_event.table
                            event.mysql_event = mysql_event
                            self.notify('WriteRowsEvent', event=event)

                        if self.subscribers('WriteRowsEvent.EachRow'):
                            self.write_rows_event_each_row_num += 1
                            logging.debug('WriteRowsEvent.EachRow #%d', self.write_rows_event_each_row_num)
                            for row in mysql_event.rows:
                                rows_num += 1
                                rows_num_since_interim_performance_report += 1
                                event = Event()
                                event.schema = mysql_event.schema
                                event.table = mysql_event.table
                                event.row = row['values']
                                self.notify('WriteRowsEvent.EachRow', event=event)

                        if rows_num_since_interim_performance_report >= 100000:
                            # speed report each N rows
                            self.performance_report(
                                start=start,
                                rows_num=rows_num,
                                rows_num_per_event_min=rows_num_per_event_min,
                                rows_num_per_event_max=rows_num_per_event_max,
                            )
                            rows_num_since_interim_performance_report = 0
                            rows_num_per_event_min = None
                            rows_num_per_event_max = None
                    else:
                        # skip non-insert events
                        pass

                # all events fetched (or none of them available)

                if rows_num > 0:
                    # we have some rows processed
                    now = time.time()
                    if now > start + 60:
                        # and processing was long enough
                        self.performance_report(start, rows_num, now)

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
