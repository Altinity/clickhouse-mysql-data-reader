#!/usr/bin/env python
# -*- coding: utf-8 -*-


from clickhouse_mysql.reader.reader import Reader
from clickhouse_mysql.writer.writer import Writer
import signal


class Pumper(object):
    """
    Pump data - read data from reader and push into writer
    """

    reader: Reader = None
    writer: Writer = None

    def __init__(self, reader=None, writer=None):
        self.reader = reader
        self.writer = writer

        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        if self.reader:
            # subscribe on reader's event notifications
            self.reader.subscribe({
                'WriteRowsEvent': self.write_rows_event,
                'UpdateRowsEvent': self.update_rows_event,
                'DeleteRowsEvent': self.delete_rows_event,
                # 'WriteRowsEvent.EachRow': self.write_rows_event_each_row,
                'ReaderIdleEvent': self.reader_idle_event,
            })

    def run(self):
        self.reader.read()

    def write_rows_event(self, event=None):
        """
        WriteRowsEvent handler
        :param event:
        """
        self.writer.insert(event)

    def write_rows_event_each_row(self, event=None):
        """
        WriteRowsEvent.EachRow handler
        :param event:
        """
        self.writer.insert(event)

    def reader_idle_event(self):
        """
        ReaderIdleEvent handler
        """
        self.writer.flush()

    def delete_rows_event(self, event=None):
        """
        DeleteRowsEvent handler
        :param event:
        """
        self.writer.delete_row(event)

    def update_rows_event(self, event=None):
        """
        UpdateRowsEvent handler
        :param event:
        """
        self.writer.update(event)
    
    def exit_gracefully(self):
        self.reader.close()
        self.writer.close()


if __name__ == '__main__':
    print("pumper")
