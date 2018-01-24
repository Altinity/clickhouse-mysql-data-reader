#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Event(object):

    # main payload - one or multiple rows

    # native mysql replication event
    # one of from pymysqlreplication.row_event import
    # contains rows internally
    pymysqlreplication_event = None

    # one row payload
    # {'id':1, 'col1':1}
    row = None

    # multi-rows payload
    # [{'id': 1, 'col1':1}, {'id': 2, 'col1': 2}, {'id': 3, 'col1': 3}]
    rows = None

    # additional meta-information
    # source-dependent

    # db name
    schema = None

    # table name
    table = None

    # /path/to/csv/file.csv
    filename = None

    # ['id', 'col1', 'col2']
    fieldnames = None

    # payload rows iterator
    _iter = None

    def __iter__(self):
        # create payload rows iterator

        if self.pymysqlreplication_event is not None:
            # we have native replication event - would iterate over its rows
            self._iter = iter(self.pymysqlreplication_event.rows)

        elif self.row is not None:
            # we have one row - would iterate over list of one row
            self._iter = iter([self.row])

        else:
            # assume multiple rows - would iterate over them
            self._iter = iter(self.rows)

        return self

    def __next__(self):
        # fetch next item from iterator

        item = next(self._iter)

        if self.pymysqlreplication_event is not None:
            # in native replication event actual data are in row['values'] dict item
            return item['values']
        else:
            # local-kept data
            return item

    def convert(self, converter):
        self.row = converter.row(self.row)
        self.rows = converter.rows(self.rows)

    def first_row(self):
        return next(iter(self or []), None)

    def verify(self):
        # verify Event has correct data structure

        if self.pymysqlreplication_event is not None:
            # have native replication event must have some data
            # must have non-empty list of rows with 'value'
            # data item of reasonable len
            if (self.pymysqlreplication_event.rows is not None) \
                    and isinstance(self.pymysqlreplication_event.rows, list) \
                    and (len(self.pymysqlreplication_event.rows) > 0) \
                    and ('values' in self.pymysqlreplication_event.rows[0]) \
                    and (len(self.pymysqlreplication_event.rows[0]['values']) > 0):
                return True
            else:
                return False

        if self.row is not None:
            # one row of data must be of a reasonable len
            if isinstance(self.row, dict) and (len(self.row) > 0):
                return True
            else:
                return False

        if self.rows is not None:
            # rows of data must contain list of dicts
            if isinstance(self.rows, list) \
                    and (len(self.rows) > 0) \
                    and isinstance(self.rows[0], dict) \
                    and (len(self.rows[0]) > 0):
                return True
            else:
                return False

        # no data available?
        return False

    def meta(self):
        # meta info

        meta = ''
        if self.pymysqlreplication_event is not None:
            meta += ' mysql_event set'
            if (self.pymysqlreplication_event.rows is not None):
                meta += ' mysql_event.rows set'
            if isinstance(self.pymysqlreplication_event.rows, list):
                meta += ' mysql_event.rows is a list'
            meta += ' len(mysql_event.rows)=' + len(self.pymysqlreplication_event.rows)
            if (len(self.pymysqlreplication_event.rows) > 0) \
                and ('values' in self.pymysqlreplication_event.rows[0]):
                meta += ' mysql_event.rows[0][values] is set'

                if len(self.pymysqlreplication_event.rows[0]['values']) > 0:
                    meta += ' len(mysql_event.rows[0][values])=' + len(self.pymysqlreplication_event.rows[0]['values'])
        else:
            meta += ' mysql_event not set'

        if self.row is not None:
            meta += ' row set'
            if isinstance(self.row, dict):
                meta += ' is dict len()=' + len(self.row)
            else:
                meta += ' is not a dict'
        else:
            meta += ' row not set'

        if self.rows is not None:
            meta += ' rows set'
            if isinstance(self.rows, list):
                meta += ' rows is a list len(rows)=' + len(self.rows)
                if (len(self.rows) > 0) and isinstance(self.rows[0], dict):
                    meta += ' with dict() len(dict)=' + len(self.rows[0])
            else:
                meta += ' rows is not a list'
        else:
            meta += ' rows not set'

        return meta

    def column_names(self):
        # fetch column names from data
        return self.first_row().keys()
