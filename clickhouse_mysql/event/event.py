#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Event(object):

    # db name
    schema = None

    # table name
    table = None

    #for row in mysql_event.rows:
    #    event.rows.append(row['values'])
    mysql_event = None

    # {'id':1, 'col1':1}
    row = None

    # [{'id': 1, 'col1':1}, {'id': 2, 'col1': 2}, {'id': 3, 'col1': 3}]
    rows = None

    # /path/to/csv/file.csv
    filename = None

    # ['id', 'col1', 'col2']
    fieldnames = None

    _iter = None

    def __iter__(self):
        if self.mysql_event is not None:
            self._iter = iter(self.mysql_event.rows)

        elif self.row is not None:
            self._iter = iter([self.row])

        else:
            self._iter = iter(self.rows)

        return self

    def __next__(self):
        item = next(self._iter)

        if self.mysql_event is not None:
            return item['values']
        else:
            return item

    def column_names(self):
        if self.mysql_event is not None:
            return self.mysql_event.rows[0]['values'].keys()

        if self.row is not None:
            return self.row.keys()

        return self.rows[0].keys()
