#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import MySQLdb
from MySQLdb.cursors import SSDictCursor
from .tableprocessor import TableProcessor


class TableMigrator(TableProcessor):

    cursorclass = SSDictCursor
    chwriter = None

    def migrate(self):
        dbs = self.dbs_tables_lists()

        if dbs is None:
            return None

        for db in dbs:
            for table in dbs[db]:
                self.migrate_table(db=db, table=table)

    def migrate_table(self, db=None, table=None, ):
        self.connect(db=db)
        self.cursor.execute("SELECT * FROM {0}".format(self.create_full_table_name(db=db, table=table)))
        cnt = 0;
        while True:
            rows = self.cursor.fetchmany(10000)
            if not rows:
                break
            self.chwriter.dst_schema = db
            self.chwriter.dst_table = table
            self.chwriter.insert(rows)
            cnt += len(rows)

        return cnt


if __name__ == '__main__':
    tb = TableBuilder(
        host='127.0.0.1',
        user='reader',
        password='qwerty',
        dbs=['db'],
        #        tables='datatypes, enum_datatypes, json_datatypes',
        tables=['datatypes', 'enum_datatypes', 'json_datatypes'],
    )
    templates = tb.templates()
    for db in templates:
        for table in templates[db]:
            print(table, '=', templates[db][table])
