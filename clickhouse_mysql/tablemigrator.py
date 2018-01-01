#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import MySQLdb

from MySQLdb.cursors import SSDictCursor
from .tableprocessor import TableProcessor
from .event.event import Event


class TableMigrator(TableProcessor):

    cursorclass = SSDictCursor
    chwriter = None
    pool_max_rows_num = 100000

    def migrate(self):
        """
        High-level migration function. Loops over tables and migrate each of them
        :return:
        """
        dbs = self.dbs_tables_lists()

        if dbs is None:
            logging.info("Nothing to migrate")
            return None

        logging.info("List for migration:")
        for db in dbs:
            for table in dbs[db]:
                logging.info("  {}.{}".format(db, table))

        for db in dbs:
            for table in dbs[db]:
                logging.info("Start migration {}.{}".format(db, table))
                self.migrate_table(db=db, table=table)

    def migrate_table(self, db=None, table=None, ):
        """
        Migrate one table
        :param db: db
        :param table: table
        :return: number of migrated rows
        """
        self.connect(db=db)
        sql = "SELECT * FROM {0}".format(self.create_full_table_name(db=db, table=table))
        try:
            self.cursor.execute(sql)
            cnt = 0;
            while True:
                rows = self.cursor.fetchmany(self.pool_max_rows_num)
                if not rows:
                    break
                self.chwriter.dst_schema = db
                self.chwriter.dst_table = table
                event = Event()
                event.rows = rows
                self.chwriter.insert(event)
                cnt += len(rows)
        except:
            raise Exception("Can not migrate table on host={} user={} password={} db={} table={} cnt={}".format(
                self.host,
                self.user,
                self.password,
                db,
                table,
                cnt
            ))

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
