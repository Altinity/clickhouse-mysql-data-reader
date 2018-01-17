#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import MySQLdb

from MySQLdb.cursors import SSDictCursor
from clickhouse_mysql.tableprocessor import TableProcessor
from clickhouse_mysql.event.event import Event


class TableMigrator(TableProcessor):

    cursorclass = SSDictCursor
    chwriter = None
    pool_max_rows_num = 100000
    wheres = {}

    def __init__(
            self,
            host=None,
            port=None,
            user=None,
            password=None,
            dbs=None,
            tables=None,
            tables_prefixes=None,
            tables_where_clauses=None,
    ):
        super().__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            dbs=dbs,
            tables=tables,
            tables_prefixes=tables_prefixes,
        )

        if not tables_where_clauses:
            return

        # [ 'db1.t1=f1, 'db2.t2=f2']
        logging.info("tables_where_clauses={}".format(tables_where_clauses))
        for table_where in tables_where_clauses:
            logging.info("table_where={}".format(table_where))

        for table_where_clause in tables_where_clauses:
            full_table_name, equals, where_file_name = table_where_clause.partition('=')
            if not full_table_name or not equals or not where_file_name:
                continue
            if not TableProcessor.is_full_table_name(full_table_name):
                continue

            db, name = TableProcessor.parse_full_table_name(full_table_name)
            # {
            #   'db1': {
            #       'table1': "a = 1 and b = 2"
            #       'table2': "c = 1 and d = 2"
            #   },
            #   'db2': {
            #       'table1': "e = 2 and f = 3"
            #       'table2': "g = 1 and h = 2"
            #   }
            # }
            if not db in self.wheres:
                self.wheres[db] = {}
            self.wheres[db][name] = open(where_file_name,'r').read().strip("\n")

        logging.info("migration where clauses")
        for db, tables in self.wheres.items():
            for name, where in tables.items():
                logging.info("{}.{}.where={}".format(db, name, where))

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
        if db in self.wheres and table in self.wheres[db]:
            sql += " WHERE {}".format(self.wheres[db][table])
        try:
            logging.info("migrate_table. sql={}".format(sql))
            self.cursor.execute(sql)
            cnt = 0;
            while True:
                rows = self.cursor.fetchmany(self.pool_max_rows_num)
                if not rows:
                    break
                event = Event()
                event.schema = db
                event.table = table
                event.rows = rows
                self.chwriter.insert(event)
                self.chwriter.flush()
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
