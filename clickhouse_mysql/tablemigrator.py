#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import MySQLdb

from MySQLdb.cursors import SSDictCursor
from clickhouse_mysql.tableprocessor import TableProcessor
from clickhouse_mysql.event.event import Event


class TableMigrator(TableProcessor):
    """
    Migrate data from MySQL to ClickHouse
    """

    cursorclass = SSDictCursor
    chwriter = None
    pool_max_rows_num = 100000

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

        # parse tables where clauses
        if not tables_where_clauses:
            return

        # tables_where_clauses
        # [ 'db1.t1=where_filename_1', 'db2.t2=where_filename_2']

        # debug info
        logging.info("tables_where_clauses={}".format(tables_where_clauses))
        for table_where in tables_where_clauses:
            logging.info("table_where={}".format(table_where))

        for table_where_clause in tables_where_clauses:
            # db1.t1=where_filename_1
            full_table_name, equals, where_file_name = table_where_clause.partition('=')

            # sanity check
            if not full_table_name or not equals or not where_file_name:
                continue
            if not TableProcessor.is_full_table_name(full_table_name):
                continue

            # prepare the following data structure:
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
            db, table = TableProcessor.parse_full_table_name(full_table_name)
            if not db in self.wheres:
                self.wheres[db] = {}
            self.wheres[db][table] = open(where_file_name,'r').read().strip("\n")

        # debug info
        logging.info("migration where clauses")
        for db, tables in self.wheres.items():
            for table, where in tables.items():
                logging.info("{}.{}.where={}".format(db, table, where))

    def migrate(self):
        """
        High-level migration function. Loops over tables and migrate each of them
        :return:
        """
        dbs = self.dbs_tables_lists()

        if dbs is None:
            logging.info("Nothing to migrate")
            return None

        # debug info
        logging.info("List for migration:")
        for db in dbs:
            for table in dbs[db]:
                logging.info("  {}.{}".format(db, table))

        # migrate table-by-table
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

        # build SQL statement
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
