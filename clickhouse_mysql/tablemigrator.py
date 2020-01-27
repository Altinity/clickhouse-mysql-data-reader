#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os.path

from MySQLdb.cursors import SSDictCursor,Cursor
from clickhouse_mysql.tableprocessor import TableProcessor
from clickhouse_mysql.tablesqlbuilder import TableSQLBuilder
from clickhouse_mysql.event.event import Event


class TableMigrator(TableSQLBuilder):
    """
    Migrate data from MySQL to ClickHouse
    """

    chwriter = None
    chclient = None
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
    where_clauses = {}

    def __init__(
            self,
            host=None,
            port=None,
            user=None,
            password=None,
            dbs=None,
            dst_schema=None,
            dst_table=None,
            dst_table_prefix=None,
            distribute=None,
            cluster=None,
            tables=None,
            tables_prefixes=None,
            tables_where_clauses=None,
            column_skip=[],
    ):
        super().__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            dbs=dbs,
            dst_schema=dst_schema,
            dst_table=dst_table,
            dst_table_prefix=dst_table_prefix,
            distribute=distribute,
            cluster=cluster,
            tables=tables,
            tables_prefixes=tables_prefixes,
            column_skip=column_skip
        )
        self.client.cursorclass = SSDictCursor

        # parse tables where clauses
        if not tables_where_clauses:
            return

        # tables_where_clauses contains:
        # [
        #   'db1.t1=where_filename_1',
        #   'db2.t2=where_filename_2'
        # ]

        # debug info
        logging.info("tables_where_clauses={}".format(tables_where_clauses))
        for table_where in tables_where_clauses:
            logging.info("table_where={}".format(table_where))

        # process WHERE migration clauses
        for table_where_clause in tables_where_clauses:
            # table_where_clause contains 'db1.t1=where_filename_1'
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
            if not db in self.where_clauses:
                self.where_clauses[db] = {}
                
            if os.path.isfile(where_file_name):
                self.where_clauses[db][table] = open(where_file_name, 'r').read().strip("\n")
            else:
                self.where_clauses[db][table] = where_file_name

        # debug info
        logging.info("migration where clauses")
        for db, tables in self.where_clauses.items():
            for table, where in tables.items():
                logging.info("{}.{}.where={}".format(db, table, where))

    def migrate_all_tables(self, with_create_database):
        """
        High-level migration function. Loops over tables and migrate each of them
        :return:
        """

        # what tables are we going to migrate
        dbs = self.dbs_tables_lists()

        # sanity check
        if dbs is None:
            logging.info("Nothing to migrate")
            return None

        # debug info
        logging.info("List for migration:")
        for db in dbs:
            for table in dbs[db]:
                logging.info("  {}.{}".format(db, table))

        # migration templates
        templates = self.templates()

        # migrate table-by-table
        for db in dbs:
            for table in dbs[db]:
                logging.info("Start migration {}.{}".format(db, table))
                if with_create_database:
                    print("Running with chclient {};".format(templates[db][table]['create_database']))
                    self.chclient.execute(templates[db][table]['create_database'])
                print("Running with chclient {};".format(templates[db][table]['create_table']))
                self.chclient.execute(templates[db][table]['create_table'])

    def migrate_all_tables_data(self):
        """
        High-level migration function. Loops over tables and migrate each of them
        :return:
        """

        # what tables are we going to migrate
        dbs = self.dbs_tables_lists()

        # sanity check
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
                self.migrate_one_table_data(db=db, table=table)

    def migrate_one_table_data(self, db=None, table=None):
        """
        Migrate one table
        :param db: db
        :param table: table
        :return: number of migrated rows
        """


        # build SQL statement
        full_table_name = self.create_full_table_name(db=db, table=table)
        sql = "SELECT {0} FROM {1}".format(",".join(self.get_columns(db, full_table_name)), full_table_name)
        # in case we have WHERE clause for this db.table - add it to SQL
        if db in self.where_clauses and table in self.where_clauses[db]:
            sql += " WHERE {}".format(self.where_clauses[db][table])

        try:
            logging.info("migrate_table. sql={}".format(sql))
            self.client.cursorclass = SSDictCursor
            self.client.connect(db=db)
            self.client.cursor.execute(sql)
            cnt = 0;
            while True:
                # fetch multiple rows from MySQL
                rows = self.client.cursor.fetchmany(self.pool_max_rows_num)
                if not rows:
                    break

                # insert Event with multiple rows into ClickHouse writer
                event = Event()
                event.schema = db
                event.table = table
                event.rows = rows
                self.chwriter.insert(event)
                self.chwriter.flush()

                cnt += len(rows)
        except Exception as ex:
            logging.critical("Critical error: {}".format(str(ex)))
            raise Exception("Can not migrate table on db={} table={}".format(
                db,
                table,
            ))

        return cnt

    def get_columns(self,db,full_table_name):
        self.client.cursorclass = Cursor
        self.client.connect(db=db)
        self.client.cursor.execute("DESC {}".format(full_table_name))
        fields = []
        for (_field, _type, _null, _key, _default, _extra,) in self.client.cursor:
            if self.column_skip.__contains__(_field):
                logging.debug("skip column %s",_field)
                continue
            fields.append('`{}`'.format(_field))

        return fields

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
