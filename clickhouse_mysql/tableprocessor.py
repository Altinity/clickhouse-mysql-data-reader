#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import MySQLdb
from MySQLdb.cursors import Cursor


class TableProcessor(object):

    connection = None
    cursor = None
    cursorclass = Cursor

    host = None
    port = None
    user = None
    password = None
    dbs = None
    tables = None

    ACTION_FAIL = 1
    ACTION_IGNORE_TABLE = 2
    ACTION_INCLUDE_TABLE = 3

    def __init__(
            self,
            host=None,
            port=None,
            user=None,
            password=None,
            dbs=None,
            tables=None
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbs = [] if dbs is None else dbs
        self.tables = [] if tables is None else tables

    def connect(self, db):
        if self.cursor:
            try:
                self.cursor.close()
                del self.cursor
            except:
                pass

        if self.connection:
            del self.connection

        self.connection = MySQLdb.connect(
            host=self.host,
            user=self.user,
            passwd=self.password,
            db=db,
            cursorclass=self.cursorclass,
        )
        self.cursor = self.connection.cursor()

    def dbs_tables_lists(self):
        """
        Prepare dict of databases and with list of tables for each db
        Include all tables into db tables list in case to tables are explicitly specified
        It still can be no tables - incase db really has no tables
        For convenient iteration over all tables

        :return:
        {
            'db1' : ('table1', 'table2', ...),
            'db2' : (),
            'db3' : ('table1', ),
        }
        """

        # prepare initial list of tables for each db
        res = TableProcessor.group_tables(self.dbs, self.tables)
        if res is None:
            # can't group tables
            return None

        # for dbs with no tables list specified - meaning all tables - list tables directly from DB
        for db in res:
            if not res[db]:
                # no tables in db, try to add all tables from DB
                res[db].add(self.tables_list(db))

        return res

    def tables_list(self, db):
        """
        List tables in specified DB

        :param db: database to list tables in
        :return: ['table1', 'table2', ...]
        """
        self.connect(db=db)
        self.cursor.execute("USE " + db)
        self.cursor.execute("SHOW TABLES")
        tables = []
        for (table_name,) in self.cursor:
            tables.append(table_name)

        return tables

    @staticmethod
    def create_full_table_name(db=None, table=None):
        """
        Create fully-specified table name as `db`.`table` or just `table`

        :param db:
        :param table:
        :return: `db`.`table` or just `table`
        """
        return '`{0}`.`{1}`'.format(db, table) if db else '`{0}`'.format(table)

    @staticmethod
    def is_full_table_name(full_name):
        """
        Checks whether it is a fully-specified table name.

        :param full_name: `db`.`name` or `name` or something else
        :return: bool
        """
        db, dot, name = full_name.partition('.')
        if not dot:
            # dot not found - treat this as a short name
            return False
        else:
            # dot found - treat it as a long name
            return True

    @staticmethod
    def parse_full_table_name(full_name):
        """
        Extract db and table names from fully-specified table name.
        Ex.: extract 'db', 'name' out of `db`.`name`

        :param full_name: `db`.`name`
        :return: (db, name, ) or None in case of short name
        """
        db, dot, name = full_name.partition('.')
        if not dot:
            name = db
            db = None

        return None if db is None else db.strip('`'), name.strip('`')

    @staticmethod
    def group_tables(dbs=[], tables=[], unsettled_tables_action=ACTION_FAIL):
        """
        Prepare dict of databases with list of tables for each db
        For convenient iteration over all tables
        :param dbs [ db1, db2, ... ]
        :param tables [ db1.table1, table2, ... ]
        :param unsettled_tables_action ignore table in case can't decide which db it belongs to.
            In case True specified function proceeds
            In case False specified function returns None meaning "can't group tables"
        :return:
        {
            'db1' : ('table1', 'table2', ...),
            'db2' : (),
            'db3' : ('table1', ...),
        }
        OR
        None
        """
        if dbs is None:
            dbs = []
        if tables is None:
            tables = []

        # prepare dict of dbs with empty sets of tables
        # {
        #   'db1': ()
        #   'db2': ()
        # }
        res = {db: set() for db in dbs}

        # set of tables with short names
        short_name_tables = set()

        # set of tables with full names
        full_name_tables = set()
        for table in tables:
            if TableProcessor.is_full_table_name(table):
                full_name_tables.add(table)
            else:
                short_name_tables.add(table)
        # now - what to do with short-name tables
        # if there is only one db name in dbs list we can treat this short tables as belonging to this one table
        # but if there is none OR many dbs listed, where those short-named tables should be included?
        if len(short_name_tables) > 0:
            # we have shot table names
            # where to include them?
            if len(dbs) == 1:
                # ok, let's include these short tables into the single db specified
                res[next(iter(res))].update(short_name_tables)
            else:
                # where to include these short tables?
                # Either none or Multiple databases specified
                if unsettled_tables_action == TableProcessor.ACTION_IGNORE_TABLE:
                    # just ignore this tables
                    pass
                elif unsettled_tables_action == TableProcessor.ACTION_FAIL:
                    # fail process
                    return None
                else:
                    if '_' not in res:
                        res['_'] = set()
                    res['_'].update(short_name_tables)
        else:
            # we do not have short table names - nothing to bother about
            pass

        # now deal with full name tables
        for table in full_name_tables:
            db, table = TableProcessor.parse_full_table_name(table)
            # add table to databases dict
            if db not in res:
                res[db] = set()
            res[db].add(table)

        return res

    @staticmethod
    def extract_dbs(dbs=[], tables=[]):
        dbs_group = TableProcessor.group_tables(dbs=dbs, tables=tables, unsettled_tables_action=TableProcessor.ACTION_IGNORE_TABLE)

        return dbs_group.keys()

    @staticmethod
    def extract_tables(tables=[]):
        dbs_group = TableProcessor.group_tables(tables=tables, unsettled_tables_action=TableProcessor.ACTION_INCLUDE_TABLE)
        res = set()
        for db in dbs_group:
            res.update(dbs_group[db])

        return res
