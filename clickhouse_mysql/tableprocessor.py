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
        For convenient iteration over all tables

        :return:
        {
            'db1' : ['table1', 'table2', 'table3'],
            'db2' : ['table1', 'table2', 'table3'],
        }
        """

        if len(self.dbs) == 0:
            # no dbs specified
            # means need to have at least 1 full table specified as `db`.`table`

            if len(self.tables) == 0:
                # nothing specified - neither db nor table
                return None

            # have something specified in tables list

            # verify that all tables have full name specified as `db`.`table`
            for table in self.tables:
                db, table = self.parse_full_table_name(table)
                if db is None:
                    # short table name found - not enough - need full specification
                    return None

            # all tables are specified in full format as `db`.`table`

            # build result dict
            dbs = {}
            for table in self.tables:
                db, table = self.parse_full_table_name(table)
                if db not in dbs:
                    dbs[db] = set()
                dbs[db].add(table)

            return dbs

        elif len(self.dbs) == 1:
            # exactly one db specified

            if len(self.tables) == 0:
                # in case none table specified - means 'all tables from this DB'
                # return list of tables for this db
                return {
                    self.dbs[0]: self.tables_list(self.dbs[0])
                }

            # multiple tables specified

            # ensure all tables have short name specification
            # meaning they all belong to this one specified table
            for table in self.tables:
                db, table = self.parse_full_table_name(table)
                if db is not None:
                    # long table name found
                    return None

            return {
                self.dbs[0]: self.tables
            }

        else:
            # multiple dbs specified

            # verify that no tables specified
            if len(self.tables) > 0:
                return None

            # build result dict
            dbs = {}
            for db in self.dbs:
                dbs[db] = self.tables_list(db)

            return dbs

        return None

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

    def create_full_table_name(self, db=None, table=None):
        """
        Create fully-specified table name as `db`.`table` or just `table`
        :param db:
        :param table:
        :return: `db`.`table` or just `table`
        """
        return '`{0}`.`{1}`'.format(db, table) if db else '`{0}`'.format(table)

    def parse_full_table_name(self, full_name):
        """
        Extract db and table names from fully-specified table name.
        Ex.: extract 'db', 'name' out of `db`.`name`
        :param full_name: `db`.`name`
        :return: (db, name, )
        """
        db, dot, name = full_name.partition('.')
        if not dot:
            name = db
            db = None

        return None if db is None else db.strip('`'), name.strip('`')
