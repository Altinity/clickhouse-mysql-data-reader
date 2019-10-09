#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import MySQLdb
from clickhouse_mysql.dbclient.mysqlclient import MySQLClient


class TableProcessor(object):

    client = None

    dbs = None
    tables = None
    tables_prefixes = None

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
            dst_schema=None,
            dst_table=None,
            dst_table_prefix=None,
            distribute=None,
            cluster=None,
            tables=None,
            tables_prefixes=None,
            column_skip=[],
    ):
        """
        :param host: string MySQL host
        :param port: int MySQL port
        :param user: string MySQL user
        :param password: string MySQL password
        :param dbs: list of string MySQL databases. May be omitted, in this case tables has to contain full table names, Ex.: db.table1
        :param tables: list of string list of table names. Table names may be short or full form
        :param tables_prefixes: list of string list of table prefixes. May be short or full form
        """
        self.dbs = [] if dbs is None else dbs
        self.tables = [] if tables is None else tables
        self.tables_prefixes = [] if tables_prefixes is None else tables_prefixes
        self.client = MySQLClient({
            'host': host,
            'port': port,
            'user': user,
            'password': password,
        })
        self.dst_schema = dst_schema
        self.dst_table = dst_table
        self.dst_table_prefix = dst_table_prefix
        self.cluster = cluster
        self.distribute = distribute
        self.column_skip = column_skip

    def dbs_tables_lists(self):
        """
        Prepare dict of databases and with list of tables for each db
        Include all tables into db tables list in case to tables are explicitly specified
        It still can be no tables - in case db really has no tables
        For convenient iteration over all tables

        :return:
        {
            'db1' : ('table1', 'table2', ...),
            'db2' : (all tables listed in 'db2'),
            'db3' : ('table1', ...),
        }
        """

        # process explicitly specified tables
        # prepare list of tables for each db
        res = TableProcessor.group_tables(self.dbs, self.tables)
        if res is None:
            logging.warning("Can't group tables for explicitly specified db/tables")
            return None
        else:
            logging.debug("{} group tables for explicitly specified db/tables".format(res))

        # process implicitly specified tables - when db name only specified and we add all tables from this db
        # for dbs with no tables list specified - meaning all tables - list tables directly from DB
        for db in res:
            if not res[db]:
                # no tables in db, try to add all tables from DB
                tables = self.tables_list(db)
                res[db].add(tables)
                logging.debug("add {} tables to {} db".format(tables, db))

        # process tables specified with prefix
        # prepare list of prefixes
        prefixes = TableProcessor.group_tables(tables=self.tables_prefixes)
        logging.debug("{} group tables for prefix specified db/tables".format(prefixes))
        for db, prefixes in prefixes.items():
            for prefix in prefixes:
                # match all tables for specified prefix
                tables_match = self.tables_match(db, prefix)
                if tables_match:
                    logging.debug("{} tables match prefix {}.{}".format(tables_match, db, prefix))
                    # we have tables which match specified prefix
                    for table in tables_match:
                        # ensure {'db': set()}
                        if db not in res:
                            res[db] = set()
                        # add table to the set of tables
                        res[db].add(table)
                else:
                    logging.debug("No tables match prefix {}.{}".format(db, prefix))
        # dict of sets
        return res

    def tables_list(self, db):
        """
        List tables in specified DB

        :param db: database to list tables in
        :return: ['table1', 'table2', ...]
        """
        return self.client.tables_list(db)

    def tables_match(self, db, prefix):
        """
        List tables which match specified prefix

        :param db: database to match tables in
        :param prefix: prefix to match tables
        :return: ['table1', 'table2', ...]
        """
        res = []
        # list all tables in db
        tables = self.tables_list(db)
        logging.debug("{} tables {}".format(db, tables))
        for table in tables:
            logging.debug("check {}.{} match prefix {}".format(db, table, prefix))
            if table.startswith(prefix):
                res.append(table)
                logging.debug("{}.{} match prefix {}".format(db, table, prefix))
        return res

    @staticmethod
    def create_full_table_name(dst_schema=None, dst_table=None, dst_table_prefix=None, db=None, table=None, distribute=None):
        """
        Create fully-specified table name as `schema_all`.`db__table_all`  or `schema`.`db__table` or just `db`.`table`

        :param dst_schema:
        :param db:
        :param table:
        :param distribute:
        :return: `schema_all`.`db__table_all`  or `schema`.`db__table` or just `db`.`table`
        """

        # target table can be renamed with dst_table
        table = dst_table if dst_table is not None else table

        # simple case - do not move table into another db
        if dst_schema is None:
            return '`{0}`.`{1}`'.format(db, table) if db else '`{0}`'.format(table)

        if distribute:
            dst_schema += "_all"
            table += "_all"

        return \
            '`{0}`.`{1}`'.format(dst_schema, TableProcessor.create_migrated_table_name(prefix=dst_table_prefix, table=table)) \
            if db else \
            '`{0}`'.format(table)

    @staticmethod
    def create_migrated_table_name(prefix=None, table=None):
        prefix = prefix if prefix is not None else ""
        return prefix + table

    @staticmethod
    def create_distributed_table_name(db=None, table=None):
        return db + "__" + table + "_all"

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
        :param unsettled_tables_action what to do with a table in case can't decide which db it belongs to.
            See ACTION_* values for full list of possible actions
            ACTION_FAIL - return None
            ACTION_IGNORE_TABLE - ignore table
            ACTION_INCLUDE_TABLE - add table to special db called '_' (just in order to include it somewhere)
        :return:
        {
            'db1' : ('table1', 'table2', ...),
            'db2' : (),
            'db3' : ('table1', ...),
            '_' : ('tableX', 'tableY', ...),
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
                    # include table
                    # use fake '_' db for this
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
        """
        Extract db/schema names from list of dbs and tables - which can contain full names, as db.table - expanding list
        provided as dbs[]
        :param dbs: list of dbs
        :param tables: list of tables with (otional) full names
        :return: set of db names
        """
        dbs_group = TableProcessor.group_tables(dbs=dbs,
                                                tables=tables,
                                                unsettled_tables_action=TableProcessor.ACTION_IGNORE_TABLE)

        return dbs_group.keys()

    @staticmethod
    def extract_tables(tables=[]):
        """
        Extract short tabke names from list of (possibly) full names
        :param tables: list of (possibly) full names
        :return: set of short names
        """
        dbs_group = TableProcessor.group_tables(tables=tables,
                                                unsettled_tables_action=TableProcessor.ACTION_INCLUDE_TABLE)
        res = set()
        for db in dbs_group:
            res.update(dbs_group[db])

        return res
