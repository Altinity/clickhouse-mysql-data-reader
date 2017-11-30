#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import MySQLdb


class TableBuilder(object):

    connection = None
    cursor = None

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

    def dbs_tables_lists(self):
        """
        :return:
        {
            'db1' : ['table1', 'table2', 'table3']
            'db2' : ['table1', 'table2', 'table3']
        }
        """

        if len(self.dbs) == 0:
            # no dbs specified
            # means we have to have
            # at least 1 full table specified

            if len(self.tables) == 0:
                # nothing specified
                return None

            # verify that all tables have full name specified
            for table in self.tables:
                db, table = self.parse_full_table_name(table)
                if db is None:
                    # short table name found
                    return None

            dbs = {}
            for table in self.tables:
                db, table = self.parse_full_table_name(table)
                if db not in dbs:
                    dbs[db] = set()
                dbs[db].add(table)

            return dbs

        elif len(self.dbs) == 1:
            # one db specified

            # verify that none table specified at all
            if len(self.tables) == 0:
                return {
                    self.dbs[0]: self.tables_list(self.dbs[0])
                }

            # OR all tables have short name specification
            # meaning they all belong to this table
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

            dbs = {}
            for db in self.dbs:
                dbs[db] = self.tables_list(db)

            return dbs

        return None

    def tables_list(self, db):
        """
        :param db:
        :return: ['table1', 'table2', etc]
        """
        self.connection = MySQLdb.connect(
            host=self.host,
            user=self.user,
            passwd=self.password,
            db=db,
        )
        self.cursor = self.connection.cursor()
        self.cursor.execute("USE " + db)
        tables = []
        self.cursor.execute("SHOW TABLES")  # execute 'SHOW TABLES' (but data is not returned)
        for (table_name,) in self.cursor:
            tables.append(table_name)

        return tables


    def templates(self, json=False):
        """
        Create templates for specified MySQL tables. In case no tables specified all tables from specified db are templated

        :param host: string MySQL host
        :param user: string MySQL user
        :param password: string MySQL password
        :param dbs: list of string MySQL datatabse/ May be omitted, in this case tables has to contain full table names, Ex.: db.table1
        :param tables: list of string list of table names. May be short (in case db specified) or full (in the form db.table, in case no db specified)
        :return: dict of CREATE TABLE () templates
        """
        dbs = self.dbs_tables_lists()

        if dbs is None:
            return None

        templates = {}
        for db in dbs:
            templates[db] = {}
            for table in dbs[db]:
                templates[db][table] = self.create_table_description(db=db, table=table, json=json)

        return templates

    def create_table_description(self, db=None, table=None, json=False):
        columns_description = self.create_table_columns_description(db=db, table=table)
        sql_template = self.create_table_sql_template(db=db, table=table, columns_descrption=columns_description)
        if json:
            return {
                "template": sql_template,
                "fields": columns_description,
            }
        else:
            return sql_template

    def create_table_sql_template(self, db=None, table=None, columns_descrption=None):
        """
        Produce template for CH's
        CREATE TABLE(
            ...
            columns specification
            ...
        ) ENGINE = MergeTree(_<PRIMARY_DATE_FIELD>, (<COMMA_SEPARATED_INDEX_FIELDS_LIST>), 8192)
        for specified MySQL's table
        :param table: string - name of the table in MySQL which will be used as a base for CH's CREATE TABLE template
        :param db: string - name of the DB in MySQL
        :return: string - almost-ready-to-use CREATE TABLE statement
        """

        ch_columns = []
        for column_description in columns_descrption:
            ch_columns.append('`{0}` {1}'.format(column_description['field'], column_description['clickhouse_type']))

        sql = """CREATE TABLE {0} (
    {1}
) ENGINE = MergeTree(<PRIMARY_DATE_FIELD>, (<COMMA_SEPARATED_INDEX_FIELDS_LIST>), 8192)
""".format(
            self.create_full_table_name(db=db, table=table),
            ",\n    ".join(ch_columns)
        )
        return sql

    def create_table_columns_description(self, db=None, table=None, ):
        # list of table columns specifications
        # [{    'field': 'f1',
        #        'mysql_type': 'int',
        #        'clickhouse_type': 'UInt32'
        #        'nullable': True,
        #        'key': 'PRI',
        #        'default': 'CURRENT TIMESTAMP',
        #        'extra': 'on update CURRENT_TIMESTAMP',
        # }, {}, {}]
        columns_description = []

        # issue 'DESCRIBE table' statement
        self.connection = MySQLdb.connect(
            host=self.host,
            user=self.user,
            passwd=self.password,
            db=db,
        )
        self.cursor = self.connection.cursor()
        self.cursor.execute("DESC {0}".format(self.create_full_table_name(db=db, table=table)))
        for (_field, _type, _null, _key, _default, _extra,) in self.cursor:
            # Field | Type | Null | Key | Default | Extra

            # build ready-to-sql column specification Ex.:
            # `integer_1` Nullable(Int32)
            # `u_integer_1` Nullable(UInt32)
            columns_description.append({
                'field': _field,
                'mysql_type': _type,
                'clickhouse_type': self.map_type(mysql_type=_type, nullable=self.is_field_nullable(_null)),
                'nullable': self.is_field_nullable(_null),
                'key': _key,
                'default': _default,
                'extra': _extra,
            })

        return columns_description

    def create_full_table_name(self, db=None, table=None):
        # `db`.`table` or just `table`
        return '`{0}`.`{1}`'.format(db, table) if db else '`{0}`'.format(table)

    def parse_full_table_name(self, full_name):
        db, dot, name = full_name.partition('.')
        if not dot:
            name = db
            db = None

        return db if db is None else db.strip('`'), name.strip('`')


    def is_field_nullable(self, nullable):
        # Deal with NULLs
        if isinstance(nullable, bool):
            # for bool - simple statement
            return nullable
        elif isinstance(nullable, str):
            # also accept case-insensitive string 'yes'
            return True if nullable.upper() == "YES" else False

    def map_type(self, mysql_type, nullable=False):
        """
        Map MySQL type (as a string from DESC table statement) to CH type (as string)
        :param mysql_type: string MySQL type (from DESC statement). Ex.: 'INT(10) UNSIGNED', 'BOOLEAN'
        :param nullable: bool is this field nullable
        :return: string CH's type specification directly usable in CREATE TABLE statement.  Ex.:
            Nullable(Int32)
            Nullable(UInt32)
        """

        # deal with UPPER CASE strings for simplicity
        mysql_type = mysql_type.upper()

        # Numeric Types
        if mysql_type.startswith('BIT'):
            ch_type = 'String'
        elif mysql_type.startswith('TINYINT'):
            ch_type = 'UInt8' if mysql_type.endswith('UNSIGNED') else 'Int8'
        elif mysql_type.startswith('BOOLEAN') or mysql_type.startswith('BOOL'):
            ch_type = 'UInt8'
        elif mysql_type.startswith('SMALLINT'):
            ch_type = 'UInt16' if mysql_type.endswith('UNSIGNED') else 'Int16'
        elif mysql_type.startswith('MEDIUMINT'):
            ch_type = 'UInt32' if mysql_type.endswith('UNSIGNED') else 'Int32'
        elif mysql_type.startswith('INTEGER') or mysql_type.startswith('INT'):
            ch_type = 'UInt32' if mysql_type.endswith('UNSIGNED') else 'Int32'
        elif mysql_type.startswith('BIGINT'):
            ch_type = 'UInt64' if mysql_type.endswith('UNSIGNED') else 'Int64'
        elif mysql_type.startswith('SERIAL'):
            ch_type = 'UInt64'
        elif mysql_type.startswith('DECIMAL') or mysql_type.startswith('DEC') or mysql_type.startswith('FIXED') or mysql_type.startswith('NUMERIC'):
            ch_type = 'String'
        elif mysql_type.startswith('FLOAT'):
            ch_type = 'Float32'
        elif mysql_type.startswith('DOUBLE') or mysql_type.startswith('REAL'):
            ch_type = 'Float64'

        # Date and Time Types
        elif mysql_type.startswith('DATETIME'):
            ch_type = 'DateTime'
        elif mysql_type.startswith('DATE'):
            ch_type = 'Date'
        elif mysql_type.startswith('TIMESTAMP'):
            ch_type = 'DateTime'
        elif mysql_type.startswith('TIME'):
            ch_type = 'String'
        elif mysql_type.startswith('YEAR'):
            ch_type = 'UInt16'

        # String Types
        elif mysql_type.startswith('CHAR'):
            ch_type = 'String'
        elif mysql_type.startswith('VARCHAR'):
            ch_type = 'String'
        elif mysql_type.startswith('BINARY'):
            ch_type = 'String'
        elif mysql_type.startswith('VARBINARY'):
            ch_type = 'String'
        elif mysql_type.startswith('TINYBLOB'):
            ch_type = 'String'
        elif mysql_type.startswith('TINYTEXT'):
            ch_type = 'String'
        elif mysql_type.startswith('BLOB'):
            ch_type = 'String'
        elif mysql_type.startswith('TEXT'):
            ch_type = 'String'
        elif mysql_type.startswith('MEDIUMBLOB'):
            ch_type = 'String'
        elif mysql_type.startswith('MEDIUMTEXT'):
            ch_type = 'String'
        elif mysql_type.startswith('LONGBLOB'):
            ch_type = 'String'
        elif mysql_type.startswith('LONGTEXT'):
            ch_type = 'String'

        # Set Types
        elif mysql_type.startswith('ENUM'):
            ch_type = 'Enum16'
        elif mysql_type.startswith('SET'):
            ch_type = 'Array(Int8)'

        # Custom Types
        elif mysql_type.startswith('JSON'):
            ch_type = 'String'

        else:
            ch_type = 'UNKNOWN'

        # Deal with NULLs
        if nullable:
            ch_type = 'Nullable(' + ch_type + ')'

        return ch_type

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
