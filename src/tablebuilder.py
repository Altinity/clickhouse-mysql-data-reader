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

    def __init__(self, host=None, port=None, user=None, password=None, dbs=None, tables=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbs = dbs
        self.tables = tables

    def templates(self):
        """
        Create templates for specified MySQL tables. In case no tables specified all tables from specified db are templated

        :param host: string MySQL host
        :param user: string MySQL user
        :param password: string MySQL password
        :param dbs: list of string MySQL datatabse/ May be omitted, in this case tables has to contain full table names, Ex.: db.table1
        :param tables: list of string list of table names. May be short (in case db specified) or full (in the form db.table, in case no db specified)
        :return: dict of CREATE TABLE () templates
        """
        res = {}

        db = None

        try:
            db = self.dbs[0]
        except:
            pass

        # sanity check
        if db is None and self.tables is None:
            return res

        # MySQL connections
        self.connection = MySQLdb.connect(
            host=self.host,
            user=self.user,
            passwd=self.password,
            db=db,
        )
        self.cursor = self.connection.cursor()

        # in case to tables specified - list all tables of the DB specified
        if db is not None and self.tables is None:
            self.cursor.execute("USE " + db)
            self.tables = []
            self.cursor.execute("SHOW TABLES")  # execute 'SHOW TABLES' (but data is not returned)
            for (table_name,) in self.cursor:
                self.tables.append(table_name)

        # create dict of table templates
        for table in self.tables:
            if not db in res:
                res[db] = {}
            res[db][table] = self.create_table_template(table, db)

        # {
        #   'db': {
        #       'table1': 'CREATE TABLE(...)...',
        #       'table2': 'CREATE TABLE(...)...',
        #   }
        # }
        return res

    def create_table_template(self, table_name, db=None):
        """
        Produce template for CH's
        CREATE TABLE(
            ...
            columns specification
            ...
        ) ENGINE = MergeTree(_SPECIFY_DateField_HERE, (SPECIFY_INDEX_FIELD1, SPECIFY_INDEX_FIELD2, ...etc...), 8192)
        for specified MySQL's table
        :param table_name: string - name of the table in MySQL which will be used as a base for CH's CREATE TABLE template
        :param db: string - name of the DB in MySQL
        :return: string - almost-ready-to-use CREATE TABLE statement
        """

        # `db`.`table` or just `table`
        name = '`{0}`.`{1}`'.format(db, table_name) if db else '`{0}`'.format(table_name)

        # list of ready-to-sql CH columns
        ch_columns = []

        # issue 'DESCRIBE table' statement
        self.cursor.execute("DESC {0}".format(name))
        for (_field, _type, _null, _key, _default, _extra,) in self.cursor:
            # Field | Type | Null | Key | Default | Extra

            # build ready-to-sql column specification Ex.:
            # `integer_1` Nullable(Int32)
            # `u_integer_1` Nullable(UInt32)
            ch_columns.append('`{0}` {1}'.format(_field, self.map_type(mysql_type=_type, nullable=_null, )))

        sql = """
CREATE TABLE {0} (
    {1}
) ENGINE = MergeTree(_SPECIFY_DateField_HERE, (SPECIFY_INDEX_FIELD1, SPECIFY_INDEX_FIELD2, ...etc...), 8192)
""".format(
            name,
            ",\n    ".join(ch_columns)
        )
        return sql

    def map_type(self, mysql_type, nullable=False):
        """
        Map MySQL type (as a string from DESC table statement) to CH type (as string)
        :param mysql_type: string MySQL type (from DESC statement). Ex.: 'INT(10) UNSIGNED', 'BOOLEAN'
        :param nullable: bool|string True|'yes' is this field nullable
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
        elif mysql_type.startswith('BOOL') or mysql_type.startswith('BOOLEAN'):
            ch_type = 'UInt8'
        elif mysql_type.startswith('SMALLINT'):
            ch_type = 'UInt16' if mysql_type.endswith('UNSIGNED') else 'Int16'
        elif mysql_type.startswith('MEDIUMINT'):
            ch_type = 'UInt32' if mysql_type.endswith('UNSIGNED') else 'Int32'
        elif mysql_type.startswith('INT') or mysql_type.startswith('INTEGER'):
            ch_type = 'UInt32' if mysql_type.endswith('UNSIGNED') else 'Int32'
        elif mysql_type.startswith('BIGINT'):
            ch_type = 'UInt64' if mysql_type.endswith('UNSIGNED') else 'Int64'
        elif mysql_type.startswith('SERIAL'):
            ch_type = 'UInt64'
        elif mysql_type.startswith('DEC') or mysql_type.startswith('DECIMAL') or mysql_type.startswith('FIXED') or mysql_type.startswith('NUMERIC'):
            ch_type = 'String'
        elif mysql_type.startswith('FLOAT'):
            ch_type = 'Float32'
        elif mysql_type.startswith('DOUBLE') or mysql_type.startswith('REAL'):
            ch_type = 'Float64'

        # Date and Time Types
        elif mysql_type.startswith('DATE'):
            ch_type = 'Date'
        elif mysql_type.startswith('DATETIME'):
            ch_type = 'DateTime'
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
        if isinstance(nullable, bool):
            # for bool - simple statement
            if nullable:
                ch_type = 'Nullable(' + ch_type + ')'
        elif isinstance(nullable, str):
            # also accept case-insencitive string 'yes'
            if nullable.upper() == "YES":
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
