#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import MySQLdb

class TableBuilder(object):

    connection = None
    cursor = None

    def template(self, host, user, password=None, db=None, tables=None):
        # sanity check
        if db is None and tables is None:
            return None

        # MySQL connections
        self.connection = MySQLdb.connect(
            host=host,
            user=user,
            passwd=password,
            db=db,
        )
        self.cursor = self.connection.cursor()

        # in case to tables specified - list all tables of the DB specified
        if db is not None and tables is None:
            self.cursor.execute("USE " + db)
            tables = []
            self.cursor.execute("SHOW TABLES")  # execute 'SHOW TABLES' (but data is not returned)
            for (table_name,) in self.cursor:
                tables.append(table_name)

        # tables can be something like 'db1, db2, db3'
        # make [db1, db2, db3]
        if isinstance(tables, str):
            tables = [table.strip() for table in tables.split(',')]

        for table in tables:
            print(self.table(table, db))

    def table(self, table_name, db=None):

        # `db`.`table` or just `table`
        name = '`{0}`.`{1}`'.format(db, table_name) if db else '`{0}`'.format(table_name)

        # list of ready-to-sql CH columns
        ch_columns = []

        # issue 'DESCRIBE table' statement
        self.cursor.execute("DESC {0}".format(name))
        for (_field, _type, _null, _key, _default, _extra,) in self.cursor:
            # Field | Type | Null | Key | Default | Extra

            # build ready-to-sql column specification
            # `integer_1` Nullable(Int32)
            # `u_integer_1` Nullable(UInt32)
            ch_columns.append('`{0}` {1}'.format(_field, self.map(mysql_type=_type, null=_null,)))

        sql = """
CREATE TABLE {0} (
    {1}
) ENGINE = MergeTree(_SPECIFY_DateField_HERE, (SPECIFY_INDEX_FIELD1, SPECIFY_INDEX_FIELD2, ...etc...), 8192)
""".format(
            name,
            ",\n    ".join(ch_columns)
        )
        return sql

    def map(self, mysql_type, null=False):
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
        if isinstance(null, bool):
            if null:
                ch_type = 'Nullable(' + ch_type + ')'
        elif isinstance(null, str):
            if null.upper() == "YES":
                ch_type = 'Nullable(' + ch_type + ')'

        return ch_type

if __name__ == '__main__':
    tb = TableBuilder()
    tb.template(
        host='127.0.0.1',
        user='reader',
        password='qwerty',
        db='db',
#        tables='datatypes, enum_datatypes, json_datatypes',
        tables=['datatypes', 'enum_datatypes', 'json_datatypes'],
    )
