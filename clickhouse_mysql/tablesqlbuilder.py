#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clickhouse_mysql.tableprocessor import TableProcessor
from MySQLdb.cursors import Cursor
import logging


class TableSQLBuilder(TableProcessor):
    """
    Build ClickHouse table(s)
    """

    def templates(self):
        """
        Create ClickHouse tables templates for specified MySQL tables.
        In case no tables specified all tables from specified MySQL db are templated
        :return: dict of ClickHouse's CREATE TABLE () templates
        {
            'db1': {
                'table-db1-1': CREATE TABLE table1 statement template,
                'table-db1-2': CREATE TABLE table2 statement template,
            },
            'db2': {
                'table-db2-1': CREATE TABLE table1 statement template,
                'table-db2-2': CREATE TABLE table2 statement template,
            }
        }
        """
        dbs = self.dbs_tables_lists()
        if dbs is None:
            return None

        templates = {}
        for db in dbs:
            templates[db] = {}
            for table in dbs[db]:
                templates[db][table] = self.create_table_description(
                    cluster=self.cluster,
                    dst_schema=self.dst_schema,
                    dst_table=self.dst_table,
                    dst_table_prefix=self.dst_table_prefix,
                    db=db,
                    table=table)

        return templates

    def create_table_description(self, cluster=None, dst_schema=None, dst_table=None, dst_table_prefix=None, db=None, table=None):
        """
        High-level function.
        Produce either text ClickHouse's table SQL CREATE TABLE() template or JSON ClikcHouse's table description
        :param db: string MySQL db name
        :param table: string MySQL table name
        :param json: bool what should return - json description or ClickHouse's SQL template
        :return: dict{"template":SQL, "fields": {}} or string SQL
        """
        columns_description = self.create_table_columns_description(db=db, table=table)
        return {
            "create_table_template": self.create_table_sql_template(cluster=cluster,
                                                                    dst_schema=dst_schema,
                                                                    dst_table=dst_table,
                                                                    dst_table_prefix=dst_table_prefix,
                                                                    db=db,
                                                                    table=table,
                                                                    columns_description=columns_description),
            "create_table": self.create_table_sql(cluster=cluster,
                                                  dst_schema=dst_schema,
                                                  dst_table=dst_table,
                                                  dst_table_prefix=dst_table_prefix,
                                                  db=db,
                                                  table=table,
                                                  columns_description=columns_description),
            "create_database": self.create_database_sql(dst_schema=dst_schema, db=db),
            "fields": columns_description,
        }

    def create_table_sql_template(self, cluster=None, dst_schema=None, dst_table=None, dst_table_prefix=None, db=None, table=None, columns_description=None):
        """
        Produce table template for ClickHouse
        CREATE TABLE schema.table (
            ...
            columns specification
            ...
        ) ENGINE = MergeTree(_<PRIMARY_DATE_FIELD>, (<COMMA_SEPARATED_INDEX_FIELDS_LIST>), 8192)
        for specified MySQL's table

        :param db: string - name of the DB in MySQL
        :param table: string - name of the table in MySQL which will be used as a base for CH's CREATE TABLE template
        :return: string - almost-ready-to-use ClickHouse CREATE TABLE statement
        """

        ch_columns = []
        for column_description in columns_description:
            ch_columns.append('`{}` {}'.format(column_description['field'], column_description['clickhouse_type_nullable']))

        sql = """CREATE TABLE IF NOT EXISTS {} {} (
    {}
) 
ENGINE = MergeTree(<PRIMARY_DATE_FIELD>, (<COMMA_SEPARATED_INDEX_FIELDS_LIST>), 8192)
""".format(
            self.create_full_table_name(dst_schema=dst_schema, dst_table=dst_table, dst_table_prefix=dst_table_prefix, db=db, table=table),
            "on cluster {}".format(cluster) if cluster is not None else "",
            ",\n    ".join(ch_columns),
        )
        return sql

    def create_table_sql(self, cluster=None, dst_schema=None, dst_table=None, dst_table_prefix=None, db=None, table=None, columns_description=None):
        """
        Produce table template for ClickHouse
        CREATE TABLE(
            ...
            columns specification
            ...
        ) ENGINE = MergeTree(PRIMARY DATE FIELD, (COMMA SEPARATED INDEX FIELDS LIST), 8192)
        for specified MySQL's table

        :param db: string - name of the DB in MySQL
        :param table: string - name of the table in MySQL which will be used as a base for CH's CREATE TABLE template
        :return: string - ready-to-use ClickHouse CREATE TABLE statement
        """

        ch_columns = []

        primary_date_field = self.fetch_primary_date_field(columns_description)
        primary_key_fields = self.fetch_primary_key_fields(columns_description)

        # if primary_date_field is None:
        #     # No primary date field found. Make one
        #     primary_date_field = 'primary_date_field'
        #     ch_columns.append('`primary_date_field` Date default today()')

        if primary_key_fields is None:
            # No primary key fields found. Make PK from primary date field
            primary_key_fields = []
            primary_key_fields.append(primary_date_field)

        for column_description in columns_description:
            field = column_description['field']
            # primary date and primary key fields can't be nullable
            ch_type = column_description['clickhouse_type'] if (field == primary_date_field) or (field in primary_key_fields) else column_description['clickhouse_type_nullable']
            ch_columns.append('`{}` {}'.format(field, ch_type))

        sql = """CREATE TABLE IF NOT EXISTS {} {} (
    {}
) 
{}
""".format(
            self.create_full_table_name(dst_schema=dst_schema, dst_table=dst_table, dst_table_prefix=dst_table_prefix, db=db, table=table, distribute=self.distribute),
            "on cluster {}".format(cluster) if not self.distribute and cluster is not None else "",
            ",\n    ".join(ch_columns),
            self.create_table_engine(self.cluster,
                                     self.dst_schema,
                                     self.create_migrated_table_name(prefix=dst_table_prefix, table=dst_table) if dst_table is not None else self.create_migrated_table_name(prefix=dst_table_prefix, table=table),
                                     primary_date_field,
                                     ",".join(primary_key_fields),
                                     self.distribute)
        )
        return sql

    def create_database_sql(self, dst_schema=None, db=None):
        """
        Produce create database statement for ClickHouse
        CREATE DATABASE
        for specified MySQL's db

        :param db: string - name of the DB
        :return: string - ready-to-use ClickHouse CREATE DATABASE statement
        """
        sql = "CREATE DATABASE IF NOT EXISTS `{}`".format(dst_schema if dst_schema is not None else db)
        return sql

    def create_table_columns_description(self, db=None, table=None, ):
        # list of table columns specifications
        # [
        #   {
        #       'field': 'f1',
        #       'mysql_type': 'int',
        #       'clickhouse_type': 'UInt32'
        #       'nullable': True,
        #       'key': 'PRI',
        #       'default': 'CURRENT TIMESTAMP',
        #       'extra': 'on update CURRENT_TIMESTAMP',
        #   },
        #   {...},
        # ]
        columns_description = []

        # issue 'DESCRIBE table' statement
        self.client.cursorclass = Cursor
        self.client.connect(db=db)
        self.client.cursor.execute("DESC {}".format(self.create_full_table_name(db=db, table=table)))
        for (_field, _type, _null, _key, _default, _extra,) in self.client.cursor:
            # Field | Type | Null | Key | Default | Extra

            # build ready-to-sql column specification Ex.:
            # `integer_1` Nullable(Int32)
            # `u_integer_1` Nullable(UInt32)
            if self.column_skip.__contains__(_field):
                logging.debug("table sql builder skip column %s",_field)
                continue
            columns_description.append({
                'field': _field,
                'mysql_type': _type,
                'clickhouse_type': self.map_type(mysql_type=_type),
                'clickhouse_type_nullable': self.map_type_nullable(mysql_type=_type, nullable=self.is_field_nullable(_null)),
                'nullable': self.is_field_nullable(_null),
                'key': _key,
                'default': _default,
                'extra': _extra,
            })

        return columns_description

    def fetch_primary_date_field(self, columns_description):
        """
        Fetch first Date column name
        :param columns_description:
        :return: string|None
        """
        for column_description in columns_description:
            if column_description['clickhouse_type'] == 'Date':
                return column_description['field']
            if column_description['clickhouse_type'] == 'DateTime':
                return column_description['field']

        return None

    def fetch_primary_key_fields(self, columns_description):
        """
        Fetch list of primary keys columns names
        :param columns_description:
        :return: list | None
        """
        primary_key_fields = []
        for column_description in columns_description:
            if self.is_field_primary_key(column_description['key']):
                primary_key_fields.append(column_description['field'])

        return None if not primary_key_fields else primary_key_fields

    def is_field_nullable(self, field):
        """
        Check whether `nullable` field description value can be interpreted as True.
        Understand MySQL's "Yes" for nullable or just bool value
        :param field: bool, string
        :return: bool
        """
        if isinstance(field, bool):
            # for bool - simple statement
            return field
        elif isinstance(field, str):
            # also accept case-insensitive string 'yes'
            return True if field.upper() == "YES" else False

    def is_field_primary_key(self, field):
        """
        Check whether `key` field description value can be interpreted as True
        :param field:
        :return:
        """
        return bool(field)

    def map_type(self, mysql_type):
        """
        Map MySQL type (as a string from DESC table statement) to ClickHouse type (as string)
        :param mysql_type: string MySQL type (from DESC statement). Ex.: 'INT(10) UNSIGNED', 'BOOLEAN'
        :return: string ClickHouse type specification directly usable in CREATE TABLE statement.  Ex.:
            Int32
            UInt32
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

        return ch_type

    def map_type_nullable(self, mysql_type, nullable=False):
        """
        Map MySQL type (as a string from DESC table statement) to ClickHouse type (as string)
        :param mysql_type: string MySQL type (from DESC statement). Ex.: 'INT(10) UNSIGNED', 'BOOLEAN'
        :param nullable: bool is this field nullable
        :return: string ClickHouse type specification directly usable in CREATE TABLE statement.  Ex.:
            Nullable(Int32)
            Nullable(UInt32)
        """
        ch_type = self.map_type(mysql_type)

        # Deal with NULLs
        if nullable:
            ch_type = 'Nullable(' + ch_type + ')'

        return ch_type

    def create_table_engine(self,
                            cluster=None,
                            dst_schema=None,
                            dst_table=None,
                            primary_date_field=None,
                            primary_key_fields=None,
                            distribute=None):
        """
        :param cluster:
        :param dst_schema:
        :param dst_table:
        :param primary_date_field:
        :param primary_key_fields:
        :param distribute:
        :return:
        """
        if distribute:
            return "ENGINE = Distributed({}, '{}', '{}', rand())".format(
                cluster,
                dst_schema,
                dst_table
            )
        else:
            engine = "ENGINE = ReplacingMergeTree() "
            if primary_date_field is not None:
                engine += "PARTITION BY toYYYYMM({}) ".format(primary_date_field)
            if primary_key_fields is not None:
                engine += "ORDER BY ({})".format(primary_key_fields)
            return engine

if __name__ == '__main__':
    tb = TableSQLBuilder(
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
