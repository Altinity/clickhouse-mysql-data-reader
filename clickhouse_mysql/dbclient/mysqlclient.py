#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import MySQLdb
from MySQLdb.cursors import Cursor


class MySQLClient(object):

    connection = None
    cursor = None
    cursorclass = Cursor

    host = None
    port = None
    user = None
    password = None

    def __init__(self, connection_settings):
        """
        :param host: string MySQL host
        :param port: int MySQL port
        :param user: string MySQL user
        :param password: string MySQL password
        """
        self.host = connection_settings['host']
        self.port = connection_settings['port']
        self.user = connection_settings['user']
        self.password = connection_settings['password']

    def disconnect(self):
        """
        Destroy connection objects
        :return:
        """
        if self.cursor:
            try:
                self.cursor.close()
                del self.cursor
            except:
                pass

        if self.connection:
            try:
                del self.connection
            except:
                pass

    def connect(self, db):
        """
        Connect to MySQL
        :param db: string schema/db name
        :return:
        """

        self.disconnect()
        try:
            self.connection = MySQLdb.connect(
                host=self.host,
                port=self.port,                
                user=self.user,
                passwd=self.password,
                db=db,
                cursorclass=self.cursorclass,
                charset='utf8',
                use_unicode=True,
            )
            self.cursor = self.connection.cursor()
            logging.debug("Connect to the database host={} port={} user={} password={} db={}".format(
                self.host,
                self.port,
                self.user,
                self.password,
                db
            ))
        except:
            raise Exception("Can not connect to the database host={} port={} user={} password={} db={}".format(
                self.host,
                self.port,
                self.user,
                self.password,
                db
            ))

    def tables_list(self, db):
        """
        List tables in specified DB

        :param db: database to list tables in
        :return: ['table1', 'table2', ...]
        """
        try:
            self.cursorclass = Cursor
            self.connect(db=db)

            sql = "USE " + db
            logging.debug(sql)
            self.cursor.execute(sql)

            sql = "SHOW TABLES"
            logging.debug(sql)
            self.cursor.execute(sql)

            tables = []
            for row in self.cursor:
                table_name = row[0]
                tables.append(table_name)

        except Exception as err:
            logging.debug("Unexpected error: {}".format(str(err)))
            raise Exception("Can not list tables on host={} port={} user={} password={} db={}".format(
                self.host,
                self.port,
                self.user,
                self.password,
                db
            ))

        return tables
