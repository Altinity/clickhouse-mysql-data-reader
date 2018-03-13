#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys

from clickhouse_driver.client import Client


class CHClient(Client):
    """ClickHouse Client"""

    def __init__(self, connection_settings):
        logging.info("CHClient() connection_settings={}".format(connection_settings))
        self.verify_connection_settings(connection_settings)
        super().__init__(**connection_settings)

    def verify_connection_settings(self, connection_settings):
        if not connection_settings:
            logging.critical("Need CH connection settings")
            sys.exit(0)

        if 'host' not in connection_settings:
            logging.critical("Need CH host in connection settings")
            sys.exit(0)

        if not connection_settings['host']:
            logging.critical("Need CH host in connection settings")
            sys.exit(0)

        if 'port' not in connection_settings:
            logging.critical("Need CH port in connection settings")
            sys.exit(0)

        if not connection_settings['port']:
            logging.critical("Need CH port in connection settings")
            sys.exit(0)

#self.client = CHClient(connection_settings)
#self.client.execute(sql, rows)
