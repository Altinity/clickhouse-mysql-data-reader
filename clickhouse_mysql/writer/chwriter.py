#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys

from decimal import Decimal

from clickhouse_mysql.dbclient.chclient import CHClient

from clickhouse_mysql.writer.writer import Writer
from clickhouse_mysql.tableprocessor import TableProcessor
import datetime


class CHWriter(Writer):
    """ClickHouse writer"""

    client = None
    dst_schema = None
    dst_table = None
    dst_distribute = None

    def __init__(
            self,
            connection_settings,
            dst_schema=None,
            dst_table=None,
            dst_table_prefix=None,
            dst_distribute=False,
            next_writer_builder=None,
            converter_builder=None,
    ):
        if dst_distribute and dst_schema is not None:
            dst_schema += "_all"
        if dst_distribute and dst_table is not None:
            dst_table += "_all"
        logging.info("CHWriter() connection_settings={} dst_schema={} dst_table={} dst_distribute={}".format(
            connection_settings, dst_schema, dst_table, dst_distribute))
        self.client = CHClient(connection_settings)
        self.dst_schema = dst_schema
        self.dst_table = dst_table
        self.dst_table_prefix = dst_table_prefix
        self.dst_distribute = dst_distribute

    def insert(self, event_or_events=None):
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]

        events = self.listify(event_or_events)
        if len(events) < 1:
            logging.warning('No events to insert. class: %s', __class__)
            return

        # assume we have at least one Event

        logging.debug('class:%s insert %d event(s)', __class__, len(events))

        # verify and converts events and consolidate converted rows from all events into one batch

        rows = []
        event_converted = None
        for event in events:
            if not event.verify:
                logging.warning(
                    'Event verification failed. Skip one event. Event: %s Class: %s', event.meta(), __class__)
                continue  # for event

            event_converted = self.convert(event)
            for row in event_converted:
                # These columns are added to identify the last change (tb_upd) and the kind of operation performed
                # 0 - INSERT, 1 - UPDATE, 2 - DELETE
                row['tb_upd'] = datetime.datetime.now()
                row['operation'] = 0

                for key in row.keys():
                    # we need to convert Decimal or timedelta value to str value for suitable for table structure
                    if type(row[key]) == [Decimal, datetime.timedelta]:
                        row[key] = str(row[key])

                # These columns are added to identify the last change (tb_upd) and when a row is deleted (1)
                # row['tb_upd'] = datetime.datetime.now()
                # row['operation'] = 0
                rows.append(row)

        logging.debug('class:%s insert %d row(s)', __class__, len(rows))

        # determine target schema.table

        schema = self.dst_schema if self.dst_schema else event_converted.schema
        table = None
        if self.dst_distribute:
            table = TableProcessor.create_distributed_table_name(
                db=event_converted.schema, table=event_converted.table)
        else:
            table = self.dst_table if self.dst_table else event_converted.table
            if self.dst_schema:
                table = TableProcessor.create_migrated_table_name(
                    prefix=self.dst_table_prefix, table=table)

        logging.debug("schema={} table={} self.dst_schema={} self.dst_table={}".format(
            schema, table, self.dst_schema, self.dst_table))

        # and INSERT converted rows

        sql = ''
        try:
            sql = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES'.format(
                schema,
                table,
                ', '.join(map(lambda column: '`%s`' % column, rows[0].keys()))
            )
            logging.debug(f"CHWRITER QUERY INSERT: {sql}")
            self.client.execute(sql, rows)
        except Exception as ex:
            logging.critical('QUERY FAILED')
            logging.critical('ex={}'.format(ex))
            logging.critical('sql={}'.format(sql))
            logging.critical('data={}'.format(rows))
            # sys.exit(0)

        # all DONE

    def delete_row(self, event_or_events):
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]

        logging.debug("Delete CHWriter")

        events = self.listify(event_or_events)
        if len(events) < 1:
            logging.warning('No events to insert. class: %s', __class__)
            return

        # assume we have at least one Event

        logging.debug('class:%s delete %d event(s)', __class__, len(events))

        # verify and converts events and consolidate converted rows from all events into one batch

        rows = []
        event_converted = None
        for event in events:
            if not event.verify:
                logging.warning('Event verification failed. Skip one event. Event: %s Class: %s', event.meta(),
                                __class__)
                continue  # for event

            event_converted = self.convert(event)
            for row in event_converted:
                # These columns are added to identify the last change (tb_upd) and the kind of operation performed
                # 0 - INSERT, 1 - UPDATE, 2 - DELETE
                row['tb_upd'] = datetime.datetime.now()
                row['operation'] = 2

                for key in row.keys():
                    # we need to convert Decimal or timedelta value to str value for suitable for table structure
                    if type(row[key]) in [Decimal, datetime.timedelta]:
                        row[key] = str(row[key])

                # These columns are added to identify the last change (tb_upd) and when a row is deleted (1)
                # row['tb_upd'] = datetime.datetime.now()
                # row['operation'] = 2
                rows.append(row)

        logging.debug('class:%s delete %d row(s)', __class__, len(rows))

        # determine target schema.table

        schema = self.dst_schema if self.dst_schema else event_converted.schema
        table = None
        if self.dst_distribute:
            table = TableProcessor.create_distributed_table_name(
                db=event_converted.schema, table=event_converted.table)
        else:
            table = self.dst_table if self.dst_table else event_converted.table
            if self.dst_schema:
                table = TableProcessor.create_migrated_table_name(
                    prefix=self.dst_table_prefix, table=table)

        logging.debug("schema={} table={} self.dst_schema={} self.dst_table={}".format(schema, table, self.dst_schema,
                                                                                       self.dst_table))

        # and DELETE converted rows

        # These columns are added to identify the last change (tb_upd) and the kind of operation performed
        # 0 - INSERT, 1 - UPDATE, 2 - DELETE
        rows[0]['tb_upd'] = datetime.datetime.now()
        rows[0]['operation'] = 2

        sql = ''
        try:
            sql = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES'.format(
                schema,
                table,
                ', '.join(map(lambda column: '`%s`' % column, rows[0].keys()))
            )
            logging.debug(f"CHWRITER QUERY DELETE: {sql}")
            self.client.execute(sql, rows)

        # sql = ''
        # try:
        #     sql = 'ALTER TABLE `{0}`.`{1}` DELETE WHERE {2}'.format(
        #         schema,
        #         table,
        #         ' and '.join(filter(None, map(
        #             lambda column, value: "" if column != pk else self.get_data_format(column, value),
        #             row.keys(), row.values())))
        #     )
        #
        #     self.client.execute(sql)

        except Exception as ex:
            logging.critical('QUERY FAILED')
            logging.critical('ex={}'.format(ex))
            logging.critical('sql={}'.format(sql))
            # sys.exit(0)

        # all DONE

    """
        Get string format pattern for update and delete operations
    """

    def get_data_format(self, column, value):
        t = type(value)
        if t == str:
            return "`%s`='%s'" % (column, value.replace("'", "\\'"))
        elif t is datetime.datetime:
            return "`%s`='%s'" % (column, value)
        else:
            # int, float
            return "`%s`=%s" % (column, value)

    def update(self, event_or_events):
        # event_or_events = [
        #   event: {
        #       row: {
        #           'before_values': {'id': 3, 'a': 3},
        #           'after_values': {'id': 3, 'a': 2}
        #       }
        #   },
        #   event: {
        #       row: {
        #          'before_values': {'id': 2, 'a': 3},
        #          'after_values': {'id': 2, 'a': 2}
        #       }
        #   },
        # ]

        logging.debug("Update CHWriter")

        events = self.listify(event_or_events)
        if len(events) < 1:
            logging.warning('No events to update. class: %s', __class__)
            return

        # assume we have at least one Event

        logging.debug('class:%s update %d event(s)', __class__, len(events))

        # verify and converts events and consolidate converted rows from all events into one batch

        rows = []
        event_converted = None
        for event in events:
            if not event.verify:
                logging.warning('Event verification failed. Skip one event. Event: %s Class: %s', event.meta(),
                                __class__)
                continue  # for event

            event_converted = self.convert(event)
            for row in event_converted.pymysqlreplication_event.rows:

                for key in row['after_values'].keys():
                    # we need to convert Decimal or timedelta value to str value for suitable for table structure
                    if type(row['after_values'][key]) in [Decimal, datetime.timedelta]:
                        row['after_values'][key] = str(
                            row['after_values'][key])

                # These columns are added to identify the last change (tb_upd) and when a row is deleted (1)
                row['after_values']['tb_upd'] = datetime.datetime.now()
                row['after_values']['operation'] = 1
                rows.append(row['after_values'])

        logging.debug('class:%s update %d row(s)', __class__, len(rows))

        # determine target schema.table

        schema = self.dst_schema if self.dst_schema else event_converted.schema
        table = None
        if self.dst_distribute:
            table = TableProcessor.create_distributed_table_name(
                db=event_converted.schema, table=event_converted.table)
        else:
            table = self.dst_table if self.dst_table else event_converted.table
            if self.dst_schema:
                table = TableProcessor.create_migrated_table_name(
                    prefix=self.dst_table_prefix, table=table)

        logging.debug("schema={} table={} self.dst_schema={} self.dst_table={}".format(schema, table, self.dst_schema,
                                                                                       self.dst_table))

        # and UPDATE converted rows

        # These columns are added to identify the last change (tb_upd) and when a row is deleted (1)
        rows[0]['tb_upd'] = datetime.datetime.now()
        rows[0]['operation'] = 1

        sql = ''
        try:
            sql = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES'.format(
                schema,
                table,
                ', '.join(map(lambda column: '`%s`' % column, rows[0].keys()))
            )
            logging.debug(f"CHWRITER QUERY UPDATE: {sql}")
            self.client.execute(sql, rows)
        except Exception as ex:
            logging.critical('QUERY FAILED')
            logging.critical('ex={}'.format(ex))
            logging.critical('sql={}'.format(sql))
            logging.critical('data={}'.format(rows))
            # sys.exit(0)

        # all DONE


if __name__ == '__main__':
    connection_settings = {
        'host': '192.168.74.230',
        'port': 9000,
        'user': 'default',
        'passwd': '',
    }

    writer = CHWriter(connection_settings=connection_settings)
    writer.insert()
