#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging


class Util(object):

    @staticmethod
    def join_lists(*args):
        res = []
        for l in args:
            if isinstance(l, list):
                res += l

        return res

    @staticmethod
    def log_row(row, header="log row"):
        log_row = header + "\n";
        if isinstance(row, dict):
            for column, value in row.items():
                log_row += "column: {}={}\n".format(column, value)
        else:
            for value in row:
                log_row += "value: {}\n".format(value)
        logging.info(log_row)
