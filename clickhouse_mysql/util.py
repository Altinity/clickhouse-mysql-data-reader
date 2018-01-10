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
        log_row += "log row start\n"
        if isinstance(row, dict):
            for column, value in row.items():
                log_row += "row: {}={}\n".format(column, value)
        else:
            for value in row:
                log_row += "row: {}\n".format(value)
        log_row += "log row end"
        logging.info(log_row)
