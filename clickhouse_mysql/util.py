#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
import pprint
import sys
import importlib.util


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

    @staticmethod
    def class_from_file(file_name, class_name):
        logging.info("sys.path")
        logging.info(pprint.pformat(sys.path))
        spec = importlib.util.spec_from_file_location("file_module", file_name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _class = getattr(module, class_name)
        return _class
