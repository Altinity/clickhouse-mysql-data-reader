#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Util(object):
    @staticmethod
    def join_lists(*args):
        res = []
        for l in args:
            if isinstance(l, list):
                res += l

        return res
