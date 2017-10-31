#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Event(object):

    # db name
    schema = None

    # table name
    table = None

    # {'id':1, 'col1':1}
    row = None

    file = None

    # ['id', 'col1', 'col2']
    fieldnames = None