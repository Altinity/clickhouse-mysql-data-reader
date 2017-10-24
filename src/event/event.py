#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Event(object):

    # db name
    schema = None

    # table name
    table = None

    # {'id':1, 'col1':1}
    row = None

    # [{'id':1, 'col1':1}, {'id':2, 'col1':2}]
    rows = None
