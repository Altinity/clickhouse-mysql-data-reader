#!/usr/bin/env python
# -*- coding: utf-8 -*-


class ObjectBuilder(object):

    class_name = None
    constructor_params = None
    instance = None

    def __init__(self, class_name=None, constructor_params=None, instance=None):
        self.class_name = class_name
        self.constructor_params = constructor_params
        self.instance = instance

    def param(self, name, value):
        if not self.constructor_params:
            self.constructor_params = {}
        self.constructor_params[name] = value

    def get(self):
        if not self.class_name:
            # no class name - return instance, it may be None
            return self.instance

        # have class name

        if self.constructor_params:
            return self.class_name(**self.constructor_params)
        else:
            return self.class_name()
