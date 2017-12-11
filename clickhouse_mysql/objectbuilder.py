#!/usr/bin/env python
# -*- coding: utf-8 -*-


class ObjectBuilder(object):

    class_name = None
    constructor_params = None
    instance = None

    def __init__(self, class_name=None, constructor_params=None, instance=None):
        """
        Builder/Wrapper for ad object.
        In case instance is provided - operates as a wrapper
        In case class_name and (optional) constructor_params provided - return instance of specified class
        :param class_name: class to instantiate
        :param constructor_params: dict of class's contrcutor params. Used as **constructor_params
        :param instance: ready-to use instance
        """
        self.class_name = class_name
        self.constructor_params = constructor_params
        self.instance = instance

    def param(self, name, value):
        if not self.constructor_params:
            self.constructor_params = {}
        self.constructor_params[name] = value

    def get(self):
        """
        Get object (in case wrapper) or an instance of a class (in case Object builder) - each time the same object
        :return: object
        """
        if not self.class_name:
            # no class name - return instance, it may be None
            return self.instance

        # have class name

        # instantiate object
        if self.constructor_params:
            self.instance =  self.class_name(**self.constructor_params)
        else:
            self.instance = self.class_name()

        # in order to return instance on next get() call
        self.class_name = None

        return self.instance

    def new(self):
        """
        Get object (in case wrapper) or an instance of a class (in case Object builder) - each time new object
        :return: object
        """
        if not self.class_name:
            # no class name - return instance, it may be None
            return self.instance

        # have class name

        # instantiate object
        if self.constructor_params:
            return self.class_name(**self.constructor_params)
        else:
            return self.class_name()
