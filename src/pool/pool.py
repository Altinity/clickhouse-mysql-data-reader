#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Pool(object):

    writer_class = None
    writer_params = None

    key_builder_class = None
    key_builder = None

    max_bucket_size = None
    max_belt_size = None
    max_interval_between_rotations = None

    def __init__(
            self,
            writer_class=None,
            writer_params={},
            key_builder_class=None,
            max_bucket_size=10000,
            max_belt_size=1,
            max_interval_between_rotations=60,
    ):
        self.writer_class = writer_class
        self.writer_params = writer_params

        self.key_builder_class = key_builder_class
        self.key_builder = self.key_builder_class()

        self.max_bucket_size = max_bucket_size
        self.max_belt_size = max_belt_size
        self.max_interval_between_rotations = max_interval_between_rotations

    def insert(self, item):
        pass

    def flush(self, key=None):
        pass
