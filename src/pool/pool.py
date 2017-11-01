#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Pool(object):

    writer_builder = None
    key_builder = None

    key_generator = None

    max_bucket_size = None
    max_belt_size = None
    max_interval_between_rotations = None

    def __init__(
            self,
            writer_builder=None,
            key_builder=None,
            max_bucket_size=10000,
            max_belt_size=1,
            max_interval_between_rotations=60,
    ):
        self.writer_builder = writer_builder
        self.key_builder = key_builder
        self.key_generator = self.key_builder.get()

        self.max_bucket_size = max_bucket_size
        self.max_belt_size = max_belt_size
        self.max_interval_between_rotations = max_interval_between_rotations

    def insert(self, item):
        pass

    def flush(self, key=None):
        pass
