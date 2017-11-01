#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

from .pool import Pool
from ..objectbuilder import ObjectBuilder


# Buckets Belts' Index Generator
class BBIndexGenerator(object):

    def generate(self, item):
        # build key of the belt on which to place item
        return str(item.schema) + '.' + str(item.table)


# Buckets Belts Pool
class BBPool(Pool):

    # buckets on the belts
    belts = {
#                  pour data into 0-index bucket
#       'key.1': [[item,], [item, item, item,], [item, item, item,]]
#       'key.2': [[item,], [item, item, item,], [item, item, item,]]
    }

    belts_rotated_at = {
#        'key.1': UNIX TIMESTAMP
#        'key.2': UNIX TIMESTAMP
    }

    buckets_count = 0

    def __init__(
            self,
            writer_builder=None,
            key_builder=None,
            max_bucket_size=10000,
            max_belt_size=1,
            max_interval_between_rotations=60,
    ):
        super().__init__(
            writer_builder=writer_builder,
            key_builder=ObjectBuilder(class_name=BBIndexGenerator),
            max_bucket_size=max_bucket_size,
            max_belt_size=max_belt_size,
            max_interval_between_rotations=max_interval_between_rotations,
        )

    def create_belt(self, belt_index):
        # create belt with one empty bucket
        self.belts[belt_index] = [[]]
        self.belts_rotated_at[belt_index] = int(time.time())

    def insert(self, item):
        # which belt we'll insert item?
        belt_index = self.key_generator.generate(item)

        # register belt if not yet
        if belt_index not in self.belts:
            self.create_belt(belt_index)

        # append item to the 0-indexed bucket of the specified belt
        self.belts[belt_index][0].append(item)

        # may be bucket is already full
        if len(self.belts[belt_index][0]) >= self.max_bucket_size:
            # bucket full, rotate the belt
            self.rotate_belt(belt_index)

    def flush(self, key=None):
        belt_index = key
        empty_belts_indexes = []

        if belt_index is None:
            for b_index in self.belts:
                if self.rotate_belt(b_index, flush=True):
                    empty_belts_indexes.append(b_index)
        else:
            if self.rotate_belt(belt_index, flush=True):
                empty_belts_indexes.append(belt_index)

        # delete belt
        for b_index in empty_belts_indexes:
            self.belts.pop(b_index)
            self.belts_rotated_at.pop(b_index)

    def rotate_belt(self, belt_index, flush=False):
        now = int(time.time())
        need_rotation = True if flush else False
        rotate_by = "FLUSH"

        if len(self.belts[belt_index][0]) >= self.max_bucket_size:
            # 0-index bucket is full
            need_rotation = True
            rotate_by = "SIZE"

        elif now >= self.belts_rotated_at[belt_index] + self.max_interval_between_rotations:
            # time interval reached
            need_rotation = True
            rotate_by = "TIME"

        if not need_rotation:
            # belt not rotated
            return False

        # belts needs rotation

        # insert empty bucket into the beginning of the belt
        self.belts[belt_index].insert(0, [])
        self.belts_rotated_at[belt_index] = now

        # in case we flush belt we'll keep one just inserted empty bucket
        buckets_num_left_on_belt = 1 if flush else self.max_belt_size

        while len(self.belts[belt_index]) > buckets_num_left_on_belt:
            # too many buckets on the belt
            # time to rotate belt and flush the most-right-bucket
            self.buckets_count += 1

            buckets_num = len(self.belts[belt_index])
            last_bucket_size = len(self.belts[belt_index][buckets_num-1])
            print(now, self.buckets_count, 'rotating belt', belt_index, 'rotate by', rotate_by, 'buckets_num', buckets_num, 'last bucket size', last_bucket_size, 'belts:', len(self.belts))

            # time to flush data for specified key
            self.writer_builder.param('csv_file_path_suffix_parts', [str(now), str(self.buckets_count)])
            writer = self.writer_builder.get()
            writer.insert(self.belts[belt_index].pop())
            writer.close()
            writer.push()
            writer.destroy()
            del writer

        # belt rotated
        return True
