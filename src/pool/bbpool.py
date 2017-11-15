#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

from .pool import Pool
from ..objectbuilder import ObjectBuilder
import logging


# Buckets Belts' Index Generator
class BBIndexGenerator(object):

    def generate(self, item):
        # build key of the belt on which to place item
        return str(item.schema) + '.' + str(item.table)


# Buckets Belts Pool
class BBPool(Pool):

    # buckets on the belts
    belts = {
        #          pour data into 0-index bucket
        # 'key.1': [[item,], [item, item, item,], [item, item, item,]]
        # 'key.2': [[item,], [item, item, item,], [item, item, item,]]
    }

    belts_rotated_at = {
        # 'key.1': UNIX TIMESTAMP
        # 'key.2': UNIX TIMESTAMP
    }

    buckets_count = 0
    buckets_content_count = 0;

    prev_time = None
    prev_buckets_count = 0
    prev_buckets_content_count = 0;

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
        """create belt with one empty bucket"""

        self.belts[belt_index] = [[]]
        self.belts_rotated_at[belt_index] = int(time.time())

    def insert(self, item):
        """Insert item into pool"""

        # which belt we'll insert item?
        belt_index = self.key_generator.generate(item)

        # register belt if not yet
        if belt_index not in self.belts:
            self.create_belt(belt_index)

        # append item to the 0-indexed bucket of the specified belt
        self.belts[belt_index][0].append(item)

        # try to rotate belt - may it it already should be rotated
        self.rotate_belt(belt_index)

    def flush(self, key=None):
        """Flush all buckets from the belt and delete the belt itself"""

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
        """Try to rotate belt"""

        now = int(time.time())

        if flush:
            # explicit flush requested
            rotate_reason = "FLUSH"

        elif len(self.belts[belt_index][0]) >= self.max_bucket_size:
            # 0-index bucket is full
            rotate_reason = "SIZE"

        elif now >= self.belts_rotated_at[belt_index] + self.max_interval_between_rotations:
            # time interval reached
            rotate_reason = "TIME"

        else:
            # no need to rotate belt - belt not rotated
            return False

        # belt(s) needs rotation

        # insert empty bucket into the beginning of the belt
        self.belts[belt_index].insert(0, [])
        self.belts_rotated_at[belt_index] = now

        # in case we flush belt we'll keep one just inserted empty bucket
        buckets_num_left_on_belt = 1 if flush else self.max_belt_size

        while len(self.belts[belt_index]) > buckets_num_left_on_belt:
            # too many buckets on the belt
            # time to rotate belt and flush the most-right-bucket

            buckets_num = len(self.belts[belt_index])
            last_bucket_size = len(self.belts[belt_index][buckets_num-1])

            self.buckets_count += 1
            self.buckets_content_count += last_bucket_size

            logging.info('rot now:%d bktcnt:%d bktcontentcnt: %d index:%s reason:%s bktsonbelt:%d bktsize:%d beltnum:%d',
                now,
                self.buckets_count,
                self.buckets_content_count,
                str(belt_index),
                rotate_reason,
                buckets_num,
                last_bucket_size,
                len(self.belts)
            )

            # time to flush data for specified key
            self.writer_builder.param('csv_file_path_suffix_parts', [str(now), str(self.buckets_count)])
            writer = self.writer_builder.get()
            writer.insert(self.belts[belt_index].pop())
            writer.close()
            writer.push()
            writer.destroy()
            del writer

        if self.prev_time is not None:
            # have previous time - meaning this is at least second rotate
            # can calculate belt speed
            window_size = now - self.prev_time
            buckets_per_sec = (self.buckets_count - self.prev_buckets_count)/window_size
            buckets_content_per_sec = (self.buckets_content_count - self.prev_buckets_content_count)/window_size
            logging.info(
                'buckets per sec:%f buckets content per sec:%f for last %d sec',
                 buckets_per_sec,
                 buckets_content_per_sec,
                 window_size
            )

        self.prev_time = now
        self.prev_buckets_count = self.buckets_count
        self.prev_buckets_content_count = self.buckets_content_count

        # belt rotated
        return True
