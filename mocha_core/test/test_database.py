#!/usr/bin/env python3
import unittest
import sys
import os
import pdb
from colorama import Fore, Back, Style
import mocha_core.database as database
import sample_db
import mocha_core.hash_comm as hc
import mocha_core.database_utils as du
import copy
import rclpy.time

import pprint

ROBOT0_TOPIC0_PRIO0 = sample_db.header_of(0, 0, 'msg2')
ROBOT0_TOPIC1_PRIO1_OLD = sample_db.header_of(0, 1, 'msg3')
ROBOT0_TOPIC1_PRIO1_NEW = sample_db.header_of(0, 1, 'msg4')
ROBOT1_TOPIC0_PRIO4_NEW = sample_db.header_of(1, 0, 'msg1')
ROBOT1_TOPIC0_PRIO4_OLD = sample_db.header_of(1, 0, 'msg2')


class test(unittest.TestCase):
    def setUp(self):
        test_name = self._testMethodName
        print("\n", Fore.RED, 20*"=", test_name, 20*"=", Style.RESET_ALL)
        super().setUp()

    def tearDown(self):
        import time
        time.sleep(1)
        super().tearDown()

    def test_get_header_list(self):
        dbl = sample_db.get_sample_dbl()
        for i in range(5):
            # Check for locked mutex with loop
            # Test get all headers
            headers = dbl.get_header_list()
            self.assertTrue(ROBOT0_TOPIC0_PRIO0 in headers)
            self.assertTrue(ROBOT1_TOPIC0_PRIO4_OLD in headers)
            # The order of headers should also be respected by the
            # priority. ROBOT0_TOPIC0_PRIO0 has the lowest priority
            # among the headers so it should be the last of the list.
            # Conversely, ROBOT1_TOPIC0_PRIO4_NEW has the highest priority
            # and it should be first
            self.assertEqual(headers[-1], ROBOT0_TOPIC0_PRIO0)
            self.assertEqual(headers[0], ROBOT1_TOPIC0_PRIO4_NEW)
            # Test get headers filtering by robot
            headers_rfilt = dbl.get_header_list(filter_robot_id=0)
            self.assertTrue(ROBOT0_TOPIC0_PRIO0 in headers_rfilt)
            self.assertFalse(ROBOT1_TOPIC0_PRIO4_NEW in headers_rfilt)
            # TOPIC2 should also be the last header after filtering
            self.assertEqual(headers_rfilt[-1], ROBOT0_TOPIC0_PRIO0)
            # TOPIC 4 and 3 should be the second and third headers. Their
            # priority is 1, so they should be ordered by timestamp
            # latest timestamp
            self.assertEqual(headers_rfilt[1], ROBOT0_TOPIC1_PRIO1_NEW)
            self.assertEqual(headers_rfilt[2], ROBOT0_TOPIC1_PRIO1_OLD)
            # Test timestamp filtering. Only one timestamp should be
            # remaining (and it is not ROBOT0_TOPIC0_PRIO0).
            headers_tfilt = dbl.get_header_list(filter_robot_id=0,
                                                filter_latest=True)
            self.assertTrue(ROBOT0_TOPIC0_PRIO0 in headers_tfilt)
            self.assertTrue(ROBOT0_TOPIC1_PRIO1_NEW in headers_tfilt)
            self.assertFalse(ROBOT0_TOPIC1_PRIO1_OLD in headers_tfilt)
            self.assertFalse(ROBOT1_TOPIC0_PRIO4_OLD in headers_tfilt)
            self.assertFalse(ROBOT1_TOPIC0_PRIO4_NEW in headers_tfilt)
            self.assertTrue(len(headers_tfilt) == 2)
            headers_tfilt = dbl.get_header_list(filter_latest=True)
            self.assertTrue(ROBOT0_TOPIC0_PRIO0 in headers_tfilt)
            self.assertTrue(ROBOT0_TOPIC1_PRIO1_NEW in headers_tfilt)
            self.assertTrue(ROBOT1_TOPIC0_PRIO4_NEW in headers_tfilt)
            self.assertFalse(ROBOT0_TOPIC1_PRIO1_OLD in headers_tfilt)
            self.assertFalse(ROBOT1_TOPIC0_PRIO4_OLD in headers_tfilt)
            self.assertTrue(len(headers_tfilt) == 3)

    def test_headers_not_in_local(self):
        dbl = sample_db.get_sample_dbl()
        header_list = dbl.get_header_list()
        # Test with all headers
        extra_header_1 = du.generate_random_header()
        extra_header_2 = du.generate_random_header()
        header_list.append(extra_header_2)
        header_list.append(extra_header_1)
        new_headers = [extra_header_1, extra_header_2]
        new_headers.sort()
        # Test without newer
        discover_extra_header = dbl.headers_not_in_local(header_list)
        discover_extra_header.sort()
        self.assertListEqual(discover_extra_header,
                             new_headers)
        # Test with older header, create a copy of one of the existing headers in the
        # db. Pick one with the ticks field that can accomodate both addition and
        # substraction
        test_header = None
        for header in header_list:
            h = hc.TsHeader.from_header(header)
            if h.ticks > 0 and h.ticks < 65535:
                test_header = header
                break
        self.assertTrue(test_header is not None)
        # Modify the timestamp to make it before the current msg
        h = hc.TsHeader.from_header(test_header)
        h.ticks -= 1
        extra_header_3 = h.bindigest()
        header_list.append(extra_header_3)
        discover_extra_header_latest = dbl.headers_not_in_local(header_list, newer=True)
        discover_extra_header_latest.sort()
        discover_extra_header_all = dbl.headers_not_in_local(header_list)
        discover_extra_header_all.sort()
        self.assertListEqual(discover_extra_header_latest, new_headers)
        diff_header = set(discover_extra_header_all) - set(discover_extra_header)
        self.assertEqual(diff_header.pop(), extra_header_3)
        # Modify the timestamp to make it after the current msg
        h = hc.TsHeader.from_header(test_header)
        h.ticks += 1
        extra_header_4 = h.bindigest()
        header_list.append(extra_header_4)
        discover_extra_header_latest = dbl.headers_not_in_local(header_list, newer=True)
        new_headers.append(extra_header_4) # The new header should show in the list
        new_headers.sort()
        discover_extra_header_latest.sort()
        self.assertListEqual(new_headers, discover_extra_header_latest)
        # Finally, getting all the headers should not filter them out
        discover_extra_header = dbl.headers_not_in_local(header_list)
        self.assertTrue(len(discover_extra_header) == 4)

    def test_find_header(self):
        dbl = sample_db.get_sample_dbl()
        header = sample_db.header_of(1, 0, 'msg2')
        robot_id = 1
        topic_id = 0
        dtype = dbl.db[robot_id][topic_id][header].dtype
        prio = dbl.db[robot_id][topic_id][header].priority
        ts = dbl.db[robot_id][topic_id][header].ts
        data = dbl.db[robot_id][topic_id][header].data
        dbm = dbl.find_header(header)
        self.assertEqual(dbm.robot_id, robot_id)
        self.assertEqual(dbm.topic_id, topic_id)
        self.assertEqual(dbm.dtype, dtype)
        self.assertEqual(dbm.priority, prio)
        self.assertAlmostEqual(dbm.ts, ts)
        self.assertEqual(dbm.data, data)

    def test_last_message_keeps_only_the_newest(self):
        # A LAST_MESSAGE topic (robot 0, topic 1) keeps one message: a newer
        # one replaces the older, and an older one arriving late is dropped
        # without the insertion callback. Topic 0 keeps its whole history.
        inserted = []
        dbl = database.DBwLock(insertion_cb=lambda r, t, ts, h: inserted.append(h),
                               last_only={(0, 1)})
        def message(topic_id, seconds):
            return database.DBMessage(0, topic_id, 0, 1,
                                      rclpy.time.Time(seconds=seconds), b'data')
        first, second, late = message(1, 10), message(1, 20), message(1, 15)
        for dbm in (first, second, late):
            dbl.add_modify_data(dbm)
        self.assertEqual(list(dbl.db[0][1]), [second.header])
        self.assertEqual(inserted, [first.header, second.header])
        self.assertRaises(Exception, dbl.find_header, first.header)
        for seconds in (10, 20):
            dbl.add_modify_data(message(0, seconds))
        self.assertEqual(len(dbl.db[0][0]), 2)

    def test_find_newer_of_a_replaced_message(self):
        # A fetch of a LAST_MESSAGE message that a newer one replaced finds the
        # newer one; a topic that keeps its history, or a header with nothing
        # newer, raises
        dbl = database.DBwLock(last_only={(0, 1)})
        def message(topic_id, seconds, data):
            return database.DBMessage(0, topic_id, 0, 1,
                                      rclpy.time.Time(seconds=seconds), data)
        old, new = message(1, 10, b'old'), message(1, 20, b'new')
        dbl.add_modify_data(old)
        self.assertRaises(Exception, dbl.find_newer, old.header)
        dbl.add_modify_data(new)
        found = dbl.find_newer(old.header)
        self.assertEqual((found.header, found.data), (new.header, b'new'))
        self.assertRaises(Exception, dbl.find_newer, new.header)
        history = message(0, 10, b'kept')
        dbl.add_modify_data(history)
        dbl.add_modify_data(message(0, 20, b'kept too'))
        self.assertRaises(Exception, dbl.find_newer, history.header)

    def test_messages_a_tenth_of_a_ms_apart_keep_their_own_headers(self):
        # A header's time is kept to 1/65536 s: two messages of one topic
        # stamped 0.1 ms apart, as a node publishing at 5 Hz gets now and then,
        # have different headers, the later one the newer, and each header
        # made again from its message's time is the same.
        first = database.DBMessage(0, 1, 0, 1,
                                   rclpy.time.Time(seconds=100, nanoseconds=500000000), b'first')
        second = database.DBMessage(0, 1, 0, 1,
                                    rclpy.time.Time(seconds=100, nanoseconds=500100000), b'second')
        self.assertNotEqual(first.header, second.header)
        self.assertTrue(database.is_newer(second.ts, first.ts))
        for m in (first, second):
            again = hc.TsHeader.from_data(0, 1, m.ts)
            self.assertEqual(again.bindigest(), m.header)
        dbl = database.DBwLock()
        dbl.add_modify_data(first)
        dbl.add_modify_data(second)
        self.assertEqual(len(dbl.db[0][1]), 2)

    def test_a_message_held_over_half_a_wrap_loses_to_any(self):
        # A LAST_MESSAGE topic quiet overnight: its message from 18:00 has
        # been held 14 h by this computer's clock, more than half a wrap, when
        # one from 08:00 comes, whose header time reads as older across the
        # wrap. The new one is kept, and a remote one is asked for.
        dbl = database.DBwLock(last_only={(0, 1)})
        def message(seconds, data):
            return database.DBMessage(0, 1, 0, 1, rclpy.time.Time(seconds=seconds), data)
        evening = message(18 * 3600, b'evening')
        morning = message(18 * 3600 + 14 * 3600, b'morning')
        self.assertFalse(database.is_newer(morning.ts, evening.ts))
        dbl.add_modify_data(evening)
        evening.held_since -= 14 * 3600
        self.assertEqual(dbl.headers_not_in_local([morning.header], newer=True), [morning.header])
        dbl.add_modify_data(morning)
        self.assertEqual(list(dbl.db[0][1]), [morning.header])

    def test_newer_across_the_wrap_of_the_header_time(self):
        # A header keeps the seconds modulo 65536. A message sent just after
        # a wrap is newer than one sent just before it: a LAST_MESSAGE topic
        # keeps it, a robot holding the earlier one asks for it, and a fetch
        # of the earlier one finds it. Two full wraps later the same holds.
        for wraps in (1, 3):
            inserted = []
            dbl = database.DBwLock(insertion_cb=lambda r, t, ts, h: inserted.append(h),
                                   last_only={(0, 1)})
            def message(seconds, data):
                return database.DBMessage(0, 1, 0, 1,
                                          rclpy.time.Time(seconds=seconds), data)
            before = message(65536*wraps - 1, b'before')
            after = message(65536*wraps + 1, b'after')
            self.assertLess(after.ts, before.ts)  # as the header keeps them
            self.assertTrue(database.is_newer(after.ts, before.ts))
            self.assertFalse(database.is_newer(before.ts, after.ts))
            self.assertFalse(database.is_newer(before.ts, before.ts))
            dbl.add_modify_data(before)
            self.assertEqual(dbl.headers_not_in_local([after.header], newer=True),
                             [after.header])
            dbl.add_modify_data(after)
            self.assertEqual(list(dbl.db[0][1]), [after.header])
            self.assertEqual(inserted, [before.header, after.header])
            self.assertEqual(dbl.get_header_list(filter_latest=True), [after.header])
            self.assertEqual(dbl.headers_not_in_local([before.header], newer=True), [])
            self.assertEqual(dbl.find_newer(before.header).data, b'after')
            # The earlier one arriving late is dropped
            dbl.add_modify_data(message(65536*wraps - 1, b'before'))
            self.assertEqual(list(dbl.db[0][1]), [after.header])

if __name__ == '__main__':
    # Run test cases!
    unittest.main()
