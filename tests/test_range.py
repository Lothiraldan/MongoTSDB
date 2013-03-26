import unittest

from mongotsdb import Range, SubRange, RangeSet, MultiRangeWorker, RangeWorker

class RangeTestCase(unittest.TestCase):

    def setUp(self):
        self.start = 0
        self.stop = 7
        self.step = self.stop - self.start
        self.r = Range(self.start, self.stop)

    def test_instantiation(self):
        self.assertTrue(self.r.is_empty())
        self.assertFalse(self.r.is_partial())
        self.assertFalse(self.r.is_full())

    def test_sub_range_beggining(self):
        value = 42
        sub_range = SubRange(0, 3, value)

        self.r.add_sub_range(sub_range)

        self.assertFalse(self.r.is_empty())
        self.assertTrue(self.r.is_partial())
        self.assertFalse(self.r.is_full())

        expected_subrange = SubRange(4, 7)
        self.assertEqual(self.r.get_missing_ranges(), [expected_subrange])

    def test_sub_range_end(self):
        value = 42
        sub_range = SubRange(4, 7, value)

        self.r.add_sub_range(sub_range)

        self.assertFalse(self.r.is_empty())
        self.assertTrue(self.r.is_partial())
        self.assertFalse(self.r.is_full())

        expected_subrange = SubRange(0, 3)
        self.assertEqual(self.r.get_missing_ranges(), [expected_subrange])

    def test_sub_range_middle(self):
        value = 42
        sub_range = SubRange(2, 6, value)

        self.r.add_sub_range(sub_range)

        self.assertFalse(self.r.is_empty())
        self.assertTrue(self.r.is_partial())
        self.assertFalse(self.r.is_full())

        expected_subrange_1 = SubRange(0, 1)
        expected_subrange_2 = SubRange(7, 7)
        self.assertEqual(self.r.get_missing_ranges(), [expected_subrange_1,
            expected_subrange_2])

    def test_sub_range_full(self):
        value = 42
        sub_range = SubRange(0, 7, value)

        self.r.add_sub_range(sub_range)

        self.assertFalse(self.r.is_empty())
        self.assertFalse(self.r.is_partial())
        self.assertTrue(self.r.is_full())

        self.assertEqual(self.r.get_missing_ranges(), [])

    def test_sub_range_full_with_2_subranges(self):
        value = 42
        sub_range1 = SubRange(0, 3, value)

        value = 42
        sub_range2 = SubRange(4, 7, value)

        self.r.add_sub_range(sub_range1)
        self.r.add_sub_range(sub_range2)

        self.assertFalse(self.r.is_empty())
        self.assertFalse(self.r.is_partial())
        self.assertTrue(self.r.is_full())

        self.assertEqual(self.r.get_missing_ranges(), [])


class RangeSetTestCase(unittest.TestCase):

    def setUp(self):
        self.start = 0
        self.stop = 49
        self.step = 10
        self.range_set = RangeSet(self.start, self.stop, self.step)

    def test_simple(self):
        workers = list(self.range_set.generate_workers())

        expected_workers = [MultiRangeWorker(0, 49, 10)]

        self.assertEqual(workers, expected_workers)

    def test_add_subrange(self):
        sub_range = SubRange(22, 25, value=42)
        self.range_set.add_sub_range(sub_range)

        workers = list(self.range_set.generate_workers())

        expected_range = Range(20, 29)
        expected_range.missing_ranges = [SubRange(20, 21),
            SubRange(26, 29)]
        expected_range.sub_ranges = [sub_range]
        partial_range_worker = RangeWorker(expected_range)

        expected_workers = [MultiRangeWorker(0, 19, 10),
            partial_range_worker, MultiRangeWorker(30, 49, 10)]
        self.assertEqual(workers, expected_workers)

    def test_not_aligned_ranges(self):
        start = 5
        stop = 25
        step = 10
        range_set = RangeSet(start, stop, step)

        self.assertEqual(range_set.ranges, [Range(5, 9), Range(10, 19),
            Range(20, 25)])

