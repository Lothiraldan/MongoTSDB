import unittest

from mongotsdb import Range, SubRange, RangeSet, MultiRange

class RangeTestCase(unittest.TestCase):

    def setUp(self):
        self.start = 0
        self.stop = 8
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

        expected_subrange = SubRange(4, 8)
        self.assertEqual(self.r.get_missing_ranges(), [expected_subrange])

    def test_sub_range_end(self):
        value = 42
        sub_range = SubRange(4, 8, value)

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
        expected_subrange_2 = SubRange(7, 8)
        self.assertEqual(self.r.get_missing_ranges(), [expected_subrange_1,
            expected_subrange_2])

    def test_sub_range_full(self):
        value = 42
        sub_range = SubRange(0, 8, value)

        self.r.add_sub_range(sub_range)

        self.assertFalse(self.r.is_empty())
        self.assertFalse(self.r.is_partial())
        self.assertTrue(self.r.is_full())

        self.assertEqual(self.r.get_missing_ranges(), [])

    def test_sub_range_full_with_2_subranges(self):
        value = 42
        sub_range1 = SubRange(0, 3, value)

        value = 42
        sub_range2 = SubRange(4, 8, value)

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
        ranges = list(self.range_set.get_sub_ranges())

        expected_ranges = [SubRange(0, 9), SubRange(10, 19), SubRange(20, 29),
            SubRange(30, 39), SubRange(40, 49)]
        self.assertEqual(ranges, expected_ranges)

    def test_add_subrange(self):
        sub_range = SubRange(12, 15)
        self.range_set.add_sub_range(sub_range)

        ranges = list(self.range_set.get_sub_ranges())
        expected_ranges = [SubRange(0, 9), SubRange(10, 11), SubRange(16, 19),
            SubRange(20, 29), SubRange(30, 39), SubRange(40, 49)]
        self.assertEqual(ranges, expected_ranges)

    def test_smart_ranges(self):
        self.assertEqual(self.range_set.smart_ranges(), [MultiRange(0, 49, 10)])

    def test_smart_ranges_and_subrange(self):
        sub_range = SubRange(22, 25)
        self.range_set.add_sub_range(sub_range)

        expected = [MultiRange(0, 19, 10), SubRange(20, 21), SubRange(26, 29),
            MultiRange(30, 49, 10)]
        self.assertEqual(self.range_set.smart_ranges(), expected)
