import unittest
import os.path
from tools.timelines_builder import TimelinesBuilder, TimeIntervals
from tools.timelines_loader import TimelinesLoader


class TestTimelines(unittest.TestCase):

    def setUp(self) -> None:
        self.builder = TimelinesBuilder()
        self.loader = TimelinesLoader({'path': None}, {})
        self.data = []
        makefile_test = not os.path.isfile('builder_data/test_timelines_builder.json')
        self.path_beginning = 'tests/' if makefile_test else ''
        self.loader.read_file(self.data, self.path_beginning + 'builder_data/test_timelines_builder.json')
        self.timelines = self.builder.get_all_timelines(self.data)

    def test_minutes_timeline(self):
        result = self.timelines[TimeIntervals.MINUTES]
        self.assertEqual(len(result), 105)
        for i in range(5):
            self.assertEqual(result[i]['value'], 1)
        for i in range(5, len(result)):
            self.assertEqual(result[i]['value'], 2)
        self.assertEqual(True, True)

    def test_hours_timeline(self):
        result = self.timelines[TimeIntervals.HOURS]
        self.assertEqual(len(result), 8)
        for i in range(5):
            self.assertEqual(result[i]['value'], 1)
        self.assertEqual(result[-1]['value'] == 34 and
                         result[-2]['value'] == 120 and
                         result[-3]['value'] == 46, True)

    def test_days_timeline(self):
        result = self.timelines[TimeIntervals.DAYS]
        self.assertEqual(len(result), 5)
        self.assertEqual(result[-1]['value'] == 200 and
                         result[-2]['value'] == 1 and
                         result[-3]['value'] == 1 and
                         result[-4]['value'] == 1 and
                         result[-5]['value'] == 2, True)

    def test_months_timeline(self):
        result = self.timelines[TimeIntervals.MONTHS]
        self.assertEqual(len(result), 4)

        self.assertEqual(result[-1]['value'] == 201 and
                         result[-2]['value'] == 1 and
                         result[-3]['value'] == 1 and
                         result[-4]['value'] == 2, True)

    def test_gap_in_data(self):
        data_with_gaps = []
        self.loader.read_file(data_with_gaps, self.path_beginning +
                                              'builder_data/test_timelines_builder_with_gaps.json')
        timelines = self.builder.get_all_timelines(data_with_gaps)
        result = timelines[TimeIntervals.MINUTES]
        self.assertEqual(len(result), 99)
        for i in range(5):
            self.assertEqual(result[i]['value'], 1)
        for i in range(5, len(result)):
            self.assertEqual(result[i]['value'], 2)

    def test_leap_year(self):
        big_data = []
        self.loader.read_file(big_data, self.path_beginning +
                                              'builder_data/test_timelines_builder_leap_years.json')
        self.ordered = self.builder.get_all_timelines(big_data)
        result = self.ordered[TimeIntervals.MONTHS]
        self.assertEqual(len(result), 5)
        for i in range(5):
            self.assertEqual(result[i]['value'], 1)

    def test_unordered_data(self):
        self.test_leap_year()
        unordered_data = []
        self.loader.read_file(unordered_data, self.path_beginning +
                                           'builder_data/test_timelines_builder_unordered.json')
        timelines = self.builder.get_all_timelines(unordered_data)
        self.ordered[0].append({'time': 1583010000.0, 'value': 2})
        self.ordered[0].append({'time': 1583010060.0, 'value': 1})
        self.ordered[1].append({'time': 1583010000.0, 'value': 3})
        self.ordered[2].append({'time': 1583010000.0, 'value': 3})
        self.ordered[3].append({'time': 1583010000.0, 'value': 3})
        self.assertEqual(timelines, self.ordered)
