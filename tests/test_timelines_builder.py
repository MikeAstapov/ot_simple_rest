import unittest
import os.path
from tools.timelines_builder import TimelinesBuilder, TimeIntervals
from tools.timelines_loader import TimelinesLoader


class TestTimelines(unittest.TestCase):

    def setUp(self) -> None:
        self.builder = TimelinesBuilder()
        self.loader = TimelinesLoader({'path': None}, {}, self.builder.BIGGEST_INTERVAL)
        data = []
        makefile_test = not os.path.isfile('builder_data/test_timelines_builder.json')
        self.path_beginning = 'tests/' if makefile_test else ''
        fresh_time = self.loader.read_file(data, self.path_beginning + 'builder_data/test_timelines_builder.json', None)
        self.timelines = self.builder.get_all_timelines(data, fresh_time)

    def test_minutes_timeline(self):
        result = self.timelines[TimeIntervals.MINUTES]
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result)):
            if result[i]['value'] != 2:
                self.assertEqual(True, False)
        self.assertEqual(True, True)

    def test_hours_timeline(self):
        result = self.timelines[TimeIntervals.HOURS]
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result) - 3):
            if result[i]['value'] != 0:
                if i == 25 and result[i]['value'] == 1:
                    continue
                self.assertEqual(True, False)
        self.assertEqual(result[-1]['value'] == 34 and
                         result[-2]['value'] == 120 and
                         result[-3]['value'] == 46, True)

    def test_days_timeline(self):
        result = self.timelines[TimeIntervals.DAYS]
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result) - 1):
            if result[i]['value'] != 0:
                if i in (14, 45, 48) and result[i]['value'] == 1:
                    continue
                self.assertEqual(True, False)
        self.assertEqual(result[-1]['value'] == 200, True)

    def test_months_timeline(self):
        result = self.timelines[TimeIntervals.MONTHS]
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result) - 4):
            if result[i]['value'] != 0:
                self.assertEqual(True, False)
        self.assertEqual(result[-1]['value'] == 201 and
                         result[-2]['value'] == 1 and
                         result[-3]['value'] == 1 and
                         result[-4]['value'] == 1, True)

    def test_gap_in_data(self):
        data_with_gaps = []
        fresh_time = self.loader.read_file(data_with_gaps, self.path_beginning +
                                           'builder_data/test_timelines_builder_with_gaps.json', None)
        timelines = self.builder.get_all_timelines(data_with_gaps, fresh_time)
        result = timelines[TimeIntervals.MINUTES]
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result)):
            if (39 < i < 45 or i == 48) and result[i]['value'] != 0:
                self.assertEqual(True, False)
            elif (i < 40 or i > 44 and i != 48) and result[i]['value'] != 2:
                self.assertEqual(True, False)
        self.assertEqual(True, True)

    def test_leap_year(self):
        big_data = []
        fresh_time = self.loader.read_file(big_data, self.path_beginning +
                                           'builder_data/test_timelines_builder_leap_years.json', None)
        self.ordered = self.builder.get_all_timelines(big_data, fresh_time)
        result = self.ordered[TimeIntervals.MONTHS]
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        self.assertEqual(result[-1]['value'], 1)

    def test_unordered_data(self):
        self.test_leap_year()
        unordered_data = []
        fresh_time = self.loader.read_file(unordered_data, self.path_beginning +
                                           'builder_data/test_timelines_builder_unordered.json', None)
        timelines = self.builder.get_all_timelines(unordered_data, fresh_time)
        self.assertEqual(timelines, self.ordered)
