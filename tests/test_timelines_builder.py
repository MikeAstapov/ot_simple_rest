import unittest
import json
from tools.timelines_builder import TimelinesBuilder
from tools.timelines_loader import TimelinesLoader


class TestTimelines(unittest.TestCase):

    def setUp(self) -> None:
        self.builder = TimelinesBuilder()
        self.loader = TimelinesLoader({'path': None}, None, self.builder.BIGGEST_INTERVAL)
        self.data = []
        self.loader.read_file_backwards(self.data, 'builder_data/test_timelines_builder.json', None)

    def test_minutes_timeline(self):
        result = self.builder.get_timeline(self.data, self.builder.INTERVALS['m'])
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result)):
            if result[i]['value'] != 2:
                self.assertEqual(True, False)
        self.assertEqual(True, True)

    def test_hours_timeline(self):
        result = self.builder.get_timeline(self.data, self.builder.INTERVALS['h'])
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result) - 3):
            if result[i]['value'] != 0:
                if i == 24 and result[i]['value'] == 1:
                    continue
                self.assertEqual(True, False)
        self.assertEqual(result[-1]['value'] == 34 and
                         result[-2]['value'] == 120 and
                         result[-3]['value'] == 46, True)

    def test_days_timeline(self):
        result = self.builder.get_timeline(self.data, self.builder.INTERVALS['d'])
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result) - 1):
            if result[i]['value'] != 0:
                if i in (12, 44, 48) and result[i]['value'] == 1:
                    continue
                self.assertEqual(True, False)
        self.assertEqual(result[-1]['value'] == 200, True)

    def test_months_timeline(self):
        result = self.builder.get_timeline(self.data, self.builder.INTERVALS['M'])
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result) - 4):
            if result[i]['value'] != 0:
                self.assertEqual(True, False)
        self.assertEqual(result[-1]['value'] == 201 and
                         result[-2]['value'] == 1 and
                         result[-3]['value'] == 1 and
                         result[-4]['value'] == 1, True)

    def test_minutes_timeline_with_field(self):
        with open('builder_data/right_values_minutes_timeline.json') as file:
            target = json.load(file)
        result = self.builder.get_timeline(self.data, self.builder.INTERVALS['m'], 'value')
        self.assertEqual(result, target)

    def test_hours_timeline_with_field(self):
        result = self.builder.get_timeline(self.data, self.builder.INTERVALS['h'], 'value')
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result) - 3):
            if result[i]['value'] != 0:
                if i == 24 and result[i]['value'] == 42:
                    continue
                self.assertEqual(True, False)
        self.assertEqual(result[-1]['value'] == 156 and
                         result[-2]['value'] == 592 and
                         result[-3]['value'] == 209, True)

    def test_days_timeline_with_field(self):
        result = self.builder.get_timeline(self.data, self.builder.INTERVALS['d'], 'value')
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result) - 1):
            if result[i]['value'] != 0:
                if i == 12 and result[i]['value'] == 84:
                    continue
                if i == 44 and result[i]['value'] == 21:
                    continue
                if i == 48 and result[i]['value'] == 42:
                    continue
                self.assertEqual(True, False)
        self.assertEqual(result[-1]['value'] == 957, True)

    def test_months_timeline_with_field(self):
        result = self.builder.get_timeline(self.data, self.builder.INTERVALS['M'], 'value')
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result) - 4):
            if result[i]['value'] != 0:
                self.assertEqual(True, False)
        self.assertEqual(result[-1]['value'] == 999 and
                         result[-2]['value'] == 21 and
                         result[-3]['value'] == 84 and
                         result[-4]['value'] == 168, True)

    def test_gap_in_data(self):
        data_with_gaps = []
        self.loader.read_file_backwards(data_with_gaps, 'builder_data/test_timelines_builder_with_gaps.json', None)
        result = self.builder.get_timeline(data_with_gaps, self.builder.INTERVALS['m'])
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        for i in range(len(result)):
            if (37 < i < 44 or i == 47 or i == 48) and result[i]['value'] != 0:
                self.assertEqual(True, False)
            elif (37 >= i or i >= 44 and i != 47 and i != 48) and result[i]['value'] != 2:
                self.assertEqual(True, False)
        self.assertEqual(True, True)

    def test_leap_year(self):
        big_data = []
        self.loader.read_file_backwards(big_data, 'builder_data/test_timelines_builder_leap_years.json', None)
        result = self.builder.get_timeline(big_data, self.builder.INTERVALS['M'])
        if len(result) != self.builder.points:  # wrong timeline len
            self.assertEqual(True, False)
        self.assertEqual(result[-1]['value'], 1)
