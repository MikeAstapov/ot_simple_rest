import unittest
import json
import pandas as pd
import os.path
from tools.interesting_fields_builder import InterestingFieldsBuilder


class TestInterestingFields(unittest.TestCase):

    def setUp(self) -> None:
        self.builder = InterestingFieldsBuilder()
        makefile_test = not os.path.isfile('builder_data/test_interesting_fields_builder.json')
        self.path_beginning = 'tests/' if makefile_test else ''
        self.data = pd.read_json(self.path_beginning + 'builder_data/test_interesting_fields_builder.json', lines=True,
                                 convert_dates=False)

    def test_interesting_fields(self):
        result = self.builder.get_interesting_fields(self.data)
        with open(self.path_beginning + 'builder_data/right_interesting_fields.json') as file:
            target = json.load(file)
        self.assertEqual(len(result), len(target))
        for i in range(len(target)):
            result_static = result[i].pop('static')
            target_static = target[i].pop('static')
            res = sorted(target_static, key=lambda elem: sorted(elem.items())) == sorted(
                result_static, key=lambda elem: sorted(elem.items()))
            self.assertEqual(res, True)
        self.assertEqual(result, target)
