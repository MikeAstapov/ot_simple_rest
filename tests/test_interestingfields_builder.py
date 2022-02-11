import unittest
import json
import pandas as pd
from tools.interesting_fields_builder import InterestingFieldsBuilder


class TestInterestingFields(unittest.TestCase):

    def setUp(self) -> None:
        self.builder = InterestingFieldsBuilder()
        self.data = pd.read_json('builder_data/test_interesting_fields_builder.json', lines=True, convert_dates=False)

    def test_interesting_fields(self):
        result = self.builder.get_interesting_fields(self.data)
        with open('builder_data/right_interesting_fields.json') as file:
            target = json.load(file)
        self.assertEqual(result, target)
