import unittest
import json
from tools.interesting_fields_builder import InterestingFieldsBuilder


class TestInterestingFields(unittest.TestCase):

    def setUp(self) -> None:
        self.builder = InterestingFieldsBuilder({'path': None}, None)
        self.data = self.builder._load_json_lines_test('builder_data/test_interesting_fields_builder.json')

    def test_interesting_fields(self):
        result = self.builder._get_fields(self.data)
        with open('builder_data/right_interesting_fields.json') as file:
            target = json.load(file)
        self.assertEqual(result, target)
