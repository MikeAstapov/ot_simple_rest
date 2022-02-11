import unittest
import json
from tools.interesting_fields_builder import InterestingFieldsBuilder
from tools.interesting_fields_loader import InterestingFieldsLoader


class TestInterestingFields(unittest.TestCase):

    def setUp(self) -> None:
        self.builder = InterestingFieldsBuilder()
        self.loader = InterestingFieldsLoader({'path': None}, None)
        self.data = self.loader._load_data_test('builder_data/test_interesting_fields_builder.json')

    def test_interesting_fields(self):
        result = self.builder.get_interesting_fields(self.data)
        with open('builder_data/right_interesting_fields.json') as file:
            target = json.load(file)
        self.assertEqual(result, target)
