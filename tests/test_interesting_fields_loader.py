import unittest
import json
import pandas as pd
import os.path
from tools.interesting_fields_loader import InterestingFieldsLoader


class TestInterestingFields(unittest.TestCase):

    def setUp(self) -> None:
        makefile_test = not os.path.isfile('builder_data/test_interesting_fields_builder.json')
        self.loader = InterestingFieldsLoader(
            mem_conf={'path': 'tests/loader_data' if makefile_test else 'loader_data'},
            static_conf={}
        )

    def test_interesting_fields(self):
        ft = 1653811804
        tt = 1655194188
        df = self.loader.load_data('0')
        self.assertEqual(len(df), 52)
        df = self.loader.load_data('0', from_time=ft)
        self.assertEqual(len(df), 46)
        for _, row in df.iterrows():
            self.assertGreaterEqual(int(row['_time']), ft)
        df = self.loader.load_data('0', to_time=tt)
        self.assertEqual(len(df), 23)
        for _, row in df.iterrows():
            self.assertLessEqual(int(row['_time']), tt)
        df = self.loader.load_data('0', from_time=ft, to_time=tt)
        self.assertEqual(len(df), 17)
        for _, row in df.iterrows():
            self.assertGreaterEqual(int(row['_time']), ft)
            self.assertLessEqual(int(row['_time']), tt)
        df = self.loader.load_data('0', from_time=tt, to_time=ft)
        self.assertEqual(len(df), 0)
        df = self.loader.load_data('0', from_time=1653552607, to_time=1653552607)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['_time'], 1653552607)



