import json
import tornado.web
from tools.interesting_fields_builder import InterestingFieldsBuilder
from tools.interesting_fields_loader import InterestingFieldsLoader
from typing import Dict

__author__ = "Ilia Sagaidak"
__copyright__ = "Copyright 2022, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Ilia Sagaidak"
__email__ = "isagaidak@isgneuro.com"
__status__ = "Dev"


class GetInterestingFields(tornado.web.RequestHandler):
    """
    Returns a list of dictionaries where every dictionary represents interesting fields for one column of data

    interesting fields consist of:
    :id: serial number of a column
    :text: name of a column
    :totalCount: number of not empty cells in the column (null is considered an empty cell)
    :static: list of dictionaries where every dictionary is an info about every unique value in a column consists of:
            :value: value itself
            :count: how many times the value appears in the column
            :%: percent of count from all rows in the data table
    """

    def initialize(self, mem_conf: Dict, static_conf: Dict):
        self.builder = InterestingFieldsBuilder()
        self.loader = InterestingFieldsLoader(mem_conf, static_conf)

    async def get(self):
        params = self.request.query_arguments
        cid = params.get('cid')[0].decode()
        try:
            data = self.loader.load_data(cid)
            interesting_fields = self.builder.get_interesting_fields(data)
        except tornado.web.HTTPError as e:
            return self.write(json.dumps({'status': 'failed', 'error': e}, default=str))
        except Exception as e:
            return self.write(json.dumps({'status': 'failed', 'error': f'{e} cid {cid}'}, default=str))
        self.write(json.dumps(interesting_fields))
