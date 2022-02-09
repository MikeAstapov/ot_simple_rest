import json
import tornado.web
from tools.interesting_fields_builder import InterestingFieldsBuilder

__author__ = "Ilia Sagaidak"
__copyright__ = "Copyright 2022, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Ilia Sagaidak"
__email__ = "isagaidak@isgneuro.com"
__status__ = "Dev"


class GetInterestingFields(tornado.web.RequestHandler):

    def initialize(self, mem_conf, static_conf):
        self.builder = InterestingFieldsBuilder(mem_conf, static_conf)

    async def get(self):
        """
        It writes response to remote side.
        :return: list of 4 timelines
        """
        params = self.request.query_arguments
        cid = params.get('cid')[0].decode()
        interesting_fields = self.builder.get_interesting_fields(cid)
        self.write(json.dumps(interesting_fields, default=str))  # to serialize timestamp