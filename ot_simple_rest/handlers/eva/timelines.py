import json
import tornado.web
from tools.timelines_builder import TimelinesBuilder

__author__ = "Ilia Sagaidak"
__copyright__ = "Copyright 2022, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Ilia Sagaidak"
__email__ = "isagaidak@isgneuro.com"
__status__ = "Dev"


class GetTimelines(tornado.web.RequestHandler):

    def initialize(self, mem_conf, static_conf):
        self.builder = TimelinesBuilder(mem_conf, static_conf)

    async def get(self):
        """
        It writes response to remote side.
        :return: list of 4 timelines
        """
        params = self.request.query_arguments
        cid = params.get('cid')[0].decode()
        # timelines = self.builder.test_get_all_timelines()  # for testing with tools/test_timelines_builder.json
        timelines = self.builder.get_all_timelines(cid)
        self.write(json.dumps(timelines))


