import json
import tornado.web
from tools.timelines_builder import TimelinesBuilder
from tools.timelines_loader import TimelinesLoader

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
        self.builder = TimelinesBuilder()
        self.loader = TimelinesLoader(mem_conf, static_conf, self.builder.BIGGEST_INTERVAL)

    async def get(self):
        """
        It writes response to remote side.
        :return: list of 4 timelines
        """
        params = self.request.query_arguments
        cid = params.get('cid')[0].decode()
        try:
            data = self.loader.load_data(cid)
        except Exception as e:
            return self.write({'status': 'failed', 'error': e})
        timelines = self.builder.get_all_timelines(data)
        self.write(json.dumps(timelines))
