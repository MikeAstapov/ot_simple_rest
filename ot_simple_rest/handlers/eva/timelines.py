import json
import tornado.web
from tools.timelines_builder import TimelinesBuilder
from tools.timelines_loader import TimelinesLoader
from typing import Dict

__author__ = "Ilia Sagaidak"
__copyright__ = "Copyright 2022, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Ilia Sagaidak"
__email__ = "isagaidak@isgneuro.com"
__status__ = "Dev"


class GetTimelines(tornado.web.RequestHandler):
    """
    Returns a list of 4 timelines. Every timeline has 50 objects. One object is a pair (time, value) and represents
    a time interval.
    :time: - unix timestamp
    :value: - how many events happened during the time interval

    Timelines differ by their time interval:
    1st - 1 minute
    2nd - 1 hour
    3rd - 1 day
    4th - 1 month
    """

    def initialize(self, mem_conf: Dict, static_conf: Dict):
        self.builder = TimelinesBuilder()
        self.loader = TimelinesLoader(mem_conf, static_conf, self.builder.BIGGEST_INTERVAL)

    async def get(self):
        params = self.request.query_arguments
        cid = params.get('cid')[0].decode()
        try:
            data = self.loader.load_data(cid)
            timelines = self.builder.get_all_timelines(data)
        except tornado.web.HTTPError as e:
            return self.write({'status': 'failed', 'error': e})
        except Exception as e:
            return self.write({'status': 'failed', 'error': f'{e} cid {cid}'})
        self.write(json.dumps(timelines))
