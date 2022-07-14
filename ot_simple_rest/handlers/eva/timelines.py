import json
from notifications.checker import NotificationChecker
from notifications.handlers import LimitedDataNotification
import tornado.web
from tools.timelines_builder import TimelinesBuilder
from tools.timelines_loader import TimelinesLoader
from tools.timelines_filterer import TimelinesFilterer
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

    def initialize(self, mem_conf: Dict, static_conf: Dict, notification_conf: Dict):
        self.loader = TimelinesLoader(mem_conf, static_conf)
        self.builder = TimelinesBuilder()

    async def get(self):
        params = self.request.query_arguments
        cid = params.get('cid')[0].decode()
        interval = params.get('interval')
        if interval:  # field is optional, by default all 4 timelines are returned
            interval = interval[0].decode()
        is_one_timeline: bool = interval and interval in {'minutes', 'hours', 'days', 'months'}

        try:
            data, fresh_time, total_lines = self.loader.load_data(cid)  # fresh_time indicates last time interval in all timelines
            if is_one_timeline:
                if interval == 'minutes':
                    response = self.builder.get_minutes_timeline(data, fresh_time)
                elif interval == 'hours':
                    response = self.builder.get_hours_timeline(data, fresh_time)
                elif interval == 'days':
                    response = self.builder.get_days_timeline(data, fresh_time)
                else:
                    response = self.builder.get_months_timeline(data, fresh_time)
            else:
                response = self.builder.get_all_timelines(data, fresh_time)
        except tornado.web.HTTPError as e:
            return self.write(json.dumps({'status': 'failed', 'error': e}, default=str))
        except KeyError:
            return self.write(json.dumps({'status': 'failed', 'error': "'_time' column is missing"}, default=str))
        except Exception as e:
            return self.write(json.dumps({'status': 'failed', 'error': f'{e} cid {cid}'}, default=str))

        if is_one_timeline:
            self.write(json.dumps(TimelinesFilterer.remove_empty_intervals(response)))
        else:
            self.write(json.dumps(TimelinesFilterer.remove_empty_intervals_many_timelines(response)))
