import os
import logging
import json
import datetime
import tornado.web
from datetime import datetime
# import pytz # $ pip install pytz

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
        self.mem_conf = mem_conf
        self.static_conf = static_conf
        self.data_path = self.mem_conf['path']
        self.logger = logging.getLogger('osr')
        self._cache_name_template = 'search_{}.cache/data'
        self.INTERVALS = {'m': 60, 'h': 3600, 'd': 86400}
        self.points = 50  # how many point on the timeline?
        # self.TIME_ZONE = 'Europe/Moscow'

    def get_timeline(self, data, interval):
        timeline = []
        if not data:
            return timeline
        if interval == self.INTERVALS['m']:
            tformat = '%Y-%m-%d %H:%M'
        elif interval == self.INTERVALS['h']:
            tformat = '%Y-%m-%d %H'
        else:
            tformat = '%Y-%m-%d'
        # dropping to biggest value (seconds to minutes, minutes to hours etc.)
        last_time = data[-1]['_time'] // interval * interval
        accumulated_value = 0
        for d in reversed(data):
            if d['_time'] >= last_time:
                accumulated_value += d['value']
            else:
                t = datetime.utcfromtimestamp(last_time).strftime(tformat)  # TODO set timezone
                timeline.append({'time': t, 'value': accumulated_value})
                accumulated_value = 0
                last_time -= interval
                accumulated_value += d['value']
        t = datetime.utcfromtimestamp(last_time).strftime(tformat)  # TODO set timezone
        timeline.append({'time': t, 'value': accumulated_value})
        while len(timeline) < self.points:
            last_time -= interval
            t = datetime.utcfromtimestamp(last_time).strftime(tformat)  # TODO set timezone
            timeline.append({'time': t, 'value': 0})
        return list(reversed(timeline[:self.points]))

    @staticmethod
    def feb_days(ts):
        y = int(datetime.utcfromtimestamp(ts).strftime('%Y'))
        if (y % 4 == 0 and not y % 100 == 0) or y % 400 == 0:
            return 29
        return 28


    def get_months_timeline(self, data):
        timeline = []
        if not data:
            return timeline
        tformat = '%Y-%m'
        months = [31, self.feb_days(data[-1]['_time']), 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        last_time = data[-1]['_time'] // (sum(months) * self.INTERVALS['d']) * (sum(months) * self.INTERVALS['d'])
        accumulated_value = 0



    async def get(self):
        """
        It writes response to remote side.

        :return:
        """
        timelines = [None] * 4
        params = self.request.query_arguments
        cid = params.get('cid')[0].decode()
        data = self.load_data_test(cid)
        timelines[0] = self.get_timeline(data, self.INTERVALS['m'])
        timelines[1] = self.get_timeline(data, self.INTERVALS['h'])
        timelines[2] = self.get_timeline(data, self.INTERVALS['d'])
        timelines[3] = self.get_months_timeline(data)
        self.write(json.dumps(timelines))

    def load_data(self, cid):
        """
        Load data by cid

        :param cid:         OT_Dispatcher's job cid
        :return:
        """
        data = []
        self.logger.debug(f'Started loading cache {cid}.')
        path_to_cache_dir = os.path.join(self.data_path, self._cache_name_template.format(cid))
        self.logger.debug(f'Path to cache {path_to_cache_dir}.')
        file_names = [file_name for file_name in os.listdir(path_to_cache_dir) if file_name[-5:] == '.json']
        for file_name in file_names:
            self.logger.debug(f'Reading part: {file_name}')
            with open(os.path.join(path_to_cache_dir, file_name)) as fr:
                for line in fr:
                    data.append(json.loads(line))
        return data

    def load_data_test(self, cid):
        """
        Load data by cid

        :param cid:         OT_Dispatcher's job cid
        :return:
        """
        data = []
        with open('/home/ilya/Desktop/ot_simple_rest/ot_simple_rest/test.json') as fr:
            for line in fr:
                data.append(json.loads(line))  # all in one
        return data
