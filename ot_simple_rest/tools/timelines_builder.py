from datetime import datetime
from .base_builder import BaseBuilder
import json
import os
from file_read_backwards import FileReadBackwards
# from dateutil.relativedelta import relativedelta
# import pytz # $ pip install pytz


class TimelinesBuilder(BaseBuilder):

    def __init__(self, mem_conf, static_conf):
        super().__init__(mem_conf, static_conf)
        self.INTERVALS = {'m': 60, 'h': 3600, 'd': 86400, 'M': -1}  # -1 is a signal for getter to count month interval
        self._interval_info = None
        self._last_time = None
        self._last_month = None
        self._months = None
        self.points = 50  # how many point on the timeline
        # approximately self.point months in seconds to optimize (limit) json reading
        self.BIGGEST_INTERVAL = self.INTERVALS['d'] * 31 * self.points
        # self.TIME_ZONE = 'Europe/Moscow'  # TODO set timezone utcfromtimestamp?

    def _load_data(self, cid):
        """
        Load data by cid

        :param cid:         OT_Dispatcher's job cid
        :return:            list of dicts from json lines
        """
        data = []
        last_time = None
        time_to_break = False
        self.logger.debug(f'Started loading cache {cid}.')
        path_to_cache_dir = os.path.join(self.data_path, self._cache_name_template.format(cid))
        self.logger.debug(f'Path to cache {path_to_cache_dir}.')
        file_names = [file_name for file_name in os.listdir(path_to_cache_dir) if file_name[-5:] == '.json']
        for file_name in file_names:
            if time_to_break:
                break
            self.logger.debug(f'Reading part: {file_name}')
            with FileReadBackwards(os.path.join(path_to_cache_dir, file_name)) as fr:
                for line in fr:
                    tmp = json.loads(line)
                    if last_time:
                        if last_time - tmp['_time'] > self.BIGGEST_INTERVAL:
                            time_to_break = True
                            break
                    else:
                        last_time = tmp['_time']
                    data.append(tmp)
        return data  # is not reversed intentionally. This way it is easier to build a timeline

    def _load_data_test(self, data_path):
        data = []
        last_time = None
        with FileReadBackwards(data_path) as fr:
            for line in fr:
                tmp = json.loads(line)
                if last_time:
                    if last_time - tmp['_time'] > self.BIGGEST_INTERVAL:
                        break
                else:
                    last_time = tmp['_time']
                data.append(tmp)
        return data  # is not reversed intentionally. This way it is easier to build a timeline

    @staticmethod
    def _convert_hours_am_pm_format(hour: str) -> (str, str):
        hour = int(hour)
        am_or_pm = 'AM'
        if hour > 12:
            hour = hour % 12
            am_or_pm = 'PM'
            if hour < 10:
                hour = f'0{hour}'
        return hour, am_or_pm

    def _set_timeformat(self, interval) -> str:
        if interval == self.INTERVALS['m']:
            return '%Y-%m-%d %H:%M'
        if interval == self.INTERVALS['h']:
            return '%Y-%m-%d %H'
        return '%Y-%m-%d'

    def _round_timestamp(self, last_time, interval):
        if interval == self.INTERVALS['m']:
            last_time = last_time.replace(second=0, microsecond=0)
        elif interval == self.INTERVALS['h']:
            last_time = last_time.replace(minute=0, second=0, microsecond=0)
        elif interval == self.INTERVALS['d']:
            last_time = last_time.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            last_time = last_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return last_time.timestamp()

    @property
    def interval(self):
        if self._interval_info != self.INTERVALS['M']:
            return self._interval_info
        self._last_month = self._calculate_month(self._last_month, self._months, self._last_time)
        return self._months[self._last_month] * self.INTERVALS['d']

    def get_timeline(self, data, interval, field: [None, str] = None):
        """
        When field is specified field value is accumulated for given interval rather than amount of events
        """
        timeline = []
        # select interval
        if not data:
            return timeline
        self._interval_info = interval
        if self._interval_info == self.INTERVALS['M']:
            self._months = [31, self._feb_days(data[0]['_time']), 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
            self._last_month = datetime.fromtimestamp(data[0]['_time']).month - 1  # month index
        # round last time to interval
        self._last_time = datetime.fromtimestamp(data[0]['_time'])
        self._last_time = self._round_timestamp(self._last_time, self._interval_info)
        accumulated_value = 0
        i = 0
        while i < len(data) and len(timeline) < self.points:
            # accumulate values for given interval
            if data[i]['_time'] >= self._last_time:
                if field:
                    accumulated_value += data[i][field]
                else:
                    accumulated_value += 1
                i += 1
            # flush accumulated values and interval to the timeline
            elif self._last_time - self.interval <= data[i]['_time'] < self._last_time or accumulated_value:
                timeline.append({'time': self._last_time, 'value': accumulated_value})
                accumulated_value = 0
                self._last_time -= self.interval
            # move to interval in which current time is located and set 0 values to intervals on the way
            else:
                while data[i]['_time'] < self._last_time - self.interval and len(timeline) < self.points:
                    timeline.append({'time': self._last_time, 'value': 0})
                    self._last_time -= self.interval
        if accumulated_value:
            timeline.append({'time': self._last_time, 'value': accumulated_value})
            self._last_time -= self.interval
        # when data ended but timeline does not have 50 values
        while len(timeline) < self.points:
            timeline.append({'time': self._last_time, 'value': 0})
            self._last_time -= self.interval
        return list(reversed(timeline))

    @staticmethod
    def _feb_days(ts):
        y = datetime.fromtimestamp(ts).year
        if (y % 4 == 0 and not y % 100 == 0) or y % 400 == 0:
            return 29
        return 28

    def _calculate_month(self, last_month, months, ts):
        if last_month > 0:
            last_month -= 1
        else:
            last_month = 11  # last month of the year
            months = [31, self._feb_days(ts), 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]  # mutating months list
        return last_month

    def get_all_timelines(self, cid, field: [None, str] = None):
        """
        When field is specified field value is accumulated for given interval rather than amount of events
        """
        timelines = [None] * 4
        data = self._load_data(cid)
        timelines[0] = self.get_timeline(data, self.INTERVALS['m'], field)
        timelines[1] = self.get_timeline(data, self.INTERVALS['h'], field)
        timelines[2] = self.get_timeline(data, self.INTERVALS['d'], field)
        timelines[3] = self.get_timeline(data, self.INTERVALS['M'], field)
        return timelines
