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
        self.INTERVALS = {'m': 60, 'h': 3600, 'd': 86400}
        self.points = 50  # how many point on the timeline
        # approximately self.point months in seconds to optimize (limit) json reading
        self.BIGGEST_INTERVAL = self.INTERVALS['d'] * 31 * self.points
        self.MONTH_NAMES = {
            '01': 'января',
            '02': 'февраля',
            '03': 'марта',
            '04': 'апреля',
            '05': 'мая',
            '06': 'июня',
            '07': 'июля',
            '08': 'августа',
            '09': 'сентября',
            '10': 'октября',
            '11': 'ноября',
            '12': 'декабря'
        }
        self.MONTH_ONLY_NAMES = {
            '01': 'январь',
            '02': 'февраль',
            '03': 'март',
            '04': 'апрель',
            '05': 'май',
            '06': 'июнь',
            '07': 'июль',
            '08': 'август',
            '09': 'сентябрь',
            '10': 'октябрь',
            '11': 'ноябрь',
            '12': 'декабрь'
        }
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
        data.reverse()
        return data

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
        data.reverse()
        return data

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

    def _format_time(self, date, tformat):
        if tformat == '%Y-%m':
            y, m = date.split('-')
            return f'{self.MONTH_ONLY_NAMES[m]} {y}'  # январь 2018
        if tformat == '%Y-%m-%d':
            y, m, d = date.split('-')
            return f'{d} {self.MONTH_NAMES[m]} {y}'  # 19 января 2018
        if tformat == '%Y-%m-%d %H':
            ymd, hour = date.split()
            y, m, d = ymd.split('-')
            hour, am_or_pm = self._convert_hours_am_pm_format(hour)
            return f'{hour} {am_or_pm} - {d} {self.MONTH_NAMES[m]} {y}'  # 2 PM - 19 января 2018
        ymd, hm = date.split()
        y, m, d = ymd.split('-')
        hour, minutes = hm.split(':')
        hour, am_or_pm = self._convert_hours_am_pm_format(hour)
        return f'{hour}:{minutes} {am_or_pm} - {d} {self.MONTH_NAMES[m]} {y}'  # 4:51 PM - 19 января 2018

    def _set_timeformat(self, interval) -> str:
        if interval == self.INTERVALS['m']:
            return '%Y-%m-%d %H:%M'
        if interval == self.INTERVALS['h']:
            return '%Y-%m-%d %H'
        return '%Y-%m-%d'

    def _round_timestamp(self, last_time, interval):
        if interval == self.INTERVALS['m']:
            last_time = last_time.replace(second=0, microsecond=0)
            return last_time.timestamp()
        if interval == self.INTERVALS['h']:
            last_time = last_time.replace(minute=0, second=0, microsecond=0)
            return last_time.timestamp()
        last_time = last_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return last_time.timestamp()

    def get_timeline(self, data, interval):
        timeline = []
        # select interval
        if not data:
            return timeline
        tformat = self._set_timeformat(interval)
        # round last time to interval
        last_time = datetime.fromtimestamp(data[-1]['_time'])
        last_time = self._round_timestamp(last_time, interval)
        accumulated_value = 0
        i = len(data) - 1
        while i >= 0 and len(timeline) < self.points:
            # accumulate values for given interval
            if data[i]['_time'] >= last_time:
                accumulated_value += 1
                i -= 1
            # flush accumulated values and interval to the timeline
            elif last_time - interval <= data[i]['_time'] < last_time or accumulated_value:
                t = self._format_time(datetime.fromtimestamp(last_time).strftime(tformat), tformat)
                timeline.append({'time': t, 'value': accumulated_value})
                accumulated_value = 0
                last_time -= interval
            # move to interval in which current time is located and set 0 values to intervals on the way
            else:
                while data[i]['_time'] < last_time - interval and len(timeline) < self.points:
                    t = self._format_time(datetime.fromtimestamp(last_time).strftime(tformat), tformat)
                    timeline.append({'time': t, 'value': 0})
                    last_time -= interval
        if accumulated_value:
            t = self._format_time(datetime.fromtimestamp(last_time).strftime(tformat), tformat)
            timeline.append({'time': t, 'value': accumulated_value})
            last_time -= interval
        # when data ended but timeline does not have 50 values
        while len(timeline) < self.points:
            t = self._format_time(datetime.fromtimestamp(last_time).strftime(tformat), tformat)
            timeline.append({'time': t, 'value': 0})
            last_time -= interval
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

    def get_months_timeline(self, data):
        timeline = []
        if not data:
            return timeline
        tformat = '%Y-%m'
        months = [31, self._feb_days(data[-1]['_time']), 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        last_month = datetime.fromtimestamp(data[-1]['_time']).month - 1  # month index
        # round last time to interval
        last_time = datetime.fromtimestamp(data[-1]['_time'])
        last_time = last_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_time = last_time.timestamp()
        accumulated_value = 0
        i = len(data) - 1
        while i >= 0 and len(timeline) < self.points:
            # accumulate values for given interval
            if data[i]['_time'] >= last_time:
                accumulated_value += 1
                i -= 1
            # flush accumulated values and interval to the timeline
            elif last_time - months[last_month] * self.INTERVALS['d'] <= data[i]['_time'] < last_time or accumulated_value:
                t = self._format_time(datetime.fromtimestamp(last_time).strftime(tformat), tformat)
                timeline.append({'time': t, 'value': accumulated_value})
                accumulated_value = 0
                last_month = self._calculate_month(last_month, months, data[i]['_time'])
                last_time -= months[last_month] * self.INTERVALS['d']
            # move to interval in which current time is located and set 0 values to intervals on the way
            else:
                while data[i]['_time'] < last_time - months[last_month] * self.INTERVALS['d'] and len(timeline) < self.points:
                    t = self._format_time(datetime.fromtimestamp(last_time).strftime(tformat), tformat)
                    timeline.append({'time': t, 'value': 0})
                    last_month = self._calculate_month(last_month, months, data[i]['_time'])
                    last_time -= months[last_month] * self.INTERVALS['d']
        if accumulated_value:
            t = self._format_time(datetime.fromtimestamp(last_time).strftime(tformat), tformat)
            timeline.append({'time': t, 'value': accumulated_value})
            last_month = self._calculate_month(last_month, months, last_time)
            last_time -= months[last_month] * self.INTERVALS['d']
        # when data ended but timeline does not have 50 values
        while len(timeline) < self.points:
            last_month = self._calculate_month(last_month, months, last_time)
            last_time -= months[last_month] * self.INTERVALS['d']
            t = self._format_time(datetime.fromtimestamp(last_time).strftime(tformat), tformat)
            timeline.append({'time': t, 'value': 0})
        return list(reversed(timeline))

    def get_all_timelines(self, cid):
        timelines = [None] * 4
        data = self._load_data(cid)
        timelines[0] = self.get_timeline(data, self.INTERVALS['m'])
        timelines[1] = self.get_timeline(data, self.INTERVALS['h'])
        timelines[2] = self.get_timeline(data, self.INTERVALS['d'])
        timelines[3] = self.get_months_timeline(data)
        return timelines

    def test_get_all_timelines(self, data_path):
        timelines = [None] * 4
        data = self._load_data_test(data_path)
        timelines[0] = self.get_timeline(data, self.INTERVALS['m'])
        timelines[1] = self.get_timeline(data, self.INTERVALS['h'])
        timelines[2] = self.get_timeline(data, self.INTERVALS['d'])
        timelines[3] = self.get_months_timeline(data)
        return timelines
