from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Optional, Union


class TimelinesBuilder:
    """
    The builder class is responsible for creating  a list of 4 timelines.
    Every timeline has 50 objects. One object is a pair (time, value) and represents a time interval.
    :time: - unix timestamp
    :value: - how many events happened during the time interval

    Timelines differ by their time interval:
    1st - 1 minute
    2nd - 1 hour
    3rd - 1 day
    4th - 1 month
    """

    def __init__(self):
        self.INTERVALS = {'m': 60, 'h': 3600, 'd': 86400, 'M': -1}  # -1 is a signal for getter to count month interval
        self._interval_info = None
        self._last_time = None
        self.points = 50  # how many points on the timeline
        # approximately self.point months in seconds to optimize (limit) json reading
        self.BIGGEST_INTERVAL = self.INTERVALS['d'] * 31 * self.points

    def _round_timestamp(self, last_time: datetime, interval: int) -> datetime:
        """
        >>> tlb = TimelinesBuilder()
        >>> dt = datetime(2007, 12, 31, 4, 19, 37)
        >>> tlb._round_timestamp(dt, tlb.INTERVALS['m'])
        datetime.datetime(2007, 12, 31, 4, 19)
        >>> tlb._round_timestamp(dt, tlb.INTERVALS['h'])
        datetime.datetime(2007, 12, 31, 4, 0)
        >>> tlb._round_timestamp(dt, tlb.INTERVALS['d'])
        datetime.datetime(2007, 12, 31, 0, 0)
        >>> tlb._round_timestamp(dt, tlb.INTERVALS['M'])
        datetime.datetime(2007, 12, 1, 0, 0)
        """
        if interval == self.INTERVALS['m']:
            last_time = last_time.replace(second=0, microsecond=0)
        elif interval == self.INTERVALS['h']:
            last_time = last_time.replace(minute=0, second=0, microsecond=0)
        elif interval == self.INTERVALS['d']:
            last_time = last_time.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            last_time = last_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return last_time

    @property
    def interval(self) -> relativedelta:
        if self._interval_info == self.INTERVALS['m']:
            return relativedelta(minutes=1)
        if self._interval_info == self.INTERVALS['h']:
            return relativedelta(hours=1)
        if self._interval_info == self.INTERVALS['d']:
            return relativedelta(days=1)
        return relativedelta(months=1)

    def get_timeline(self, data: List[Dict], interval: int, field: Optional[str] = None) \
            -> List[Dict[str, Union[int, float]]]:
        """
        When field is specified field value is accumulated for given interval rather than amount of events
        """
        timeline = []
        # select interval
        if not data:
            raise Exception('Empty data')
        self._interval_info = interval
        self._last_time = datetime.fromtimestamp(data[0]['_time'])
        self._last_time = self._round_timestamp(self._last_time, self._interval_info)
        accumulated_value = 0
        i = 0
        while i < len(data) and len(timeline) < self.points:
            # accumulate values for given interval
            if datetime.fromtimestamp(data[i]['_time']) >= self._last_time:
                if field:
                    accumulated_value += data[i][field]
                else:
                    accumulated_value += 1
                i += 1
            # flush accumulated values and interval to the timeline
            elif self._last_time - self.interval <= datetime.fromtimestamp(data[i]['_time']) < self._last_time:
                timeline.append({'time': self._last_time.timestamp(), 'value': accumulated_value})
                accumulated_value = 0
                self._last_time -= self.interval
            # move to interval in which current time is located and set 0 values to intervals on the way
            else:
                if accumulated_value:
                    timeline.append({'time': self._last_time.timestamp(), 'value': accumulated_value})
                    accumulated_value = 0
                while datetime.fromtimestamp(data[i]['_time']) < self._last_time - self.interval \
                        and len(timeline) < self.points:
                    timeline.append({'time': self._last_time.timestamp(), 'value': 0})
                    self._last_time -= self.interval
        if accumulated_value:
            timeline.append({'time': self._last_time.timestamp(), 'value': accumulated_value})
            self._last_time -= self.interval
        # when data ended but timeline does not have 50 values
        while len(timeline) < self.points:
            timeline.append({'time': self._last_time.timestamp(), 'value': 0})
            self._last_time -= self.interval
        return list(reversed(timeline))

    def get_all_timelines(self, data: List[Dict], field: Optional[str] = None) \
            -> List[List[Dict[str, Union[int, float]]]]:
        """
        When field is specified field value is accumulated for given interval rather than amount of events
        """
        timelines = [self.get_timeline(data, self.INTERVALS['m'], field),
                     self.get_timeline(data, self.INTERVALS['h'], field),
                     self.get_timeline(data, self.INTERVALS['d'], field),
                     self.get_timeline(data, self.INTERVALS['M'], field)]
        return timelines
