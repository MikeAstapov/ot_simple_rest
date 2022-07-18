from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict


class TimeIntervals:
    MINUTES = 0
    HOURS = 1
    DAYS = 2
    MONTHS = 3


class TimelinesBuilder:
    """
    The builder class is responsible for creating  a list of 4 timelines.
    One object is a pair (time, value) and represents a time interval.
    :time: - unix timestamp
    :value: - how many events happened during the time interval

    Timelines differ by their time interval:
    1st - 1 minute
    2nd - 1 hour
    3rd - 1 day
    4th - 1 month
    """

    MINUTES_IN_SECONDS = 60
    HOURS_IN_SECONDS = 3600
    DAYS_IN_SECONDS = 86400

    def __init__(self):
        self.intervals = {  # intervals for every timeline
            TimeIntervals.MINUTES: relativedelta(minutes=1),
            TimeIntervals.HOURS: relativedelta(hours=1),
            TimeIntervals.DAYS: relativedelta(days=1),
            TimeIntervals.MONTHS: relativedelta(months=1)
        }

    def get_all_timelines(self, data: List[int]) -> List[List[Dict[str, int]]]:
        """
        You can use get_<interval>_timeline one by one, but it will be around for times longer
        This method is optimized
        """

        timelines = [[], [], [], []]

        sorted_data = sorted(data)

        left_border = datetime.fromtimestamp(sorted_data[0])
        left_border_minutes = left_border.replace(second=0)
        left_border_hours = left_border_minutes.replace(minute=0)
        left_border_days = left_border_hours.replace(hour=0)
        left_border_months = left_border_days.replace(day=1)
        right_border_minutes = left_border_minutes + self.intervals[TimeIntervals.MINUTES]
        right_border_hours = left_border_hours + self.intervals[TimeIntervals.HOURS]
        right_border_days = left_border_days + self.intervals[TimeIntervals.DAYS]
        right_border_months = left_border_months + self.intervals[TimeIntervals.MONTHS]

        for elem in sorted_data:
            elem_date = datetime.fromtimestamp(elem)
            right_border_minutes = self._add_element(right_border_minutes,
                                                     timelines[TimeIntervals.MINUTES],
                                                     elem_date,
                                                     TimeIntervals.MINUTES)
            right_border_hours = self._add_element(right_border_hours,
                                                   timelines[TimeIntervals.HOURS],
                                                   elem_date,
                                                   TimeIntervals.HOURS)
            right_border_days = self._add_element(right_border_days,
                                                  timelines[TimeIntervals.DAYS],
                                                  elem_date,
                                                  TimeIntervals.DAYS)
            right_border_months = self._add_element(right_border_months,
                                                    timelines[TimeIntervals.MONTHS],
                                                    elem_date,
                                                    TimeIntervals.MONTHS)

        return timelines

    def _add_element(self, right_border: datetime, timeline: List[Dict[str, int]], elem_date: datetime, interval: int) \
            -> datetime:
        """Adds element to timeline and returns next right border"""
        if not timeline:
            timeline.append({'time': (right_border - self.intervals[interval]).timestamp(), 'value': 1})
            return right_border
        if elem_date < right_border:
            timeline[-1]['value'] += 1
            return right_border
        timeline.append({'time': right_border.timestamp(), 'value': 1})
        if interval is TimeIntervals.MINUTES:
            return elem_date.replace(second=0) + self.intervals[interval]
        if interval is TimeIntervals.HOURS:
            return elem_date.replace(minute=0, second=0) + self.intervals[interval]
        if interval is TimeIntervals.DAYS:
            return elem_date.replace(hour=0, minute=0, second=0) + self.intervals[interval]
        return elem_date.replace(day=1, hour=0, minute=0, second=0) + self.intervals[interval]

    def _get_one_timeline(self, data: List[int], left_border: datetime, interval: int) -> List[Dict[str, int]]:
        sorted_data = sorted(data)
        timeline = []
        right_border = left_border + self.intervals[interval]

        for elem in sorted_data:
            elem_date = datetime.fromtimestamp(elem)
            right_border = self._add_element(right_border,
                                             timeline,
                                             elem_date,
                                             interval)
        return timeline

    def get_minutes_timeline(self, data: List[int]) -> List[Dict[str, int]]:
        left_border = datetime.fromtimestamp(min(data)).replace(second=0)
        return self._get_one_timeline(data, left_border, TimeIntervals.MINUTES)

    def get_hours_timeline(self, data: List[int]) -> List[Dict[str, int]]:
        left_border = datetime.fromtimestamp(min(data)).replace(minute=0, second=0)
        return self._get_one_timeline(data, left_border, TimeIntervals.HOURS)

    def get_days_timeline(self, data: List[int]) -> List[Dict[str, int]]:
        left_border = datetime.fromtimestamp(min(data)).replace(hour=0, minute=0, second=0)
        return self._get_one_timeline(data, left_border, TimeIntervals.DAYS)

    def get_months_timeline(self, data: List[int]) -> List[Dict[str, int]]:
        left_border = datetime.fromtimestamp(min(data)).replace(day=1, hour=0, minute=0, second=0)
        return self._get_one_timeline(data, left_border, TimeIntervals.MONTHS)
