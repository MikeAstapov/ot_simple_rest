from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Tuple


class TimeIntervals:
    MINUTES = 0
    HOURS = 1
    DAYS = 2
    MONTHS = 3


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
        self.points = 50  # how many points on the timeline

    def fill_in_time(
            self, timelines: List[List[Dict[str, int]]], old_time: datetime, step: relativedelta, interval: int):
        """fills in time for a single timeline"""
        for i in range(self.points):
            timelines[interval][i].update(time=old_time.timestamp(), value=0)
            old_time += step

    def fill_in_all_time(self, timelines: List, old_times: Tuple, intervals: Tuple):
        """fills in time for every timeline"""
        for interval in (TimeIntervals.MINUTES, TimeIntervals.HOURS, TimeIntervals.DAYS, TimeIntervals.MONTHS):
            self.fill_in_time(timelines, old_times[interval], intervals[interval], interval)

    @staticmethod
    def find_index_in_timeline(old_time: datetime, current_time: datetime, interval: int) -> int:
        """returns position of timeinterval in a given timeline"""
        if interval == TimeIntervals.MONTHS:
            return (current_time.year - old_time.year) * 12 + current_time.month - old_time.month
        diff = current_time - old_time
        if interval == TimeIntervals.DAYS:
            return int(diff.total_seconds() / 86400)
        if interval == TimeIntervals.HOURS:
            return int(diff.total_seconds() / 3600)
        if interval == TimeIntervals.MINUTES:
            return int(diff.total_seconds() / 60)

    def get_all_timelines(self, data: List[int], fresh_time: int) \
            -> List[List[Dict[str, int]]]:

        fresh_time = datetime.fromtimestamp(fresh_time)
        timelines = [
            [{} for _ in range(self.points)],
            [{} for _ in range(self.points)],
            [{} for _ in range(self.points)],
            [{} for _ in range(self.points)]
        ]
        old_times = (  # oldest time for every timeline
            fresh_time.replace(second=0) - relativedelta(minutes=self.points - 1),
            fresh_time.replace(minute=0, second=0) - relativedelta(hours=self.points - 1),
            fresh_time.replace(hour=0, minute=0, second=0) - relativedelta(days=self.points - 1),
            fresh_time.replace(day=1, hour=0, minute=0, second=0) - relativedelta(months=self.points - 1)
        )
        intervals = (  # intervals for every timeline
            relativedelta(minutes=1),
            relativedelta(hours=1),
            relativedelta(days=1),
            relativedelta(months=1)
        )
        self.fill_in_all_time(timelines, old_times, intervals)
        for elem in data:
            elem = datetime.fromtimestamp(elem)
            # there's no point to check days timeline if months timeline doesn't pass condition
            for interval in (TimeIntervals.MONTHS, TimeIntervals.DAYS, TimeIntervals.HOURS, TimeIntervals.MINUTES):
                if elem >= old_times[interval]:
                    timelines[interval][self.find_index_in_timeline(old_times[interval], elem, interval)]['value'] += 1
                else:
                    break
        return timelines
