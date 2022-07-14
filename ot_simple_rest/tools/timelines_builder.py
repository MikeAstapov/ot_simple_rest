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
        self.intervals = {  # intervals for every timeline
            TimeIntervals.MINUTES: relativedelta(minutes=1),
            TimeIntervals.HOURS: relativedelta(hours=1),
            TimeIntervals.DAYS: relativedelta(days=1),
            TimeIntervals.MONTHS: relativedelta(months=1)
        }

    def fill_in_time(
            self, timeline: List[Dict[str, int]], old_time: datetime, step: relativedelta):
        """fills in time for a single timeline"""
        for i in range(self.points):
            timeline[i].update(time=old_time.timestamp(), value=0)
            old_time += step

    def fill_in_all_time(self, timelines: List, old_times: Tuple):
        """fills in time for every timeline"""
        for interval in self.intervals.keys():
            self.fill_in_time(timelines[interval], old_times[interval], self.intervals[interval])

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

    def get_all_timelines(self, data: List[int], fresh_time: int) -> List[List[Dict[str, int]]]:

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
        self.fill_in_all_time(timelines, old_times)
        for elem in data:
            elem = datetime.fromtimestamp(elem)
            # list transformation is required cause in python 3.6 dict object doesn't implement __reversed__
            for interval in reversed(list(self.intervals.keys())):
                if elem >= old_times[interval]:
                    timelines[interval][self.find_index_in_timeline(old_times[interval], elem, interval)]['value'] += 1
                else:
                    break  # there's no point to check days timeline if months timeline doesn't pass condition
        return timelines

    def get_one_timeline(self, data: List[int], old_time: datetime, step: int) -> List[Dict[str, int]]:
        timeline = [{} for _ in range(self.points)]
        interval = self.intervals[step]
        self.fill_in_time(timeline, old_time, interval)
        for elem in data:
            elem = datetime.fromtimestamp(elem)
            if elem >= old_time:
                timeline[self.find_index_in_timeline(old_time, elem, TimeIntervals.MINUTES)]['value'] += 1
            else:
                break
        return timeline

    def get_minutes_timeline(self, data: List[int], fresh_time: int) -> List[Dict[str, int]]:
        fresh_time = datetime.fromtimestamp(fresh_time)
        old_time = fresh_time.replace(second=0) - relativedelta(minutes=self.points - 1)  # oldest time
        return self.get_one_timeline(data, old_time, TimeIntervals.MINUTES)

    def get_hours_timeline(self, data: List[int], fresh_time: int) -> List[Dict[str, int]]:
        fresh_time = datetime.fromtimestamp(fresh_time)
        old_time = fresh_time.replace(minute=0, second=0) - relativedelta(hours=self.points - 1)  # oldest time
        return self.get_one_timeline(data, old_time, TimeIntervals.HOURS)

    def get_days_timeline(self, data: List[int], fresh_time: int) -> List[Dict[str, int]]:
        fresh_time = datetime.fromtimestamp(fresh_time)
        old_time = fresh_time.replace(hour=0, minute=0, second=0) - relativedelta(days=self.points - 1)  # oldest time
        return self.get_one_timeline(data, old_time, TimeIntervals.DAYS)

    def get_months_timeline(self, data: List[int], fresh_time: int) -> List[Dict[str, int]]:
        fresh_time = datetime.fromtimestamp(fresh_time)
        old_time = fresh_time.replace(day=1, hour=0, minute=0, second=0) - relativedelta(months=self.points - 1)  # oldest time
        return self.get_one_timeline(data, old_time, TimeIntervals.MONTHS)
