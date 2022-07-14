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

    def fill_in_time(
            self, timeline: List[Dict[str, int]], oldest_time: datetime, step: relativedelta):
        """fills in time for a single timeline"""
        for i in range(len(timeline)):
            timeline[i].update(time=oldest_time.timestamp(), value=0)
            oldest_time += step

    def fill_in_all_time(self, timelines: List, oldest_time: datetime):
        """fills in time for every timeline"""
        for interval in self.intervals.keys():
            self.fill_in_time(timelines[interval], oldest_time, self.intervals[interval])

    def find_index_in_timeline(self, oldest_time: datetime, current_time: datetime, interval: int) -> int:
        """returns position of timeinterval in a given timeline"""
        if interval == TimeIntervals.MONTHS:
            return (current_time.year - oldest_time.year) * 12 + current_time.month - oldest_time.month
        diff = current_time - oldest_time
        if interval == TimeIntervals.DAYS:
            return int(diff.total_seconds() / self.DAYS_IN_SECONDS)
        if interval == TimeIntervals.HOURS:
            return int(diff.total_seconds() / self.HOURS_IN_SECONDS)
        if interval == TimeIntervals.MINUTES:
            return int(diff.total_seconds() / self.MINUTES_IN_SECONDS)

    def get_all_timelines(self, data: List[int]) -> List[List[Dict[str, int]]]:

        newest_time = datetime.fromtimestamp(max(data))
        oldest_time = datetime.fromtimestamp(min(data))
        delta = newest_time - oldest_time

        timelines = [
            [{} for _ in range(int(delta.total_seconds() / self.MINUTES_IN_SECONDS) + 1)],
            [{} for _ in range(int(delta.total_seconds() / self.HOURS_IN_SECONDS) + 1)],
            [{} for _ in range(int(delta.total_seconds() / self.DAYS_IN_SECONDS) + 1)],
            [{} for _ in range((newest_time.year - oldest_time.year) * 12 + newest_time.month - oldest_time.month + 1)]
        ]

        self.fill_in_all_time(timelines, oldest_time)
        for elem in data:
            elem = datetime.fromtimestamp(elem)
            for interval in self.intervals.keys():
                timelines[interval][self.find_index_in_timeline(oldest_time, elem, interval)]['value'] += 1
        return timelines

    def get_one_timeline(self, data: List[int], oldest_time: datetime, step: int, timeline: List) -> List[Dict[str, int]]:
        interval = self.intervals[step]
        self.fill_in_time(timeline, oldest_time, interval)
        for elem in data:
            elem = datetime.fromtimestamp(elem)
            timeline[self.find_index_in_timeline(oldest_time, elem, step)]['value'] += 1
        return timeline

    def get_minutes_timeline(self, data: List[int]) -> List[Dict[str, int]]:
        newest_time = datetime.fromtimestamp(max(data))
        oldest_time = datetime.fromtimestamp(min(data))
        delta = newest_time - oldest_time
        timeline = [{} for _ in range(int(delta.total_seconds() / self.MINUTES_IN_SECONDS) + 1)]
        return self.get_one_timeline(data, oldest_time, TimeIntervals.MINUTES, timeline)

    def get_hours_timeline(self, data: List[int]) -> List[Dict[str, int]]:
        newest_time = datetime.fromtimestamp(max(data))
        oldest_time = datetime.fromtimestamp(min(data))
        delta = newest_time - oldest_time
        timeline = [{} for _ in range(int(delta.total_seconds() / self.HOURS_IN_SECONDS) + 1)]
        return self.get_one_timeline(data, oldest_time, TimeIntervals.HOURS, timeline)

    def get_days_timeline(self, data: List[int]) -> List[Dict[str, int]]:
        newest_time = datetime.fromtimestamp(max(data))
        oldest_time = datetime.fromtimestamp(min(data))
        delta = newest_time - oldest_time
        timeline = [{} for _ in range(int(delta.total_seconds() / self.DAYS_IN_SECONDS) + 1)]
        return self.get_one_timeline(data, oldest_time, TimeIntervals.DAYS, timeline)

    def get_months_timeline(self, data: List[int]) -> List[Dict[str, int]]:
        newest_time = datetime.fromtimestamp(max(data))
        oldest_time = datetime.fromtimestamp(min(data))
        timeline = [{} for _ in range((newest_time.year - oldest_time.year) * 12 + newest_time.month - oldest_time.month + 1)]
        return self.get_one_timeline(data, oldest_time, TimeIntervals.MONTHS, timeline)
