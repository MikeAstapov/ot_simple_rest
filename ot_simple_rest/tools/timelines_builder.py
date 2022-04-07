from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Tuple
import math

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
        # approximately self.point months in seconds to optimize (limit) json reading
        self.BIGGEST_INTERVAL = 86400 * 31 * self.points

    def fill_in_time(self, timelines: List[List[Dict[str, int]]], old_time: datetime, step: relativedelta, interval: int):
        for i in range(self.points):
            timelines[interval][i] = {'time': old_time.timestamp(), 'value': 0}
            old_time += step

    def fill_in_all_time(self, timelines: List, old_times: Tuple, intervals: Tuple):
        for interval in (TimeIntervals.MINUTES, TimeIntervals.HOURS, TimeIntervals.DAYS, TimeIntervals.MONTHS):
            self.fill_in_time(timelines, old_times[interval], intervals[interval], interval)

    def find_index_in_timeline(self, old_time: datetime, current_time: datetime, interval: int) -> int:
        if interval == TimeIntervals.MONTHS:
            return (current_time.year - old_time.year) * 12 + current_time.month - old_time.month
        if interval == TimeIntervals.DAYS:
            diff = current_time - old_time
            return int(diff.total_seconds() / 86400)
        if interval == TimeIntervals.HOURS:
            diff = current_time - old_time
            return int(diff.total_seconds() / 3600)
        if interval == TimeIntervals.MINUTES:
            diff = current_time - old_time
            return int(diff.total_seconds() / 60)

    def get_all_timelines(self, data: List[int], fresh_time: int) \
            -> List[List[Dict[str, int]]]:
        """
        When field is specified field value is accumulated for given interval rather than amount of events
        """
        fresh_time = datetime.fromtimestamp(fresh_time)
        timelines = [[0]*50, [0]*50, [0]*50, [0]*50]
        old_times = (
            fresh_time.replace(second=0) - relativedelta(minutes=self.points - 1),
            fresh_time.replace(minute=0, second=0) - relativedelta(hours=self.points - 1),
            fresh_time.replace(hour=0, minute=0, second=0) - relativedelta(days=self.points - 1),
            fresh_time.replace(day=1, hour=0, minute=0, second=0) - relativedelta(months=self.points - 1)
        )
        intervals = (
            relativedelta(minutes=1),
            relativedelta(hours=1),
            relativedelta(days=1),
            relativedelta(months=1)
        )
        self.fill_in_all_time(timelines, old_times, intervals)
        for elem in data:
            elem = datetime.fromtimestamp(elem)
            for interval in (TimeIntervals.MONTHS, TimeIntervals.DAYS, TimeIntervals.HOURS, TimeIntervals.MINUTES):
                if elem >= old_times[interval]:
                    try:
                        timelines[interval][self.find_index_in_timeline(old_times[interval], elem, interval)]['value'] += 1
                    except IndexError as e:
                        print(e)
                else:
                    break
        return timelines
