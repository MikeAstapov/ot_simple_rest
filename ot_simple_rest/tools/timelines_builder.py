from datetime import datetime
from dateutil import tz
from collections import Counter
from typing import List, Dict


class TimeIntervals:
    MINUTES = 0
    HOURS = 1
    DAYS = 2
    MONTHS = 3


class Interval:
    def __init__(self, timestamp: float):
        self._dt: datetime = self._set_interval(
            datetime.fromtimestamp(timestamp, tz=tz.UTC)
        )

    def _set_interval(self, dt: datetime):
        """
        Calculates datetime of interval beginning
        """
        raise NotImplementedError

    def as_timestamp(self) -> float:
        """
        Returns timestamp of beginning
        """
        return self._dt.timestamp()

    def __hash__(self):
        return self._dt.__hash__()

    def __lt__(self, other):
        return self._dt < other._dt

    def __le__(self, other):
        return self._dt <= other._dt

    def __gt__(self, other):
        return self._dt > other._dt

    def __ge__(self, other):
        return self._dt >= other._dt

    def __eq__(self, other):
        return self._dt == other._dt

    def __ne__(self, other):
        return self._dt != other._dt


class MinuteInterval(Interval):
    def _set_interval(self, dt: datetime):
        return dt.replace(second=0, microsecond=0)


class HourInterval(Interval):
    def _set_interval(self, dt: datetime):
        return dt.replace(minute=0, second=0, microsecond=0)


class DayInterval(Interval):
    def _set_interval(self, dt: datetime):
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)


class MonthInterval(Interval):
    def _set_interval(self, dt: datetime):
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


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

    @staticmethod
    def _get_timeline(data: List[int], interval_type: type) -> List[Dict[str, int]]:
        """
        Args:
            data: list of intergers representing timestamps
            interval_type: interval class, one of the Interval childs
        Returns:
             timeline for class interval
        """
        counter = Counter(
            [interval_type(timestamp) for timestamp in data]
        )
        return [
            {
                "time": interval.as_timestamp(),
                "value": counter[interval]
            }
            for interval in sorted(counter)
        ]

    def get_all_timelines(self, data: List[int]) -> List[List[Dict[str, int]]]:
        """
        You can use get_<interval>_timeline one by one, but it will be around for times longer
        This method is optimized
        """
        return [
            self.get_minutes_timeline(data),
            self.get_hours_timeline(data),
            self.get_days_timeline(data),
            self.get_months_timeline(data),
        ]

    def get_minutes_timeline(self, data: List[int]) -> List[Dict[str, int]]:
        return self._get_timeline(data, MinuteInterval)

    def get_hours_timeline(self, data: List[int]) -> List[Dict[str, int]]:
        return self._get_timeline(data, HourInterval)

    def get_days_timeline(self, data: List[int]) -> List[Dict[str, int]]:
        return self._get_timeline(data, DayInterval)

    def get_months_timeline(self, data: List[int]) -> List[Dict[str, int]]:
        return self._get_timeline(data, MonthInterval)
