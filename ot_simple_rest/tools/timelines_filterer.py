from typing import List, Dict


class TimelinesFilterer:
    """
    It takes resulting timelines and does its sole job of filtering them
    """

    @classmethod
    def remove_empty_intervals(cls, timeline: List[Dict[str, int]]) -> List[Dict[str, int]]:
        """
        Remove intervals where no event has happened
        """
        return list(filter(lambda elem: elem['value'] > 0, timeline))

    @classmethod
    def remove_empty_intervals_many_timelines(cls, timelines: List[List[Dict[str, int]]]) -> List[List[Dict[str, int]]]:
        for i in range(len(timelines)):
            timelines[i] = cls.remove_empty_intervals(timelines[i])
        return timelines


