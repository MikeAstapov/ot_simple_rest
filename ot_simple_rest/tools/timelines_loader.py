from .base_loader import BaseLoader
import json
import os
from typing import List, Dict, Optional, Tuple, Union


class TimelinesLoader(BaseLoader):
    """
    Main purpose: to load data from cid, filter it.
    For timelines we don't need all data but only the data that enters timelines timeinterval
    multiplied by the number of objects on a timeline.
    """

    def __init__(self, mem_conf: Dict, static_conf: Dict):
        super().__init__(mem_conf, static_conf)
        self.points = 50  # how many points on the timeline
        # approximately self.point months in seconds
        self.BIGGEST_INTERVAL = 86400 * 31 * self.points

    def load_data(self, cid: str) -> Tuple[List[int], int, int]:
        """
        Load data by cid
        :param cid:         OT_Dispatcher's job cid
        :return:            list of timestamps and last_timestamp
        """
        data = []
        fresh_time = None
        path_to_cache_dir = self._get_path_to_cache_dir(cid)
        file_names = self._get_cache_file_names(path_to_cache_dir, cid)
        total_lines = 0
        for file_name in file_names:
            self.logger.debug(f'Reading part: {file_name}')
            fresh_time, line_number = self.read_file(data, os.path.join(path_to_cache_dir, file_name), fresh_time)
            total_lines += line_number
        if not len(data):
            raise Exception('Empty data')
        return data, fresh_time, total_lines

    @staticmethod
    def read_file(data: List[int], data_path: str, fresh_time: Optional[int]) -> Tuple[Union[int, int], int]:
        """
        Reads file and adds it to data list
        :param data:        list of timestamps that is mutated inside this method
        :param data_path:   path to file
        :param fresh_time:  last time
        """
        with open(data_path) as fr:
            line_number = 0
            for line in fr:
                line_number += 1
                _time = json.loads(line)['_time']
                if (fresh_time and _time > fresh_time) or not fresh_time:
                    fresh_time = _time
                data.append(_time)
        return fresh_time, line_number
