from .base_loader import BaseLoader
import json
import os
from typing import List, Dict, Optional, Tuple


class TimelinesLoader(BaseLoader):
    """
    Main purpose: to load data from cid, filter it.
    For timelines we don't need all data but only the data that enters timelines timeinterval
    multiplied by the number of objects on a timeline.
    """

    def __init__(self, mem_conf: Dict, static_conf: Dict, biggest_interval: int):
        super().__init__(mem_conf, static_conf)
        self.BIGGEST_INTERVAL = biggest_interval

    def load_data(self, cid: str) -> Tuple[List[int], int]:
        """
        Load data by cid
        :param cid:         OT_Dispatcher's job cid
        :return:            list of dicts from json lines
        """
        data = []
        fresh_time = None
        path_to_cache_dir = self._get_path_to_cache_dir(cid)
        file_names = self._get_cache_file_names(path_to_cache_dir, cid)
        for file_name in file_names:
            self.logger.debug(f'Reading part: {file_name}')
            fresh_time = self.read_file(os.path.join(path_to_cache_dir, file_name), fresh_time)
        if not len(data):
            raise Exception('Empty data')
        return data, fresh_time

    @staticmethod
    def read_file(data: List[int], data_path: str, fresh_time: Optional[int]) -> int:
        """
        Reads file and adds it to data list
        :param data:        list of data that is mutated inside this method
        :param data_path:   path to file
        :param fresh_time:  last time
        """
        with open(data_path) as fr:
            for line in fr:
                _time = json.loads(line)['_time']
                if (fresh_time and _time > fresh_time) or not fresh_time:
                    fresh_time = _time
                data.append(_time)
        return fresh_time
