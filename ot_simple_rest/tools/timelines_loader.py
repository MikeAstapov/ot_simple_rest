from .base_loader import BaseLoader
import json
import os
from typing import List, Dict, Optional, Tuple


class TimelinesLoader(BaseLoader):
    """
    Main purpose: to load data from cid, sort it by _time in ascending order, return the data as a list of dictionaries.
    For timelines we don't need all data but only the data that enters timelines timeinterval
    multiplied by the number of objects on a timeline.
    """

    def __init__(self, mem_conf: Dict, static_conf: Dict, biggest_interval: int):
        super().__init__(mem_conf, static_conf)
        self.BIGGEST_INTERVAL = biggest_interval

    def extract_data_for_timelines(self, data: List[Dict]) -> List[Dict]:
        index = len(data) - 1
        last_time = data[index]['_time']
        while index >= 0 and last_time - data[index]['_time'] > self.BIGGEST_INTERVAL:
            index -= 1
        return data[index:]

    def load_data(self, cid: str) -> List[Dict]:
        """
        Load data by cid
        :param cid:         OT_Dispatcher's job cid
        :return:            list of dicts from json lines
        """
        data = []
        path_to_cache_dir = self._get_path_to_cache_dir(cid)
        file_names = self._get_cache_file_names(path_to_cache_dir, cid)
        for file_name in file_names:
            self.logger.debug(f'Reading part: {file_name}')
            self.read_file(data, os.path.join(path_to_cache_dir, file_name))
        if not len(data):
            raise Exception('Empty data')
        if '_time' not in data[0]:
            raise Exception("'_time' column is missing")
        data.sort(key=lambda entry: entry['_time'])
        # get only entries that required to build timelines
        return data

    def read_file(self, data: List[Dict], data_path: str):
        """
        Reads file and adds it to data list
        :param data:        list of data that is mutated inside this method
        :param data_path:   path to file
        """
        with open(data_path) as fr:
            for line in fr:
                data.append(json.loads(line))
