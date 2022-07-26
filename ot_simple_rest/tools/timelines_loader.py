from .base_loader import BaseLoader
import json
import os
from typing import List, Dict, Optional, Tuple, Union


class TimelinesLoader(BaseLoader):
    """
    Main purpose: to load data from cid, filter it.
    """

    def __init__(self, mem_conf: Dict, static_conf: Dict):
        super().__init__(mem_conf, static_conf)

    def load_data(self, cid: str, from_time: Optional[int] = None, to_time: Optional[int] = None) -> List[int]:
        """
        Load data by cid
        :param cid:         OT_Dispatcher's job cid
        :param from_time:         not relevant for this class
        :param to_time:         not relevant for this class
        :return:            list of timestamps and last_timestamp
        """
        data = []
        path_to_cache_dir = self._get_path_to_cache_dir(cid)
        file_names = self._get_cache_file_names(path_to_cache_dir, cid)
        total_lines = 0
        for file_name in file_names:
            self.logger.debug(f'Reading part: {file_name}')
            self.read_file(data, os.path.join(path_to_cache_dir, file_name))
        if not len(data):
            raise Exception('Empty data')
        return data

    @staticmethod
    def read_file(data: List[int], data_path: str):
        """
        Reads file and adds it to data list
        :param data:        list of timestamps that is mutated inside this method
        :param data_path:   path to file
        """
        with open(data_path) as fr:
            for line in fr:
                data.append(json.loads(line)['_time'])
